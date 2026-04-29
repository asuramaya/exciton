use crate::config::Config;
use crate::db::Db;
use crate::execution;
use crate::forecaster::{Forecaster, Regime};
use crate::ingester::RpcRouter;
use crate::intel;
use crate::metadata;
use crate::signals;
use crate::templates::{self, Template};
use rmcp::handler::server::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::*;
use rmcp::{tool, tool_handler, tool_router, ServerHandler};
use serde::{Deserialize, Serialize};
use solana_sdk::signature::Signer;
use std::sync::Arc;

#[derive(Clone)]
pub struct PhotonServer {
    db: Arc<Db>,
    config: Config,
    rpc: Arc<RpcRouter>,
    /// Captured at construction for future diagnostic tools (status, MCP
    /// debug). Not currently surfaced; retain to avoid recomputing.
    #[allow(dead_code)]
    resolved_endpoints: Vec<String>,
    forecaster: Forecaster,
    http: reqwest::Client,
    notifier: Option<Arc<crate::notifier::Notifier>>,
    tool_router: ToolRouter<Self>,
}

// -- Parameter types --

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct InspectParams {
    /// Token mint address or wallet address to investigate
    pub address: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ScoutParams {
    /// Token mint address to scout — pulls deployer profile + website text
    /// + socials. Pure data; no LLM narrative generation.
    pub address: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct HistoryParams {
    /// Token mint address to analyze across stored snapshots.
    pub address: String,
    /// Optional max number of snapshots to load. Defaults to 96.
    #[serde(default)]
    pub limit: Option<usize>,
    /// Optional trailing window. When set, only snapshots within this many
    /// hours of the latest stored point are analyzed.
    #[serde(default)]
    pub window_hours: Option<i64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct HolderForensicsParams {
    /// Token mint address whose owners / top holders should be mapped.
    pub address: String,
    /// Number of top holders to inspect. Defaults to 12, max 20.
    #[serde(default)]
    pub top_n: Option<usize>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct WalletXrayParams {
    /// Wallet address to inspect.
    pub address: String,
    /// Optional mint to compute net 24h/7d balance change for.
    #[serde(default)]
    pub focus_mint: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct PostNoteParams {
    /// Human-readable title — becomes the thought_title on the site.
    pub title: String,
    /// Full markdown body. Image placeholders in the form
    /// `<div class="img-placeholder">[IMAGE: caption]</div>` get picked up
    /// by the image processor on its next tick.
    pub body: String,
    /// Optional filename slug — kebab-case. Auto-derived from the title
    /// when omitted.
    #[serde(default)]
    pub slug: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct FireCallParams {
    /// Mint address of the token being called.
    pub mint: String,
    /// Optional short note documenting the thesis / sizing. Surfaces
    /// publicly via `data/calls.json`.
    #[serde(default)]
    pub note: Option<String>,
    /// Days until auto-expiration. Defaults to 14 when omitted.
    #[serde(default)]
    pub expires_days: Option<i64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct CloseCallParams {
    /// Mint address of the active call to close.
    pub mint: String,
    /// Optional exit note — documents outcome ("trapped", "took +40%",
    /// "timed out, reassessed"). Public via `data/calls.json`.
    #[serde(default)]
    pub exit_note: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct PresentParams {
    /// Token mint address to render
    pub address: String,
    /// Template style: 'monster' (terse one-liner), 'winner' (rich card),
    /// 'ops' (compact), or 'inspect' (full signal dump)
    pub style: String,
}

#[derive(Debug, Serialize)]
struct PresentResult {
    address: String,
    style: String,
    html: String,
    classification: String,
    confidence: i32,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct TradeParams {
    /// Token mint address
    pub token: String,
    /// 'buy' or 'sell'
    pub side: String,
    /// Amount in SOL (for buys) or token UI amount (for sells)
    pub amount: f64,
    /// Must be true to actually execute. When false or omitted, returns a preview.
    #[serde(default)]
    pub confirmed: bool,
}

// -- Response types --

#[derive(Debug, Serialize)]
struct ScanResult {
    healthy: bool,
    rpc_connected: bool,
    rpc_endpoints: usize,
    rpc_healthy: usize,
    regime: String,
    opportunities: Vec<Opportunity>,
    alerts: Vec<AlertInfo>,
}

#[derive(Debug, Serialize)]
struct Opportunity {
    token: String,
    confidence: i32,
    momentum: i32,
    distribution: i32,
    spring: i32,
    classification: String,
    coverage: usize,
    recommended_position_pct: f64,
    reasoning: String,
}

#[derive(Debug, Serialize)]
struct AlertInfo {
    alert_type: String,
    message: String,
    /// Raw confidence when the alert was written by the scanner
    confidence: i32,
    /// Freshness-decayed confidence — scales with age. 0 means suppressed.
    effective_confidence: i32,
    /// Seconds since the alert was written
    age_seconds: i64,
    /// Live re-inspection result for the top 3 alerts — None for the rest
    #[serde(skip_serializing_if = "Option::is_none")]
    live_state: Option<LiveState>,
    /// Market metadata (DexScreener) for the top 3 alerts — None for the rest
    #[serde(skip_serializing_if = "Option::is_none")]
    meta: Option<MetaSummary>,
}

#[derive(Debug, Serialize)]
struct LiveState {
    classification: String,
    confidence: i32,
    momentum: i32,
    distribution: i32,
    top_holder_pct: f64,
    tx_rate: f64,
    /// True when live classification no longer matches the alert's original class
    drifted: bool,
}

#[derive(Debug, Serialize)]
struct MetaSummary {
    symbol: String,
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    price_usd: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    market_cap_usd: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    liquidity_usd: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    volume_24h_usd: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    price_change_5m: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    price_change_1h: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    age_seconds: Option<i64>,
}

use crate::notifier::{effective_confidence, STALE_SECS};

/// Maps an alert_type string like "staircase" / "grinder" to the expected
/// live classification — used to detect drift between stored and current state.
fn alert_type_to_classification(alert_type: &str) -> Option<&'static str> {
    match alert_type {
        "staircase" => Some("STAIRCASE"),
        "grinder" => Some("GRINDER"),
        "spring" => Some("SPRING"),
        "surge" => Some("SURGE"),
        "active_trap" => Some("ACTIVE_TRAP"),
        "developing" => Some("DEVELOPING"),
        _ => None,
    }
}

#[derive(Debug, Serialize)]
struct InspectResult {
    target: String,
    target_type: String,
    classification: String,
    safety: Vec<SignalDetail>,
    signals: Vec<SignalDetail>,
    risk_rating: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    delta: Option<DeltaInfo>,
}

#[derive(Debug, Serialize)]
struct DeltaInfo {
    time_since_last: String,
    top_holder_delta: String,
    top5_delta: String,
    holder_count_delta: String,
    momentum_delta: String,
    concentration_direction: String,
    classification_changed: bool,
    previous_classification: String,
}

#[derive(Debug, Serialize)]
struct SignalDetail {
    layer: String,
    signal_type: String,
    score: i32,
    details: String,
}

#[derive(Debug, Serialize)]
struct TradePreview {
    action: String,
    token: String,
    /// SOL for buys; token UI amount for sells.
    amount_in: f64,
    amount_in_unit: String,
    estimated_output: String,
    slippage_bps: u16,
    fees: String,
    confidence: i32,
    safety_checks: Vec<String>,
    requires_confirmation: bool,
}

#[derive(Debug, Serialize)]
struct StatusResult {
    system_health: SystemHealth,
    pending_alerts: usize,
    positions: Vec<Position>,
    wallet: String,
    total_balance_sol: f64,
    total_pnl_sol: f64,
    exposure_pct: f64,
    message: String,
}

#[derive(Debug, Serialize)]
struct SystemHealth {
    rpc_connected: bool,
    rpc_endpoints: usize,
    rpc_healthy: usize,
    db_writable: bool,
    signal_layers_active: usize,
    current_slot: Option<u64>,
    data_freshness: String,
}

#[derive(Debug, Serialize)]
struct Position {
    token: String,
    amount_sol_in: f64,
    current_value_sol: f64,
    pnl_sol: f64,
    pnl_pct: f64,
}

/// Turn a human title into a kebab-case slug — lowercase ASCII, dashes
/// between word boundaries, nothing fancy. Used by `post_note` when the
/// caller doesn't provide a slug explicitly.
fn slugify(title: &str) -> String {
    let mut out = String::with_capacity(title.len());
    let mut prev_dash = true;
    for c in title.chars() {
        if c.is_alphanumeric() {
            for lo in c.to_lowercase() {
                out.push(lo);
            }
            prev_dash = false;
        } else if !prev_dash {
            out.push('-');
            prev_dash = true;
        }
    }
    while out.ends_with('-') {
        out.pop();
    }
    if out.is_empty() {
        out.push_str("note");
    }
    out
}

fn to_json<T: Serialize>(data: &T) -> String {
    serde_json::to_string_pretty(data).unwrap_or_else(|e| format!("Error: {e}"))
}

impl PhotonServer {
    pub fn new(
        db: Arc<Db>,
        config: Config,
        rpc: Arc<RpcRouter>,
        resolved_endpoints: Vec<String>,
        notifier: Option<Arc<crate::notifier::Notifier>>,
    ) -> Self {
        Self {
            db,
            config,
            rpc,
            resolved_endpoints,
            forecaster: Forecaster::new(),
            http: reqwest::Client::new(),
            notifier,
            tool_router: Self::tool_router(),
        }
    }

    pub fn is_healthy(&self) -> bool {
        self.db.list_tables().is_ok()
    }

    fn spawn_publisher_flush(&self) {
        let Some(mp) = self.config.madapes.clone() else { return; };
        if !mp.enabled { return; }
        let pub_instance = crate::publisher::Publisher::new(
            mp,
            self.config.wallet.public_key.clone(),
            self.rpc.clone(),
            self.db.clone(),
        );
        tokio::spawn(async move {
            if let Err(e) = pub_instance.run_once().await {
                tracing::warn!("post-call publisher flush failed: {}", e);
            }
        });
    }
}

#[tool_router]
impl PhotonServer {
    /// Scan the market: read alerts from background scanner, show pending opportunities.
    /// The background scanner runs every 30s discovering and analyzing new tokens.
    ///
    /// Applies freshness decay (alerts past 30m are auto-acknowledged and suppressed),
    /// re-sorts by effective confidence, then live-re-inspects the top 3 alongside
    /// fetching market metadata so the caller sees current state, not stale state.
    #[tool]
    async fn scan(&self) -> String {
        let _ = self.db.audit_log("claude", "scan", "Market scan requested");

        let rpc_connected = self.rpc.check_connection().await.unwrap_or(false);

        // Drain alerts that have aged out entirely before reading
        let _ = self.db.acknowledge_stale_alerts(STALE_SECS);

        let pending_alerts = self.db.get_pending_alerts(50).unwrap_or_default();
        let now_ts = chrono::Utc::now().timestamp();

        let mut opportunities = Vec::new();
        let mut alerts: Vec<AlertInfo> = Vec::new();
        let mut alert_ids = Vec::new();

        for alert in &pending_alerts {
            alert_ids.push(alert.id);
            let age = (now_ts - alert.timestamp).max(0);
            let eff = effective_confidence(alert.confidence, age);

            if alert.alert_type == "discovery" {
                let position_pct = self.forecaster.position_pct(alert.confidence, 3);
                opportunities.push(Opportunity {
                    token: alert.token_address.clone().unwrap_or_default(),
                    confidence: alert.confidence,
                    momentum: 0,
                    distribution: 0,
                    spring: 0,
                    classification: "FROM_ALERT".to_string(),
                    coverage: 3,
                    recommended_position_pct: position_pct,
                    reasoning: alert.message.clone(),
                });
            }

            alerts.push(AlertInfo {
                alert_type: alert.alert_type.clone(),
                message: alert.message.clone(),
                confidence: alert.confidence,
                effective_confidence: eff,
                age_seconds: age,
                live_state: None,
                meta: None,
            });
        }

        let _ = self.db.acknowledge_alerts(&alert_ids);

        // Re-sort by freshness-adjusted confidence so stale alerts drop to the bottom
        alerts.sort_by(|a, b| b.effective_confidence.cmp(&a.effective_confidence));

        // Live re-inspect the top 3 tokens and fetch metadata concurrently
        let top_addrs: Vec<(usize, String, String)> = alerts
            .iter()
            .enumerate()
            .filter_map(|(i, a)| {
                if a.effective_confidence == 0 {
                    return None;
                }
                pending_alerts
                    .iter()
                    .find(|p| p.message == a.message)
                    .and_then(|p| p.token_address.clone())
                    .map(|addr| (i, addr, a.alert_type.clone()))
            })
            .take(3)
            .collect();

        if !top_addrs.is_empty() && rpc_connected {
            let mut handles = Vec::new();
            for (idx, addr, alert_type) in top_addrs {
                let rpc = self.rpc.clone();
                let db = self.db.clone();
                handles.push(tokio::spawn(async move {
                    let analysis = signals::analyze_token(&rpc, &addr, Some(&db), None).await.ok();
                    let meta = metadata::fetch(&addr).await.ok().flatten();
                    (idx, alert_type, analysis, meta)
                }));
            }
            for h in handles {
                if let Ok((idx, alert_type, analysis, meta)) = h.await {
                    if let Some(a) = analysis {
                        let expected = alert_type_to_classification(&alert_type);
                        let drifted = expected
                            .map(|e| e != a.confidence.classification)
                            .unwrap_or(false);
                        alerts[idx].live_state = Some(LiveState {
                            classification: a.confidence.classification.clone(),
                            confidence: a.confidence.total,
                            momentum: a.confidence.momentum,
                            distribution: a.confidence.distribution,
                            top_holder_pct: a.top_holder_pct,
                            tx_rate: a.tx_rate,
                            drifted,
                        });
                    }
                    if let Some(m) = meta {
                        let age_seconds = m.age_seconds();
                        alerts[idx].meta = Some(MetaSummary {
                            symbol: m.symbol,
                            name: m.name,
                            price_usd: m.price_usd,
                            market_cap_usd: m.market_cap_usd.or(m.fdv_usd),
                            liquidity_usd: m.liquidity_usd,
                            volume_24h_usd: m.volume_24h_usd,
                            price_change_5m: m.price_change_5m,
                            price_change_1h: m.price_change_1h,
                            age_seconds,
                        });
                    }
                }
            }
        }

        if !rpc_connected {
            alerts.push(AlertInfo {
                alert_type: "system".to_string(),
                message: "RPC not connected — check API keys and endpoints".to_string(),
                confidence: 100,
                effective_confidence: 100,
                age_seconds: 0,
                live_state: None,
                meta: None,
            });
        }

        opportunities.sort_by(|a, b| b.confidence.cmp(&a.confidence));

        to_json(&ScanResult {
            healthy: self.is_healthy() && rpc_connected,
            rpc_connected,
            rpc_endpoints: self.rpc.endpoint_count(),
            rpc_healthy: self.rpc.healthy_count(),
            regime: Regime::LowActivityGrind.to_string(),
            opportunities,
            alerts,
        })
    }

    /// Deep-dive investigation of a token or wallet.
    /// Flow: check existence -> run all signal layers -> pull history -> safety checks -> present full picture.
    #[tool]
    async fn inspect(&self, Parameters(params): Parameters<InspectParams>) -> String {
        let _ = self.db.audit_log(
            "claude",
            "inspect",
            &format!("Inspecting {}", params.address),
        );

        // Run full token analysis through all signal layers
        match signals::analyze_token(&self.rpc, &params.address, Some(&self.db), None).await {
            Ok(analysis) => {
                let safety_scores: Vec<SignalDetail> = analysis
                    .scores
                    .iter()
                    .filter(|s| s.layer == signals::SignalLayer::Safety)
                    .map(|s| SignalDetail {
                        layer: "Safety".to_string(),
                        signal_type: s.signal_type.clone(),
                        score: s.score,
                        details: s.details.clone(),
                    })
                    .collect();

                let other_scores: Vec<SignalDetail> = analysis
                    .scores
                    .iter()
                    .filter(|s| s.layer != signals::SignalLayer::Safety)
                    .map(|s| SignalDetail {
                        layer: format!("{:?}", s.layer),
                        signal_type: s.signal_type.clone(),
                        score: s.score,
                        details: s.details.clone(),
                    })
                    .collect();

                let risk_rating = format!(
                    "[{}] {} — {} signals",
                    analysis.confidence.classification,
                    analysis.confidence.reasoning,
                    analysis.scores.len()
                );

                // Store in DB for historical tracking
                let _ = self
                    .db
                    .insert_token(&params.address, analysis.confidence.total);

                let delta_info = analysis.delta.as_ref().map(|d| {
                    let elapsed = if d.time_elapsed_seconds < 60 {
                        format!("{}s ago", d.time_elapsed_seconds)
                    } else if d.time_elapsed_seconds < 3600 {
                        format!("{}m ago", d.time_elapsed_seconds / 60)
                    } else {
                        format!("{:.1}h ago", d.time_elapsed_seconds as f64 / 3600.0)
                    };
                    DeltaInfo {
                        time_since_last: elapsed,
                        top_holder_delta: format!("{:+.1}%", d.top_holder_delta),
                        top5_delta: format!("{:+.1}%", d.top5_delta),
                        holder_count_delta: format!("{:+}", d.holder_count_delta),
                        momentum_delta: format!("{:+}", d.momentum_delta),
                        concentration_direction: d.concentration_direction.clone(),
                        classification_changed: d.classification_changed,
                        previous_classification: d.previous.classification.clone(),
                    }
                });

                to_json(&InspectResult {
                    target: params.address,
                    target_type: "token".to_string(),
                    classification: analysis.confidence.classification.clone(),
                    safety: safety_scores,
                    signals: other_scores,
                    risk_rating,
                    delta: delta_info,
                })
            }
            Err(e) => to_json(&InspectResult {
                target: params.address,
                target_type: "unknown".to_string(),
                classification: "ERROR".to_string(),
                safety: vec![],
                signals: vec![SignalDetail {
                    layer: "System".to_string(),
                    signal_type: "error".to_string(),
                    score: 0,
                    details: format!("Analysis failed: {}", e),
                }],
                risk_rating: format!("UNKNOWN — analysis error: {}", e),
                delta: None,
            }),
        }
    }

    /// Render a Telegram-ready HTML block for a token using one of four templates.
    /// Does not post — returns the rendered string so templates can be iterated on.
    /// Styles: 'monster' (one-line alert), 'winner' (rich promotion card),
    ///         'ops' (compact line for heartbeats), 'inspect' (full signal dump).
    #[tool]
    async fn present(&self, Parameters(params): Parameters<PresentParams>) -> String {
        let _ = self.db.audit_log(
            "claude",
            "present",
            &format!("Render {} as {}", params.address, params.style),
        );

        let template = match Template::parse(&params.style) {
            Some(t) => t,
            None => {
                return to_json(&PresentResult {
                    address: params.address,
                    style: params.style,
                    html: String::new(),
                    classification: "ERROR".to_string(),
                    confidence: 0,
                });
            }
        };

        match signals::analyze_token(&self.rpc, &params.address, Some(&self.db), None).await {
            Ok(analysis) => {
                let meta = metadata::fetch(&params.address).await.ok().flatten();
                let html = templates::render(&analysis, meta.as_ref(), template);
                to_json(&PresentResult {
                    address: params.address,
                    style: params.style,
                    html,
                    classification: analysis.confidence.classification.clone(),
                    confidence: analysis.confidence.total,
                })
            }
            Err(e) => to_json(&PresentResult {
                address: params.address,
                style: params.style,
                html: format!("Error rendering: {}", e),
                classification: "ERROR".to_string(),
                confidence: 0,
            }),
        }
    }

    /// Scout a token with raw, LLM-free data extractors: deployer wallet
    /// profile (current balance, % sold vs. launch, 24h/7d activity) +
    /// registered website fetched and stripped to readable text + socials.
    /// Returns structured JSON for the caller to synthesize.
    #[tool]
    async fn scout(&self, Parameters(params): Parameters<ScoutParams>) -> String {
        let _ = self
            .db
            .audit_log("claude", "scout", &format!("Scouting {}", params.address));
        match crate::scout::scout(&params.address, &self.rpc, &self.db).await {
            Ok(report) => to_json(&report),
            Err(e) => format!("{{\"error\": \"{}\"}}", e),
        }
    }

    /// Deep scout: runs all six on-chain analysis tools against a mint —
    /// whale behavior, LP status, deployer track record, holder evolution,
    /// sniper retention, and reference-cohort overlap. Pure data, no LLM.
    /// ~30–60s per mint under normal load.
    #[tool]
    async fn deep_scout(&self, Parameters(params): Parameters<ScoutParams>) -> String {
        let _ = self.db.audit_log(
            "claude",
            "deep_scout",
            &format!("Deep scout {}", params.address),
        );

        let market = crate::market::get_market(&params.address)
            .await
            .ok()
            .flatten();
        let pair_addr = market
            .as_ref()
            .map(|m| m.pair_address.clone())
            .unwrap_or_default();
        let pair_dex = market
            .as_ref()
            .map(|m| m.pair_dex.clone())
            .unwrap_or_default();

        let basic = crate::scout::scout(&params.address, &self.rpc, &self.db)
            .await
            .ok();
        let whales = crate::scout::whale_trace(&params.address, &self.rpc)
            .await
            .unwrap_or_default();
        let lp = if !pair_addr.is_empty() && !pair_dex.is_empty() {
            crate::scout::lp_check(&pair_addr, &pair_dex, &self.rpc)
                .await
                .ok()
        } else {
            None
        };
        let deployer_history = match &basic {
            Some(b) => match &b.deployer {
                Some(d) => crate::scout::deployer_history(&d.deployer_address, &self.db)
                    .await
                    .unwrap_or_default(),
                None => Vec::new(),
            },
            None => Vec::new(),
        };
        let holder_evo =
            crate::scout::holder_evolution(&params.address, 24, &self.db).unwrap_or_default();
        let snipers = crate::scout::sniper_cohort(&params.address, &self.rpc, &self.db)
            .await
            .unwrap_or_default();
        let cohort = crate::scout::cohort_overlap(&params.address, &self.rpc, &self.db)
            .await
            .unwrap_or_default();

        let combined = serde_json::json!({
            "mint": params.address,
            "market": {
                "symbol": market.as_ref().map(|m| m.symbol.clone()).unwrap_or_default(),
                "name": market.as_ref().map(|m| m.name.clone()).unwrap_or_default(),
                "mcap_usd": market.as_ref().map(|m| m.mcap_usd).unwrap_or(0.0),
                "liquidity_usd": market.as_ref().map(|m| m.liquidity_usd).unwrap_or(0.0),
                "pair_dex": pair_dex,
                "pair_address": pair_addr,
            },
            "basic_scout": basic,
            "whales": whales,
            "lp": lp,
            "deployer_history": deployer_history,
            "holder_evolution_24h": holder_evo,
            "sniper_cohort": snipers,
            "cohort_overlap": cohort,
        });
        serde_json::to_string_pretty(&combined)
            .unwrap_or_else(|e| format!("{{\"error\":\"{}\"}}", e))
    }

    /// Historical analysis over stored token snapshots — trend, concentration,
    /// classification flips, and hidden compression / decay flags.
    #[tool]
    async fn historical_analysis(&self, Parameters(params): Parameters<HistoryParams>) -> String {
        let _ = self.db.audit_log(
            "claude",
            "historical_analysis",
            &format!("Historical analysis {}", params.address),
        );
        match intel::historical_analysis(
            &params.address,
            &self.db,
            params.limit.unwrap_or(96),
            params.window_hours,
        ) {
            Ok(report) => to_json(&report),
            Err(e) => format!("{{\"error\":\"{}\"}}", e),
        }
    }

    /// Owner / holder forensics — maps top holders to owners, labels AMM vs
    /// wallets, checks smart-wallet / reference-mint overlap, and detects
    /// repeated balance clusters or insider-network concentration.
    #[tool]
    async fn holder_forensics(
        &self,
        Parameters(params): Parameters<HolderForensicsParams>,
    ) -> String {
        let _ = self.db.audit_log(
            "claude",
            "holder_forensics",
            &format!("Holder forensics {}", params.address),
        );
        match intel::holder_forensics(
            &params.address,
            &self.rpc,
            &self.db,
            params.top_n.unwrap_or(12),
        )
        .await
        {
            Ok(report) => to_json(&report),
            Err(e) => format!("{{\"error\":\"{}\"}}", e),
        }
    }

    /// Wallet x-ray — holdings, recent activity cadence, reference-mint
    /// overlap, and optional focus-mint flow for a single wallet owner.
    #[tool]
    async fn wallet_xray(&self, Parameters(params): Parameters<WalletXrayParams>) -> String {
        let _ = self.db.audit_log(
            "claude",
            "wallet_xray",
            &format!("Wallet xray {}", params.address),
        );
        match intel::wallet_xray(
            &params.address,
            params.focus_mint.as_deref(),
            &self.rpc,
            &self.db,
        )
        .await
        {
            Ok(report) => to_json(&report),
            Err(e) => format!("{{\"error\":\"{}\"}}", e),
        }
    }

    /// Second-order signal synthesis — combines stored history, holder
    /// forensics, whales, deployer, LP, sniper retention, and cohort overlap
    /// into machine-readable hidden signals.
    #[tool]
    async fn deep_signals(&self, Parameters(params): Parameters<ScoutParams>) -> String {
        let _ = self.db.audit_log(
            "claude",
            "deep_signals",
            &format!("Deep signals {}", params.address),
        );
        match intel::deep_signals(&params.address, &self.rpc, &self.db).await {
            Ok(report) => to_json(&report),
            Err(e) => format!("{{\"error\":\"{}\"}}", e),
        }
    }

    /// One-shot health view across every subsystem — RPC router state,
    /// scanner's last alerts, publisher / image-processor / auto-ack
    /// freshness (inferred from DB activity and filesystem timestamps),
    /// active calls, wallet balance. Designed so a context-less instance
    /// can answer "is anything broken?" in a single tool call.
    #[tool]
    async fn pipeline_health(&self) -> String {
        let rpc_ok = self.rpc.check_connection().await.unwrap_or(false);
        let rpc_total = self.rpc.endpoint_count();
        let rpc_healthy = self.rpc.healthy_count();
        let wallet_lamports = self
            .rpc
            .get_balance(&self.config.wallet.public_key)
            .await
            .unwrap_or(0);
        let wallet_sol = wallet_lamports as f64 / 1e9;

        // Publisher freshness: mtime on data/health.json in the MadApes repo.
        let (publisher_path, publisher_age_seconds): (String, i64) =
            match self.config.madapes.as_ref() {
                Some(mp) => {
                    let p = format!("{}/data/health.json", mp.repo_path);
                    let age = std::fs::metadata(&p)
                        .and_then(|m| m.modified())
                        .ok()
                        .and_then(|t| t.elapsed().ok())
                        .map(|d| d.as_secs() as i64)
                        .unwrap_or(-1);
                    (p, age)
                }
                None => ("(madapes disabled)".to_string(), -1),
            };
        let assets_age_seconds: i64 = match self.config.madapes.as_ref() {
            Some(mp) => {
                let p = format!("{}/thoughts/assets.json", mp.repo_path);
                std::fs::metadata(&p)
                    .and_then(|m| m.modified())
                    .ok()
                    .and_then(|t| t.elapsed().ok())
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(-1)
            }
            None => -1,
        };

        // Scanner / alert activity — newest alert ts.
        let last_alert_ts: i64 = {
            let alerts = self.db.get_pending_alerts(1).unwrap_or_default();
            alerts.first().map(|a| a.timestamp).unwrap_or(0)
        };
        let now = chrono::Utc::now().timestamp();
        let last_alert_age = if last_alert_ts > 0 {
            now - last_alert_ts
        } else {
            -1
        };

        let active_calls = self.db.list_calls(true, 50).unwrap_or_default();
        let smart_wallets = self.db.list_active_smart_wallets().unwrap_or_default();
        let ref_mints = self.db.list_reference_mints().unwrap_or_default();

        let payload = serde_json::json!({
            "rpc": {
                "any_endpoint_up": rpc_ok,
                "total_endpoints": rpc_total,
                "healthy_endpoints": rpc_healthy,
            },
            "wallet": {
                "address": self.config.wallet.public_key,
                "sol_balance": wallet_sol,
            },
            "publisher": {
                "health_path": publisher_path,
                "last_push_seconds_ago": publisher_age_seconds,
                "status": if publisher_age_seconds < 0 || publisher_age_seconds > 720 {
                    "stale"
                } else {
                    "fresh"
                },
            },
            "image_processor": {
                "manifest_age_seconds": assets_age_seconds,
            },
            "scanner": {
                "newest_alert_age_seconds": last_alert_age,
            },
            "calls": {
                "active_count": active_calls.len(),
                "active_mints": active_calls.iter().map(|c| &c.mint).collect::<Vec<_>>(),
            },
            "curated_state": {
                "smart_wallets_tracked": smart_wallets.len(),
                "reference_mints": ref_mints.len(),
            },
            "timestamp": now,
        });
        serde_json::to_string_pretty(&payload)
            .unwrap_or_else(|e| format!("{{\"error\":\"{}\"}}", e))
    }

    /// Force a one-shot publisher run immediately instead of waiting for the
    /// background cadence. Useful after firing/closing calls or after a note
    /// / image / data mutation that should land on the public surface now.
    #[tool]
    async fn pulse(&self) -> String {
        let _ = self
            .db
            .audit_log("claude", "pulse", "manual publisher pulse");
        let Some(mp) = self.config.madapes.clone() else {
            return "{\"error\":\"madapes config missing\"}".to_string();
        };
        if !mp.enabled {
            return "{\"error\":\"madapes publisher disabled\"}".to_string();
        }

        let publisher = crate::publisher::Publisher::new(
            mp.clone(),
            self.config.wallet.public_key.clone(),
            self.rpc.clone(),
            self.db.clone(),
        );

        match publisher.run_once().await {
            Ok(committed) => serde_json::json!({
                "ok": true,
                "committed": committed,
                "repo_path": mp.repo_path,
                "timestamp": chrono::Utc::now().timestamp(),
            })
            .to_string(),
            Err(e) => serde_json::json!({
                "ok": false,
                "error": format!("{}", e),
            })
            .to_string(),
        }
    }

    /// Append a new note to MadApes.ai/thoughts, update the index, commit
    /// with the `note:` prefix and push. Respects append-only — fails with
    /// an error if the target filename already exists. The image processor
    /// picks up any `<div class="img-placeholder">[IMAGE: ...]</div>` blocks
    /// on its next 15-minute tick (no action needed here).
    #[tool]
    fn post_note(&self, Parameters(params): Parameters<PostNoteParams>) -> String {
        let _ = self.db.audit_log("claude", "post_note", &params.title);
        let Some(mp) = self.config.madapes.clone() else {
            return "{\"error\":\"madapes config missing\"}".to_string();
        };
        let date = chrono::Utc::now().format("%Y-%m-%d").to_string();
        let slug = params.slug.unwrap_or_else(|| slugify(&params.title));
        let file = format!("{}_{}.md", date, slug);
        let thoughts_dir = std::path::PathBuf::from(&mp.repo_path).join("thoughts");
        let path = thoughts_dir.join(&file);

        if path.exists() {
            return serde_json::json!({
                "error": "filename collision — note already exists",
                "path": path.to_string_lossy(),
            })
            .to_string();
        }
        if let Err(e) = std::fs::write(&path, &params.body) {
            return serde_json::json!({ "error": format!("write failed: {}", e) }).to_string();
        }

        // Update the index.json — preserve existing order, prepend newest.
        let index_path = thoughts_dir.join("index.json");
        let mut index_val: serde_json::Value = std::fs::read_to_string(&index_path)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_else(|| serde_json::json!({"thoughts": []}));
        let arr = index_val.get_mut("thoughts").and_then(|v| v.as_array_mut());
        if let Some(arr) = arr {
            arr.insert(
                0,
                serde_json::json!({
                    "date": date,
                    "file": file,
                    "title": params.title,
                }),
            );
        }
        let _ = std::fs::write(
            &index_path,
            serde_json::to_string_pretty(&index_val).unwrap_or_default(),
        );

        // git add + commit + push with the `note:` prefix.
        let repo = &mp.repo_path;
        let msg = format!("note: {}", params.title);
        let _ = std::process::Command::new("git")
            .args(["-C", repo, "add", "thoughts/"])
            .output();
        let commit = std::process::Command::new("git")
            .args(["-C", repo, "commit", "-m", &msg])
            .output();
        let committed = commit.as_ref().map(|o| o.status.success()).unwrap_or(false);
        if committed {
            let _ = std::process::Command::new("git")
                .args(["-C", repo, "push", "--quiet"])
                .output();
        }

        serde_json::json!({
            "file": file,
            "path": path.to_string_lossy(),
            "committed": committed,
            "next_tick": "image processor will pick up placeholders within 15 min",
        })
        .to_string()
    }

    /// Fire a public call — freezes the entry state (mcap, price, liquidity,
    /// top holder %) and writes a row into the `calls` table. Publisher
    /// mirrors to `data/calls.json` on its next tick; per-mint
    /// `data/whales/<mint>.json` gets published alongside so the triggers
    /// are publicly auditable. Default 14-day expiration window.
    #[tool]
    async fn fire_call(&self, Parameters(params): Parameters<FireCallParams>) -> String {
        let _ = self.db.audit_log("claude", "fire_call", &params.mint);

        if self.db.has_active_call(&params.mint).unwrap_or(false) {
            return serde_json::json!({
                "error": "active call already exists for this mint",
            })
            .to_string();
        }

        let market = crate::market::get_market(&params.mint).await.ok().flatten();
        let (sym, mcap, price, liq, dex) = match &market {
            Some(m) => (
                m.symbol.clone(),
                m.mcap_usd,
                m.price_usd,
                m.liquidity_usd,
                m.pair_dex.clone(),
            ),
            None => (String::new(), 0.0, 0.0, 0.0, String::new()),
        };
        let top_pct = match self.rpc.get_token_largest_accounts(&params.mint).await {
            Ok(holders) if !holders.is_empty() => {
                let supply_ui = self
                    .rpc
                    .get_token_supply(&params.mint)
                    .await
                    .map(|s| s.ui_amount)
                    .unwrap_or(1_000_000_000.0);
                if supply_ui > 0.0 {
                    holders[0].ui_amount / supply_ui * 100.0
                } else {
                    0.0
                }
            }
            _ => 0.0,
        };

        let called_at = chrono::Utc::now().timestamp();
        let note = params.note.clone().unwrap_or_default();
        let entry_tx_rate = self
            .db
            .get_latest_snapshot(&params.mint)
            .ok()
            .flatten()
            .map(|s| s.tx_rate)
            .unwrap_or(0.0);
        let inserted = self.db.insert_call(
            &params.mint,
            &sym,
            "MANUAL",
            0,
            called_at,
            mcap,
            price,
            liq,
            top_pct,
            &dex,
            &note,
            "mcp",
            entry_tx_rate,
        );
        let id = match inserted {
            Ok(Some(id)) => id,
            Ok(None) => {
                return serde_json::json!({"error": "insert returned None (duplicate active call)"})
                    .to_string();
            }
            Err(e) => {
                return serde_json::json!({"error": format!("insert failed: {}", e)}).to_string();
            }
        };

        let expires_days = params.expires_days.unwrap_or(14).max(1);
        let expires_at = called_at + expires_days * 86_400;
        let _ = self.db.set_call_expiration(&params.mint, Some(expires_at));

        // Propagate to Telegram and website immediately.
        if let Some(ref n) = self.notifier {
            let note_str = note.clone();
            let entry_mcap = mcap;
            let addr = params.mint.clone();
            let n = n.clone();
            tokio::spawn(async move {
                if let Err(e) = n.fire_call_card(&addr, &note_str, entry_mcap).await {
                    tracing::warn!("fire_call_card failed: {}", e);
                }
            });
        }
        self.spawn_publisher_flush();

        serde_json::json!({
            "id": id,
            "mint": params.mint,
            "symbol": sym,
            "entry_mcap_usd": mcap,
            "entry_price_usd": price,
            "entry_liquidity_usd": liq,
            "entry_top_holder_pct": top_pct,
            "entry_pair_dex": dex,
            "expires_at": expires_at,
            "expires_in_days": expires_days,
            "next_tick": "publisher mirrors to data/calls.json within 5 min",
        })
        .to_string()
    }

    /// Record a deliberate withdrawal from an active call. Sets status to
    /// 'withdrew' — distinguishable from 'failed' (auto-closed by scanner on
    /// collapse) and 'expired' (timed out). Publisher moves the row to history
    /// on the next tick. Always call this when exiting intentionally.
    #[tool]
    async fn close_call(&self, Parameters(params): Parameters<CloseCallParams>) -> String {
        let _ = self.db.audit_log("claude", "close_call", &params.mint);
        let exit_price = crate::market::get_market(&params.mint)
            .await
            .ok()
            .flatten()
            .map(|m| m.price_usd)
            .unwrap_or(0.0);
        let note = params.exit_note.clone().unwrap_or_default();
        match self.db.close_call(&params.mint, exit_price, &note) {
            Ok(true) => {
                if let Some(ref n) = self.notifier {
                    let mint = params.mint.clone();
                    let note_str = note.clone();
                    let n = n.clone();
                    tokio::spawn(async move {
                        if let Err(e) = n.update_call_outcome(&mint, "withdrew", None, &note_str).await {
                            tracing::warn!("update_call_outcome failed: {}", e);
                        }
                    });
                }
                self.spawn_publisher_flush();
                serde_json::json!({
                    "mint": params.mint,
                    "exit_price_usd": exit_price,
                    "exit_note": note,
                    "next_tick": "publisher moves to history within 5 min",
                })
                .to_string()
            }
            Ok(false) => serde_json::json!({
                "error": "no active call found for that mint",
            })
            .to_string(),
            Err(e) => serde_json::json!({"error": format!("close failed: {}", e)}).to_string(),
        }
    }

    /// List currently active calls. Use before firing a new one or when
    /// deciding which calls to close. Returns the same rows visible on the
    /// public site.
    #[tool]
    async fn active_calls(&self) -> String {
        let rows = self.db.list_calls(true, 100).unwrap_or_default();
        serde_json::to_string_pretty(&rows).unwrap_or_else(|e| format!("{{\"error\":\"{}\"}}", e))
    }

    /// Execute a trade via Jupiter v6 + Jito MEV protection.
    ///
    /// Set confirmed=false (or omit) to preview without executing.
    /// Set confirmed=true to actually sign and submit the transaction.
    ///
    /// For buys: amount = SOL to spend.
    /// For sells: amount = token UI amount to sell (e.g. 50000.0 tokens).
    ///
    /// Requires PHOTON_PRIVATE_KEY env var (base58 secret key).
    #[tool]
    async fn trade(&self, Parameters(params): Parameters<TradeParams>) -> String {
        let _ = self.db.audit_log(
            "claude",
            if params.confirmed { "trade_execute" } else { "trade_preview" },
            &format!("{} {} on {}", params.side, params.amount, params.token),
        );

        let side = params.side.to_lowercase();
        if side != "buy" && side != "sell" {
            return serde_json::json!({"error": "side must be 'buy' or 'sell'"}).to_string();
        }

        // Safety: run signal analysis. For buys, a failed analysis (RPC error) is
        // treated as a block — we won't spend money on a token we can't vet. For
        // sells we allow degraded-RPC through so a stuck position can still exit.
        let analysis = signals::analyze_token(&self.rpc, &params.token, Some(&self.db), None).await;
        if side == "buy" {
            match &analysis {
                Err(e) => {
                    return serde_json::json!({
                        "error": "trade blocked",
                        "reason": format!("safety analysis failed — cannot vet token: {}", e),
                        "hint": "retry once RPC is healthy; pass side=sell to exit existing positions",
                    })
                    .to_string();
                }
                Ok(a) if a.confidence.classification.starts_with("UNSAFE") => {
                    return serde_json::json!({
                        "error": "trade blocked",
                        "reason": "token classified UNSAFE — rug mechanics detected",
                        "classification": a.confidence.classification,
                    })
                    .to_string();
                }
                Ok(_) => {}
            }
        } else if let Ok(a) = &analysis {
            // For sells: UNSAFE still blocks — exiting is fine, but we won't
            // accidentally double-down on a rugged token via a mis-keyed sell.
            if a.confidence.classification.starts_with("UNSAFE") {
                return serde_json::json!({
                    "error": "trade blocked",
                    "reason": "token classified UNSAFE",
                    "classification": a.confidence.classification,
                })
                .to_string();
            }
        }
        let analysis = analysis.ok();

        // Balance check
        let balance_lamports = self
            .rpc
            .get_balance(&self.config.wallet.public_key)
            .await
            .unwrap_or(0);
        let balance_sol = balance_lamports as f64 / 1_000_000_000.0;

        if side == "buy" && params.amount > balance_sol - 0.01 {
            return serde_json::json!({
                "error": "insufficient SOL",
                "balance_sol": balance_sol,
                "requested_sol": params.amount,
                "minimum_reserve": 0.01,
            })
            .to_string();
        }

        // Risk limit: max_position_pct of wallet balance
        let max_sol = balance_sol * self.config.risk.max_position_pct / 100.0;
        if side == "buy" && params.amount > max_sol {
            return serde_json::json!({
                "error": "position too large",
                "requested_sol": params.amount,
                "max_allowed_sol": max_sol,
                "max_position_pct": self.config.risk.max_position_pct,
                "hint": "reduce amount or raise max_position_pct in config",
            })
            .to_string();
        }

        // Fetch live market data for price context
        let market = crate::market::get_market(&params.token).await.ok().flatten();
        let price_usd = market.as_ref().map(|m| m.price_usd).unwrap_or(0.0);
        let mcap_usd = market.as_ref().map(|m| m.mcap_usd).unwrap_or(0.0);

        // Preview mode — quote but don't execute
        if !params.confirmed {
            let quote_result = if side == "buy" {
                let lamports = (params.amount * 1_000_000_000.0) as u64;
                execution::get_quote(
                    &self.http,
                    execution::SOL_MINT,
                    &params.token,
                    lamports,
                    self.config.risk.slippage_bps,
                )
                .await
            } else {
                let token_supply = self.rpc.get_token_supply(&params.token).await;
                let decimals = token_supply.as_ref().map(|s| s.decimals).unwrap_or(6);
                let base_units = (params.amount * 10_f64.powi(decimals as i32)) as u64;
                execution::get_quote(
                    &self.http,
                    &params.token,
                    execution::SOL_MINT,
                    base_units,
                    self.config.risk.slippage_bps,
                )
                .await
            };

            return match quote_result {
                Ok(q) => {
                    let out = q.out_amount_u64();
                    let estimated = if side == "buy" {
                        format!("{:.2} tokens", out as f64 / 1_000_000.0)
                    } else {
                        format!("{:.6} SOL", out as f64 / 1_000_000_000.0)
                    };
                    let unit = if side == "buy" { "SOL" } else { "tokens" };
                    to_json(&TradePreview {
                        action: side,
                        token: params.token,
                        amount_in: params.amount,
                        amount_in_unit: unit.to_string(),
                        estimated_output: estimated,
                        slippage_bps: self.config.risk.slippage_bps,
                        fees: format!(
                            "{} lamports priority + {} lamports Jito tip",
                            self.config.risk.priority_fee_lamports,
                            self.config.risk.jito_tip_lamports,
                        ),
                        confidence: analysis.map(|a| a.confidence.total).unwrap_or(0),
                        safety_checks: vec![
                            format!("balance: {:.4} SOL", balance_sol),
                            format!("price impact: {}%", q.price_impact_pct),
                            "pass confirmed=true to execute".to_string(),
                        ],
                        requires_confirmation: true,
                    })
                }
                Err(e) => serde_json::json!({"error": format!("quote failed: {}", e)}).to_string(),
            };
        }

        // Execution path — requires private key
        let keypair = match execution::load_keypair() {
            Ok(k) => k,
            Err(e) => {
                return serde_json::json!({
                    "error": "keypair not available",
                    "reason": e.to_string(),
                    "hint": "export PHOTON_PRIVATE_KEY=<base58 secret key>",
                })
                .to_string();
            }
        };

        // Verify the keypair matches the configured public key
        if !self.config.wallet.public_key.is_empty()
            && keypair.pubkey().to_string() != self.config.wallet.public_key
        {
            return serde_json::json!({
                "error": "keypair mismatch",
                "reason": "PHOTON_PRIVATE_KEY pubkey does not match wallet.public_key in config",
                "env_pubkey": keypair.pubkey().to_string(),
                "config_pubkey": self.config.wallet.public_key,
            })
            .to_string();
        }

        let result = if side == "buy" {
            execution::buy(
                &self.http,
                &self.rpc,
                &self.db,
                &keypair,
                &params.token,
                params.amount,
                self.config.risk.slippage_bps,
                self.config.risk.priority_fee_lamports,
                self.config.risk.jito_tip_lamports,
                price_usd,
                mcap_usd,
            )
            .await
        } else {
            let token_supply = self.rpc.get_token_supply(&params.token).await;
            let decimals = token_supply.as_ref().map(|s| s.decimals).unwrap_or(6);
            execution::sell(
                &self.http,
                &self.rpc,
                &self.db,
                &keypair,
                &params.token,
                params.amount,
                decimals,
                self.config.risk.slippage_bps,
                self.config.risk.priority_fee_lamports,
                self.config.risk.jito_tip_lamports,
                price_usd,
                mcap_usd,
            )
            .await
        };

        match result {
            Ok(r) => serde_json::to_string_pretty(&r)
                .unwrap_or_else(|e| format!("{{\"error\":\"{}\"}}", e)),
            Err(e) => serde_json::json!({"error": format!("trade failed: {}", e)}).to_string(),
        }
    }

    /// Portfolio status and system health.
    /// Flow: check all components -> query wallet balance live -> positions with live P&L -> exposure vs risk limits -> data freshness.
    #[tool]
    async fn status(&self) -> String {
        let _ = self
            .db
            .audit_log("claude", "status", "Status check requested");

        let wallet_key = &self.config.wallet.public_key;

        // Check RPC connectivity and get slot
        let rpc_connected = self.rpc.check_connection().await.unwrap_or(false);
        let current_slot = if rpc_connected {
            self.rpc.get_slot().await.ok()
        } else {
            None
        };

        // Get live wallet balance if configured and connected
        let balance_lamports = if !wallet_key.is_empty() && rpc_connected {
            self.rpc.get_balance(wallet_key).await.ok()
        } else {
            None
        };
        let balance_sol = balance_lamports.map(|l| l as f64 / 1_000_000_000.0);

        let data_freshness = if let Some(slot) = current_slot {
            format!("Live — slot {}", slot)
        } else if !rpc_connected {
            "No connection — check RPC endpoints and API keys".to_string()
        } else {
            "Connected but unable to read slot".to_string()
        };

        let message = if wallet_key.is_empty() {
            "No wallet configured. Set wallet.public_key in config.toml".to_string()
        } else if !rpc_connected {
            format!(
                "Wallet {} configured but RPC not connected — set API keys",
                wallet_key
            )
        } else if let Some(sol) = balance_sol {
            if sol < 0.01 {
                format!(
                    "Wallet {} has {:.4} SOL — fund this wallet to begin trading",
                    wallet_key, sol
                )
            } else {
                format!("Wallet {} — {:.4} SOL available", wallet_key, sol)
            }
        } else {
            format!("Wallet {} — unable to fetch balance", wallet_key)
        };

        let pending_alerts = self.db.pending_alert_count().unwrap_or(0);

        let message = if pending_alerts > 0 {
            format!(
                "{} — {} pending alerts, call scan to review",
                message, pending_alerts
            )
        } else {
            message
        };

        // Derive open positions and fetch live prices concurrently.
        let open = execution::open_positions(&self.db, wallet_key);
        let exposure_sol: f64 = open.iter().map(|p| p.sol_in).sum();
        let total_sol = balance_sol.unwrap_or(0.0);
        let exposure_pct = if total_sol > 0.0 {
            exposure_sol / total_sol * 100.0
        } else {
            0.0
        };

        // SOL price in USD: use the madapes fallback if configured, else 150 as
        // a conservative floor. This converts token USD price → SOL value.
        let sol_price_usd = self
            .config
            .madapes
            .as_ref()
            .map(|m| m.sol_price_fallback_usd)
            .unwrap_or(150.0);

        let positions: Vec<Position> = if open.is_empty() {
            vec![]
        } else {
            let mut handles = Vec::new();
            for pos in &open {
                let mint = pos.mint.clone();
                let sol_in = pos.sol_in;
                let tokens_held = pos.tokens_held;
                handles.push(tokio::spawn(async move {
                    let market = crate::market::get_market(&mint).await.ok().flatten();
                    // current_value_sol = tokens_held × price_usd / sol_price_usd
                    let current_value_sol = market
                        .as_ref()
                        .map(|m| m.price_usd)
                        .map(|p| tokens_held * p / sol_price_usd)
                        .unwrap_or(0.0);
                    let pnl_sol = if current_value_sol > 0.0 {
                        current_value_sol - sol_in
                    } else {
                        0.0
                    };
                    let pnl_pct = if sol_in > 0.0 && current_value_sol > 0.0 {
                        pnl_sol / sol_in * 100.0
                    } else {
                        0.0
                    };
                    Position {
                        token: mint,
                        amount_sol_in: sol_in,
                        current_value_sol,
                        pnl_sol,
                        pnl_pct,
                    }
                }));
            }
            let mut positions = Vec::with_capacity(handles.len());
            for h in handles {
                if let Ok(p) = h.await {
                    positions.push(p);
                }
            }
            positions
        };

        let total_pnl_sol: f64 = positions.iter().map(|p| p.pnl_sol).sum();

        to_json(&StatusResult {
            system_health: SystemHealth {
                rpc_connected,
                rpc_endpoints: self.rpc.endpoint_count(),
                rpc_healthy: self.rpc.healthy_count(),
                db_writable: self.is_healthy(),
                signal_layers_active: 3,
                current_slot,
                data_freshness,
            },
            pending_alerts,
            positions,
            wallet: wallet_key.clone(),
            total_balance_sol: total_sol,
            total_pnl_sol,
            exposure_pct,
            message,
        })
    }
}

#[tool_handler]
impl ServerHandler for PhotonServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::V_2025_03_26,
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            server_info: Implementation {
                name: "photon".to_string(),
                title: Some("Photon Signal Forecaster".to_string()),
                version: env!("CARGO_PKG_VERSION").to_string(),
                description: Some("Collaborative Solana trading intelligence".to_string()),
                icons: None,
                website_url: None,
            },
            instructions: Some(
                "Photon Signal Forecaster — collaborative Solana trading intelligence. \
                 Use scan() for market overview, inspect() for deep analysis, \
                 scout()/deep_scout() for raw token forensics, \
                 historical_analysis()/holder_forensics()/wallet_xray()/deep_signals() \
                 for higher-order intelligence, present() to render tokens as Telegram-ready HTML blocks \
                 (styles: monster/winner/ops/inspect), trade() to execute with \
                 guardrails, status() for portfolio health."
                    .to_string(),
            ),
        }
    }
}
