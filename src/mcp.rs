use crate::config::Config;
use crate::db::Db;
use crate::discovery;
use crate::forecaster::{Forecaster, Regime};
use crate::ingester::RpcRouter;
use crate::signals;
use rmcp::handler::server::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::*;
use rmcp::{tool, tool_handler, tool_router, ServerHandler};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone)]
pub struct PhotonServer {
    db: Arc<Db>,
    config: Config,
    rpc: Arc<RpcRouter>,
    resolved_endpoints: Vec<String>,
    forecaster: Forecaster,
    tool_router: ToolRouter<Self>,
}

// -- Parameter types --

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct InspectParams {
    /// Token mint address or wallet address to investigate
    pub address: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct TradeParams {
    /// Token mint address
    pub token: String,
    /// 'buy' or 'sell'
    pub side: String,
    /// Amount in SOL (for buys) or token amount (for sells)
    pub amount: f64,
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
    coverage: usize,
    recommended_position_pct: f64,
    reasoning: String,
}

#[derive(Debug, Serialize)]
struct AlertInfo {
    alert_type: String,
    message: String,
    confidence: i32,
}

#[derive(Debug, Serialize)]
struct InspectResult {
    target: String,
    target_type: String,
    safety: Vec<SignalDetail>,
    signals: Vec<SignalDetail>,
    risk_rating: String,
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
    amount_sol: f64,
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

fn to_json<T: Serialize>(data: &T) -> String {
    serde_json::to_string_pretty(data).unwrap_or_else(|e| format!("Error: {e}"))
}

impl PhotonServer {
    pub fn new(db: Arc<Db>, config: Config, rpc: Arc<RpcRouter>, resolved_endpoints: Vec<String>) -> Self {
        Self {
            db,
            config,
            rpc,
            resolved_endpoints,
            forecaster: Forecaster::new(),
            tool_router: Self::tool_router(),
        }
    }

    pub fn is_healthy(&self) -> bool {
        self.db.list_tables().is_ok()
    }
}

#[tool_router]
impl PhotonServer {
    /// Scan the market: discover new tokens from Pump.fun, analyze them, rank by confidence.
    /// Flow: check health -> discover tokens -> run signal analysis -> rank -> present.
    #[tool]
    async fn scan(&self) -> String {
        let _ = self.db.audit_log("claude", "scan", "Market scan requested");

        let rpc_connected = self.rpc.check_connection().await.unwrap_or(false);

        if !rpc_connected {
            return to_json(&ScanResult {
                healthy: false,
                rpc_connected: false,
                rpc_endpoints: self.rpc.endpoint_count(),
                rpc_healthy: self.rpc.healthy_count(),
                regime: "Unknown".to_string(),
                opportunities: vec![],
                alerts: vec![AlertInfo {
                    alert_type: "system".to_string(),
                    message: "RPC not connected — check API keys and endpoints".to_string(),
                    confidence: 100,
                }],
            });
        }

        // Discover and analyze new tokens (limit to 5 to stay within rate limits)
        let helius_url = self.resolved_endpoints.first().cloned().unwrap_or_default();
        let analyses = discovery::discover_new_tokens(&helius_url, &self.db, &self.rpc, 5)
            .await
            .unwrap_or_default();

        let opportunities: Vec<Opportunity> = analyses
            .iter()
            .map(|a| {
                let position_pct = self.forecaster.position_pct(
                    a.confidence.total,
                    a.confidence.coverage,
                );
                Opportunity {
                    token: a.address.clone(),
                    confidence: a.confidence.total,
                    coverage: a.confidence.coverage,
                    recommended_position_pct: position_pct,
                    reasoning: a.confidence.reasoning.clone(),
                }
            })
            .collect();

        let mut alerts = Vec::new();
        // Alert on any high-confidence opportunities
        for opp in &opportunities {
            if opp.confidence >= self.config.alerts.confidence_threshold as i32 {
                alerts.push(AlertInfo {
                    alert_type: "opportunity".to_string(),
                    message: format!(
                        "Token {} scored {}/100 — recommended {}% position",
                        opp.token, opp.confidence, opp.recommended_position_pct
                    ),
                    confidence: opp.confidence,
                });
            }
        }

        to_json(&ScanResult {
            healthy: true,
            rpc_connected: true,
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
        let _ = self
            .db
            .audit_log("claude", "inspect", &format!("Inspecting {}", params.address));

        // Run full token analysis through all signal layers
        match signals::analyze_token(&self.rpc, &params.address).await {
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

                let risk_rating = if analysis.confidence.total >= 70 {
                    format!(
                        "LOW RISK — confidence {}/100 ({} layers, {} signals)",
                        analysis.confidence.total,
                        analysis.confidence.coverage,
                        analysis.scores.len()
                    )
                } else if analysis.confidence.total >= 40 {
                    format!(
                        "MEDIUM RISK — confidence {}/100 ({} layers, {} signals)",
                        analysis.confidence.total,
                        analysis.confidence.coverage,
                        analysis.scores.len()
                    )
                } else {
                    format!(
                        "HIGH RISK — confidence {}/100 ({} layers, {} signals)",
                        analysis.confidence.total,
                        analysis.confidence.coverage,
                        analysis.scores.len()
                    )
                };

                // Store in DB for historical tracking
                let _ = self.db.insert_token(
                    &params.address,
                    analysis.confidence.total,
                );

                to_json(&InspectResult {
                    target: params.address,
                    target_type: "token".to_string(),
                    safety: safety_scores,
                    signals: other_scores,
                    risk_rating,
                })
            }
            Err(e) => to_json(&InspectResult {
                target: params.address,
                target_type: "unknown".to_string(),
                safety: vec![],
                signals: vec![SignalDetail {
                    layer: "System".to_string(),
                    signal_type: "error".to_string(),
                    score: 0,
                    details: format!("Analysis failed: {}", e),
                }],
                risk_rating: format!("UNKNOWN — analysis error: {}", e),
            }),
        }
    }

    /// Execute a trade with full guardrails.
    /// Flow: safety checks -> balance check -> simulate -> preview -> WAIT FOR CONFIRMATION -> sign -> submit via Jito -> verify -> record.
    #[tool]
    fn trade(&self, Parameters(params): Parameters<TradeParams>) -> String {
        let _ = self.db.audit_log(
            "claude",
            "trade_preview",
            &format!("{} {} on {}", params.side, params.amount, params.token),
        );

        to_json(&TradePreview {
            action: params.side,
            token: params.token,
            amount_sol: params.amount,
            estimated_output: "Not yet connected to Jupiter - connect RPC first".to_string(),
            slippage_bps: self.config.risk.slippage_bps,
            fees: "1% platform fee + priority fee + Jito tip".to_string(),
            confidence: 0,
            safety_checks: vec![
                "BLOCKED: System not yet connected to Solana RPC".to_string(),
                "BLOCKED: Wallet not funded".to_string(),
            ],
            requires_confirmation: true,
        })
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

        to_json(&StatusResult {
            system_health: SystemHealth {
                rpc_connected,
                rpc_endpoints: self.rpc.endpoint_count(),
                rpc_healthy: self.rpc.healthy_count(),
                db_writable: self.is_healthy(),
                signal_layers_active: 0,
                current_slot,
                data_freshness,
            },
            positions: vec![],
            wallet: wallet_key.clone(),
            total_balance_sol: balance_sol.unwrap_or(0.0),
            total_pnl_sol: 0.0,
            exposure_pct: 0.0,
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
                 trade() to execute with guardrails, status() for portfolio health."
                    .to_string(),
            ),
        }
    }
}
