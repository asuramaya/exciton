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
pub struct ExcitonServer {
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
pub struct BackfillCardsParams {
    /// Trailing window in hours. Default 720 (30 days). Caps at 8760
    /// (1 year). Each card costs ~5-30s of zeroclaw time so big windows
    /// run as a long-tail background task.
    #[serde(default)]
    pub hours: Option<i64>,
    /// Idempotency: skip cards whose Telegram delivery was edited within
    /// this many minutes — those were almost certainly just re-rendered
    /// by a recent backfill or settle close. Default 60. Set 0 to force
    /// re-render of every card in the window.
    #[serde(default)]
    pub skip_if_edited_within_minutes: Option<i64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct CleanupLoungeAnchorParams {
    /// Comma-separated list of message_ids in the lounge channel to
    /// delete. Idempotent — already-missing messages are treated as a
    /// no-op success. Use this to clear stale Safeguard verify forwards
    /// left in the lounge from before the anchor moved to calls.
    pub msg_ids: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct RefreshCardParams {
    /// Comma-separated list of mints to refresh. For each: if the call is
    /// active, re-renders the open card via claw_entry_line + fresh chart
    /// screenshot via editMessageMedia. If closed, replays through
    /// force_update_card (same path as backfill_cards). Use this to upgrade
    /// recently-posted cards to the new chart-screenshot + claw-entry path
    /// without waiting for natural close.
    pub mints: String,
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
pub struct DbCallLookupParams {
    /// Symbol (case-insensitive substring) or full mint address.
    pub query: String,
    /// Max rows to return. Default 10, cap 50.
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct DbTokenHistoryParams {
    /// Mint address. Required — symbols won't match here, this hits
    /// token_snapshots.token_address directly.
    pub mint: String,
    /// Max snapshots to return, newest first. Default 30, cap 200.
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct DbNearMissesParams {
    /// Max rows to return, newest first. Default 30, cap 200.
    #[serde(default)]
    pub limit: Option<usize>,
    /// Optional mint to scope the search to a single token.
    #[serde(default)]
    pub mint: Option<String>,
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

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ProposeTuneParams {
    /// Field to override. Must be in the tunable allow-list — see
    /// `tunable_fields` doc on the server. Examples:
    /// "min_effective_confidence", "max_top_holder_pct",
    /// "min_liquidity_usd", "min_volume_24h_usd", "min_token_age_secs".
    pub field: String,
    /// Scope of the override. "global" or "class:STAIRCASE" /
    /// "class:GRINDER" / "class:SPRING". Per-class scope only valid
    /// for fields that support it.
    pub scope: String,
    /// Current value (stringified). For numeric fields, the agent passes
    /// the integer or float as a string. The server validates type.
    pub old_value: String,
    /// Proposed new value (stringified). Must parse to the same type as
    /// old_value. Validators check effect-size and holdout against the
    /// evidence the agent supplies.
    pub new_value: String,
    /// Required: evidence_json must contain `current` and `proposed`
    /// objects each with `n` and `mean_pnl_pct` (and ideally
    /// `win_rate_pct`), plus a `holdout` object with `n` and
    /// `mean_pnl_pct`. Validators reject anything missing these.
    pub evidence_json: String,
    /// Trader-voice 3-4 sentence narrative explaining what changed and
    /// why. Goes into the diary entry verbatim. Must be non-empty and
    /// not a placeholder.
    pub narrative: String,
    /// Identifier of the proposing agent — "claw" by default; operator
    /// proposals (manual MCP calls from a human) should pass "operator".
    #[serde(default)]
    pub proposed_by: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct CommitTuneParams {
    /// Proposal id returned by propose_tune. Must be in `pending` status.
    pub proposal_id: i64,
    /// Agent-authored markdown for the public diary entry. The renderer
    /// validates structure (non-empty, must include the narrative + an
    /// evidence section) but does NOT enforce a fixed template — voice
    /// is whatever the agent's prompt produces. Length bounded to
    /// 200..=4000 characters.
    pub body_md: String,
    /// Optional one-line headline for the diary entry. When omitted,
    /// `record_evolution` derives one from field/old/new.
    #[serde(default)]
    pub summary: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ListTunesParams {
    /// Filter by status: "pending" | "committed" | "rejected" | "reverted".
    /// None = all.
    #[serde(default)]
    pub status: Option<String>,
    /// Cap on rows returned. Default 50, max 500.
    #[serde(default)]
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct RevertTuneParams {
    /// Proposal id to revert. Must be in `committed` status. Deletes the
    /// matching signal_overrides row (so should_signal falls back to
    /// the compile-time default) and marks the proposal `reverted`.
    pub proposal_id: i64,
    /// Why the revert is happening. Goes into the audit log. Required.
    pub reason: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ProposePromptParams {
    /// Full new system prompt content (markdown). Length 200..=20000.
    /// Replaces the current prompt verbatim — this is not a diff.
    pub content: String,
    /// Why the prompt should change. 3-4 sentence trader-voice
    /// explanation citing what didn't work or what the agent learned.
    pub why: String,
    /// "claw" (default) | "operator". Operator-authored prompts skip
    /// the propose/commit dance and go straight to commit.
    #[serde(default)]
    pub proposed_by: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct CommitPromptParams {
    /// Proposal id from `propose_prompt`. Must be `pending`.
    pub proposal_id: i64,
    /// Agent-authored markdown for the diary entry that will accompany
    /// this prompt change. Same body_md rules as commit_tune
    /// (200..=4000 chars, must reference the change).
    pub body_md: String,
    /// Optional one-line headline. Auto-derived if omitted.
    #[serde(default)]
    pub summary: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ListPromptsParams {
    /// Cap on revisions returned. Default 20, max 200.
    #[serde(default)]
    pub limit: Option<i64>,
    /// When true, includes the full markdown of each prompt revision.
    /// Default false to keep payloads compact — call with
    /// `include_content=true` when the agent actually needs to see
    /// what its previous voice looked like.
    #[serde(default)]
    pub include_content: bool,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct CommitSiteChangeParams {
    /// One-line headline of the site change (e.g. "added per-classification
    /// win rate page"). Becomes the evolution event summary.
    pub summary: String,
    /// Agent-authored markdown for the diary entry. Same length rules
    /// as commit_tune.
    pub body_md: String,
    /// Optional structured payload describing what changed (paths,
    /// before/after pointers, etc.) — stored verbatim in
    /// evolution_events.evidence_json.
    #[serde(default)]
    pub evidence_json: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct AnalyzeOutcomesParams {
    /// Optional classification filter (e.g. "STAIRCASE", "GRINDER", "SPRING").
    /// None = all classifications.
    #[serde(default)]
    pub classification: Option<String>,
    /// Optional horizon filter (SHORT | LONG | MOONSHOT | SCALP).
    /// None = all horizons.
    #[serde(default)]
    pub horizon: Option<String>,
    /// Earliest called_at to include (epoch seconds). None = unlimited
    /// history. The agent typically passes `now - 30d`.
    #[serde(default)]
    pub since: Option<i64>,
    /// When true, the response includes the raw call list (capped by `limit`)
    /// in addition to the bucketed aggregates. Useful when the agent wants
    /// to cite specific calls in its narrative. Default false to keep the
    /// payload compact.
    #[serde(default)]
    pub include_raw: bool,
    /// Cap on raw rows returned when `include_raw` is true. Default 50,
    /// max 500.
    #[serde(default)]
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SweepThresholdParams {
    /// Tunable field to sweep. Currently supported by sweep:
    /// "min_effective_confidence", "max_top_holder_pct",
    /// "min_liquidity_usd". The other tunables
    /// (min_volume_24h_usd, min_token_age_secs) are gated pre-call so
    /// historical sweep is meaningless — propose those manually with
    /// dedicated evidence.
    pub field: String,
    /// Scope: "global" or "class:STAIRCASE" / "class:GRINDER" /
    /// "class:SPRING" / "class:DEVELOPING". The sweep filters the
    /// closed-call universe by this scope before applying candidates.
    pub scope: String,
    /// Candidate values (stringified) to test. Each becomes one row in
    /// the response. Skipped silently if it doesn't parse for the
    /// field's type.
    pub candidates: Vec<String>,
    /// Earliest called_at to include (epoch seconds). None = unlimited
    /// history. Default: trailing 30 days.
    #[serde(default)]
    pub since: Option<i64>,
    /// Holdout split as percent of universe (newest by called_at).
    /// Default 25. Range: 10..=50.
    #[serde(default)]
    pub holdout_pct: Option<i64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct RankTuneCandidatesParams {
    /// Restrict to specific scopes (e.g. ["class:GRINDER", "global"]).
    /// Default: all scopes the field supports.
    #[serde(default)]
    pub scopes: Option<Vec<String>>,
    /// Restrict to specific fields. Default: every field that
    /// sweep_threshold supports today.
    #[serde(default)]
    pub fields: Option<Vec<String>>,
    /// Number of top candidates to return, ranked by effect × √n.
    /// Default 5, max 20.
    #[serde(default)]
    pub top_k: Option<i64>,
    /// Earliest called_at to include. Default: trailing 30 days.
    #[serde(default)]
    pub since: Option<i64>,
    /// Holdout split percent. Default 25.
    #[serde(default)]
    pub holdout_pct: Option<i64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct AnalyzeDriftParams {
    /// Scope of the analysis. "global" or "class:STAIRCASE" /
    /// "class:GRINDER" / "class:SPRING" / "class:DEVELOPING".
    pub scope: String,
    /// Number of time windows to slice the universe into. Default 4.
    /// Range 2..=12.
    #[serde(default)]
    pub window_count: Option<i64>,
    /// Total trailing span in seconds. Default: 30 days.
    #[serde(default)]
    pub window_secs: Option<i64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct AnalyzeFailureModesParams {
    /// Scope filter — same shape as analyze_drift.
    pub scope: String,
    /// Earliest called_at to include. Default: now - 30d.
    #[serde(default)]
    pub since: Option<i64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ListEvolutionEventsParams {
    /// Optional kind filter: "strategy" | "tool" | "site". None = all.
    #[serde(default)]
    pub kind: Option<String>,
    /// Cap on rows. Default 20, max 200.
    #[serde(default)]
    pub limit: Option<i64>,
    /// When true, includes the full body_md. Default false to keep
    /// payloads compact.
    #[serde(default)]
    pub include_body: bool,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ComparePeriodsParams {
    /// Scope filter.
    pub scope: String,
    /// Pivot timestamp (epoch seconds) — typically a prior tune's
    /// committed_at. before_secs of universe up to this point becomes
    /// the "before" window; after_secs from this point becomes the
    /// "after" window. Defaults to now (use this with after_secs=0
    /// to ask "what did the trailing window look like").
    #[serde(default)]
    pub anchor_at: Option<i64>,
    /// "before" window length in seconds. Default 30 days.
    #[serde(default)]
    pub before_secs: Option<i64>,
    /// "after" window length in seconds. Default = (now - anchor_at).
    #[serde(default)]
    pub after_secs: Option<i64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct DescribeClassificationsParams {}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SimulateOverridesParams {
    /// List of overrides to apply on top of current state. Each entry
    /// must be a (field, scope, new_value) triple. Stacks atop already-
    /// committed overrides; if a (field, scope) is in this list and
    /// also a real override, the simulation uses this list's value.
    pub overrides: Vec<SimulateOverrideEntry>,
    /// Earliest called_at to include. Default: trailing 30d.
    #[serde(default)]
    pub since: Option<i64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SimulateOverrideEntry {
    pub field: String,
    pub scope: String,
    pub new_value: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ReviewLogParams {
    /// How many recent cycles to return. Default 10, max 200.
    #[serde(default)]
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ReviewLogWriteParams {
    /// Epoch seconds when the cycle started.
    pub started_at: i64,
    /// "propose" | "commit".
    pub mode: String,
    /// "proposed" | "committed" | "stopped" | "failed".
    pub outcome: String,
    /// One-paragraph summary of what the cycle did and why. Becomes
    /// the agent's note-to-future-self.
    pub summary: String,
    /// Optional proposal id this cycle produced.
    #[serde(default)]
    pub proposal_id: Option<i64>,
    /// Optional turn count (claw fills this in).
    #[serde(default)]
    pub turns: Option<i64>,
    /// Optional tool-call count (claw fills this in).
    #[serde(default)]
    pub tool_calls: Option<i64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct DescribeSignalFiltersParams {}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ListOverridesParams {}

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
    #[serde(skip_serializing_if = "Option::is_none")]
    forensics: Option<ForensicsInfo>,
}

#[derive(Debug, Serialize)]
struct ForensicsInfo {
    bundle_pct: f64,
    sniper_pct: f64,
    insider_pct: f64,
    smart_money_count: i32,
    /// 0 when forensics never measured for this token. Newer tokens may
    /// show 0 until the async refresh completes (~next analysis cycle).
    computed_at: i64,
    /// Trailing 1h tape — surfaces alongside forensics for narrative context.
    buys_h1: i32,
    sells_h1: i32,
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

impl ExcitonServer {
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
impl ExcitonServer {
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

                let forensics = Some(ForensicsInfo {
                    bundle_pct: analysis.bundle_pct,
                    sniper_pct: analysis.sniper_pct,
                    insider_pct: analysis.insider_pct,
                    smart_money_count: analysis.smart_money_count,
                    computed_at: analysis.forensics_computed_at,
                    buys_h1: analysis.buys_h1,
                    sells_h1: analysis.sells_h1,
                });
                to_json(&InspectResult {
                    target: params.address,
                    target_type: "token".to_string(),
                    classification: analysis.confidence.classification.clone(),
                    safety: safety_scores,
                    signals: other_scores,
                    risk_rating,
                    delta: delta_info,
                    forensics,
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
                forensics: None,
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
        // -1 = disabled (no recraft key). Front-end pipeline_health
        // reports "stale" for any positive number above the threshold;
        // -1 means we shouldn't flag it stale at all (the processor
        // intentionally isn't running).
        let assets_age_seconds: i64 = match self.config.madapes.as_ref() {
            Some(mp) if !mp.recraft_api_key.is_empty() => {
                let p = format!("{}/thoughts/assets.json", mp.repo_path);
                std::fs::metadata(&p)
                    .and_then(|m| m.modified())
                    .ok()
                    .and_then(|t| t.elapsed().ok())
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(-1)
            }
            _ => -1,
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
                "endpoints": self.rpc.endpoint_stats(),
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

    /// Append a new note to the publisher repo's `thoughts/` folder, update the index, commit
    /// with the `note:` prefix and push. Respects append-only — fails with
    /// an error if the target filename already exists. The image processor
    /// picks up any `<div class="img-placeholder">[IMAGE: ...]</div>` blocks
    /// on its next 15-minute tick (no action needed here).
    #[tool]
    async fn post_note(&self, Parameters(params): Parameters<PostNoteParams>) -> String {
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

        // git add + commit + push with the `note:` prefix. Each git op
        // capped at 60s via tokio::process + timeout — same pattern as
        // publisher::run_git, prevents hung pushes from blocking the
        // MCP tool indefinitely (and stealing a tokio worker thread).
        let repo = &mp.repo_path;
        let msg = format!("note: {}", params.title);
        let _ = run_git_with_timeout(&["-C", repo, "add", "thoughts/"]).await;
        let commit = run_git_with_timeout(&["-C", repo, "commit", "-m", &msg]).await;
        let committed = commit.as_ref().map(|o| o.status.success()).unwrap_or(false);
        if committed {
            let _ = run_git_with_timeout(&["-C", repo, "push", "--quiet"]).await;
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

    /// Re-render closed Telegram call cards in the current ape format.
    /// Walks every closed call (withdrew/failed/expired/voided) within
    /// the trailing window and triggers force_update_card on each — the
    /// claws-channel card gets the new ape verdict + flat band + chart
    /// preview, the lounge mirror gets the heavy data card.
    ///
    /// Sequential with 1.5s spacing between cards to stay under
    /// Telegram's 30/min channel-edit cap and avoid hammering zeroclaw
    /// with concurrent verdict requests. Default window 24h.
    #[tool]
    async fn backfill_cards(
        &self,
        Parameters(params): Parameters<BackfillCardsParams>,
    ) -> String {
        let hours = params.hours.unwrap_or(720).clamp(1, 8760);
        let skip_minutes = params.skip_if_edited_within_minutes.unwrap_or(60).max(0);
        let _ = self.db.audit_log(
            "claude",
            "backfill_cards",
            &format!("{}h skip={}m", hours, skip_minutes),
        );

        let notifier = match self.notifier.as_ref() {
            Some(n) => n.clone(),
            None => {
                return serde_json::json!({"error": "notifier not configured"}).to_string()
            }
        };
        let now_ts = chrono::Utc::now().timestamp();
        let since = now_ts - hours * 3600;
        let skip_cutoff = now_ts - skip_minutes * 60;
        // Pull a wide call set since long windows need it. list_calls
        // returns newest-first; the time filter handles the trailing edge.
        let candidates: Vec<crate::db::CallRow> = self
            .db
            .list_calls(false, 2000)
            .unwrap_or_default()
            .into_iter()
            .filter(|c| {
                matches!(
                    c.status.as_str(),
                    "withdrew" | "failed" | "expired" | "closed"
                ) && c.closed_at.unwrap_or(0) >= since
            })
            .collect();

        // Idempotency: skip cards whose calls-channel delivery was
        // edited within `skip_minutes` of now — almost certainly already
        // re-rendered by a recent backfill / normal close path. Avoids
        // burning claw quota on cards that already have a fresh voice.
        let db = self.db.clone();
        let (closed, recently_voiced): (Vec<_>, usize) = {
            let mut keep = Vec::new();
            let mut recent = 0usize;
            for c in candidates {
                let recently_edited = db
                    .get_active_delivery(&c.mint, "winners")
                    .ok()
                    .flatten()
                    .and_then(|d| d.last_edit_at)
                    .map(|t| t >= skip_cutoff)
                    .unwrap_or(false);
                if recently_edited {
                    recent += 1;
                    continue;
                }
                keep.push(c);
            }
            (keep, recent)
        };
        let total = closed.len();

        // Run the backfill in a detached task so the MCP call returns
        // promptly. Each card is up to ~150s with the new 30s × 5
        // retries; long windows can run for many minutes.
        tokio::spawn(async move {
            let mut ok = 0usize;
            let mut skipped = 0usize;
            for c in closed {
                let exit_pct = if c.entry_price_usd > 0.0
                    && c.exit_price_usd.unwrap_or(0.0) > 0.0
                {
                    Some(
                        (c.exit_price_usd.unwrap() - c.entry_price_usd) / c.entry_price_usd
                            * 100.0,
                    )
                } else {
                    None
                };
                let exit_note = c.exit_note.clone().unwrap_or_default();
                match notifier
                    .force_update_card(&c.mint, &c.status, exit_pct, &exit_note)
                    .await
                {
                    Ok(_) => ok += 1,
                    Err(e) => {
                        skipped += 1;
                        tracing::warn!("backfill {}: {}", c.mint, e);
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
            }
            tracing::info!("backfill_cards done: {} ok, {} skipped", ok, skipped);
        });

        serde_json::json!({
            "queued": total,
            "skipped_recently_voiced": recently_voiced,
            "window_hours": hours,
            "cadence_secs_per_card": 1.5,
            "note": "running in background — claw verdict + ape format applied per card",
        })
        .to_string()
    }

    /// Delete one or more stale forwards from the lounge channel. Used
    /// to clean up Safeguard verify forwards left over from before the
    /// anchor was moved to the calls channel. Idempotent.
    #[tool]
    async fn cleanup_lounge_anchor(
        &self,
        Parameters(params): Parameters<CleanupLoungeAnchorParams>,
    ) -> String {
        let _ = self.db.audit_log("claude", "cleanup_lounge_anchor", &params.msg_ids);
        let notifier = match self.notifier.as_ref() {
            Some(n) => n.clone(),
            None => return serde_json::json!({"error": "notifier not configured"}).to_string(),
        };
        let ids: Vec<i64> = params
            .msg_ids
            .split(',')
            .filter_map(|s| s.trim().parse::<i64>().ok())
            .filter(|n| *n > 0)
            .collect();
        if ids.is_empty() {
            return serde_json::json!({"error": "no valid msg_ids"}).to_string();
        }
        let mut results = Vec::new();
        for msg in ids {
            match notifier.delete_lounge_message(msg).await {
                Ok(_) => results.push(serde_json::json!({"msg_id": msg, "ok": true})),
                Err(e) => {
                    let s = format!("{}", e);
                    let already_gone = s.contains("not found") || s.contains("can't be deleted");
                    results.push(serde_json::json!({
                        "msg_id": msg,
                        "ok": already_gone,
                        "note": if already_gone { "already gone" } else { "delete failed" },
                        "error": s,
                    }));
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
        serde_json::json!({"results": results}).to_string()
    }

    /// Refresh specific cards by mint. Active calls re-render their open
    /// card (new claw entry line + fresh chart screenshot via
    /// editMessageMedia). Closed calls go through force_update_card (same
    /// as backfill_cards). Use this to upgrade a handful of recent cards
    /// to the latest renderer without scanning the full history window.
    #[tool]
    async fn refresh_card(&self, Parameters(params): Parameters<RefreshCardParams>) -> String {
        let _ = self.db.audit_log("claude", "refresh_card", &params.mints);
        let notifier = match self.notifier.as_ref() {
            Some(n) => n.clone(),
            None => return serde_json::json!({"error": "notifier not configured"}).to_string(),
        };
        let mints: Vec<String> = params
            .mints
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if mints.is_empty() {
            return serde_json::json!({"error": "no mints provided"}).to_string();
        }
        let mut results = Vec::new();
        for mint in mints {
            let call = self.db.get_call_by_mint(&mint).ok().flatten();
            let status = call.as_ref().map(|c| c.status.clone()).unwrap_or_default();
            let res = if status == "active" {
                notifier.refresh_active_card(&mint).await
            } else {
                let exit_pct = call.as_ref().and_then(|c| {
                    if c.entry_price_usd > 0.0 && c.exit_price_usd.unwrap_or(0.0) > 0.0 {
                        Some((c.exit_price_usd.unwrap() - c.entry_price_usd) / c.entry_price_usd * 100.0)
                    } else {
                        None
                    }
                });
                let exit_note = call.as_ref().and_then(|c| c.exit_note.clone()).unwrap_or_default();
                notifier.force_update_card(&mint, &status, exit_pct, &exit_note).await
            };
            match res {
                Ok(_) => results.push(serde_json::json!({"mint": mint, "status": status, "ok": true})),
                Err(e) => results.push(serde_json::json!({"mint": mint, "status": status, "ok": false, "error": format!("{}", e)})),
            }
            tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
        }
        serde_json::json!({"results": results}).to_string()
    }

    /// Bump the lounge anchor — delete the previous copy of the
    /// configured "always at the bottom" message and copyMessage from
    /// the source again as a fresh post. Photon does this automatically
    /// after every exciton-originated lounge send; this tool lets
    /// zeroclaw or the operator trigger it manually after posting to
    /// the lounge through some path exciton doesn't observe.
    #[tool]
    async fn bump_lounge_anchor(&self) -> String {
        let _ = self.db.audit_log("claude", "bump_lounge_anchor", "");
        let notifier = match self.notifier.as_ref() {
            Some(n) => n.clone(),
            None => {
                return serde_json::json!({"error": "notifier not configured"}).to_string()
            }
        };
        notifier.bump_lounge_anchor().await;
        serde_json::json!({
            "current_msg_id": self.db.get_lounge_anchor_msg_id().unwrap_or(0),
        })
        .to_string()
    }

    /// List currently active calls. Use before firing a new one or when
    /// deciding which calls to close. Returns the same rows visible on the
    /// public site.
    #[tool]
    async fn active_calls(&self) -> String {
        let rows = self.db.list_calls(true, 100).unwrap_or_default();
        serde_json::to_string_pretty(&rows).unwrap_or_else(|e| format!("{{\"error\":\"{}\"}}", e))
    }

    /// Look up calls by symbol (case-insensitive substring) or exact mint.
    /// Returns full call rows — entry/exit fields, classification,
    /// confidence, exit_note. Use this for post-mortem ("why did BUTT
    /// exit at +4.7%?") instead of grepping the DB by hand.
    #[tool]
    async fn db_call_lookup(&self, Parameters(params): Parameters<DbCallLookupParams>) -> String {
        let limit = params.limit.unwrap_or(10).min(50);
        match self.db.find_calls(&params.query, limit) {
            Ok(rows) => serde_json::to_string_pretty(&rows)
                .unwrap_or_else(|e| format!("{{\"error\":\"{}\"}}", e)),
            Err(e) => serde_json::json!({"error": format!("{}", e)}).to_string(),
        }
    }

    /// Snapshot history for a single mint, newest first. Each row is one
    /// analyzer cycle: classification, confidence, momentum/distribution/
    /// spring, top-holder %, holder count, tx_rate, price, mcap, liquidity,
    /// buys/sells, price_change_h1, plus launch-forensics fields. Use to
    /// reconstruct what the bot saw during a run — i.e. peak price vs exit
    /// price for a fakeout investigation.
    #[tool]
    async fn db_token_history(
        &self,
        Parameters(params): Parameters<DbTokenHistoryParams>,
    ) -> String {
        let limit = params.limit.unwrap_or(30).min(200);
        match self.db.get_snapshot_history(&params.mint, limit) {
            Ok(snaps) => serde_json::to_string_pretty(&snaps)
                .unwrap_or_else(|e| format!("{{\"error\":\"{}\"}}", e)),
            Err(e) => serde_json::json!({"error": format!("{}", e)}).to_string(),
        }
    }

    /// Recent signal_near_misses — cycles where the analyzer almost fired
    /// but a gate blocked. Use to figure out *why* a runner went uncalled
    /// (was it sniper%, liquidity, momentum_delta?). Pass `mint` to scope
    /// to a single token; omit for cross-token triage.
    #[tool]
    async fn db_recent_near_misses(
        &self,
        Parameters(params): Parameters<DbNearMissesParams>,
    ) -> String {
        let limit = params.limit.unwrap_or(30).min(200);
        let result = match params.mint.as_deref() {
            Some(m) if !m.is_empty() => self.db.get_token_near_misses(m, limit),
            _ => self.db.get_recent_near_misses(limit),
        };
        match result {
            Ok(rows) => {
                let pretty: Vec<_> = rows
                    .iter()
                    .map(|r| {
                        serde_json::json!({
                            "mint": r.token_address,
                            "classification": r.classification,
                            "effective_confidence": r.effective_confidence,
                            "top_holder_pct": r.top_holder_pct,
                            "momentum_delta": r.momentum_delta,
                            "gate": r.gate_that_failed,
                            "gap": r.gap,
                            "timestamp": r.timestamp,
                        })
                    })
                    .collect();
                serde_json::to_string_pretty(&pretty)
                    .unwrap_or_else(|e| format!("{{\"error\":\"{}\"}}", e))
            }
            Err(e) => serde_json::json!({"error": format!("{}", e)}).to_string(),
        }
    }

    /// Execute a trade via Jupiter v6 + Jito MEV protection.
    ///
    /// Set confirmed=false (or omit) to preview without executing.
    /// Set confirmed=true to actually sign and submit the transaction.
    ///
    /// For buys: amount = SOL to spend.
    /// For sells: amount = token UI amount to sell (e.g. 50000.0 tokens).
    ///
    /// Requires EXCITON_PRIVATE_KEY env var (base58 secret key).
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
                    "hint": "export EXCITON_PRIVATE_KEY=<base58 secret key>",
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
                "reason": "EXCITON_PRIVATE_KEY pubkey does not match wallet.public_key in config",
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

    /// Read-only outcome analyzer. Pulls closed calls (withdrew / failed /
    /// expired / closed), groups by (classification, horizon), returns
    /// per-bucket aggregates: count, win-rate, PnL distribution, hold time,
    /// verdict breakdown. The agent's primary input when proposing a
    /// strategy tune.
    ///
    /// Output structure:
    /// ```text
    /// {
    ///   "since": <i64>,
    ///   "raw_count": <usize>,
    ///   "aggregates": [
    ///     { "classification": "STAIRCASE", "horizon": "SHORT",
    ///       "n": 14, "win_rate_pct": 42.8, "mean_pnl_pct": 3.2,
    ///       "median_pnl_pct": -10.5, "p25_pnl_pct": -28.1,
    ///       "p75_pnl_pct": 12.4, "mean_hold_secs": 4231,
    ///       "verdicts": { "withdrew": 6, "failed": 5, "expired": 3 } },
    ///     ...
    ///   ],
    ///   "outcomes": [...]   // present only when include_raw = true
    /// }
    /// ```
    ///
    /// Win rate is `withdrew / n`. PnL stats ignore rows missing exit_price.
    /// Buckets with n < 1 are omitted.
    #[tool]
    async fn analyze_outcomes(
        &self,
        Parameters(params): Parameters<AnalyzeOutcomesParams>,
    ) -> String {
        // Treat empty strings as "omit this filter" — the agent often
        // emits "" instead of leaving the field out entirely.
        let class = params
            .classification
            .as_deref()
            .filter(|s| !s.trim().is_empty());
        let horizon = params
            .horizon
            .as_deref()
            .filter(|s| !s.trim().is_empty());
        let since = params.since;
        let limit = params.limit.unwrap_or(50).clamp(1, 500);
        // Pull a generous slice — bucketing happens here; cap at 5000 to
        // avoid degenerate full-table scans on giant DBs.
        let outcomes = match self
            .db
            .list_closed_call_outcomes(class, horizon, since, 5000)
        {
            Ok(v) => v,
            Err(e) => {
                return serde_json::json!({"error": format!("query failed: {e}")}).to_string()
            }
        };

        let _ = self.db.audit_log(
            "claude",
            "analyze_outcomes",
            &format!(
                "class={} horizon={} since={} n={}",
                class.unwrap_or("*"),
                horizon.unwrap_or("*"),
                since.map(|s| s.to_string()).unwrap_or_else(|| "*".into()),
                outcomes.len()
            ),
        );

        // Group by (classification, horizon) into ordered buckets. We use a
        // BTreeMap so the output is deterministic across calls.
        use std::collections::BTreeMap;
        let mut buckets: BTreeMap<(String, String), Vec<&crate::db::CallOutcome>> = BTreeMap::new();
        for o in &outcomes {
            buckets
                .entry((o.classification.clone(), o.horizon.clone()))
                .or_default()
                .push(o);
        }

        let aggregates: Vec<serde_json::Value> = buckets
            .into_iter()
            .map(|((classification, horizon), rows)| {
                let n = rows.len() as i64;
                let mut pnls: Vec<f64> = rows.iter().filter_map(|r| r.pnl_pct).collect();
                pnls.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let mean_pnl = if pnls.is_empty() {
                    None
                } else {
                    Some(pnls.iter().sum::<f64>() / pnls.len() as f64)
                };
                let pct = |p: f64| -> Option<f64> {
                    if pnls.is_empty() {
                        None
                    } else {
                        let i = ((pnls.len() as f64 - 1.0) * p).round() as usize;
                        pnls.get(i).copied()
                    }
                };
                let median = pct(0.50);
                let p25 = pct(0.25);
                let p75 = pct(0.75);

                let win_count = rows.iter().filter(|r| r.status == "withdrew").count() as i64;
                let win_rate = if n > 0 {
                    100.0 * win_count as f64 / n as f64
                } else {
                    0.0
                };

                let holds: Vec<i64> = rows.iter().filter_map(|r| r.hold_secs).collect();
                let mean_hold = if holds.is_empty() {
                    None
                } else {
                    Some(holds.iter().sum::<i64>() / holds.len() as i64)
                };

                let mut verdicts: BTreeMap<&str, i64> = BTreeMap::new();
                for r in &rows {
                    *verdicts.entry(r.status.as_str()).or_insert(0) += 1;
                }

                serde_json::json!({
                    "classification": classification,
                    "horizon": horizon,
                    "n": n,
                    "win_rate_pct": round2(win_rate),
                    "mean_pnl_pct": mean_pnl.map(round2),
                    "median_pnl_pct": median.map(round2),
                    "p25_pnl_pct": p25.map(round2),
                    "p75_pnl_pct": p75.map(round2),
                    "mean_hold_secs": mean_hold,
                    "verdicts": verdicts,
                })
            })
            .collect();

        let mut payload = serde_json::json!({
            "since": since,
            "raw_count": outcomes.len(),
            "aggregates": aggregates,
        });
        if params.include_raw {
            // Hard byte budget so the agent can't accidentally pull
            // 90KB of raw rows (which it did in the D1 smoke test).
            // Trim until under budget; the limit param is a soft cap
            // on top of this.
            const RAW_BYTE_BUDGET: usize = 12_000;
            let mut take = (limit as usize).min(outcomes.len());
            loop {
                let raw: Vec<&crate::db::CallOutcome> = outcomes.iter().take(take).collect();
                let serialized = serde_json::to_value(&raw).unwrap_or(serde_json::Value::Null);
                let size = serialized.to_string().len();
                if size <= RAW_BYTE_BUDGET || take <= 5 {
                    payload["outcomes"] = serialized;
                    payload["raw_truncated_to"] = serde_json::json!(take);
                    payload["raw_byte_budget"] = serde_json::json!(RAW_BYTE_BUDGET);
                    break;
                }
                take = (take as f64 * 0.7) as usize;
                if take == 0 {
                    take = 1;
                }
            }
        }
        payload.to_string()
    }

    /// Propose a strategy tune. Server-side validators reject anything
    /// that fails: allow-list field, parseable value, n ≥ 10, effect
    /// size ≥ EFFECT_FLOOR_PCT, holdout non-negative, narrative present.
    /// Returns the proposal id on success or an error JSON on rejection.
    /// Pending proposals do NOT take effect — the agent must follow up
    /// with commit_tune to activate.
    #[tool]
    async fn propose_tune(
        &self,
        Parameters(params): Parameters<ProposeTuneParams>,
    ) -> String {
        let now = chrono::Utc::now().timestamp();

        if let Err(e) = validate_field_scope(&params.field, &params.scope) {
            return reject_proposal("invalid_field_or_scope", &e);
        }
        if let Err(e) = validate_field_value(&params.field, &params.new_value) {
            return reject_proposal("invalid_new_value", &e);
        }
        if let Err(e) = validate_field_value(&params.field, &params.old_value) {
            return reject_proposal("invalid_old_value", &e);
        }
        if params.narrative.trim().len() < 40 {
            return reject_proposal(
                "narrative_too_short",
                "narrative must be ≥ 40 chars; supply 3-4 trader-voice sentences",
            );
        }

        let evidence: serde_json::Value = match serde_json::from_str(&params.evidence_json) {
            Ok(v) => v,
            Err(e) => return reject_proposal("evidence_json_invalid", &e.to_string()),
        };

        let n = match evidence["current"]["n"].as_i64() {
            Some(v) if v >= EVIDENCE_FLOOR => v,
            Some(v) => {
                return reject_proposal(
                    "insufficient_evidence",
                    &format!("current.n = {} < floor of {}", v, EVIDENCE_FLOOR),
                )
            }
            None => return reject_proposal("missing_current_n", "evidence.current.n required"),
        };

        let cur_pnl = match evidence["current"]["mean_pnl_pct"].as_f64() {
            Some(v) => v,
            None => {
                return reject_proposal(
                    "missing_current_pnl",
                    "evidence.current.mean_pnl_pct required",
                )
            }
        };
        let prop_pnl = match evidence["proposed"]["mean_pnl_pct"].as_f64() {
            Some(v) => v,
            None => {
                return reject_proposal(
                    "missing_proposed_pnl",
                    "evidence.proposed.mean_pnl_pct required",
                )
            }
        };
        let effect = prop_pnl - cur_pnl;
        if effect < EFFECT_FLOOR_PCT {
            return reject_proposal(
                "insufficient_effect_size",
                &format!(
                    "proposed mean PnL improvement {:.2}% < floor of {:.1}%",
                    effect, EFFECT_FLOOR_PCT
                ),
            );
        }

        let holdout_n = evidence["holdout"]["n"].as_i64().unwrap_or(0);
        let holdout_pnl = evidence["holdout"]["mean_pnl_pct"].as_f64();
        if holdout_n < 1 {
            return reject_proposal(
                "missing_holdout",
                "evidence.holdout.n must be ≥ 1 — show the proposed setup beats current on a recent slice",
            );
        }
        if let Some(h) = holdout_pnl {
            if h < 0.0 {
                return reject_proposal(
                    "holdout_negative",
                    &format!("holdout mean PnL {:.2}% < 0; proposed setup loses on the recent slice", h),
                );
            }
        }

        let new_proposal = crate::db::NewTuneProposal {
            proposed_at: now,
            proposed_by: params
                .proposed_by
                .clone()
                .unwrap_or_else(|| "claw".to_string()),
            field: params.field.clone(),
            scope: params.scope.clone(),
            old_value: params.old_value.clone(),
            new_value: params.new_value.clone(),
            sample_size: n,
            effect_size: Some(round2(effect)),
            holdout_metric: holdout_pnl.map(round2),
            evidence_json: params.evidence_json.clone(),
            narrative: params.narrative.clone(),
        };
        let id = match self.db.insert_tune_proposal(&new_proposal) {
            Ok(v) => v,
            Err(e) => return reject_proposal("db_error", &e.to_string()),
        };
        let _ = self.db.audit_log(
            "claude",
            "propose_tune",
            &format!(
                "id={} field={} scope={} {} → {} effect={:+.2}% n={}",
                id, params.field, params.scope, params.old_value, params.new_value, effect, n
            ),
        );

        serde_json::json!({
            "ok": true,
            "proposal_id": id,
            "status": "pending",
            "field": params.field,
            "scope": params.scope,
            "effect_size_pct": round2(effect),
            "sample_size": n,
            "next_step": "call commit_tune(proposal_id, body_md) to activate + post the diary entry"
        })
        .to_string()
    }

    /// Commit a pending proposal — writes the signal_overrides row that
    /// makes the change effective for future scan cycles, and creates an
    /// evolution_events row with the agent-authored body_md. Channel post
    /// + diary file write happen here too once Phase A4 is wired in.
    #[tool]
    async fn commit_tune(
        &self,
        Parameters(params): Parameters<CommitTuneParams>,
    ) -> String {
        let proposal = match self.db.get_tune_proposal(params.proposal_id) {
            Ok(Some(p)) => p,
            Ok(None) => {
                return reject_proposal(
                    "not_found",
                    &format!("no proposal with id {}", params.proposal_id),
                )
            }
            Err(e) => return reject_proposal("db_error", &e.to_string()),
        };
        if proposal.status != "pending" {
            return reject_proposal(
                "not_pending",
                &format!(
                    "proposal {} is in status '{}', cannot commit",
                    proposal.id, proposal.status
                ),
            );
        }

        let body = params.body_md.trim();
        if body.len() < BODY_MD_MIN || body.len() > BODY_MD_MAX {
            return reject_proposal(
                "body_md_length",
                &format!(
                    "body_md must be {}..={} chars (got {})",
                    BODY_MD_MIN,
                    BODY_MD_MAX,
                    body.len()
                ),
            );
        }
        // Soft structural check: the body must reference the change
        // (old or new value, or the field name) so the diary entry isn't
        // disconnected from the proposal it narrates.
        let body_lc = body.to_lowercase();
        if !body_lc.contains(&proposal.field.to_lowercase())
            && !body.contains(&proposal.old_value)
            && !body.contains(&proposal.new_value)
        {
            return reject_proposal(
                "body_md_disconnected",
                "body_md must reference either the field name, old_value, or new_value",
            );
        }

        let now = chrono::Utc::now().timestamp();
        if let Err(e) = self.db.upsert_signal_override(
            &proposal.field,
            &proposal.scope,
            &proposal.new_value,
            now,
            Some(proposal.id),
        ) {
            return reject_proposal("db_error_override", &e.to_string());
        }

        let summary = params.summary.unwrap_or_else(|| {
            format!(
                "{} ({}): {} → {}",
                proposal.field, proposal.scope, proposal.old_value, proposal.new_value
            )
        });
        let evo_id = match self.db.insert_evolution_event(
            "strategy",
            &summary,
            body,
            Some(&proposal.evidence_json),
            Some(proposal.id),
            now,
        ) {
            Ok(v) => v,
            Err(e) => return reject_proposal("db_error_evolution", &e.to_string()),
        };

        if let Err(e) = self.db.update_proposal_status(
            proposal.id,
            "committed",
            "claw",
            now,
            None,
            Some(evo_id),
        ) {
            return reject_proposal("db_error_status", &e.to_string());
        }

        let _ = self.db.audit_log(
            "claude",
            "commit_tune",
            &format!(
                "id={} field={} scope={} new_value={} evo_id={}",
                proposal.id, proposal.field, proposal.scope, proposal.new_value, evo_id
            ),
        );

        // Broadcast: post to evolution channel + write markdown to the
        // publisher's thoughts dir + git push. Failures here are NON-fatal
        // — the evolution row is already in DB, so the website diary will
        // pick it up on the next sync, and a channel re-post can be done
        // manually by reading the row.
        let publish = self
            .publish_evolution(evo_id, "STRATEGY", &summary, body)
            .await;

        serde_json::json!({
            "ok": true,
            "proposal_id": proposal.id,
            "evolution_event_id": evo_id,
            "status": "committed",
            "summary": summary,
            "publish": publish,
        })
        .to_string()
    }

    /// List tune proposals, optionally filtered by status. Returns the
    /// full proposal records (status, sample_size, effect_size, etc.)
    /// so the agent can audit its own history before proposing again.
    #[tool]
    async fn list_tunes(
        &self,
        Parameters(params): Parameters<ListTunesParams>,
    ) -> String {
        let limit = params.limit.unwrap_or(50).clamp(1, 500);
        // Treat empty status as no filter — same coercion as analyze_outcomes.
        let status_filter = params
            .status
            .as_deref()
            .filter(|s| !s.trim().is_empty());
        let rows = match self.db.list_tune_proposals(status_filter, limit) {
            Ok(v) => v,
            Err(e) => {
                return serde_json::json!({"error": format!("query failed: {e}")}).to_string()
            }
        };
        serde_json::json!({
            "ok": true,
            "count": rows.len(),
            "proposals": rows
        })
        .to_string()
    }

    /// Revert a previously-committed proposal. Deletes the matching
    /// signal_overrides row (gate falls back to compile-time default)
    /// and marks the proposal `reverted` with a reason. Does NOT
    /// auto-fire an evolution event — the agent should narrate the
    /// revert with a fresh propose+commit cycle if it wants the diary
    /// to record it.
    #[tool]
    async fn revert_tune(
        &self,
        Parameters(params): Parameters<RevertTuneParams>,
    ) -> String {
        let reason = params.reason.trim();
        if reason.len() < 20 {
            return reject_proposal(
                "reason_too_short",
                "reason must be ≥ 20 chars — explain why the override is being removed",
            );
        }
        let proposal = match self.db.get_tune_proposal(params.proposal_id) {
            Ok(Some(p)) => p,
            Ok(None) => {
                return reject_proposal(
                    "not_found",
                    &format!("no proposal with id {}", params.proposal_id),
                )
            }
            Err(e) => return reject_proposal("db_error", &e.to_string()),
        };
        if proposal.status != "committed" {
            return reject_proposal(
                "not_committed",
                &format!(
                    "proposal {} is '{}', not 'committed' — only committed tunes can be reverted",
                    proposal.id, proposal.status
                ),
            );
        }
        if let Err(e) = self.db.delete_signal_override(&proposal.field, &proposal.scope) {
            return reject_proposal("db_error_override", &e.to_string());
        }
        let now = chrono::Utc::now().timestamp();
        if let Err(e) = self.db.update_proposal_status(
            proposal.id,
            "reverted",
            "claw",
            now,
            Some(reason),
            proposal.evolution_event_id,
        ) {
            return reject_proposal("db_error_status", &e.to_string());
        }
        let _ = self.db.audit_log(
            "claude",
            "revert_tune",
            &format!(
                "id={} field={} scope={} reason={}",
                proposal.id, proposal.field, proposal.scope, reason
            ),
        );
        serde_json::json!({
            "ok": true,
            "proposal_id": proposal.id,
            "status": "reverted"
        })
        .to_string()
    }

    /// Propose a new system prompt revision. The agent supplies the full
    /// new content (markdown) plus a `why` explaining what motivated the
    /// rewrite. No statistical floor here — voice is qualitative — but
    /// the content has length bounds and `why` must read like a real
    /// explanation, not a placeholder.
    #[tool]
    async fn propose_prompt(
        &self,
        Parameters(params): Parameters<ProposePromptParams>,
    ) -> String {
        let now = chrono::Utc::now().timestamp();
        let content = params.content.trim();
        if !(PROMPT_MIN..=PROMPT_MAX).contains(&content.len()) {
            return reject_proposal(
                "content_length",
                &format!(
                    "content must be {}..={} chars (got {})",
                    PROMPT_MIN,
                    PROMPT_MAX,
                    content.len()
                ),
            );
        }
        let why = params.why.trim();
        if why.len() < 60 {
            return reject_proposal(
                "why_too_short",
                "why must be ≥ 60 chars — a prompt rewrite is high-blast-radius, explain it",
            );
        }
        let base_version = self
            .db
            .get_current_prompt()
            .ok()
            .flatten()
            .map(|p| p.version);
        let new_proposal = crate::db::NewPromptProposal {
            proposed_at: now,
            proposed_by: params
                .proposed_by
                .clone()
                .unwrap_or_else(|| "claw".to_string()),
            content: content.to_string(),
            why: why.to_string(),
            base_version,
        };
        let id = match self.db.insert_prompt_proposal(&new_proposal) {
            Ok(v) => v,
            Err(e) => return reject_proposal("db_error", &e.to_string()),
        };
        let _ = self.db.audit_log(
            "claude",
            "propose_prompt",
            &format!(
                "id={} base_version={:?} content_len={}",
                id,
                base_version,
                content.len()
            ),
        );
        serde_json::json!({
            "ok": true,
            "proposal_id": id,
            "status": "pending",
            "base_version": base_version,
            "next_step": "call commit_prompt(proposal_id, body_md) to activate the new prompt + post the diary entry"
        })
        .to_string()
    }

    /// Commit a pending prompt revision. Inserts the new content into
    /// `agent_prompt` (auto-incremented version), creates an evolution
    /// event with kind=tool, marks the proposal committed, and publishes.
    #[tool]
    async fn commit_prompt(
        &self,
        Parameters(params): Parameters<CommitPromptParams>,
    ) -> String {
        let proposal = match self.db.get_prompt_proposal(params.proposal_id) {
            Ok(Some(p)) => p,
            Ok(None) => {
                return reject_proposal(
                    "not_found",
                    &format!("no prompt proposal with id {}", params.proposal_id),
                )
            }
            Err(e) => return reject_proposal("db_error", &e.to_string()),
        };
        if proposal.status != "pending" {
            return reject_proposal(
                "not_pending",
                &format!(
                    "prompt proposal {} is in status '{}', cannot commit",
                    proposal.id, proposal.status
                ),
            );
        }
        let body = params.body_md.trim();
        if body.len() < BODY_MD_MIN || body.len() > BODY_MD_MAX {
            return reject_proposal(
                "body_md_length",
                &format!(
                    "body_md must be {}..={} chars (got {})",
                    BODY_MD_MIN,
                    BODY_MD_MAX,
                    body.len()
                ),
            );
        }

        let now = chrono::Utc::now().timestamp();
        let new_version = match self.db.append_prompt(
            &proposal.content,
            now,
            &proposal.proposed_by,
            Some(&proposal.why),
            Some(proposal.id),
        ) {
            Ok(v) => v,
            Err(e) => return reject_proposal("db_error_prompt", &e.to_string()),
        };

        let summary = params
            .summary
            .unwrap_or_else(|| format!("agent prompt rewritten (v{})", new_version));
        let evidence = serde_json::json!({
            "base_version": proposal.base_version,
            "new_version": new_version,
            "why": proposal.why,
            "content_chars": proposal.content.len(),
        })
        .to_string();
        let evo_id = match self.db.insert_evolution_event(
            "tool",
            &summary,
            body,
            Some(&evidence),
            Some(proposal.id),
            now,
        ) {
            Ok(v) => v,
            Err(e) => return reject_proposal("db_error_evolution", &e.to_string()),
        };

        if let Err(e) = self.db.update_prompt_proposal_status(
            proposal.id,
            "committed",
            "claw",
            now,
            None,
            Some(evo_id),
            Some(new_version),
        ) {
            return reject_proposal("db_error_status", &e.to_string());
        }

        let _ = self.db.audit_log(
            "claude",
            "commit_prompt",
            &format!(
                "proposal={} version={} evo_id={}",
                proposal.id, new_version, evo_id
            ),
        );

        let publish = self.publish_evolution(evo_id, "TOOL", &summary, body).await;

        serde_json::json!({
            "ok": true,
            "proposal_id": proposal.id,
            "evolution_event_id": evo_id,
            "agent_prompt_version": new_version,
            "status": "committed",
            "summary": summary,
            "publish": publish,
        })
        .to_string()
    }

    /// List prompt revisions + proposals. By default returns lightweight
    /// rows (content truncated to a preview); pass `include_content=true`
    /// to fetch full markdown bodies.
    #[tool]
    async fn list_prompts(
        &self,
        Parameters(params): Parameters<ListPromptsParams>,
    ) -> String {
        let limit = params.limit.unwrap_or(20).clamp(1, 200);
        let proposals = match self
            .db
            .list_prompt_proposals(limit, params.include_content)
        {
            Ok(v) => v,
            Err(e) => {
                return serde_json::json!({"error": format!("query failed: {e}")}).to_string()
            }
        };
        // Always also surface the currently-active prompt so the agent
        // doesn't have to mentally diff against a copy in its head.
        let current = self.db.get_current_prompt().ok().flatten().map(|p| {
            let content_preview = if params.include_content {
                p.content.clone()
            } else {
                let mut s = p.content;
                if s.len() > 240 {
                    s.truncate(240);
                    s.push_str("…");
                }
                s
            };
            serde_json::json!({
                "version": p.version,
                "created_at": p.created_at,
                "created_by": p.created_by,
                "why": p.why,
                "content": content_preview,
            })
        });
        serde_json::json!({
            "ok": true,
            "current": current,
            "proposals": proposals,
        })
        .to_string()
    }

    /// Record an agent-driven site change as an evolution event. The agent
    /// calls this AFTER it has performed a deliberate site mutation (e.g.
    /// added a new visualization, restructured the diary index). This is
    /// NOT auto-fired by file mutations from the publisher tick — those
    /// are routine, not evolutions.
    #[tool]
    async fn commit_site_change(
        &self,
        Parameters(params): Parameters<CommitSiteChangeParams>,
    ) -> String {
        let now = chrono::Utc::now().timestamp();
        let summary = params.summary.trim();
        if summary.is_empty() || summary.len() > 200 {
            return reject_proposal(
                "summary_length",
                "summary must be 1..=200 chars",
            );
        }
        let body = params.body_md.trim();
        if body.len() < BODY_MD_MIN || body.len() > BODY_MD_MAX {
            return reject_proposal(
                "body_md_length",
                &format!(
                    "body_md must be {}..={} chars (got {})",
                    BODY_MD_MIN,
                    BODY_MD_MAX,
                    body.len()
                ),
            );
        }

        let evo_id = match self.db.insert_evolution_event(
            "site",
            summary,
            body,
            params.evidence_json.as_deref(),
            None,
            now,
        ) {
            Ok(v) => v,
            Err(e) => return reject_proposal("db_error_evolution", &e.to_string()),
        };
        let _ = self.db.audit_log(
            "claude",
            "commit_site_change",
            &format!("evo_id={} summary={}", evo_id, summary),
        );
        let publish = self.publish_evolution(evo_id, "SITE", summary, body).await;
        serde_json::json!({
            "ok": true,
            "evolution_event_id": evo_id,
            "summary": summary,
            "publish": publish,
        })
        .to_string()
    }

    /// Read the tunable knob inventory: name, current effective value,
    /// compile-time default, valid range, supported scopes. The agent
    /// uses this so its prompt doesn't have to hardcode the field list
    /// — and the operator can extend the surface without re-prompting
    /// the agent.
    #[tool]
    async fn describe_signal_filters(
        &self,
        Parameters(_): Parameters<DescribeSignalFiltersParams>,
    ) -> String {
        let overrides = match self.db.list_signal_overrides() {
            Ok(rows) => rows,
            Err(e) => {
                return serde_json::json!({"error": format!("query failed: {e}")}).to_string()
            }
        };
        let active = |field: &str, scope: &str| -> Option<String> {
            overrides
                .iter()
                .find(|(f, s, _, _)| f == field && s == scope)
                .map(|(_, _, v, _)| v.clone())
        };
        let entries = serde_json::json!([
            {
                "field": "min_effective_confidence",
                "kind": "minimum",
                "value_type": "i32",
                "range": {"min": 0, "max": 100},
                "scopes": ["global", "class:STAIRCASE", "class:GRINDER", "class:SPRING"],
                "compile_time_defaults": {
                    "STAIRCASE": 70,
                    "GRINDER": 65,
                    "DEVELOPING": 60,
                },
                "active_overrides": {
                    "global": active("min_effective_confidence", "global"),
                    "class:STAIRCASE": active("min_effective_confidence", "class:STAIRCASE"),
                    "class:GRINDER": active("min_effective_confidence", "class:GRINDER"),
                    "class:SPRING": active("min_effective_confidence", "class:SPRING"),
                },
                "sweepable": true,
                "description": "Per-class confidence floor. Calls below the floor are blocked. Higher = stricter.",
            },
            {
                "field": "max_top_holder_pct",
                "kind": "maximum",
                "value_type": "f64",
                "range": {"min": 0.0, "max": 100.0},
                "scopes": ["global", "class:STAIRCASE", "class:GRINDER", "class:SPRING"],
                "compile_time_defaults": {"global": crate::notifier::SIGNAL_MAX_TOP_HOLDER_PCT},
                "active_overrides": {
                    "global": active("max_top_holder_pct", "global"),
                    "class:STAIRCASE": active("max_top_holder_pct", "class:STAIRCASE"),
                    "class:GRINDER": active("max_top_holder_pct", "class:GRINDER"),
                    "class:SPRING": active("max_top_holder_pct", "class:SPRING"),
                },
                "sweepable": true,
                "description": "Top-1 holder concentration ceiling. Higher top-holder = more rug-prone. Lower = stricter.",
            },
            {
                "field": "min_liquidity_usd",
                "kind": "minimum",
                "value_type": "f64",
                "range": {"min": 0.0, "max": null},
                "scopes": ["global"],
                "compile_time_defaults": {"global": crate::notifier::SIGNAL_MIN_LIQUIDITY_USD},
                "active_overrides": {
                    "global": active("min_liquidity_usd", "global"),
                },
                "sweepable": true,
                "description": "Pool-liquidity floor. Calls below the floor have unhealthy depth. Higher = stricter.",
            },
            {
                "field": "min_volume_24h_usd",
                "kind": "minimum",
                "value_type": "f64",
                "range": {"min": 0.0, "max": null},
                "scopes": ["global"],
                "compile_time_defaults": {"global": crate::notifier::SIGNAL_MIN_VOLUME_24H_USD},
                "active_overrides": {
                    "global": active("min_volume_24h_usd", "global"),
                },
                "sweepable": false,
                "description": "24h volume floor. NOT sweepable from history (rejected calls aren't stored). Tune manually with snapshot evidence.",
            },
            {
                "field": "min_token_age_secs",
                "kind": "minimum",
                "value_type": "i64",
                "range": {"min": 0, "max": null},
                "scopes": ["global"],
                "compile_time_defaults": {"global": crate::notifier::SIGNAL_MIN_TOKEN_AGE_SECS},
                "active_overrides": {
                    "global": active("min_token_age_secs", "global"),
                },
                "sweepable": false,
                "description": "Minimum token age at signal time. NOT sweepable from history.",
            },
        ]);
        serde_json::json!({
            "ok": true,
            "fields": entries,
            "evidence_floor": EVIDENCE_FLOOR,
            "effect_floor_pct": EFFECT_FLOOR_PCT,
        })
        .to_string()
    }

    /// List active runtime overrides (committed tunes). Read counterpart
    /// to commit_tune writes — lets the agent see its own current state
    /// without grepping the DB. Each row is one (field, scope, value)
    /// triple with set_at timestamp.
    #[tool]
    async fn list_overrides(
        &self,
        Parameters(_): Parameters<ListOverridesParams>,
    ) -> String {
        match self.db.list_signal_overrides() {
            Ok(rows) => {
                let entries: Vec<serde_json::Value> = rows
                    .into_iter()
                    .map(|(field, scope, value, set_at)| {
                        serde_json::json!({
                            "field": field,
                            "scope": scope,
                            "value": value,
                            "set_at": set_at,
                        })
                    })
                    .collect();
                serde_json::json!({
                    "ok": true,
                    "count": entries.len(),
                    "overrides": entries,
                })
                .to_string()
            }
            Err(e) => serde_json::json!({"error": format!("query failed: {e}")}).to_string(),
        }
    }

    /// Threshold sweep — for one (field, scope), test a list of
    /// candidate values against the closed-call universe. Returns per
    /// candidate the validator-ready evidence_json (current/proposed/
    /// holdout) plus summary stats. The agent picks one row and feeds
    /// the embedded evidence_json straight into propose_tune — no math
    /// in token-space.
    #[tool]
    async fn sweep_threshold(
        &self,
        Parameters(params): Parameters<SweepThresholdParams>,
    ) -> String {
        if let Err(e) = validate_field_scope(&params.field, &params.scope) {
            return serde_json::json!({"ok": false, "error": "invalid_field_or_scope", "message": e}).to_string();
        }
        if !field_is_sweepable(&params.field) {
            return serde_json::json!({
                "ok": false,
                "error": "field_not_sweepable",
                "message": format!("field '{}' is gated pre-call so historical calls don't carry the value needed to sweep — propose manually with operator evidence", params.field),
            })
            .to_string();
        }
        let holdout_pct = params.holdout_pct.unwrap_or(25).clamp(10, 50) as f64 / 100.0;
        let since = params.since.or_else(|| {
            Some(chrono::Utc::now().timestamp() - 30 * 24 * 60 * 60)
        });
        let class = scope_class(&params.scope);
        let outcomes = match self
            .db
            .list_closed_call_outcomes(class.as_deref(), None, since, 5000)
        {
            Ok(v) => v,
            Err(e) => {
                return serde_json::json!({"ok": false, "error": "db_error", "message": e.to_string()}).to_string();
            }
        };

        if outcomes.is_empty() {
            return serde_json::json!({
                "ok": true,
                "field": params.field,
                "scope": params.scope,
                "since": since,
                "baseline": null,
                "candidates": [],
                "note": "no closed calls in window",
            })
            .to_string();
        }

        // Time split: holdout = newest holdout_pct of universe by called_at.
        let mut sorted = outcomes.clone();
        sorted.sort_by_key(|o| o.called_at);
        let holdout_size = ((sorted.len() as f64) * holdout_pct).round() as usize;
        let holdout_size = holdout_size.max(1).min(sorted.len() - 1);
        let cutoff_idx = sorted.len() - holdout_size;
        let holdout_cutoff = sorted[cutoff_idx].called_at;

        let baseline = bucket_stats(&sorted);
        let current_value = current_effective_value(&self.db, &params.field, &params.scope);

        let candidates: Vec<serde_json::Value> = params
            .candidates
            .iter()
            .filter_map(|cand_str| {
                if validate_field_value(&params.field, cand_str).is_err() {
                    return Some(serde_json::json!({
                        "value": cand_str,
                        "ok": false,
                        "error": "value_invalid",
                    }));
                }
                let mut row = evaluate_candidate(
                    &sorted,
                    &params.field,
                    cand_str,
                    holdout_cutoff,
                );
                row["propose_tune_args"] = serde_json::json!({
                    "field": params.field,
                    "scope": params.scope,
                    "old_value": current_value,
                    "new_value": cand_str,
                    "evidence_json": row.get("evidence_json").cloned().unwrap_or(serde_json::Value::Null),
                });
                Some(row)
            })
            .collect();

        serde_json::json!({
            "ok": true,
            "field": params.field,
            "scope": params.scope,
            "since": since,
            "holdout_cutoff": holdout_cutoff,
            "universe_n": sorted.len(),
            "baseline": baseline,
            "current_value": current_value,
            "candidates": candidates,
        })
        .to_string()
    }

    /// Rank tunable candidates across (scope × field × candidate-grid)
    /// using a built-in candidate set per field. Returns the top-K
    /// ranked by `effect_pct × √n_passing`, each with a fully-formed
    /// evidence_json the agent can pass to propose_tune verbatim.
    /// This is the "what should I tune?" endpoint — it surveys the
    /// space deterministically so the LLM doesn't have to enumerate.
    #[tool]
    async fn rank_tune_candidates(
        &self,
        Parameters(params): Parameters<RankTuneCandidatesParams>,
    ) -> String {
        let top_k = params.top_k.unwrap_or(5).clamp(1, 20) as usize;
        let holdout_pct = params.holdout_pct.unwrap_or(25).clamp(10, 50) as f64 / 100.0;
        let since = params.since.or_else(|| {
            Some(chrono::Utc::now().timestamp() - 30 * 24 * 60 * 60)
        });

        // Treat empty arrays as "use defaults" — matches the coercion
        // pattern elsewhere (empty string → no filter). The agent often
        // passes `fields:[]` when it means "all of them."
        let fields_param = params
            .fields
            .clone()
            .filter(|v| !v.is_empty());
        let scopes_param = params
            .scopes
            .clone()
            .filter(|v| !v.is_empty());
        let fields = fields_param
            .unwrap_or_else(|| {
                vec![
                    "min_effective_confidence".to_string(),
                    "max_top_holder_pct".to_string(),
                    "min_liquidity_usd".to_string(),
                ]
            })
            .into_iter()
            .filter(|f| field_is_sweepable(f))
            .collect::<Vec<_>>();

        let mut all_candidates: Vec<serde_json::Value> = Vec::new();

        for field in &fields {
            let scopes_to_try: Vec<String> = scopes_param
                .clone()
                .unwrap_or_else(|| default_scopes_for_field(field));
            for scope in &scopes_to_try {
                if validate_field_scope(field, scope).is_err() {
                    continue;
                }
                let class = scope_class(scope);
                let outcomes = match self
                    .db
                    .list_closed_call_outcomes(class.as_deref(), None, since, 5000)
                {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                if outcomes.len() < EVIDENCE_FLOOR as usize {
                    continue;
                }
                let mut sorted = outcomes;
                sorted.sort_by_key(|o| o.called_at);
                let holdout_size = ((sorted.len() as f64) * holdout_pct).round() as usize;
                let holdout_size = holdout_size.max(1).min(sorted.len() - 1);
                let cutoff_idx = sorted.len() - holdout_size;
                let holdout_cutoff = sorted[cutoff_idx].called_at;

                let current_value = current_effective_value(&self.db, field, scope);
                for cand in default_candidates_for_field(field) {
                    let row = evaluate_candidate(&sorted, field, &cand, holdout_cutoff);
                    if row.get("ok").and_then(|v| v.as_bool()) != Some(true) {
                        continue;
                    }
                    let n = row.get("n_passing").and_then(|v| v.as_i64()).unwrap_or(0);
                    let effect = row
                        .get("effect_vs_baseline_pct")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0);
                    let holdout_n = row
                        .get("holdout")
                        .and_then(|v| v.get("n"))
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);
                    let holdout_pnl = row
                        .get("holdout")
                        .and_then(|v| v.get("mean_pnl_pct"))
                        .and_then(|v| v.as_f64());
                    if n < EVIDENCE_FLOOR
                        || effect < EFFECT_FLOOR_PCT
                        || holdout_n < 1
                        || holdout_pnl.map_or(true, |h| h < 0.0)
                    {
                        continue;
                    }
                    let leverage = effect * (n as f64).sqrt();
                    let mut enriched = row.clone();
                    enriched["field"] = serde_json::json!(field);
                    enriched["scope"] = serde_json::json!(scope);
                    enriched["leverage"] = serde_json::json!(round2(leverage));
                    enriched["current_value"] = serde_json::json!(&current_value);
                    enriched["propose_tune_args"] = serde_json::json!({
                        "field": field,
                        "scope": scope,
                        "old_value": current_value,
                        "new_value": cand,
                        "evidence_json": enriched.get("evidence_json").cloned().unwrap_or(serde_json::Value::Null),
                    });
                    all_candidates.push(enriched);
                }
            }
        }

        all_candidates.sort_by(|a, b| {
            let la = a.get("leverage").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let lb = b.get("leverage").and_then(|v| v.as_f64()).unwrap_or(0.0);
            lb.partial_cmp(&la).unwrap_or(std::cmp::Ordering::Equal)
        });
        all_candidates.truncate(top_k);

        serde_json::json!({
            "ok": true,
            "since": since,
            "fields_searched": fields,
            "candidate_count": all_candidates.len(),
            "candidates": all_candidates,
            "note": "All candidates pre-validated against the same floors propose_tune enforces (n>=10, effect>=5%, holdout n>=1 and mean>=0). evidence_json on each row can be passed to propose_tune verbatim.",
        })
        .to_string()
    }

    /// Time-windowed drift detector. Slices the closed-call universe
    /// into N equal time windows and reports per-window (n, mean_pnl,
    /// win_rate). The agent uses this to spot "this bucket was +12%
    /// for the first 3 weeks then -8% in week 4" patterns that
    /// `analyze_outcomes` smoothes over.
    #[tool]
    async fn analyze_drift(
        &self,
        Parameters(params): Parameters<AnalyzeDriftParams>,
    ) -> String {
        let window_count = params.window_count.unwrap_or(4).clamp(2, 12);
        let span = params.window_secs.unwrap_or(30 * 24 * 60 * 60);
        let now = chrono::Utc::now().timestamp();
        let since = now - span;
        let class = scope_class(&params.scope);
        let outcomes = match self
            .db
            .list_closed_call_outcomes(class.as_deref(), None, Some(since), 5000)
        {
            Ok(v) => v,
            Err(e) => return serde_json::json!({"ok": false, "error": "db_error", "message": e.to_string()}).to_string(),
        };
        let window_size = span / window_count;
        let windows: Vec<serde_json::Value> = (0..window_count)
            .map(|i| {
                let start = since + i * window_size;
                let end = start + window_size;
                let slice: Vec<&crate::db::CallOutcome> = outcomes
                    .iter()
                    .filter(|o| o.called_at >= start && o.called_at < end)
                    .collect();
                let n = slice.len() as i64;
                let pnls: Vec<f64> = slice.iter().filter_map(|r| r.pnl_pct).collect();
                let mean_pnl = if pnls.is_empty() {
                    None
                } else {
                    Some(pnls.iter().sum::<f64>() / pnls.len() as f64)
                };
                let wins = slice.iter().filter(|r| r.status == "withdrew").count() as i64;
                let win_rate = if n > 0 { 100.0 * wins as f64 / n as f64 } else { 0.0 };
                serde_json::json!({
                    "window_index": i,
                    "start": start,
                    "end": end,
                    "n": n,
                    "mean_pnl_pct": mean_pnl.map(round2),
                    "win_rate_pct": round2(win_rate),
                })
            })
            .collect();
        let overall_pnls: Vec<f64> = outcomes.iter().filter_map(|r| r.pnl_pct).collect();
        let overall_mean = if overall_pnls.is_empty() {
            None
        } else {
            Some(overall_pnls.iter().sum::<f64>() / overall_pnls.len() as f64)
        };
        // Drift signal: largest absolute delta of any window's mean from
        // the overall mean, plus the trend direction (last - first).
        let mut max_dev: f64 = 0.0;
        let mut max_dev_idx: i64 = -1;
        if let Some(om) = overall_mean {
            for w in &windows {
                if let Some(m) = w.get("mean_pnl_pct").and_then(|v| v.as_f64()) {
                    let dev = (m - om).abs();
                    if dev > max_dev {
                        max_dev = dev;
                        max_dev_idx = w.get("window_index").and_then(|v| v.as_i64()).unwrap_or(-1);
                    }
                }
            }
        }
        let first_mean = windows
            .first()
            .and_then(|w| w.get("mean_pnl_pct"))
            .and_then(|v| v.as_f64());
        let last_mean = windows
            .last()
            .and_then(|w| w.get("mean_pnl_pct"))
            .and_then(|v| v.as_f64());
        let trend = match (first_mean, last_mean) {
            (Some(f), Some(l)) => Some(l - f),
            _ => None,
        };
        serde_json::json!({
            "ok": true,
            "scope": params.scope,
            "since": since,
            "window_count": window_count,
            "window_size_secs": window_size,
            "universe_n": outcomes.len(),
            "overall_mean_pnl_pct": overall_mean.map(round2),
            "windows": windows,
            "max_deviation_pct": round2(max_dev),
            "max_deviation_window": max_dev_idx,
            "trend_first_to_last_pct": trend.map(round2),
        })
        .to_string()
    }

    /// Failure-mode taxonomy. For closed calls in scope, classify each
    /// non-winning outcome into a category by status + exit_note pattern,
    /// returning counts + mean PnL per category. Lets the agent name
    /// what's broken (rug vs timeout vs slow-bleed) instead of just
    /// reporting "lots of failures."
    #[tool]
    async fn analyze_failure_modes(
        &self,
        Parameters(params): Parameters<AnalyzeFailureModesParams>,
    ) -> String {
        let since = params.since.or_else(|| {
            Some(chrono::Utc::now().timestamp() - 30 * 24 * 60 * 60)
        });
        let class = scope_class(&params.scope);
        let outcomes = match self
            .db
            .list_closed_call_outcomes(class.as_deref(), None, since, 5000)
        {
            Ok(v) => v,
            Err(e) => return serde_json::json!({"ok": false, "error": "db_error", "message": e.to_string()}).to_string(),
        };
        use std::collections::BTreeMap;
        let mut buckets: BTreeMap<&str, Vec<&crate::db::CallOutcome>> = BTreeMap::new();
        for o in &outcomes {
            let mode = classify_failure_mode(o);
            buckets.entry(mode).or_default().push(o);
        }
        let summaries: Vec<serde_json::Value> = buckets
            .into_iter()
            .map(|(mode, rows)| {
                let n = rows.len() as i64;
                let pnls: Vec<f64> = rows.iter().filter_map(|r| r.pnl_pct).collect();
                let mean_pnl = if pnls.is_empty() {
                    None
                } else {
                    Some(pnls.iter().sum::<f64>() / pnls.len() as f64)
                };
                let median = if pnls.is_empty() {
                    None
                } else {
                    let mut sorted = pnls.clone();
                    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                    Some(sorted[sorted.len() / 2])
                };
                serde_json::json!({
                    "mode": mode,
                    "n": n,
                    "share_pct": round2(100.0 * n as f64 / outcomes.len().max(1) as f64),
                    "mean_pnl_pct": mean_pnl.map(round2),
                    "median_pnl_pct": median.map(round2),
                })
            })
            .collect();
        serde_json::json!({
            "ok": true,
            "scope": params.scope,
            "since": since,
            "universe_n": outcomes.len(),
            "modes": summaries,
        })
        .to_string()
    }

    /// Read the public diary so the agent can avoid repeating itself
    /// and can cite prior moves when narrating a new one. Body_md is
    /// omitted by default to keep payloads small — set
    /// `include_body=true` when the agent specifically wants to see
    /// what its previous voice looked like.
    #[tool]
    async fn list_evolution_events(
        &self,
        Parameters(params): Parameters<ListEvolutionEventsParams>,
    ) -> String {
        let limit = params.limit.unwrap_or(20).clamp(1, 200);
        let kind_filter = params
            .kind
            .as_deref()
            .filter(|s| !s.trim().is_empty());
        match self.db.list_evolution_events(kind_filter, limit) {
            Ok(rows) => {
                let entries: Vec<serde_json::Value> = rows
                    .into_iter()
                    .map(|e| {
                        let mut obj = serde_json::json!({
                            "id": e.id,
                            "kind": e.kind,
                            "summary": e.summary,
                            "committed_at": e.committed_at,
                            "posted_at": e.posted_at,
                            "proposal_id": e.proposal_id,
                            "diary_path": e.diary_path,
                        });
                        if params.include_body {
                            obj["body_md"] = serde_json::json!(e.body_md);
                        }
                        obj
                    })
                    .collect();
                serde_json::json!({
                    "ok": true,
                    "count": entries.len(),
                    "events": entries,
                })
                .to_string()
            }
            Err(e) => serde_json::json!({"error": format!("query failed: {e}")}).to_string(),
        }
    }

    /// Before/after comparison anchored at a timestamp (e.g. a prior
    /// tune's committed_at). Returns per-window aggregates + delta.
    /// "Did my last tune actually work?" tool. anchor_at defaults to
    /// now; use the tune's committed_at to compare pre-tune vs
    /// post-tune performance.
    #[tool]
    async fn compare_periods(
        &self,
        Parameters(params): Parameters<ComparePeriodsParams>,
    ) -> String {
        let now = chrono::Utc::now().timestamp();
        let anchor = params.anchor_at.unwrap_or(now);
        let before_span = params.before_secs.unwrap_or(30 * 24 * 60 * 60);
        let after_span = params.after_secs.unwrap_or((now - anchor).max(0));
        let before_start = anchor - before_span;
        let before_end = anchor;
        let after_start = anchor;
        let after_end = anchor + after_span;
        let class = scope_class(&params.scope);
        // Pull a wide superset and partition in-memory; cheaper than two
        // round trips and keeps the bucket math identical.
        let outcomes = match self
            .db
            .list_closed_call_outcomes(class.as_deref(), None, Some(before_start), 5000)
        {
            Ok(v) => v,
            Err(e) => return serde_json::json!({"ok": false, "error": "db_error", "message": e.to_string()}).to_string(),
        };
        let before: Vec<&crate::db::CallOutcome> = outcomes
            .iter()
            .filter(|o| o.called_at >= before_start && o.called_at < before_end)
            .collect();
        let after: Vec<&crate::db::CallOutcome> = outcomes
            .iter()
            .filter(|o| o.called_at >= after_start && o.called_at < after_end)
            .collect();
        let stats = |rows: &[&crate::db::CallOutcome]| -> serde_json::Value {
            let n = rows.len() as i64;
            let pnls: Vec<f64> = rows.iter().filter_map(|r| r.pnl_pct).collect();
            let mean = if pnls.is_empty() {
                None
            } else {
                Some(pnls.iter().sum::<f64>() / pnls.len() as f64)
            };
            let wins = rows.iter().filter(|r| r.status == "withdrew").count() as i64;
            let win_rate = if n > 0 { 100.0 * wins as f64 / n as f64 } else { 0.0 };
            serde_json::json!({
                "n": n,
                "mean_pnl_pct": mean.map(round2),
                "win_rate_pct": round2(win_rate),
            })
        };
        let before_stats = stats(&before);
        let after_stats = stats(&after);
        let delta_mean = match (
            after_stats.get("mean_pnl_pct").and_then(|v| v.as_f64()),
            before_stats.get("mean_pnl_pct").and_then(|v| v.as_f64()),
        ) {
            (Some(a), Some(b)) => Some(a - b),
            _ => None,
        };
        let delta_win = match (
            after_stats.get("win_rate_pct").and_then(|v| v.as_f64()),
            before_stats.get("win_rate_pct").and_then(|v| v.as_f64()),
        ) {
            (Some(a), Some(b)) => Some(a - b),
            _ => None,
        };
        serde_json::json!({
            "ok": true,
            "scope": params.scope,
            "anchor_at": anchor,
            "before": {"start": before_start, "end": before_end, "stats": before_stats},
            "after": {"start": after_start, "end": after_end, "stats": after_stats},
            "delta_mean_pnl_pct": delta_mean.map(round2),
            "delta_win_rate_pct": delta_win.map(round2),
        })
        .to_string()
    }

    /// Glossary of classifications + horizons in voice. Returns the
    /// agent's frame for what each label *means* — so the prompt
    /// doesn't have to carry the glossary and so a future operator
    /// can extend the surface without re-prompting.
    #[tool]
    async fn describe_classifications(
        &self,
        Parameters(_): Parameters<DescribeClassificationsParams>,
    ) -> String {
        serde_json::json!({
            "ok": true,
            "classifications": [
                {
                    "name": "STAIRCASE",
                    "voice": "stair-stepping accumulation; volume + price advancing in steps. Higher confidence threshold because false positives on this shape are usually still alive — the strategy wants the cleanest stair, not the questionable one.",
                    "default_floor": 70,
                },
                {
                    "name": "GRINDER",
                    "voice": "slow upward drift, narrow ATR, persistent bid. Forgivable on confidence (lower default floor) because the shape itself is anti-rug — quiet tape, organic distribution.",
                    "default_floor": 65,
                },
                {
                    "name": "SPRING",
                    "voice": "compression + release; basing range broken. Newer pattern — no per-class default floor in production yet.",
                    "default_floor": null,
                },
                {
                    "name": "DEVELOPING",
                    "voice": "still forming; not yet committed to a shape. Conservative floor (60) because the pattern is unproven; mostly a watch class.",
                    "default_floor": 60,
                }
            ],
            "horizons": [
                {"name": "SCALP", "voice": "minutes-scale flip; exit on first stall."},
                {"name": "SHORT", "voice": "hours-scale; ride the first leg, exit at first lower-high."},
                {"name": "LONG", "voice": "days-scale; let position breathe through normal pullbacks."},
                {"name": "MOONSHOT", "voice": "low-probability multi-x runner; size small, hold through most drawdowns."},
            ],
            "tunable_classes": ["STAIRCASE", "GRINDER", "SPRING"],
            "note": "DEVELOPING has a compile-time floor but is not in the per-class scope allow-list for tunes — only STAIRCASE/GRINDER/SPRING accept class:X overrides."
        })
        .to_string()
    }

    /// Counterfactual replay applying multiple overrides at once.
    /// Lets the agent test combined effects ("what if I tighten BOTH
    /// confidence and liquidity?") deterministically — no LLM math.
    /// Returns the would-have-been bucket aggregates plus delta vs the
    /// current bucket.
    #[tool]
    async fn simulate_overrides(
        &self,
        Parameters(params): Parameters<SimulateOverridesParams>,
    ) -> String {
        if params.overrides.is_empty() {
            return serde_json::json!({"ok": false, "error": "empty_overrides", "message": "at least one override required"}).to_string();
        }
        for o in &params.overrides {
            if let Err(e) = validate_field_scope(&o.field, &o.scope) {
                return serde_json::json!({"ok": false, "error": "invalid_field_or_scope", "detail": e}).to_string();
            }
            if let Err(e) = validate_field_value(&o.field, &o.new_value) {
                return serde_json::json!({"ok": false, "error": "invalid_value", "detail": e}).to_string();
            }
            if !field_is_sweepable(&o.field) {
                return serde_json::json!({"ok": false, "error": "field_not_sweepable", "field": o.field}).to_string();
            }
        }
        let since = params.since.or_else(|| {
            Some(chrono::Utc::now().timestamp() - 30 * 24 * 60 * 60)
        });
        let outcomes = match self.db.list_closed_call_outcomes(None, None, since, 5000) {
            Ok(v) => v,
            Err(e) => return serde_json::json!({"ok": false, "error": "db_error", "message": e.to_string()}).to_string(),
        };
        let baseline = bucket_stats(&outcomes);

        // For each call, check if it passes ALL the simulated overrides.
        // Per-class scopes only apply when call.classification matches.
        let passing: Vec<&crate::db::CallOutcome> = outcomes
            .iter()
            .filter(|c| {
                params.overrides.iter().all(|ov| {
                    let scope_class = ov.scope.strip_prefix("class:");
                    if let Some(sc) = scope_class {
                        if c.classification != sc {
                            // Override doesn't apply to this call's class — pass.
                            return true;
                        }
                    }
                    call_passes(c, &ov.field, &ov.new_value)
                })
            })
            .collect();
        let pnls: Vec<f64> = passing.iter().filter_map(|r| r.pnl_pct).collect();
        let mean = if pnls.is_empty() {
            None
        } else {
            Some(pnls.iter().sum::<f64>() / pnls.len() as f64)
        };
        let wins = passing.iter().filter(|r| r.status == "withdrew").count() as i64;
        let win_rate = if !passing.is_empty() {
            100.0 * wins as f64 / passing.len() as f64
        } else {
            0.0
        };
        let cur_mean = baseline.get("mean_pnl_pct").and_then(|v| v.as_f64());
        let delta = match (mean, cur_mean) {
            (Some(p), Some(c)) => Some(p - c),
            _ => None,
        };
        serde_json::json!({
            "ok": true,
            "since": since,
            "overrides": params.overrides.iter().map(|o| serde_json::json!({
                "field": o.field, "scope": o.scope, "new_value": o.new_value
            })).collect::<Vec<_>>(),
            "baseline": baseline,
            "simulated": {
                "n": passing.len(),
                "mean_pnl_pct": mean.map(round2),
                "win_rate_pct": round2(win_rate),
            },
            "delta_mean_pnl_pct": delta.map(round2),
        })
        .to_string()
    }

    /// Read recent claw review cycles. Lets the agent recall what it
    /// looked at + concluded last time so the cycles aren't independent.
    #[tool]
    async fn review_log(
        &self,
        Parameters(params): Parameters<ReviewLogParams>,
    ) -> String {
        let limit = params.limit.unwrap_or(10).clamp(1, 200);
        match self.db.list_review_cycles(limit) {
            Ok(rows) => serde_json::json!({
                "ok": true,
                "count": rows.len(),
                "cycles": rows,
            })
            .to_string(),
            Err(e) => serde_json::json!({"error": format!("query failed: {e}")}).to_string(),
        }
    }

    /// Write a review-cycle ledger row. Claw calls this once per cycle
    /// at the end with a one-paragraph summary; future cycles read via
    /// `review_log`. The agent's memory across runs.
    #[tool]
    async fn review_log_write(
        &self,
        Parameters(params): Parameters<ReviewLogWriteParams>,
    ) -> String {
        if !["propose", "commit"].contains(&params.mode.as_str()) {
            return serde_json::json!({"ok": false, "error": "invalid_mode"}).to_string();
        }
        if !["proposed", "committed", "stopped", "failed"].contains(&params.outcome.as_str()) {
            return serde_json::json!({"ok": false, "error": "invalid_outcome"}).to_string();
        }
        let summary = params.summary.trim();
        if summary.is_empty() || summary.len() > 2000 {
            return serde_json::json!({"ok": false, "error": "summary_length", "message": "summary must be 1..=2000 chars"}).to_string();
        }
        let now = chrono::Utc::now().timestamp();
        match self.db.insert_review_cycle(
            params.started_at,
            Some(now),
            &params.mode,
            &params.outcome,
            params.proposal_id,
            summary,
            params.turns,
            params.tool_calls,
        ) {
            Ok(id) => serde_json::json!({
                "ok": true,
                "review_cycle_id": id,
                "ended_at": now,
            })
            .to_string(),
            Err(e) => serde_json::json!({"ok": false, "error": "db_error", "message": e.to_string()}).to_string(),
        }
    }

    /// Internal helper — publishes a freshly-inserted evolution event to
    /// the operator's surfaces (Telegram channel + publisher diary). All
    /// failures are non-fatal; the evolution row already lives in DB and
    /// can be re-published manually from the operator surface. Returns a
    /// JSON value the caller surfaces to the agent so it can see the
    /// fan-out result.
    async fn publish_evolution(
        &self,
        evo_id: i64,
        kind_label: &str,
        summary: &str,
        body_md: &str,
    ) -> serde_json::Value {
        let mut channel_msg_id: Option<i64> = None;
        let mut diary_path: Option<String> = None;
        let mut errors: Vec<String> = Vec::new();
        let now = chrono::Utc::now().timestamp();

        // Telegram channel post — short HTML preview + link to diary.
        if let Some(tg) = &self.config.telegram {
            if tg.enabled && !tg.bot_token.is_empty() {
                let chat_id = if !tg.evolution_chat_id.is_empty() {
                    tg.evolution_chat_id.as_str()
                } else if !tg.ops_chat_id.is_empty() {
                    tg.ops_chat_id.as_str()
                } else {
                    ""
                };
                if !chat_id.is_empty() {
                    let preview = body_md
                        .lines()
                        .filter(|l| !l.trim().is_empty())
                        .take(8)
                        .collect::<Vec<_>>()
                        .join("\n");
                    let preview = html_escape_basic(&preview);
                    let link = if !tg.public_url.is_empty() {
                        format!(
                            "\n\n📜 <a href=\"{}/#diary={}\">full diary entry</a>",
                            tg.public_url.trim_end_matches('/'),
                            evo_id
                        )
                    } else {
                        String::new()
                    };
                    let text = format!(
                        "🧬 <b>EVOLVED — {kind}</b> · {summary_html}\n\n{preview}{link}",
                        kind = kind_label,
                        summary_html = html_escape_basic(summary),
                    );
                    let url = format!(
                        "https://api.telegram.org/bot{}/sendMessage",
                        tg.bot_token
                    );
                    let resp = self
                        .http
                        .post(&url)
                        .form(&[
                            ("chat_id", chat_id),
                            ("text", text.as_str()),
                            ("parse_mode", "HTML"),
                            ("disable_web_page_preview", "true"),
                        ])
                        .send()
                        .await;
                    match resp {
                        Ok(r) => match r.json::<serde_json::Value>().await {
                            Ok(v) if v["ok"].as_bool() == Some(true) => {
                                channel_msg_id = v["result"]["message_id"].as_i64();
                            }
                            Ok(v) => errors.push(format!("telegram: {}", v)),
                            Err(e) => errors.push(format!("telegram parse: {}", e)),
                        },
                        Err(e) => errors.push(format!("telegram post: {}", e)),
                    }
                }
            }
        }

        // Diary file write + git push to the publisher repo.
        if let Some(mp) = self.config.madapes.clone() {
            if mp.enabled && !mp.repo_path.is_empty() {
                let date = chrono::Utc::now().format("%Y-%m-%d").to_string();
                let slug = slugify(summary);
                let file = format!("{}_evo-{}-{}.md", date, kind_label.to_lowercase(), slug);
                let thoughts_dir = std::path::PathBuf::from(&mp.repo_path).join("thoughts");
                if let Err(e) = std::fs::create_dir_all(&thoughts_dir) {
                    errors.push(format!("diary mkdir: {}", e));
                } else {
                    let path = thoughts_dir.join(&file);
                    let title = format!("EVOLVED — {kind_label} · {summary}");
                    let frontmatter = format!(
                        "---\nkind: evolution\ncategory: {}\ndate: {}\nevolution_event_id: {}\ntitle: {}\n---\n\n",
                        kind_label.to_lowercase(),
                        date,
                        evo_id,
                        title
                    );
                    let contents = format!("{}{}\n", frontmatter, body_md);
                    if let Err(e) = std::fs::write(&path, &contents) {
                        errors.push(format!("diary write: {}", e));
                    } else {
                        diary_path = Some(format!("thoughts/{}", file));
                        // Update index.json so the front end picks it up.
                        let index_path = thoughts_dir.join("index.json");
                        let mut index_val: serde_json::Value = std::fs::read_to_string(&index_path)
                            .ok()
                            .and_then(|s| serde_json::from_str(&s).ok())
                            .unwrap_or_else(|| serde_json::json!({"thoughts": []}));
                        if let Some(arr) =
                            index_val.get_mut("thoughts").and_then(|v| v.as_array_mut())
                        {
                            arr.insert(
                                0,
                                serde_json::json!({
                                    "date": date,
                                    "file": file,
                                    "title": title,
                                    "kind": "evolution",
                                    "category": kind_label.to_lowercase(),
                                }),
                            );
                        }
                        let _ = std::fs::write(
                            &index_path,
                            serde_json::to_string_pretty(&index_val).unwrap_or_default(),
                        );
                        let repo = &mp.repo_path;
                        let msg = format!("evo: {kind_label} · {summary}");
                        let _ = run_git_with_timeout(&["-C", repo, "add", "thoughts/"]).await;
                        let commit =
                            run_git_with_timeout(&["-C", repo, "commit", "-m", &msg]).await;
                        let committed =
                            commit.as_ref().map(|o| o.status.success()).unwrap_or(false);
                        if committed {
                            let _ = run_git_with_timeout(&["-C", repo, "push", "--quiet"]).await;
                        } else if let Ok(out) = commit {
                            errors.push(format!(
                                "diary commit non-zero: {}",
                                String::from_utf8_lossy(&out.stderr).trim()
                            ));
                        }
                    }
                }
            }
        }

        if let Err(e) = self.db.update_evolution_posted(
            evo_id,
            now,
            channel_msg_id,
            diary_path.as_deref(),
        ) {
            errors.push(format!("db update: {}", e));
        }

        serde_json::json!({
            "channel_msg_id": channel_msg_id,
            "diary_path": diary_path,
            "errors": errors,
        })
    }
}

/// Minimal HTML escape for the Telegram preview body. Telegram parses a
/// small HTML subset; we only need to neutralize `< > &` to prevent the
/// agent's markdown from breaking the bot's <b>/<a> tags.
fn html_escape_basic(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

// ── autonomy support: validators + constants ──────────────────────────

/// Hardcoded floor on closed-call sample size in the agent's evidence.
/// Anything below this is "noise pretending to be a pattern" and gets
/// rejected with `insufficient_evidence`. Lifting the floor requires
/// a code change — by design.
pub const EVIDENCE_FLOOR: i64 = 10;

/// Minimum mean-PnL improvement (proposed - current) required for a
/// proposal to pass the effect-size validator. Defaults to 5%.
pub const EFFECT_FLOOR_PCT: f64 = 5.0;

const BODY_MD_MIN: usize = 200;
const BODY_MD_MAX: usize = 4000;

/// System prompt content bounds. Wider than body_md because the prompt
/// itself carries the agent's full identity and instructions; cap at
/// 20K to prevent runaway prompt sprawl.
const PROMPT_MIN: usize = 200;
const PROMPT_MAX: usize = 20_000;

/// Validates that (field, scope) is in the tunable allow-list. Adding a
/// new tunable requires editing this map AND wiring it into should_signal
/// (Phase A5). Until both are done, the agent gets `invalid_field_or_scope`
/// — preventing the agent from inventing fields the runtime won't honor.
/// Classify a closed call into a failure-mode bucket based on status
/// + exit_note text. Coarse but useful: `winner` (positive PnL),
/// `flat` (small abs PnL), `slow_bleed` (negative + slow exit),
/// `drawdown_cap` (note mentions stop/dropped), `rug_or_fast_dump`
/// (large negative + fast exit), `timeout` (status=expired), `void`
/// (status=voided / failed-but-no-fill).
fn classify_failure_mode(o: &crate::db::CallOutcome) -> &'static str {
    if o.status == "voided" {
        return "void";
    }
    if o.status == "expired" {
        return "timeout";
    }
    let pnl = o.pnl_pct.unwrap_or(0.0);
    if pnl >= 5.0 {
        return "winner";
    }
    if pnl >= -5.0 {
        return "flat";
    }
    let note = o.exit_note.as_deref().unwrap_or("").to_lowercase();
    if note.contains("rug") || note.contains("dump") || note.contains("trap") {
        return "rug_or_fast_dump";
    }
    if note.contains("stop") || note.contains("dropped") || note.contains("cap") {
        return "drawdown_cap";
    }
    let hold = o.hold_secs.unwrap_or(0);
    if pnl <= -25.0 && hold < 600 {
        return "rug_or_fast_dump";
    }
    if pnl <= -15.0 && hold > 1800 {
        return "slow_bleed";
    }
    "slow_bleed"
}

/// Resolve the current effective value of a (field, scope) — the
/// override if one exists, otherwise the compile-time default. Returns
/// the value as a string so the agent can pass it directly as
/// `old_value` to propose_tune. Eliminates a class of self-recovery
/// turns where the agent had to grep list_overrides + describe_signal_filters
/// just to learn the current value.
fn current_effective_value(db: &crate::db::Db, field: &str, scope: &str) -> String {
    if let Ok(Some(v)) = db.get_signal_override(field, scope) {
        return v;
    }
    // Per-class scope falls back to global override before compile-time default.
    if scope.starts_with("class:") {
        if let Ok(Some(v)) = db.get_signal_override(field, "global") {
            return v;
        }
    }
    match (field, scope) {
        ("min_effective_confidence", "class:STAIRCASE") => "70".into(),
        ("min_effective_confidence", "class:GRINDER") => "65".into(),
        ("min_effective_confidence", "class:SPRING") => "0".into(),
        ("min_effective_confidence", _) => "0".into(),
        ("max_top_holder_pct", _) => format!("{}", crate::notifier::SIGNAL_MAX_TOP_HOLDER_PCT),
        ("min_liquidity_usd", _) => format!("{}", crate::notifier::SIGNAL_MIN_LIQUIDITY_USD),
        ("min_volume_24h_usd", _) => format!("{}", crate::notifier::SIGNAL_MIN_VOLUME_24H_USD),
        ("min_token_age_secs", _) => format!("{}", crate::notifier::SIGNAL_MIN_TOKEN_AGE_SECS),
        _ => "0".into(),
    }
}

/// Whether a tunable field can be swept against historical closed calls.
/// Fields gated *pre-call* (volume, age) don't store the value on the
/// CallOutcome row so we have nothing to filter against — the agent
/// must propose those manually with operator-supplied evidence.
fn field_is_sweepable(field: &str) -> bool {
    matches!(
        field,
        "min_effective_confidence" | "max_top_holder_pct" | "min_liquidity_usd"
    )
}

/// Map a scope string to a classification filter for
/// `list_closed_call_outcomes`. "global" / unknown → None (no filter);
/// "class:STAIRCASE" → Some("STAIRCASE").
fn scope_class(scope: &str) -> Option<String> {
    scope.strip_prefix("class:").map(|s| s.to_string())
}

/// Default scope set per field for `rank_tune_candidates` when the
/// caller doesn't specify. Per-class scopes only for fields that
/// support them; global-only otherwise.
fn default_scopes_for_field(field: &str) -> Vec<String> {
    match field {
        "min_effective_confidence" | "max_top_holder_pct" => vec![
            "global".into(),
            "class:STAIRCASE".into(),
            "class:GRINDER".into(),
            "class:SPRING".into(),
        ],
        _ => vec!["global".into()],
    }
}

/// Default candidate grid per field. Picked to span "stricter than today"
/// without overshooting the realistic operating range.
fn default_candidates_for_field(field: &str) -> Vec<String> {
    match field {
        "min_effective_confidence" => vec!["62", "66", "70", "74", "78", "82"]
            .into_iter()
            .map(String::from)
            .collect(),
        "max_top_holder_pct" => vec!["10", "15", "20", "25", "30"]
            .into_iter()
            .map(String::from)
            .collect(),
        "min_liquidity_usd" => {
            vec!["20000", "30000", "50000", "75000", "100000", "150000"]
                .into_iter()
                .map(String::from)
                .collect()
        }
        _ => vec![],
    }
}

/// Pull the bucket-level baseline stats — n, mean PnL, win rate.
fn bucket_stats(rows: &[crate::db::CallOutcome]) -> serde_json::Value {
    let n = rows.len() as i64;
    let pnls: Vec<f64> = rows.iter().filter_map(|r| r.pnl_pct).collect();
    let mean_pnl = if pnls.is_empty() {
        None
    } else {
        Some(pnls.iter().sum::<f64>() / pnls.len() as f64)
    };
    let wins = rows.iter().filter(|r| r.status == "withdrew").count() as i64;
    let win_rate = if n > 0 {
        100.0 * wins as f64 / n as f64
    } else {
        0.0
    };
    serde_json::json!({
        "n": n,
        "mean_pnl_pct": mean_pnl.map(round2),
        "win_rate_pct": round2(win_rate),
    })
}

/// Decide if a closed call would still pass under a candidate value
/// for `field`. Direction depends on the field semantic:
///   - min_X: pass when call's value ≥ candidate
///   - max_X: pass when call's value ≤ candidate
fn call_passes(call: &crate::db::CallOutcome, field: &str, candidate_str: &str) -> bool {
    match field {
        "min_effective_confidence" => candidate_str
            .parse::<i32>()
            .map_or(false, |c| call.confidence >= c),
        "max_top_holder_pct" => candidate_str
            .parse::<f64>()
            .map_or(false, |c| call.entry_top_holder_pct <= c),
        "min_liquidity_usd" => candidate_str
            .parse::<f64>()
            .map_or(false, |c| call.entry_liquidity_usd >= c),
        _ => false,
    }
}

/// Run one candidate through the (sorted-by-called_at) universe; emit
/// the validator-shaped evidence_json plus summary stats. Returns
/// `{ok:false, error: "insufficient_passing_n"}` when the proposed
/// gate would leave fewer than EVIDENCE_FLOOR / 2 calls — sweep can
/// still surface this to the agent as "we tried but it's too tight".
fn evaluate_candidate(
    sorted: &[crate::db::CallOutcome],
    field: &str,
    candidate_str: &str,
    holdout_cutoff: i64,
) -> serde_json::Value {
    let baseline = bucket_stats(sorted);
    let cur_pnl = baseline
        .get("mean_pnl_pct")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let cur_n = baseline.get("n").and_then(|v| v.as_i64()).unwrap_or(0);

    let passing: Vec<&crate::db::CallOutcome> = sorted
        .iter()
        .filter(|c| call_passes(c, field, candidate_str))
        .collect();
    let rejected: Vec<&crate::db::CallOutcome> = sorted
        .iter()
        .filter(|c| !call_passes(c, field, candidate_str))
        .collect();

    let prop_pnls: Vec<f64> = passing.iter().filter_map(|r| r.pnl_pct).collect();
    let prop_mean = if prop_pnls.is_empty() {
        None
    } else {
        Some(prop_pnls.iter().sum::<f64>() / prop_pnls.len() as f64)
    };
    let rej_pnls: Vec<f64> = rejected.iter().filter_map(|r| r.pnl_pct).collect();
    let rej_mean = if rej_pnls.is_empty() {
        None
    } else {
        Some(rej_pnls.iter().sum::<f64>() / rej_pnls.len() as f64)
    };
    let prop_wins = passing.iter().filter(|r| r.status == "withdrew").count() as i64;
    let prop_win_rate = if !passing.is_empty() {
        100.0 * prop_wins as f64 / passing.len() as f64
    } else {
        0.0
    };

    // Holdout = passing calls with called_at >= cutoff.
    let holdout: Vec<&crate::db::CallOutcome> = passing
        .iter()
        .copied()
        .filter(|c| c.called_at >= holdout_cutoff)
        .collect();
    let holdout_pnls: Vec<f64> = holdout.iter().filter_map(|r| r.pnl_pct).collect();
    let holdout_mean = if holdout_pnls.is_empty() {
        None
    } else {
        Some(holdout_pnls.iter().sum::<f64>() / holdout_pnls.len() as f64)
    };

    let effect = match prop_mean {
        Some(p) => p - cur_pnl,
        None => 0.0,
    };

    let evidence = serde_json::json!({
        "current": {
            "n": cur_n,
            "mean_pnl_pct": baseline.get("mean_pnl_pct").cloned().unwrap_or(serde_json::Value::Null),
            "win_rate_pct": baseline.get("win_rate_pct").cloned().unwrap_or(serde_json::Value::Null),
        },
        "proposed": {
            "n": passing.len(),
            "mean_pnl_pct": prop_mean.map(round2),
            "win_rate_pct": round2(prop_win_rate),
        },
        "holdout": {
            "n": holdout.len(),
            "mean_pnl_pct": holdout_mean.map(round2),
            "cutoff": holdout_cutoff,
        },
        "method": format!("sweep_threshold field={} cand={}", field, candidate_str),
    });

    serde_json::json!({
        "ok": true,
        "value": candidate_str,
        "n_passing": passing.len(),
        "n_rejected": rejected.len(),
        "mean_pnl_passing_pct": prop_mean.map(round2),
        "mean_pnl_rejected_pct": rej_mean.map(round2),
        "win_rate_passing_pct": round2(prop_win_rate),
        "effect_vs_baseline_pct": round2(effect),
        "holdout": {
            "n": holdout.len(),
            "mean_pnl_pct": holdout_mean.map(round2),
        },
        "evidence_json": evidence.to_string(),
    })
}

fn validate_field_scope(field: &str, scope: &str) -> Result<(), String> {
    let per_class_ok = matches!(
        scope,
        "global" | "class:STAIRCASE" | "class:GRINDER" | "class:SPRING"
    );
    let global_only_ok = scope == "global";
    match field {
        "min_effective_confidence" | "max_top_holder_pct" => {
            if !per_class_ok {
                return Err(format!(
                    "field '{}' supports scope 'global' or 'class:STAIRCASE|GRINDER|SPRING' (got '{}')",
                    field, scope
                ));
            }
        }
        "min_liquidity_usd" | "min_volume_24h_usd" | "min_token_age_secs" => {
            if !global_only_ok {
                return Err(format!(
                    "field '{}' supports scope 'global' only (got '{}')",
                    field, scope
                ));
            }
        }
        other => {
            return Err(format!(
                "field '{}' is not in the tunable allow-list. Allowed: \
                 min_effective_confidence, max_top_holder_pct, \
                 min_liquidity_usd, min_volume_24h_usd, min_token_age_secs",
                other
            ));
        }
    }
    Ok(())
}

/// Validates that `value` parses to the expected type for `field`. The
/// agent passes everything as strings; we coerce here.
fn validate_field_value(field: &str, value: &str) -> Result<(), String> {
    match field {
        "min_effective_confidence" => value
            .parse::<i64>()
            .map_err(|_| format!("'{}' is not a valid integer", value))
            .and_then(|n| {
                if (0..=100).contains(&n) {
                    Ok(())
                } else {
                    Err(format!("expected 0..=100, got {}", n))
                }
            }),
        "max_top_holder_pct" => value
            .parse::<f64>()
            .map_err(|_| format!("'{}' is not a valid number", value))
            .and_then(|x| {
                if (0.0..=100.0).contains(&x) {
                    Ok(())
                } else {
                    Err(format!("expected 0.0..=100.0, got {}", x))
                }
            }),
        "min_liquidity_usd" | "min_volume_24h_usd" => value
            .parse::<f64>()
            .map_err(|_| format!("'{}' is not a valid number", value))
            .and_then(|x| {
                if x >= 0.0 {
                    Ok(())
                } else {
                    Err(format!("expected ≥ 0, got {}", x))
                }
            }),
        "min_token_age_secs" => value
            .parse::<i64>()
            .map_err(|_| format!("'{}' is not a valid integer", value))
            .and_then(|n| {
                if n >= 0 {
                    Ok(())
                } else {
                    Err(format!("expected ≥ 0, got {}", n))
                }
            }),
        _ => Err(format!("no value validator for field '{}'", field)),
    }
}

fn reject_proposal(code: &str, message: &str) -> String {
    serde_json::json!({
        "ok": false,
        "error": code,
        "message": message
    })
    .to_string()
}

fn round2(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}

#[tool_handler]
impl ServerHandler for ExcitonServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::V_2025_03_26,
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            server_info: Implementation {
                name: "exciton".to_string(),
                title: Some("Exciton".to_string()),
                version: env!("CARGO_PKG_VERSION").to_string(),
                description: Some("Collaborative Solana trading intelligence".to_string()),
                icons: None,
                website_url: None,
            },
            instructions: Some(
                "Exciton — collaborative Solana trading intelligence. \
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

/// Async git invocation with a hard 60s timeout. Mirrors the helper in
/// publisher.rs / thought_images.rs — keeps a stalled `git push` from
/// blocking an MCP tool call (and the underlying tokio worker thread).
async fn run_git_with_timeout(args: &[&str]) -> Result<std::process::Output, anyhow::Error> {
    use std::time::Duration;
    let mut cmd = tokio::process::Command::new("git");
    cmd.args(args);
    let fut = cmd.output();
    match tokio::time::timeout(Duration::from_secs(60), fut).await {
        Ok(Ok(out)) => Ok(out),
        Ok(Err(e)) => Err(anyhow::anyhow!("git spawn: {}", e)),
        Err(_) => Err(anyhow::anyhow!("git {} timed out after 60s", args.join(" "))),
    }
}
