use crate::config::Config;
use crate::db::Db;
use crate::forecaster::{Forecaster, Regime};
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
    db_writable: bool,
    signal_layers_active: usize,
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
    pub fn new(db: Arc<Db>, config: Config) -> Self {
        Self {
            db,
            config,
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
    /// Scan the market: system health, current regime, top opportunities, active alerts.
    /// Flow: check health -> query forecaster -> present ranked results.
    #[tool]
    fn scan(&self) -> String {
        let _ = self.db.audit_log("claude", "scan", "Market scan requested");

        to_json(&ScanResult {
            healthy: self.is_healthy(),
            regime: Regime::LowActivityGrind.to_string(),
            opportunities: vec![],
            alerts: vec![],
        })
    }

    /// Deep-dive investigation of a token or wallet.
    /// Flow: check existence -> run all signal layers -> pull history -> safety checks -> present full picture.
    #[tool]
    fn inspect(&self, Parameters(params): Parameters<InspectParams>) -> String {
        let _ = self
            .db
            .audit_log("claude", "inspect", &format!("Inspecting {}", params.address));

        let target_type = if params.address.len() > 40 {
            "token"
        } else {
            "wallet"
        };

        to_json(&InspectResult {
            target: params.address,
            target_type: target_type.to_string(),
            safety: vec![],
            signals: vec![],
            risk_rating: "unknown - no data yet, connect RPC to begin analysis".to_string(),
        })
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
    /// Flow: check all components -> positions with live P&L -> exposure vs risk limits -> data freshness.
    #[tool]
    fn status(&self) -> String {
        let _ = self
            .db
            .audit_log("claude", "status", "Status check requested");

        let wallet_key = &self.config.wallet.public_key;
        let message = if wallet_key.is_empty() {
            "No wallet configured. Set wallet.public_key in config.toml".to_string()
        } else {
            format!(
                "Wallet {} has 0 SOL - fund this wallet to begin trading",
                wallet_key
            )
        };

        to_json(&StatusResult {
            system_health: SystemHealth {
                rpc_connected: false,
                db_writable: self.is_healthy(),
                signal_layers_active: 0,
                data_freshness: "No data yet - awaiting RPC connection".to_string(),
            },
            positions: vec![],
            wallet: wallet_key.clone(),
            total_balance_sol: 0.0,
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
            capabilities: ServerCapabilities::builder()
                .enable_tools()
                .build(),
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
