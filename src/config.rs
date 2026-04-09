use anyhow::Result;
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub rpc: RpcConfig,
    pub wallet: WalletConfig,
    pub risk: RiskConfig,
    pub tracking: TrackingConfig,
    pub alerts: AlertConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RpcConfig {
    pub endpoints: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WalletConfig {
    pub public_key: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RiskConfig {
    pub max_position_pct: f64,
    pub default_position_pct: f64,
    pub high_confidence_threshold: u8,
    pub slippage_bps: u16,
    pub priority_fee_lamports: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TrackingConfig {
    pub wallets: Vec<String>,
    pub max_active_tokens: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AlertConfig {
    pub confidence_threshold: u8,
    pub stale_feed_seconds: u64,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let contents = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&contents)?;
        Ok(config)
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            rpc: RpcConfig {
                endpoints: vec!["wss://api.mainnet-beta.solana.com".to_string()],
            },
            wallet: WalletConfig {
                public_key: String::new(),
            },
            risk: RiskConfig {
                max_position_pct: 15.0,
                default_position_pct: 0.5,
                high_confidence_threshold: 80,
                slippage_bps: 100,
                priority_fee_lamports: 10000,
            },
            tracking: TrackingConfig {
                wallets: vec![],
                max_active_tokens: 500,
            },
            alerts: AlertConfig {
                confidence_threshold: 70,
                stale_feed_seconds: 30,
            },
        }
    }
}
