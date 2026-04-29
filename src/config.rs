// Config fields are populated by TOML deserialization. Some are loaded for
// future use or consumed only by environments that aren't compiled in this
// build (e.g. operator-only knobs). Module-wide allow keeps the noise down.
#![allow(dead_code)]

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
    /// Optional — when absent, the Telegram notifier is fully disabled
    /// and no background posting task is spawned.
    #[serde(default)]
    pub telegram: Option<TelegramConfig>,
    /// Optional — when absent, no data is published to the MadApes.ai repo.
    #[serde(default)]
    pub madapes: Option<MadapesConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MadapesConfig {
    /// Master switch. Defaults to `false` so local runs don't accidentally
    /// push to the public repo.
    #[serde(default)]
    pub enabled: bool,
    /// Absolute path to the MadApes.ai repo checkout on this machine.
    pub repo_path: String,
    /// Publish cadence in seconds. Data snapshots are pushed this often.
    /// Notes are always manual and never touched by this task.
    #[serde(default = "default_publish_interval")]
    pub interval_seconds: u64,
    /// Fallback SOL price (USD) used for total-value math when we can't
    /// fetch a live price. Set conservatively.
    #[serde(default = "default_sol_price_fallback")]
    pub sol_price_fallback_usd: f64,
    /// Recraft API key for generating thought-note images. Optional — when
    /// absent, the image processor is disabled and placeholders remain as
    /// meta-captions only. Keep this server-side; never commit to the
    /// public repo.
    #[serde(default)]
    pub recraft_api_key: String,
    /// How often the image processor scans thoughts/ for new placeholders.
    /// Independent from the data publisher's cadence — images are a rare
    /// event (one per note addition) so a slower tick keeps API calls low.
    #[serde(default = "default_image_interval")]
    pub image_interval_seconds: u64,
}

fn default_publish_interval() -> u64 {
    300
}
fn default_sol_price_fallback() -> f64 {
    150.0
}
fn default_image_interval() -> u64 {
    900
}
fn default_jito_tip() -> u64 {
    100_000
}

#[derive(Debug, Clone, Deserialize)]
pub struct TelegramConfig {
    /// Master switch — defaults to `false` so local runs never post by accident.
    #[serde(default)]
    pub enabled: bool,
    /// Bot token used for channel posts (signals, calls, ops digest).
    /// Supports ${ENV_VAR} expansion so secrets stay out of config.
    pub bot_token: String,
    /// Optional separate bot token for the DM interface. When empty,
    /// `bot_token` is used. Splitting matters when the channel bot is
    /// also bound to other consumers (group webhooks, etc) — long-poll
    /// + sendMessage on the same token from two clients triggers
    /// 409 Conflict on getUpdates. Configure a dedicated bot for DMs
    /// to keep the long-poll exclusive.
    #[serde(default)]
    pub dm_bot_token: String,
    /// Channel ID for signal cards — "we think this makes money" calls.
    /// Verdict evolves in-card; failures stay visible with FAILED header.
    pub signals_chat_id: String,
    /// Chat ID for ops heartbeats + hourly monster digest.
    pub ops_chat_id: String,

    /// Enable the direct-message bot interface (long-poll loop for /commands).
    /// Separate switch from channel posting — can disable DM while keeping
    /// autonomous channel pipeline alive.
    #[serde(default)]
    pub dm_enabled: bool,

    /// Telegram user IDs with admin-tier command access (halt, threshold,
    /// force_signal, stats). Everyone else gets read-only query commands.
    #[serde(default)]
    pub admin_user_ids: Vec<i64>,

    /// Per-user rate limit for the DM bot — commands per 60-second window.
    /// Default: 30. Set to 0 to disable.
    #[serde(default = "default_rate_limit")]
    pub dm_rate_limit_per_minute: i64,

    /// Telegram username (no @) that can invoke /claw. Empty = disabled.
    #[serde(default)]
    pub claw_username: String,

    /// Anthropic API key powering /claw. Supports ${ENV_VAR} expansion.
    #[serde(default)]
    pub anthropic_api_key: String,

    /// OpenAI API key for /claw. Used when anthropic_api_key is empty.
    /// Supports ${ENV_VAR} expansion. Set one of the two — Anthropic is
    /// preferred when both are present.
    #[serde(default)]
    pub openai_api_key: String,

    /// Secret token required for the HTTP /api/claw endpoint.
    /// Empty = web endpoint disabled.
    #[serde(default)]
    pub claw_api_secret: String,

    /// Port for the claw HTTP API server. Default: 8081.
    #[serde(default = "default_claw_port")]
    pub claw_api_port: u16,
}

fn default_claw_port() -> u16 {
    8081
}

fn default_rate_limit() -> i64 {
    30
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
    #[serde(default = "default_jito_tip")]
    pub jito_tip_lamports: u64,
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
                jito_tip_lamports: 100_000,
            },
            tracking: TrackingConfig {
                wallets: vec![],
                max_active_tokens: 200,
            },
            alerts: AlertConfig {
                confidence_threshold: 70,
                stale_feed_seconds: 30,
            },
            telegram: None,
            madapes: None,
        }
    }
}
