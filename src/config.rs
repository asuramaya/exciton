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
    /// Optional — when absent OR `enabled=false`, photon stays paper-only.
    /// Auto-calls insert rows + post TG cards but never sign trades.
    #[serde(default)]
    pub execution: Option<ExecutionConfig>,
}

/// Trade-execution sizing + safety budget. Master kill switch is `enabled`.
/// When false (or section absent) photon is paper-only and the keypair
/// loaded from `PHOTON_PRIVATE_KEY` is never used by the auto path.
///
/// Sizing is **adaptive**: `*_size_pct` is a percentage of the wallet's
/// current SOL balance at call-fire time. As wallet grows, position size
/// grows proportionally — no manual rebalancing. With $2 wallet at 5%,
/// each call risks ~$0.10 (just above Jupiter's ~0.001 SOL floor).
#[derive(Debug, Clone, Deserialize)]
pub struct ExecutionConfig {
    /// MASTER KILL SWITCH. Default false. Flip to true on the VM in
    /// config.vm.toml when ready to go live.
    #[serde(default)]
    pub enabled: bool,
    /// Bucket A (SHORT default) sizing — % of wallet SOL.
    #[serde(default = "default_size_pct_5")]
    pub bucket_a_size_pct: f64,
    /// Bucket B (MOONSHOT) sizing — % of wallet SOL.
    #[serde(default = "default_size_pct_5")]
    pub bucket_b_size_pct: f64,
    /// LONG-horizon (operator) sizing — % of wallet SOL.
    #[serde(default = "default_size_pct_5")]
    pub long_size_pct: f64,
    /// SCALP sizing if/when re-enabled — % of wallet SOL.
    #[serde(default = "default_size_pct_5")]
    pub scalp_size_pct: f64,
    /// Hard floor on per-trade SOL — Jupiter rejects below ~0.001.
    #[serde(default = "default_min_trade_sol")]
    pub min_trade_sol: f64,
    /// Slippage tolerance (basis points) on Jupiter quotes. Memes
    /// routinely slip 5-10%; below 1000 bps we'll get blocked a lot.
    #[serde(default = "default_slippage_bps")]
    pub slippage_bps: u16,
    /// Max calls with an open position at one time. Settle waits for
    /// sell confirmation before the slot frees up.
    #[serde(default = "default_max_simultaneous")]
    pub max_simultaneous_positions: i64,
    /// Daily SOL spend cap (sum of buy_sol_spent over rolling 24h).
    /// At 5% sizing on $2 this caps roughly 50 trades/day.
    #[serde(default = "default_max_daily_sol")]
    pub max_daily_position_sol: f64,
    /// Buy-confirmation timeout. Beyond this with no signature, the
    /// settle path voids the call.
    #[serde(default = "default_buy_timeout_secs")]
    pub buy_confirm_timeout_secs: i64,
    /// Sell retry cap. After this many failures, settle marks the call
    /// failed for manual operator exit.
    #[serde(default = "default_sell_retry_max")]
    pub sell_retry_max: i32,
}

fn default_size_pct_5() -> f64 { 5.0 }
fn default_min_trade_sol() -> f64 { 0.001 }
fn default_slippage_bps() -> u16 { 1000 }
fn default_max_simultaneous() -> i64 { 5 }
fn default_max_daily_sol() -> f64 { 1.0 }
fn default_buy_timeout_secs() -> i64 { 60 }
fn default_sell_retry_max() -> i32 { 6 }

impl ExecutionConfig {
    /// Pick the size pct for a given horizon string (matches the
    /// horizon::Horizon::tag values + a fallback).
    pub fn size_pct_for_horizon(&self, horizon_tag: &str) -> f64 {
        match horizon_tag {
            "horizon=MOONSHOT" => self.bucket_b_size_pct,
            "horizon=SCALP" => self.scalp_size_pct,
            "horizon=LONG" => self.long_size_pct,
            _ => self.bucket_a_size_pct,
        }
    }
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
    /// Channel ID for the public-facing call channel. Ape-voiced cards
    /// only — entry punchline + chart + verdict-on-close. No numbers
    /// dump, no timeline blockquote.
    pub signals_chat_id: String,
    /// Chat ID for ops heartbeats + hourly monster digest. Also used as
    /// the lounge fallback when `lounge_chat_id` is empty (back-compat).
    pub ops_chat_id: String,
    /// Chat ID for the data-junkie lounge — full structural signal card
    /// (numbers, classification, forensics, timeline) + every per-call
    /// flip mirrors here. When empty, falls back to `ops_chat_id` so
    /// existing single-channel deploys keep working.
    #[serde(default)]
    pub lounge_chat_id: String,
    /// Optional message_id of an "anchor" message that should always
    /// appear at the bottom of `anchor_chat_id`. Telegram only pins to
    /// the TOP of a chat — the magic trick is: after every photon-
    /// originated send to the anchor chat, delete the previous forward
    /// of the anchor and forwardMessage from the source again as a
    /// fresh post. Set to 0 (or omit) to disable.
    ///
    /// Common use: keep a Safeguard "tap to verify" message permanently
    /// visible at the bottom of the public calls channel so new joiners
    /// always see it without having to scroll up.
    #[serde(default)]
    pub anchor_msg_id: i64,
    /// Destination chat for the anchor — where the bump fires after
    /// every photon-originated send. Empty = use `signals_chat_id`
    /// (calls channel — the most common case for a verify gate).
    #[serde(default)]
    pub anchor_chat_id: String,
    /// Source chat for the anchor message. Empty = same as
    /// `anchor_chat_id` (the anchor lives in the channel where it's
    /// being kept-at-bottom — most common case).
    #[serde(default)]
    pub anchor_source_chat: String,

    /// Bot username (no @) for the public bot — used to construct
    /// deep-links on call cards: t.me/<username>?start=call_<address>.
    /// When empty, the [Details] inline-keyboard button is omitted and
    /// the legacy [Chart]/[Solscan]/[Addr] row renders instead.
    #[serde(default)]
    pub public_bot_username: String,

    /// Stale lounge-side anchor msg_id to delete. The bump only tracks
    /// ONE active anchor slot (the destination chat), so any forward
    /// that landed in the lounge before the channel flip is never
    /// cleaned up. Set this to the stale lounge msg_id; on next bump the
    /// magic trick sweeps lounge_chat_id and zeroes the field on success.
    /// Set back to 0 once cleaned (or leave non-zero — successive bumps
    /// will keep no-oping on the missing message).
    #[serde(default)]
    pub stale_lounge_anchor_msg_id: i64,

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

impl RpcConfig {
    /// Extract a Helius API key from the `endpoints` list, if present.
    /// Helius URLs come in shape `https://mainnet.helius-rpc.com/?api-key=<KEY>`.
    /// Returned key powers the wallet_observer (Helius enhanced txns API)
    /// and any future Helius-specific feature without a duplicated config
    /// field. Empty string when no Helius endpoint is configured.
    pub fn helius_api_key(&self) -> String {
        for url in &self.endpoints {
            if url.contains("helius-rpc.com") || url.contains("helius.xyz") {
                if let Some(idx) = url.find("api-key=") {
                    let after = &url[idx + "api-key=".len()..];
                    let key = after.split(|c: char| c == '&' || c == '#').next().unwrap_or("");
                    if !key.is_empty() {
                        return key.to_string();
                    }
                }
            }
        }
        String::new()
    }
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
            execution: None,
        }
    }
}
