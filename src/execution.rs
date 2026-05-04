use anyhow::{bail, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use reqwest::Client;
use serde::{Deserialize, Serialize};
// `solana_sdk::system_instruction` is deprecated in favour of the
// `solana_system_interface` crate; migration would require adding that
// dependency. Scoped allow until the SDK upgrade lands as one PR.
#[allow(deprecated)]
use solana_sdk::{
    hash::Hash,
    message::Message,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    system_instruction,
    transaction::{Transaction, VersionedTransaction},
};
use std::str::FromStr;
use std::sync::Arc;

use crate::db::Db;
use crate::ingester::RpcRouter;

// ── Constants ─────────────────────────────────────────────────────────────────

pub const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

const JUPITER_QUOTE_URL: &str = "https://quote-api.jup.ag/v6/quote";
const JUPITER_SWAP_URL: &str = "https://quote-api.jup.ag/v6/swap";
const JITO_BUNDLE_URL: &str = "https://mainnet.block-engine.jito.labs/api/v1/bundles";

// Rotate through tip accounts to distribute Jito load.
const JITO_TIP_ACCOUNTS: &[&str] = &[
    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
    "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
    "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
    "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
    "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
    "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
    "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
];

// ── Keypair loading ───────────────────────────────────────────────────────────

/// Load the trading keypair from EXCITON_PRIVATE_KEY (base58 64-byte secret key).
pub fn load_keypair() -> Result<Keypair> {
    let raw = std::env::var("EXCITON_PRIVATE_KEY")
        .context("EXCITON_PRIVATE_KEY not set — fund a wallet and export its private key")?;
    let bytes = bs58::decode(raw.trim())
        .into_vec()
        .context("EXCITON_PRIVATE_KEY: not valid base58")?;
    Keypair::try_from(bytes.as_slice())
        .context("EXCITON_PRIVATE_KEY: invalid keypair — expected 64-byte secret key")
}

// ── Jupiter types ─────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QuoteResponse {
    pub in_amount: String,
    pub out_amount: String,
    pub price_impact_pct: String,
    #[serde(flatten)]
    pub extra: serde_json::Value,
}

impl QuoteResponse {
    pub fn out_amount_u64(&self) -> u64 {
        self.out_amount.parse().unwrap_or(0)
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SwapBody<'a> {
    user_public_key: &'a str,
    quote_response: &'a QuoteResponse,
    wrap_and_unwrap_sol: bool,
    dynamic_compute_unit_limit: bool,
    prioritization_fee_lamports: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SwapResponse {
    swap_transaction: String,
    #[allow(dead_code)]
    last_valid_block_height: u64,
}

// ── Result type ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct TradeResult {
    pub signature: String,
    pub side: String,
    pub mint: String,
    pub sol_ui: f64,
    pub token_ui: f64,
    pub price_usd: f64,
    pub submitted_via: String,
}

// ── Open position ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct Position {
    pub mint: String,
    pub tokens_held: f64,
    pub sol_in: f64,
    pub avg_buy_price_sol: f64,
}

/// Derive open positions from the wallet_ledger for a given wallet address.
/// A position is open when total tokens bought > total tokens sold (net > 0).
pub fn open_positions(db: &Db, wallet: &str) -> Vec<Position> {
    let Ok(basis) = db.get_wallet_cost_basis(wallet) else {
        return vec![];
    };
    basis
        .into_iter()
        .filter_map(|(mint, bought_tok, sol_in, sold_tok, _sol_out)| {
            let held = bought_tok - sold_tok;
            if held < 1.0 || sol_in < 0.0001 {
                return None;
            }
            let avg = if bought_tok > 0.0 {
                sol_in / bought_tok
            } else {
                0.0
            };
            Some(Position {
                mint,
                tokens_held: held,
                sol_in,
                avg_buy_price_sol: avg,
            })
        })
        .collect()
}

// ── Quote ─────────────────────────────────────────────────────────────────────

/// Fetch a Jupiter v6 swap quote.
/// For buys:  input_mint = SOL_MINT, output_mint = token, amount = lamports
/// For sells: input_mint = token,    output_mint = SOL_MINT, amount = token base units
pub async fn get_quote(
    http: &Client,
    input_mint: &str,
    output_mint: &str,
    amount: u64,
    slippage_bps: u16,
) -> Result<QuoteResponse> {
    let url = format!(
        "{}?inputMint={}&outputMint={}&amount={}&slippageBps={}&onlyDirectRoutes=false",
        JUPITER_QUOTE_URL, input_mint, output_mint, amount, slippage_bps
    );
    let resp = http
        .get(&url)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
        .context("Jupiter quote: request failed")?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        bail!("Jupiter quote {}: {}", status, body);
    }
    resp.json::<QuoteResponse>()
        .await
        .context("Jupiter quote: response parse failed")
}

// ── Sign + submit ─────────────────────────────────────────────────────────────

/// Full trade: request swap tx from Jupiter, sign, submit via Jito (with direct
/// RPC fallback), then record in wallet_ledger. Returns the tx signature.
#[allow(clippy::too_many_arguments)]
pub async fn execute_swap(
    http: &Client,
    rpc: &Arc<RpcRouter>,
    db: &Arc<Db>,
    keypair: &Keypair,
    quote: &QuoteResponse,
    mint: &str,
    side: &str,
    sol_ui: f64,
    token_ui: f64,
    price_usd: f64,
    mcap_usd: f64,
    priority_fee_lamports: u64,
    jito_tip_lamports: u64,
) -> Result<TradeResult> {
    let wallet_pk_str = keypair.pubkey().to_string();

    // 1. Request swap transaction from Jupiter
    let body = SwapBody {
        user_public_key: &wallet_pk_str,
        quote_response: quote,
        wrap_and_unwrap_sol: true,
        dynamic_compute_unit_limit: true,
        prioritization_fee_lamports: priority_fee_lamports,
    };
    let swap_resp = http
        .post(JUPITER_SWAP_URL)
        .json(&body)
        .timeout(std::time::Duration::from_secs(15))
        .send()
        .await
        .context("Jupiter swap: request failed")?;
    if !swap_resp.status().is_success() {
        let status = swap_resp.status();
        let text = swap_resp.text().await.unwrap_or_default();
        bail!("Jupiter swap {}: {}", status, text);
    }
    let swap_data: SwapResponse = swap_resp
        .json()
        .await
        .context("Jupiter swap: response parse failed")?;

    // 2. Deserialize the base64 VersionedTransaction
    let tx_bytes = STANDARD
        .decode(&swap_data.swap_transaction)
        .context("swap tx: base64 decode failed")?;
    let mut swap_tx: VersionedTransaction =
        bincode::deserialize(&tx_bytes).context("swap tx: deserialize failed")?;

    // 3. Sign — Jupiter sets the wallet as fee payer (signatures[0])
    let blockhash: Hash = *swap_tx.message.recent_blockhash();
    let msg_bytes = swap_tx.message.serialize();
    let wallet_sig = keypair
        .try_sign_message(&msg_bytes)
        .map_err(|e| anyhow::anyhow!("signing failed: {}", e))?;
    if swap_tx.signatures.is_empty() {
        bail!("swap tx has no signature slots");
    }
    swap_tx.signatures[0] = wallet_sig;

    // 4. Build Jito tip transfer using the same blockhash
    let tip_idx = (chrono::Utc::now().timestamp_subsec_nanos() as usize) % JITO_TIP_ACCOUNTS.len();
    let tip_account = Pubkey::from_str(JITO_TIP_ACCOUNTS[tip_idx])?;
    let wallet_pk = keypair.pubkey();
    let tip_ix = system_instruction::transfer(&wallet_pk, &tip_account, jito_tip_lamports);
    let tip_msg = Message::new_with_blockhash(&[tip_ix], Some(&wallet_pk), &blockhash);
    let mut tip_tx = Transaction::new_unsigned(tip_msg);
    tip_tx.sign(&[keypair], blockhash);

    // 5. Serialize both for the Jito bundle (base58 — Jito's default encoding)
    let swap_b58 = bs58::encode(bincode::serialize(&swap_tx).context("swap tx: serialize")?)
        .into_string();
    let tip_b58 = bs58::encode(bincode::serialize(&tip_tx).context("tip tx: serialize")?)
        .into_string();

    // 6. Submit to Jito block engine; fall back to direct RPC on any failure
    let jito_payload = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "sendBundle",
        "params": [[swap_b58, tip_b58]]
    });

    let submitted_via = match http
        .post(JITO_BUNDLE_URL)
        .json(&jito_payload)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => {
            tracing::info!("jito bundle accepted sig={}", wallet_sig);
            "jito".to_string()
        }
        Ok(r) => {
            let body = r.text().await.unwrap_or_default();
            tracing::warn!("jito bundle rejected ({}), falling back to rpc", body);
            rpc.send_versioned_transaction(&swap_tx)
                .await
                .context("RPC fallback: send failed")?;
            "rpc-fallback".to_string()
        }
        Err(e) => {
            tracing::warn!("jito unreachable ({}), falling back to rpc", e);
            rpc.send_versioned_transaction(&swap_tx)
                .await
                .context("RPC fallback: send failed")?;
            "rpc-fallback".to_string()
        }
    };

    // 7. Record in wallet_ledger (idempotent by signature)
    let sig_str = wallet_sig.to_string();
    let ts = chrono::Utc::now().timestamp();
    let _ = db.upsert_wallet_trade(
        &sig_str,
        &wallet_pk_str,
        mint,
        side,
        token_ui,
        sol_ui,
        price_usd,
        mcap_usd,
        ts,
    );

    tracing::info!(
        "trade: {} {} {:.4} SOL / {:.2} tokens @ ${:.6} via={} sig={}",
        side, mint, sol_ui, token_ui, price_usd, submitted_via, sig_str
    );

    Ok(TradeResult {
        signature: sig_str,
        side: side.to_string(),
        mint: mint.to_string(),
        sol_ui,
        token_ui,
        price_usd,
        submitted_via,
    })
}

// ── Convenience: full buy/sell wrappers ───────────────────────────────────────

/// Execute a buy: swap `sol_amount` SOL for the given token mint.
pub async fn buy(
    http: &Client,
    rpc: &Arc<RpcRouter>,
    db: &Arc<Db>,
    keypair: &Keypair,
    mint: &str,
    sol_amount: f64,
    slippage_bps: u16,
    priority_fee_lamports: u64,
    jito_tip_lamports: u64,
    price_usd: f64,
    mcap_usd: f64,
) -> Result<TradeResult> {
    let lamports = (sol_amount * 1_000_000_000.0) as u64;
    let quote = get_quote(http, SOL_MINT, mint, lamports, slippage_bps).await?;
    let out_raw = quote.out_amount_u64();

    // We don't know the token decimals until after the swap, so use out_amount
    // as a proxy (most pump.fun tokens have 6 decimals → divide by 1e6).
    // Caller should reconcile against actual balance change for accounting.
    let token_ui_estimate = out_raw as f64 / 1_000_000.0;

    execute_swap(
        http,
        rpc,
        db,
        keypair,
        &quote,
        mint,
        "buy",
        sol_amount,
        token_ui_estimate,
        price_usd,
        mcap_usd,
        priority_fee_lamports,
        jito_tip_lamports,
    )
    .await
}

/// Execute a sell: swap `token_amount_ui` tokens back to SOL.
/// `decimals` is needed to convert the UI amount to base units (usually 6).
pub async fn sell(
    http: &Client,
    rpc: &Arc<RpcRouter>,
    db: &Arc<Db>,
    keypair: &Keypair,
    mint: &str,
    token_amount_ui: f64,
    decimals: u8,
    slippage_bps: u16,
    priority_fee_lamports: u64,
    jito_tip_lamports: u64,
    price_usd: f64,
    mcap_usd: f64,
) -> Result<TradeResult> {
    let base_units = (token_amount_ui * 10_f64.powi(decimals as i32)) as u64;
    let quote = get_quote(http, mint, SOL_MINT, base_units, slippage_bps).await?;
    let sol_out = quote.out_amount_u64() as f64 / 1_000_000_000.0;

    execute_swap(
        http,
        rpc,
        db,
        keypair,
        &quote,
        mint,
        "sell",
        sol_out,
        token_amount_ui,
        price_usd,
        mcap_usd,
        priority_fee_lamports,
        jito_tip_lamports,
    )
    .await
}

// ── Shared execution context ──────────────────────────────────────────────────
//
// Single Arc-clonable bundle carried by both Notifier (buy on call-fire)
// and BackgroundScanner (sell on settle-decision). When None at the
// component level, that component stays paper-only — buys/sells aren't
// spawned. Construction happens once at boot in main.rs after the
// keypair loads from EXCITON_PRIVATE_KEY; absence of the env var keeps
// exciton in safe paper mode.

pub struct ExecutionCtx {
    pub db: Arc<Db>,
    pub rpc: Arc<RpcRouter>,
    pub http: Client,
    pub keypair: Keypair,
    pub cfg: crate::config::ExecutionConfig,
    pub priority_fee_lamports: u64,
    pub jito_tip_lamports: u64,
    /// Wallet pubkey as a string for balance queries.
    pub wallet_pubkey: String,
}

impl std::fmt::Debug for ExecutionCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutionCtx")
            .field("enabled", &self.cfg.enabled)
            .field("wallet_pubkey", &self.wallet_pubkey)
            .field("priority_fee_lamports", &self.priority_fee_lamports)
            .field("jito_tip_lamports", &self.jito_tip_lamports)
            .finish()
    }
}

impl ExecutionCtx {
    /// Build from boot config. Loads EXCITON_PRIVATE_KEY env var; returns
    /// Err when missing OR when the secret is malformed. Caller decides
    /// whether to fail boot or continue in paper mode.
    pub fn from_env(
        db: Arc<Db>,
        rpc: Arc<RpcRouter>,
        cfg: crate::config::ExecutionConfig,
        priority_fee_lamports: u64,
        jito_tip_lamports: u64,
    ) -> Result<Self> {
        let keypair = load_keypair()?;
        let wallet_pubkey = keypair.pubkey().to_string();
        let http = Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .user_agent("exciton/0.1")
            .build()?;
        Ok(Self {
            db,
            rpc,
            http,
            keypair,
            cfg,
            priority_fee_lamports,
            jito_tip_lamports,
            wallet_pubkey,
        })
    }

    /// Live SOL balance for the trading wallet. Wraps RPC in a tight
    /// timeout so a degraded fleet can't block trade decisions.
    pub async fn wallet_sol_balance(&self) -> Result<f64> {
        let lamports = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            self.rpc.get_balance(&self.wallet_pubkey),
        )
        .await
        .map_err(|_| anyhow::anyhow!("get_balance timed out"))??;
        Ok(lamports as f64 / 1_000_000_000.0)
    }
}

// ── Call-aware adaptive wrappers ──────────────────────────────────────────────
//
// Higher-level wrappers that bind execution to a specific call_id, enforce the
// adaptive sizing + daily/concurrent budgets, and write the result back to the
// `calls` row. Used by notifier::process_token (buy on call-fire) and
// scanner::settle_calls (sell on outcome-decision).
//
// All single-flight: re-entry on the same call_id is a no-op via DB-level
// guards (mark_buy_attempt unique-flight, sell_signature null-check). Safe
// across container restarts: state lives in the calls row.

/// Pump.fun + most pumpswap-graduated tokens use 6 decimals. Hard-coded
/// for now since `metadata.rs` doesn't expose decimals; if a non-pump
/// token enters the call universe we'd need to pull decimals from the
/// mint account.
const DEFAULT_TOKEN_DECIMALS: u8 = 6;

/// Compute the SOL size for a bucket given current wallet balance.
/// Returns Ok(amount) when within all caps, Err with reason otherwise.
pub fn derive_position_size_sol(
    cfg: &crate::config::ExecutionConfig,
    horizon_tag: &str,
    wallet_sol_balance: f64,
    open_positions: i64,
    daily_buy_sol_so_far: f64,
) -> Result<f64> {
    if !cfg.enabled {
        bail!("execution disabled");
    }
    if open_positions >= cfg.max_simultaneous_positions {
        bail!(
            "max_simultaneous_positions reached ({}/{})",
            open_positions,
            cfg.max_simultaneous_positions
        );
    }
    if daily_buy_sol_so_far >= cfg.max_daily_position_sol {
        bail!(
            "max_daily_position_sol reached ({:.4}/{:.4})",
            daily_buy_sol_so_far,
            cfg.max_daily_position_sol
        );
    }
    let pct = cfg.size_pct_for_horizon(horizon_tag);
    let raw = wallet_sol_balance * pct / 100.0;
    let capped_daily = (cfg.max_daily_position_sol - daily_buy_sol_so_far).max(0.0);
    let size = raw.min(capped_daily);
    if size < cfg.min_trade_sol {
        bail!(
            "size {:.6} SOL below min_trade_sol {:.6} (wallet {:.4} × {:.1}%)",
            size,
            cfg.min_trade_sol,
            wallet_sol_balance,
            pct
        );
    }
    Ok(size)
}

/// Execute a buy bound to a call row. Idempotent: re-entry while a buy is
/// already attempted/recorded is a no-op. Records buy_signature/sol_spent/
/// token_received on success; records buy_failed_reason + voids the call
/// on failure.
#[allow(clippy::too_many_arguments)]
pub async fn execute_buy_for_call(
    http: &Client,
    rpc: &Arc<RpcRouter>,
    db: &Arc<Db>,
    keypair: &Keypair,
    call_id: i64,
    mint: &str,
    sol_amount: f64,
    slippage_bps: u16,
    priority_fee_lamports: u64,
    jito_tip_lamports: u64,
    price_usd: f64,
    mcap_usd: f64,
) {
    let now = chrono::Utc::now().timestamp();
    // Single-flight: only one task can take this call's buy slot.
    match db.mark_buy_attempt(call_id, now) {
        Ok(true) => {}
        Ok(false) => {
            tracing::debug!("execute_buy_for_call: call {} already attempted", call_id);
            return;
        }
        Err(e) => {
            tracing::warn!("execute_buy_for_call: mark_buy_attempt failed: {}", e);
            return;
        }
    }
    match buy(
        http,
        rpc,
        db,
        keypair,
        mint,
        sol_amount,
        slippage_bps,
        priority_fee_lamports,
        jito_tip_lamports,
        price_usd,
        mcap_usd,
    )
    .await
    {
        Ok(res) => {
            if let Err(e) = db.record_buy(call_id, &res.signature, res.sol_ui, res.token_ui) {
                tracing::warn!("record_buy failed for call {}: {}", call_id, e);
            } else {
                tracing::info!(
                    "execute_buy_for_call: call {} BUY filled {:.4} SOL → {:.2} tokens sig={}",
                    call_id, res.sol_ui, res.token_ui, res.signature
                );
            }
        }
        Err(e) => {
            let reason = format!("{}", e).chars().take(180).collect::<String>();
            if let Err(db_err) = db.record_buy_failure(call_id, &reason) {
                tracing::warn!("record_buy_failure failed for call {}: {}", call_id, db_err);
            }
            tracing::warn!("execute_buy_for_call: call {} BUY failed: {}", call_id, reason);
        }
    }
}

/// Execute a sell bound to a call row. Idempotent: re-entry while sell already
/// recorded is a no-op. Increments sell_attempt_count on failure for the
/// retry-cap logic in settle.
#[allow(clippy::too_many_arguments)]
pub async fn execute_sell_for_call(
    http: &Client,
    rpc: &Arc<RpcRouter>,
    db: &Arc<Db>,
    keypair: &Keypair,
    call_id: i64,
    mint: &str,
    token_amount_ui: f64,
    slippage_bps: u16,
    priority_fee_lamports: u64,
    jito_tip_lamports: u64,
    price_usd: f64,
    mcap_usd: f64,
) -> Result<()> {
    if db.call_has_sell(call_id).unwrap_or(false) {
        return Ok(());
    }
    match sell(
        http,
        rpc,
        db,
        keypair,
        mint,
        token_amount_ui,
        DEFAULT_TOKEN_DECIMALS,
        slippage_bps,
        priority_fee_lamports,
        jito_tip_lamports,
        price_usd,
        mcap_usd,
    )
    .await
    {
        Ok(res) => {
            if let Err(e) = db.record_sell(call_id, &res.signature, res.token_ui, res.sol_ui) {
                tracing::warn!("record_sell failed for call {}: {}", call_id, e);
            } else {
                tracing::info!(
                    "execute_sell_for_call: call {} SELL filled {:.2} tokens → {:.4} SOL sig={}",
                    call_id, res.token_ui, res.sol_ui, res.signature
                );
            }
            Ok(())
        }
        Err(e) => {
            let reason = format!("{}", e).chars().take(180).collect::<String>();
            let _ = db.record_sell_failure(call_id, &reason);
            tracing::warn!("execute_sell_for_call: call {} SELL failed: {}", call_id, reason);
            Err(e)
        }
    }
}
