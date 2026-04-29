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

/// Load the trading keypair from PHOTON_PRIVATE_KEY (base58 64-byte secret key).
pub fn load_keypair() -> Result<Keypair> {
    let raw = std::env::var("PHOTON_PRIVATE_KEY")
        .context("PHOTON_PRIVATE_KEY not set — fund a wallet and export its private key")?;
    let bytes = bs58::decode(raw.trim())
        .into_vec()
        .context("PHOTON_PRIVATE_KEY: not valid base58")?;
    Keypair::try_from(bytes.as_slice())
        .context("PHOTON_PRIVATE_KEY: invalid keypair — expected 64-byte secret key")
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
