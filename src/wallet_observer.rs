//! Wallet observer — Layer 1 of the self-evolving smart-wallet curation
//! system. When a fresh mint passes a basic viability gate (classifier in
//! a healthy class + age + holder + mcap floors), we walk back its first
//! ~20 buyer wallets via Helius's enhanced-transactions API and persist
//! them in `wallet_observations`. Layer 2 (outcome scoring) joins those
//! observations against `token_snapshots` to compute realized PnL per
//! wallet; layer 3 promotes consistent winners onto a watchlist; layer 4
//! drives forced signal-fires when multiple watched wallets cluster on
//! the same fresh mint.
//!
//! Why selective trace and not passive stream:
//!   - Pumpportal exposes new-token + migration events but NOT a global
//!     trade firehose. Per-mint trade subs would be 1000s of concurrent
//!     channels — pumpportal would throttle/ban.
//!   - Most fresh mints die in 30s. Walking buyers for every one of them
//!     burns RPC for zero signal.
//!   - Promoting only "viable" mints (passed classifier + age + holders
//!     + mcap thresholds) keeps the trace count to ~30-100/day. Each
//!     trace is a single Helius enhanced-txns call (~50 events).
//!
//! Trace shape:
//!   - GET https://api.helius.xyz/v0/addresses/{mint}/transactions
//!         ?api-key=<KEY>&type=SWAP&limit=100
//!   - Helius parses the swap and gives us feePayer, source, native/token
//!     amounts. We treat feePayer as the buyer (signer is the buy
//!     initiator on pump-amm and Raydium swaps).
//!   - Sort ascending by timestamp; keep first 20 distinct payers
//!     observed in BUY direction; record rank, ts, buy_sol.
//!   - Idempotent at the DB layer via UNIQUE(wallet, mint) and the
//!     `wallet_observation_traces` claim row — re-runs no-op cleanly.

use crate::db::Db;
use anyhow::Result;
use std::sync::Arc;

/// Min token age (secs) before we'll trace its early buyers. Below this
/// the buyer set is dominated by the dev wallet + 1-2 launch snipers,
/// which is noise. 90s gives us "first push" plus the initial organic
/// fillers — the cohort we actually want to score.
const MIN_AGE_SECS: i64 = 90;
/// Min market cap floor for promotion. Tokens that haven't even hit $5k
/// are pre-attention; even early buyers don't yet know they're "in".
const MIN_MCAP_USD: f64 = 5_000.0;
/// Min liquidity floor — enough that the buys we observe were against
/// real depth, not a 4-token AMM.
const MIN_LIQ_USD: f64 = 5_000.0;
/// Min holder count — a token with 3 holders has 3 buyers including the
/// dev. We want enough holders that the rank distribution carries
/// meaningful signal.
const MIN_HOLDERS: i64 = 10;
/// Healthy classifications. Anything else is a dead/unsafe shape.
const HEALTHY_CLASSES: &[&str] = &["DEVELOPING", "STAIRCASE", "GRINDER", "SPRING"];
/// Buyer-trace cap. Beyond rank-20 the signal flattens into general
/// momentum followers — those wallets will surface on later fresh
/// promotes if they're consistent alpha.
const MAX_RANK: usize = 20;
/// Drop tiny dust buys at the trace stage. < 0.005 SOL (~$0.50) at
/// $100/SOL is too small to represent intent.
const MIN_BUY_SOL: f64 = 0.005;

/// Decide whether a snapshot's metrics promote the mint into the trace
/// queue. Caller (notifier::process_token) invokes this once per
/// snapshot; the DB-layer `claim_observation_trace` makes the actual
/// trace single-fire across the mint's lifetime.
pub fn should_trace(
    classification: &str,
    age_secs: i64,
    mcap_usd: f64,
    liquidity_usd: f64,
    holder_count: i64,
) -> bool {
    if !HEALTHY_CLASSES.iter().any(|c| *c == classification) {
        return false;
    }
    if age_secs < MIN_AGE_SECS {
        return false;
    }
    if mcap_usd < MIN_MCAP_USD {
        return false;
    }
    if liquidity_usd < MIN_LIQ_USD {
        return false;
    }
    if holder_count < MIN_HOLDERS {
        return false;
    }
    true
}

/// Spawn the trace task. Detached — caller continues on the
/// process_token hot path while the trace runs in the background. No-op
/// when no Helius key is configured, or when this mint already claimed
/// a trace earlier.
pub fn spawn_trace(
    db: Arc<Db>,
    http: reqwest::Client,
    helius_api_key: String,
    mint: String,
) {
    if helius_api_key.is_empty() {
        return;
    }
    if !db.claim_observation_trace(&mint).unwrap_or(false) {
        // Already traced (or another task holds the claim).
        return;
    }
    tokio::spawn(async move {
        match run_trace(&db, &http, &helius_api_key, &mint).await {
            Ok(n) => {
                let _ = db.finalize_observation_trace(&mint, n as i64, true);
                tracing::info!(
                    "wallet_observer: traced {} early buyers for {}",
                    n, mint
                );
            }
            Err(e) => {
                let _ = db.finalize_observation_trace(&mint, 0, false);
                tracing::warn!("wallet_observer: trace failed for {}: {}", mint, e);
            }
        }
    });
}

/// One Helius enhanced-txns call; parse swaps; insert first 20 distinct
/// buyer wallets. Returns the number of buyers persisted.
async fn run_trace(
    db: &Db,
    http: &reqwest::Client,
    api_key: &str,
    mint: &str,
) -> Result<usize> {
    let url = format!(
        "https://api.helius.xyz/v0/addresses/{}/transactions?api-key={}&type=SWAP&limit=100",
        mint, api_key
    );
    let resp = http
        .get(&url)
        .timeout(std::time::Duration::from_secs(15))
        .send()
        .await?;
    let status = resp.status();
    if !status.is_success() {
        anyhow::bail!("helius {} for {}", status, mint);
    }
    let txns: Vec<serde_json::Value> = resp.json().await?;
    if txns.is_empty() {
        return Ok(0);
    }

    // Helius returns newest-first by default; reverse so rank-1 is the
    // earliest buyer (chronological order matches "who got in first").
    let mut sorted = txns;
    sorted.sort_by_key(|t| t.get("timestamp").and_then(|v| v.as_i64()).unwrap_or(0));

    // Walk in chrono order, keeping first observation per wallet.
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut count = 0usize;
    for tx in &sorted {
        if count >= MAX_RANK {
            break;
        }
        let ts = tx.get("timestamp").and_then(|v| v.as_i64()).unwrap_or(0);
        let buyer = match extract_buyer(tx, mint) {
            Some(b) => b,
            None => continue,
        };
        if !seen.insert(buyer.wallet.clone()) {
            continue;
        }
        if buyer.buy_sol < MIN_BUY_SOL {
            continue;
        }
        let rank = (count + 1) as i64;
        let _ = db.insert_wallet_observation(&buyer.wallet, mint, rank, ts, buyer.buy_sol);
        count += 1;
    }
    Ok(count)
}

struct ExtractedBuy {
    wallet: String,
    buy_sol: f64,
}

/// Heuristic buy-side extraction from a Helius enhanced-txn record.
/// Helius schemas vary by source (PUMP_AMM, RAYDIUM, JUPITER, …) so we
/// look at multiple fields and accept the first reasonable read:
///   1. `events.swap.nativeInput` — feePayer paid SOL → bought tokens
///   2. `nativeTransfers` — first SOL transfer FROM feePayer
///   3. fall back to feePayer + zero amount when shape is unfamiliar
/// Returns None when the txn doesn't look like a buy of this mint.
fn extract_buyer(tx: &serde_json::Value, _mint: &str) -> Option<ExtractedBuy> {
    let fee_payer = tx.get("feePayer").and_then(|v| v.as_str())?.to_string();

    // Path 1: events.swap.nativeInput
    if let Some(native_input) = tx.pointer("/events/swap/nativeInput") {
        if let Some(amt) = native_input.get("amount").and_then(|v| v.as_str()) {
            if let Ok(lamports) = amt.parse::<u64>() {
                let sol = lamports as f64 / 1_000_000_000.0;
                if sol > 0.0 {
                    return Some(ExtractedBuy { wallet: fee_payer, buy_sol: sol });
                }
            }
        }
    }

    // Path 2: nativeTransfers — first transfer where fromUserAccount == feePayer
    if let Some(transfers) = tx.get("nativeTransfers").and_then(|v| v.as_array()) {
        for t in transfers {
            let from = t.get("fromUserAccount").and_then(|v| v.as_str()).unwrap_or("");
            if from != fee_payer {
                continue;
            }
            if let Some(lamports) = t.get("amount").and_then(|v| v.as_i64()) {
                let sol = lamports as f64 / 1_000_000_000.0;
                if sol > 0.0 {
                    return Some(ExtractedBuy { wallet: fee_payer, buy_sol: sol });
                }
            }
        }
    }

    // Path 3: unknown shape — record buyer with zero amount. Layer 2
    // scoring uses pct-from-call which doesn't depend on buy amount.
    Some(ExtractedBuy { wallet: fee_payer, buy_sol: 0.0 })
}
