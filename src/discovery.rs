use crate::db::Db;
use crate::ingester::RpcRouter;
use crate::signals::{self, TokenAnalysis};
use anyhow::Result;
use std::collections::HashSet;
use std::sync::Arc;

const PUMPFUN_PROGRAM: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const PUMPSWAP_PROGRAM: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
// Don't re-analyze a graduating token more than once per 10 minutes.
const GRADUATION_COOLDOWN_SECS: i64 = 10 * 60;

// Signatures pulled per discovery cycle, and cap on per-cycle full-tx fetches.
// Each full-tx fetch is one extra RPC call; the cap controls spend while still
// giving us enough fresh mints per cycle to keep the watchlist moving.
const SIG_BATCH: usize = 40;
const MAX_TX_FETCHES: usize = 12;

/// Discover new tokens by walking recent Pump.fun program signatures via the
/// RPC router. Provider-agnostic — works against any standard Solana RPC
/// (Alchemy, QuickNode, Helius) because it only uses `getSignaturesForAddress`
/// and `getTransaction`.
pub async fn discover_new_tokens(
    db: &Arc<Db>,
    rpc: &Arc<RpcRouter>,
    limit: usize,
) -> Result<Vec<TokenAnalysis>> {
    let signatures = rpc
        .get_recent_signatures(PUMPFUN_PROGRAM, SIG_BATCH)
        .await?;

    let mut seen_mints: HashSet<String> = HashSet::new();
    let mut fresh_mints: Vec<String> = Vec::new();
    let mut tx_fetches = 0usize;

    for sig in &signatures {
        if fresh_mints.len() >= limit || tx_fetches >= MAX_TX_FETCHES {
            break;
        }
        tx_fetches += 1;
        let mints = match rpc.get_transaction_mints(&sig.signature).await {
            Ok(m) => m,
            Err(e) => {
                tracing::debug!("discovery: skip sig {}: {}", sig.signature, e);
                continue;
            }
        };
        for mint in mints {
            if mint == SOL_MINT || seen_mints.contains(&mint) {
                continue;
            }
            seen_mints.insert(mint.clone());
            match db.get_token(&mint) {
                Ok(Some(_)) => {}
                _ => fresh_mints.push(mint),
            }
            if fresh_mints.len() >= limit {
                break;
            }
        }
    }

    tracing::info!(
        "Discovery: walked {} sigs, {} tx, {} new mints",
        signatures.len(),
        tx_fetches,
        fresh_mints.len()
    );

    let mut analyses: Vec<TokenAnalysis> = Vec::new();
    for mint in &fresh_mints {
        match signals::analyze_token(rpc, mint, Some(db), None).await {
            Ok(analysis) => {
                let _ = db.insert_token(&analysis.address, analysis.confidence.total);
                let _ = db.audit_log(
                    "discovery",
                    "new_token",
                    &format!("{} confidence={}", mint, analysis.confidence.total),
                );
                analyses.push(analysis);
            }
            Err(e) => {
                tracing::warn!("Failed to analyze {}: {}", mint, e);
            }
        }
    }

    analyses.sort_by(|a, b| b.confidence.total.cmp(&a.confidence.total));
    Ok(analyses)
}

/// Walk recent PumpSwap AMM signatures to detect pump.fun graduations.
/// When a mint we already know from pump.fun discovery appears in pumpswap
/// transactions but isn't on the watchlist yet, it just graduated — re-analyze
/// it immediately instead of waiting for Phase 4's 22-minute polling cycle.
///
/// Returns analyses for any graduated mints that are now worth tracking.
/// The caller (scanner) applies should_track_on_watchlist() to decide.
pub async fn check_graduated_tokens(
    db: &Arc<Db>,
    rpc: &Arc<RpcRouter>,
    limit: usize,
) -> Result<Vec<TokenAnalysis>> {
    let signatures = rpc
        .get_recent_signatures(PUMPSWAP_PROGRAM, 30)
        .await?;

    let mut seen_mints: HashSet<String> = HashSet::new();
    let mut graduated: Vec<String> = Vec::new();
    let mut tx_fetches = 0usize;

    for sig in &signatures {
        if graduated.len() >= limit || tx_fetches >= 8 {
            break;
        }
        tx_fetches += 1;
        let mints = match rpc.get_transaction_mints(&sig.signature).await {
            Ok(m) => m,
            Err(e) => {
                tracing::debug!("graduation: skip sig {}: {}", sig.signature, e);
                continue;
            }
        };
        for mint in mints {
            if mint == SOL_MINT || seen_mints.contains(&mint) {
                continue;
            }
            seen_mints.insert(mint.clone());
            // Only re-analyze mints we've already seen on pump.fun that haven't
            // made it to the watchlist yet, and aren't in cooldown.
            match db.is_graduation_candidate(&mint, GRADUATION_COOLDOWN_SECS) {
                Ok(true) => {
                    graduated.push(mint);
                }
                _ => {}
            }
            if graduated.len() >= limit {
                break;
            }
        }
    }

    if !graduated.is_empty() {
        tracing::info!(
            "graduation: {} known mints active on pumpswap, re-analyzing",
            graduated.len()
        );
    }

    let mut analyses: Vec<TokenAnalysis> = Vec::new();
    for mint in &graduated {
        match signals::analyze_token(rpc, mint, Some(db), None).await {
            Ok(analysis) => {
                let _ = db.insert_token(&analysis.address, analysis.confidence.total);
                analyses.push(analysis);
            }
            Err(e) => {
                tracing::warn!("graduation: analyze_token {} failed: {}", mint, e);
            }
        }
    }

    Ok(analyses)
}
