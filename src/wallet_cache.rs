//! Wallet snapshot cache — shared between a slow background refresh
//! task and any read sites (publisher, MCP, etc).
//!
//! Architectural rule: read paths NEVER call Solana RPC for our own
//! wallet state. The RPC budget is reserved for the scanner/scout
//! decision loop. This cache holds the last-known-good wallet
//! snapshot; refresh runs on its own slow cadence with a long
//! per-call budget so RPC degradation only causes staleness, never
//! a publisher tick failure.

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

use crate::ingester::RpcRouter;

#[derive(Clone, Debug, Default)]
pub struct WalletSnapshot {
    pub sol_balance: f64,
    /// (mint, balance_human) — same shape as `RpcRouter::get_wallet_token_holdings`.
    pub holdings: Vec<(String, f64)>,
    /// Unix seconds when this snapshot was last successfully refreshed.
    /// Zero means never refreshed (engine just started).
    pub last_updated: i64,
}

pub type SharedWalletCache = Arc<RwLock<WalletSnapshot>>;

pub fn new_cache() -> SharedWalletCache {
    Arc::new(RwLock::new(WalletSnapshot::default()))
}

/// Spawn a background task that refreshes the cache every
/// `interval_secs` (clamped to ≥60s). On RPC failure the cache
/// keeps its prior value — read sites stay unblocked.
pub fn spawn_refresh(
    cache: SharedWalletCache,
    rpc: Arc<RpcRouter>,
    wallet: String,
    interval_secs: u64,
) {
    let interval = Duration::from_secs(interval_secs.max(60));
    tokio::spawn(async move {
        tracing::info!(
            "wallet_cache: refresh loop starting (every {}s)",
            interval.as_secs()
        );
        let mut tick = tokio::time::interval(interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Don't sleep through the first interval — populate cache ASAP.
        loop {
            // Long timeout — this is a background task, not on a critical
            // path. 30s lets an endpoint chain fully walk through 429s
            // and find a healthy provider.
            let bal_fut = tokio::time::timeout(Duration::from_secs(30), rpc.get_balance(&wallet));
            let hold_fut = tokio::time::timeout(
                Duration::from_secs(30),
                rpc.get_wallet_token_holdings(&wallet),
            );
            let (bal_res, hold_res) = tokio::join!(bal_fut, hold_fut);

            let mut snap = cache.write().await;
            let mut updated = false;
            if let Ok(Ok(lamports)) = bal_res {
                snap.sol_balance = lamports as f64 / 1e9;
                updated = true;
            }
            if let Ok(Ok(holdings)) = hold_res {
                snap.holdings = holdings;
                updated = true;
            }
            if updated {
                snap.last_updated = chrono::Utc::now().timestamp();
                tracing::debug!(
                    "wallet_cache: refreshed sol={:.4} holdings={}",
                    snap.sol_balance,
                    snap.holdings.len()
                );
            } else {
                tracing::warn!("wallet_cache: refresh degraded — keeping stale snapshot");
            }
            drop(snap);
            tick.tick().await;
        }
    });
}
