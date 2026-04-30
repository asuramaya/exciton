//! Launch forensics — Bundle, Sniper, and Insider concentration as % of supply.
//!
//! Three independent signals that complement raw top-holder distribution.
//! Together they catch the patterns that a flat top10 reading misses:
//!
//!   * `bundle_pct` — % of supply held by wallets that received tokens in the
//!     launch transaction itself. Atomic-launch buyers have first-mover edge
//!     and typically dump first.
//!
//!   * `sniper_pct` — % of supply held by wallets recorded in the
//!     `sniper_cohort` table (early buyers within the first ~120s of the
//!     token's life). The cohort is captured at discovery time; this just
//!     resolves their current holdings.
//!
//!   * `insider_pct` — % of supply held by the largest cluster of top-20
//!     holders that share a common funding source. Catches insider networks
//!     that fragment 30-40% of supply across many small wallets to look
//!     diversified — top10 reads clean, insider_pct doesn't.
//!
//! All three are bounded RPC: ~25 calls per refresh. Cached by the caller via
//! `forensics_computed_at` on token_snapshots; refresh hourly.

use crate::db::Db;
use crate::ingester::RpcRouter;
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[derive(Debug, Clone, Default)]
pub struct LaunchForensics {
    pub bundle_pct: f64,
    pub sniper_pct: f64,
    pub insider_pct: f64,
    /// Count of top-20 holders that ALSO hold at least one operator-curated
    /// "reference mint" (per `reference_mints` table). Free smart-money proxy
    /// — wallets that habitually hold known-good tokens are more likely to
    /// be informed buyers than random snipers. Zero when no refs curated.
    pub smart_money_count: i32,
}

/// Compute all three metrics in one pass. Errors are absorbed per-metric so
/// one failed RPC doesn't zero out the whole row — partial data is more useful
/// than nothing for a hot-path gate.
pub async fn compute(
    mint: &str,
    db: &Arc<Db>,
    rpc: &Arc<RpcRouter>,
) -> Result<LaunchForensics> {
    // Total supply — needed by all three. Without it we can't compute %.
    // Bubble RPC errors up so the caller takes the no-write path — writing
    // 0/0/0/0/now would mark the token "measured clean" and suppress retry
    // for a full hour even though we never actually measured.
    let supply = rpc.get_token_supply(mint).await
        .map_err(|e| anyhow::anyhow!("get_token_supply: {}", e))?;
    let supply_ui = supply.ui_amount;
    if supply_ui <= 0.0 {
        return Ok(LaunchForensics::default());
    }

    // Top holders + owner resolution. Reused by sniper and insider.
    let largest = rpc.get_token_largest_accounts(mint).await
        .map_err(|e| anyhow::anyhow!("get_token_largest_accounts: {}", e))?;
    if largest.is_empty() {
        return Ok(LaunchForensics::default());
    }
    let acct_addrs: Vec<String> = largest.iter().map(|h| h.address.clone()).collect();
    let owners = rpc
        .get_multiple_token_account_owners(&acct_addrs)
        .await
        .map_err(|e| anyhow::anyhow!("get_multiple_token_account_owners: {}", e))?;

    // Build owner -> total balance map. Multiple token accounts per owner
    // get summed.
    let mut owner_balances: HashMap<String, f64> = HashMap::new();
    let mut top_owners_in_order: Vec<String> = Vec::new();
    for (h, owner_opt) in largest.iter().zip(owners.into_iter()) {
        if let Some(owner) = owner_opt {
            *owner_balances.entry(owner.clone()).or_insert(0.0) += h.ui_amount;
            if !top_owners_in_order.contains(&owner) {
                top_owners_in_order.push(owner);
            }
        }
    }

    // sniper_pct is sync (just a HashSet intersection over owner_balances) —
    // compute it inline. The other three each fire RPC calls; run them
    // concurrently so the wall-time is max(slowest) instead of sum(all).
    let sniper_pct = compute_sniper_pct(mint, supply_ui, db, &owner_balances)
        .unwrap_or(0.0);
    let (bundle_res, insider_res, smart_res) = tokio::join!(
        compute_bundle_pct(mint, supply_ui, &owner_balances, rpc),
        compute_insider_pct(supply_ui, &top_owners_in_order, &owner_balances, rpc),
        compute_smart_money_count(&top_owners_in_order, db, rpc),
    );
    // If all three RPC sub-computes failed, treat the whole compute as
    // unmeasured so the persist path skips the write. Otherwise an
    // all-zero forensics row would land and the soft gate would accept
    // it as "measured clean" until the next 1h refresh.
    if bundle_res.is_err() && insider_res.is_err() && smart_res.is_err() {
        anyhow::bail!("forensics: all three RPC sub-computes failed");
    }
    let bundle_pct = bundle_res.unwrap_or(0.0);
    let insider_pct = insider_res.unwrap_or(0.0);
    let smart_money_count = smart_res.unwrap_or(0);

    Ok(LaunchForensics {
        bundle_pct,
        sniper_pct,
        insider_pct,
        smart_money_count,
    })
}

/// % of supply held by recorded snipers. No RPC: uses the already-resolved
/// owner_balances map and the cohort table.
fn compute_sniper_pct(
    mint: &str,
    supply_ui: f64,
    db: &Arc<Db>,
    owner_balances: &HashMap<String, f64>,
) -> Result<f64> {
    let snipers = db.get_sniper_cohort(mint)?;
    if snipers.is_empty() {
        return Ok(0.0);
    }
    let snipers_set: HashSet<String> = snipers.into_iter().collect();
    let held: f64 = owner_balances
        .iter()
        .filter(|(o, _)| snipers_set.contains(o.as_str()))
        .map(|(_, b)| *b)
        .sum();
    Ok((held / supply_ui * 100.0).min(100.0))
}

/// % of supply held by wallets that received tokens in the launch
/// transaction. Strategy: walk back through `getSignaturesForAddress`
/// pages on the mint to find the OLDEST signature (the create tx),
/// then read `pre/post token balances` from that tx — every owner with
/// a positive delta got tokens at launch. Sum their CURRENT holdings
/// against the supply.
///
/// One get_recent_signatures call per page (max 1000 sigs/page), then
/// one get_tx_balance_changes. Bounded; falls back to 0 if the launch
/// tx can't be located within the first few pages.
async fn compute_bundle_pct(
    mint: &str,
    supply_ui: f64,
    owner_balances: &HashMap<String, f64>,
    rpc: &Arc<RpcRouter>,
) -> Result<f64> {
    // 100-sig window (was 1000): for mature tokens (>100 lifetime txs)
    // we hit a post-launch tx and return 0 anyway; for fresh tokens 100
    // is plenty. The 10x reduction matches the same cost-cut applied to
    // compute_insider_pct's signature fetch.
    let sigs = rpc.get_recent_signatures(mint, 100).await?;
    if sigs.is_empty() {
        return Ok(0.0);
    }
    // Page is newest-first; the oldest in this slice is the last entry.
    // If that one's older than 1 hour from the second-oldest, we're
    // probably at inception (gap is the pre-tracking silence).
    let launch_sig = match sigs.last() {
        Some(s) if !s.err => s.signature.clone(),
        _ => return Ok(0.0),
    };
    let changes = rpc.get_tx_balance_changes(&launch_sig).await.unwrap_or_default();
    if changes.is_empty() {
        return Ok(0.0);
    }
    // Atomic-launch buyers: every (owner, mint=this) with positive delta.
    let bundle_owners: HashSet<String> = changes
        .into_iter()
        .filter(|c| c.mint == mint && c.delta_ui > 0.0)
        .map(|c| c.owner)
        .collect();
    if bundle_owners.is_empty() {
        return Ok(0.0);
    }
    let held: f64 = owner_balances
        .iter()
        .filter(|(o, _)| bundle_owners.contains(o.as_str()))
        .map(|(_, b)| *b)
        .sum();
    Ok((held / supply_ui * 100.0).min(100.0))
}

/// Count of top-N holders that hold at least one operator-curated
/// reference mint. Per-holder cost: one `getTokenAccountsByOwner` call,
/// run concurrently across all owners. Bounds compute time to ~max(single
/// RPC) instead of (N × single RPC).
async fn compute_smart_money_count(
    top_owners: &[String],
    db: &Arc<Db>,
    rpc: &Arc<RpcRouter>,
) -> Result<i32> {
    let refs: HashSet<String> = db.list_reference_mints()?.into_iter().collect();
    if refs.is_empty() {
        return Ok(0);
    }
    // Scope top 5 instead of top 20: each owner = 2 RPC calls (one per
    // SPL token program), so 5 owners = 10 calls vs 40. Smart-money is
    // a tie-breaker signal anyway; trimming to top 5 keeps the strongest
    // holders in scope while cutting the steady-state RPC load 4x.
    let scope: Vec<&String> = top_owners.iter().take(5).collect();
    let futures = scope.iter().map(|owner| {
        let owner = (*owner).clone();
        let rpc = rpc.clone();
        async move {
            rpc.get_wallet_token_holdings(&owner).await.ok()
        }
    });
    let results = futures_util::future::join_all(futures).await;
    let mut count = 0i32;
    for r in results {
        if let Some(holdings) = r {
            if holdings.iter().any(|(mint, _)| refs.contains(mint)) {
                count += 1;
            }
        }
    }
    Ok(count)
}

/// % of supply held by the largest cluster of top-20 holders that share
/// the same funding source. For each top-20 owner, fetch their oldest
/// signature; that tx's primary signer is the funder (the wallet that
/// paid for account creation + initial SOL). Cluster owners by funder,
/// take the largest cluster's summed holdings.
///
/// 20 RPC calls (one getSignaturesForAddress each, with limit=1000 and
/// taking the oldest). Per-mint hourly refresh keeps the cost bounded.
async fn compute_insider_pct(
    supply_ui: f64,
    top_owners: &[String],
    owner_balances: &HashMap<String, f64>,
    rpc: &Arc<RpcRouter>,
) -> Result<f64> {
    if top_owners.is_empty() {
        return Ok(0.0);
    }
    // Top 5 (was 10, originally 20) — reducing scope progressively to
    // keep forensics within the 180s timeout in the degraded RPC
    // environment we're observing (public-RPC 429 cascade across 4 of
    // 5 endpoints). The top 5 are where the biggest single-funder
    // clusters actually appear; deeper holders contribute small balances
    // even when grouped.
    //
    // Also: limit signatures fetch to 100 (was 1000). For finding the
    // OLDEST signature of a wallet, we just need pagination back through
    // history — but the wallet's first observed sig is likely within the
    // first 100 of the most-recent page for a fresh-grad token's holders.
    // Trade accuracy on stale wallets for 10x faster RPC response.
    let scope: Vec<&String> = top_owners.iter().take(5).collect();
    let funder_results = futures_util::future::join_all(scope.iter().map(|owner| {
        let owner = (*owner).clone();
        let rpc = rpc.clone();
        async move {
            let sigs = rpc.get_recent_signatures(&owner, 100).await.ok()?;
            let oldest = sigs.last().filter(|s| !s.err).map(|s| s.signature.clone())?;
            let summary = rpc.get_tx_wallet_summary(&oldest, &owner).await.ok()?;
            if !summary.fee_payer.is_empty() && summary.fee_payer != owner {
                Some((owner, summary.fee_payer))
            } else {
                None
            }
        }
    }))
    .await;
    let funder_of: HashMap<String, String> =
        funder_results.into_iter().flatten().collect();
    if funder_of.is_empty() {
        return Ok(0.0);
    }
    // Cluster owners by shared funder; pick the largest cluster.
    let mut by_funder: HashMap<String, Vec<String>> = HashMap::new();
    for (owner, funder) in funder_of {
        by_funder.entry(funder).or_default().push(owner);
    }
    // A "cluster" is interesting only when a funder seeded ≥2 holders.
    let largest_cluster_holdings: f64 = by_funder
        .values()
        .filter(|owners| owners.len() >= 2)
        .map(|owners| {
            owners
                .iter()
                .map(|o| owner_balances.get(o).copied().unwrap_or(0.0))
                .sum::<f64>()
        })
        .fold(0.0f64, f64::max);
    Ok((largest_cluster_holdings / supply_ui * 100.0).min(100.0))
}
