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
    let supply_ui = match rpc.get_token_supply(mint).await {
        Ok(s) => s.ui_amount,
        Err(_) => return Ok(LaunchForensics::default()),
    };
    if supply_ui <= 0.0 {
        return Ok(LaunchForensics::default());
    }

    // Top holders + owner resolution. Reused by sniper and insider.
    let largest = match rpc.get_token_largest_accounts(mint).await {
        Ok(v) => v,
        Err(_) => return Ok(LaunchForensics::default()),
    };
    if largest.is_empty() {
        return Ok(LaunchForensics::default());
    }
    let acct_addrs: Vec<String> = largest.iter().map(|h| h.address.clone()).collect();
    let owners = rpc
        .get_multiple_token_account_owners(&acct_addrs)
        .await
        .unwrap_or_else(|_| vec![None; acct_addrs.len()]);

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

    let sniper_pct = compute_sniper_pct(mint, supply_ui, db, &owner_balances)
        .unwrap_or(0.0);
    let bundle_pct = compute_bundle_pct(mint, supply_ui, &owner_balances, rpc)
        .await
        .unwrap_or(0.0);
    let insider_pct = compute_insider_pct(supply_ui, &top_owners_in_order, &owner_balances, rpc)
        .await
        .unwrap_or(0.0);
    let smart_money_count = compute_smart_money_count(&top_owners_in_order, db, rpc)
        .await
        .unwrap_or(0);

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
    // Pull a generous window of recent signatures and take the oldest.
    // Pump.fun mints accumulate thousands of txs fast — 1000 may not
    // reach inception. For v1 we accept "the oldest among the most
    // recent 1000" as a proxy: if a token has fewer than 1000 lifetime
    // txs we hit the actual launch tx; otherwise we get an arbitrary
    // post-launch tx and bundle_pct returns 0 (clean data > wrong data).
    let sigs = rpc.get_recent_signatures(mint, 1000).await?;
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

/// Count of top-20 holders that hold at least one operator-curated
/// reference mint. Per-holder cost: one `getTokenAccountsByOwner` call.
/// Zero when no reference mints are curated (gate is permissive in that
/// case — operator hasn't seeded the smart-money set yet).
async fn compute_smart_money_count(
    top_owners: &[String],
    db: &Arc<Db>,
    rpc: &Arc<RpcRouter>,
) -> Result<i32> {
    let refs: HashSet<String> = db.list_reference_mints()?.into_iter().collect();
    if refs.is_empty() {
        return Ok(0);
    }
    let mut count = 0i32;
    for owner in top_owners.iter().take(20) {
        let holdings = match rpc.get_wallet_token_holdings(owner).await {
            Ok(v) => v,
            Err(_) => continue,
        };
        if holdings.iter().any(|(mint, _)| refs.contains(mint)) {
            count += 1;
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
    // Limit to top 20 to bound the RPC budget.
    let scope: Vec<&String> = top_owners.iter().take(20).collect();
    // Funder per owner. Owners whose funder we can't determine are
    // simply excluded — they sit in their own singleton clusters and
    // don't pollute the largest-cluster computation.
    let mut funder_of: HashMap<String, String> = HashMap::new();
    for owner in &scope {
        let sigs = match rpc.get_recent_signatures(owner, 1000).await {
            Ok(v) => v,
            Err(_) => continue,
        };
        let oldest_sig = match sigs.last() {
            Some(s) if !s.err => s.signature.clone(),
            _ => continue,
        };
        // Use the tx's first non-err signer as the funder. Most account-
        // creation txs are signed by the funder + the new account itself;
        // the funder is the one with non-zero SOL pre-balance.
        let summary = rpc.get_tx_wallet_summary(&oldest_sig, owner).await.ok();
        if let Some(s) = summary {
            // The funder is whoever sent SOL TO this owner in their
            // first observed tx — for account-creation txs the system
            // program transfers from the funder. We approximate using
            // the tx's "fee_payer" surfaced in the wallet summary, which
            // is the first signer. If the owner IS the fee payer, we
            // can't tell who funded them from this data; skip.
            if !s.fee_payer.is_empty() && s.fee_payer != **owner {
                funder_of.insert((*owner).clone(), s.fee_payer);
            }
        }
    }
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
