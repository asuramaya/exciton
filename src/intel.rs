use crate::db::{Db, TokenSnapshot};
use crate::ingester::RpcRouter;
use crate::market;
use crate::scout;
use anyhow::Result;
use serde::Serialize;
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

const NULL_ADDRESS: &str = "11111111111111111111111111111111";

#[derive(Debug, Clone, Serialize)]
pub struct SnapshotPoint {
    pub timestamp: i64,
    pub classification: String,
    pub confidence: i32,
    pub top_holder_pct: f64,
    pub top5_pct: f64,
    pub holder_count: i32,
    pub tx_rate: f64,
    pub velocity: f64,
    pub momentum: i32,
    pub distribution: i32,
    pub spring: i32,
    pub price_usd: f64,
    pub mcap_usd: f64,
    pub liquidity_usd: f64,
    pub volume_h1_usd: f64,
    pub buys_h1: i32,
    pub sells_h1: i32,
    pub price_change_h1: f64,
    pub pair_dex: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct HistoryChanges {
    pub price_pct: Option<f64>,
    pub mcap_pct: Option<f64>,
    pub liquidity_pct: Option<f64>,
    pub volume_h1_pct: Option<f64>,
    pub holder_count_delta: i32,
    pub top_holder_pct_delta: f64,
    pub top5_pct_delta: f64,
    pub tx_rate_delta: f64,
    pub momentum_delta: i32,
    pub distribution_delta: i32,
    pub confidence_delta: i32,
}

#[derive(Debug, Clone, Serialize)]
pub struct HistoryRanges {
    pub price_usd: RangeF64,
    pub mcap_usd: RangeF64,
    pub liquidity_usd: RangeF64,
    pub holder_count: RangeI32,
    pub top_holder_pct: RangeF64,
    pub tx_rate: RangeF64,
    pub momentum: RangeI32,
    pub distribution: RangeI32,
    pub confidence: RangeI32,
}

#[derive(Debug, Clone, Serialize)]
pub struct RangeF64 {
    pub min: f64,
    pub max: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct RangeI32 {
    pub min: i32,
    pub max: i32,
}

#[derive(Debug, Clone, Serialize)]
pub struct HistoricalAnalysis {
    pub mint: String,
    pub snapshots_analyzed: usize,
    pub requested_limit: usize,
    pub window_hours: Option<i64>,
    pub first_timestamp: Option<i64>,
    pub latest_timestamp: Option<i64>,
    pub hours_covered: f64,
    pub avg_gap_minutes: f64,
    pub classification_flips: i32,
    pub classifications_seen: Vec<String>,
    pub oldest: Option<SnapshotPoint>,
    pub latest: Option<SnapshotPoint>,
    pub changes: Option<HistoryChanges>,
    pub ranges: Option<HistoryRanges>,
    pub hidden_flags: Vec<String>,
    pub timeline: Vec<SnapshotPoint>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OwnerExposure {
    pub rank: usize,
    pub token_account: String,
    pub owner: String,
    pub owner_label: Option<String>,
    pub owner_type: Option<String>,
    pub balance_ui: f64,
    pub pct_supply: f64,
    pub is_amm: bool,
    pub is_creator: bool,
    pub is_smart_wallet: bool,
    pub smart_wallet_label: Option<String>,
    pub portfolio_token_count: Option<usize>,
    pub reference_overlap_labels: Vec<String>,
    pub same_balance_cluster_size: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct RepeatedBalanceCluster {
    pub amount_ui: f64,
    pub holder_count: usize,
    pub combined_pct_supply: f64,
    pub owners: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HolderForensics {
    pub mint: String,
    pub top_n: usize,
    pub total_holders: Option<i32>,
    pub top_n_pct: f64,
    pub wallet_controlled_pct: f64,
    pub amm_controlled_pct: f64,
    pub creator_controlled_pct: f64,
    pub smart_wallet_hits: usize,
    pub reference_overlap_hits: usize,
    pub graph_insiders_detected: Option<i64>,
    pub largest_insider_network_size: Option<i64>,
    pub largest_insider_network_pct_supply: Option<f64>,
    pub repeated_balance_clusters: Vec<RepeatedBalanceCluster>,
    pub flags: Vec<String>,
    pub owners: Vec<OwnerExposure>,
}

#[derive(Debug, Clone, Serialize)]
pub struct WalletHolding {
    pub mint: String,
    pub balance_ui: f64,
    pub reference_label: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FocusMintFlow {
    pub mint: String,
    pub current_balance_ui: f64,
    pub net_change_24h_ui: f64,
    pub net_change_7d_ui: f64,
    pub relevant_txs_24h: i32,
    pub relevant_txs_7d: i32,
}

#[derive(Debug, Clone, Serialize)]
pub struct WalletXray {
    pub wallet: String,
    pub smart_wallet_label: Option<String>,
    pub holdings_count: usize,
    pub tx_count_24h: i32,
    pub tx_count_7d: i32,
    pub seconds_since_last_tx: Option<i64>,
    pub reference_overlap_labels: Vec<String>,
    pub top_holdings: Vec<WalletHolding>,
    pub focus: Option<FocusMintFlow>,
}

#[derive(Debug, Clone, Serialize)]
pub struct WhaleSummary {
    pub whales_analyzed: usize,
    pub accumulating_count: usize,
    pub distributing_count: usize,
    pub idle_count: usize,
    pub owner_unresolved_count: usize,
    pub net_change_24h_ui: f64,
    pub net_change_7d_ui: f64,
    pub top_accumulators: Vec<scout::WhaleMove>,
    pub top_distributors: Vec<scout::WhaleMove>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HiddenSignal {
    pub key: String,
    pub direction: String,
    pub severity: String,
    pub summary: String,
    pub evidence: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DeepSignalReport {
    pub mint: String,
    pub market: Option<MarketIntel>,
    pub history: HistoricalAnalysis,
    pub owners: HolderForensics,
    pub whales: WhaleSummary,
    pub deployer: Option<scout::DeployerProfile>,
    pub lp: Option<scout::LpStatus>,
    pub sniper_cohort: scout::SniperCohort,
    pub cohort_overlap: scout::CohortOverlap,
    pub hidden_signals: Vec<HiddenSignal>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MarketIntel {
    pub symbol: String,
    pub name: String,
    pub price_usd: f64,
    pub mcap_usd: f64,
    pub liquidity_usd: f64,
    pub volume_h1_usd: f64,
    pub volume_h24_usd: f64,
    pub buys_h1: i32,
    pub sells_h1: i32,
    pub price_change_h1: f64,
    pub pair_dex: String,
    pub pair_address: String,
    pub pair_created_at: i64,
}

#[derive(Debug, Clone)]
struct RugHolder {
    token_account: String,
    owner: String,
    balance_ui: f64,
    pct_supply: f64,
    amount_key: String,
}

#[derive(Debug, Clone)]
struct RugReport {
    top_holders: Vec<RugHolder>,
    known_accounts: HashMap<String, (String, String)>,
    total_holders: Option<i32>,
    creator: Option<String>,
    graph_insiders_detected: Option<i64>,
    largest_insider_network_size: Option<i64>,
    largest_insider_network_pct_supply: Option<f64>,
}

pub fn historical_analysis(
    mint: &str,
    db: &Arc<Db>,
    limit: usize,
    window_hours: Option<i64>,
) -> Result<HistoricalAnalysis> {
    let requested_limit = limit.max(2);
    let mut history = db.get_snapshot_history(mint, requested_limit)?;
    history.reverse();

    if let Some(hours) = window_hours {
        let latest_ts = history.last().map(|s| s.timestamp);
        if let Some(latest_ts) = latest_ts {
            let cutoff = latest_ts - hours.max(1) * 3600;
            history.retain(|s| s.timestamp >= cutoff);
        }
    }

    let points: Vec<SnapshotPoint> = history.iter().map(snapshot_point).collect();
    let first_timestamp = history.first().map(|s| s.timestamp);
    let latest_timestamp = history.last().map(|s| s.timestamp);
    let hours_covered = match (first_timestamp, latest_timestamp) {
        (Some(first), Some(last)) if last > first => (last - first) as f64 / 3600.0,
        _ => 0.0,
    };

    let avg_gap_minutes = if history.len() >= 2 {
        let total_gap: i64 = history
            .windows(2)
            .map(|w| w[1].timestamp - w[0].timestamp)
            .sum();
        total_gap as f64 / (history.len() as f64 - 1.0) / 60.0
    } else {
        0.0
    };

    let classifications_seen: Vec<String> = history
        .iter()
        .map(|s| s.classification.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let classification_flips = history
        .windows(2)
        .filter(|w| w[0].classification != w[1].classification)
        .count() as i32;

    let oldest = history.first().map(snapshot_point);
    let latest = history.last().map(snapshot_point);
    let changes = history_changes(&history);
    let ranges = history_ranges(&history);
    let hidden_flags = history_flags(&history, changes.as_ref(), classification_flips);

    Ok(HistoricalAnalysis {
        mint: mint.to_string(),
        snapshots_analyzed: history.len(),
        requested_limit,
        window_hours,
        first_timestamp,
        latest_timestamp,
        hours_covered,
        avg_gap_minutes,
        classification_flips,
        classifications_seen,
        oldest,
        latest,
        changes,
        ranges,
        hidden_flags,
        timeline: points,
    })
}

pub async fn holder_forensics(
    mint: &str,
    rpc: &Arc<RpcRouter>,
    db: &Arc<Db>,
    top_n: usize,
) -> Result<HolderForensics> {
    let top_n = top_n.clamp(3, 20);
    let rug = fetch_rug_report(mint, rpc).await?;
    let smart_wallets: HashMap<String, String> =
        db.list_active_smart_wallets()?.into_iter().collect();
    let refs: HashMap<String, String> = db.list_reference_mints_with_label()?.into_iter().collect();

    let mut amount_clusters: HashMap<String, Vec<(String, f64, bool)>> = HashMap::new();
    for holder in rug.top_holders.iter().take(top_n) {
        let owner_type = rug
            .known_accounts
            .get(&holder.owner)
            .or_else(|| rug.known_accounts.get(&holder.token_account))
            .map(|(_, kind)| kind.clone())
            .unwrap_or_default();
        let is_amm = owner_type.contains("AMM");
        amount_clusters
            .entry(holder.amount_key.clone())
            .or_default()
            .push((holder.owner.clone(), holder.pct_supply, is_amm));
    }

    let repeated_balance_clusters: Vec<RepeatedBalanceCluster> = amount_clusters
        .iter()
        .filter_map(|(amount_key, owners)| {
            let non_amm: Vec<_> = owners.iter().filter(|(_, _, is_amm)| !*is_amm).collect();
            if non_amm.len() < 2 {
                return None;
            }
            Some(RepeatedBalanceCluster {
                amount_ui: amount_key.parse::<f64>().unwrap_or(0.0),
                holder_count: non_amm.len(),
                combined_pct_supply: non_amm.iter().map(|(_, pct, _)| *pct).sum(),
                owners: non_amm
                    .iter()
                    .map(|(owner, _, _)| (*owner).clone())
                    .collect(),
            })
        })
        .collect();

    let same_balance_cluster_sizes: HashMap<String, usize> = repeated_balance_clusters
        .iter()
        .flat_map(|cluster| {
            cluster
                .owners
                .iter()
                .cloned()
                .map(|owner| (owner, cluster.holder_count))
                .collect::<Vec<_>>()
        })
        .collect();

    let mut owners = Vec::new();
    let mut wallet_controlled_pct = 0.0;
    let mut amm_controlled_pct = 0.0;
    let mut creator_controlled_pct = 0.0;
    let mut smart_wallet_hits = 0usize;
    let mut reference_overlap_hits = 0usize;

    for (idx, holder) in rug.top_holders.iter().take(top_n).enumerate() {
        let label_entry = rug
            .known_accounts
            .get(&holder.owner)
            .or_else(|| rug.known_accounts.get(&holder.token_account));
        let owner_label = label_entry.map(|(name, _)| name.clone());
        let owner_type = label_entry.map(|(_, kind)| kind.clone());
        let is_amm = owner_type
            .as_deref()
            .map(|kind| kind.contains("AMM"))
            .unwrap_or(false);
        let is_creator = rug
            .creator
            .as_ref()
            .map(|creator| creator == &holder.owner)
            .unwrap_or(false);
        let smart_wallet_label = smart_wallets.get(&holder.owner).cloned();
        let is_smart_wallet = smart_wallet_label.is_some();
        if is_smart_wallet {
            smart_wallet_hits += 1;
        }

        let mut reference_overlap_labels = Vec::new();
        let mut portfolio_token_count = None;
        if !is_amm && holder.owner != NULL_ADDRESS && !holder.owner.is_empty() {
            let holdings = rpc
                .get_wallet_token_holdings(&holder.owner)
                .await
                .unwrap_or_default();
            portfolio_token_count = Some(holdings.len());
            for (portfolio_mint, _) in &holdings {
                if let Some(label) = refs.get(portfolio_mint) {
                    reference_overlap_labels.push(if label.is_empty() {
                        portfolio_mint.clone()
                    } else {
                        format!("{} ({})", label, portfolio_mint)
                    });
                }
            }
            reference_overlap_labels.sort();
            reference_overlap_labels.dedup();
            if !reference_overlap_labels.is_empty() {
                reference_overlap_hits += 1;
            }
        }

        if is_amm {
            amm_controlled_pct += holder.pct_supply;
        } else {
            wallet_controlled_pct += holder.pct_supply;
        }
        if is_creator {
            creator_controlled_pct += holder.pct_supply;
        }

        owners.push(OwnerExposure {
            rank: idx + 1,
            token_account: holder.token_account.clone(),
            owner: holder.owner.clone(),
            owner_label,
            owner_type,
            balance_ui: holder.balance_ui,
            pct_supply: holder.pct_supply,
            is_amm,
            is_creator,
            is_smart_wallet,
            smart_wallet_label,
            portfolio_token_count,
            reference_overlap_labels,
            same_balance_cluster_size: same_balance_cluster_sizes
                .get(&holder.owner)
                .copied()
                .unwrap_or(1),
        });
    }

    let top_n_pct: f64 = owners.iter().map(|owner| owner.pct_supply).sum();
    let mut flags = Vec::new();
    if owners
        .iter()
        .any(|owner| !owner.is_amm && owner.pct_supply >= 15.0)
    {
        flags.push("one non-AMM owner controls at least 15% of supply".to_string());
    }
    if !repeated_balance_clusters.is_empty() {
        flags.push("multiple top holders carry identical balances, suggesting wallet splitting or synchronized distribution".to_string());
    }
    if rug
        .largest_insider_network_pct_supply
        .map(|pct| pct >= 20.0)
        .unwrap_or(false)
    {
        flags.push("largest detected insider network controls more than 20% of supply".to_string());
    }
    if smart_wallet_hits >= 2 {
        flags.push("multiple top holders match tracked smart wallets".to_string());
    }

    Ok(HolderForensics {
        mint: mint.to_string(),
        top_n,
        total_holders: rug.total_holders,
        top_n_pct,
        wallet_controlled_pct,
        amm_controlled_pct,
        creator_controlled_pct,
        smart_wallet_hits,
        reference_overlap_hits,
        graph_insiders_detected: rug.graph_insiders_detected,
        largest_insider_network_size: rug.largest_insider_network_size,
        largest_insider_network_pct_supply: rug.largest_insider_network_pct_supply,
        repeated_balance_clusters,
        flags,
        owners,
    })
}

pub async fn wallet_xray(
    address: &str,
    focus_mint: Option<&str>,
    rpc: &Arc<RpcRouter>,
    db: &Arc<Db>,
) -> Result<WalletXray> {
    let refs: HashMap<String, String> = db.list_reference_mints_with_label()?.into_iter().collect();
    let smart_wallets: HashMap<String, String> =
        db.list_active_smart_wallets()?.into_iter().collect();
    let holdings = rpc
        .get_wallet_token_holdings(address)
        .await
        .unwrap_or_default();
    let mut sorted_holdings = holdings.clone();
    sorted_holdings.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));

    let sigs = rpc
        .get_recent_signatures(address, 50)
        .await
        .unwrap_or_default();
    let now = chrono::Utc::now().timestamp();
    let mut tx_count_24h = 0;
    let mut tx_count_7d = 0;
    let mut seconds_since_last_tx = None;

    for sig in &sigs {
        let Some(ts) = sig.block_time else { continue };
        if seconds_since_last_tx
            .map(|cur| ts > now - cur)
            .unwrap_or(true)
        {
            seconds_since_last_tx = Some(now - ts);
        }
        let age = now - ts;
        if age <= 86_400 {
            tx_count_24h += 1;
        }
        if age <= 7 * 86_400 {
            tx_count_7d += 1;
        }
    }

    let reference_overlap_labels: Vec<String> = sorted_holdings
        .iter()
        .filter_map(|(mint, _)| refs.get(mint).map(|label| (mint, label)))
        .map(|(mint, label)| {
            if label.is_empty() {
                mint.clone()
            } else {
                format!("{} ({})", label, mint)
            }
        })
        .collect();

    let top_holdings: Vec<WalletHolding> = sorted_holdings
        .iter()
        .take(12)
        .map(|(mint, balance_ui)| WalletHolding {
            mint: mint.clone(),
            balance_ui: *balance_ui,
            reference_label: refs.get(mint).cloned(),
        })
        .collect();

    let focus = if let Some(focus_mint) = focus_mint {
        let current_balance_ui = holdings
            .iter()
            .find(|(mint, _)| mint == focus_mint)
            .map(|(_, bal)| *bal)
            .unwrap_or(0.0);
        let mut net_change_24h_ui = 0.0;
        let mut net_change_7d_ui = 0.0;
        let mut relevant_txs_24h = 0;
        let mut relevant_txs_7d = 0;

        for sig in &sigs {
            let Some(ts) = sig.block_time else { continue };
            if ts < now - 7 * 86_400 {
                continue;
            }
            let changes = rpc
                .get_tx_balance_changes(&sig.signature)
                .await
                .unwrap_or_default();
            let delta: f64 = changes
                .iter()
                .filter(|change| change.owner == address && change.mint == focus_mint)
                .map(|change| change.delta_ui)
                .sum();
            if delta == 0.0 {
                continue;
            }
            net_change_7d_ui += delta;
            relevant_txs_7d += 1;
            if ts >= now - 86_400 {
                net_change_24h_ui += delta;
                relevant_txs_24h += 1;
            }
        }

        Some(FocusMintFlow {
            mint: focus_mint.to_string(),
            current_balance_ui,
            net_change_24h_ui,
            net_change_7d_ui,
            relevant_txs_24h,
            relevant_txs_7d,
        })
    } else {
        None
    };

    Ok(WalletXray {
        wallet: address.to_string(),
        smart_wallet_label: smart_wallets.get(address).cloned(),
        holdings_count: holdings.len(),
        tx_count_24h,
        tx_count_7d,
        seconds_since_last_tx,
        reference_overlap_labels,
        top_holdings,
        focus,
    })
}

pub async fn deep_signals(
    mint: &str,
    rpc: &Arc<RpcRouter>,
    db: &Arc<Db>,
) -> Result<DeepSignalReport> {
    let market_data = market::get_market(mint).await.ok().flatten();
    let history = historical_analysis(mint, db, 96, Some(48))?;
    let owners = holder_forensics(mint, rpc, db, 12).await?;
    let basic = scout::scout(mint, rpc, db).await.ok();
    let whales = scout::whale_trace(mint, rpc).await.unwrap_or_default();
    let whale_summary = summarize_whales(&whales);
    let lp = if let Some(market) = &market_data {
        if !market.pair_address.is_empty() && !market.pair_dex.is_empty() {
            scout::lp_check(&market.pair_address, &market.pair_dex, rpc)
                .await
                .ok()
        } else {
            None
        }
    } else {
        None
    };
    let sniper_cohort = scout::sniper_cohort(mint, rpc, db)
        .await
        .unwrap_or_default();
    let cohort_overlap = scout::cohort_overlap(mint, rpc, db)
        .await
        .unwrap_or_default();

    let mut hidden_signals = Vec::new();

    if let Some(changes) = &history.changes {
        if changes.price_pct.unwrap_or(0.0) > 5.0 && changes.volume_h1_pct.unwrap_or(0.0) < -40.0 {
            hidden_signals.push(HiddenSignal {
                key: "price_up_volume_down".to_string(),
                direction: "bearish".to_string(),
                severity: "medium".to_string(),
                summary: "Price advanced while hourly participation collapsed.".to_string(),
                evidence: vec![
                    format!(
                        "price change over stored window: {:+.2}%",
                        changes.price_pct.unwrap_or(0.0)
                    ),
                    format!(
                        "h1 volume change over stored window: {:+.2}%",
                        changes.volume_h1_pct.unwrap_or(0.0)
                    ),
                ],
            });
        }
        if changes.top_holder_pct_delta > 1.5 && changes.holder_count_delta <= 0 {
            hidden_signals.push(HiddenSignal {
                key: "concentration_creep".to_string(),
                direction: "bearish".to_string(),
                severity: "high".to_string(),
                summary: "Top-holder concentration rose while holder breadth failed to expand."
                    .to_string(),
                evidence: vec![
                    format!("top holder delta: {:+.2}%", changes.top_holder_pct_delta),
                    format!("holder count delta: {:+}", changes.holder_count_delta),
                ],
            });
        }
        if changes.momentum_delta < -10 && changes.price_pct.unwrap_or(0.0) > 0.0 {
            hidden_signals.push(HiddenSignal {
                key: "momentum_divergence".to_string(),
                direction: "bearish".to_string(),
                severity: "medium".to_string(),
                summary: "Stored momentum cooled even though price stayed green.".to_string(),
                evidence: vec![
                    format!("momentum delta: {:+}", changes.momentum_delta),
                    format!("price delta: {:+.2}%", changes.price_pct.unwrap_or(0.0)),
                ],
            });
        }
    }

    if history.classification_flips >= 2 {
        hidden_signals.push(HiddenSignal {
            key: "classification_instability".to_string(),
            direction: "bearish".to_string(),
            severity: "medium".to_string(),
            summary: "The token has flipped regimes multiple times across stored snapshots."
                .to_string(),
            evidence: vec![
                format!("classification flips: {}", history.classification_flips),
                format!("classes seen: {}", history.classifications_seen.join(", ")),
            ],
        });
    }

    if whale_summary.distributing_count >= whale_summary.accumulating_count + 2
        && whale_summary.net_change_7d_ui < 0.0
    {
        hidden_signals.push(HiddenSignal {
            key: "whale_distribution_agreement".to_string(),
            direction: "bearish".to_string(),
            severity: "high".to_string(),
            summary: "Top holders are aligned on net distribution.".to_string(),
            evidence: vec![
                format!("distributing whales: {}", whale_summary.distributing_count),
                format!("accumulating whales: {}", whale_summary.accumulating_count),
                format!(
                    "net whale change 7d: {:+.2}",
                    whale_summary.net_change_7d_ui
                ),
            ],
        });
    } else if whale_summary.accumulating_count >= whale_summary.distributing_count + 2
        && whale_summary.net_change_7d_ui > 0.0
    {
        hidden_signals.push(HiddenSignal {
            key: "whale_accumulation_agreement".to_string(),
            direction: "bullish".to_string(),
            severity: "medium".to_string(),
            summary: "Multiple top holders are adding rather than distributing.".to_string(),
            evidence: vec![
                format!("accumulating whales: {}", whale_summary.accumulating_count),
                format!("distributing whales: {}", whale_summary.distributing_count),
                format!(
                    "net whale change 7d: {:+.2}",
                    whale_summary.net_change_7d_ui
                ),
            ],
        });
    }

    if owners
        .largest_insider_network_pct_supply
        .map(|pct| pct >= 20.0)
        .unwrap_or(false)
    {
        hidden_signals.push(HiddenSignal {
            key: "dense_insider_graph".to_string(),
            direction: "bearish".to_string(),
            severity: "high".to_string(),
            summary: "A large linked insider graph controls a material share of supply."
                .to_string(),
            evidence: vec![
                format!(
                    "graph insiders detected: {}",
                    owners.graph_insiders_detected.unwrap_or(0)
                ),
                format!(
                    "largest insider network pct supply: {:.2}%",
                    owners.largest_insider_network_pct_supply.unwrap_or(0.0)
                ),
            ],
        });
    }

    if !owners.repeated_balance_clusters.is_empty() {
        let cluster = &owners.repeated_balance_clusters[0];
        hidden_signals.push(HiddenSignal {
            key: "wallet_split_pattern".to_string(),
            direction: "bearish".to_string(),
            severity: if cluster.combined_pct_supply >= 8.0 {
                "high".to_string()
            } else {
                "medium".to_string()
            },
            summary: "Repeated top-holder balances suggest coordinated wallet splitting."
                .to_string(),
            evidence: vec![
                format!("largest repeated cluster size: {}", cluster.holder_count),
                format!(
                    "largest repeated cluster pct supply: {:.2}%",
                    cluster.combined_pct_supply
                ),
            ],
        });
    }

    if let Some(basic) = &basic {
        if let Some(deployer) = &basic.deployer {
            if deployer.pct_sold >= 80.0 && deployer.active {
                hidden_signals.push(HiddenSignal {
                    key: "deployer_exited_but_active".to_string(),
                    direction: "bearish".to_string(),
                    severity: "medium".to_string(),
                    summary: "The deployer has sold most of the launch bag but still shows recent wallet activity.".to_string(),
                    evidence: vec![
                        format!("deployer pct sold: {:.2}%", deployer.pct_sold),
                        format!("deployer tx count 24h: {}", deployer.tx_count_24h),
                    ],
                });
            }
        }
    }

    if sniper_cohort.available && sniper_cohort.retention_pct >= 15.0 {
        hidden_signals.push(HiddenSignal {
            key: "sniper_overhang".to_string(),
            direction: "bearish".to_string(),
            severity: "medium".to_string(),
            summary: "A notable fraction of early buyers still sit in the top-100 holder set."
                .to_string(),
            evidence: vec![
                format!("snipers captured: {}", sniper_cohort.sniper_count),
                format!("retention pct: {:.2}%", sniper_cohort.retention_pct),
            ],
        });
    }

    if cohort_overlap.holders_with_any_overlap >= 4 {
        hidden_signals.push(HiddenSignal {
            key: "reference_cohort_overlap".to_string(),
            direction: "bullish".to_string(),
            severity: "low".to_string(),
            summary: "Multiple top holders overlap with the curated reference-mint cohort."
                .to_string(),
            evidence: vec![
                format!(
                    "holders with any overlap: {}",
                    cohort_overlap.holders_with_any_overlap
                ),
                format!("reference mint count: {}", cohort_overlap.reference_count),
            ],
        });
    }

    if let Some(latest) = &history.latest {
        if latest.sells_h1 > latest.buys_h1 && latest.price_change_h1 > 0.0 {
            hidden_signals.push(HiddenSignal {
                key: "absorption_under_sell_pressure".to_string(),
                direction: "mixed".to_string(),
                severity: "medium".to_string(),
                summary: "Price is holding positive despite sell-heavy recent flow.".to_string(),
                evidence: vec![
                    format!("h1 buys/sells: {}/{}", latest.buys_h1, latest.sells_h1),
                    format!("h1 price change: {:+.2}%", latest.price_change_h1),
                ],
            });
        }
    }

    Ok(DeepSignalReport {
        mint: mint.to_string(),
        market: market_data.map(|market| MarketIntel {
            symbol: market.symbol,
            name: market.name,
            price_usd: market.price_usd,
            mcap_usd: market.mcap_usd,
            liquidity_usd: market.liquidity_usd,
            volume_h1_usd: market.volume_h1_usd,
            volume_h24_usd: market.volume_h24_usd,
            buys_h1: market.buys_h1,
            sells_h1: market.sells_h1,
            price_change_h1: market.price_change_h1,
            pair_dex: market.pair_dex,
            pair_address: market.pair_address,
            pair_created_at: market.pair_created_at,
        }),
        history,
        owners,
        whales: whale_summary,
        deployer: basic.and_then(|basic| basic.deployer),
        lp,
        sniper_cohort,
        cohort_overlap,
        hidden_signals,
    })
}

fn snapshot_point(snap: &TokenSnapshot) -> SnapshotPoint {
    SnapshotPoint {
        timestamp: snap.timestamp,
        classification: snap.classification.clone(),
        confidence: snap.confidence,
        top_holder_pct: snap.top_holder_pct,
        top5_pct: snap.top5_pct,
        holder_count: snap.holder_count,
        tx_rate: snap.tx_rate,
        velocity: snap.velocity,
        momentum: snap.momentum,
        distribution: snap.distribution,
        spring: snap.spring,
        price_usd: snap.price_usd,
        mcap_usd: snap.mcap_usd,
        liquidity_usd: snap.liquidity_usd,
        volume_h1_usd: snap.volume_h1_usd,
        buys_h1: snap.buys_h1,
        sells_h1: snap.sells_h1,
        price_change_h1: snap.price_change_h1,
        pair_dex: snap.pair_dex.clone(),
    }
}

fn history_changes(history: &[TokenSnapshot]) -> Option<HistoryChanges> {
    let oldest = history.first()?;
    let latest = history.last()?;
    Some(HistoryChanges {
        price_pct: pct_change(latest.price_usd, oldest.price_usd),
        mcap_pct: pct_change(latest.mcap_usd, oldest.mcap_usd),
        liquidity_pct: pct_change(latest.liquidity_usd, oldest.liquidity_usd),
        volume_h1_pct: pct_change(latest.volume_h1_usd, oldest.volume_h1_usd),
        holder_count_delta: latest.holder_count - oldest.holder_count,
        top_holder_pct_delta: latest.top_holder_pct - oldest.top_holder_pct,
        top5_pct_delta: latest.top5_pct - oldest.top5_pct,
        tx_rate_delta: latest.tx_rate - oldest.tx_rate,
        momentum_delta: latest.momentum - oldest.momentum,
        distribution_delta: latest.distribution - oldest.distribution,
        confidence_delta: latest.confidence - oldest.confidence,
    })
}

fn history_ranges(history: &[TokenSnapshot]) -> Option<HistoryRanges> {
    let first = history.first()?;
    let mut price = RangeF64 {
        min: first.price_usd,
        max: first.price_usd,
    };
    let mut mcap = RangeF64 {
        min: first.mcap_usd,
        max: first.mcap_usd,
    };
    let mut liquidity = RangeF64 {
        min: first.liquidity_usd,
        max: first.liquidity_usd,
    };
    let mut holder_count = RangeI32 {
        min: first.holder_count,
        max: first.holder_count,
    };
    let mut top_holder = RangeF64 {
        min: first.top_holder_pct,
        max: first.top_holder_pct,
    };
    let mut tx_rate = RangeF64 {
        min: first.tx_rate,
        max: first.tx_rate,
    };
    let mut momentum = RangeI32 {
        min: first.momentum,
        max: first.momentum,
    };
    let mut distribution = RangeI32 {
        min: first.distribution,
        max: first.distribution,
    };
    let mut confidence = RangeI32 {
        min: first.confidence,
        max: first.confidence,
    };

    for snap in history.iter().skip(1) {
        price.min = price.min.min(snap.price_usd);
        price.max = price.max.max(snap.price_usd);
        mcap.min = mcap.min.min(snap.mcap_usd);
        mcap.max = mcap.max.max(snap.mcap_usd);
        liquidity.min = liquidity.min.min(snap.liquidity_usd);
        liquidity.max = liquidity.max.max(snap.liquidity_usd);
        holder_count.min = holder_count.min.min(snap.holder_count);
        holder_count.max = holder_count.max.max(snap.holder_count);
        top_holder.min = top_holder.min.min(snap.top_holder_pct);
        top_holder.max = top_holder.max.max(snap.top_holder_pct);
        tx_rate.min = tx_rate.min.min(snap.tx_rate);
        tx_rate.max = tx_rate.max.max(snap.tx_rate);
        momentum.min = momentum.min.min(snap.momentum);
        momentum.max = momentum.max.max(snap.momentum);
        distribution.min = distribution.min.min(snap.distribution);
        distribution.max = distribution.max.max(snap.distribution);
        confidence.min = confidence.min.min(snap.confidence);
        confidence.max = confidence.max.max(snap.confidence);
    }

    Some(HistoryRanges {
        price_usd: price,
        mcap_usd: mcap,
        liquidity_usd: liquidity,
        holder_count,
        top_holder_pct: top_holder,
        tx_rate,
        momentum,
        distribution,
        confidence,
    })
}

fn history_flags(
    history: &[TokenSnapshot],
    changes: Option<&HistoryChanges>,
    classification_flips: i32,
) -> Vec<String> {
    let mut flags = Vec::new();
    let Some(latest) = history.last() else {
        return flags;
    };
    if latest.sells_h1 > latest.buys_h1 && latest.price_change_h1 > 0.0 {
        flags.push("recent price held positive despite sell-heavy h1 tape".to_string());
    }
    if let Some(changes) = changes {
        if changes.price_pct.unwrap_or(0.0) > 0.0 && changes.volume_h1_pct.unwrap_or(0.0) < -30.0 {
            flags.push("stored history shows price-up / volume-down compression".to_string());
        }
        if changes.top_holder_pct_delta > 1.5 {
            flags.push("top-holder concentration increased across the stored window".to_string());
        }
        if changes.momentum_delta < -10 {
            flags.push("momentum cooled materially across the stored window".to_string());
        }
        if changes.liquidity_pct.unwrap_or(0.0) < -20.0 && changes.price_pct.unwrap_or(0.0) >= 0.0 {
            flags.push("liquidity thinned faster than price".to_string());
        }
    }
    if classification_flips >= 2 {
        flags.push("classification has flipped more than once in the stored window".to_string());
    }
    flags
}

fn pct_change(current: f64, previous: f64) -> Option<f64> {
    if previous.abs() < f64::EPSILON {
        None
    } else {
        Some((current - previous) / previous * 100.0)
    }
}



fn summarize_whales(whales: &[scout::WhaleMove]) -> WhaleSummary {
    let accumulating_count = whales.iter().filter(|w| w.action == "accumulating").count();
    let distributing_count = whales.iter().filter(|w| w.action == "distributing").count();
    let idle_count = whales.iter().filter(|w| w.action == "idle").count();
    let owner_unresolved_count = whales
        .iter()
        .filter(|w| w.action == "owner_unresolved")
        .count();
    let net_change_24h_ui: f64 = whales.iter().map(|w| w.net_change_24h_ui).sum();
    let net_change_7d_ui: f64 = whales.iter().map(|w| w.net_change_7d_ui).sum();

    let mut top_accumulators: Vec<_> = whales
        .iter()
        .filter(|w| w.net_change_7d_ui > 0.0)
        .cloned()
        .collect();
    top_accumulators.sort_by(|a, b| {
        b.net_change_7d_ui
            .partial_cmp(&a.net_change_7d_ui)
            .unwrap_or(Ordering::Equal)
    });
    top_accumulators.truncate(3);

    let mut top_distributors: Vec<_> = whales
        .iter()
        .filter(|w| w.net_change_7d_ui < 0.0)
        .cloned()
        .collect();
    top_distributors.sort_by(|a, b| {
        a.net_change_7d_ui
            .partial_cmp(&b.net_change_7d_ui)
            .unwrap_or(Ordering::Equal)
    });
    top_distributors.truncate(3);

    WhaleSummary {
        whales_analyzed: whales.len(),
        accumulating_count,
        distributing_count,
        idle_count,
        owner_unresolved_count,
        net_change_24h_ui,
        net_change_7d_ui,
        top_accumulators,
        top_distributors,
    }
}

async fn fetch_rug_report(mint: &str, rpc: &Arc<RpcRouter>) -> Result<RugReport> {
    use once_cell::sync::Lazy;
    static HTTP: Lazy<reqwest::Client> = Lazy::new(|| {
        reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(8))
            .user_agent("photon/0.1")
            .build()
            .expect("intel rugcheck HTTP init")
    });
    let url = format!("https://api.rugcheck.xyz/v1/tokens/{}/report", mint);
    let resp = HTTP.get(&url).send().await?;
    let resp = resp.error_for_status()?;
    let value: Value = resp.json().await?;

    let known_accounts: HashMap<String, (String, String)> = value
        .get("knownAccounts")
        .and_then(|v| v.as_object())
        .map(|obj| {
            obj.iter()
                .map(|(addr, v)| {
                    let name = json_str(v.get("name")).unwrap_or_default().to_string();
                    let kind = json_str(v.get("type")).unwrap_or_default().to_string();
                    (addr.clone(), (name, kind))
                })
                .collect()
        })
        .unwrap_or_default();

    let total_holders = value
        .get("totalHolders")
        .and_then(|v| v.as_i64())
        .map(|v| v as i32);
    let creator = json_str(value.get("creator")).map(ToString::to_string);
    let graph_insiders_detected = value.get("graphInsidersDetected").and_then(|v| v.as_i64());

    let token_supply_raw = value
        .get("token")
        .and_then(|v| v.get("supply"))
        .and_then(as_f64);
    let decimals = value
        .get("token")
        .and_then(|v| v.get("decimals"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as i32;
    let supply_ui = token_supply_raw.map(|s| s / 10f64.powi(decimals));

    let (largest_insider_network_size, largest_insider_network_pct_supply) = value
        .get("insiderNetworks")
        .and_then(|v| v.as_array())
        .and_then(|arr| arr.first())
        .map(|network| {
            let size = network.get("size").and_then(|v| v.as_i64());
            let token_amount_ui = network
                .get("tokenAmount")
                .and_then(as_f64)
                .map(|amt| amt / 10f64.powi(decimals));
            let pct = match (token_amount_ui, supply_ui) {
                (Some(network_ui), Some(supply_ui)) if supply_ui > 0.0 => {
                    Some(network_ui / supply_ui * 100.0)
                }
                _ => None,
            };
            (size, pct)
        })
        .unwrap_or((None, None));

    let top_holders = if let Some(arr) = value.get("topHolders").and_then(|v| v.as_array()) {
        let mut holders = Vec::new();
        for entry in arr {
            let token_account = json_str(entry.get("address"))
                .unwrap_or_default()
                .to_string();
            let mut owner = json_str(entry.get("owner")).unwrap_or_default().to_string();
            if owner.is_empty() && !token_account.is_empty() {
                owner = rpc
                    .get_token_account_owner(&token_account)
                    .await
                    .unwrap_or_default();
            }
            let balance_ui = entry
                .get("uiAmount")
                .and_then(as_f64)
                .or_else(|| entry.get("uiAmountString").and_then(as_f64))
                .unwrap_or(0.0);
            let pct_supply = entry.get("pct").and_then(as_f64).unwrap_or(0.0);
            let amount_key = entry
                .get("uiAmountString")
                .and_then(|v| v.as_str())
                .map(ToString::to_string)
                .unwrap_or_else(|| format!("{:.6}", balance_ui));
            holders.push(RugHolder {
                token_account,
                owner,
                balance_ui,
                pct_supply,
                amount_key,
            });
        }
        holders
    } else {
        let supply_ui = rpc
            .get_token_supply(mint)
            .await
            .map(|s| s.ui_amount)
            .unwrap_or(1_000_000_000.0);
        let largest = rpc
            .get_token_largest_accounts(mint)
            .await
            .unwrap_or_default();
        let mut holders = Vec::new();
        for holder in largest.into_iter().take(20) {
            let owner = rpc
                .get_token_account_owner(&holder.address)
                .await
                .unwrap_or_default();
            let pct_supply = if supply_ui > 0.0 {
                holder.ui_amount / supply_ui * 100.0
            } else {
                0.0
            };
            holders.push(RugHolder {
                token_account: holder.address,
                owner,
                balance_ui: holder.ui_amount,
                pct_supply,
                amount_key: format!("{:.6}", holder.ui_amount),
            });
        }
        holders
    };

    Ok(RugReport {
        top_holders,
        known_accounts,
        total_holders,
        creator,
        graph_insiders_detected,
        largest_insider_network_size,
        largest_insider_network_pct_supply,
    })
}

fn json_str(value: Option<&Value>) -> Option<&str> {
    value.and_then(|v| v.as_str())
}

fn as_f64(value: &Value) -> Option<f64> {
    if let Some(v) = value.as_f64() {
        Some(v)
    } else {
        value.as_str().and_then(|s| s.parse::<f64>().ok())
    }
}
