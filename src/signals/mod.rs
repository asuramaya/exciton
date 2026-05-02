pub mod microstructure;
pub mod onchain;
pub mod safety;
pub mod smartmoney;

use crate::ingester::{parse_mint_account, RpcRouter};
use anyhow::{Context, Result};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

static FORENSICS_IN_FLIGHT: Lazy<Mutex<HashSet<String>>> =
    Lazy::new(|| Mutex::new(HashSet::new()));

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SignalLayer {
    OnChain,
    Microstructure,
    Safety,
    SmartMoney,
}

impl SignalLayer {
    pub fn all() -> &'static [SignalLayer] {
        &[
            SignalLayer::OnChain,
            SignalLayer::Microstructure,
            SignalLayer::Safety,
            SignalLayer::SmartMoney,
        ]
    }

}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalScore {
    pub layer: SignalLayer,
    pub signal_type: String,
    pub score: i32,
    pub details: String,
    pub timestamp: i64,
}

impl SignalScore {
    pub fn new(layer: SignalLayer, signal_type: &str, score: i32, details: &str) -> Self {
        Self {
            layer,
            signal_type: signal_type.to_string(),
            score: score.clamp(0, 100),
            details: details.to_string(),
            timestamp: chrono::Utc::now().timestamp(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Confidence {
    pub total: i32,
    /// Weighted sum (mom×0.40 + dist×0.30 + spring×0.30) before the
    /// top-holder bonus is applied. Exposed so callers can show the breakdown.
    #[serde(default)]
    pub base_total: i32,
    /// Distribution bonus applied on top of base_total: 0, 10, or 15.
    /// Fires when top_holder_score ≥ 75 (+10%) or ≥ 90 (+15%).
    #[serde(default)]
    pub top_holder_bonus_pct: i32,
    pub momentum: i32,
    pub distribution: i32,
    pub spring: i32,
    pub coverage: usize,
    pub layer_scores: Vec<(SignalLayer, i32)>,
    pub classification: String,
    pub reasoning: String,
}

impl Confidence {
    pub fn from_scores(scores: &[SignalScore]) -> Self {
        let mut layer_avgs: HashMap<SignalLayer, Vec<i32>> = HashMap::new();

        for s in scores {
            layer_avgs.entry(s.layer).or_default().push(s.score);
        }

        let mut layer_scores = Vec::new();
        let mut momentum_sum = 0.0;
        let mut momentum_count = 0;
        let mut distribution_sum = 0.0;
        let mut distribution_count = 0;

        // Extract top_holder score for the distribution-bonus pattern check
        // below. Other per-signal scores (velocity, tx_rate) are re-derived
        // later in this function from the scores Vec where they're needed.
        let mut top_holder_score = 5i32;
        for s in scores {
            if s.signal_type == "top_holder" {
                top_holder_score = s.score;
            }
        }

        for layer in SignalLayer::all() {
            if let Some(vals) = layer_avgs.get(layer) {
                let avg = vals.iter().sum::<i32>() as f64 / vals.len() as f64;
                layer_scores.push((*layer, avg as i32));

                match layer {
                    SignalLayer::Safety => {
                        // Safety layer now means distribution quality
                        distribution_sum += avg;
                        distribution_count += 1;
                    }
                    SignalLayer::OnChain => {
                        // OnChain is structural context — contributes to distribution
                        distribution_sum += avg * 0.5;
                        distribution_count += 1;
                    }
                    SignalLayer::Microstructure | SignalLayer::SmartMoney => {
                        momentum_sum += avg;
                        momentum_count += 1;
                    }
                }
            }
        }

        let coverage = layer_scores.len();
        let momentum = if momentum_count > 0 {
            (momentum_sum / momentum_count as f64) as i32
        } else {
            0
        };
        let distribution = if distribution_count > 0 {
            (distribution_sum / distribution_count as f64) as i32
        } else {
            0
        };

        // Spring score: good distribution + low current activity = coiled potential
        // These are the dormant tokens that have already distributed and are waiting
        // for ignition. High spring + any momentum = the trade.
        // Pattern: SENNA sat at 22.9% top holder for weeks, then spiked.
        //          AVA had 47K holders grinding up with staircase pattern.
        let spring = if distribution > 50 && momentum < 40 {
            // Well distributed, quiet — loaded spring
            (distribution as f64 * 0.8 + 20.0) as i32
        } else if distribution > 50 && momentum > 60 {
            // Well distributed AND active — the surge is happening now
            (distribution as f64 * 0.5 + momentum as f64 * 0.5) as i32
        } else if distribution < 30 {
            // Concentrated — not a spring, just a trap
            (distribution as f64 * 0.3) as i32
        } else {
            // Middle ground
            (distribution as f64 * 0.5) as i32
        };

        // Total score. 2026-05-02 PM retune — backtest of 24h cohort
        // (n=182 STAIRCASE/GRINDER/SPRING, 69 ran ≥1.5x) showed the old
        // 40/30/30 weights structurally clipped runners at 60-67 because
        // `spring` is largely a derivative of `distribution` (e.g.
        // `dist*0.5 + mom*0.5` for STAIRCASE band) — 30% spring weight
        // double-counted distribution and capped well-shaped patterns
        // around 64. Rebalance:
        //   Momentum     50% — the tape is the call
        //   Distribution 40% — survival under selling pressure
        //   Spring       10% — residual signal after de-duping with dist
        let mut total = ((momentum as f64 * 0.50)
            + (distribution as f64 * 0.40)
            + (spring as f64 * 0.10)) as i32;

        // Exceptional distribution bonus: top_holder < 10% is rare and powerful
        // BFiGUx at 2% should score higher than a token at 25%
        if top_holder_score >= 75 {
            total = (total as f64 * 1.10) as i32; // +10% for < 15% top holder
        }
        if top_holder_score >= 90 {
            total = (total as f64 * 1.05) as i32; // additional +5% for < 5% top holder
        }

        // Check for demand congestion and recency signals
        let has_demand_congestion = scores.iter().any(|s| s.signal_type == "demand_congestion");
        let congestion_score = scores
            .iter()
            .find(|s| s.signal_type == "demand_congestion")
            .map(|s| s.score)
            .unwrap_or(0);
        let recency_score = scores
            .iter()
            .find(|s| s.signal_type == "recency")
            .map(|s| s.score)
            .unwrap_or(0);
        let tx_rate_score = scores
            .iter()
            .find(|s| s.signal_type == "tx_rate")
            .map(|s| s.score)
            .unwrap_or(0);

        // Safety veto: any safety signal that encodes a hard-stop
        // (PermanentDelegate, default-frozen mint, non-transferable) gets
        // score = 0 with signal_type in the veto list. If present, classify
        // as UNSAFE and zero the total — no confidence scoring applies.
        let veto_signal = scores.iter().find(|s| {
            s.score == 0
                && matches!(
                    s.signal_type.as_str(),
                    "permanent_delegate" | "default_frozen" | "non_transferable"
                )
        });

        // Crash detector: distributed-but-failing tokens where the wave is
        // breaking. Still-active traffic (>= 10 tpm) with success rate collapsing
        // (< 30%) and velocity decelerating is an actionable exit signal.
        let tx_success_score = scores
            .iter()
            .find(|s| s.signal_type == "tx_success_rate")
            .map(|s| s.score)
            .unwrap_or(100);
        let velocity_score = scores
            .iter()
            .find(|s| s.signal_type == "velocity")
            .map(|s| s.score)
            .unwrap_or(100);
        let has_exit_warning = scores
            .iter()
            .any(|s| s.signal_type == "velocity_exit_warning");
        // tx_success_rate scoring in microstructure: 90=100%, 70=~94%, 15=~22%
        // Using score < 30 as proxy for "success rate under 30%"
        let is_crashing = tx_rate_score >= 30
            && tx_success_score < 30
            && (velocity_score < 30 || has_exit_warning);

        let classification = if let Some(v) = veto_signal {
            format!("UNSAFE:{}", v.signal_type)
        } else if is_crashing {
            "CRASHING".to_string()
        } else if distribution > 50
            && has_demand_congestion
            && congestion_score > 70
            && recency_score >= 60
            && tx_rate_score >= 30
        {
            "GRINDER".to_string()
        } else if momentum > 70 && distribution > 50 {
            "STAIRCASE".to_string()
        } else if momentum > 70 && distribution < 30 {
            "SURGE".to_string()
        } else if momentum < 30 && distribution > 50 {
            "SPRING".to_string()
        } else if momentum < 20 && distribution < 30 {
            "DEAD".to_string()
        } else if momentum > 50 && distribution < 40 {
            "ACTIVE_TRAP".to_string()
        } else {
            "DEVELOPING".to_string()
        };

        // Pattern-classification bonus. STAIRCASE/GRINDER/SPRING are
        // structural pattern matches (mom>70 AND dist>50 etc.). A token
        // that has cleared those branches has earned confidence beyond
        // what the linear weights capture. +8 lifts the 60-67 cluster
        // into the actionable band where the runner cohort actually
        // lives. Applied AFTER classification is determined.
        if matches!(classification.as_str(), "STAIRCASE" | "GRINDER" | "SPRING") {
            total += 8;
        }

        // UNSAFE vetoes override the total score — never let a risky token carry
        // confidence into downstream ranking.
        if classification.starts_with("UNSAFE") {
            total = 0;
        }

        // Reasoning strings carry raw metrics only. No narrative, no hopeful
        // language ("multi-wave potential", "coiled for ignition"). The reader
        // has the numbers and the classification rule — that's the truth.
        let reasoning = match classification.as_str() {
            c if c.starts_with("UNSAFE") => {
                let suffix = c.strip_prefix("UNSAFE:").unwrap_or("unknown");
                format!("UNSAFE · {} · total forced to 0", suffix)
            }
            "CRASHING" => format!(
                "CRASHING · mom {} · dist {} · tpm_score {} · success {} · vel {}",
                momentum, distribution, tx_rate_score, tx_success_score, velocity_score
            ),
            "GRINDER" => format!(
                "GRINDER · dist {} · congestion {} · mom {}",
                distribution, congestion_score, momentum
            ),
            "STAIRCASE" => format!(
                "STAIRCASE · mom {} · dist {} · spring {}",
                momentum, distribution, spring
            ),
            "SURGE" => format!(
                "SURGE · mom {} · dist {} · spring {}",
                momentum, distribution, spring
            ),
            "SPRING" => format!(
                "SPRING · mom {} · dist {} · spring {}",
                momentum, distribution, spring
            ),
            "DEAD" => format!(
                "DEAD · mom {} · dist {} · spring {}",
                momentum, distribution, spring
            ),
            "ACTIVE_TRAP" => format!(
                "ACTIVE_TRAP · mom {} · dist {} · spring {}",
                momentum, distribution, spring
            ),
            _ => format!(
                "DEVELOPING · mom {} · dist {} · spring {}",
                momentum, distribution, spring
            ),
        };

        // Decomposition — expose the breakdown so downstream can show WHY
        // the total landed where it did. Base = weighted sum before bonuses.
        let base_total = ((momentum as f64 * 0.50)
            + (distribution as f64 * 0.40)
            + (spring as f64 * 0.10)) as i32;
        let bonus_pct = if top_holder_score >= 90 {
            15
        } else if top_holder_score >= 75 {
            10
        } else {
            0
        };

        Self {
            total: total.clamp(0, 100),
            base_total: base_total.clamp(0, 100),
            top_holder_bonus_pct: bonus_pct,
            momentum: momentum.clamp(0, 100),
            distribution: distribution.clamp(0, 100),
            spring: spring.clamp(0, 100),
            coverage,
            layer_scores,
            classification,
            reasoning,
        }
    }
}

// -- Token Analysis Orchestrator --

#[derive(Debug, Clone, Serialize)]
pub struct TokenAnalysis {
    pub address: String,
    pub mint_authority: Option<String>,
    pub freeze_authority: Option<String>,
    pub is_token_2022: bool,
    pub supply_ui: f64,
    pub decimals: u8,
    pub holder_count: usize,
    pub top_holder_pct: f64,
    pub top5_pct: f64,
    pub top10_pct: f64,
    pub tx_rate: f64,
    pub velocity: f64,
    pub recent_tx_count: usize,
    pub scores: Vec<SignalScore>,
    pub confidence: Confidence,
    pub delta: Option<crate::db::TokenDelta>,
    /// Launch forensics, populated from the latest snapshot row. Computed
    /// asynchronously and refreshed hourly per mint, so values may lag the
    /// rest of the analysis by up to 1h. Zero when never computed (fresh token).
    pub bundle_pct: f64,
    pub sniper_pct: f64,
    pub insider_pct: f64,
    /// Smart-money proxy: count of top-20 holders that hold at least one
    /// operator-curated reference mint. Zero when no refs are seeded.
    pub smart_money_count: i32,
    /// When forensics last computed (unix seconds). 0 = never measured —
    /// gates treat 0 as "data unavailable, do not fire" so calls never go
    /// out on tokens whose bundle/sniper/insider have defaulted-to-zero
    /// rather than been measured.
    pub forensics_computed_at: i64,
    /// Trailing 1h buy count (DexScreener `txns.h1.buys`).
    pub buys_h1: i32,
    /// Trailing 1h sell count.
    pub sells_h1: i32,
}

/// Run all signal layers against a live token, store snapshot, compute delta
pub async fn analyze_token(
    rpc: &Arc<RpcRouter>,
    mint_address: &str,
    db: Option<&Arc<crate::db::Db>>,
    prefetched_market: Option<crate::market::MarketData>,
) -> Result<TokenAnalysis> {
    let safety_checker = safety::SafetyChecker::new();
    let micro = microstructure::MicrostructureAnalyzer::new();
    let onchain_analyzer = onchain::OnChainAnalyzer::new();

    let mut all_scores: Vec<SignalScore> = Vec::new();

    // 1. Fetch mint account info
    let account = rpc.get_account_info(mint_address).await?;
    let mint_info = account
        .as_ref()
        .and_then(|a| parse_mint_account(mint_address, &a.data, &a.owner));

    let (mint_authority, freeze_authority, is_token_2022, supply_raw, decimals) =
        if let Some(ref mint) = mint_info {
            all_scores.extend(safety_checker.analyze_mint(mint));
            (
                mint.mint_authority.clone(),
                mint.freeze_authority.clone(),
                mint.is_token_2022,
                mint.supply,
                mint.decimals,
            )
        } else {
            all_scores.push(SignalScore::new(
                SignalLayer::Safety,
                "mint_account",
                5,
                "Could not parse mint account — may not be a valid SPL token",
            ));
            (None, None, false, 0, 0)
        };

    // 2. Fetch token supply
    let supply_info = rpc.get_token_supply(mint_address).await.ok();
    let supply_ui = supply_info
        .as_ref()
        .map(|s| s.ui_amount)
        .unwrap_or_else(|| {
            if decimals > 0 {
                supply_raw as f64 / 10f64.powi(decimals as i32)
            } else {
                supply_raw as f64
            }
        });

    if let (Some(ref supply), Some(ref mint)) = (&supply_info, &mint_info) {
        all_scores.extend(onchain_analyzer.analyze_supply(supply, mint));
    }

    // 3. Fetch largest holders — propagate RPC errors so we don't persist
    // zero-data snapshots that would misclassify the token as DEAD.
    let holders = rpc
        .get_token_largest_accounts(mint_address)
        .await
        .with_context(|| format!("fetch largest accounts for {}", mint_address))?;
    let holder_count = holders.len();
    let top_holder_pct = if !holders.is_empty() && supply_ui > 0.0 {
        (holders[0].ui_amount / supply_ui) * 100.0
    } else {
        0.0
    };
    let top5_pct: f64 = holders
        .iter()
        .take(5)
        .map(|h| {
            if supply_ui > 0.0 {
                (h.ui_amount / supply_ui) * 100.0
            } else {
                0.0
            }
        })
        .sum();
    let top10_pct: f64 = holders
        .iter()
        .take(10)
        .map(|h| {
            if supply_ui > 0.0 {
                (h.ui_amount / supply_ui) * 100.0
            } else {
                0.0
            }
        })
        .sum();

    all_scores.extend(safety_checker.analyze_holders(&holders, supply_ui));

    // 4. Fetch recent transactions — propagate RPC errors for the same reason
    // as the holders fetch above.
    let signatures = rpc
        .get_recent_signatures(mint_address, 50)
        .await
        .with_context(|| format!("fetch signatures for {}", mint_address))?;
    let recent_tx_count = signatures.len();

    // Compute raw metrics for snapshot storage and cross-layer synthesis
    let metrics = micro.compute_metrics(&signatures);
    all_scores.extend(micro.analyze_activity(&signatures));
    all_scores.extend(onchain_analyzer.analyze_history_depth(&signatures));

    // 4b. Fetch DexScreener market data — gives us price, mcap, liquidity, volume,
    // buy/sell split, and host DEX. Signals keyed purely to DEX-agnostic holder
    // state can't answer "is this real liquidity?" or "is there buy pressure?";
    // this is where those answers come from. Errors are non-fatal — a freshly
    // deployed mint may not be indexed yet.
    let market = if let Some(m) = prefetched_market {
        Some(m)
    } else {
        match crate::market::get_market(mint_address).await {
            Ok(m) => m,
            Err(e) => {
                tracing::debug!("dexscreener fetch failed for {}: {}", mint_address, e);
                None
            }
        }
    };
    if let Some(m) = &market {
        // Liquidity floor — below $5k is noise, above $50k is healthy.
        // Skip entirely when liquidity is 0 — that means DexScreener failed to
        // return data (not a real zero), and penalising momentum would cause
        // false DEAD classifications.
        if m.liquidity_usd > 0.0 {
            let liq_score = if m.liquidity_usd < 2_000.0 {
                5
            } else if m.liquidity_usd < 5_000.0 {
                20
            } else if m.liquidity_usd < 20_000.0 {
                55
            } else if m.liquidity_usd < 50_000.0 {
                75
            } else {
                85
            };
            all_scores.push(SignalScore::new(
                SignalLayer::Microstructure,
                "liquidity_depth",
                liq_score,
                &format!("liq ${:.0}k", m.liquidity_usd / 1000.0),
            ));
        }

        // Buy/sell pressure — net inflow over the last hour.
        let pressure = m.buy_pressure();
        let total_txns = m.buys_h1 + m.sells_h1;
        if total_txns >= 20 {
            let score = (pressure * 100.0) as i32;
            all_scores.push(SignalScore::new(
                SignalLayer::Microstructure,
                "buy_pressure_h1",
                score,
                &format!(
                    "buys {} vs sells {} ({:.0}%)",
                    m.buys_h1,
                    m.sells_h1,
                    pressure * 100.0
                ),
            ));
        }

        // LP lock/burn hint — DexScreener occasionally tags pairs with a
        // "burnt" or "locked" label when it can detect the LP tokens sitting
        // at a known burn/lock address. It's not comprehensive (most pump.fun
        // pairs carry no label), so we treat it as a bonus when present, not
        // a veto when absent.
        let lp_marker = m.labels.iter().any(|l| {
            let l = l.to_lowercase();
            l.contains("burn") || l.contains("lock")
        });
        if lp_marker {
            all_scores.push(SignalScore::new(
                SignalLayer::Safety,
                "lp_secured",
                85,
                &format!("LP labeled {:?} — liquidity de-risked", m.labels),
            ));
        }

        // Freshly graduated from Pump.fun to a full AMM — single biggest
        // inflection in a Pump.fun token's life cycle. Boost confidence.
        if m.is_graduated() {
            let age_hours = if m.pair_created_at > 0 {
                (chrono::Utc::now().timestamp_millis() - m.pair_created_at) as f64 / 3_600_000.0
            } else {
                f64::INFINITY
            };
            if age_hours < 6.0 {
                all_scores.push(SignalScore::new(
                    SignalLayer::Microstructure,
                    "post_graduation_fresh",
                    85,
                    &format!("graduated to {} {:.1}h ago", m.pair_dex, age_hours),
                ));
            }
        }
    }

    // 5. Cross-layer signal synthesis — patterns that emerge from combining layers
    let has_deep_holders = holder_count >= 15;
    let has_good_distribution = top_holder_pct < 30.0;

    // Demand congestion: high failure + deep holders + good distribution = bullish
    if metrics.failure_rate_pct > 20.0 && has_deep_holders && has_good_distribution {
        let congestion_score = (70.0 + (metrics.failure_rate_pct / 100.0 * 30.0)).min(95.0) as i32;
        all_scores.push(SignalScore::new(
            SignalLayer::Microstructure,
            "demand_congestion",
            congestion_score,
            &format!(
                "DEMAND CONGESTION: {:.0}% tx failure on distributed token (top holder {:.1}%) — crowd fighting to enter",
                metrics.failure_rate_pct, top_holder_pct
            ),
        ));
    } else if metrics.failure_rate_pct > 30.0 && !has_good_distribution {
        all_scores.push(SignalScore::new(
            SignalLayer::Microstructure,
            "congestion_warning",
            30,
            &format!(
                "High failure rate ({:.0}%) on concentrated token (top holder {:.1}%) — congestion without deep demand",
                metrics.failure_rate_pct, top_holder_pct
            ),
        ));
    }

    // Spring ignition: good distribution + velocity picking up
    if has_good_distribution && has_deep_holders && metrics.velocity_multiplier > 1.5 {
        all_scores.push(SignalScore::new(
            SignalLayer::Microstructure,
            "spring_ignition",
            90,
            &format!(
                "SPRING IGNITING: velocity {:.1}x on distributed token (top holder {:.1}%) — potential wave forming",
                metrics.velocity_multiplier, top_holder_pct
            ),
        ));
    }

    // Velocity exit warning: velocity dropping below 1.0x is a leading exit signal
    // This fires before concentration delta shows re-concentration
    if metrics.velocity_multiplier < 0.5 && metrics.tx_per_minute > 1.0 {
        all_scores.push(SignalScore::new(
            SignalLayer::Microstructure,
            "velocity_exit_warning",
            20,
            &format!(
                "EXIT WARNING: velocity {:.1}x (decelerating) with {:.1} tx/min — momentum dying",
                metrics.velocity_multiplier, metrics.tx_per_minute
            ),
        ));
    }

    // 6. Aggregate confidence
    let confidence = Confidence::from_scores(&all_scores);

    // Raw values for snapshot (not scores)
    let tx_rate = metrics.tx_per_minute;
    let velocity = metrics.velocity_multiplier;

    // 6. Store snapshot and compute delta
    let now = chrono::Utc::now().timestamp();
    let current_snapshot = crate::db::TokenSnapshot {
        token_address: mint_address.to_string(),
        top_holder_pct,
        top5_pct,
        top10_pct,
        holder_count: holder_count as i32,
        tx_rate,
        velocity,
        momentum: confidence.momentum,
        distribution: confidence.distribution,
        spring: confidence.spring,
        classification: confidence.classification.clone(),
        confidence: confidence.total,
        timestamp: now,
        price_usd: market.as_ref().map(|m| m.price_usd).unwrap_or(0.0),
        mcap_usd: market.as_ref().map(|m| m.mcap_usd).unwrap_or(0.0),
        liquidity_usd: market.as_ref().map(|m| m.liquidity_usd).unwrap_or(0.0),
        volume_h1_usd: market.as_ref().map(|m| m.volume_h1_usd).unwrap_or(0.0),
        buys_h1: market.as_ref().map(|m| m.buys_h1).unwrap_or(0),
        sells_h1: market.as_ref().map(|m| m.sells_h1).unwrap_or(0),
        price_change_h1: market.as_ref().map(|m| m.price_change_h1).unwrap_or(0.0),
        pair_dex: market
            .as_ref()
            .map(|m| m.pair_dex.clone())
            .unwrap_or_default(),
        // Forensics fields default to 0 here; the async refresh writes
        // real values into the row via update_launch_forensics. Reading
        // them via get_latest_forensics into TokenAnalysis happens later
        // in this same function.
        bundle_pct: 0.0,
        sniper_pct: 0.0,
        insider_pct: 0.0,
        smart_money_count: 0,
        forensics_computed_at: 0,
    };

    let delta = if let Some(db) = db {
        // 2026-05-02 PM: removed the "top-1 holder = deployer" heuristic
        // that lived here previously. Top holder is the largest CURRENT
        // owner — that's not the creator wallet. The actual deployer is
        // the wallet that called pump.fun's create instruction, which
        // PumpPortal exposes as `traderPublicKey` on the new-token
        // event; we now capture that in main.rs's pumpportal sink. The
        // prior heuristic produced 100% one-shot deployer rows (each
        // mint had a different "top holder"), making cluster scoring
        // impossible.

        // First-time sniper cohort capture: the top-20 holders at first-scan
        // are our best proxy for the early-buyer cohort on a pump.fun mint
        // we just discovered. Resolve each token account to its owner wallet
        // and persist. Skipped silently for tokens that already have a
        // cohort recorded.
        if !holders.is_empty() && !db.has_sniper_cohort(mint_address).unwrap_or(true) {
            let addrs: Vec<String> = holders.iter().take(20).map(|h| h.address.clone()).collect();
            if let Ok(batch) = rpc.get_multiple_token_account_owners(&addrs).await {
                let owners: Vec<String> = batch.into_iter().flatten().collect();
                if !owners.is_empty() {
                    let _ = db.insert_snipers(mint_address, &owners);
                }
            }
        }

        // Detect graduation: prior snapshot was Pump.fun, current is a full AMM.
        if let Ok(Some(prev)) = db.get_previous_snapshot(mint_address) {
            let was_pumpfun = matches!(prev.pair_dex.as_str(), "pumpswap" | "pumpfun");
            let is_amm = matches!(
                current_snapshot.pair_dex.as_str(),
                "raydium" | "meteora" | "orca"
            );
            if was_pumpfun && is_amm {
                let mcap_k = current_snapshot.mcap_usd / 1000.0;
                let _ = db.insert_alert(
                    "graduation",
                    Some(mint_address),
                    &format!(
                        "GRADUATION {} · {} → {} · mcap ${:.0}k · liq ${:.0}k",
                        mint_address,
                        prev.pair_dex,
                        current_snapshot.pair_dex,
                        mcap_k,
                        current_snapshot.liquidity_usd / 1000.0,
                    ),
                    85,
                );

                // Fire a background scout — deployer profile + website text
                // land as a follow-up `graduation_scout` alert a few seconds
                // later. Kept off the hot path so graduation latency stays
                // tight even when a website is slow.
                let mint_clone = mint_address.to_string();
                let rpc_clone = rpc.clone();
                let db_clone = db.clone();
                tokio::spawn(async move {
                    let report = match crate::scout::scout(&mint_clone, &rpc_clone, &db_clone).await
                    {
                        Ok(r) => r,
                        Err(_) => return,
                    };
                    let mut parts: Vec<String> = Vec::new();
                    if let Some(d) = &report.deployer {
                        let age = d
                            .seconds_since_last_tx
                            .map(|s| {
                                if s < 3600 {
                                    format!("{}m", s / 60)
                                } else if s < 86_400 {
                                    format!("{}h", s / 3600)
                                } else {
                                    format!("{}d", s / 86_400)
                                }
                            })
                            .unwrap_or_else(|| "—".to_string());
                        parts.push(format!(
                            "deployer sold {:.1}% · 7d tx {} · last {}",
                            d.pct_sold, d.tx_count_7d, age
                        ));
                    }
                    if let Some(w) = &report.website {
                        // Include only first 240 chars of stripped text as a
                        // teaser — full text is available via /scout.
                        let snippet: String = w
                            .text
                            .chars()
                            .take(240)
                            .collect::<String>()
                            .replace('\n', " ");
                        parts.push(format!("site {} · \"{}…\"", w.url, snippet));
                    } else if !report.socials.is_empty() {
                        parts.push(format!("socials: {}", report.socials.len()));
                    }
                    if parts.is_empty() {
                        return;
                    }
                    let msg = format!("GRADUATION_SCOUT {} · {}", mint_clone, parts.join(" · "));
                    let _ = db_clone.insert_alert("graduation_scout", Some(&mint_clone), &msg, 82);
                });
            }
        }

        let d = db
            .compute_delta(mint_address, &current_snapshot)
            .ok()
            .flatten();
        let _ = db.insert_snapshot(&current_snapshot);

        // Persist the top-20 addresses+balances JSON on the just-inserted
        // snapshot so holder_evolution can diff composition over time.
        // Kept separate from the INSERT to avoid adding yet another
        // `params!` positional; a trailing UPDATE is cheaper than widening
        // the hot-path INSERT statement.
        if !holders.is_empty() {
            let json = serde_json::to_string(
                &holders
                    .iter()
                    .take(20)
                    .map(|h| {
                        serde_json::json!({
                            "address": h.address,
                            "ui_amount": h.ui_amount,
                        })
                    })
                    .collect::<Vec<_>>(),
            )
            .unwrap_or_else(|_| "[]".to_string());
            let _ = db.update_latest_top_holders_json(mint_address, &json);
        }

        // Dev-wallet sell detector. Threshold raised from 10% → 30% based on
        // mining of 1717 dev_selling alerts over 7d (2026-04-29 session):
        // 30m_mean post-alert was +10.5% (BULLISH on average), with 35%
        // gaining >15% vs 36% dropping >15% — i.e., light dev-selling is
        // noise. Only severe exits (>=30% drop) carry directional weight.
        // The settle phase already filters with `has_recent_severe_alert`
        // (conf >= 90, drop >= 40); raising the insert threshold prevents
        // the alerts table from filling with non-predictive 10-20% drops.
        if let Ok(Some((dep_addr, initial_bal))) = db.get_deployer(mint_address) {
            if !dep_addr.is_empty() && initial_bal > 0.0 {
                let current_bal = holders
                    .iter()
                    .find(|h| h.address == dep_addr)
                    .map(|h| h.ui_amount)
                    .unwrap_or(0.0);
                let drop_pct = (1.0 - current_bal / initial_bal) * 100.0;
                if drop_pct >= 30.0 {
                    let _ = db.insert_alert(
                        "dev_selling",
                        Some(mint_address),
                        &format!(
                            "DEV_SELLING {} · deployer down {:.1}% ({:.0} → {:.0})",
                            mint_address, drop_pct, initial_bal, current_bal,
                        ),
                        (50.0 + drop_pct).min(95.0) as i32,
                    );
                }
            }
        }

        // Launch forensics refresh — fire-and-forget background task. Cached
        // on the snapshot row; only rerun if the persisted stamp is older than
        // 1 hour. Bounded ~25-45 RPC calls per token; spawned async so it
        // never gates the analyze() return.
        let now = chrono::Utc::now().timestamp();
        let (.., forensics_age) = db
            .get_latest_forensics(mint_address)
            .unwrap_or((0.0, 0.0, 0.0, 0, 0));
        if now - forensics_age >= 3600 {
            let mint_owned = mint_address.to_string();
            // Single-flight guard: only spawn if no compute task already running for this mint.
            let claimed = {
                let mut set = FORENSICS_IN_FLIGHT.lock().unwrap();
                set.insert(mint_owned.clone())
            };
            if claimed {
            let db_clone = db.clone();
            let rpc_clone = rpc.clone();
            tokio::spawn(async move {
                let mint_short = &mint_owned[..mint_owned.len().min(12)];
                tracing::info!("launch_forensics: {} starting compute", mint_short);
                // Hard cap: each compute() does up to ~45 RPC calls. With
                // 30s per-call timeouts that could in principle take 20+
                // minutes if every call retries through every endpoint.
                // 90s ceiling keeps the spawned task bounded so it doesn't
                // stack up across many tokens during an RPC degraded period.
                let compute_fut = crate::launch_forensics::compute(
                    &mint_owned,
                    &db_clone,
                    &rpc_clone,
                );
                // 180s ceiling. Even with the parallelized fan-out, the
                // pre-fanout serial chain (get_token_supply → largest_accounts
                // → owner_resolution) is 3 × up-to-30s = 90s worst case
                // before the parallel block runs. Add the parallel max of
                // 30-60s, total can hit 120-150s during RPC-degraded periods
                // (429s + endpoint sidelining cascade). 180s gives headroom
                // without unbounding the task.
                let f = match tokio::time::timeout(
                    std::time::Duration::from_secs(180),
                    compute_fut,
                )
                .await
                {
                    Ok(Ok(f)) => Some(f),
                    Ok(Err(e)) => {
                        tracing::warn!(
                            "launch_forensics: {} compute err: {}",
                            mint_owned, e
                        );
                        None
                    }
                    Err(_) => {
                        tracing::warn!(
                            "launch_forensics: {} timed out after 180s",
                            mint_owned
                        );
                        None
                    }
                };
                if let Some(f) = f {
                    let stamp = chrono::Utc::now().timestamp();
                    if let Err(e) = db_clone.update_launch_forensics(
                        &mint_owned,
                        f.bundle_pct,
                        f.sniper_pct,
                        f.insider_pct,
                        f.smart_money_count,
                        stamp,
                    ) {
                        tracing::warn!("launch_forensics: persist {} failed: {}", mint_owned, e);
                    } else {
                        tracing::info!(
                            "launch_forensics: {} bundle={:.1}% sniper={:.1}% insider={:.1}% smart={}",
                            &mint_owned[..mint_owned.len().min(12)],
                            f.bundle_pct, f.sniper_pct, f.insider_pct, f.smart_money_count
                        );
                    }
                }
                FORENSICS_IN_FLIGHT.lock().unwrap().remove(&mint_owned);
            });
            }
        }

        d
    } else {
        None
    };

    // Read previously-persisted forensics. The async refresh spawned above
    // populates these on its own cadence; reading here lets should_signal
    // gate against them without waiting for compute() to finish.
    let (bundle_pct, sniper_pct, insider_pct, smart_money_count, forensics_computed_at) =
        match db {
            Some(d) => d
                .get_latest_forensics(mint_address)
                .unwrap_or((0.0, 0.0, 0.0, 0, 0)),
            None => (0.0, 0.0, 0.0, 0, 0),
        };

    Ok(TokenAnalysis {
        address: mint_address.to_string(),
        mint_authority,
        freeze_authority,
        is_token_2022,
        supply_ui,
        decimals,
        holder_count,
        top_holder_pct,
        top5_pct,
        top10_pct,
        tx_rate,
        velocity,
        recent_tx_count,
        scores: all_scores,
        confidence,
        delta,
        bundle_pct,
        sniper_pct,
        insider_pct,
        smart_money_count,
        forensics_computed_at,
        buys_h1: market.as_ref().map(|m| m.buys_h1).unwrap_or(0),
        sells_h1: market.as_ref().map(|m| m.sells_h1).unwrap_or(0),
    })
}
