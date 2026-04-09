pub mod microstructure;
pub mod onchain;
pub mod safety;
pub mod smartmoney;

use crate::ingester::{parse_mint_account, RpcRouter};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

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

    pub fn weight(&self) -> f64 {
        match self {
            SignalLayer::Safety => 0.30,
            SignalLayer::OnChain => 0.25,
            SignalLayer::Microstructure => 0.25,
            SignalLayer::SmartMoney => 0.20,
        }
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

        // Extract specific signal scores for pattern detection
        let mut velocity_score = 50i32;
        let mut holder_count_score = 20i32;
        let mut top_holder_score = 5i32;
        let mut tx_rate_score = 15i32;

        for s in scores {
            match s.signal_type.as_str() {
                "velocity" => velocity_score = s.score,
                "holder_count" => holder_count_score = s.score,
                "top_holder" => top_holder_score = s.score,
                "tx_rate" => tx_rate_score = s.score,
                _ => {}
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

        // Total score: what matters is the combination
        // Momentum (40%) — is something happening right now?
        // Distribution (30%) — can this survive selling pressure?
        // Spring (30%) — is this coiled for a move?
        let total = ((momentum as f64 * 0.40)
            + (distribution as f64 * 0.30)
            + (spring as f64 * 0.30)) as i32;

        // Classification based on the pattern taxonomy
        let classification = if momentum > 70 && distribution > 50 {
            "STAIRCASE".to_string() // Active + distributed = AVA/GAYCOIN pattern
        } else if momentum > 70 && distribution < 30 {
            "SURGE".to_string() // Explosive + concentrated = rideable trap
        } else if momentum < 30 && distribution > 50 {
            "SPRING".to_string() // Quiet + distributed = loaded, waiting for ignition
        } else if momentum < 20 && distribution < 30 {
            "DEAD".to_string() // Nothing happening, concentrated
        } else if momentum > 50 && distribution < 40 {
            "ACTIVE_TRAP".to_string() // Activity on a still-concentrated token
        } else {
            "DEVELOPING".to_string() // In between states
        };

        let reasoning = match classification.as_str() {
            "STAIRCASE" => format!(
                "STAIRCASE — momentum {}, distribution {}, spring {} — active with deep holder base, multi-wave potential",
                momentum, distribution, spring
            ),
            "SURGE" => format!(
                "SURGE — momentum {}, distribution {}, spring {} — explosive activity on concentrated token, window may be open",
                momentum, distribution, spring
            ),
            "SPRING" => format!(
                "SPRING — momentum {}, distribution {}, spring {} — distributed and quiet, coiled for potential ignition",
                momentum, distribution, spring
            ),
            "DEAD" => format!(
                "DEAD — momentum {}, distribution {}, spring {} — concentrated with no activity",
                momentum, distribution, spring
            ),
            "ACTIVE_TRAP" => format!(
                "ACTIVE TRAP — momentum {}, distribution {}, spring {} — activity but still concentrated, watch for distribution",
                momentum, distribution, spring
            ),
            _ => format!(
                "DEVELOPING — momentum {}, distribution {}, spring {} — between states, watch for classification change",
                momentum, distribution, spring
            ),
        };

        Self {
            total: total.clamp(0, 100),
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
}

/// Run all signal layers against a live token, store snapshot, compute delta
pub async fn analyze_token(rpc: &Arc<RpcRouter>, mint_address: &str, db: Option<&Arc<crate::db::Db>>) -> Result<TokenAnalysis> {
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

    // 3. Fetch largest holders
    let holders = rpc
        .get_token_largest_accounts(mint_address)
        .await
        .unwrap_or_default();
    let holder_count = holders.len();
    let top_holder_pct = if !holders.is_empty() && supply_ui > 0.0 {
        (holders[0].ui_amount / supply_ui) * 100.0
    } else {
        0.0
    };
    let top5_pct: f64 = holders.iter().take(5).map(|h| {
        if supply_ui > 0.0 { (h.ui_amount / supply_ui) * 100.0 } else { 0.0 }
    }).sum();
    let top10_pct: f64 = holders.iter().take(10).map(|h| {
        if supply_ui > 0.0 { (h.ui_amount / supply_ui) * 100.0 } else { 0.0 }
    }).sum();

    all_scores.extend(safety_checker.analyze_holders(&holders, supply_ui));

    // 4. Fetch recent transactions
    let signatures = rpc
        .get_recent_signatures(mint_address, 50)
        .await
        .unwrap_or_default();
    let recent_tx_count = signatures.len();

    // Compute raw metrics for snapshot storage and cross-layer synthesis
    let metrics = micro.compute_metrics(&signatures);
    all_scores.extend(micro.analyze_activity(&signatures));
    all_scores.extend(onchain_analyzer.analyze_history_depth(&signatures));

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
    };

    let delta = if let Some(db) = db {
        let d = db.compute_delta(mint_address, &current_snapshot).ok().flatten();
        let _ = db.insert_snapshot(&current_snapshot);
        d
    } else {
        None
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
    })
}
