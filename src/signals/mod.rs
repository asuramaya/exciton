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
    pub coverage: usize,
    pub layer_scores: Vec<(SignalLayer, i32)>,
    pub reasoning: String,
}

impl Confidence {
    pub fn from_scores(scores: &[SignalScore]) -> Self {
        let mut layer_avgs: HashMap<SignalLayer, Vec<i32>> = HashMap::new();

        for s in scores {
            layer_avgs.entry(s.layer).or_default().push(s.score);
        }

        let mut layer_scores = Vec::new();
        let mut weighted_sum = 0.0;
        let mut total_weight = 0.0;

        for layer in SignalLayer::all() {
            if let Some(vals) = layer_avgs.get(layer) {
                let avg = vals.iter().sum::<i32>() as f64 / vals.len() as f64;
                layer_scores.push((*layer, avg as i32));
                weighted_sum += avg * layer.weight();
                total_weight += layer.weight();
            }
        }

        let coverage = layer_scores.len();
        let coverage_penalty = coverage as f64 / SignalLayer::all().len() as f64;
        let raw_total = if total_weight > 0.0 {
            weighted_sum / total_weight
        } else {
            0.0
        };
        let total = (raw_total * coverage_penalty) as i32;

        Self {
            total: total.clamp(0, 100),
            coverage,
            layer_scores,
            reasoning: format!(
                "{} of {} layers reporting, weighted score {}",
                coverage,
                SignalLayer::all().len(),
                total
            ),
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
    pub recent_tx_count: usize,
    pub scores: Vec<SignalScore>,
    pub confidence: Confidence,
}

/// Run all signal layers against a live token
pub async fn analyze_token(rpc: &Arc<RpcRouter>, mint_address: &str) -> Result<TokenAnalysis> {
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

    all_scores.extend(safety_checker.analyze_holders(&holders, supply_ui));

    // 4. Fetch recent transactions
    let signatures = rpc
        .get_recent_signatures(mint_address, 50)
        .await
        .unwrap_or_default();
    let recent_tx_count = signatures.len();

    all_scores.extend(micro.analyze_activity(&signatures));
    all_scores.extend(onchain_analyzer.analyze_age(&signatures));

    // 5. Aggregate confidence
    let confidence = Confidence::from_scores(&all_scores);

    Ok(TokenAnalysis {
        address: mint_address.to_string(),
        mint_authority,
        freeze_authority,
        is_token_2022,
        supply_ui,
        decimals,
        holder_count,
        top_holder_pct,
        recent_tx_count,
        scores: all_scores,
        confidence,
    })
}
