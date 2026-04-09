use super::{SignalLayer, SignalScore};
use crate::ingester::{MintInfo, SignatureInfo, TokenSupplyInfo};

pub struct OnChainAnalyzer;

impl OnChainAnalyzer {
    pub fn new() -> Self {
        Self
    }

    /// Analyze on-chain supply characteristics
    pub fn analyze_supply(&self, supply: &TokenSupplyInfo, mint: &MintInfo) -> Vec<SignalScore> {
        let mut scores = Vec::new();

        // Decimals check — standard is 6 or 9 for Solana tokens
        let decimals_score = match supply.decimals {
            6 | 9 => 80,
            5 | 8 => 70,
            0..=4 => 40,
            _ => 50,
        };

        scores.push(SignalScore::new(
            SignalLayer::OnChain,
            "decimals",
            decimals_score,
            &format!("{} decimals", supply.decimals),
        ));

        // Supply magnitude — extremely large or small supply can be a flag
        let supply_score = if supply.ui_amount > 0.0 && supply.ui_amount < 1.0 {
            30 // Suspiciously small
        } else if supply.ui_amount > 1_000_000_000_000.0 {
            40 // Extremely inflated supply
        } else {
            75 // Normal range
        };

        scores.push(SignalScore::new(
            SignalLayer::OnChain,
            "supply",
            supply_score,
            &format!("Total supply: {:.2}", supply.ui_amount),
        ));

        scores
    }

    /// Analyze token age from its transaction history
    pub fn analyze_age(&self, signatures: &[SignatureInfo]) -> Vec<SignalScore> {
        let mut scores = Vec::new();

        if let Some(oldest) = signatures.last() {
            if let Some(block_time) = oldest.block_time {
                let now = chrono::Utc::now().timestamp();
                let age_seconds = now - block_time;
                let age_hours = age_seconds as f64 / 3600.0;

                // Very new tokens are higher risk
                let age_score = if age_hours < 1.0 {
                    30 // Less than 1 hour old
                } else if age_hours < 24.0 {
                    50 // Less than a day
                } else if age_hours < 168.0 {
                    70 // Less than a week
                } else {
                    85 // Over a week — survived initial period
                };

                let age_str = if age_hours < 1.0 {
                    format!("{:.0}m old", age_hours * 60.0)
                } else if age_hours < 24.0 {
                    format!("{:.1}h old", age_hours)
                } else {
                    format!("{:.0}d old", age_hours / 24.0)
                };

                scores.push(SignalScore::new(
                    SignalLayer::OnChain,
                    "token_age",
                    age_score,
                    &age_str,
                ));
            }
        } else {
            scores.push(SignalScore::new(
                SignalLayer::OnChain,
                "token_age",
                10,
                "No transaction history found",
            ));
        }

        scores
    }
}
