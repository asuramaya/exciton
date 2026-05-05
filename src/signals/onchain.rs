use super::{SignalLayer, SignalScore};
use crate::ingester::{MintInfo, SignatureInfo, TokenSupplyInfo};

pub struct OnChainAnalyzer;

impl Default for OnChainAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

impl OnChainAnalyzer {
    pub fn new() -> Self {
        Self
    }

    /// Analyze on-chain supply characteristics
    pub fn analyze_supply(&self, supply: &TokenSupplyInfo, _mint: &MintInfo) -> Vec<SignalScore> {
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

    /// Analyze transaction history depth — how far back does observable activity go?
    /// Note: getSignaturesForAddress returns most recent N, not the full history.
    /// A short window with high activity = actively traded. A short window with low activity = new or dying.
    pub fn analyze_history_depth(&self, signatures: &[SignatureInfo]) -> Vec<SignalScore> {
        let mut scores = Vec::new();

        if signatures.is_empty() {
            scores.push(SignalScore::new(
                SignalLayer::OnChain,
                "history_depth",
                10,
                "No transaction history found",
            ));
            return scores;
        }

        // How much time does our sample window cover?
        let newest_time = signatures.first().and_then(|s| s.block_time).unwrap_or(0);
        let oldest_time = signatures.last().and_then(|s| s.block_time).unwrap_or(0);
        let window_seconds = (newest_time - oldest_time).abs();
        let window_hours = window_seconds as f64 / 3600.0;

        // If 50 transactions span a long window = moderate activity on an established token
        // If 50 transactions span seconds = extremely active (just launched or pumping)
        let (depth_score, depth_label) = if window_hours > 24.0 {
            (
                85,
                format!(
                    "{:.0} txs over {:.0}d — established activity",
                    signatures.len(),
                    window_hours / 24.0
                ),
            )
        } else if window_hours > 1.0 {
            (
                70,
                format!(
                    "{:.0} txs over {:.1}h — active trading",
                    signatures.len(),
                    window_hours
                ),
            )
        } else if window_seconds > 300 {
            (
                55,
                format!(
                    "{:.0} txs over {:.0}m — moderate activity",
                    signatures.len(),
                    window_seconds as f64 / 60.0
                ),
            )
        } else if window_seconds > 30 {
            (
                40,
                format!(
                    "{:.0} txs in {:.0}s — very high activity, possible launch/pump",
                    signatures.len(),
                    window_seconds
                ),
            )
        } else {
            (
                25,
                format!(
                    "{:.0} txs in <30s — extreme burst, likely just launched",
                    signatures.len()
                ),
            )
        };

        scores.push(SignalScore::new(
            SignalLayer::OnChain,
            "history_depth",
            depth_score,
            &depth_label,
        ));

        scores
    }
}
