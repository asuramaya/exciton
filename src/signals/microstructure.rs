use super::{SignalLayer, SignalScore};
use crate::ingester::SignatureInfo;

pub struct MicrostructureAnalyzer;

impl MicrostructureAnalyzer {
    pub fn new() -> Self {
        Self
    }

    /// Analyze recent transaction activity for a token
    pub fn analyze_activity(&self, signatures: &[SignatureInfo]) -> Vec<SignalScore> {
        let mut scores = Vec::new();

        if signatures.is_empty() {
            scores.push(SignalScore::new(
                SignalLayer::Microstructure,
                "activity",
                10,
                "No recent transactions — token may be dead",
            ));
            return scores;
        }

        let total_txs = signatures.len();
        let successful_txs = signatures.iter().filter(|s| !s.err).count();
        let failed_txs = total_txs - successful_txs;
        let success_rate = if total_txs > 0 {
            (successful_txs as f64 / total_txs as f64) * 100.0
        } else {
            0.0
        };

        // Transaction rate (tx per minute) — more meaningful than raw count
        let time_window = time_span(signatures);
        let tx_per_minute = if time_window > 0 {
            (total_txs as f64 / time_window as f64) * 60.0
        } else {
            total_txs as f64 // all in same second = very fast
        };

        let volume_score = if tx_per_minute > 30.0 {
            95 // Extremely active
        } else if tx_per_minute > 10.0 {
            80
        } else if tx_per_minute > 3.0 {
            65
        } else if tx_per_minute > 1.0 {
            50
        } else if tx_per_minute > 0.1 {
            30
        } else {
            15 // Nearly dead
        };

        scores.push(SignalScore::new(
            SignalLayer::Microstructure,
            "tx_rate",
            volume_score,
            &format!("{:.1} tx/min ({} txs over {}s)", tx_per_minute, total_txs, time_window),
        ));

        // Success rate — high failure rate can indicate honeypot or congestion
        let success_score = if success_rate > 95.0 {
            90
        } else if success_rate > 85.0 {
            70
        } else if success_rate > 70.0 {
            40
        } else {
            15
        };

        scores.push(SignalScore::new(
            SignalLayer::Microstructure,
            "tx_success_rate",
            success_score,
            &format!(
                "{:.1}% success rate ({} failed of {})",
                success_rate, failed_txs, total_txs
            ),
        ));

        // Recency — how recent is the last transaction?
        if let Some(latest) = signatures.first() {
            if let Some(block_time) = latest.block_time {
                let now = chrono::Utc::now().timestamp();
                let age_seconds = now - block_time;

                let recency_score = if age_seconds < 60 {
                    95
                } else if age_seconds < 300 {
                    80
                } else if age_seconds < 3600 {
                    60
                } else if age_seconds < 86400 {
                    30
                } else {
                    10
                };

                let age_str = if age_seconds < 60 {
                    format!("{}s ago", age_seconds)
                } else if age_seconds < 3600 {
                    format!("{}m ago", age_seconds / 60)
                } else if age_seconds < 86400 {
                    format!("{}h ago", age_seconds / 3600)
                } else {
                    format!("{}d ago", age_seconds / 86400)
                };

                scores.push(SignalScore::new(
                    SignalLayer::Microstructure,
                    "recency",
                    recency_score,
                    &format!("Last transaction {}", age_str),
                ));
            }
        }

        // Transaction velocity — are transactions accelerating?
        if signatures.len() >= 10 {
            let recent_5 = &signatures[..5];
            let older_5 = &signatures[5..10];

            let recent_span = time_span(recent_5);
            let older_span = time_span(older_5);

            if recent_span > 0 && older_span > 0 {
                let recent_rate = 5.0 / recent_span as f64;
                let older_rate = 5.0 / older_span as f64;

                let acceleration = if older_rate > 0.0 {
                    recent_rate / older_rate
                } else {
                    1.0
                };

                let accel_score = if acceleration > 3.0 {
                    90 // Accelerating fast
                } else if acceleration > 1.5 {
                    75
                } else if acceleration > 0.8 {
                    50 // Steady
                } else if acceleration > 0.3 {
                    30 // Decelerating
                } else {
                    15 // Dying
                };

                scores.push(SignalScore::new(
                    SignalLayer::Microstructure,
                    "velocity",
                    accel_score,
                    &format!(
                        "Tx velocity {:.1}x vs prior period",
                        acceleration
                    ),
                ));
            }
        }

        scores
    }
}

/// Calculate the time span between first and last signature in a slice
fn time_span(sigs: &[SignatureInfo]) -> i64 {
    if sigs.len() < 2 {
        return 0;
    }
    let first_time = sigs.first().and_then(|s| s.block_time).unwrap_or(0);
    let last_time = sigs.last().and_then(|s| s.block_time).unwrap_or(0);
    (first_time - last_time).abs().max(1)
}
