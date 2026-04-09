use super::{SignalLayer, SignalScore};
use crate::ingester::SignatureInfo;

pub struct MicrostructureAnalyzer;

/// Raw computed metrics from transaction analysis — stored in snapshots
#[derive(Debug, Clone)]
pub struct MicroMetrics {
    pub tx_per_minute: f64,
    pub velocity_multiplier: f64,
    pub success_rate_pct: f64,
    pub failure_rate_pct: f64,
    pub last_tx_age_seconds: i64,
    pub window_seconds: i64,
}

impl MicrostructureAnalyzer {
    pub fn new() -> Self {
        Self
    }

    /// Compute raw metrics from signatures
    pub fn compute_metrics(&self, signatures: &[SignatureInfo]) -> MicroMetrics {
        if signatures.is_empty() {
            return MicroMetrics {
                tx_per_minute: 0.0,
                velocity_multiplier: 0.0,
                success_rate_pct: 0.0,
                failure_rate_pct: 0.0,
                last_tx_age_seconds: i64::MAX,
                window_seconds: 0,
            };
        }

        let total = signatures.len();
        let successful = signatures.iter().filter(|s| !s.err).count();
        let success_rate = (successful as f64 / total as f64) * 100.0;

        let window = time_span(signatures);
        let tx_per_minute = if window > 0 {
            (total as f64 / window as f64) * 60.0
        } else {
            total as f64
        };

        let last_tx_age = signatures
            .first()
            .and_then(|s| s.block_time)
            .map(|t| chrono::Utc::now().timestamp() - t)
            .unwrap_or(i64::MAX);

        let velocity = if signatures.len() >= 10 {
            let recent_span = time_span(&signatures[..5]);
            let older_span = time_span(&signatures[5..10]);
            if recent_span > 0 && older_span > 0 {
                let recent_rate = 5.0 / recent_span as f64;
                let older_rate = 5.0 / older_span as f64;
                if older_rate > 0.0 {
                    recent_rate / older_rate
                } else {
                    1.0
                }
            } else {
                1.0
            }
        } else {
            1.0
        };

        MicroMetrics {
            tx_per_minute,
            velocity_multiplier: velocity,
            success_rate_pct: success_rate,
            failure_rate_pct: 100.0 - success_rate,
            last_tx_age_seconds: last_tx_age,
            window_seconds: window,
        }
    }

    /// Analyze recent transaction activity, producing scored signals
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

        let m = self.compute_metrics(signatures);

        // Transaction rate score
        let rate_score = if m.tx_per_minute > 30.0 {
            95
        } else if m.tx_per_minute > 10.0 {
            80
        } else if m.tx_per_minute > 3.0 {
            65
        } else if m.tx_per_minute > 1.0 {
            50
        } else if m.tx_per_minute > 0.1 {
            30
        } else {
            15
        };

        scores.push(SignalScore::new(
            SignalLayer::Microstructure,
            "tx_rate",
            rate_score,
            &format!(
                "{:.1} tx/min ({} txs over {}s)",
                m.tx_per_minute,
                signatures.len(),
                m.window_seconds
            ),
        ));

        // Success rate
        let success_score = if m.success_rate_pct > 95.0 {
            90
        } else if m.success_rate_pct > 85.0 {
            70
        } else if m.success_rate_pct > 70.0 {
            40
        } else {
            15
        };

        let failed = signatures.iter().filter(|s| s.err).count();
        scores.push(SignalScore::new(
            SignalLayer::Microstructure,
            "tx_success_rate",
            success_score,
            &format!(
                "{:.0}% success rate ({} failed of {})",
                m.success_rate_pct,
                failed,
                signatures.len()
            ),
        ));

        // Recency
        if m.last_tx_age_seconds < i64::MAX {
            let recency_score = if m.last_tx_age_seconds < 60 {
                95
            } else if m.last_tx_age_seconds < 300 {
                80
            } else if m.last_tx_age_seconds < 3600 {
                60
            } else if m.last_tx_age_seconds < 86400 {
                30
            } else {
                10
            };

            let age_str = if m.last_tx_age_seconds < 60 {
                format!("{}s ago", m.last_tx_age_seconds)
            } else if m.last_tx_age_seconds < 3600 {
                format!("{}m ago", m.last_tx_age_seconds / 60)
            } else if m.last_tx_age_seconds < 86400 {
                format!("{}h ago", m.last_tx_age_seconds / 3600)
            } else {
                format!("{}d ago", m.last_tx_age_seconds / 86400)
            };

            scores.push(SignalScore::new(
                SignalLayer::Microstructure,
                "recency",
                recency_score,
                &format!("Last transaction {}", age_str),
            ));
        }

        // Velocity
        if signatures.len() >= 10 {
            let accel_score = if m.velocity_multiplier > 3.0 {
                90
            } else if m.velocity_multiplier > 1.5 {
                75
            } else if m.velocity_multiplier > 0.8 {
                50
            } else if m.velocity_multiplier > 0.3 {
                30
            } else {
                15
            };

            scores.push(SignalScore::new(
                SignalLayer::Microstructure,
                "velocity",
                accel_score,
                &format!("Tx velocity {:.1}x vs prior period", m.velocity_multiplier),
            ));
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
