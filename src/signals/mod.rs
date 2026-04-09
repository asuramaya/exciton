pub mod microstructure;
pub mod onchain;
pub mod safety;
pub mod smartmoney;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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
