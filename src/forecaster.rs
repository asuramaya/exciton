use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Regime {
    LaunchFrenzy,
    WhaleAccumulation,
    LowActivityGrind,
    DumpCascade,
}

impl std::fmt::Display for Regime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Regime::LaunchFrenzy => write!(f, "Launch Frenzy"),
            Regime::WhaleAccumulation => write!(f, "Whale Accumulation"),
            Regime::LowActivityGrind => write!(f, "Low Activity Grind"),
            Regime::DumpCascade => write!(f, "Dump Cascade"),
        }
    }
}

#[derive(Clone)]
pub struct Forecaster;

impl Forecaster {
    pub fn new() -> Self {
        Self
    }

    /// Returns recommended position size as percentage of portfolio
    pub fn position_pct(&self, confidence: i32, coverage: usize) -> f64 {
        let base = match confidence {
            90..=100 => 15.0,
            80..=89 => 10.0,
            70..=79 => 5.0,
            50..=69 => 2.0,
            _ => 0.5,
        };
        let coverage_factor = coverage as f64 / 4.0;
        base * coverage_factor
    }
}
