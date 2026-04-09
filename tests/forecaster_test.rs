use photon::forecaster::{Forecaster, Regime};
use photon::signals::{SignalLayer, SignalScore};

#[test]
fn test_forecaster_aggregate_scores() {
    let forecaster = Forecaster::new();
    let scores = vec![
        SignalScore::new(SignalLayer::Safety, "honeypot", 90, "clear"),
        SignalScore::new(SignalLayer::Safety, "mint_authority", 90, "renounced"),
        SignalScore::new(SignalLayer::OnChain, "lp_locked", 80, "locked 6mo"),
        SignalScore::new(SignalLayer::Microstructure, "buy_pressure", 75, "2:1 ratio"),
        SignalScore::new(SignalLayer::SmartMoney, "convergence", 85, "3 wallets in"),
    ];
    let confidence = forecaster.aggregate(&scores);
    assert!(confidence.total >= 50);
    assert!(confidence.momentum > 0);
    assert!(confidence.safety > 0);
    assert_eq!(confidence.coverage, 4);
}

#[test]
fn test_momentum_driven_scoring() {
    let forecaster = Forecaster::new();

    // High momentum, low safety = surge candidate
    let surge = vec![
        SignalScore::new(SignalLayer::Safety, "top_holder", 10, "95% concentrated"),
        SignalScore::new(SignalLayer::Microstructure, "tx_rate", 95, "200 tx/min"),
        SignalScore::new(SignalLayer::Microstructure, "velocity", 90, "5x acceleration"),
        SignalScore::new(SignalLayer::OnChain, "history_depth", 40, "just launched"),
    ];
    let surge_conf = forecaster.aggregate(&surge);

    // Low momentum, high safety = boring
    let boring = vec![
        SignalScore::new(SignalLayer::Safety, "top_holder", 90, "well distributed"),
        SignalScore::new(SignalLayer::Microstructure, "tx_rate", 15, "dead"),
        SignalScore::new(SignalLayer::Microstructure, "velocity", 15, "dying"),
        SignalScore::new(SignalLayer::OnChain, "history_depth", 85, "established"),
    ];
    let boring_conf = forecaster.aggregate(&boring);

    // Surge candidate should score HIGHER than boring safe token
    assert!(
        surge_conf.total > boring_conf.total,
        "Surge candidate {} should beat boring token {}",
        surge_conf.total,
        boring_conf.total
    );
    assert!(surge_conf.momentum > 70);
    assert!(boring_conf.momentum < 40);
}

#[test]
fn test_forecaster_position_sizing() {
    let forecaster = Forecaster::new();

    assert!(forecaster.position_pct(95, 4) >= 10.0);
    assert!(forecaster.position_pct(95, 4) <= 15.0);
    assert!(forecaster.position_pct(50, 2) <= 2.0);
    assert!(forecaster.position_pct(30, 1) <= 0.5);
}

#[test]
fn test_regime_classification() {
    let forecaster = Forecaster::new();

    assert_eq!(forecaster.classify_regime(500.0, 50, 3.0), Regime::LaunchFrenzy);
    assert_eq!(forecaster.classify_regime(10.0, 2, 1.1), Regime::LowActivityGrind);
    assert_eq!(forecaster.classify_regime(200.0, 5, 0.3), Regime::DumpCascade);
    assert_eq!(forecaster.classify_regime(80.0, 10, 1.8), Regime::WhaleAccumulation);
}
