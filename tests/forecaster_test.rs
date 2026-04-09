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
    assert!(confidence.total > 0);
    assert!(confidence.momentum > 0);
    assert!(confidence.distribution > 0);
    assert_eq!(confidence.coverage, 4);
}

#[test]
fn test_spring_detection() {
    let forecaster = Forecaster::new();

    // Good distribution, quiet activity = SPRING
    let spring_scores = vec![
        SignalScore::new(SignalLayer::Safety, "top_holder", 75, "well distributed"),
        SignalScore::new(SignalLayer::Safety, "holder_count", 80, "many holders"),
        SignalScore::new(SignalLayer::Safety, "mint_authority", 90, "renounced"),
        SignalScore::new(SignalLayer::Microstructure, "tx_rate", 15, "nearly dead"),
        SignalScore::new(SignalLayer::Microstructure, "velocity", 15, "no movement"),
        SignalScore::new(SignalLayer::OnChain, "history_depth", 85, "established"),
    ];
    let spring_conf = forecaster.aggregate(&spring_scores);
    assert_eq!(spring_conf.classification, "SPRING");
    assert!(spring_conf.spring > 50, "Spring score {} should be > 50", spring_conf.spring);
}

#[test]
fn test_staircase_detection() {
    let forecaster = Forecaster::new();

    // Good distribution AND high activity = STAIRCASE
    let staircase_scores = vec![
        SignalScore::new(SignalLayer::Safety, "top_holder", 75, "well distributed"),
        SignalScore::new(SignalLayer::Safety, "holder_count", 80, "many holders"),
        SignalScore::new(SignalLayer::Safety, "mint_authority", 90, "renounced"),
        SignalScore::new(SignalLayer::Microstructure, "tx_rate", 95, "very active"),
        SignalScore::new(SignalLayer::Microstructure, "velocity", 90, "accelerating"),
        SignalScore::new(SignalLayer::OnChain, "history_depth", 70, "hours of trading"),
    ];
    let conf = forecaster.aggregate(&staircase_scores);
    assert_eq!(conf.classification, "STAIRCASE");
}

#[test]
fn test_surge_detection() {
    let forecaster = Forecaster::new();

    // High activity on concentrated token = SURGE
    let surge_scores = vec![
        SignalScore::new(SignalLayer::Safety, "top_holder", 5, "99% concentrated"),
        SignalScore::new(SignalLayer::Safety, "holder_count", 20, "few holders"),
        SignalScore::new(SignalLayer::Microstructure, "tx_rate", 95, "explosive"),
        SignalScore::new(SignalLayer::Microstructure, "velocity", 90, "5x acceleration"),
        SignalScore::new(SignalLayer::OnChain, "history_depth", 25, "just launched"),
    ];
    let conf = forecaster.aggregate(&surge_scores);
    assert_eq!(conf.classification, "SURGE");
}

#[test]
fn test_staircase_beats_dead() {
    let forecaster = Forecaster::new();

    let staircase = vec![
        SignalScore::new(SignalLayer::Safety, "top_holder", 75, "distributed"),
        SignalScore::new(SignalLayer::Microstructure, "tx_rate", 80, "active"),
        SignalScore::new(SignalLayer::Microstructure, "velocity", 75, "accelerating"),
    ];
    let dead = vec![
        SignalScore::new(SignalLayer::Safety, "top_holder", 5, "concentrated"),
        SignalScore::new(SignalLayer::Microstructure, "tx_rate", 15, "dead"),
        SignalScore::new(SignalLayer::Microstructure, "velocity", 15, "dying"),
    ];

    let staircase_conf = forecaster.aggregate(&staircase);
    let dead_conf = forecaster.aggregate(&dead);
    assert!(staircase_conf.total > dead_conf.total);
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
