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
    assert!(confidence.total >= 60);
    assert_eq!(confidence.coverage, 4);
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

    let regime = forecaster.classify_regime(500.0, 50, 3.0);
    assert_eq!(regime, Regime::LaunchFrenzy);

    let regime = forecaster.classify_regime(10.0, 2, 1.1);
    assert_eq!(regime, Regime::LowActivityGrind);

    let regime = forecaster.classify_regime(200.0, 5, 0.3);
    assert_eq!(regime, Regime::DumpCascade);

    let regime = forecaster.classify_regime(80.0, 10, 1.8);
    assert_eq!(regime, Regime::WhaleAccumulation);
}
