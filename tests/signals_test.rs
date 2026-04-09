use photon::signals::safety::SafetyChecker;
use photon::signals::{Confidence, SignalLayer, SignalScore};

#[test]
fn test_signal_score_creation() {
    let score = SignalScore::new(SignalLayer::Safety, "honeypot_clear", 90, "Sell simulation passed");
    assert_eq!(score.layer, SignalLayer::Safety);
    assert_eq!(score.score, 90);
}

#[test]
fn test_confidence_from_scores() {
    let scores = vec![
        SignalScore::new(SignalLayer::Safety, "honeypot", 90, ""),
        SignalScore::new(SignalLayer::OnChain, "lp_locked", 80, ""),
        SignalScore::new(SignalLayer::Microstructure, "buy_pressure", 70, ""),
        SignalScore::new(SignalLayer::SmartMoney, "convergence", 85, ""),
    ];
    let confidence = Confidence::from_scores(&scores);
    assert!(confidence.total > 0);
    assert!(confidence.total <= 100);
    assert_eq!(confidence.layer_scores.len(), 4);
}

#[test]
fn test_confidence_degrades_with_missing_layers() {
    let full = vec![
        SignalScore::new(SignalLayer::Safety, "honeypot", 90, ""),
        SignalScore::new(SignalLayer::OnChain, "lp_locked", 80, ""),
        SignalScore::new(SignalLayer::Microstructure, "buy_pressure", 70, ""),
        SignalScore::new(SignalLayer::SmartMoney, "convergence", 85, ""),
    ];
    let partial = vec![
        SignalScore::new(SignalLayer::Safety, "honeypot", 90, ""),
        SignalScore::new(SignalLayer::OnChain, "lp_locked", 80, ""),
    ];
    let full_conf = Confidence::from_scores(&full);
    let partial_conf = Confidence::from_scores(&partial);
    assert!(partial_conf.total < full_conf.total);
    assert_eq!(partial_conf.coverage, 2);
}

// Safety signal tests

#[test]
fn test_safety_flags_active_mint_authority() {
    let checker = SafetyChecker::new();
    let scores = checker.check_authorities(true, false, false);
    let mint_score = scores
        .iter()
        .find(|s| s.signal_type == "mint_authority")
        .unwrap();
    assert!(
        mint_score.score < 30,
        "Active mint authority should score low"
    );
}

#[test]
fn test_safety_passes_renounced_authorities() {
    let checker = SafetyChecker::new();
    let scores = checker.check_authorities(false, false, false);
    let mint_score = scores
        .iter()
        .find(|s| s.signal_type == "mint_authority")
        .unwrap();
    assert!(mint_score.score >= 80);
}

#[test]
fn test_safety_flags_permanent_delegate() {
    let checker = SafetyChecker::new();
    let scores = checker.check_authorities(false, false, true);
    let delegate_score = scores
        .iter()
        .find(|s| s.signal_type == "permanent_delegate")
        .unwrap();
    assert_eq!(delegate_score.score, 0, "Permanent delegate = instant zero");
}

#[test]
fn test_safety_bundled_launch_detection() {
    let checker = SafetyChecker::new();
    let score = checker.check_bundled_launch("WalletA", &["WalletA", "WalletB", "WalletC"]);
    assert!(score.score < 20);
}

#[test]
fn test_safety_clean_launch() {
    let checker = SafetyChecker::new();
    let score = checker.check_bundled_launch("WalletA", &["WalletB", "WalletC", "WalletD"]);
    assert!(score.score >= 80);
}
