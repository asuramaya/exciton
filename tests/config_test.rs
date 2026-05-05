use exciton::config::Config;
use std::path::PathBuf;

#[test]
fn test_load_config_from_file() {
    let config = Config::load(&PathBuf::from("config.example.toml")).unwrap();
    assert!(!config.rpc.endpoints.is_empty());
    assert_eq!(config.risk.max_position_pct, 15.0);
    assert_eq!(config.risk.high_confidence_threshold, 80);
}

#[test]
fn test_config_default_values() {
    let config = Config::default();
    assert_eq!(config.risk.default_position_pct, 0.5);
    assert_eq!(config.alerts.stale_feed_seconds, 30);
}
