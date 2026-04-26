use photon::db::Db;
use photon::db::TokenSnapshot;
use tempfile::tempdir;

#[test]
fn test_db_initializes_schema() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let db = Db::open(&db_path).unwrap();

    let tables = db.list_tables().unwrap();
    assert!(tables.contains(&"tokens".to_string()));
    assert!(tables.contains(&"token_signals".to_string()));
    assert!(tables.contains(&"wallets".to_string()));
    assert!(tables.contains(&"wallet_trades".to_string()));
    assert!(tables.contains(&"trades".to_string()));
    assert!(tables.contains(&"regimes".to_string()));
    assert!(tables.contains(&"alerts".to_string()));
    assert!(tables.contains(&"audit_log".to_string()));
}

#[test]
fn test_db_wal_mode_enabled() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let db = Db::open(&db_path).unwrap();
    assert_eq!(db.journal_mode().unwrap(), "wal");
}

#[test]
fn test_db_insert_and_query_token() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let db = Db::open(&db_path).unwrap();

    db.insert_token("So11111111111111111111111111111111111111112", 100)
        .unwrap();
    let token = db
        .get_token("So11111111111111111111111111111111111111112")
        .unwrap();
    assert!(token.is_some());
    assert_eq!(token.unwrap().safety_score, 100);
}

#[test]
fn test_audit_log_append_only() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let db = Db::open(&db_path).unwrap();

    db.audit_log("system", "startup", "Photon started").unwrap();
    let logs = db.get_audit_logs(10).unwrap();
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].action, "startup");
}

#[test]
fn test_watchlist_candidates_preserve_age_and_join_latest_snapshot() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let db = Db::open(&db_path).unwrap();

    db.add_to_watchlist("mint-a", "DEVELOPING").unwrap();
    db.update_watchlist_checked("mint-a", "DEVELOPING").unwrap();
    let first = db.list_active_watchlist_candidates().unwrap();
    assert_eq!(first.len(), 1);
    let original_added_at = first[0].added_at;
    let original_last_checked = first[0].last_checked;

    db.insert_snapshot(&TokenSnapshot {
        token_address: "mint-a".into(),
        top_holder_pct: 18.0,
        top5_pct: 34.0,
        top10_pct: 46.0,
        holder_count: 42,
        tx_rate: 11.0,
        velocity: 2.2,
        momentum: 78,
        distribution: 74,
        spring: 49,
        classification: "STAIRCASE".into(),
        confidence: 81,
        timestamp: chrono::Utc::now().timestamp(),
        ..Default::default()
    })
    .unwrap();

    db.add_to_watchlist("mint-a", "STAIRCASE").unwrap();
    let candidates = db.list_active_watchlist_candidates().unwrap();
    assert_eq!(candidates.len(), 1);
    let candidate = &candidates[0];
    assert_eq!(candidate.watch_classification, "STAIRCASE");
    assert_eq!(candidate.added_at, original_added_at);
    assert_eq!(candidate.last_checked, original_last_checked);
    assert_eq!(
        candidate.snapshot_classification.as_deref(),
        Some("STAIRCASE")
    );
    assert_eq!(candidate.snapshot_confidence, Some(81));
    assert_eq!(candidate.snapshot_holder_count, Some(42));
    assert_eq!(candidate.snapshot_top_holder_pct, Some(18.0));
}
