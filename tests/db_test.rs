use photon::db::Db;
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
