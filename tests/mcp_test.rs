use photon::db::Db;
use photon::config::Config;
use photon::mcp::PhotonServer;
use std::sync::Arc;
use tempfile::tempdir;

#[test]
fn test_server_creation() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Db::open(&dir.path().join("test.db")).unwrap());
    let config = Config::default();
    let server = PhotonServer::new(db, config);
    assert!(server.is_healthy());
}
