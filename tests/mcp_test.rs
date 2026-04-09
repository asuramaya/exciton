use photon::config::Config;
use photon::db::Db;
use photon::ingester::RpcRouter;
use photon::mcp::PhotonServer;
use std::sync::Arc;
use tempfile::tempdir;

#[test]
fn test_server_creation() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Db::open(&dir.path().join("test.db")).unwrap());
    let config = Config::default();
    let rpc = Arc::new(
        RpcRouter::new(&["https://api.mainnet-beta.solana.com".to_string()]).unwrap(),
    );
    let server = PhotonServer::new(db, config, rpc);
    assert!(server.is_healthy());
}
