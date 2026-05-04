use exciton::config::Config;
use exciton::db::Db;
use exciton::ingester::RpcRouter;
use exciton::mcp::ExcitonServer;
use std::sync::Arc;
use tempfile::tempdir;

#[test]
fn test_server_creation() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Db::open(&dir.path().join("test.db")).unwrap());
    let config = Config::default();
    let rpc =
        Arc::new(RpcRouter::new(&["https://api.mainnet-beta.solana.com".to_string()]).unwrap());
    let endpoints = vec!["https://api.mainnet-beta.solana.com".to_string()];
    let server = ExcitonServer::new(db, config, rpc, endpoints);
    assert!(server.is_healthy());
}
