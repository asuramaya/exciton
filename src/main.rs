use anyhow::Result;
use rmcp::ServiceExt;
use std::path::PathBuf;
use std::sync::Arc;

mod config;
mod db;
mod discovery;
mod execution;
mod forecaster;
mod ingester;
mod mcp;
mod signals;

use config::Config;
use db::Db;
use ingester::{resolve_endpoints, RpcRouter};
use mcp::PhotonServer;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("photon=info".parse()?),
        )
        .with_writer(std::io::stderr)
        .init();

    let config_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("config.toml"));

    let config = Config::load(&config_path)?;
    tracing::info!("Photon Signal Forecaster starting");

    let db_path = PathBuf::from("photon.db");
    let db = Arc::new(Db::open(&db_path)?);
    db.audit_log("system", "startup", "Photon Signal Forecaster started")?;
    tracing::info!("Database initialized at {:?}", db_path);

    // Resolve env vars in endpoint URLs and create RPC router
    let resolved_endpoints = resolve_endpoints(&config.rpc.endpoints);
    let rpc = Arc::new(RpcRouter::new(&resolved_endpoints)?);
    tracing::info!(
        "RPC router ready: {} endpoints configured",
        rpc.endpoint_count()
    );

    // Test connectivity
    match rpc.check_connection().await {
        Ok(true) => tracing::info!("RPC connection verified"),
        Ok(false) => tracing::warn!("RPC connection check failed — will retry on demand"),
        Err(e) => tracing::warn!("RPC connection error: {} — will retry on demand", e),
    }

    let server = PhotonServer::new(db, config, rpc, resolved_endpoints);
    tracing::info!("MCP server starting on stdio");

    let service = server.serve(rmcp::transport::stdio()).await?;
    service.waiting().await?;

    Ok(())
}
