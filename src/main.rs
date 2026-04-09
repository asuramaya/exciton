use anyhow::Result;
use rmcp::ServiceExt;
use std::path::PathBuf;
use std::sync::Arc;

mod config;
mod db;
mod execution;
mod forecaster;
mod ingester;
mod mcp;
mod signals;

use config::Config;
use db::Db;
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

    let server = PhotonServer::new(db, config);
    tracing::info!("MCP server starting on stdio");

    let service = server.serve(rmcp::transport::stdio()).await?;
    service.waiting().await?;

    Ok(())
}
