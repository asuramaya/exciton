//! Dev-tool: run `scout` against a mint address and print the report.
//!
//!     cargo run --release --example scout_probe -- <mint_address>

use exciton::db::Db;
use exciton::ingester::{resolve_endpoints, RpcRouter};
use exciton::scout;
use std::path::PathBuf;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mint = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "NV2RYH954cTJ3ckFUpvfqaQXU4ARqqDH3562nFSpump".to_string());

    let cfg = exciton::config::Config::load(&PathBuf::from("config.toml"))?;
    let endpoints = resolve_endpoints(&cfg.rpc.endpoints);
    let rpc = Arc::new(RpcRouter::new(&endpoints)?);
    let db = Arc::new(Db::open(&PathBuf::from("exciton.db"))?);

    let report = scout::scout(&mint, &rpc, &db).await?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
