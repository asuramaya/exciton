//! Internal probe for the intel layer.
//!
//! Usage:
//!   cargo run --release --example intel_probe -- wallet <wallet> [focus_mint]
//!   cargo run --release --example intel_probe -- holders <mint> [top_n]
//!   cargo run --release --example intel_probe -- history <mint> [limit] [window_hours]
//!   cargo run --release --example intel_probe -- signals <mint>

use photon::db::Db;
use photon::ingester::{resolve_endpoints, RpcRouter};
use photon::intel;
use std::path::PathBuf;
use std::sync::Arc;

fn usage() -> anyhow::Error {
    anyhow::anyhow!("usage: intel_probe <wallet|holders|history|signals> <address> [extra args]")
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let command = args.get(1).ok_or_else(usage)?.as_str();
    let target = args.get(2).ok_or_else(usage)?;

    let cfg = photon::config::Config::load(&PathBuf::from("config.toml"))?;
    let endpoints = resolve_endpoints(&cfg.rpc.endpoints);
    let rpc = Arc::new(RpcRouter::new(&endpoints)?);
    let db = Arc::new(Db::open(&PathBuf::from("photon.db"))?);

    let output = match command {
        "wallet" => {
            let focus_mint = args.get(3).map(String::as_str);
            serde_json::to_value(intel::wallet_xray(target, focus_mint, &rpc, &db).await?)?
        }
        "holders" => {
            let top_n = args
                .get(3)
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(12);
            serde_json::to_value(intel::holder_forensics(target, &rpc, &db, top_n).await?)?
        }
        "history" => {
            let limit = args
                .get(3)
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(96);
            let window_hours = args.get(4).and_then(|value| value.parse::<i64>().ok());
            serde_json::to_value(intel::historical_analysis(
                target,
                &db,
                limit,
                window_hours,
            )?)?
        }
        "signals" => serde_json::to_value(intel::deep_signals(target, &rpc, &db).await?)?,
        _ => return Err(usage()),
    };

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}
