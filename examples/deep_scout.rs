//! Run the full chain-tool suite against a mint and dump structured JSON.
//!
//!     cargo run --release --example deep_scout -- <mint_address>
//!
//! Pure data extractors — the operator (or LLM) synthesizes.

use photon::db::Db;
use photon::ingester::{resolve_endpoints, RpcRouter};
use photon::market;
use photon::scout;
use serde::Serialize;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Serialize)]
struct DeepScoutReport {
    mint: String,
    symbol: String,
    name: String,
    mcap_usd: f64,
    liquidity_usd: f64,
    pair_dex: String,
    pair_address: String,
    socials: Vec<(String, String)>,
    websites: Vec<String>,
    basic_scout: scout::ScoutReport,
    whales: Vec<scout::WhaleMove>,
    lp: Option<scout::LpStatus>,
    deployer_history: Vec<scout::PastLaunch>,
    holder_evolution_24h: scout::HolderEvolution,
    sniper_cohort: scout::SniperCohort,
    cohort_overlap: scout::CohortOverlap,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mint = std::env::args()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("usage: deep_scout <mint>"))?;

    let cfg = photon::config::Config::load(&PathBuf::from("config.toml"))?;
    let endpoints = resolve_endpoints(&cfg.rpc.endpoints);
    let rpc = Arc::new(RpcRouter::new(&endpoints)?);
    let db = Arc::new(Db::open(&PathBuf::from("photon.db"))?);

    let market_data = market::get_market(&mint).await.ok().flatten();
    let (symbol, name, mcap, liq, dex, pair_addr, socials, websites) = match &market_data {
        Some(m) => (
            m.symbol.clone(),
            m.name.clone(),
            m.mcap_usd,
            m.liquidity_usd,
            m.pair_dex.clone(),
            m.pair_address.clone(),
            m.socials.clone(),
            m.websites.clone(),
        ),
        None => Default::default(),
    };

    let basic_scout = scout::scout(&mint, &rpc, &db).await?;
    let whales = scout::whale_trace(&mint, &rpc).await.unwrap_or_default();

    let lp = if !pair_addr.is_empty() && !dex.is_empty() {
        scout::lp_check(&pair_addr, &dex, &rpc).await.ok()
    } else {
        None
    };

    let deployer_history = match &basic_scout.deployer {
        Some(d) => scout::deployer_history(&d.deployer_address, &db)
            .await
            .unwrap_or_default(),
        None => Vec::new(),
    };

    let holder_evolution_24h = scout::holder_evolution(&mint, 24, &db).unwrap_or_default();
    let sniper = scout::sniper_cohort(&mint, &rpc, &db)
        .await
        .unwrap_or_default();
    let cohort = scout::cohort_overlap(&mint, &rpc, &db)
        .await
        .unwrap_or_default();

    let report = DeepScoutReport {
        mint: mint.clone(),
        symbol,
        name,
        mcap_usd: mcap,
        liquidity_usd: liq,
        pair_dex: dex,
        pair_address: pair_addr,
        socials,
        websites,
        basic_scout,
        whales,
        lp,
        deployer_history,
        holder_evolution_24h,
        sniper_cohort: sniper,
        cohort_overlap: cohort,
    };

    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
