//! DexScreener market-data client.
//!
//! Single responsibility: given a Solana mint address, return the market view
//! (price, mcap, liquidity, volume, buy/sell counts, host DEX). One free
//! endpoint covers everything the signal pipeline can't get from RPC alone.

use anyhow::Result;
use serde::Deserialize;
use std::collections::HashMap;

/// Normalized market snapshot returned to the signal pipeline. Zero-initialized
/// fields mean "not reported by DexScreener"; callers decide how to treat that.
/// Some fields (fdv_usd, labels, websites, socials, pair_address) are kept
/// for parsing fidelity even though only specific gates read them today.
#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct MarketData {
    pub price_usd: f64,
    pub mcap_usd: f64,
    pub fdv_usd: f64,
    pub liquidity_usd: f64,
    pub volume_h1_usd: f64,
    pub volume_h24_usd: f64,
    pub buys_h1: i32,
    pub sells_h1: i32,
    pub price_change_h1: f64,
    /// "pumpswap", "raydium", "meteora", etc. Empty when no pair is reported —
    /// typical for tokens still inside the Pump.fun bonding curve.
    pub pair_dex: String,
    pub pair_address: String,
    /// Free-form DexScreener labels (e.g. "v3", "cpmm"). Occasionally carries
    /// LP lock/burn hints but mostly decorative.
    pub labels: Vec<String>,
    /// Unix millis. 0 if unknown.
    pub pair_created_at: i64,
    pub symbol: String,
    pub name: String,
    /// Website URLs registered with the pair (usually 0–1 entries).
    pub websites: Vec<String>,
    /// (type, url) pairs — type ∈ {"twitter", "telegram", "tiktok", ...}.
    pub socials: Vec<(String, String)>,
}

impl MarketData {
    /// True when the host DEX indicates the Pump.fun bonding curve has
    /// graduated to a full AMM pool (Raydium etc). This is the single biggest
    /// inflection in a Pump.fun token's life.
    pub fn is_graduated(&self) -> bool {
        matches!(self.pair_dex.as_str(), "raydium" | "meteora" | "orca")
    }

    /// Buy/sell ratio over the last hour. Returns 1.0 when no trades happened.
    pub fn buy_pressure(&self) -> f64 {
        let total = self.buys_h1 + self.sells_h1;
        if total == 0 {
            return 1.0;
        }
        self.buys_h1 as f64 / total as f64
    }
}

#[derive(Debug, Deserialize)]
struct DsResponse {
    #[serde(default)]
    pairs: Option<Vec<DsPair>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DsPair {
    #[serde(default)]
    chain_id: String,
    #[serde(default)]
    dex_id: String,
    #[serde(default)]
    pair_address: String,
    #[serde(default)]
    base_token: DsToken,
    #[serde(default)]
    price_usd: Option<String>,
    #[serde(default)]
    price_change: Option<DsWindows>,
    #[serde(default)]
    volume: Option<DsWindows>,
    #[serde(default)]
    txns: Option<DsTxnsWindows>,
    #[serde(default)]
    liquidity: Option<DsLiquidity>,
    #[serde(default)]
    fdv: Option<f64>,
    #[serde(default)]
    market_cap: Option<f64>,
    #[serde(default)]
    labels: Option<Vec<String>>,
    #[serde(default)]
    pair_created_at: Option<i64>,
    #[serde(default)]
    info: Option<DsInfo>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DsInfo {
    #[serde(default)]
    websites: Vec<DsLink>,
    #[serde(default)]
    socials: Vec<DsSocial>,
}

#[derive(Debug, Default, Deserialize)]
struct DsLink {
    #[serde(default)]
    url: String,
}

#[derive(Debug, Default, Deserialize)]
struct DsSocial {
    #[serde(default, rename = "type")]
    social_type: String,
    #[serde(default)]
    url: String,
}

#[derive(Debug, Default, Deserialize)]
struct DsToken {
    #[serde(default)]
    address: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    symbol: String,
}

#[derive(Debug, Default, Deserialize)]
struct DsWindows {
    #[serde(default)]
    h1: Option<f64>,
    #[serde(default)]
    h24: Option<f64>,
}

#[derive(Debug, Default, Deserialize)]
struct DsTxnsWindows {
    #[serde(default)]
    h1: Option<DsTxnCount>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct DsTxnCount {
    #[serde(default)]
    buys: i32,
    #[serde(default)]
    sells: i32,
}

#[derive(Debug, Default, Deserialize)]
struct DsLiquidity {
    #[serde(default)]
    usd: Option<f64>,
}

/// Convert a raw DexScreener pair into a `MarketData`. Shared by `get_market`
/// and `get_market_batch` so parsing logic isn't duplicated.
fn pair_to_market(pair: DsPair) -> MarketData {
    let price = pair
        .price_usd
        .as_deref()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);
    let txns_h1 = pair
        .txns
        .as_ref()
        .and_then(|t| t.h1.clone())
        .unwrap_or_default();
    MarketData {
        price_usd: price,
        mcap_usd: pair.market_cap.unwrap_or(0.0),
        fdv_usd: pair.fdv.unwrap_or(0.0),
        liquidity_usd: pair.liquidity.and_then(|l| l.usd).unwrap_or(0.0),
        volume_h1_usd: pair.volume.as_ref().and_then(|v| v.h1).unwrap_or(0.0),
        volume_h24_usd: pair.volume.and_then(|v| v.h24).unwrap_or(0.0),
        buys_h1: txns_h1.buys,
        sells_h1: txns_h1.sells,
        price_change_h1: pair.price_change.and_then(|c| c.h1).unwrap_or(0.0),
        pair_dex: pair.dex_id,
        pair_address: pair.pair_address,
        labels: pair.labels.unwrap_or_default(),
        pair_created_at: pair.pair_created_at.unwrap_or(0),
        symbol: pair.base_token.symbol,
        name: pair.base_token.name,
        websites: pair
            .info
            .as_ref()
            .map(|i| {
                i.websites
                    .iter()
                    .filter(|w| !w.url.is_empty())
                    .map(|w| w.url.clone())
                    .collect()
            })
            .unwrap_or_default(),
        socials: pair
            .info
            .map(|i| {
                i.socials
                    .into_iter()
                    .filter(|s| !s.url.is_empty())
                    .map(|s| (s.social_type, s.url))
                    .collect()
            })
            .unwrap_or_default(),
    }
}

/// Pick the deepest-liquidity Solana pair from a flat list.
fn best_solana_pair(pairs: Vec<DsPair>) -> Option<DsPair> {
    pairs
        .into_iter()
        .filter(|p| p.chain_id == "solana")
        .max_by(|a, b| {
            let la = a.liquidity.as_ref().and_then(|l| l.usd).unwrap_or(0.0);
            let lb = b.liquidity.as_ref().and_then(|l| l.usd).unwrap_or(0.0);
            la.partial_cmp(&lb).unwrap_or(std::cmp::Ordering::Equal)
        })
}

/// Fetch market data for a Solana mint. Picks the pair with the deepest
/// liquidity on a Solana DEX. Returns `Ok(None)` when DexScreener has no
/// record of the token — common for freshly-created pump.fun mints that
/// haven't been indexed yet.
pub async fn get_market(mint: &str) -> Result<Option<MarketData>> {
    let url = format!("https://api.dexscreener.com/latest/dex/tokens/{}", mint);
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()?;
    let resp = client.get(&url).send().await?;
    if !resp.status().is_success() {
        anyhow::bail!("dexscreener status {}", resp.status());
    }
    let parsed: DsResponse = resp.json().await?;
    Ok(best_solana_pair(parsed.pairs.unwrap_or_default()).map(pair_to_market))
}

/// Fetch market data for multiple Solana mints in a single DexScreener request.
/// Returns a map of `mint → MarketData` for those mints DexScreener has indexed.
/// Silently omits unindexed mints — callers treat a missing key the same as
/// `get_market` returning `Ok(None)`.
pub async fn get_market_batch(mints: &[&str]) -> Result<HashMap<String, MarketData>> {
    if mints.is_empty() {
        return Ok(HashMap::new());
    }
    let url = format!(
        "https://api.dexscreener.com/latest/dex/tokens/{}",
        mints.join(",")
    );
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(8))
        .build()?;
    let resp = client.get(&url).send().await?;
    if !resp.status().is_success() {
        anyhow::bail!("dexscreener batch status {}", resp.status());
    }
    let parsed: DsResponse = resp.json().await?;
    let pairs = parsed.pairs.unwrap_or_default();

    // For each mint, keep only the Solana pair with the deepest liquidity.
    let mut by_mint: HashMap<String, DsPair> = HashMap::new();
    for pair in pairs.into_iter().filter(|p| p.chain_id == "solana") {
        let mint = pair.base_token.address.clone();
        let liq = pair.liquidity.as_ref().and_then(|l| l.usd).unwrap_or(0.0);
        match by_mint.entry(mint) {
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(pair);
            }
            std::collections::hash_map::Entry::Occupied(mut e) => {
                let cur_liq = e.get().liquidity.as_ref().and_then(|l| l.usd).unwrap_or(0.0);
                if liq > cur_liq {
                    e.insert(pair);
                }
            }
        }
    }

    Ok(by_mint
        .into_iter()
        .map(|(mint, pair)| (mint, pair_to_market(pair)))
        .collect())
}
