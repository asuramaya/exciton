//! Token metadata + market data via DexScreener public API.
//!
//! Free, unauthenticated, ~300 req/min rate limit. Returns name, symbol, price,
//! market cap, liquidity, volume, and age. Bonding-curve tokens on Pump.fun are
//! indexed too, though some brand-new mints may not appear yet.

use anyhow::Result;
use once_cell::sync::Lazy;
use serde::Deserialize;

/// Shared reqwest client for all metadata::fetch calls. Reusing the
/// client (and its connection pool) instead of building a fresh one
/// per call saves the TLS handshake + DNS resolution on every fetch.
/// At 30+ fetches/min across the system that's a meaningful win.
static HTTP: Lazy<reqwest::Client> = Lazy::new(|| {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .user_agent("photon/0.1")
        .build()
        .expect("metadata::HTTP client init")
});

#[derive(Debug, Clone)]
pub struct TokenMeta {
    pub name: String,
    pub symbol: String,
    pub price_usd: Option<f64>,
    pub market_cap_usd: Option<f64>,
    pub fdv_usd: Option<f64>,
    pub liquidity_usd: Option<f64>,
    pub volume_24h_usd: Option<f64>,
    pub volume_1h_usd: Option<f64>,
    pub price_change_5m: Option<f64>,
    pub price_change_1h: Option<f64>,
    pub price_change_24h: Option<f64>,
    pub pair_created_ms: Option<i64>,
    pub pair_url: Option<String>,
    pub dex_id: Option<String>,
}

impl TokenMeta {
    pub fn age_seconds(&self) -> Option<i64> {
        let ms = self.pair_created_ms?;
        let now = chrono::Utc::now().timestamp_millis();
        Some((now - ms) / 1000)
    }

    pub fn age_human(&self) -> Option<String> {
        let s = self.age_seconds()?;
        Some(if s < 60 {
            format!("{}s", s)
        } else if s < 3600 {
            format!("{}m", s / 60)
        } else if s < 86400 {
            format!("{:.1}h", s as f64 / 3600.0)
        } else {
            format!("{:.1}d", s as f64 / 86400.0)
        })
    }
}

#[derive(Deserialize)]
struct DsResponse {
    pairs: Option<Vec<DsPair>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DsPair {
    #[serde(default)]
    dex_id: Option<String>,
    #[serde(default)]
    url: Option<String>,
    base_token: DsToken,
    price_usd: Option<String>,
    price_change: Option<DsPriceChange>,
    volume: Option<DsVolume>,
    liquidity: Option<DsLiquidity>,
    fdv: Option<f64>,
    market_cap: Option<f64>,
    pair_created_at: Option<i64>,
}

#[derive(Deserialize)]
struct DsToken {
    name: String,
    symbol: String,
}

#[derive(Deserialize, Default)]
struct DsPriceChange {
    m5: Option<f64>,
    h1: Option<f64>,
    h24: Option<f64>,
}

#[derive(Deserialize, Default)]
struct DsVolume {
    h1: Option<f64>,
    h24: Option<f64>,
}

#[derive(Deserialize, Default)]
struct DsLiquidity {
    usd: Option<f64>,
}

/// Fetch metadata for a mint. Returns None if DexScreener has no pairs indexed
/// (e.g. brand-new mint still on bonding curve before first swap).
/// Picks the pair with highest liquidity when multiple exist.
pub async fn fetch(mint_address: &str) -> Result<Option<TokenMeta>> {
    let url = format!(
        "https://api.dexscreener.com/latest/dex/tokens/{}",
        mint_address
    );
    let resp = HTTP.get(&url).send().await?;
    if !resp.status().is_success() {
        return Ok(None);
    }
    let body: DsResponse = resp.json().await?;
    let pairs = match body.pairs {
        Some(p) if !p.is_empty() => p,
        _ => return Ok(None),
    };

    // Pick the pair with highest USD liquidity
    let best = pairs
        .into_iter()
        .max_by(|a, b| {
            let la = a.liquidity.as_ref().and_then(|l| l.usd).unwrap_or(0.0);
            let lb = b.liquidity.as_ref().and_then(|l| l.usd).unwrap_or(0.0);
            la.partial_cmp(&lb).unwrap_or(std::cmp::Ordering::Equal)
        })
        .unwrap();

    let price_change = best.price_change.unwrap_or_default();
    let volume = best.volume.unwrap_or_default();
    let liquidity = best.liquidity.unwrap_or_default();

    Ok(Some(TokenMeta {
        name: best.base_token.name,
        symbol: best.base_token.symbol,
        price_usd: best.price_usd.and_then(|s| s.parse().ok()),
        market_cap_usd: best.market_cap,
        fdv_usd: best.fdv,
        liquidity_usd: liquidity.usd,
        volume_24h_usd: volume.h24,
        volume_1h_usd: volume.h1,
        price_change_5m: price_change.m5,
        price_change_1h: price_change.h1,
        price_change_24h: price_change.h24,
        pair_created_ms: best.pair_created_at,
        pair_url: best.url,
        dex_id: best.dex_id,
    }))
}
