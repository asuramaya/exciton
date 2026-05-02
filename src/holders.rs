//! Real holder count via Birdeye + Solscan APIs. Replaces our previous
//! RPC-based reading which was capped at 20 (the limit of
//! `getTokenLargestAccounts`) — meaning the entire `holder_count`
//! column in token_snapshots was effectively constant at 0/20 and the
//! moonshot gate's holder-range check (15-60) couldn't read truth.
//!
//! Birdeye is primary (cleaner data, returns `holder` directly on
//! `/defi/token_overview`). Solscan is fallback for when Birdeye
//! rate-limits or returns null. Both are cached per-mint at 60s — the
//! holder-count signal moves slowly, no need to spam the providers.
//!
//! Keys come from env (`BIRDEYE_API_KEY`, `SOLSCAN_API_KEY`); when
//! absent the helper returns None and callers fall back to the prior
//! RPC-cap reading.

use anyhow::Result;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

const CACHE_TTL: Duration = Duration::from_secs(60);

#[derive(Clone, Copy)]
struct CacheEntry {
    count: u32,
    fetched_at: Instant,
}

static CACHE: Lazy<Mutex<HashMap<String, CacheEntry>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

static HTTP: Lazy<reqwest::Client> = Lazy::new(|| {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(6))
        .user_agent("photon/0.1 holders-fetch")
        .build()
        .expect("reqwest client init")
});

fn cached(mint: &str) -> Option<u32> {
    let map = CACHE.lock().ok()?;
    let entry = map.get(mint).copied()?;
    if entry.fetched_at.elapsed() < CACHE_TTL {
        Some(entry.count)
    } else {
        None
    }
}

fn cache_put(mint: &str, count: u32) {
    if let Ok(mut map) = CACHE.lock() {
        map.insert(
            mint.to_string(),
            CacheEntry {
                count,
                fetched_at: Instant::now(),
            },
        );
    }
}

async fn fetch_birdeye(mint: &str) -> Result<Option<u32>> {
    let key = std::env::var("BIRDEYE_API_KEY").unwrap_or_default();
    if key.is_empty() {
        return Ok(None);
    }
    let url = format!(
        "https://public-api.birdeye.so/defi/token_overview?address={}",
        mint
    );
    let resp = HTTP
        .get(&url)
        .header("X-API-KEY", key)
        .header("x-chain", "solana")
        .send()
        .await?;
    if !resp.status().is_success() {
        return Ok(None);
    }
    let body: serde_json::Value = resp.json().await?;
    let h = body
        .get("data")
        .and_then(|d| d.get("holder"))
        .and_then(|v| v.as_u64())
        .map(|v| v as u32);
    Ok(h)
}

async fn fetch_solscan(mint: &str) -> Result<Option<u32>> {
    let key = std::env::var("SOLSCAN_API_KEY").unwrap_or_default();
    if key.is_empty() {
        return Ok(None);
    }
    // Pro Solscan endpoint: /token/holders returns total + paginated list.
    let url = format!(
        "https://pro-api.solscan.io/v2.0/token/holders?address={}&page=1&page_size=1",
        mint
    );
    let resp = HTTP.get(&url).header("token", key).send().await?;
    if !resp.status().is_success() {
        return Ok(None);
    }
    let body: serde_json::Value = resp.json().await?;
    let h = body
        .get("data")
        .and_then(|d| d.get("total"))
        .and_then(|v| v.as_u64())
        .map(|v| v as u32);
    Ok(h)
}

/// Get the real holder count for a mint. Tries Birdeye first, falls back
/// to Solscan, then None. Results cached for 60s. Returns None when no
/// API keys are configured or both providers fail — callers should
/// preserve backward-compat behavior (treat None as "unknown").
pub async fn get_holder_count(mint: &str) -> Option<u32> {
    if let Some(c) = cached(mint) {
        return Some(c);
    }
    if let Ok(Some(c)) = fetch_birdeye(mint).await {
        cache_put(mint, c);
        return Some(c);
    }
    if let Ok(Some(c)) = fetch_solscan(mint).await {
        cache_put(mint, c);
        return Some(c);
    }
    None
}
