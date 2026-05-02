//! Screenshot the live DexScreener chart embed for a token. Returns PNG
//! bytes ready for Telegram sendPhoto multipart upload.
//!
//! Why a real screenshot vs. our own plotters render: DexScreener's chart
//! is what traders actually see. The candlesticks, volume, and live tape
//! are pixel-identical to the user's reference. We could approximate with
//! GeckoTerminal OHLCV + plotters candlesticks, but the brand recognition
//! of the DexScreener UI carries weight in TG cards.
//!
//! Implementation: shell out to `chromium --headless=new` with a persistent
//! user-data-dir so the Cloudflare clearance cookie is reused across calls.
//! First request takes ~7s (CF challenge JS run); subsequent ~2-3s.

use anyhow::{anyhow, Context, Result};
use std::path::PathBuf;
use std::time::Duration;
use tokio::process::Command;

const VIEWPORT_W: u32 = 900;
// 440 trims the empty footer band below the DexScreener watermark — the
// chart canvas itself ends around y=420, so 440 keeps the watermark
// visible without the dead space below it.
const VIEWPORT_H: u32 = 440;
// 18s lets DexScreener's TradingView embed finish loading candles after
// the initial WebSocket subscribe. With 9s the chart was rendering as
// "No data here" because the OHLCV fetch hadn't completed yet.
const VIRTUAL_TIME_BUDGET_MS: u32 = 18_000;
const TIMEOUT_SECS: u64 = 35;
const USER_AGENT: &str = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36";

/// Persistent profile dir. Chromium reuses CF cookies + cache from prior
/// runs; first-call latency drops dramatically after the initial warm-up.
fn profile_dir() -> PathBuf {
    PathBuf::from("/var/cache/chromium-photon")
}

/// DexScreener embed URL — strips chrome around the chart, dark theme,
/// hides info/trades panels so the chart fills the viewport.
fn embed_url(pair_address: &str) -> String {
    format!(
        "https://dexscreener.com/solana/{}?embed=1&theme=dark&info=0&trades=0&loadChartSettings=0&chartLeftToolbar=0",
        pair_address
    )
}

/// Capture the current chart for a pair. Returns raw PNG bytes.
pub async fn screenshot_pair(pair_address: &str, label: &str) -> Result<Vec<u8>> {
    let _ = tokio::fs::create_dir_all(profile_dir()).await;
    let out_path = std::env::temp_dir().join(format!(
        "madapes_chart_{}_{}.png",
        label.chars().filter(|c| c.is_ascii_alphanumeric()).collect::<String>(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
    ));

    let url = embed_url(pair_address);
    let window = format!("--window-size={},{}", VIEWPORT_W, VIEWPORT_H);
    let budget = format!("--virtual-time-budget={}", VIRTUAL_TIME_BUDGET_MS);
    let profile = format!("--user-data-dir={}", profile_dir().display());
    let screenshot = format!("--screenshot={}", out_path.display());

    let mut cmd = Command::new("chromium");
    cmd.arg("--headless=new")
        .arg("--no-sandbox")
        .arg("--disable-dev-shm-usage")
        .arg("--disable-gpu")
        .arg("--hide-scrollbars")
        .arg("--disable-blink-features=AutomationControlled")
        .arg(format!("--user-agent={}", USER_AGENT))
        .arg(&window)
        .arg(&budget)
        .arg(&profile)
        .arg(&screenshot)
        .arg(&url)
        .kill_on_drop(true);

    let run = tokio::time::timeout(Duration::from_secs(TIMEOUT_SECS), cmd.output())
        .await
        .map_err(|_| anyhow!("chromium screenshot timeout for {}", pair_address))?
        .context("failed to spawn chromium")?;

    if !run.status.success() {
        let stderr = String::from_utf8_lossy(&run.stderr);
        return Err(anyhow!(
            "chromium exit {}: {}",
            run.status,
            stderr.lines().take(5).collect::<Vec<_>>().join(" | ")
        ));
    }

    let bytes = tokio::fs::read(&out_path)
        .await
        .with_context(|| format!("could not read screenshot {}", out_path.display()))?;
    let _ = tokio::fs::remove_file(&out_path).await;

    if bytes.len() < 4096 {
        return Err(anyhow!(
            "chromium output too small ({} bytes) — likely CF challenge or load failure",
            bytes.len()
        ));
    }
    Ok(bytes)
}
