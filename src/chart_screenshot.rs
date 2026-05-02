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
//!
//! Two post-processing steps after the raw chromium PNG comes back:
//! 1. Variance-check the chart-canvas region. The "Loading pair…" mid-load
//!    state is a low-variance grey rectangle in the center; loaded
//!    candles are a high-variance mix of green/red/grid. If the variance
//!    sits below threshold we retry once with a longer budget — virtual
//!    time alone doesn't guarantee the chart's WebSocket subscription
//!    has delivered its first OHLCV chunk by snapshot time.
//! 2. Crop the bottom band. The DexScreener embed page lays out with
//!    extra padding below the watermark; cropping to a tighter height
//!    gives more chart area in the TG card. User has explicitly waived
//!    the watermark for chart real estate.

use anyhow::{anyhow, Context, Result};
use image::ImageReader;
use std::io::Cursor;
use std::path::PathBuf;
use std::time::Duration;
use tokio::process::Command;

const VIEWPORT_W: u32 = 900;
// Chromium renders the embed at 400h so the chart canvas is fully
// populated. We crop the result to CROP_H below — the watermark + chin
// occupy the bottom ~200 rows and that's wasted real estate in the TG
// card. The user explicitly OK'd dropping the DexScreener attribution
// for more chart area.
const VIEWPORT_H: u32 = 400;
const CROP_H: u32 = 220;
// Variance threshold for "loaded" vs "Loading pair…" state. The grey
// loading rectangle has near-uniform color (variance < ~150 over the
// sample region); a fully rendered chart with candles + grid + axes
// produces variance well over 1500. Threshold 600 sits comfortably
// between the two without false-positives on slow markets.
const VARIANCE_LOADED_THRESHOLD: f64 = 600.0;

const VIRTUAL_TIME_BUDGET_MS_FAST: u32 = 12_000;
const VIRTUAL_TIME_BUDGET_MS_SLOW: u32 = 35_000;
const TIMEOUT_SECS: u64 = 50;
const USER_AGENT: &str = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36";

fn profile_dir() -> PathBuf {
    PathBuf::from("/var/cache/chromium-photon")
}

/// DexScreener embed URL — strips chrome around the chart, dark theme,
/// hides info/trades panels so the chart fills the viewport. `interval=1`
/// requests 1-minute candles.
fn embed_url(pair_address: &str) -> String {
    format!(
        "https://dexscreener.com/solana/{}?embed=1&theme=dark&info=0&trades=0&loadChartSettings=0&chartLeftToolbar=0&interval=1",
        pair_address
    )
}

/// Shell out to chromium and return raw PNG bytes at the configured
/// viewport. No post-processing here — caller variance-checks + crops.
async fn raw_screenshot(pair_address: &str, label: &str, virtual_time_budget_ms: u32) -> Result<Vec<u8>> {
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
    let budget = format!("--virtual-time-budget={}", virtual_time_budget_ms);
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

/// Variance of the green channel over the chart-canvas region. The
/// "Loading pair…" mid-load state is a near-uniform light-grey
/// rectangle centered in the canvas; a loaded chart with candles + grid
/// produces high color variance. We sample the green channel because
/// red/green candles diverge most strongly there.
fn chart_variance(png_bytes: &[u8]) -> Result<f64> {
    let img = ImageReader::new(Cursor::new(png_bytes))
        .with_guessed_format()
        .context("guess png format")?
        .decode()
        .context("decode png")?
        .to_rgb8();
    // Sample a generous chunk of the canvas: skip the top toolbar (~30px)
    // and bottom labels (~last 80px), and the outer 80px on each side
    // so we don't catch axis text.
    let xs: Vec<u32> = (80u32..VIEWPORT_W - 80).step_by(8).collect();
    let ys: Vec<u32> = (40u32..VIEWPORT_H - 80).step_by(6).collect();
    let mut sum: f64 = 0.0;
    let mut sum_sq: f64 = 0.0;
    let mut n: f64 = 0.0;
    for &y in &ys {
        for &x in &xs {
            if x < img.width() && y < img.height() {
                let p = img.get_pixel(x, y).0[1] as f64; // green channel
                sum += p;
                sum_sq += p * p;
                n += 1.0;
            }
        }
    }
    if n == 0.0 {
        return Err(anyhow!("empty sample region"));
    }
    let mean = sum / n;
    let var = sum_sq / n - mean * mean;
    Ok(var)
}

/// Crop the screenshot to keep the top CROP_H rows and re-encode as PNG.
/// Drops the DexScreener watermark + chin → the TG card uses the freed
/// vertical space for the chart itself.
fn crop_top(png_bytes: &[u8]) -> Result<Vec<u8>> {
    let img = ImageReader::new(Cursor::new(png_bytes))
        .with_guessed_format()
        .context("guess png format")?
        .decode()
        .context("decode png")?;
    let cropped = img.crop_imm(0, 0, VIEWPORT_W, CROP_H);
    let mut out = Cursor::new(Vec::new());
    cropped
        .write_to(&mut out, image::ImageFormat::Png)
        .context("re-encode cropped png")?;
    Ok(out.into_inner())
}

/// Capture the current chart for a pair. Returns processed PNG bytes
/// (variance-checked + cropped). Tries fast budget first; on low-variance
/// retries once with a longer budget so virtual time has more headroom
/// for the embed's WS subscription to deliver the first OHLCV chunk.
pub async fn screenshot_pair(pair_address: &str, label: &str) -> Result<Vec<u8>> {
    // Attempt 1: fast budget.
    let fast = raw_screenshot(pair_address, label, VIRTUAL_TIME_BUDGET_MS_FAST).await?;
    let var_fast = chart_variance(&fast).unwrap_or(0.0);
    let chosen = if var_fast >= VARIANCE_LOADED_THRESHOLD {
        tracing::debug!(
            "chart screenshot {}: loaded on fast pass (variance {:.0})",
            pair_address, var_fast
        );
        fast
    } else {
        // Attempt 2: slow budget. The first attempt looked stuck on
        // "Loading pair…" — retry with longer virtual time so the WS
        // handshake + first OHLCV frame finishes inside the budget.
        tracing::info!(
            "chart screenshot {}: fast pass low-variance ({:.0}), retrying with slow budget",
            pair_address, var_fast
        );
        let slow = raw_screenshot(pair_address, label, VIRTUAL_TIME_BUDGET_MS_SLOW).await?;
        let var_slow = chart_variance(&slow).unwrap_or(0.0);
        if var_slow < VARIANCE_LOADED_THRESHOLD {
            tracing::warn!(
                "chart screenshot {}: still low-variance after slow pass ({:.0}) — sending anyway",
                pair_address, var_slow
            );
        } else {
            tracing::debug!(
                "chart screenshot {}: loaded on slow pass (variance {:.0})",
                pair_address, var_slow
            );
        }
        slow
    };
    crop_top(&chosen)
}
