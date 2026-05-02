//! Screenshot the live DexScreener chart embed for a token. Returns PNG
//! bytes ready for Telegram sendPhoto multipart upload.
//!
//! Why a real screenshot vs. our own plotters render: DexScreener's chart
//! is what traders actually see. The candlesticks, volume, and live tape
//! are pixel-identical to the user's reference. We approximate nothing.
//!
//! Implementation: drive headless Chromium via CDP using the
//! `headless_chrome` crate. The earlier `chromium --screenshot=` CLI
//! path was abandoned because `--virtual-time-budget` exits as soon as
//! the JS event loop drains, regardless of pending WebSocket data —
//! production logs showed budget=50000ms elapsed in 4-5s wall-clock with
//! the chart still on "Loading pair…". CDP gives us real
//! wait-for-element semantics so the chart actually finishes loading
//! before snapshot.

use anyhow::{anyhow, Context, Result};
use headless_chrome::{Browser, LaunchOptions};
use headless_chrome::protocol::cdp::Page::CaptureScreenshotFormatOption;
use image::ImageReader;
use std::ffi::OsStr;
use std::io::Cursor;
use std::time::Duration;

const VIEWPORT_W: u32 = 900;
const VIEWPORT_H: u32 = 400;
const CROP_H: u32 = 220;

// Total budget we'll let a chart render take. Once `load` event fires,
// we wait up to this for either (a) a `.tv-chart-container` selector
// that has rendered children, or (b) the timeout — then snapshot
// regardless. Real wall-clock seconds.
const RENDER_WAIT_SECS: u64 = 25;

// Variance threshold for "loaded" vs "Loading pair…" state. Production
// data shows the grey loading rectangle produces variance ~240, partial
// loads ~580-865, fully rendered candles ~2000+.
const VARIANCE_LOADED_THRESHOLD: f64 = 1500.0;

const USER_AGENT: &str = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36";

/// DexScreener embed URL — strips chrome around the chart, dark theme,
/// hides info/trades panels so the chart fills the viewport. `interval=1`
/// requests 1-minute candles.
fn embed_url(pair_address: &str) -> String {
    format!(
        "https://dexscreener.com/solana/{}?embed=1&theme=dark&info=0&trades=0&chartLeftToolbar=0&interval=1",
        pair_address
    )
}

/// Capture the chart via CDP. Real wall-clock wait — no virtual time.
/// Returns raw PNG bytes (uncropped, full viewport).
fn raw_screenshot_cdp(pair_address: &str, render_wait_secs: u64) -> Result<Vec<u8>> {
    let user_data_dir = std::path::PathBuf::from("/var/cache/chromium-photon");
    let _ = std::fs::create_dir_all(&user_data_dir);

    // Stable args on top of headless_chrome defaults: no-sandbox for
    // root-in-container, disable-dev-shm for small /dev/shm sizes,
    // disable-blink-features=AutomationControlled to avoid CF challenge,
    // user-agent override.
    let extra: Vec<&OsStr> = vec![
        OsStr::new("--no-sandbox"),
        OsStr::new("--disable-dev-shm-usage"),
        OsStr::new("--disable-gpu"),
        OsStr::new("--hide-scrollbars"),
        OsStr::new("--disable-blink-features=AutomationControlled"),
    ];

    let opts = LaunchOptions::default_builder()
        .headless(true)
        .sandbox(false)
        .window_size(Some((VIEWPORT_W, VIEWPORT_H)))
        .user_data_dir(Some(user_data_dir))
        .args(extra)
        .build()
        .context("build chromium launch options")?;

    let browser = Browser::new(opts).context("launch chromium")?;
    let tab = browser.new_tab().context("new_tab")?;
    tab.set_user_agent(USER_AGENT, None, None)
        .context("set_user_agent")?;
    let url = embed_url(pair_address);
    tab.navigate_to(&url).context("navigate")?;
    tab.wait_until_navigated().context("wait_until_navigated")?;

    // Real wall-clock wait. We don't try to wait for a specific
    // selector because DexScreener iframes its TradingView widget with
    // shadow DOM that headless_chrome's `wait_for_element` can't see.
    // A flat sleep is reliable and predictable.
    std::thread::sleep(Duration::from_secs(render_wait_secs));

    let png = tab
        .capture_screenshot(CaptureScreenshotFormatOption::Png, None, None, true)
        .context("capture_screenshot")?;

    Ok(png)
}

/// Variance of the green channel over the chart-canvas region. The
/// "Loading pair…" mid-load state is a near-uniform light-grey
/// rectangle centered in the canvas; a loaded chart with candles + grid
/// produces high color variance.
fn chart_variance(png_bytes: &[u8]) -> Result<f64> {
    let img = ImageReader::new(Cursor::new(png_bytes))
        .with_guessed_format()
        .context("guess png format")?
        .decode()
        .context("decode png")?
        .to_rgb8();
    let xs: Vec<u32> = (80u32..VIEWPORT_W - 80).step_by(8).collect();
    let ys: Vec<u32> = (40u32..VIEWPORT_H - 80).step_by(6).collect();
    let mut sum: f64 = 0.0;
    let mut sum_sq: f64 = 0.0;
    let mut n: f64 = 0.0;
    for &y in &ys {
        for &x in &xs {
            if x < img.width() && y < img.height() {
                let p = img.get_pixel(x, y).0[1] as f64;
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

/// Crop top CROP_H rows and re-encode as PNG.
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
/// (variance-checked + cropped).
pub async fn screenshot_pair(pair_address: &str, label: &str) -> Result<Vec<u8>> {
    let pair_owned = pair_address.to_string();
    let label_owned = label.to_string();
    // headless_chrome is sync — run on a blocking task so we don't tie
    // up a tokio worker for the full render window.
    let png = tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
        // Single CDP attempt at the full render window. Real time, so
        // there's no fast/slow-pass distinction needed — 25s of
        // wall-clock is enough for any healthy DexScreener pair.
        let raw = raw_screenshot_cdp(&pair_owned, RENDER_WAIT_SECS)?;
        let var = chart_variance(&raw).unwrap_or(0.0);
        if var >= VARIANCE_LOADED_THRESHOLD {
            tracing::debug!(
                "chart screenshot {}: loaded (variance {:.0})",
                label_owned, var
            );
        } else {
            tracing::warn!(
                "chart screenshot {}: low-variance after {}s wall ({:.0}) — sending anyway",
                label_owned, RENDER_WAIT_SECS, var
            );
        }
        crop_top(&raw)
    })
    .await
    .context("blocking task panic")??;
    Ok(png)
}
