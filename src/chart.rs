//! Sparkline chart renderer for the calls-channel cards. Renders the
//! token's price-over-time as a PNG sent via Telegram sendPhoto. Single
//! line, dark background matching the site, entry marker + current marker.
//!
//! Inputs come from `db::get_price_history` (token_snapshots) — we don't
//! depend on DexScreener for chart data so the image renders even when the
//! external API is degraded.

use anyhow::Result;
use plotters::prelude::*;
use plotters::style::register_font;
use std::sync::Once;

const W: u32 = 600;

// ab_glyph backend has no system-font discovery — it needs every named
// font registered with raw TTF bytes. We bundle DejaVuSans.ttf at build
// time and register it as both "sans-serif" and "DejaVu Sans" on first
// chart render. Without this, `("sans-serif", N).into_font()` returns a
// no-op that panics at draw time with "The font implementation is unable
// to draw text".
const DEJAVU_SANS_TTF: &[u8] = include_bytes!("../assets/DejaVuSans.ttf");
static FONT_INIT: Once = Once::new();
fn ensure_font_registered() {
    FONT_INIT.call_once(|| {
        // register_font returns Err on duplicate registration; we ignore
        // that since Once already guards us against re-entry.
        let _ = register_font("sans-serif", FontStyle::Normal, DEJAVU_SANS_TTF);
        let _ = register_font("DejaVu Sans", FontStyle::Normal, DEJAVU_SANS_TTF);
    });
}
const H: u32 = 600;
const BG: RGBColor = RGBColor(13, 14, 17);          // matches site --bg
const FG_DIM: RGBColor = RGBColor(110, 110, 120);
const GREEN: RGBColor = RGBColor(80, 220, 100);
const RED: RGBColor = RGBColor(240, 80, 80);
const ENTRY_LINE: RGBColor = RGBColor(140, 140, 150);

/// Render a sparkline PNG from a price series + entry marker. Returns the
/// raw PNG bytes ready for sendPhoto multipart upload. Empty/insufficient
/// history falls back to a "fresh entry" placeholder so the call still
/// posts cleanly.
pub fn render_sparkline(
    series: &[(i64, f64)],
    entry_price: f64,
    symbol: &str,
) -> Result<Vec<u8>> {
    ensure_font_registered();
    let path = std::env::temp_dir().join(format!(
        "madapes_chart_{}_{}.png",
        symbol.chars().filter(|c| c.is_ascii_alphanumeric()).collect::<String>(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    {
        let root = BitMapBackend::new(&path, (W, H)).into_drawing_area();
        root.fill(&BG)?;

        // Insufficient data path — render a centered ticker + "fresh entry"
        // line so the card still has visual content the moment it fires.
        if series.len() < 2 {
            let title_style = ("sans-serif", 56).into_font().color(&FG_DIM);
            let center_x = W as i32 / 2;
            let center_y = H as i32 / 2 - 30;
            root.draw(&Text::new(
                format!("${}", symbol),
                (center_x - (symbol.len() as i32 * 16), center_y),
                title_style,
            ))?;
            let sub_style = ("sans-serif", 28).into_font().color(&FG_DIM);
            root.draw(&Text::new(
                "fresh entry — chart building".to_string(),
                (center_x - 200, center_y + 80),
                sub_style,
            ))?;
            root.present()?;
        } else {
            // Bounds: pad y-range by 10% on each side so peak/trough don't
            // touch the edges. x-range is full series timestamps.
            let prices: Vec<f64> = series.iter().map(|(_, p)| *p).chain(std::iter::once(entry_price)).collect();
            let mut p_min = prices.iter().cloned().fold(f64::INFINITY, f64::min);
            let mut p_max = prices.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            if (p_max - p_min) / p_max < 0.005 {
                // Flat-line guard: if range is < 0.5% pad it manually so
                // the line sits visibly across the middle.
                let center = (p_min + p_max) / 2.0;
                p_min = center * 0.99;
                p_max = center * 1.01;
            } else {
                let pad = (p_max - p_min) * 0.10;
                p_min -= pad;
                p_max += pad;
            }
            let t_min = series.first().map(|(t, _)| *t).unwrap_or(0);
            let t_max = series.last().map(|(t, _)| *t).unwrap_or(t_min + 1);

            let current = series.last().map(|(_, p)| *p).unwrap_or(entry_price);
            let line_color = if current >= entry_price { GREEN } else { RED };

            let mut chart = ChartBuilder::on(&root)
                .margin(40)
                .build_cartesian_2d(t_min..t_max.max(t_min + 1), p_min..p_max)?;

            // Entry-price horizontal reference line (dim, dashed via
            // sparse points). plotters minimal feature set has no
            // dashed-line primitive, so we draw a thin solid line.
            chart.draw_series(LineSeries::new(
                vec![(t_min, entry_price), (t_max, entry_price)],
                ShapeStyle::from(&ENTRY_LINE).stroke_width(1),
            ))?;

            // Price line — main sparkline.
            chart.draw_series(LineSeries::new(
                series.iter().map(|(t, p)| (*t, *p)),
                ShapeStyle::from(&line_color).stroke_width(4),
            ))?;

            // Entry marker (start of the series — first snapshot at or
            // after entry). Bigger filled circle.
            if let Some((t0, p0)) = series.first() {
                chart.draw_series(std::iter::once(Circle::new(
                    (*t0, *p0),
                    7,
                    ShapeStyle::from(&FG_DIM).filled(),
                )))?;
            }
            // Current marker.
            if let Some((tn, pn)) = series.last() {
                chart.draw_series(std::iter::once(Circle::new(
                    (*tn, *pn),
                    9,
                    ShapeStyle::from(&line_color).filled(),
                )))?;
            }

            // Symbol label top-left, price-change label top-right.
            let pct = if entry_price > 0.0 {
                (current - entry_price) / entry_price * 100.0
            } else {
                0.0
            };
            let header_style = ("sans-serif", 36).into_font().color(&FG_DIM);
            root.draw(&Text::new(
                format!("${}", symbol),
                (24, 18),
                header_style.clone(),
            ))?;
            let pct_color = if pct >= 0.0 { GREEN } else { RED };
            let pct_style = ("sans-serif", 36).into_font().color(&pct_color);
            let pct_label = if pct.abs() >= 100.0 {
                format!("{:.1}x", 1.0 + pct / 100.0)
            } else {
                format!("{:+.1}%", pct)
            };
            // Right-align approximation: shift left by character estimate.
            let approx_w = (pct_label.len() as i32) * 20;
            root.draw(&Text::new(
                pct_label,
                (W as i32 - 24 - approx_w, 18),
                pct_style,
            ))?;

            root.present()?;
        }
    }

    // present() above wrote the PNG to `path` via plotters' bitmap_encoder.
    let png_bytes = std::fs::read(&path)?;
    let _ = std::fs::remove_file(&path);
    Ok(png_bytes)
}
