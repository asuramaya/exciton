//! Debug: run the trap pipeline against a specific hour bucket and print
//! what it would render. Optionally post to ops chat.
//! Usage:
//!   cargo run --release --example trap_debug 1776661200   # specific hour
//!   cargo run --release --example trap_debug               # defaults to 1h ago

use photon::config::TelegramConfig;
use photon::db::Db;
use photon::notifier::Notifier;
use std::path::PathBuf;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let now = chrono::Utc::now().timestamp();
    let hour_bucket: i64 = args
        .get(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(now - (now % 3600) - 3600);

    let db = Arc::new(Db::open(&PathBuf::from("photon.db"))?);

    let cfg = TelegramConfig {
        enabled: true,
        bot_token: std::env::var("TELEGRAM_BOT_TOKEN").expect("TELEGRAM_BOT_TOKEN must be set"),
        signals_chat_id: "-1003735501034".into(),
        ops_chat_id: "-1003869647282".into(),
    };
    let notifier = Notifier::new(cfg, db.clone())?;

    println!(
        "== hour bucket {} ({}) ==",
        hour_bucket,
        chrono::DateTime::from_timestamp(hour_bucket, 0)
            .map(|d| d.format("%Y-%m-%d %H:%M UTC").to_string())
            .unwrap_or_default()
    );

    // Use the public API via render_hour_traps — but it's private, so we re-run
    // the pipeline with public helpers
    let since_ts = hour_bucket;
    let degraded = db.get_degradation_tokens_since(since_ts)?;
    println!("degradation_tokens = {}", degraded.len());

    let peak_since = now - 6 * 3600;
    let mut count_peak = 0;
    let mut count_current = 0;
    let mut count_distinct = 0;
    let mut evidence_lines: Vec<(f64, String)> = Vec::new();

    for addr in &degraded {
        let peak = db.get_peak_snapshot(addr, peak_since)?;
        let current = db.get_latest_snapshot(addr)?;
        if peak.is_some() {
            count_peak += 1;
        }
        if current.is_some() {
            count_current += 1;
        }
        let (p, c) = match (peak, current) {
            (Some(p), Some(c)) => (p, c),
            _ => continue,
        };
        if c.timestamp == p.timestamp {
            continue;
        }
        count_distinct += 1;

        let top_jump = (c.top_holder_pct - p.top_holder_pct).max(0.0);
        let momentum_loss = (p.momentum - c.momentum).max(0) as f64;
        let conf_loss = (p.confidence - c.confidence).max(0) as f64;
        let good = |k: &str| matches!(k, "STAIRCASE" | "GRINDER" | "SPRING" | "SURGE");
        let bad =
            |k: &str| k.starts_with("UNSAFE") || matches!(k, "ACTIVE_TRAP" | "CRASHING" | "DEAD");
        let class_penalty = if good(&p.classification) && bad(&c.classification) {
            30.0
        } else if good(&p.classification) && c.classification == "DEVELOPING" {
            10.0
        } else {
            0.0
        };
        let severity = top_jump * 2.0 + momentum_loss + conf_loss * 0.5 + class_penalty;

        if severity >= 5.0 {
            evidence_lines.push((
                severity,
                format!(
                    "sev={:>5.1}  {} {}({})→{}({})  top {:.1}→{:.1}%  mom {}→{}",
                    severity,
                    addr,
                    p.classification,
                    p.confidence,
                    c.classification,
                    c.confidence,
                    p.top_holder_pct,
                    c.top_holder_pct,
                    p.momentum,
                    c.momentum
                ),
            ));
        }
    }

    evidence_lines.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
    println!(
        "peaks_found = {}, currents_found = {}, distinct_peak_vs_current = {}, above_floor = {}",
        count_peak,
        count_current,
        count_distinct,
        evidence_lines.len()
    );
    println!("\n-- top 10 by severity --");
    for (_, line) in evidence_lines.iter().take(10) {
        println!("{}", line);
    }

    // Render the wrap + post to ops as a standalone debug message
    if evidence_lines.is_empty() {
        return Ok(());
    }

    let body = notifier.render_digest_body()?;
    println!("\n-- body --\n{}", body);

    println!("\n[done] would have posted wrap at hour rollover");
    Ok(())
}
