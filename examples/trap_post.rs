//! Render + post the trap wrap for a given hour bucket to ops chat.
//! Uses the exact same notifier code path as production, so what you see here
//! is what the scanner would post at hour rollover.

use photon::config::TelegramConfig;
use photon::db::Db;
use photon::notifier::Notifier;
use std::path::PathBuf;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let now = chrono::Utc::now().timestamp();
    let hour_bucket: i64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(now - (now % 3600) - 3600);

    let db = Arc::new(Db::open(&PathBuf::from("photon.db"))?);
    let cfg = TelegramConfig {
        enabled: true,
        bot_token: std::env::var("TELEGRAM_BOT_TOKEN")?,
        signals_chat_id: "-1003735501034".into(),
        ops_chat_id: "-1003869647282".into(),
    };
    let notifier = Notifier::new(cfg, db.clone())?;

    let hour_label = chrono::DateTime::from_timestamp(hour_bucket, 0)
        .map(|d| d.format("%H:00 UTC").to_string())
        .unwrap_or_default();
    let degraded_total = db.get_degradation_tokens_since(hour_bucket)?.len();

    let traps = notifier.render_hour_traps(hour_bucket, 8).await?;
    if traps.is_empty() {
        println!(
            "[empty — no candidates above severity floor for hour {}]",
            hour_label
        );
        return Ok(());
    }

    // Wrap with a clear why-first header per the style guide
    let header = format!(
        "💥 <b>TRAP REPORT</b> · hour {label}\n\
         <i>Why: end-of-hour wrap — ranked collapses by real peak→current delta. Tap any ticker for live DexScreener evidence.</i>\n\
         <b>{n}</b> tokens degraded this hour · <b>top 8 shown</b>",
        label = hour_label,
        n = degraded_total,
    );
    let text = format!("{}{}", header, traps);
    println!("{}\n", text);

    // Post to ops chat
    let client = reqwest::Client::new();
    let token = std::env::var("TELEGRAM_BOT_TOKEN")?;
    let resp = client
        .post(format!("https://api.telegram.org/bot{}/sendMessage", token))
        .form(&[
            ("chat_id", "-1003869647282".to_string()),
            ("text", text),
            ("parse_mode", "HTML".to_string()),
            (
                "link_preview_options",
                r#"{"is_disabled":true}"#.to_string(),
            ),
        ])
        .send()
        .await?;
    let body: serde_json::Value = resp.json().await?;
    println!(
        "[tg ok={}] message_id={}",
        body["ok"], body["result"]["message_id"]
    );
    Ok(())
}
