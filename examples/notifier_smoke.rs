//! Smoke test — exercises promote / edit / demote lifecycle against live Telegram.
//! Run: `TELEGRAM_BOT_TOKEN=... cargo run --example notifier_smoke`
//!
//! Creates a throwaway SQLite DB, constructs a Notifier with enabled=true,
//! feeds it three synthetic analyses in sequence, and prints the rendered
//! digest body.

use photon::config::TelegramConfig;
use photon::db::{Db, TokenDelta, TokenSnapshot};
use photon::notifier::Notifier;
use photon::signals::{Confidence, SignalLayer, SignalScore, TokenAnalysis};
use std::sync::Arc;

fn mk_score(layer: SignalLayer, kind: &str, score: i32, details: &str) -> SignalScore {
    SignalScore {
        layer,
        signal_type: kind.to_string(),
        score,
        details: details.to_string(),
        timestamp: 0,
    }
}

fn promote_worthy() -> TokenAnalysis {
    TokenAnalysis {
        address: "4JBeo37fKhEsTXp6PtAYktYRnDAa8DcXZaZ4tTuPpump".into(),
        mint_authority: None,
        freeze_authority: None,
        is_token_2022: true,
        supply_ui: 999_999_942.96,
        decimals: 6,
        holder_count: 20,
        top_holder_pct: 3.8,
        top5_pct: 5.6,
        top10_pct: 6.5,
        tx_rate: 230.8,
        velocity: 1.0,
        recent_tx_count: 50,
        scores: vec![
            mk_score(
                SignalLayer::Microstructure,
                "recency",
                95,
                "Last transaction 14s ago",
            ),
            mk_score(
                SignalLayer::Safety,
                "top_holder",
                90,
                "Top holder owns 3.8% of supply",
            ),
            mk_score(
                SignalLayer::Safety,
                "token_2022_extensions",
                80,
                "Token-2022 extensions present but benign: MetadataPointer, TokenMetadata",
            ),
        ],
        confidence: Confidence {
            total: 82,
            base_total: 75,
            top_holder_bonus_pct: 10,
            momentum: 78,
            distribution: 55,
            spring: 65,
            coverage: 4,
            layer_scores: vec![],
            classification: "STAIRCASE".into(),
            reasoning: "STAIRCASE · mom 78 · dist 55 · spring 65".into(),
        },
        delta: Some(TokenDelta {
            previous: TokenSnapshot {
                token_address: "4JBeo37fKhEsTXp6PtAYktYRnDAa8DcXZaZ4tTuPpump".into(),
                top_holder_pct: 3.5,
                top5_pct: 5.3,
                top10_pct: 6.2,
                holder_count: 20,
                tx_rate: 220.0,
                velocity: 0.9,
                momentum: 72,
                distribution: 54,
                spring: 63,
                classification: "STAIRCASE".into(),
                confidence: 78,
                timestamp: 0,
            },
            current: TokenSnapshot {
                token_address: "4JBeo37fKhEsTXp6PtAYktYRnDAa8DcXZaZ4tTuPpump".into(),
                top_holder_pct: 3.8,
                top5_pct: 5.6,
                top10_pct: 6.5,
                holder_count: 20,
                tx_rate: 230.8,
                velocity: 1.0,
                momentum: 78,
                distribution: 55,
                spring: 65,
                classification: "STAIRCASE".into(),
                confidence: 82,
                timestamp: 0,
            },
            top_holder_delta: 0.3,
            top5_delta: 0.3,
            holder_count_delta: 0,
            momentum_delta: 6,
            time_elapsed_seconds: 900,
            concentration_direction: "stable".into(),
            classification_changed: false,
        }),
    }
}

fn material_update(base: &TokenAnalysis) -> TokenAnalysis {
    let mut a = base.clone();
    a.confidence.total = 88; // +6 confidence → material
    a.confidence.momentum = 85;
    a.top_holder_pct = 4.2;
    if let Some(ref mut d) = a.delta {
        d.momentum_delta = 7;
    }
    a
}

fn demote_worthy(base: &TokenAnalysis) -> TokenAnalysis {
    let mut a = base.clone();
    a.confidence.classification = "ACTIVE_TRAP".into();
    a.confidence.total = 45;
    a.confidence.momentum = 58;
    a.top_holder_pct = 42.0;
    if let Some(ref mut d) = a.delta {
        d.momentum_delta = -20;
        d.classification_changed = true;
        d.previous.classification = "STAIRCASE".into();
    }
    a
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let bot_token = std::env::var("TELEGRAM_BOT_TOKEN").expect("TELEGRAM_BOT_TOKEN must be set");

    let tmp = std::env::temp_dir().join(format!("photon-smoke-{}.db", std::process::id()));
    let db = Arc::new(Db::open(&tmp)?);

    let cfg = TelegramConfig {
        enabled: true,
        bot_token,
        signals_chat_id: "-1003735501034".into(), // Claudeinator channel
        ops_chat_id: "-1003869647282".into(),     // Claudeinator Chat
    };
    let notifier = Notifier::new(cfg, db.clone(), None)?;

    println!("== digest render ==\n{}\n", notifier.render_digest_body()?);
    notifier.tick_digest_now().await?;
    println!("[ok] digest posted to ops chat");

    let signal = promote_worthy();
    notifier
        .process_token(&signal, signal.confidence.total)
        .await?;
    println!("[ok] called — SIGNAL card posted (STAIRCASE conf 82)");

    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    let update = material_update(&signal);
    notifier
        .process_token(&update, update.confidence.total)
        .await?;
    println!("[ok] update — card edited with timeline entry (conf 82 → 88)");

    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    let fail = demote_worthy(&signal);
    notifier.process_token(&fail, fail.confidence.total).await?;
    println!("[ok] failed — verdict collapsed, header flipped to FAILED (STAIRCASE → ACTIVE_TRAP)");

    let _ = std::fs::remove_file(&tmp);
    Ok(())
}
