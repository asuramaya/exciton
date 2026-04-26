//! Preview all 4 template styles using real data from today's scan.
//! Run: `cargo run --example preview_templates`
//!
//! Optionally posts to Telegram when TG_BOT_TOKEN and TG_CHAT_ID are set.

use photon::db::{TokenDelta, TokenSnapshot};
use photon::metadata;
use photon::signals::{Confidence, SignalLayer, SignalScore, TokenAnalysis};
use photon::templates::{self, Template};

fn mk_score(layer: SignalLayer, kind: &str, score: i32, details: &str) -> SignalScore {
    SignalScore {
        layer,
        signal_type: kind.to_string(),
        score,
        details: details.to_string(),
        timestamp: 0,
    }
}

fn sample_winner() -> TokenAnalysis {
    let prev = TokenSnapshot {
        token_address: "4JBeo37fKhEsTXp6PtAYktYRnDAa8DcXZaZ4tTuPpump".into(),
        top_holder_pct: 3.5,
        top5_pct: 5.3,
        top10_pct: 6.2,
        holder_count: 20,
        tx_rate: 220.0,
        velocity: 0.9,
        momentum: 69,
        distribution: 53,
        spring: 63,
        classification: "GRINDER".into(),
        confidence: 62,
        timestamp: 0,
    };
    let current = TokenSnapshot {
        classification: "STAIRCASE".into(),
        ..prev.clone()
    };
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
                "tx_rate",
                95,
                "230.8 tx/min (50 txs over 13s)",
            ),
            mk_score(
                SignalLayer::Microstructure,
                "tx_success_rate",
                70,
                "94% success rate (3 failed of 50)",
            ),
            mk_score(
                SignalLayer::Microstructure,
                "recency",
                95,
                "Last transaction 14s ago",
            ),
            mk_score(
                SignalLayer::Microstructure,
                "velocity",
                50,
                "Tx velocity 1.0x vs prior period",
            ),
            mk_score(
                SignalLayer::Safety,
                "top_holder",
                90,
                "Top holder owns 3.8% of supply",
            ),
            mk_score(
                SignalLayer::Safety,
                "top5_concentration",
                85,
                "Top 5 holders own 5.6% of supply",
            ),
            mk_score(
                SignalLayer::OnChain,
                "history_depth",
                25,
                "50 txs in <30s — extreme burst, likely just launched",
            ),
        ],
        confidence: Confidence {
            total: 70,
            base_total: 64,
            top_holder_bonus_pct: 10,
            momentum: 77,
            distribution: 53,
            spring: 65,
            coverage: 4,
            layer_scores: vec![],
            classification: "STAIRCASE".into(),
            reasoning: "STAIRCASE · mom 77 · dist 53 · spring 65".into(),
        },
        delta: Some(TokenDelta {
            previous: prev,
            current,
            top_holder_delta: 0.3,
            top5_delta: 0.3,
            holder_count_delta: 0,
            momentum_delta: 8,
            time_elapsed_seconds: 9000,
            concentration_direction: "stable".into(),
            classification_changed: true,
        }),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let sample = sample_winner();
    let meta = metadata::fetch(&sample.address).await.ok().flatten();
    match &meta {
        Some(m) => println!(
            "[meta] ${} \"{}\" · px={:?} mc={:?} liq={:?} age={:?}",
            m.symbol,
            m.name,
            m.price_usd,
            m.market_cap_usd,
            m.liquidity_usd,
            m.age_human()
        ),
        None => println!("[meta] no DexScreener data for {}", sample.address),
    }

    let styles = [
        ("monster", Template::Monster),
        ("winner", Template::Winner),
        ("ops", Template::Ops),
        ("inspect", Template::Inspect),
    ];

    for (name, style) in styles {
        let html = templates::render(&sample, meta.as_ref(), style);
        println!(
            "\n========== {} ==========\n{}\n",
            name.to_uppercase(),
            html
        );

        if let (Ok(token), Ok(chat)) = (std::env::var("TG_BOT_TOKEN"), std::env::var("TG_CHAT_ID"))
        {
            let url = format!("https://api.telegram.org/bot{}/sendMessage", token);
            let body = [
                ("chat_id", chat.as_str()),
                ("text", &html),
                ("parse_mode", "HTML"),
                ("disable_web_page_preview", "true"),
            ];
            let client = reqwest::Client::new();
            let resp = client.post(&url).form(&body).send().await?;
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            println!(
                "[tg] {} -> {} ({})",
                name,
                status,
                body.chars().take(120).collect::<String>()
            );
            tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
        }
    }
    Ok(())
}
