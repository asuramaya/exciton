//! Bitquery streaming subscriber — migration capture redundancy for the
//! PumpPortal WS. PumpPortal occasionally drops events (NUT was never
//! delivered on 2026-05-02, possibly CATHOLIC too). Bitquery's
//! `streaming.bitquery.io/graphql` exposes the same Solana DEX
//! activity through a different vendor with different gaps, so two
//! independent feeds reduces single-point-of-failure missed runners.
//!
//! Auth: Ory access token passed via `connection_init` payload as
//! `{ "headers": { "Authorization": "Bearer <token>" } }`. The token
//! comes from `BITQUERY_API_TOKEN` env. When the token is empty the
//! module logs a single warning and exits cleanly — photon stays
//! functional without it.
//!
//! What this scaffold does today: connects, authenticates, subscribes
//! to fresh-token DEX trade activity on Solana, and emits parsed mint
//! addresses through an mpsc channel. The sink in `main.rs` routes
//! mints into the same `tokens` + `add_to_watchlist` path PumpPortal
//! uses, so downstream gates don't care which feed surfaced a mint.
//!
//! What it does NOT do yet: schema-validated migration filtering,
//! cross-source dedup metrics, or rate-limit-aware backoff beyond
//! exponential reconnect. Those land once the live feed proves
//! useful.

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

/// Bitquery's EAP (early-access program) endpoint. The standard
/// `/graphql` endpoint accepts HTTP queries but rejects WS upgrades
/// for `Solana(network: solana)` realtime streams; `/eap` is the
/// canonical streaming path.
const WS_URL: &str = "wss://streaming.bitquery.io/eap";
/// graphql-transport-ws — Bitquery's selected sub-protocol. The legacy
/// `graphql-ws` (apollo) protocol is also accepted but uses different
/// message types; we standardize on transport-ws for forward-compat.
const SUBPROTOCOL: &str = "graphql-transport-ws";

/// Subscribe to new Solana DEX trade events on pump.fun's program.
/// Each event surfaces a mint that just had a swap — for fresh tokens
/// this is effectively first-buy detection and seeds the discovery
/// pipeline. Limited to 100 events/window to stay inside free-tier
/// quota.
const SUBSCRIPTION_QUERY: &str = r#"
subscription {
  Solana(network: solana) {
    DEXTrades(
      limit: { count: 100 }
      where: {
        Trade: {
          Dex: {
            ProgramAddress: { is: "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P" }
          }
        }
      }
    ) {
      Block { Time }
      Trade {
        Buy { Currency { MintAddress Symbol Name } Amount }
      }
      Transaction { Signature }
    }
  }
}
"#;

#[derive(Debug, Clone)]
pub struct BitqueryEvent {
    pub mint: String,
    pub symbol: Option<String>,
    pub name: Option<String>,
    pub signature: Option<String>,
}

pub struct BitqueryHealth {
    pub is_connected: Arc<AtomicBool>,
    pub last_event_at: Arc<AtomicI64>,
}

pub struct BitqueryClient {
    pub events: mpsc::Receiver<BitqueryEvent>,
    pub health: Arc<BitqueryHealth>,
}

/// Spawn the Bitquery subscriber. When `BITQUERY_API_TOKEN` is empty,
/// returns a client whose channel will close immediately — caller's
/// `recv()` returns None and the sink loop exits cleanly. No auth
/// retries; that's a configuration problem, not a transient one.
pub fn spawn() -> BitqueryClient {
    let (tx, rx) = mpsc::channel(512);
    let health = Arc::new(BitqueryHealth {
        is_connected: Arc::new(AtomicBool::new(false)),
        last_event_at: Arc::new(AtomicI64::new(0)),
    });
    let token = std::env::var("BITQUERY_API_TOKEN").unwrap_or_default();
    if token.is_empty() {
        tracing::warn!("bitquery: BITQUERY_API_TOKEN unset, subscriber not starting");
        return BitqueryClient { events: rx, health };
    }
    let health_inner = health.clone();
    tokio::spawn(async move {
        run_loop(token, tx, health_inner).await;
    });
    BitqueryClient { events: rx, health }
}

async fn run_loop(
    token: String,
    tx: mpsc::Sender<BitqueryEvent>,
    health: Arc<BitqueryHealth>,
) {
    let mut backoff_secs: u64 = 1;
    loop {
        match connect_and_subscribe(&token, &tx, &health).await {
            Ok(()) => {
                tracing::info!("bitquery: receiver dropped, exiting");
                return;
            }
            Err(e) => {
                health.is_connected.store(false, Ordering::Relaxed);
                tracing::warn!(
                    "bitquery: disconnected: {} — reconnecting in {}s",
                    e,
                    backoff_secs
                );
                tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                backoff_secs = (backoff_secs * 2).min(60);
            }
        }
    }
}

async fn connect_and_subscribe(
    token: &str,
    tx: &mpsc::Sender<BitqueryEvent>,
    health: &Arc<BitqueryHealth>,
) -> Result<()> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::http::HeaderValue;
    let mut req = WS_URL.into_client_request().context("build ws request")?;
    req.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        HeaderValue::from_static(SUBPROTOCOL),
    );
    // Bitquery's WS upgrade authenticates via the HTTP Authorization
    // header, NOT via connection_init payload (the older Apollo
    // subscriptions-transport-ws style). Without this header the
    // upgrade returns 401 before the WS handshake completes.
    let bearer = format!("Bearer {}", token);
    req.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&bearer).context("auth header")?,
    );
    let (ws, _resp) = connect_async(req).await.context("ws connect")?;
    let (mut writer, mut reader) = ws.split();
    health.is_connected.store(true, Ordering::Relaxed);
    tracing::info!("bitquery: connected, authenticating");

    // graphql-transport-ws: client must send connection_init first.
    // Payload is empty since auth already happened on the HTTP upgrade.
    let init = serde_json::json!({
        "type": "connection_init",
        "payload": {}
    });
    writer
        .send(Message::Text(init.to_string()))
        .await
        .context("send connection_init")?;

    // Wait for connection_ack before sending the subscription.
    while let Some(msg) = reader.next().await {
        let msg = msg.context("read ack frame")?;
        if let Message::Text(text) = msg {
            let v: serde_json::Value =
                serde_json::from_str(&text).context("parse ack")?;
            match v.get("type").and_then(|t| t.as_str()) {
                Some("connection_ack") => break,
                Some("connection_error") | Some("error") => {
                    anyhow::bail!("bitquery rejected auth: {}", text);
                }
                _ => continue,
            }
        }
    }

    let sub = serde_json::json!({
        "id": "1",
        "type": "subscribe",
        "payload": { "query": SUBSCRIPTION_QUERY },
    });
    writer
        .send(Message::Text(sub.to_string()))
        .await
        .context("send subscribe")?;
    tracing::info!("bitquery: subscription active");

    while let Some(msg) = reader.next().await {
        let msg = msg.context("read frame")?;
        match msg {
            Message::Text(text) => {
                let now = chrono::Utc::now().timestamp();
                health.last_event_at.store(now, Ordering::Relaxed);
                tracing::debug!(target: "bitquery::raw", "{}", text);
                for ev in parse_events(&text) {
                    if tx.send(ev).await.is_err() {
                        return Ok(());
                    }
                }
            }
            Message::Ping(p) => {
                let _ = writer.send(Message::Pong(p)).await;
            }
            Message::Close(frame) => {
                anyhow::bail!("server closed: {:?}", frame);
            }
            _ => {}
        }
    }
    anyhow::bail!("stream ended without close frame")
}

/// Each `next` message wraps a `data.Solana.DEXTrades` array. Extract
/// `Trade.Buy.Currency.MintAddress` per trade. Schema drift surfaces
/// as zero events extracted — caller logs raw at debug, operator can
/// inspect.
fn parse_events(text: &str) -> Vec<BitqueryEvent> {
    let v: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };
    if v.get("type").and_then(|t| t.as_str()) != Some("next") {
        return Vec::new();
    }
    let trades = match v
        .pointer("/payload/data/Solana/DEXTrades")
        .and_then(|t| t.as_array())
    {
        Some(arr) => arr,
        None => return Vec::new(),
    };
    let mut out = Vec::new();
    for trade in trades {
        let currency = trade.pointer("/Trade/Buy/Currency");
        let mint = currency
            .and_then(|c| c.get("MintAddress"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let Some(mint) = mint else { continue };
        if mint.is_empty() {
            continue;
        }
        // Skip the system program and wrapped SOL — these are the
        // "buy side" when someone is SELLING a token for SOL; not real
        // discoverable mints. Without this filter the firehose floods
        // with thousands of useless SOL events per minute.
        if mint == "11111111111111111111111111111111"
            || mint == "So11111111111111111111111111111111111111112"
        {
            continue;
        }
        out.push(BitqueryEvent {
            mint,
            symbol: currency
                .and_then(|c| c.get("Symbol"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            name: currency
                .and_then(|c| c.get("Name"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            signature: trade
                .pointer("/Transaction/Signature")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
        });
    }
    out
}
