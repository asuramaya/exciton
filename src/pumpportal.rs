//! PumpPortal WebSocket client. The data source that should have been
//! anchoring discovery + graduation detection from day one.
//!
//! photon's original sig-walking discovery (Phase 2) and graduation walk
//! (Phase 2b) were placeholders standing in for the real upstream: an
//! event feed of pump.fun activity. PumpPortal hosts that feed at
//! `wss://pumpportal.fun/api/data` over a single multiplexed
//! WebSocket. New-token, trade, account-trade, and migration streams
//! are all subscriptions on the one connection.
//!
//! What this module owns:
//!   - one WS connection (the docs explicitly forbid multiple — they
//!     ban for up to an hour)
//!   - the active subscription set (so reconnect can re-subscribe)
//!   - exponential reconnect backoff (1s → 60s cap)
//!   - parsed events emitted into an mpsc::Sender for downstream sinks
//!   - health: is_connected() + last_event_at()
//!
//! Scanner Phase 2 / 2b read these health signals and skip their RPC
//! sig-walks when the WS is fresh, falling back when it's stale. There
//! is no feature flag — connectivity is the gate.
//!
//! What this module does NOT own:
//!   - DB writes. The sink task converts events to inserts/updates.
//!   - Reanalysis. Migration events trigger the sink to call
//!     scanner-side helpers; the WS layer stays IO-only.

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

pub const WS_URL: &str = "wss://pumpportal.fun/api/data";

/// Subscription methods we care about today. Trade + account-trade
/// methods exist too but they're a separate phase.
#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub enum Subscription {
    NewToken,
    Migration,
}

impl Subscription {
    fn method(&self) -> &'static str {
        match self {
            Subscription::NewToken => "subscribeNewToken",
            Subscription::Migration => "subscribeMigration",
        }
    }

    fn payload(&self) -> serde_json::Value {
        serde_json::json!({ "method": self.method() })
    }
}

/// Parsed event types. The two streams we subscribe to today
/// (`subscribeNewToken`, `subscribeMigration`) get typed variants;
/// anything unknown lands as `Raw` so the sink can still see it for
/// debugging and we surface schema changes loudly instead of silently.
#[derive(Debug, Clone)]
pub enum PumpEvent {
    NewToken(NewTokenEvent),
    Migration(MigrationEvent),
    Raw(serde_json::Value),
}

/// `subscribeMigration` event payload. Captured from a real production
/// sample on 2026-04-28 — undocumented field shape but consistent across
/// every event observed:
///
///   {
///     "signature": "...",
///     "mint": "...",
///     "txType": "migrate",
///     "pool": "pump-amm"   // or "raydium" historically
///   }
///
/// Pushed when a pump.fun token completes its bonding curve and gets
/// migrated to an AMM. Replaces the `discovery::check_graduated_tokens`
/// RPC sig-walking path, which used to walk pumpswap signatures every
/// 15s burning ~50 RPC calls/min.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MigrationEvent {
    pub mint: String,
    #[serde(default)]
    pub signature: Option<String>,
    #[serde(rename = "txType", default)]
    pub tx_type: Option<String>,
    /// AMM destination — usually `"pump-amm"` post-March-2025; `"raydium"`
    /// for the older migration path. Shown in TG cards as a transparency
    /// signal of where the token's now trading.
    #[serde(default)]
    pub pool: Option<String>,
}

/// `subscribeNewToken` event payload, fields per PumpPortal docs +
/// community examples. Optional fields are tolerated as `None` so a
/// schema drift on PumpPortal's side doesn't crash the consumer.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NewTokenEvent {
    pub mint: String,
    #[serde(rename = "bondingCurveKey", default)]
    pub bonding_curve_key: Option<String>,
    #[serde(default)]
    pub signature: Option<String>,
    #[serde(rename = "traderPublicKey", default)]
    pub trader_public_key: Option<String>,
    #[serde(rename = "initialBuy", default)]
    pub initial_buy: Option<f64>,
    #[serde(rename = "marketCapSol", default)]
    pub market_cap_sol: Option<f64>,
    #[serde(rename = "vSolInBondingCurve", default)]
    pub v_sol_in_bonding_curve: Option<f64>,
    #[serde(rename = "vTokensInBondingCurve", default)]
    pub v_tokens_in_bonding_curve: Option<f64>,
    #[serde(rename = "txType", default)]
    pub tx_type: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub symbol: Option<String>,
    #[serde(default)]
    pub uri: Option<String>,
}

/// Health surface read by scanner. `is_connected` is true while a
/// socket is open; `last_event_at` ticks on every received message.
/// Scanner gates on `now - last_event_at < 30s` rather than just
/// `is_connected` — a "connected but silent" socket isn't useful.
pub struct PumpPortalHealth {
    pub is_connected: Arc<AtomicBool>,
    pub last_event_at: Arc<AtomicI64>,
}

impl PumpPortalHealth {
    pub fn fresh(&self, max_age_secs: i64) -> bool {
        if !self.is_connected.load(Ordering::Relaxed) {
            return false;
        }
        let last = self.last_event_at.load(Ordering::Relaxed);
        if last == 0 {
            return false;
        }
        chrono::Utc::now().timestamp() - last < max_age_secs
    }

    pub fn snapshot(&self) -> (bool, i64) {
        (
            self.is_connected.load(Ordering::Relaxed),
            self.last_event_at.load(Ordering::Relaxed),
        )
    }
}

/// Public handle to the running client. Drop to stop (mpsc closes,
/// connection task exits).
pub struct PumpPortalClient {
    pub events: mpsc::Receiver<PumpEvent>,
    pub health: Arc<PumpPortalHealth>,
}

/// Spawn the WS task. Returns a receiver of parsed events + a health
/// handle. Connection lifecycle is fully owned by the spawned task —
/// caller never touches the socket directly.
pub fn spawn(subscriptions: Vec<Subscription>) -> PumpPortalClient {
    // 1024 events of in-flight buffer. New-token + migration combined
    // peak at maybe ~10/sec on busy stretches; 1024 is ~100s of headroom
    // before backpressure kicks in. The sink should be much faster than
    // ingestion; if it ever fills, that's a real bug to surface.
    let (tx, rx) = mpsc::channel(1024);
    let health = Arc::new(PumpPortalHealth {
        is_connected: Arc::new(AtomicBool::new(false)),
        last_event_at: Arc::new(AtomicI64::new(0)),
    });
    let health_inner = health.clone();
    tokio::spawn(async move {
        run_loop(subscriptions, tx, health_inner).await;
    });
    PumpPortalClient { events: rx, health }
}

/// Connect + listen forever. On any disconnect, exponential backoff and
/// retry. Subscriptions persist across reconnects (the docs say a single
/// connection should hold all of them, so on reconnect we re-establish
/// the full set).
async fn run_loop(
    subscriptions: Vec<Subscription>,
    tx: mpsc::Sender<PumpEvent>,
    health: Arc<PumpPortalHealth>,
) {
    let subs: HashSet<Subscription> = subscriptions.into_iter().collect();
    let mut backoff_secs: u64 = 1;
    loop {
        match connect_and_listen(&subs, &tx, &health).await {
            Ok(()) => {
                // Clean exit (channel closed by drop). Bail.
                tracing::info!("pumpportal: receiver dropped, exiting");
                return;
            }
            Err(e) => {
                health.is_connected.store(false, Ordering::Relaxed);
                tracing::warn!("pumpportal: disconnected: {} — reconnecting in {}s", e, backoff_secs);
                tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                backoff_secs = (backoff_secs * 2).min(60);
            }
        }
    }
}

async fn connect_and_listen(
    subs: &HashSet<Subscription>,
    tx: &mpsc::Sender<PumpEvent>,
    health: &Arc<PumpPortalHealth>,
) -> Result<()> {
    let (ws, _resp) = connect_async(WS_URL).await.context("ws connect")?;
    let (mut writer, mut reader) = ws.split();
    health.is_connected.store(true, Ordering::Relaxed);
    tracing::info!(
        "pumpportal: connected, subscribing to {} stream(s)",
        subs.len()
    );

    // Subscribe to everything in one connection (the docs are explicit
    // about not opening multiple connections).
    for s in subs {
        let payload = s.payload();
        writer
            .send(Message::Text(payload.to_string()))
            .await
            .with_context(|| format!("send {}", s.method()))?;
    }

    while let Some(msg) = reader.next().await {
        let msg = msg.context("read frame")?;
        match msg {
            Message::Text(text) => {
                let now = chrono::Utc::now().timestamp();
                health.last_event_at.store(now, Ordering::Relaxed);

                // Always log raw at trace until we've captured every
                // event shape we care about. Keep this even at debug
                // level for the first deploy so 8.4 (migration shape
                // capture) has a record to grep through.
                tracing::debug!(target: "pumpportal::raw", "{}", text);

                let parsed = parse_event(&text);
                if tx.send(parsed).await.is_err() {
                    // Sink dropped; exit cleanly.
                    return Ok(());
                }
            }
            Message::Ping(p) => {
                // Some servers expect pong; tungstenite handles auto-
                // pong by default but be explicit for reliability.
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

/// Route by `txType` — currently `"create"` (new token) and `"migrate"`
/// (graduation). Anything else falls through to Raw so unknown events
/// surface in logs rather than getting silently parsed as the wrong
/// thing.
fn parse_event(text: &str) -> PumpEvent {
    let value: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => return PumpEvent::Raw(serde_json::Value::String(text.to_string())),
    };
    let tx_type = value.get("txType").and_then(|v| v.as_str());
    match tx_type {
        Some("create") => {
            if let Ok(evt) = serde_json::from_value::<NewTokenEvent>(value.clone()) {
                return PumpEvent::NewToken(evt);
            }
        }
        Some("migrate") => {
            if let Ok(evt) = serde_json::from_value::<MigrationEvent>(value.clone()) {
                return PumpEvent::Migration(evt);
            }
        }
        _ => {}
    }
    PumpEvent::Raw(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_known_new_token_event() {
        let raw = r#"{
            "mint": "7xKa...",
            "bondingCurveKey": "abc123",
            "signature": "5Lm...",
            "traderPublicKey": "abc",
            "initialBuy": 1000.0,
            "marketCapSol": 30.0,
            "vSolInBondingCurve": 30.0,
            "vTokensInBondingCurve": 1000000000.0,
            "txType": "create",
            "name": "test token",
            "symbol": "TEST",
            "uri": "https://..."
        }"#;
        match parse_event(raw) {
            PumpEvent::NewToken(e) => {
                assert_eq!(e.mint, "7xKa...");
                assert_eq!(e.tx_type.as_deref(), Some("create"));
                assert_eq!(e.symbol.as_deref(), Some("TEST"));
            }
            other => panic!("expected NewToken, got {:?}", other),
        }
    }

    #[test]
    fn unknown_event_falls_back_to_raw() {
        let raw = r#"{"someField": "value", "txType": "buy"}"#;
        match parse_event(raw) {
            PumpEvent::Raw(_) => {}
            other => panic!("expected Raw, got {:?}", other),
        }
    }

    #[test]
    fn malformed_json_falls_back_to_raw_string() {
        match parse_event("not json {") {
            PumpEvent::Raw(serde_json::Value::String(_)) => {}
            _ => panic!("expected Raw string"),
        }
    }

    #[test]
    fn health_fresh_requires_recent_event() {
        let h = PumpPortalHealth {
            is_connected: Arc::new(AtomicBool::new(true)),
            last_event_at: Arc::new(AtomicI64::new(0)),
        };
        assert!(!h.fresh(30), "no events ever ⇒ not fresh");
        h.last_event_at
            .store(chrono::Utc::now().timestamp(), Ordering::Relaxed);
        assert!(h.fresh(30), "just-received event ⇒ fresh");
        h.is_connected.store(false, Ordering::Relaxed);
        assert!(!h.fresh(30), "disconnected ⇒ not fresh regardless of last_event_at");
    }
}
