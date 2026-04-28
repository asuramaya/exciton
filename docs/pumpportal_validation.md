# PumpPortal Integration — Falsifiable Validation

Run timestamp: 2026-04-28 ~05:10 UTC. Container `madapes-photon` up 5 hours; PumpPortal first connected 2026-04-27 23:50 UTC, current uninterrupted session began 2026-04-28 04:35:05 UTC.

## 1. Validation table

| # | Claim | Verdict | Evidence |
|---|---|---|---|
| 1 | New-token inserts/h ≥ 500 | **PASS** | `SELECT COUNT(*) FROM tokens WHERE first_seen > now-3600` returned **871**. Trailing 30m = 452 (≈904/h), 5m = 56 (≈672/h). Steady well above threshold. |
| 2 | On-chain `create` → row in `tokens` ≤ 10s | **PASS (sink-side)** with caveat | Joined `audit_log.timestamp` (sink fired) to `tokens.first_seen` (row inserted) for 10 most recent samples — all deltas were **0 seconds**. Note: `pumpportal::raw` is logged at DEBUG only for non-NewToken events (main.rs:221 logs Raw at INFO; pumpportal.rs:221 logs all-text at DEBUG); the global subscriber is INFO so we can't compare WS-arrival vs DB-insert timestamps from logs alone. The on-chain → PumpPortal-publish leg is not observable from this VM. Sink internal latency is sub-second. |
| 3 | At least one migration event captured | **INCONCLUSIVE / suspicious** | `audit_log` shows 5453 `pumpportal/new_token` rows and **0** non-new_token rows since first connect. `docker logs ... grep pumpportal::raw` returns empty for the full session. Photon subscribes to `Subscription::Migration` (main.rs:182) but no Raw events have been emitted in 5h+ uptime. Could be (a) genuinely zero pumpswap migrations in the window, (b) subscribe call accepted but server not pushing, or (c) deserialiser swallowing migrations as NewToken. Needs a longer window plus targeted shape capture (Task #34). |
| 4 | RPC `getSignaturesForAddress` against pump.fun program drops to ~0 | **PASS** | `docker logs --since 30m \| grep 'getSignaturesForAddress' \| grep '6EF8rrec'` → empty. `grep 'Discovery: walked'` (Phase 2 success log, discovery.rs:64) → empty for 30m. Phase 2 sig-walk is not running. Caveat: 14 `getSignaturesForAddress` 403/429 warnings persist in 30m, but those originate from `photon::ingester` paths (graduation/Phase 2b on pumpswap, and other ingester users) — not Phase 2 on `6EF8rrec…`. |
| 5 | PumpPortal client recovers from disconnect | **PASS (organic)** | Not destructively tested, but logs show natural recovery: `pumpportal: disconnected: read frame — reconnecting in 1s` at 04:35:01 → reconnect attempt 04:35:03 → `pumpportal: connected, subscribing to 2 stream(s)` at 04:35:05.396Z. All-time: 5 connects, 2 disconnects, 100% recovery. Token inserts continued without gap. |

## 2. Diagnostic findings

1. **txType distribution**: Of 5453 `pumpportal/*` audit rows since first connect, 100% are `new_token`, 0% anything else. Distinct actions queried via `SELECT DISTINCT action FROM audit_log WHERE actor='pumpportal'` returned only `new_token`. Reinforces row 3 concern.
2. **Stream freshness**: Latest token `first_seen` = 1777352979, query time = 1777352985 → **6s lag**, well within the 30s `PP_FRESH_SECS` gate (scanner.rs:404). Scanner is correctly suppressing Phase 2.
3. **Connection stability**: 105 total log lines in last 30m, zero `pumpportal: disconnected` and zero `pumpportal: connected` entries — single-session stability since 04:35:05Z (~35 min uninterrupted). Client+server pair is healthy under steady-state load.

## 3. Honest conclusion

PumpPortal integration is **working in the meaningful sense** for its primary job: Phase 2 RPC sig-walking has been replaced by a WebSocket feed that delivers ~870 new-token rows/hour with sub-second sink latency, the scanner gate is correctly suppressing Phase 2 (no `Discovery: walked` lines, no 6EF8rrec sig calls), and the client recovered organically from one disconnect without operator intervention. Confidence rests on three independent signals: SQLite row counts (871/h tokens, 5453 audit_log entries), absence of Phase 2 logs, and the recovery sequence at 04:35Z. **Not yet shippable** without: (a) capturing at least one migration event to confirm the Migration subscription actually delivers data (Task #34) — 0 in 5h is suspicious enough to investigate before retiring Phase 2b, and (b) instrumenting end-to-end latency (on-chain slot → DB insert) since the 0-second sink delta only proves sink-side speed, not full pipeline. Phase 2b graduation is still RPC-driven and still hitting 403/429s on chainstack — that's an orthogonal cleanup but worth flagging.
