# PumpPortal Migration Event Shape — Capture Attempt

## Status: NO MIGRATION EVENTS CAPTURED YET

Capture run: 2026-04-28 ~05:10 UTC
Window scanned: 2026-04-27 23:50:17 UTC → 2026-04-28 05:09:26 UTC (~5h19m of pumpportal-connected uptime)

## Method

```
ssh claudeinator "docker logs madapes-photon 2>&1 | grep 'pumpportal::raw' | head -2000"
```

The photon container is running with the default tracing filter (no `RUST_LOG` env var set in `docker inspect madapes-photon`). The `Raw` event arm at `photon/src/main.rs:214–222` emits at `tracing::info!(target: "pumpportal::raw", ...)`, which IS visible at the default INFO level — so any non-`create` event would have appeared as a truncated 240-char preview.

(Note: there is also a `tracing::debug!(target: "pumpportal::raw", ...)` at `photon/src/pumpportal.rs:221` that logs the *full* raw text. That one is suppressed at INFO level. But the sink-side preview at main.rs:221 is INFO and would have fired.)

## Raw Counts

| Source | Count |
|---|---|
| Total `docker logs madapes-photon` lines | 1958 |
| Lines matching `pumpportal::raw` | **0** |
| Lines matching `migrat` (any case) | **0** |
| Lines matching `txType` | **0** |
| `audit_log` rows where `actor='pumpportal'` AND `action='new_token'` | **5453** |
| `audit_log` rows where `actor='pumpportal'` AND `action!='new_token'` | **0** |

## txType Breakdown

Only one txType was *implicitly* observed via the sink's NewToken arm:

| txType | Count | Source |
|---|---|---|
| `create` | 5453 | inferred from `audit_log` where `actor='pumpportal' AND action='new_token'` (sink only inserts new_token rows for `PumpEvent::NewToken`, which `parse_event` only returns when `txType == "create"`) |
| (anything else) | **0** | grep of stdout for `pumpportal::raw` lines |

So: ~17 create events/min over ~5h19m, and zero non-create events of any kind.

## Findings

1. **PumpPortal connectivity is healthy.** The WS reconnected cleanly across at least 5 connect/reconnect cycles in the window (`pumpportal: connected, subscribing to 2 stream(s)` appears 5+ times).
2. **`subscribeNewToken` is firing.** 5453 create events flowed through the sink and into the DB.
3. **`subscribeMigration` produced zero observable events.** No raw payload appeared in stdout, despite the sink unconditionally logging every `Raw(...)` PumpEvent at INFO level under `target: pumpportal::raw`.
4. **This is suspicious but not necessarily a bug.** Migrations on pump.fun are rarer than creates (a small fraction of created tokens ever graduate). Five hours with zero migrations is plausible but on the low end. Worth keeping the capture running longer before assuming the subscribe is broken.

## Possible Explanations (in rough order of likelihood)

1. **Sample window too short / migrations are genuinely rare.** Need to leave the capture running another ~24h and re-grep.
2. **`subscribeMigration` payload is malformed.** Currently `Subscription::payload()` sends `{"method": "subscribeMigration"}` with no params. PumpPortal docs may require a different shape (e.g. an empty `keys: []` array). Worth double-checking against the official pumpportal.fun docs page.
3. **PumpPortal silently rejects unknown subscribe methods.** No ack/error is logged. Could be testing this by sending `subscribeMigration` directly via wscat from the VM and watching for a response or error frame.
4. **Migrations come with a different `txType` value AND don't decode as JSON object.** `parse_event` falls back to `Raw(serde_json::Value::String(text))` on JSON parse failure, which would still log at the sink — so this is unlikely but technically possible if the migration shape is non-JSON-object (extremely improbable for a JSON WS API).

## Proposed Rust Struct

**Not produced.** No live samples to base it on. Adding a speculative struct now would just create code we'd have to rewrite the moment a real event arrives. The existing `PumpEvent::Raw(Value)` fallback is the right behaviour until we have a sample.

## Confidence Rating: 1/5 (cannot produce struct)

Sample size: 0. Cannot derive a struct.

## Suggested Next Steps

1. Leave the capture running. Re-run the same grep in ~24h and ~72h.
2. If still zero migrations after 72h: send a manual `subscribeMigration` from the VM via `wscat -c wss://pumpportal.fun/api/data` and observe what comes back. If nothing comes for ~6h+, the subscribe payload format is the suspect — check pumpportal.fun docs and adjust `Subscription::payload()`.
3. Independent cross-check: the current `discovery::graduation` codepath logs `graduation: 1 known mints active on pumpswap, re-analyzing` regularly (visible in the buffer). That codepath uses RPC sig-walking, not PumpPortal. If those graduations are real, they are happening but not coming through the WS — strong evidence that `subscribeMigration` payload format is wrong.

   In fact, the buffer shows ~10 `graduation: ... known mints active on pumpswap` lines over ~5h19m. That's a real graduation rate from the RPC fallback. PumpPortal's migration stream should have shown the same events. **The fact that RPC sig-walk sees graduations and PumpPortal sees zero is the strongest signal that the migration subscribe is broken.** This is the recommended thing to debug next.

## Files Referenced

- Source: `/home/asuramaya/code/MadApesAI/photon/src/pumpportal.rs`
- Sink: `/home/asuramaya/code/MadApesAI/photon/src/main.rs:193–226`
- Subscribe payload: `/home/asuramaya/code/MadApesAI/photon/src/pumpportal.rs:56–58`
