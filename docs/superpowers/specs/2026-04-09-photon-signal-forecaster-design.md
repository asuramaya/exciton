# Photon Signal Forecaster — Design Spec

## Purpose

A collaborative intelligence system for Solana token trading. Single Rust binary with embedded SQLite database that ingests chain data, scores it through four signal layers, and exposes everything to a human-Claude partnership via MCP tools.

Not an autonomous bot. A forecaster with superhuman sensor range and reaction speed, where neither the model nor the human flies blind. Full parity between both operators.

## Core Principles

1. **Wallet security** — private keys never in config or binary, signing isolated, no action without confirmation
2. **Security** — encrypted database, no stored secrets, TLS-pinned RPC, audit log
3. **Safety** — honeypot simulation, rug detection, bundled launch detection, Token-2022 checks
4. **Integrity** — immutable signal history, WAL-mode SQLite, every forecast reproducible from its inputs
5. **Reliability** — graceful degradation, RPC failover, stale-feed detection, clean shutdown
6. **Simple interface** — four tools, each with built-in check-present-confirm-act-verify flow

## Philosophy

Study structure, not social deception. The chain doesn't lie, it just moves fast. Social signals are excluded by design — they lag structure or actively mislead.

The core engineering problem is regime detection: the market's story changes constantly. The forecaster must know what kind of market it's in right now before deciding how to act.

Aggressive positioning (up to 15% portfolio) only on high-confidence multi-layer signal convergence.

---

## Architecture

Single Rust binary. No microservices. Modules connected by async channels (`tokio::mpsc`).

```
┌──────────────────────────────────────────────────────┐
│                    photon binary                      │
│                                                       │
│  ┌──────────┐                                         │
│  │   RPC    │──────────┐                              │
│  │ Ingester │          ▼                              │
│  └──────────┘ ┌─────────────────┐                     │
│               │    SQLite DB    │                     │
│               │                 │◄──── Signal         │
│               │  Token history  │      Processors     │
│               │  Wallet profiles│      read & write   │
│               │  Trade outcomes │                     │
│               │  Signal scores  │                     │
│               │  Regime states  │                     │
│               │  Audit log      │                     │
│               └────────┬────────┘                     │
│                        │                              │
│               ┌────────▼────────┐                     │
│               │   Forecaster    │                     │
│               │  Queries history│                     │
│               │  Scores now vs  │                     │
│               │  past patterns  │                     │
│               └────────┬────────┘                     │
│                        │                              │
│               ┌────────▼────────┐                     │
│               │   MCP Server    │                     │
│               │   stdio transport│                    │
│               └─────────────────┘                     │
└──────────────────────────────────────────────────────┘
```

---

## Signal Layers

### 1. On-chain signals

- New liquidity pool creation (Raydium, Pump.fun migrations)
- LP lock/burn events
- Whale wallet movements (large transfers in/out)
- Token holder distribution changes (concentration, distribution velocity)
- Mint/freeze authority changes

### 2. Market microstructure

- Buy/sell pressure ratio over sliding windows (10s, 1m, 5m)
- Volume spike detection relative to token's historical baseline
- Spread changes and depth shifts
- Trade size distribution (retail vs whale clustering)

### 3. Safety scoring

- Mint authority check (renounced or active)
- Freeze authority check
- Token-2022 Permanent Delegate extension detection
- Honeypot simulation (simulate sell before recommending buy)
- Bundled launch detection (creator buys in same block as deploy)
- LP unlock schedule risk

### 4. Smart money tracking

- Wallet performance history (rolling win rate stored in SQLite)
- Convergence detection — multiple tracked wallets entering the same token
- Position sizing signals — are smart wallets going big or testing?
- Exit pattern tracking — when do profitable wallets sell?

---

## Forecaster

Reads all four signal layers plus historical data from SQLite.

- **Confidence score**: 0–100, weighted sum of layer scores with dynamic weights that shift based on detected regime
- **Regime detection**: classifies current market state (e.g. "launch frenzy", "whale accumulation", "low-activity grind", "dump cascade") by comparing current signal patterns against historical labeled windows
- **Adaptive sizing**: maps confidence to position size — low confidence = 0.5% portfolio, high conviction multi-layer convergence = up to 15%
- **Decay**: scores decay over time if not reinforced by new signals — stale opportunities drop off
- **Feedback loop**: every trade outcome recorded with the signal state at entry, simple statistical reweighting over time — transparent, inspectable, no black-box ML

---

## Database (SQLite, encrypted via SQLCipher)

WAL mode. Immutable signal history. Write-ahead logging for crash safety.

### Tables

| Table | Purpose |
|-------|---------|
| `tokens` | Address, first_seen, metadata, current safety score |
| `token_signals` | Timestamped signal events per token per layer |
| `wallets` | Tracked wallets, cumulative stats (win rate, avg return, trade count) |
| `wallet_trades` | Every observed trade from tracked wallets |
| `trades` | Our executed trades — entry, exit, outcome, signal state at entry |
| `regimes` | Timestamped regime classifications with feature vectors |
| `alerts` | Threshold-crossing events with context |
| `audit_log` | Immutable append-only log of every action taken |

---

## MCP Interface — Four Tools

Every tool follows the same flow: **check → present → confirm → act → verify**.

### `scan`

What's happening right now.

**Flow:**
1. Check system health (RPC feed alive, DB writable, signal layers active)
2. Query forecaster for top scored opportunities
3. Include current regime classification
4. Include any active alerts above threshold
5. Present with confidence breakdowns and supporting evidence

**Returns:** System health, regime state, ranked opportunities with per-layer scores, active alerts.

### `inspect`

Deep dive on a token or wallet.

**Flow:**
1. Check if token/wallet exists in database — if not, fetch live from chain
2. Run all four signal layers against the target
3. Pull full history from database (price action, past signals, related wallet activity)
4. Run safety checks (honeypot sim, authority checks, Token-2022 scan)
5. Present complete picture with risk assessment

**Returns:** Full signal breakdown, safety report, historical context, related smart money activity, risk rating.

### `trade`

Execute a trade with full guardrails.

**Flow:**
1. Run safety checks on target token (honeypot sim, authority, Token-2022)
2. Check wallet balance — verify sufficient funds, never overdraw
3. Simulate transaction — estimate output, slippage, fees
4. Present full trade preview: what you'll spend, what you'll get, fees, risks, confidence score
5. **Wait for explicit confirmation**
6. Build and sign transaction (signing module isolated, keys from OS keychain)
7. Submit via Jito bundle for MEV protection
8. Verify transaction landed — check on-chain confirmation
9. Record to `trades` table with full signal state for feedback loop
10. Report outcome

**Returns:** Trade preview (pre-confirm), then execution result with tx signature and verification.

### `status`

Portfolio and system health.

**Flow:**
1. Check all system components (RPC connection, DB, signal layers, ingester lag)
2. Query current positions with live valuations
3. Calculate P&L (per position and total)
4. Check exposure against risk parameters
5. Report data feed freshness
6. Present clean summary

**Returns:** Positions, P&L, total exposure, system health, data freshness, risk status.

---

## Wallet Security

- Private keys **never** stored in config, database, or binary
- Keys loaded from OS keychain or hardware wallet at signing time only
- Transaction signing isolated in its own module — no network access except Solana RPC submit
- All outbound transactions require explicit MCP confirmation
- Wallet balance verified before every trade
- No blanket approvals — every trade is individually confirmed

## System Security

- SQLite encrypted at rest via SQLCipher
- Config file holds no secrets — RPC keys and sensitive values via environment variables only
- All RPC connections over TLS, certificate pinned
- Audit log is append-only, never modified or truncated
- Binary runs with minimal OS permissions

## Reliability

- Graceful degradation — if a signal layer fails, others continue; confidence score reflects reduced coverage
- RPC connection retry with exponential backoff
- Multiple RPC endpoints configurable for failover
- Watchdog on ingester — alerts through MCP if data feed goes stale beyond threshold
- Clean shutdown — flush all pending writes, close DB cleanly
- WAL checkpoint on shutdown

---

## Infrastructure

- **Language**: Rust, first principles, no unnecessary dependencies
- **Async runtime**: tokio
- **Database**: SQLite via rusqlite + SQLCipher
- **Solana interaction**: solana-sdk, solana-client (direct RPC, no wrapper frameworks)
- **DEX routing**: Jupiter aggregator for best execution across pools
- **MEV protection**: Jito bundle submission
- **MCP transport**: stdio (Claude connects directly)
- **Config**: TOML file (RPC endpoints, tracked wallets, risk parameters, alert thresholds)
- **Deployment**: Single Docker container, SQLite file as mounted volume
- **Dev workflow**: Docker-native, iterates on-device via Kubernetes

---

## Rust Workspace Structure

```
photon/
├── Cargo.toml              # workspace root
├── src/
│   ├── main.rs             # bootstrap, config loading, channel wiring
│   ├── config.rs           # TOML config parsing
│   ├── db/
│   │   ├── mod.rs          # connection pool, migrations, WAL setup
│   │   └── schema.rs       # table definitions, queries
│   ├── ingester/
│   │   ├── mod.rs          # RPC WebSocket listener, tx parsing
│   │   └── parser.rs       # Solana transaction → domain events
│   ├── signals/
│   │   ├── mod.rs          # signal layer trait, shared types
│   │   ├── onchain.rs      # liquidity, whale, holder signals
│   │   ├── microstructure.rs # pressure, volume, spread signals
│   │   ├── safety.rs       # rug detection, honeypot, Token-2022
│   │   └── smartmoney.rs   # wallet tracking, convergence
│   ├── forecaster/
│   │   ├── mod.rs          # aggregation, confidence scoring
│   │   ├── regime.rs       # regime detection and classification
│   │   └── feedback.rs     # trade outcome → weight adjustment
│   ├── execution/
│   │   ├── mod.rs          # trade building, Jupiter routing
│   │   ├── signer.rs       # isolated signing module (keychain access)
│   │   └── jito.rs         # Jito bundle submission
│   └── mcp/
│       ├── mod.rs          # MCP server, stdio transport
│       ├── scan.rs         # scan tool implementation
│       ├── inspect.rs      # inspect tool implementation
│       ├── trade.rs        # trade tool with confirmation flow
│       └── status.rs       # portfolio and health reporting
├── config.example.toml
├── Dockerfile
└── docs/
```

---

## Edge Cases

These are the structural edge cases the forecaster must handle — the situations where naive bots lose money:

### Token Lifecycle Traps
- **Bundled launches**: Creator buys tokens in the same block as deployment, creating fake initial volume. Detection: compare deployer wallet with first N buyers.
- **Delayed rugs**: Token looks safe for hours/days, then LP is removed. Mitigation: continuous LP monitoring, not just at entry.
- **Token-2022 Permanent Delegate burn**: Token uses the extension to burn your tokens after purchase. Detection: flag any token with this extension, block by default.
- **Fake renounce**: Mint authority "renounced" to a contract the creator still controls. Detection: trace authority to check if it's an EOA or a program with known patterns.

### Market Structure Traps
- **Wash trading**: Smart money wallets trading with themselves to create volume signals. Detection: graph analysis of buyer/seller overlap.
- **Coordinated pump**: Multiple wallets buying in sequence to trigger volume/pressure signals. Detection: timing analysis, wallet clustering, funding source tracing.
- **Liquidity mirages**: Thin LP that looks deep due to concentrated ranges. Detection: simulate a sell of your intended position size — if slippage exceeds threshold, downgrade confidence.
- **Stale signals**: Market moved but RPC data lagged. Detection: compare timestamps across multiple data sources, degrade confidence when freshness drops.

### Execution Traps
- **Sandwich attacks**: MEV bots front-run and back-run your trade. Mitigation: Jito bundles, reasonable slippage limits.
- **Failed transactions during congestion**: TX submitted but never lands. Handling: monitor confirmation, retry with higher priority fee, alert if repeated failures.
- **Partial fills**: On low-liquidity tokens. Handling: simulate expected output before submitting, alert if actual deviates from expected.
- **Price movement between signal and execution**: By the time you act, the opportunity is gone. Handling: freshness check immediately before execution, abort if signal has decayed.

### System Traps
- **RPC endpoint failure**: Single point of failure. Handling: failover to backup endpoints, degrade gracefully, alert.
- **Database corruption**: SQLite file damaged. Handling: WAL mode, regular checkpoint, backup strategy.
- **Memory pressure**: Tracking too many tokens/wallets fills RAM. Handling: configurable limits on active tracking, prune stale entries.
- **Clock drift**: Timestamp-based analysis breaks if system clock is wrong. Handling: compare system time against Solana slot timestamps.
