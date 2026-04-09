# Photon Signal Forecaster — Field Research Findings

Date: 2026-04-09
Session: First live trading session with structural analysis

## Summary

Built and tested a Solana token signal forecaster against live Pump.fun and Raydium data. Studied ~25 tokens across their full lifecycles. Entered one trade (PNUT), lost on it, and used the loss to calibrate the model. The core insight: every Pump.fun token is a trap. The question is never "is this safe?" — it's "when does the trap have a window, and how wide is it?"

## Token Lifecycle (Universal)

Every Pump.fun token follows the same skeleton:

```
Birth (100% concentrated)
  → Distribution phase (creator sells into demand)
    → Either: sustained distribution → staircase (rare)
    → Or: re-concentration → collapse (common)
      → Death (back to 90%+ concentrated, activity dies)
```

The entire lifecycle can complete in 3 minutes (PNUT) or stretch over months (GAYCOIN). The difference is holder depth.

## Pattern Taxonomy

### DEAD
- 100% concentrated, no activity
- Never achieved distribution
- Vast majority of Pump.fun launches die this way
- Example: 43AX...pump — 1 holder, Token-2022, velocity 0.0x

### ACTIVE_TRAP
- Activity present but still concentrated (>50% top holder)
- Distribution may or may not be starting
- Watch for top holder % to drop on consecutive reads
- Example: PNUT at T+0 — 43% top holder, 54 tx/min, looked alive

### SURGE
- Explosive activity on concentrated token
- Very short window (1-5 minutes typically)
- The ride is fast or there is no ride
- Example: 61V8...pump — 429 tx/min, 32% success rate, 43% top holder

### DEVELOPING
- Between states — could go either way
- Watch for classification transitions
- Example: 7dAX...pump — started at 53% top holder, dropped to 47% (distributing)

### SPRING
- Well distributed (top holder <30%), quiet activity
- Loaded potential — waiting for ignition catalyst
- Highest value signal when velocity starts rising
- Example: SENNA — 22.9% top holder for weeks, then spiked vertically

### STAIRCASE
- Active with deep holder base — the target pattern
- Multiple waves, each consolidating higher
- 1000+ holders create structural resilience
- Example: GAYCOIN — 4,082 holders, months old, 302% 24h, wave 3 forming

## Specimens Studied

| Token | MC | Holders | Top1% | Pattern | Key Lesson |
|---|---|---|---|---|---|
| PNUT | $9.4K→$3.4K | 20 | 43→80% | Fast collapse | Re-concentration = death. Entered at 0.3x velocity (already dying) |
| ONI | $11.6K→dead | 131→5 | 35→99% | Fake second wave | Velocity 3.5x was exit acceleration, not entry. Direction matters |
| WILD | $165K | 141 | 9.8% | Spike + fade | Best distribution but momentum died. Structure without ignition |
| SENNA | $216K | 597 | 22.9% | Dormant spike | Zero sells after spike. Loaded spring. What triggers ignition? |
| GAYCOIN | $123K | 4,082 | 27.4% | Multi-wave staircase | Months old, survived multiple cycles. Holder depth = immune system |
| PIPPIN | $32.5M | 47,017 | 21.7% | Equilibrium | $4.7M liquidity absorbs everything. Gravitational mass |
| AVA AI | $7.62M | 47,755 | 28.7% | Staircase climbing | Each wave higher than last. Low velocity + rising price = organic |
| CHILLGUY | $9.93M | 116,539 | 11.3% | Demand congestion | 84% tx failure is BULLISH with 116K holders. Context is everything |
| POPDOG | $55K | 387 | 14.5% | Post-spike pullback | Sell $ > Buy $ despite more buy count. Whale distributing into retail |
| 7dAX | fresh | 20 | 53→47% | Live distribution | Caught DEVELOPING → STAIRCASE transition in real time |

## Key Findings

### 1. Holder Count is the Immune System
- <100 holders = dies in minutes
- 100-1000 = survives hours, maybe a day
- 1000-5000 = can sustain multiple waves
- >10,000 = gravitational — the token pulls in more buyers just by existing
- The threshold between fragile and resilient is somewhere in the hundreds

### 2. Velocity Without Direction is Noise
- ONI at 3.5x velocity was collapsing (exits accelerating)
- GAYCOIN at 3.0x velocity was building a wave (entries accelerating)
- Must cross-reference velocity with concentration delta
- Velocity + concentration dropping = surge (bullish)
- Velocity + concentration rising = collapse (bearish)

### 3. Re-concentration is the Death Signal
- Top holder % increasing across two consecutive reads predicted collapse in every case
- Zero false positives in our sample (PNUT, ONI both confirmed)
- This should be an automatic EXIT alert

### 4. Buy/Sell Count vs Dollar Ratio
- Count ratio shows sentiment (more people buying or selling)
- Dollar ratio shows power (who has more capital)
- Many small buys absorbing few large sells = whale distributing into retail
- When dollar ratio approaches 1:1 despite high count ratio — whale is winning
- The wave lives as long as retail demand absorbs the distribution

### 5. Transaction Failure Rate is Contextual
- 84% failure on CHILLGUY (116K holders) = demand congestion = BULLISH
- 84% failure on PNUT (20 holders) = just congestion = NOISE
- Same metric, completely different meaning depending on holder depth
- Encoded as cross-layer "demand_congestion" signal

### 6. The Staircase Requires a Dormancy Period
- GAYCOIN, WILD, SENNA, AVA all had extended quiet periods before waves
- Theory: dormancy filters weak hands, leaving holders who won't sell at first spike
- Not confirmed as causal — could be coincidental

### 7. Second Waves are Usually Traps
- WILD's second wave was lower than first
- ONI's "second wave" was the exit stampede
- Exception: GAYCOIN (multiple waves over months)
- The difference is holder base depth — 4,082 holders can absorb velocity changes

### 8. Liquidity Pool Depth Determines Wave Amplitude
- PNUT: $8K liquidity — one sell drains the pool
- AVA: $925K — sells get absorbed, staircase continues
- PIPPIN: $4.7M — effectively infinite buffer
- Ratio of liquidity to MC predicts survivability

## Open Questions

1. **What triggers ignition on a spring?** SENNA sat dormant for weeks then spiked. What was the catalyst?
2. **Where exactly is the holder count threshold?** Need more data points between 100 and 1000.
3. **Does Pump.fun graduation to Raydium change dynamics?** Every staircase had graduated. Correlated or causal?
4. **What does early distribution look like minute-by-minute?** We always see it after the fact.
5. **Is velocity direction reliably inferable from snapshots?** We're seeing net delta, not the path.
6. **Are Token-2022 tokens structurally different?** Fresh launches are Token-2022, established tokens are standard SPL.
7. **Do spring tokens ignite more than once?** GAYCOIN suggests yes, but is that the rule?
8. **What's the optimal entry timing?** Velocity > 2x on a distributing token, or wait for holder base to grow?

## Model Evolution

### v1: Safety-gated (broken)
- High safety + any activity = high score
- Failed: everything scored "medium risk", concentrated tokens tanked the score
- Lesson: safety as a gate blocks the signal

### v2: Momentum-driven (better)
- 70% momentum + 30% safety
- Surge candidates now score higher than safe dead tokens
- Failed: didn't distinguish between momentum directions

### v3: Pattern taxonomy (current)
- Classification: SPRING / STAIRCASE / SURGE / ACTIVE_TRAP / DEAD / DEVELOPING
- 40% momentum + 30% distribution + 30% spring
- Cross-layer synthesis: demand congestion, spring ignition, velocity exit warning
- Delta tracking: concentration direction, classification transitions
- Watchlist: auto-tracking interesting tokens every 15 seconds

## Architecture State

```
Scanner (15s cycle)
  Phase 1: Re-analyze watchlist (3 tokens) with delta
  Phase 2: Discover new from Pump.fun (3 tokens)
  → Classify, alert, watchlist enrollment

4 MCP tools:
  scan    — read alert queue (instant)
  inspect — full analysis with delta from previous snapshot
  trade   — preview with guardrails (execution not yet wired)
  status  — wallet balance, RPC health, pending alerts

SQLite tables:
  tokens, token_signals, token_snapshots, watchlist,
  alerts, trades, wallets, wallet_trades, regimes, audit_log

Signal layers:
  Safety: mint/freeze authority, Token-2022, holder concentration
  Microstructure: tx rate, success rate, velocity, recency, demand congestion
  OnChain: supply, decimals, history depth
  Cross-layer: demand congestion, spring ignition, velocity exit warning
```

## Next Steps

1. Wire execution pipeline (Jupiter routing, Jito bundles) for the trade tool
2. Build statistical model for window duration prediction from snapshot time-series
3. Add Helius total holder count API (our RPC only sees top 20)
4. Track buy/sell dollar ratio from parsed transaction data
5. Study more specimens in the DEVELOPING → STAIRCASE transition
6. Feedback loop: record trade outcomes, reweight signals from results
