// DB layer: many helpers exist as operator-API surface (MCP tools, manual
// queries) without compile-time callers. Allowing dead_code module-wide
// keeps the warnings noise-free without sprinkling per-method attributes.
#![allow(dead_code)]

use anyhow::Result;
use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;
use std::sync::Mutex;

pub struct Db {
    conn: Mutex<Connection>,
}

#[derive(Debug, Clone)]
pub struct Token {
    pub address: String,
    pub first_seen: i64,
    pub safety_score: i32,
}

#[derive(Debug, Clone)]
pub struct AuditEntry {
    pub timestamp: String,
    pub actor: String,
    pub action: String,
    pub details: String,
}

#[derive(Debug, Clone)]
pub struct WatchlistCandidate {
    pub token_address: String,
    pub watch_classification: String,
    pub added_at: i64,
    pub last_checked: i64,
    pub snapshot_classification: Option<String>,
    pub snapshot_confidence: Option<i32>,
    pub snapshot_holder_count: Option<i32>,
    pub snapshot_top_holder_pct: Option<f64>,
    pub snapshot_momentum: Option<i32>,
    pub snapshot_distribution: Option<i32>,
    pub snapshot_timestamp: Option<i64>,
}

impl Db {
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)?;

        conn.pragma_update(None, "journal_mode", "wal")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;

        let db = Self {
            conn: Mutex::new(conn),
        };
        db.create_schema()?;
        Ok(db)
    }

    fn create_schema(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS tokens (
                address TEXT PRIMARY KEY,
                first_seen INTEGER NOT NULL,
                metadata TEXT,
                safety_score INTEGER NOT NULL DEFAULT 0
            );

            -- Tables removed 2026-04-30 (no compile-time inserters):
            --   token_signals — replaced by per-snapshot scores in
            --     token_snapshots (mom/dist/spring/conf columns)
            --   wallets / wallet_trades / trades — superseded by
            --     wallet_ledger (signature-keyed, used by publisher)
            --   regimes — Forecaster::classify_regime was deleted in the
            --     hygiene-audit cleanup; nothing populates this anymore
            -- Existing rows in production DBs are left in place. The
            -- IF NOT EXISTS pattern means dropping the CREATE doesn't
            -- delete data on upgrade.

            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                alert_type TEXT NOT NULL,
                token_address TEXT,
                message TEXT NOT NULL,
                confidence INTEGER NOT NULL,
                acknowledged INTEGER NOT NULL DEFAULT 0,
                timestamp INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL DEFAULT (datetime('now')),
                actor TEXT NOT NULL,
                action TEXT NOT NULL,
                details TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS telegram_deliveries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token_address TEXT NOT NULL,
                channel TEXT NOT NULL,
                message_id INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                last_edit_at INTEGER,
                edit_count INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'active',
                snapshot_conf INTEGER NOT NULL DEFAULT 0,
                snapshot_class TEXT NOT NULL DEFAULT '',
                snapshot_price REAL,
                snapshot_top_holder REAL,
                timeline_json TEXT NOT NULL,
                UNIQUE(token_address, channel)
            );

            CREATE TABLE IF NOT EXISTS telegram_digests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                hour_bucket INTEGER NOT NULL UNIQUE,
                channel TEXT NOT NULL,
                message_id INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                last_edit_at INTEGER,
                edit_count INTEGER NOT NULL DEFAULT 0,
                finalized INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS watchlist (
                token_address TEXT PRIMARY KEY,
                classification TEXT NOT NULL,
                added_at INTEGER NOT NULL,
                last_checked INTEGER NOT NULL DEFAULT 0,
                active INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS token_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token_address TEXT NOT NULL,
                top_holder_pct REAL NOT NULL,
                top5_pct REAL NOT NULL,
                top10_pct REAL NOT NULL,
                holder_count INTEGER NOT NULL,
                tx_rate REAL NOT NULL,
                velocity REAL NOT NULL,
                momentum INTEGER NOT NULL,
                distribution INTEGER NOT NULL,
                spring INTEGER NOT NULL,
                classification TEXT NOT NULL,
                confidence INTEGER NOT NULL,
                timestamp INTEGER NOT NULL,
                price_usd REAL NOT NULL DEFAULT 0,
                mcap_usd REAL NOT NULL DEFAULT 0,
                liquidity_usd REAL NOT NULL DEFAULT 0,
                volume_h1_usd REAL NOT NULL DEFAULT 0,
                buys_h1 INTEGER NOT NULL DEFAULT 0,
                sells_h1 INTEGER NOT NULL DEFAULT 0,
                price_change_h1 REAL NOT NULL DEFAULT 0,
                pair_dex TEXT NOT NULL DEFAULT ''
            );

            CREATE INDEX IF NOT EXISTS idx_snapshots_token ON token_snapshots(token_address);
            CREATE INDEX IF NOT EXISTS idx_snapshots_timestamp ON token_snapshots(timestamp);
            CREATE INDEX IF NOT EXISTS idx_snapshots_token_time ON token_snapshots(token_address, timestamp);
            CREATE INDEX IF NOT EXISTS idx_watchlist_active_checked ON watchlist(active, last_checked);

            -- Indexes for the dead tables (token_signals/wallet_trades/
            -- trades/regimes) removed alongside their CREATE TABLE
            -- statements. Existing indexes in production DBs survive.
            CREATE INDEX IF NOT EXISTS idx_alerts_timestamp ON alerts(timestamp);

            -- Signal-gate near-misses: tokens that came close but didn't pass.
            -- Truth-telling — shows what almost fired so gate tuning is informed
            -- by data, not intuition. Persisted, queryable via /nearmisses.
            CREATE TABLE IF NOT EXISTS signal_near_misses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token_address TEXT NOT NULL,
                classification TEXT NOT NULL,
                effective_confidence INTEGER NOT NULL,
                top_holder_pct REAL NOT NULL,
                momentum_delta INTEGER,
                gate_that_failed TEXT NOT NULL,
                gap TEXT,
                timestamp INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_near_misses_ts ON signal_near_misses(timestamp);

            -- DM bot: per-user state and audit
            CREATE TABLE IF NOT EXISTS users (
                telegram_user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                first_seen INTEGER NOT NULL,
                last_seen INTEGER NOT NULL,
                message_count INTEGER NOT NULL DEFAULT 0,
                is_admin INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS user_watchlist (
                telegram_user_id INTEGER NOT NULL,
                token_address TEXT NOT NULL,
                added_at INTEGER NOT NULL,
                note TEXT,
                PRIMARY KEY (telegram_user_id, token_address)
            );

            CREATE TABLE IF NOT EXISTS user_mutes (
                telegram_user_id INTEGER NOT NULL,
                token_address TEXT NOT NULL,
                muted_at INTEGER NOT NULL,
                PRIMARY KEY (telegram_user_id, token_address)
            );

            CREATE TABLE IF NOT EXISTS command_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_user_id INTEGER NOT NULL,
                command TEXT NOT NULL,
                args TEXT,
                timestamp INTEGER NOT NULL,
                duration_ms INTEGER
            );

            CREATE INDEX IF NOT EXISTS idx_command_log_user ON command_log(telegram_user_id);
            CREATE INDEX IF NOT EXISTS idx_command_log_ts ON command_log(timestamp);
        ",
        )?;

        // Additive migration: `finalized` column on telegram_digests was introduced
        // after initial deploy. ALTER is idempotent-by-intent here — we silently
        // absorb the "duplicate column" error when it already exists.
        let _ = conn.execute(
            "ALTER TABLE telegram_digests ADD COLUMN finalized INTEGER NOT NULL DEFAULT 0",
            [],
        );
        // Track which chat the prior anchor forward lives in so a config
        // flip (e.g. anchor moved from lounge → calls channel) can clean
        // up the orphan in the old chat. Empty means legacy row — the
        // bump path falls back to lounge_chat_id where pre-migration
        // forwards lived.
        let _ = conn.execute(
            "ALTER TABLE lounge_anchor_state ADD COLUMN current_chat_id TEXT NOT NULL DEFAULT ''",
            [],
        );

        // Market-data columns on token_snapshots (DexScreener integration).
        // Each ALTER is individually idempotent — SQLite returns "duplicate
        // column" for rows that already exist, which we ignore.
        for stmt in [
            "ALTER TABLE token_snapshots ADD COLUMN price_usd REAL NOT NULL DEFAULT 0",
            "ALTER TABLE token_snapshots ADD COLUMN mcap_usd REAL NOT NULL DEFAULT 0",
            "ALTER TABLE token_snapshots ADD COLUMN liquidity_usd REAL NOT NULL DEFAULT 0",
            "ALTER TABLE token_snapshots ADD COLUMN volume_h1_usd REAL NOT NULL DEFAULT 0",
            "ALTER TABLE token_snapshots ADD COLUMN buys_h1 INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE token_snapshots ADD COLUMN sells_h1 INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE token_snapshots ADD COLUMN price_change_h1 REAL NOT NULL DEFAULT 0",
            "ALTER TABLE token_snapshots ADD COLUMN pair_dex TEXT NOT NULL DEFAULT ''",
        ] {
            let _ = conn.execute(stmt, []);
        }

        // Smart-wallet tracking (imitation-alpha feed).
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS smart_wallets (
                address TEXT PRIMARY KEY,
                label TEXT NOT NULL DEFAULT '',
                added_at INTEGER NOT NULL,
                last_checked INTEGER NOT NULL DEFAULT 0,
                active INTEGER NOT NULL DEFAULT 1
            );
            CREATE TABLE IF NOT EXISTS smart_wallet_holdings (
                wallet_address TEXT NOT NULL,
                mint_address TEXT NOT NULL,
                balance_ui REAL NOT NULL,
                first_seen INTEGER NOT NULL,
                last_seen INTEGER NOT NULL,
                PRIMARY KEY (wallet_address, mint_address)
            );
            ",
        )?;

        // Deployer tracking on tokens — populated once per new token at discovery.
        let _ = conn.execute(
            "ALTER TABLE tokens ADD COLUMN deployer_address TEXT NOT NULL DEFAULT ''",
            [],
        );
        let _ = conn.execute(
            "ALTER TABLE tokens ADD COLUMN deployer_initial_balance REAL NOT NULL DEFAULT 0",
            [],
        );

        // Top-holder snapshot JSON on each token_snapshots row — the minimal
        // shape holder_evolution needs to diff composition over time.
        let _ = conn.execute(
            "ALTER TABLE token_snapshots ADD COLUMN top_holders_json TEXT NOT NULL DEFAULT '[]'",
            [],
        );
        // Launch forensics — persisted on the latest snapshot so should_signal
        // can read without a separate join. Refreshed hourly per mint.
        let _ = conn.execute(
            "ALTER TABLE token_snapshots ADD COLUMN bundle_pct REAL NOT NULL DEFAULT 0",
            [],
        );
        let _ = conn.execute(
            "ALTER TABLE token_snapshots ADD COLUMN sniper_pct REAL NOT NULL DEFAULT 0",
            [],
        );
        let _ = conn.execute(
            "ALTER TABLE token_snapshots ADD COLUMN insider_pct REAL NOT NULL DEFAULT 0",
            [],
        );
        let _ = conn.execute(
            "ALTER TABLE token_snapshots ADD COLUMN forensics_computed_at INTEGER NOT NULL DEFAULT 0",
            [],
        );
        // Smart-money proxy: count of top-20 holders that also hold an
        // operator-curated reference mint. Computed alongside the forensics
        // triple, free (no external API).
        let _ = conn.execute(
            "ALTER TABLE token_snapshots ADD COLUMN smart_money_count INTEGER NOT NULL DEFAULT 0",
            [],
        );

        // One-shot purge: pumpportal/new_token + discovery/new_token
        // audit entries dominated audit_log (~127k of 130k rows). The
        // hot-path code that wrote them was removed 2026-04-30; this
        // migration cleans the historical noise. tokens table remains
        // the canonical record of new mints; audit_log is reserved for
        // state changes and human/MCP actions.
        let _ = conn.execute(
            "DELETE FROM audit_log WHERE action = 'new_token' AND actor IN ('pumpportal','discovery')",
            [],
        );

        // Curve_snapshots GC: keep only the latest snapshot per mint plus
        // the most recent 24h. Curves graduate or die within minutes;
        // anything older than 24h is reference data we don't need to
        // re-query. observe_bonding_curves only polls non-graduated mints
        // so write-rate is bounded going forward, but the backlog from
        // before this GC was 143k rows.
        let _ = conn.execute(
            "DELETE FROM curve_snapshots WHERE timestamp < strftime('%s','now') - 86400",
            [],
        );
        // token_snapshots GC: 7d retention. LONG-auto now timeouts at 6h,
        // not 30d, so the old retention assumption is stale. 7d still
        // covers operator-held positions beyond the auto window and is
        // plenty for short-window calibration backtests.
        let _ = conn.execute(
            "DELETE FROM token_snapshots WHERE timestamp < strftime('%s','now') - 7*86400",
            [],
        );

        // sniper_cohort GC: only keep cohorts for mints that still have
        // recent snapshots OR an active/recent call. 356k rows of
        // dust-cohort data on retired pump.fun mints is pure storage
        // bloat — those tokens will never be re-analyzed.
        let _ = conn.execute(
            "DELETE FROM sniper_cohort WHERE mint_address NOT IN (
                 SELECT DISTINCT token_address FROM token_snapshots
                 UNION SELECT mint FROM calls
                       WHERE called_at >= strftime('%s','now') - 30*86400
             )",
            [],
        );

        // Clear forensics-timeout sentinels (100/100/100) so the soft
        // gate doesn't permanently block those tokens. The buggy write
        // happened during the 2026-04-30 RPC-degradation hours; now
        // that the sentinel write has been removed, clearing the
        // existing rows lets a future compute succeed and overwrite
        // with real values.
        let _ = conn.execute(
            "UPDATE token_snapshots
             SET bundle_pct=0, sniper_pct=0, insider_pct=0, forensics_computed_at=0
             WHERE bundle_pct=100 AND sniper_pct=100 AND insider_pct=100",
            [],
        );

        // Sniper cohort — the wallets that bought in the first window of
        // a token's life. Captured once per token at discovery.
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS sniper_cohort (
                mint_address TEXT NOT NULL,
                owner_address TEXT NOT NULL,
                first_buy_ts INTEGER NOT NULL,
                PRIMARY KEY (mint_address, owner_address)
            );
            -- Wallet observations — early buyers captured per promoted mint.
            -- Layer 1 of the self-evolving smart-wallet curation system.
            -- Populated by wallet_observer on first promotion of a mint
            -- (mint passes basic classifier + age + holder thresholds).
            -- One row per (wallet, mint) — UNIQUE prevents duplicate inserts
            -- if the buyer-trace runs twice. Layer 2 (outcomes) joins this
            -- against token_snapshots to compute realized PnL per wallet.
            CREATE TABLE IF NOT EXISTS wallet_observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wallet TEXT NOT NULL,
                mint TEXT NOT NULL,
                rank INTEGER NOT NULL,
                buy_ts INTEGER NOT NULL,
                buy_sol REAL NOT NULL DEFAULT 0,
                observed_at INTEGER NOT NULL,
                UNIQUE(wallet, mint)
            );
            CREATE INDEX IF NOT EXISTS idx_wallet_obs_wallet ON wallet_observations(wallet);
            CREATE INDEX IF NOT EXISTS idx_wallet_obs_mint ON wallet_observations(mint);
            -- Lounge anchor state — singleton row tracking the current
            -- copy of the always-at-bottom message in the lounge.
            -- After every photon-originated lounge send, bump_lounge_anchor
            -- deletes the previous copy and re-copies the source. Row 1
            -- holds the most recent copy message_id so deletes survive
            -- a process restart. INSERT OR IGNORE seeds the singleton.
            CREATE TABLE IF NOT EXISTS lounge_anchor_state (
                id INTEGER PRIMARY KEY CHECK(id = 1),
                current_msg_id INTEGER NOT NULL DEFAULT 0,
                last_bump_at INTEGER NOT NULL DEFAULT 0
            );
            INSERT OR IGNORE INTO lounge_anchor_state (id, current_msg_id, last_bump_at) VALUES (1, 0, 0);
            -- Idempotency: tracks which mints we've already traced so the
            -- promotion trigger fires the buyer-walk exactly once.
            CREATE TABLE IF NOT EXISTS wallet_observation_traces (
                mint TEXT PRIMARY KEY,
                traced_at INTEGER NOT NULL,
                buyer_count INTEGER NOT NULL DEFAULT 0,
                success INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS reference_mints (
                mint_address TEXT PRIMARY KEY,
                label TEXT NOT NULL DEFAULT '',
                added_at INTEGER NOT NULL
            );
            -- DexScreener paid-boost observations. The `amount` column is
            -- the boost level the project paid for (10/50/100/500 etc.);
            -- `source` is which feed it came from. Used as an early signal:
            -- a token actively paying for promotion is a strong intent
            -- marker even before classification + tape gates qualify.
            CREATE TABLE IF NOT EXISTS token_boosts (
                token_address TEXT NOT NULL,
                amount INTEGER NOT NULL,
                source TEXT NOT NULL,
                observed_at INTEGER NOT NULL,
                PRIMARY KEY (token_address, observed_at)
            );
            CREATE INDEX IF NOT EXISTS idx_token_boosts_addr_time
                ON token_boosts (token_address, observed_at DESC);
            -- Wallet ledger: every detected swap on the operating wallet,
            -- keyed by tx signature for idempotent replays. side = 'buy'
            -- when we gained the token, 'sell' when we exited.
            CREATE TABLE IF NOT EXISTS wallet_ledger (
                signature TEXT PRIMARY KEY,
                wallet TEXT NOT NULL,
                mint TEXT NOT NULL,
                side TEXT NOT NULL,
                token_amount_ui REAL NOT NULL,
                sol_amount_ui REAL NOT NULL,
                price_usd_at_trade REAL NOT NULL DEFAULT 0,
                mcap_usd_at_trade REAL NOT NULL DEFAULT 0,
                ts INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_wallet_ledger_wallet ON wallet_ledger(wallet);
            CREATE INDEX IF NOT EXISTS idx_wallet_ledger_mint ON wallet_ledger(mint);
            CREATE INDEX IF NOT EXISTS idx_wallet_ledger_ts ON wallet_ledger(ts);

            -- Public calls: once we commit to a token in the Calls channel,
            -- the entry state gets frozen here. Mirrors to the site as an
            -- active commitment the ape cannot quietly edit out.
            CREATE TABLE IF NOT EXISTS calls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                mint TEXT NOT NULL,
                symbol TEXT NOT NULL DEFAULT '',
                classification TEXT NOT NULL DEFAULT '',
                confidence INTEGER NOT NULL DEFAULT 0,
                called_at INTEGER NOT NULL,
                entry_mcap_usd REAL NOT NULL DEFAULT 0,
                entry_price_usd REAL NOT NULL DEFAULT 0,
                entry_liquidity_usd REAL NOT NULL DEFAULT 0,
                entry_top_holder_pct REAL NOT NULL DEFAULT 0,
                entry_pair_dex TEXT NOT NULL DEFAULT '',
                note TEXT NOT NULL DEFAULT '',
                source TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'active',
                closed_at INTEGER,
                exit_price_usd REAL,
                exit_note TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_calls_status ON calls(status);
            CREATE INDEX IF NOT EXISTS idx_calls_called_at ON calls(called_at);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_calls_mint_active
                ON calls(mint) WHERE status = 'active';
            ",
        )?;
        // Time-based expiration — calls that haven't been closed or moved on
        // after N days auto-expire. Keeps the book free of zombie "active"
        // rows that no one remembers. Default 14 days; 0 = no expiration.
        let _ = conn.execute("ALTER TABLE calls ADD COLUMN expires_at INTEGER", []);
        // Entry tx_rate (transactions/min at call-fire). Lets the
        // settling phase fire a volume-collapse close: SHORT calls
        // whose tx_rate has fallen ≤10% of entry across two snapshots
        // are silently dying — no need to wait for the price to drop
        // 40% or 6h to elapse. Migration; pre-existing rows get 0
        // which disables the rule for them.
        let _ = conn.execute(
            "ALTER TABLE calls ADD COLUMN entry_tx_rate REAL NOT NULL DEFAULT 0",
            [],
        );

        // Trade-execution columns. NULL = not yet executed (or paper-only
        // call). When execution.enabled is on, notifier spawns a buy on
        // call-fire and writes the buy_* columns on confirmation; settle
        // spawns a sell when the outcome is decided and writes sell_*
        // before the row's status flips to withdrew/failed/expired. The
        // call lifecycle stays paper-compatible — these columns are
        // additive metadata. Pre-existing rows get NULL.
        let _ = conn.execute("ALTER TABLE calls ADD COLUMN buy_signature TEXT", []);
        let _ = conn.execute("ALTER TABLE calls ADD COLUMN buy_sol_spent REAL", []);
        let _ = conn.execute("ALTER TABLE calls ADD COLUMN buy_token_received REAL", []);
        let _ = conn.execute("ALTER TABLE calls ADD COLUMN buy_attempted_at INTEGER", []);
        let _ = conn.execute("ALTER TABLE calls ADD COLUMN buy_failed_reason TEXT", []);
        let _ = conn.execute("ALTER TABLE calls ADD COLUMN sell_signature TEXT", []);
        let _ = conn.execute("ALTER TABLE calls ADD COLUMN sell_token_sold REAL", []);
        let _ = conn.execute("ALTER TABLE calls ADD COLUMN sell_sol_received REAL", []);
        let _ = conn.execute("ALTER TABLE calls ADD COLUMN sell_attempt_count INTEGER NOT NULL DEFAULT 0", []);
        let _ = conn.execute("ALTER TABLE calls ADD COLUMN sell_last_error TEXT", []);

        // Bonding-curve observation snapshots. Pre-graduation pump.fun
        // tokens have no DexScreener pair, so token_snapshots can't hold
        // their state. We track them here from chain reads of the curve
        // PDA. Each row is a 60s-cadence sample of one curve. After the
        // token graduates the row stream stops; downstream gates fall
        // back to the existing token_snapshots feed.
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS curve_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                mint TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                virtual_sol REAL NOT NULL,
                virtual_tok REAL NOT NULL,
                real_sol REAL NOT NULL,
                real_tok REAL NOT NULL,
                price_sol REAL NOT NULL,
                fill_pct REAL NOT NULL,
                complete INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_curve_mint_ts
                ON curve_snapshots(mint, timestamp DESC);
            ",
        )?;

        Ok(())
    }

    // -- Periodic GC ---------------------------------------------------------

    /// Periodic retention pass — same DELETE policies as the init-time GC,
    /// but runnable on a timer so the DB doesn't grow unbounded between
    /// container restarts. Returns total rows deleted across all tables.
    pub fn run_periodic_gc(&self) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let mut deleted = 0usize;
        deleted += conn.execute(
            "DELETE FROM curve_snapshots WHERE timestamp < strftime('%s','now') - 86400",
            [],
        )?;
        // 7d retention (was 30d). Old comment claimed 30d "matches LONG
        // horizon timeout"; LONG auto is now 6h and the longest operator
        // hold rarely needs more than a week of pre-history. Steady-state
        // size drops ~80% at the cohort write rate.
        deleted += conn.execute(
            "DELETE FROM token_snapshots WHERE timestamp < strftime('%s','now') - 7*86400",
            [],
        )?;
        deleted += conn.execute(
            "DELETE FROM sniper_cohort WHERE mint_address NOT IN (
                 SELECT DISTINCT token_address FROM token_snapshots
                 UNION SELECT mint FROM calls
                       WHERE called_at >= strftime('%s','now') - 30*86400
             )",
            [],
        )?;
        deleted += conn.execute(
            "DELETE FROM alerts WHERE timestamp < strftime('%s','now') - 7*86400",
            [],
        )?;
        // signal_near_misses: 7d retention. Each near-miss is a snapshot
        // of why a token failed the gate at one moment in time — useful
        // for short-window calibration, dust beyond a week.
        deleted += conn.execute(
            "DELETE FROM signal_near_misses WHERE timestamp < strftime('%s','now') - 7*86400",
            [],
        )?;
        // tokens: 30d retention with active-call protection. The discovery
        // queue accretes thousands of fresh-grad mints per day, most of
        // which never become calls. Without this, the table grows linearly
        // forever (169k rows / 22d at present). 30d window keeps recent
        // discovery surface; anything older that wasn't called is dead
        // inventory. Active calls + their snapshots lock referenced mints
        // so we never prune anything still in motion.
        deleted += conn.execute(
            "DELETE FROM tokens WHERE first_seen < strftime('%s','now') - 30*86400
             AND address NOT IN (SELECT mint FROM calls WHERE status='active')
             AND address NOT IN (SELECT DISTINCT token_address FROM token_snapshots)",
            [],
        )?;
        Ok(deleted)
    }

    // -- Signal near-misses --------------------------------------------------

    pub fn insert_near_miss(
        &self,
        token_address: &str,
        classification: &str,
        effective_confidence: i32,
        top_holder_pct: f64,
        momentum_delta: Option<i32>,
        gate_that_failed: &str,
        gap: &str,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO signal_near_misses
             (token_address, classification, effective_confidence, top_holder_pct,
              momentum_delta, gate_that_failed, gap, timestamp)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, strftime('%s','now'))",
            params![
                token_address,
                classification,
                effective_confidence,
                top_holder_pct,
                momentum_delta,
                gate_that_failed,
                gap
            ],
        )?;
        Ok(())
    }

    /// Latest snapshot per mint matching watching criteria, used by the
    /// publisher's stream "watching" tier. Pulls tokens currently
    /// classified at conf >= conf_min in classes that fire calls
    /// (STAIRCASE/GRINDER/SPRING/DEVELOPING) within `since_ts`. The
    /// near-miss table only logs tokens that ALMOST fired on a single
    /// gate; this surfaces the broader set the bot is actively analyzing.
    pub fn get_active_watching_candidates(
        &self,
        since_ts: i64,
        conf_min: i32,
        limit: usize,
    ) -> Result<Vec<(String, String, i32, f64, f64, i64)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT s.token_address, s.classification, s.confidence,
                    s.top_holder_pct, s.top10_pct, s.timestamp
             FROM token_snapshots s
             INNER JOIN (
               SELECT token_address, MAX(timestamp) AS max_ts
               FROM token_snapshots
               WHERE timestamp >= ?1 AND confidence >= ?2
                 AND classification IN ('STAIRCASE','GRINDER','SPRING','DEVELOPING')
               GROUP BY token_address
             ) latest
               ON latest.token_address = s.token_address
              AND latest.max_ts = s.timestamp
             ORDER BY s.confidence DESC, s.timestamp DESC
             LIMIT ?3",
        )?;
        let rows = stmt
            .query_map(params![since_ts, conf_min, limit], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, i32>(2)?,
                    r.get::<_, f64>(3)?,
                    r.get::<_, f64>(4)?,
                    r.get::<_, i64>(5)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Compute a deployer's track record from prior tokens. Returns
    /// `(prior_count, runner_count, rug_count)`. A "runner" is any prior
    /// token where token_snapshots ever recorded a price >=2x first
    /// seen; "rug" is a token whose price collapsed >=80% from peak
    /// within the snapshot window. Used by gates to relax for proven
    /// deployers and harden against repeat ruggers.
    pub fn deployer_track_record(&self, deployer: &str, exclude_mint: &str) -> Result<(i64, i64, i64)> {
        if deployer.is_empty() { return Ok((0, 0, 0)); }
        let conn = self.conn.lock().unwrap();
        let prior_mints: Vec<String> = conn
            .prepare("SELECT address FROM tokens WHERE deployer_address = ?1 AND address != ?2")?
            .query_map(params![deployer, exclude_mint], |r| r.get::<_, String>(0))?
            .filter_map(|r| r.ok())
            .collect();
        if prior_mints.is_empty() { return Ok((0, 0, 0)); }
        let mut runners = 0i64;
        let mut rugs = 0i64;
        for mint in &prior_mints {
            let row: Option<(f64, f64, f64)> = conn
                .query_row(
                    "SELECT MIN(price_usd), MAX(price_usd), price_usd FROM token_snapshots
                     WHERE token_address = ?1 AND price_usd > 0",
                    params![mint],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()?;
            if let Some((lo, hi, _)) = row {
                if lo > 0.0 {
                    let peak_pct = (hi / lo - 1.0) * 100.0;
                    if peak_pct >= 100.0 { runners += 1; }
                    // Rug: latest price <= 20% of peak
                    let latest: Option<f64> = conn
                        .query_row(
                            "SELECT price_usd FROM token_snapshots
                             WHERE token_address = ?1 AND price_usd > 0
                             ORDER BY timestamp DESC LIMIT 1",
                            params![mint],
                            |r| r.get(0),
                        )
                        .optional()?;
                    if let Some(latest_px) = latest {
                        if hi > 0.0 && latest_px <= hi * 0.2 { rugs += 1; }
                    }
                }
            }
        }
        Ok((prior_mints.len() as i64, runners, rugs))
    }

    /// Record a DexScreener boost observation for a token. Called from
    /// the discovery poller when an item from boost feeds carries an
    /// `amount` field. Idempotent on (token, observed_at) — repeated
    /// polls of the same boost within the same second collapse.
    pub fn record_token_boost(
        &self,
        token_address: &str,
        amount: i64,
        source: &str,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = chrono::Utc::now().timestamp();
        conn.execute(
            "INSERT OR IGNORE INTO token_boosts (token_address, amount, source, observed_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![token_address, amount, source, now],
        )?;
        Ok(())
    }

    /// Latest boost amount observed for a token within `lookback_secs`,
    /// or 0 if none. Used by signal gates as an early-conviction marker.
    pub fn latest_boost_within(
        &self,
        token_address: &str,
        lookback_secs: i64,
    ) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let cutoff = chrono::Utc::now().timestamp() - lookback_secs;
        let amount: Option<i64> = conn
            .query_row(
                "SELECT MAX(amount) FROM token_boosts
                 WHERE token_address = ?1 AND observed_at >= ?2",
                params![token_address, cutoff],
                |r| r.get(0),
            )
            .optional()?;
        Ok(amount.unwrap_or(0))
    }

    /// Trail-stop fakeout guard. NYAN (May 2): called at $60k mcap, took
    /// a single 80-second wick to -28% (triggered moonshot stop at -25),
    /// closed; then immediately recovered and ran to +147%. Mitigation:
    /// before firing any stop-out, require that AT LEAST ONE prior
    /// snapshot in the last 90s also priced below the floor — a single
    /// live-fetch wick alone isn't proof of a real breach.
    ///
    /// Returns the count of snapshots in (now - 120s, now - 15s] whose
    /// price is at or below `price_threshold`. Caller treats `>= 1` as
    /// "confirmed breach", `== 0` as "wick — wait one more cycle".
    pub fn count_recent_snapshots_below(
        &self,
        token_address: &str,
        price_threshold: f64,
    ) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let now = chrono::Utc::now().timestamp();
        let lo = now - 120;
        let hi = now - 15;
        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM token_snapshots
             WHERE token_address = ?1
               AND timestamp BETWEEN ?2 AND ?3
               AND price_usd > 0
               AND price_usd <= ?4",
            params![token_address, lo, hi, price_threshold],
            |r| r.get(0),
        )?;
        Ok(n)
    }

    pub fn get_recent_near_misses(&self, limit: usize) -> Result<Vec<NearMiss>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT token_address, classification, effective_confidence, top_holder_pct,
                    momentum_delta, gate_that_failed, gap, timestamp
             FROM signal_near_misses
             ORDER BY timestamp DESC LIMIT ?1",
        )?;
        let rows: Vec<NearMiss> = stmt
            .query_map(params![limit], |r| {
                Ok(NearMiss {
                    token_address: r.get(0)?,
                    classification: r.get(1)?,
                    effective_confidence: r.get(2)?,
                    top_holder_pct: r.get(3)?,
                    momentum_delta: r.get(4)?,
                    gate_that_failed: r.get(5)?,
                    gap: r.get(6)?,
                    timestamp: r.get(7)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    // -- DM bot: user state --------------------------------------------------

    /// Upsert a user on interaction. Returns true if newly registered.
    pub fn touch_user(
        &self,
        telegram_user_id: i64,
        username: Option<&str>,
        first_name: Option<&str>,
    ) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let now = chrono::Utc::now().timestamp();
        let existed: Option<i64> = conn
            .query_row(
                "SELECT telegram_user_id FROM users WHERE telegram_user_id = ?1",
                params![telegram_user_id],
                |r| r.get(0),
            )
            .optional()?;
        if existed.is_some() {
            conn.execute(
                "UPDATE users SET last_seen = ?1, message_count = message_count + 1,
                                  username = COALESCE(?2, username),
                                  first_name = COALESCE(?3, first_name)
                 WHERE telegram_user_id = ?4",
                params![now, username, first_name, telegram_user_id],
            )?;
            Ok(false)
        } else {
            conn.execute(
                "INSERT INTO users (telegram_user_id, username, first_name, first_seen, last_seen, message_count)
                 VALUES (?1, ?2, ?3, ?4, ?4, 1)",
                params![telegram_user_id, username, first_name, now],
            )?;
            Ok(true)
        }
    }

    pub fn is_admin(&self, telegram_user_id: i64) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let v: i64 = conn
            .query_row(
                "SELECT is_admin FROM users WHERE telegram_user_id = ?1",
                params![telegram_user_id],
                |r| r.get(0),
            )
            .unwrap_or(0);
        Ok(v != 0)
    }

    pub fn set_admin(&self, telegram_user_id: i64, is_admin: bool) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO users (telegram_user_id, first_seen, last_seen, message_count, is_admin)
             VALUES (?1, strftime('%s','now'), strftime('%s','now'), 0, ?2)
             ON CONFLICT(telegram_user_id) DO UPDATE SET is_admin = excluded.is_admin",
            params![telegram_user_id, if is_admin { 1 } else { 0 }],
        )?;
        Ok(())
    }

    pub fn add_user_watch(&self, user: i64, token: &str, note: Option<&str>) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO user_watchlist (telegram_user_id, token_address, added_at, note)
             VALUES (?1, ?2, strftime('%s','now'), ?3)
             ON CONFLICT DO UPDATE SET note = excluded.note",
            params![user, token, note],
        )?;
        Ok(())
    }

    pub fn remove_user_watch(&self, user: i64, token: &str) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let n = conn.execute(
            "DELETE FROM user_watchlist WHERE telegram_user_id = ?1 AND token_address = ?2",
            params![user, token],
        )?;
        Ok(n > 0)
    }

    pub fn get_user_watches(&self, user: i64) -> Result<Vec<(String, Option<String>, i64)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT token_address, note, added_at FROM user_watchlist
             WHERE telegram_user_id = ?1 ORDER BY added_at DESC",
        )?;
        let rows: Vec<(String, Option<String>, i64)> = stmt
            .query_map(params![user], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn add_user_mute(&self, user: i64, token: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO user_mutes (telegram_user_id, token_address, muted_at)
             VALUES (?1, ?2, strftime('%s','now'))",
            params![user, token],
        )?;
        Ok(())
    }

    pub fn remove_user_mute(&self, user: i64, token: &str) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let n = conn.execute(
            "DELETE FROM user_mutes WHERE telegram_user_id = ?1 AND token_address = ?2",
            params![user, token],
        )?;
        Ok(n > 0)
    }

    pub fn get_user_mutes(&self, user: i64) -> Result<Vec<(String, i64)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT token_address, muted_at FROM user_mutes
             WHERE telegram_user_id = ?1 ORDER BY muted_at DESC",
        )?;
        let rows: Vec<(String, i64)> = stmt
            .query_map(params![user], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn log_command(
        &self,
        user: i64,
        command: &str,
        args: Option<&str>,
        duration_ms: i64,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO command_log (telegram_user_id, command, args, timestamp, duration_ms)
             VALUES (?1, ?2, ?3, strftime('%s','now'), ?4)",
            params![user, command, args, duration_ms],
        )?;
        Ok(())
    }

    pub fn total_users(&self) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let n: i64 = conn.query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0))?;
        Ok(n)
    }

    pub fn total_commands_since(&self, since_ts: i64) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM command_log WHERE timestamp >= ?1",
            params![since_ts],
            |r| r.get(0),
        )?;
        Ok(n)
    }

    pub fn command_count_since(&self, user: i64, since_ts: i64) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM command_log WHERE telegram_user_id = ?1 AND timestamp >= ?2",
            params![user, since_ts],
            |r| r.get(0),
        )?;
        Ok(n)
    }

    /// Mark a digest as finalized — no further edits/wrap-ups should apply.
    pub fn finalize_digest(&self, id: i64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE telegram_digests SET finalized = 1 WHERE id = ?1",
            params![id],
        )?;
        Ok(())
    }

    /// Any digest whose hour has ended but hasn't been finalized yet.
    /// Used to drive the end-of-hour trap wrap-up.
    pub fn get_unfinalized_digest_before(
        &self,
        current_hour_bucket: i64,
    ) -> Result<Option<TelegramDigest>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, hour_bucket, channel, message_id, created_at, last_edit_at, edit_count
             FROM telegram_digests
             WHERE hour_bucket < ?1 AND finalized = 0
             ORDER BY hour_bucket DESC LIMIT 1",
        )?;
        let mut rows = stmt.query(params![current_hour_bucket])?;
        if let Some(row) = rows.next()? {
            Ok(Some(TelegramDigest {
                id: row.get(0)?,
                hour_bucket: row.get(1)?,
                channel: row.get(2)?,
                message_id: row.get(3)?,
                created_at: row.get(4)?,
                last_edit_at: row.get(5)?,
                edit_count: row.get(6)?,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn list_tables(&self) -> Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
        )?;
        let tables = stmt
            .query_map([], |row| row.get(0))?
            .collect::<Result<Vec<String>, _>>()?;
        Ok(tables)
    }

    pub fn insert_token(&self, address: &str, safety_score: i32) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = chrono::Utc::now().timestamp();
        conn.execute(
            "INSERT OR REPLACE INTO tokens (address, first_seen, safety_score) VALUES (?1, ?2, ?3)",
            params![address, now, safety_score],
        )?;
        Ok(())
    }

    pub fn get_token(&self, address: &str) -> Result<Option<Token>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT address, first_seen, safety_score FROM tokens WHERE address = ?1")?;
        let token = stmt
            .query_row(params![address], |row| {
                Ok(Token {
                    address: row.get(0)?,
                    first_seen: row.get(1)?,
                    safety_score: row.get(2)?,
                })
            })
            .optional()?;
        Ok(token)
    }

    /// Look up the deployer address + initial balance recorded for a
    /// mint. Returns None when no deployer was captured (older rows
    /// from before the pumpportal trader_public_key fix).
    pub fn get_token_deployer(&self, address: &str) -> Result<Option<(String, f64)>> {
        let conn = self.conn.lock().unwrap();
        let row: Option<(String, f64)> = conn
            .query_row(
                "SELECT deployer_address, deployer_initial_balance FROM tokens WHERE address = ?1",
                params![address],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, f64>(1)?)),
            )
            .optional()?;
        match row {
            Some((d, b)) if !d.is_empty() => Ok(Some((d, b))),
            _ => Ok(None),
        }
    }

    /// Record the deployer wallet for a token the first time we see it.
    /// Idempotent — only sets when the columns are still empty, so the
    /// "initial" balance locks at discovery time and can be compared later
    /// to detect dev selling.
    pub fn set_deployer_if_empty(
        &self,
        address: &str,
        deployer: &str,
        initial_balance: f64,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE tokens SET deployer_address = ?1, deployer_initial_balance = ?2
             WHERE address = ?3 AND (deployer_address = '' OR deployer_initial_balance = 0)",
            params![deployer, initial_balance, address],
        )?;
        Ok(())
    }

    /// Returns (deployer_address, deployer_initial_balance) for a token, or None
    /// if we haven't recorded one yet.
    pub fn get_deployer(&self, address: &str) -> Result<Option<(String, f64)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT deployer_address, deployer_initial_balance FROM tokens WHERE address = ?1",
        )?;
        let row = stmt
            .query_row(params![address], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, f64>(1)?))
            })
            .optional()?;
        Ok(row)
    }

    // -- Smart-wallet tracking (imitation-alpha) -----------------------------

    pub fn add_smart_wallet(&self, address: &str, label: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = chrono::Utc::now().timestamp();
        conn.execute(
            "INSERT OR REPLACE INTO smart_wallets (address, label, added_at, active)
             VALUES (?1, ?2, ?3, 1)",
            params![address, label, now],
        )?;
        Ok(())
    }

    pub fn remove_smart_wallet(&self, address: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE smart_wallets SET active = 0 WHERE address = ?1",
            params![address],
        )?;
        Ok(())
    }

    /// Returns (address, label) pairs for every active smart wallet.
    pub fn list_active_smart_wallets(&self) -> Result<Vec<(String, String)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT address, label FROM smart_wallets WHERE active = 1")?;
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Every mint currently known to be held by the given wallet, with its
    /// last-observed ui balance.
    pub fn get_smart_wallet_mints(&self, wallet: &str) -> Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT mint_address FROM smart_wallet_holdings WHERE wallet_address = ?1")?;
        let rows = stmt
            .query_map(params![wallet], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Record or update a wallet's holding of a mint. `first_seen` only gets
    /// set on initial insert so we can tell what's new vs what's held.
    pub fn upsert_smart_wallet_holding(
        &self,
        wallet: &str,
        mint: &str,
        balance_ui: f64,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = chrono::Utc::now().timestamp();
        conn.execute(
            "INSERT INTO smart_wallet_holdings (wallet_address, mint_address, balance_ui,
                                                first_seen, last_seen)
             VALUES (?1, ?2, ?3, ?4, ?4)
             ON CONFLICT(wallet_address, mint_address) DO UPDATE SET
                 balance_ui = excluded.balance_ui,
                 last_seen = excluded.last_seen",
            params![wallet, mint, balance_ui, now],
        )?;
        Ok(())
    }

    pub fn touch_smart_wallet(&self, address: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = chrono::Utc::now().timestamp();
        conn.execute(
            "UPDATE smart_wallets SET last_checked = ?1 WHERE address = ?2",
            params![now, address],
        )?;
        Ok(())
    }

    // -- Deep-scout DB methods ----------------------------------------------

    /// All mint addresses that were recorded with this deployer, newest first.
    /// Returns (mint, first_seen_ts).
    pub fn get_tokens_by_deployer(&self, deployer: &str) -> Result<Vec<(String, i64)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT address, first_seen FROM tokens
             WHERE deployer_address = ?1 ORDER BY first_seen DESC",
        )?;
        let rows = stmt
            .query_map(params![deployer], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Returns addresses from the tokens table that were discovered within
    /// [age_min_secs, age_max_secs] ago, have no active watchlist entry, and
    /// haven't had a snapshot in the last `recent_snapshot_cutoff` seconds.
    /// Used by Phase 4 re-ingest to catch tokens that were too young/concentrated
    /// at first sight but may have since matured.
    /// Mints to poll for bonding-curve state. Pump.fun tokens whose
    /// first_seen is within `max_age_secs`, that haven't been recorded
    /// as `complete` yet (`real_sol < graduation_target` AND no row
    /// with complete=1). Cap at `limit`.
    pub fn list_curve_observation_candidates(
        &self,
        max_age_secs: i64,
        limit: usize,
    ) -> Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let now = chrono::Utc::now().timestamp();
        let oldest = now - max_age_secs;
        let mut stmt = conn.prepare(
            "SELECT t.address FROM tokens t
             WHERE t.first_seen >= ?1
               AND NOT EXISTS (
                 SELECT 1 FROM curve_snapshots c
                 WHERE c.mint = t.address AND c.complete = 1
               )
             ORDER BY t.first_seen DESC
             LIMIT ?2",
        )?;
        let rows = stmt
            .query_map(params![oldest, limit], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Persist one bonding-curve snapshot.
    pub fn insert_curve_snapshot(
        &self,
        mint: &str,
        ts: i64,
        virtual_sol: f64,
        virtual_tok: f64,
        real_sol: f64,
        real_tok: f64,
        price_sol: f64,
        fill_pct: f64,
        complete: bool,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO curve_snapshots
             (mint, timestamp, virtual_sol, virtual_tok, real_sol, real_tok,
              price_sol, fill_pct, complete)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                mint, ts, virtual_sol, virtual_tok, real_sol, real_tok,
                price_sol, fill_pct, if complete { 1 } else { 0 },
            ],
        )?;
        Ok(())
    }

    pub fn list_stale_discovered_candidates(
        &self,
        age_min_secs: i64,
        age_max_secs: i64,
        recent_snapshot_cutoff: i64,
        limit: usize,
    ) -> Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let now = chrono::Utc::now().timestamp();
        let oldest = now - age_max_secs;
        let youngest = now - age_min_secs;
        let snap_cutoff = now - recent_snapshot_cutoff;
        let mut stmt = conn.prepare(
            "SELECT t.address FROM tokens t
             WHERE t.first_seen BETWEEN ?1 AND ?2
               AND NOT EXISTS (
                 SELECT 1 FROM watchlist w WHERE w.token_address = t.address AND w.active = 1
               )
               AND NOT EXISTS (
                 SELECT 1 FROM token_snapshots s
                 WHERE s.token_address = t.address AND s.timestamp > ?3
               )
             ORDER BY COALESCE(
               (SELECT MAX(s2.timestamp) FROM token_snapshots s2 WHERE s2.token_address = t.address),
               t.first_seen
             ) ASC
             LIMIT ?4",
        )?;
        let rows = stmt
            .query_map(params![oldest, youngest, snap_cutoff, limit as i64], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Most recent (timestamp, top_holders_json) for a mint, or None if we
    /// haven't populated the column yet for any of its snapshots.
    pub fn get_latest_top_holders(&self, mint: &str) -> Result<Option<(i64, String)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT timestamp, top_holders_json FROM token_snapshots
             WHERE token_address = ?1 AND top_holders_json != '[]'
             ORDER BY timestamp DESC LIMIT 1",
        )?;
        let row = stmt
            .query_row(params![mint], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .optional()?;
        Ok(row)
    }

    /// Oldest snapshot at or before the given cutoff — used by
    /// holder_evolution to pick the "N hours ago" baseline.
    pub fn get_top_holders_at_or_before(
        &self,
        mint: &str,
        cutoff_ts: i64,
    ) -> Result<Option<(i64, String)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT timestamp, top_holders_json FROM token_snapshots
             WHERE token_address = ?1 AND top_holders_json != '[]' AND timestamp <= ?2
             ORDER BY timestamp DESC LIMIT 1",
        )?;
        let row = stmt
            .query_row(params![mint, cutoff_ts], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .optional()?;
        Ok(row)
    }

    /// Record a sniper cohort batch. `INSERT OR IGNORE` so we can call this
    /// repeatedly on rescans without clobbering the initial record.
    pub fn insert_snipers(&self, mint: &str, owners: &[String]) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = chrono::Utc::now().timestamp();
        for owner in owners {
            conn.execute(
                "INSERT OR IGNORE INTO sniper_cohort (mint_address, owner_address, first_buy_ts)
                 VALUES (?1, ?2, ?3)",
                params![mint, owner, now],
            )?;
        }
        Ok(())
    }

    pub fn get_sniper_cohort(&self, mint: &str) -> Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt =
            conn.prepare("SELECT owner_address FROM sniper_cohort WHERE mint_address = ?1")?;
        let rows = stmt
            .query_map(params![mint], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn has_sniper_cohort(&self, mint: &str) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM sniper_cohort WHERE mint_address = ?1",
            params![mint],
            |row| row.get(0),
        )?;
        Ok(n > 0)
    }

    pub fn add_reference_mint(&self, mint: &str, label: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = chrono::Utc::now().timestamp();
        conn.execute(
            "INSERT OR REPLACE INTO reference_mints (mint_address, label, added_at)
             VALUES (?1, ?2, ?3)",
            params![mint, label, now],
        )?;
        Ok(())
    }

    pub fn remove_reference_mint(&self, mint: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM reference_mints WHERE mint_address = ?1",
            params![mint],
        )?;
        Ok(())
    }

    pub fn list_reference_mints(&self) -> Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT mint_address FROM reference_mints")?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn list_reference_mints_with_label(&self) -> Result<Vec<(String, String)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT mint_address, label FROM reference_mints")?;
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    // -- Wallet ledger (operating wallet trade history) --------------------

    /// Record a swap on the operating wallet. Idempotent by tx signature —
    /// running the publisher multiple times never double-counts a trade.
    #[allow(clippy::too_many_arguments)]
    pub fn upsert_wallet_trade(
        &self,
        signature: &str,
        wallet: &str,
        mint: &str,
        side: &str,
        token_amount_ui: f64,
        sol_amount_ui: f64,
        price_usd_at_trade: f64,
        mcap_usd_at_trade: f64,
        ts: i64,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO wallet_ledger
             (signature, wallet, mint, side, token_amount_ui, sol_amount_ui,
              price_usd_at_trade, mcap_usd_at_trade, ts)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                signature,
                wallet,
                mint,
                side,
                token_amount_ui,
                sol_amount_ui,
                price_usd_at_trade,
                mcap_usd_at_trade,
                ts,
            ],
        )?;
        Ok(())
    }

    /// Aggregate cost basis for every mint the wallet has ever touched.
    /// Returns Vec<(mint, total_bought_tokens, total_spent_sol, total_sold_tokens, total_received_sol)>.
    /// Publisher uses this to derive WAC and realized/unrealized PnL.
    #[allow(clippy::type_complexity)]
    pub fn get_wallet_cost_basis(&self, wallet: &str) -> Result<Vec<(String, f64, f64, f64, f64)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT mint,
                    SUM(CASE WHEN side='buy' THEN token_amount_ui ELSE 0 END),
                    SUM(CASE WHEN side='buy' THEN sol_amount_ui ELSE 0 END),
                    SUM(CASE WHEN side='sell' THEN token_amount_ui ELSE 0 END),
                    SUM(CASE WHEN side='sell' THEN sol_amount_ui ELSE 0 END)
             FROM wallet_ledger WHERE wallet = ?1 GROUP BY mint",
        )?;
        let rows = stmt
            .query_map(params![wallet], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, f64>(1).unwrap_or(0.0),
                    row.get::<_, f64>(2).unwrap_or(0.0),
                    row.get::<_, f64>(3).unwrap_or(0.0),
                    row.get::<_, f64>(4).unwrap_or(0.0),
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Recent wallet trades for the graph overlay, newest first.
    #[allow(clippy::type_complexity)]
    pub fn get_wallet_trades_recent(
        &self,
        wallet: &str,
        limit: usize,
    ) -> Result<Vec<(i64, String, String, String, f64, f64)>> {
        // (ts, mint, side, signature, token_amount_ui, sol_amount_ui)
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT ts, mint, side, signature, token_amount_ui, sol_amount_ui
             FROM wallet_ledger WHERE wallet = ?1 ORDER BY ts DESC LIMIT ?2",
        )?;
        let rows = stmt
            .query_map(params![wallet, limit], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, f64>(4)?,
                    row.get::<_, f64>(5)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    // -- Public calls (Calls-channel commitments) --------------------------

    /// Register a new public call with the entry state frozen at call-time.
    /// Returns the row id. Idempotent on an active (mint, status='active')
    /// pair via a unique partial index — a second call for the same mint
    /// while the first is still open gets silently ignored.
    #[allow(clippy::too_many_arguments)]
    pub fn insert_call(
        &self,
        mint: &str,
        symbol: &str,
        classification: &str,
        confidence: i32,
        called_at: i64,
        entry_mcap_usd: f64,
        entry_price_usd: f64,
        entry_liquidity_usd: f64,
        entry_top_holder_pct: f64,
        entry_pair_dex: &str,
        note: &str,
        source: &str,
        entry_tx_rate: f64,
    ) -> Result<Option<i64>> {
        let conn = self.conn.lock().unwrap();
        let changed = conn.execute(
            "INSERT OR IGNORE INTO calls
             (mint, symbol, classification, confidence, called_at,
              entry_mcap_usd, entry_price_usd, entry_liquidity_usd,
              entry_top_holder_pct, entry_pair_dex, note, source, status,
              entry_tx_rate)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, 'active', ?13)",
            params![
                mint,
                symbol,
                classification,
                confidence,
                called_at,
                entry_mcap_usd,
                entry_price_usd,
                entry_liquidity_usd,
                entry_top_holder_pct,
                entry_pair_dex,
                note,
                source,
                entry_tx_rate,
            ],
        )?;
        if changed == 0 {
            return Ok(None);
        }
        Ok(Some(conn.last_insert_rowid()))
    }

    /// Close an active call as a deliberate exit — the operator chose to
    /// withdraw, regardless of P&L. Use `fail_call` for scanner-detected
    /// collapses so the two outcomes stay visually distinct.
    pub fn close_call(&self, mint: &str, exit_price_usd: f64, exit_note: &str) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let now = chrono::Utc::now().timestamp();
        let changed = conn.execute(
            "UPDATE calls SET status='withdrew', closed_at=?1,
             exit_price_usd=?2, exit_note=?3
             WHERE mint=?4 AND status='active'",
            params![now, exit_price_usd, exit_note, mint],
        )?;
        Ok(changed > 0)
    }

    /// Auto-close an active call because the token collapsed (rug / UNSAFE /
    /// CRASHING / DEAD). Called by the background scanner so no one has to
    /// manually close a call that already imploded.
    pub fn fail_call(&self, mint: &str, exit_price_usd: f64, exit_note: &str) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let now = chrono::Utc::now().timestamp();
        let changed = conn.execute(
            "UPDATE calls SET status='failed', closed_at=?1,
             exit_price_usd=?2, exit_note=?3
             WHERE mint=?4 AND status='active'",
            params![now, exit_price_usd, exit_note, mint],
        )?;
        Ok(changed > 0)
    }

    /// Administratively void an active call. Use for cleanup paths where the
    /// close is bookkeeping (orphan-restart race, operator retraction), not a
    /// market verdict. Voided calls stay in the DB for forensics but the
    /// publisher excludes them so the public ledger only shows real outcomes.
    pub fn void_call(&self, mint: &str, reason: &str) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let now = chrono::Utc::now().timestamp();
        let changed = conn.execute(
            "UPDATE calls SET status='voided', closed_at=?1, exit_note=?2
             WHERE mint=?3 AND status='active'",
            params![now, reason, mint],
        )?;
        Ok(changed > 0)
    }

    /// Expire a single active call by mint, with a custom exit_note.
    /// Used by the horizon-aware settling phase.
    pub fn expire_call(&self, mint: &str, exit_price_usd: f64, exit_note: &str) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let now = chrono::Utc::now().timestamp();
        let changed = conn.execute(
            "UPDATE calls SET status='expired', closed_at=?1,
             exit_price_usd=?2, exit_note=?3
             WHERE mint=?4 AND status='active'",
            params![now, exit_price_usd, exit_note, mint],
        )?;
        Ok(changed > 0)
    }

    /// Expire any active call whose `expires_at` timestamp has passed.
    /// Records the outcome as `expired`, banks whatever the mark-to-market
    /// price was at the moment of expiry. Returns the number expired.
    pub fn expire_stale_calls(&self, now: i64) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let n = conn.execute(
            "UPDATE calls SET status='expired', closed_at=?1,
             exit_note='expired — no thesis confirmation within the window'
             WHERE status='active' AND expires_at IS NOT NULL AND expires_at <= ?1",
            params![now],
        )?;
        Ok(n)
    }

    /// Set (or clear) the expiration ts on the currently-active call for
    /// a mint. Pass `None` to remove the expiration.
    pub fn set_call_expiration(&self, mint: &str, expires_at: Option<i64>) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE calls SET expires_at=?1 WHERE mint=?2 AND status='active'",
            params![expires_at, mint],
        )?;
        Ok(())
    }

    /// True when a mint already has an active call — useful for the
    /// notifier + manual `/call` paths to short-circuit duplicate posts.
    pub fn has_active_call(&self, mint: &str) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM calls WHERE mint=?1 AND status='active'",
            params![mint],
            |r| r.get(0),
        )?;
        Ok(n > 0)
    }

    #[allow(clippy::type_complexity)]
    pub fn list_calls(&self, only_active: bool, limit: usize) -> Result<Vec<CallRow>> {
        // `closed` is the legacy value written before the withdrew/failed split —
        // treat it identically to `withdrew` everywhere in display logic.
        let sql = if only_active {
            "SELECT id, mint, symbol, classification, confidence, called_at,
                    entry_mcap_usd, entry_price_usd, entry_liquidity_usd,
                    entry_top_holder_pct, entry_pair_dex, note, source,
                    status, closed_at, exit_price_usd, exit_note, expires_at,
                    entry_tx_rate
             FROM calls WHERE status='active' ORDER BY called_at DESC LIMIT ?1"
        } else {
            "SELECT id, mint, symbol, classification, confidence, called_at,
                    entry_mcap_usd, entry_price_usd, entry_liquidity_usd,
                    entry_top_holder_pct, entry_pair_dex, note, source,
                    status, closed_at, exit_price_usd, exit_note, expires_at,
                    entry_tx_rate
             FROM calls ORDER BY called_at DESC LIMIT ?1"
        };
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(sql)?;
        let rows = stmt
            .query_map(params![limit], |row| {
                Ok(CallRow {
                    id: row.get(0)?,
                    mint: row.get(1)?,
                    symbol: row.get(2)?,
                    classification: row.get(3)?,
                    confidence: row.get(4)?,
                    called_at: row.get(5)?,
                    entry_mcap_usd: row.get(6)?,
                    entry_price_usd: row.get(7)?,
                    entry_liquidity_usd: row.get(8)?,
                    entry_top_holder_pct: row.get(9)?,
                    entry_pair_dex: row.get(10)?,
                    note: row.get(11)?,
                    source: row.get(12)?,
                    status: row.get(13)?,
                    closed_at: row.get(14).ok(),
                    exit_price_usd: row.get(15).ok(),
                    exit_note: row.get(16).ok(),
                    expires_at: row.get(17).ok(),
                    entry_tx_rate: row.get(18).unwrap_or(0.0),
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Persist the launch-forensics quad (bundle/sniper/insider/smart-money)
    /// onto the most recent snapshot for this mint. Stamped with `computed_at`
    /// so the caller can decide whether to refresh on the next analysis cycle.
    pub fn update_launch_forensics(
        &self,
        mint: &str,
        bundle_pct: f64,
        sniper_pct: f64,
        insider_pct: f64,
        smart_money_count: i32,
        computed_at: i64,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE token_snapshots
             SET bundle_pct = ?1, sniper_pct = ?2, insider_pct = ?3,
                 smart_money_count = ?4, forensics_computed_at = ?5
             WHERE id = (SELECT id FROM token_snapshots
                         WHERE token_address = ?6 ORDER BY timestamp DESC LIMIT 1)",
            params![bundle_pct, sniper_pct, insider_pct, smart_money_count, computed_at, mint],
        )?;
        Ok(())
    }

    /// Read latest forensics + smart-money + age. Returns
    /// `(bundle_pct, sniper_pct, insider_pct, smart_money_count, computed_at)`.
    /// All zero when no snapshot exists or forensics never ran.
    pub fn get_latest_forensics(&self, mint: &str) -> Result<(f64, f64, f64, i32, i64)> {
        let conn = self.conn.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT bundle_pct, sniper_pct, insider_pct, smart_money_count,
                        forensics_computed_at
                 FROM token_snapshots WHERE token_address = ?1
                 ORDER BY timestamp DESC LIMIT 1",
                params![mint],
                |r| {
                    Ok((
                        r.get::<_, f64>(0)?,
                        r.get::<_, f64>(1)?,
                        r.get::<_, f64>(2)?,
                        r.get::<_, i32>(3)?,
                        r.get::<_, i64>(4)?,
                    ))
                },
            )
            .optional()?;
        Ok(row.unwrap_or((0.0, 0.0, 0.0, 0, 0)))
    }

    /// Replace the stored top-holders JSON for the most recent snapshot of
    /// this mint. The scanner calls this after each successful analysis so
    /// holder_evolution has a consistent time series to diff against.
    pub fn update_latest_top_holders_json(&self, mint: &str, json: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE token_snapshots SET top_holders_json = ?1
             WHERE id = (SELECT id FROM token_snapshots
                         WHERE token_address = ?2 ORDER BY timestamp DESC LIMIT 1)",
            params![json, mint],
        )?;
        Ok(())
    }

    pub fn audit_log(&self, actor: &str, action: &str, details: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO audit_log (actor, action, details) VALUES (?1, ?2, ?3)",
            params![actor, action, details],
        )?;
        Ok(())
    }

    pub fn get_audit_logs(&self, limit: usize) -> Result<Vec<AuditEntry>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT timestamp, actor, action, details FROM audit_log ORDER BY id DESC LIMIT ?1",
        )?;
        let logs = stmt
            .query_map(params![limit], |row| {
                Ok(AuditEntry {
                    timestamp: row.get(0)?,
                    actor: row.get(1)?,
                    action: row.get(2)?,
                    details: row.get(3)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(logs)
    }

    // -- Alert queue methods --

    pub fn insert_alert(
        &self,
        alert_type: &str,
        token_address: Option<&str>,
        message: &str,
        confidence: i32,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = chrono::Utc::now().timestamp();
        conn.execute(
            "INSERT INTO alerts (alert_type, token_address, message, confidence, timestamp) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![alert_type, token_address, message, confidence, now],
        )?;
        Ok(())
    }

    /// Acknowledge all alerts older than `max_age_seconds` to drain the queue
    /// without surfacing them. Returns number of rows affected.
    pub fn acknowledge_stale_alerts(&self, max_age_seconds: i64) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let cutoff = chrono::Utc::now().timestamp() - max_age_seconds;
        let n = conn.execute(
            "UPDATE alerts SET acknowledged = 1 WHERE acknowledged = 0 AND timestamp < ?1",
            params![cutoff],
        )?;
        Ok(n)
    }

    pub fn get_pending_alerts(&self, limit: usize) -> Result<Vec<Alert>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, alert_type, token_address, message, confidence, timestamp \
             FROM alerts WHERE acknowledged = 0 ORDER BY confidence DESC, id DESC LIMIT ?1",
        )?;
        let alerts = stmt
            .query_map(params![limit], |row| {
                Ok(Alert {
                    id: row.get(0)?,
                    alert_type: row.get(1)?,
                    token_address: row.get(2)?,
                    message: row.get(3)?,
                    confidence: row.get(4)?,
                    timestamp: row.get(5)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(alerts)
    }

    /// Only returns signal-worthy alert types (staircase / grinder / spring / developing).
    /// Operational events (crashing, velocity_crash, classification_change, etc.) are
    /// internal state-tracking and must NOT surface in the user-facing scan command.
    pub fn get_signal_alerts(&self, limit: usize) -> Result<Vec<Alert>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, alert_type, token_address, message, confidence, timestamp \
             FROM alerts \
             WHERE acknowledged = 0 \
               AND alert_type IN ('staircase','grinder','spring','developing') \
             ORDER BY confidence DESC, id DESC LIMIT ?1",
        )?;
        let alerts = stmt
            .query_map(params![limit], |row| {
                Ok(Alert {
                    id: row.get(0)?,
                    alert_type: row.get(1)?,
                    token_address: row.get(2)?,
                    message: row.get(3)?,
                    confidence: row.get(4)?,
                    timestamp: row.get(5)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(alerts)
    }

    /// Active calls whose watchlist entry is already gone or deactivated.
    /// These can only accumulate when the service restarts after a watchlist
    /// deactivation that happened before the auto-fail hook fires.
    pub fn list_orphaned_calls(&self) -> Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT c.mint FROM calls c \
             WHERE c.status = 'active' \
               AND NOT EXISTS ( \
                 SELECT 1 FROM watchlist w \
                 WHERE w.token_address = c.mint AND w.active = 1 \
               )",
        )?;
        let rows = stmt.query_map([], |row| row.get(0))?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    /// Demote telegram_deliveries rows that are still marked active but whose
    /// token is no longer in the active watchlist and has no active call.
    /// Run every scan cycle to keep the delivery table and /signals count clean.
    pub fn demote_orphaned_deliveries(&self) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let changed = conn.execute(
            "UPDATE telegram_deliveries \
             SET status = 'demoted' \
             WHERE status = 'active' \
               AND NOT EXISTS ( \
                 SELECT 1 FROM watchlist w \
                 WHERE w.token_address = telegram_deliveries.token_address AND w.active = 1 \
               ) \
               AND NOT EXISTS ( \
                 SELECT 1 FROM calls c \
                 WHERE c.mint = telegram_deliveries.token_address AND c.status = 'active' \
               )",
            [],
        )?;
        Ok(changed)
    }

    /// Token classification distribution across the last `lookback_secs` seconds.
    /// Used by /regime to derive a narrative market label from real scan data.
    pub fn get_regime_distribution(&self, lookback_secs: i64) -> Result<Vec<(String, i64)>> {
        let conn = self.conn.lock().unwrap();
        let cutoff = chrono::Utc::now().timestamp() - lookback_secs;
        let mut stmt = conn.prepare(
            "SELECT classification, COUNT(DISTINCT token_address) AS n \
             FROM token_snapshots \
             WHERE timestamp > ?1 \
               AND classification NOT LIKE 'UNSAFE%' \
             GROUP BY classification \
             ORDER BY n DESC \
             LIMIT 10",
        )?;
        let rows = stmt
            .query_map(params![cutoff], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn acknowledge_alerts(&self, ids: &[i64]) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        for id in ids {
            conn.execute(
                "UPDATE alerts SET acknowledged = 1 WHERE id = ?1",
                params![id],
            )?;
        }
        Ok(())
    }

    // -- telegram_deliveries -------------------------------------------------

    pub fn get_active_delivery(
        &self,
        token_address: &str,
        channel: &str,
    ) -> Result<Option<TelegramDelivery>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, token_address, channel, message_id, created_at, last_edit_at,
                    edit_count, status, snapshot_conf, snapshot_class,
                    snapshot_price, snapshot_top_holder, timeline_json
             FROM telegram_deliveries
             WHERE token_address = ?1 AND channel = ?2",
        )?;
        let mut rows = stmt.query(params![token_address, channel])?;
        if let Some(row) = rows.next()? {
            Ok(Some(TelegramDelivery {
                id: row.get(0)?,
                token_address: row.get(1)?,
                channel: row.get(2)?,
                message_id: row.get(3)?,
                created_at: row.get(4)?,
                last_edit_at: row.get(5)?,
                edit_count: row.get(6)?,
                status: row.get(7)?,
                snapshot_conf: row.get(8)?,
                snapshot_class: row.get(9)?,
                snapshot_price: row.get(10)?,
                snapshot_top_holder: row.get(11)?,
                timeline_json: row.get(12)?,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn insert_delivery(
        &self,
        token_address: &str,
        channel: &str,
        message_id: i64,
        snapshot_conf: i32,
        snapshot_class: &str,
        snapshot_price: Option<f64>,
        snapshot_top_holder: Option<f64>,
        timeline_json: &str,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = chrono::Utc::now().timestamp();
        conn.execute(
            "INSERT INTO telegram_deliveries
             (token_address, channel, message_id, created_at, status,
              snapshot_conf, snapshot_class, snapshot_price, snapshot_top_holder, timeline_json)
             VALUES (?1, ?2, ?3, ?4, 'active', ?5, ?6, ?7, ?8, ?9)",
            params![
                token_address,
                channel,
                message_id,
                now,
                snapshot_conf,
                snapshot_class,
                snapshot_price,
                snapshot_top_holder,
                timeline_json
            ],
        )?;
        Ok(())
    }

    pub fn update_delivery(
        &self,
        id: i64,
        status: &str,
        snapshot_conf: i32,
        snapshot_class: &str,
        snapshot_price: Option<f64>,
        snapshot_top_holder: Option<f64>,
        timeline_json: &str,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = chrono::Utc::now().timestamp();
        conn.execute(
            "UPDATE telegram_deliveries
             SET last_edit_at = ?1, edit_count = edit_count + 1, status = ?2,
                 snapshot_conf = ?3, snapshot_class = ?4,
                 snapshot_price = ?5, snapshot_top_holder = ?6,
                 timeline_json = ?7
             WHERE id = ?8",
            params![
                now,
                status,
                snapshot_conf,
                snapshot_class,
                snapshot_price,
                snapshot_top_holder,
                timeline_json,
                id
            ],
        )?;
        Ok(())
    }

    // -- telegram_digests ----------------------------------------------------

    pub fn get_digest(&self, hour_bucket: i64) -> Result<Option<TelegramDigest>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, hour_bucket, channel, message_id, created_at, last_edit_at, edit_count
             FROM telegram_digests WHERE hour_bucket = ?1",
        )?;
        let mut rows = stmt.query(params![hour_bucket])?;
        if let Some(row) = rows.next()? {
            Ok(Some(TelegramDigest {
                id: row.get(0)?,
                hour_bucket: row.get(1)?,
                channel: row.get(2)?,
                message_id: row.get(3)?,
                created_at: row.get(4)?,
                last_edit_at: row.get(5)?,
                edit_count: row.get(6)?,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn insert_digest(&self, hour_bucket: i64, channel: &str, message_id: i64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = chrono::Utc::now().timestamp();
        conn.execute(
            "INSERT INTO telegram_digests (hour_bucket, channel, message_id, created_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![hour_bucket, channel, message_id, now],
        )?;
        Ok(())
    }

    pub fn touch_digest(&self, id: i64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = chrono::Utc::now().timestamp();
        conn.execute(
            "UPDATE telegram_digests SET last_edit_at = ?1, edit_count = edit_count + 1 WHERE id = ?2",
            params![now, id],
        )?;
        Ok(())
    }

    /// Remove a stale digest row by id. Used when an edit fails because the
    /// Telegram message no longer exists (deleted in the channel) — we drop
    /// the cached pointer and the next tick posts a fresh message.
    pub fn delete_digest(&self, id: i64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM telegram_digests WHERE id = ?1", params![id])?;
        Ok(())
    }

    /// Pending alerts grouped by type — for the hourly digest.
    pub fn pending_alerts_by_type(&self) -> Result<Vec<(String, i64)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT alert_type, COUNT(*) FROM alerts WHERE acknowledged = 0
             GROUP BY alert_type ORDER BY COUNT(*) DESC",
        )?;
        let rows: Vec<(String, i64)> = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Count active telegram deliveries for a given channel.
    pub fn active_delivery_count(&self, channel: &str) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM telegram_deliveries WHERE channel = ?1 AND status = 'active'",
            params![channel],
            |row| row.get(0),
        )?;
        Ok(count)
    }

    pub fn pending_alert_count(&self) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM alerts WHERE acknowledged = 0",
            [],
            |row| row.get(0),
        )?;
        Ok(count as usize)
    }

    /// Check if we've analyzed a token recently (within last N seconds)
    pub fn token_analyzed_recently(&self, address: &str, within_seconds: i64) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let cutoff = chrono::Utc::now().timestamp() - within_seconds;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM tokens WHERE address = ?1 AND first_seen > ?2",
            params![address, cutoff],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    // -- Watchlist --

    /// True if the token is known (in tokens table), NOT on the active watchlist,
    /// and has no snapshot in the last `cooldown_secs` seconds. Used by the
    /// graduation detector to avoid re-checking the same mint every cycle.
    pub fn is_graduation_candidate(&self, address: &str, cooldown_secs: i64) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let now = chrono::Utc::now().timestamp();
        let cutoff = now - cooldown_secs;
        let found = conn
            .query_row(
                "SELECT 1 FROM tokens t
                 WHERE t.address = ?1
                   AND NOT EXISTS (
                     SELECT 1 FROM watchlist w
                     WHERE w.token_address = ?1 AND w.active = 1
                   )
                   AND NOT EXISTS (
                     SELECT 1 FROM token_snapshots s
                     WHERE s.token_address = ?1 AND s.timestamp > ?2
                   )",
                params![address, cutoff],
                |_| Ok(true),
            )
            .optional()?;
        Ok(found.unwrap_or(false))
    }

    pub fn add_to_watchlist(&self, address: &str, classification: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = chrono::Utc::now().timestamp();
        conn.execute(
            "INSERT INTO watchlist (token_address, classification, added_at, last_checked, active) \
             VALUES (?1, ?2, ?3, 0, 1)
             ON CONFLICT(token_address) DO UPDATE SET
               classification = excluded.classification,
               active = 1",
            params![address, classification, now],
        )?;
        Ok(())
    }

    pub fn list_active_watchlist_candidates(&self) -> Result<Vec<WatchlistCandidate>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT
                w.token_address,
                w.classification,
                w.added_at,
                w.last_checked,
                s.classification,
                s.confidence,
                s.holder_count,
                s.top_holder_pct,
                s.momentum,
                s.distribution,
                s.timestamp
             FROM watchlist w
             LEFT JOIN token_snapshots s
               ON s.id = (
                    SELECT ts.id
                    FROM token_snapshots ts
                    WHERE ts.token_address = w.token_address
                    ORDER BY ts.timestamp DESC
                    LIMIT 1
               )
             WHERE w.active = 1",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok(WatchlistCandidate {
                    token_address: row.get(0)?,
                    watch_classification: row.get(1)?,
                    added_at: row.get(2)?,
                    last_checked: row.get(3)?,
                    snapshot_classification: row.get(4)?,
                    snapshot_confidence: row.get(5)?,
                    snapshot_holder_count: row.get(6)?,
                    snapshot_top_holder_pct: row.get(7)?,
                    snapshot_momentum: row.get(8)?,
                    snapshot_distribution: row.get(9)?,
                    snapshot_timestamp: row.get(10)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn get_watchlist_due(
        &self,
        check_interval_seconds: i64,
        limit: usize,
    ) -> Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let cutoff = chrono::Utc::now().timestamp() - check_interval_seconds;
        let mut stmt = conn.prepare(
            "SELECT token_address FROM watchlist \
             WHERE active = 1 AND last_checked < ?1 \
             ORDER BY last_checked ASC LIMIT ?2",
        )?;
        let addrs = stmt
            .query_map(params![cutoff, limit], |row| row.get(0))?
            .collect::<Result<Vec<String>, _>>()?;
        Ok(addrs)
    }

    pub fn update_watchlist_checked(&self, address: &str, classification: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = chrono::Utc::now().timestamp();
        conn.execute(
            "UPDATE watchlist SET last_checked = ?1, classification = ?2 WHERE token_address = ?3",
            params![now, classification, address],
        )?;
        Ok(())
    }

    /// Bump only `last_checked` without touching classification. Used by the
    /// scanner when an analysis fails — moves the failing token to the back
    /// of the queue so subsequent items get a turn.
    pub fn update_watchlist_last_checked_only(&self, address: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = chrono::Utc::now().timestamp();
        conn.execute(
            "UPDATE watchlist SET last_checked = ?1 WHERE token_address = ?2",
            params![now, address],
        )?;
        Ok(())
    }

    pub fn deactivate_watchlist(&self, address: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE watchlist SET active = 0 WHERE token_address = ?1",
            params![address],
        )?;
        Ok(())
    }

    pub fn deactivate_watchlist_many(&self, addresses: &[String]) -> Result<usize> {
        if addresses.is_empty() {
            return Ok(0);
        }
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let mut changed = 0usize;
        for address in addresses {
            changed += tx.execute(
                "UPDATE watchlist SET active = 0 WHERE token_address = ?1",
                params![address],
            )?;
        }
        tx.commit()?;
        Ok(changed)
    }

    pub fn reactivate_watchlist(&self, address: &str, classification: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = chrono::Utc::now().timestamp();
        conn.execute(
            "UPDATE watchlist SET active = 1, classification = ?2, last_checked = ?3 \
             WHERE token_address = ?1",
            params![address, classification, now],
        )?;
        Ok(())
    }

    pub fn get_volume_spike_candidates(
        &self,
        min_volume_usd: f64,
        lookback_secs: i64,
        limit: usize,
    ) -> Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let cutoff = chrono::Utc::now().timestamp() - lookback_secs;
        let mut stmt = conn.prepare(
            "SELECT DISTINCT s.token_address \
             FROM token_snapshots s \
             WHERE s.volume_h1_usd >= ?1 \
               AND s.timestamp >= ?2 \
               AND NOT EXISTS ( \
                 SELECT 1 FROM watchlist w \
                 WHERE w.token_address = s.token_address AND w.active = 1 \
               ) \
             ORDER BY s.volume_h1_usd DESC \
             LIMIT ?3",
        )?;
        let rows = stmt.query_map(params![min_volume_usd, cutoff, limit as i64], |row| {
            row.get(0)
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    // -- Snapshot tracking (film, not frames) --

    pub fn insert_snapshot(&self, snap: &TokenSnapshot) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        // Carry forensics forward — schema defaults to 0, which would race with the next async refresh and reopen gates.
        let prior: Option<(f64, f64, f64, i32, i64)> = conn
            .query_row(
                "SELECT bundle_pct, sniper_pct, insider_pct, smart_money_count, forensics_computed_at \
                 FROM token_snapshots WHERE token_address = ?1 ORDER BY timestamp DESC LIMIT 1",
                params![snap.token_address],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )
            .optional()?;
        let (bp, sp, ip, sm, fc) = prior.unwrap_or((0.0, 0.0, 0.0, 0, 0));
        conn.execute(
            "INSERT INTO token_snapshots (token_address, top_holder_pct, top5_pct, top10_pct, \
             holder_count, tx_rate, velocity, momentum, distribution, spring, classification, \
             confidence, timestamp, price_usd, mcap_usd, liquidity_usd, volume_h1_usd, \
             buys_h1, sells_h1, price_change_h1, pair_dex, \
             bundle_pct, sniper_pct, insider_pct, smart_money_count, forensics_computed_at) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,?26)",
            params![
                snap.token_address,
                snap.top_holder_pct,
                snap.top5_pct,
                snap.top10_pct,
                snap.holder_count,
                snap.tx_rate,
                snap.velocity,
                snap.momentum,
                snap.distribution,
                snap.spring,
                snap.classification,
                snap.confidence,
                snap.timestamp,
                snap.price_usd,
                snap.mcap_usd,
                snap.liquidity_usd,
                snap.volume_h1_usd,
                snap.buys_h1,
                snap.sells_h1,
                snap.price_change_h1,
                snap.pair_dex,
                bp,
                sp,
                ip,
                sm,
                fc,
            ],
        )?;
        Ok(())
    }

    /// Highest observed price-pct vs `entry` since `since_ts`, in percent.
    /// Used by SCALP "no-pump" settle to distinguish a token that briefly
    /// ran but is now red (let it run, may recover) from one that never
    /// went green (exit, structural failure).
    pub fn get_peak_pct_since(&self, address: &str, since_ts: i64, entry: f64) -> Result<f64> {
        if entry <= 0.0 {
            return Ok(0.0);
        }
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT MAX(price_usd) FROM token_snapshots
             WHERE token_address = ?1 AND timestamp >= ?2 AND price_usd > 0",
        )?;
        let max_price: Option<f64> = stmt
            .query_row(params![address, since_ts], |row| row.get(0))
            .ok();
        Ok(max_price.map(|p| (p / entry - 1.0) * 100.0).unwrap_or(0.0))
    }

    /// Fetch a single call row by mint, status-agnostic. Returns the most
    /// recent row when there are multiple historical entries for the same
    /// mint. None when no row exists. Used by the TG card renderer to
    /// recover the original call narrative even after the call has closed.
    pub fn get_call_by_mint(&self, mint: &str) -> Result<Option<CallRow>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, mint, symbol, classification, confidence, called_at,
                    entry_mcap_usd, entry_price_usd, entry_liquidity_usd,
                    entry_top_holder_pct, entry_pair_dex, note, source,
                    status, closed_at, exit_price_usd, exit_note, expires_at,
                    entry_tx_rate
             FROM calls WHERE mint = ?1 ORDER BY called_at DESC LIMIT 1",
        )?;
        let mut rows = stmt.query(params![mint])?;
        if let Some(row) = rows.next()? {
            Ok(Some(CallRow {
                id: row.get(0)?,
                mint: row.get(1)?,
                symbol: row.get(2)?,
                classification: row.get(3)?,
                confidence: row.get(4)?,
                called_at: row.get(5)?,
                entry_mcap_usd: row.get(6)?,
                entry_price_usd: row.get(7)?,
                entry_liquidity_usd: row.get(8)?,
                entry_top_holder_pct: row.get(9)?,
                entry_pair_dex: row.get(10)?,
                note: row.get(11)?,
                source: row.get(12)?,
                status: row.get(13)?,
                closed_at: row.get(14).ok(),
                exit_price_usd: row.get(15).ok(),
                exit_note: row.get(16).ok(),
                expires_at: row.get(17).ok(),
                entry_tx_rate: row.get(18).unwrap_or(0.0),
            }))
        } else {
            Ok(None)
        }
    }

    /// Oldest snapshot price within [since_ts, until_ts]. Used by the
    /// moonshot gate to compute pre-DEV trajectory slope: compare current
    /// entry price to the oldest price in the lookback window. None means
    /// no observable history — caller decides whether to treat as pass or
    /// reject. Returns the price only (not the full snapshot) to keep the
    /// query cheap.
    pub fn get_oldest_price_in_window(
        &self,
        address: &str,
        since_ts: i64,
        until_ts: i64,
    ) -> Result<Option<f64>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT price_usd FROM token_snapshots
             WHERE token_address = ?1 AND timestamp >= ?2 AND timestamp <= ?3 AND price_usd > 0
             ORDER BY timestamp ASC LIMIT 1",
        )?;
        let price: Option<f64> = stmt
            .query_row(params![address, since_ts, until_ts], |row| row.get(0))
            .ok();
        Ok(price)
    }

    /// Read the current anchor forward's (chat_id, message_id). Empty
    /// chat_id means legacy row (pre-migration); empty/0 msg_id means
    /// no forward has ever been posted.
    pub fn get_anchor_state(&self) -> Result<(String, i64)> {
        let conn = self.conn.lock().unwrap();
        let row: (String, i64) = conn
            .query_row(
                "SELECT current_chat_id, current_msg_id FROM lounge_anchor_state WHERE id = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap_or_else(|_| (String::new(), 0));
        Ok(row)
    }

    /// Persist the new anchor forward's (chat_id, message_id) after a
    /// successful forwardMessage. Subsequent bumps read it back to
    /// know where to delete the prior orphan.
    pub fn set_anchor_state(&self, chat_id: &str, msg_id: i64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = chrono::Utc::now().timestamp();
        conn.execute(
            "UPDATE lounge_anchor_state SET current_chat_id = ?1, current_msg_id = ?2, last_bump_at = ?3 WHERE id = 1",
            params![chat_id, msg_id, now],
        )?;
        Ok(())
    }

    /// Back-compat shim — returns just the msg_id for the MCP tool's
    /// "current_msg_id" return field. Drop when MCP gets a richer state
    /// surface.
    pub fn get_lounge_anchor_msg_id(&self) -> Result<i64> {
        Ok(self.get_anchor_state()?.1)
    }

    /// Claim a mint for buyer-trace. Atomic: returns Ok(true) when the
    /// mint was newly inserted into wallet_observation_traces, Ok(false)
    /// if a prior trace already exists. Used by wallet_observer to make
    /// the per-mint trace fire exactly once across the lifetime of the
    /// process (idempotency survives restart). Caller is responsible for
    /// updating buyer_count + success after the trace completes.
    pub fn claim_observation_trace(&self, mint: &str) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let now = chrono::Utc::now().timestamp();
        let n = conn.execute(
            "INSERT OR IGNORE INTO wallet_observation_traces
                (mint, traced_at, buyer_count, success)
             VALUES (?1, ?2, 0, 0)",
            params![mint, now],
        )?;
        Ok(n > 0)
    }

    /// Mark a trace complete with realized buyer count. Called after the
    /// buyer-walk finishes (success or partial). Failure path leaves
    /// success=0 so a future operator inspection can find them.
    pub fn finalize_observation_trace(&self, mint: &str, buyer_count: i64, success: bool) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE wallet_observation_traces
                SET buyer_count = ?2, success = ?3
              WHERE mint = ?1",
            params![mint, buyer_count, if success { 1 } else { 0 }],
        )?;
        Ok(())
    }

    /// Insert a (wallet, mint, rank) observation. UNIQUE(wallet,mint)
    /// makes this idempotent — re-runs of a partial trace won't error.
    pub fn insert_wallet_observation(
        &self,
        wallet: &str,
        mint: &str,
        rank: i64,
        buy_ts: i64,
        buy_sol: f64,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = chrono::Utc::now().timestamp();
        conn.execute(
            "INSERT OR IGNORE INTO wallet_observations
                (wallet, mint, rank, buy_ts, buy_sol, observed_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![wallet, mint, rank, buy_ts, buy_sol, now],
        )?;
        Ok(())
    }

    /// Most recent (top_holder_pct, top10_pct) pair from token_snapshots
    /// where top_holder_pct > 0 (i.e. the read wasn't a getMultipleAccounts
    /// partial). Used as a fallback when the current analysis came back
    /// with top_holder=0 due to RPC degradation — better to gate on
    /// last-known distribution than to silently skip the candidate.
    /// Bounded by max_age_secs so we don't carry forward distribution
    /// data that's already stale.
    pub fn get_last_good_top_holder(
        &self,
        address: &str,
        max_age_secs: i64,
    ) -> Result<Option<(f64, f64)>> {
        let conn = self.conn.lock().unwrap();
        let since_ts = chrono::Utc::now().timestamp() - max_age_secs;
        let mut stmt = conn.prepare(
            "SELECT top_holder_pct, top10_pct FROM token_snapshots
             WHERE token_address = ?1 AND timestamp >= ?2 AND top_holder_pct > 0
             ORDER BY timestamp DESC LIMIT 1",
        )?;
        let row = stmt
            .query_row(params![address, since_ts], |row| {
                Ok((row.get::<_, f64>(0)?, row.get::<_, f64>(1)?))
            })
            .ok();
        Ok(row)
    }

    /// Return the (timestamp, price_usd) series for a token within a time
    /// window — used by the chart sparkline renderer. Excludes price=0
    /// snapshots (failed fetches). Sorted ascending by ts.
    pub fn get_price_history(
        &self,
        address: &str,
        since_ts: i64,
    ) -> Result<Vec<(i64, f64)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT timestamp, price_usd FROM token_snapshots
             WHERE token_address = ?1 AND timestamp >= ?2 AND price_usd > 0
             ORDER BY timestamp ASC",
        )?;
        let rows: Vec<(i64, f64)> = stmt
            .query_map(params![address, since_ts], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, f64>(1)?))
            })?
            .filter_map(Result::ok)
            .collect();
        Ok(rows)
    }

    /// Peak snapshot by confidence within a time window — used to measure
    /// collapse severity against a token's best state in recent memory.
    pub fn get_peak_snapshot(&self, address: &str, since_ts: i64) -> Result<Option<TokenSnapshot>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(&format!(
            "SELECT {} FROM token_snapshots
             WHERE token_address = ?1 AND timestamp >= ?2
             ORDER BY confidence DESC, timestamp DESC LIMIT 1",
            SNAPSHOT_COLS,
        ))?;
        let mut rows = stmt.query(params![address, since_ts])?;
        if let Some(row) = rows.next()? {
            Ok(Some(snapshot_from_row(row)?))
        } else {
            Ok(None)
        }
    }

    /// Highest and lowest price observed for a token between two timestamps,
    /// derived from `token_snapshots`. Used by the publisher to enrich each
    /// call snapshot with peak / trough — the journey, not just entry+exit.
    /// Returns ((max_price, max_ts), (min_price, min_ts)) or None when no
    /// price-bearing snapshots exist in the window.
    pub fn get_price_extremes(
        &self,
        address: &str,
        since_ts: i64,
        until_ts: i64,
    ) -> Result<Option<((f64, i64), (f64, i64))>> {
        let conn = self.conn.lock().unwrap();
        // Two-row pull. Filtering price > 0 strips snapshots taken before
        // DexScreener had data (curve-only pump.fun tokens).
        let high: Option<(f64, i64)> = conn
            .query_row(
                "SELECT price_usd, timestamp FROM token_snapshots
                 WHERE token_address = ?1 AND price_usd > 0
                   AND timestamp >= ?2 AND timestamp <= ?3
                 ORDER BY price_usd DESC, timestamp ASC LIMIT 1",
                params![address, since_ts, until_ts],
                |row| Ok((row.get::<_, f64>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()?;
        let low: Option<(f64, i64)> = conn
            .query_row(
                "SELECT price_usd, timestamp FROM token_snapshots
                 WHERE token_address = ?1 AND price_usd > 0
                   AND timestamp >= ?2 AND timestamp <= ?3
                 ORDER BY price_usd ASC, timestamp ASC LIMIT 1",
                params![address, since_ts, until_ts],
                |row| Ok((row.get::<_, f64>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()?;
        match (high, low) {
            (Some(h), Some(l)) => Ok(Some((h, l))),
            _ => Ok(None),
        }
    }

    /// Latest snapshot (by timestamp) for a token.
    pub fn get_latest_snapshot(&self, address: &str) -> Result<Option<TokenSnapshot>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(&format!(
            "SELECT {} FROM token_snapshots
             WHERE token_address = ?1 ORDER BY timestamp DESC LIMIT 1",
            SNAPSHOT_COLS,
        ))?;
        let mut rows = stmt.query(params![address])?;
        if let Some(row) = rows.next()? {
            Ok(Some(snapshot_from_row(row)?))
        } else {
            Ok(None)
        }
    }

    /// Whether any alert of `alert_type` for this mint exists since `since_ts`.
    /// Used by the settle phase to detect DEV_SELLING and classification
    /// regressions on active calls.
    pub fn has_recent_alert(
        &self,
        mint: &str,
        alert_type: &str,
        since_ts: i64,
    ) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT 1 FROM alerts \
             WHERE token_address = ?1 AND alert_type = ?2 AND timestamp >= ?3 LIMIT 1",
        )?;
        let exists = stmt
            .query_row(params![mint, alert_type, since_ts], |_| Ok(()))
            .optional()?
            .is_some();
        Ok(exists)
    }

    /// Whether any alert of `alert_type` with confidence >= `min_conf` for
    /// this mint exists since `since_ts`. Used by the settle phase to filter
    /// dev_selling alerts — only severe drops (deployer down ≥40%, encoded as
    /// confidence ≥90) close calls. Mining of 1700+ dev_selling alerts (see
    /// 2026-04-29 mining session) showed light dev_selling is bullish on
    /// average; only severe exits predict downside.
    pub fn has_recent_severe_alert(
        &self,
        mint: &str,
        alert_type: &str,
        since_ts: i64,
        min_conf: i32,
    ) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT 1 FROM alerts \
             WHERE token_address = ?1 AND alert_type = ?2 \
               AND timestamp >= ?3 AND confidence >= ?4 LIMIT 1",
        )?;
        let exists = stmt
            .query_row(params![mint, alert_type, since_ts, min_conf], |_| Ok(()))
            .optional()?
            .is_some();
        Ok(exists)
    }

    /// Distinct tokens that fired degradation alerts (concentrating, velocity_crash,
    /// classification_change, crashing) within a time window. Used to seed the
    /// trap-report candidate list.
    pub fn get_degradation_tokens_since(&self, since_ts: i64) -> Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT DISTINCT token_address FROM alerts
             WHERE timestamp >= ?1 AND token_address IS NOT NULL
               AND alert_type IN ('concentrating','velocity_crash','classification_change','crashing')"
        )?;
        let rows: Vec<String> = stmt
            .query_map(params![since_ts], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn get_previous_snapshot(&self, address: &str) -> Result<Option<TokenSnapshot>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(&format!(
            "SELECT {} FROM token_snapshots WHERE token_address = ?1 ORDER BY timestamp DESC LIMIT 1",
            SNAPSHOT_COLS,
        ))?;
        let snap = stmt
            .query_row(params![address], |row| snapshot_from_row(row))
            .optional()?;
        Ok(snap)
    }

    pub fn get_snapshot_history(&self, address: &str, limit: usize) -> Result<Vec<TokenSnapshot>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(&format!(
            "SELECT {} FROM token_snapshots WHERE token_address = ?1 ORDER BY timestamp DESC LIMIT ?2",
            SNAPSHOT_COLS,
        ))?;
        let snaps = stmt
            .query_map(params![address, limit], |row| snapshot_from_row(row))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(snaps)
    }

    pub fn compute_delta(
        &self,
        address: &str,
        current: &TokenSnapshot,
    ) -> Result<Option<TokenDelta>> {
        let previous = self.get_previous_snapshot(address)?;
        match previous {
            None => Ok(None),
            Some(prev) => {
                let top_holder_delta = current.top_holder_pct - prev.top_holder_pct;
                let top5_delta = current.top5_pct - prev.top5_pct;
                let holder_count_delta = current.holder_count - prev.holder_count;
                let momentum_delta = current.momentum - prev.momentum;
                let time_elapsed = current.timestamp - prev.timestamp;

                let concentration_direction = if top_holder_delta < -2.0 {
                    "distributing".to_string()
                } else if top_holder_delta > 2.0 {
                    "concentrating".to_string()
                } else {
                    "stable".to_string()
                };

                let classification_changed = current.classification != prev.classification;

                Ok(Some(TokenDelta {
                    previous: prev,
                    current: current.clone(),
                    top_holder_delta,
                    top5_delta,
                    holder_count_delta,
                    momentum_delta,
                    time_elapsed_seconds: time_elapsed,
                    concentration_direction,
                    classification_changed,
                }))
            }
        }
    }

    // ── Trade execution helpers ───────────────────────────────────────
    //
    // The trade columns on `calls` are additive: they record the on-chain
    // result of the buy/sell against an existing call row, but the row's
    // status is still owned by the paper lifecycle. Helpers below are
    // idempotent — re-firing a buy or sell after a successful record is
    // a no-op via the WHERE-clause guards.

    /// Mark that we've attempted a buy on this call (so retries don't
    /// double-fire). Sets `buy_attempted_at` to now. Returns false if
    /// the call already has a buy_signature OR a buy_attempted_at
    /// (single-flight).
    pub fn mark_buy_attempt(&self, call_id: i64, ts: i64) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let n = conn.execute(
            "UPDATE calls SET buy_attempted_at = ?2
             WHERE id = ?1
               AND buy_signature IS NULL
               AND buy_attempted_at IS NULL",
            rusqlite::params![call_id, ts],
        )?;
        Ok(n > 0)
    }

    /// Record a successful buy. Idempotent — if buy_signature already set,
    /// this is a no-op.
    pub fn record_buy(
        &self,
        call_id: i64,
        signature: &str,
        sol_spent: f64,
        token_received: f64,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE calls SET buy_signature = ?2,
                              buy_sol_spent = ?3,
                              buy_token_received = ?4,
                              buy_failed_reason = NULL
             WHERE id = ?1 AND buy_signature IS NULL",
            rusqlite::params![call_id, signature, sol_spent, token_received],
        )?;
        Ok(())
    }

    /// Record a failed buy. The call row is also voided so the front-end
    /// doesn't show a paper-positive call we never actually entered.
    pub fn record_buy_failure(&self, call_id: i64, reason: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE calls SET buy_failed_reason = ?2,
                              status = 'voided',
                              exit_note = ?3
             WHERE id = ?1 AND buy_signature IS NULL",
            rusqlite::params![call_id, reason, format!("buy failed: {}", reason)],
        )?;
        Ok(())
    }

    /// Record a successful sell. Caller is responsible for then transitioning
    /// the call status to withdrew/failed/expired via close_call/fail_call/
    /// expire_call as the paper outcome dictates.
    pub fn record_sell(
        &self,
        call_id: i64,
        signature: &str,
        token_sold: f64,
        sol_received: f64,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE calls SET sell_signature = ?2,
                              sell_token_sold = ?3,
                              sell_sol_received = ?4,
                              sell_last_error = NULL
             WHERE id = ?1 AND sell_signature IS NULL",
            rusqlite::params![call_id, signature, token_sold, sol_received],
        )?;
        Ok(())
    }

    /// Record a sell failure. Increments attempt counter so the settle
    /// loop can decide when to give up and mark the call failed for
    /// manual operator exit.
    pub fn record_sell_failure(&self, call_id: i64, reason: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE calls SET sell_attempt_count = sell_attempt_count + 1,
                              sell_last_error = ?2
             WHERE id = ?1 AND sell_signature IS NULL",
            rusqlite::params![call_id, reason],
        )?;
        Ok(())
    }

    /// Number of currently-active calls whose buy fired but sell hasn't.
    /// Used to enforce max_simultaneous_positions.
    pub fn count_open_positions(&self) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM calls
             WHERE buy_signature IS NOT NULL
               AND sell_signature IS NULL
               AND status = 'active'",
            [],
            |r| r.get(0),
        )?;
        Ok(n)
    }

    /// Sum of SOL spent on buys in the last 24h. Used to enforce
    /// max_daily_position cap.
    pub fn sum_daily_buy_sol(&self, since: i64) -> Result<f64> {
        let conn = self.conn.lock().unwrap();
        let s: f64 = conn.query_row(
            "SELECT COALESCE(SUM(buy_sol_spent),0) FROM calls
             WHERE buy_signature IS NOT NULL AND called_at >= ?1",
            rusqlite::params![since],
            |r| r.get(0),
        )?;
        Ok(s)
    }

    /// Look up the sell-amount source for a call: returns the token amount
    /// we recorded at buy time. None if no buy was made (paper call).
    pub fn get_buy_token_amount(&self, call_id: i64) -> Result<Option<f64>> {
        let conn = self.conn.lock().unwrap();
        let v: Option<f64> = conn
            .query_row(
                "SELECT buy_token_received FROM calls WHERE id = ?1",
                rusqlite::params![call_id],
                |r| r.get(0),
            )
            .ok();
        Ok(v)
    }

    /// Has this call's buy fired (signature present)? Used by settle
    /// to gate sell attempts.
    pub fn call_has_buy(&self, call_id: i64) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let v: Option<String> = conn
            .query_row(
                "SELECT buy_signature FROM calls WHERE id = ?1",
                rusqlite::params![call_id],
                |r| r.get(0),
            )
            .ok()
            .flatten();
        Ok(v.is_some())
    }

    /// Has this call's sell fired? Single-flight guard for sell spawn.
    pub fn call_has_sell(&self, call_id: i64) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let v: Option<String> = conn
            .query_row(
                "SELECT sell_signature FROM calls WHERE id = ?1",
                rusqlite::params![call_id],
                |r| r.get(0),
            )
            .ok()
            .flatten();
        Ok(v.is_some())
    }

    /// Sell attempts so far on this call (for retry-cap enforcement).
    pub fn get_sell_attempt_count(&self, call_id: i64) -> Result<i32> {
        let conn = self.conn.lock().unwrap();
        let n: i32 = conn.query_row(
            "SELECT sell_attempt_count FROM calls WHERE id = ?1",
            rusqlite::params![call_id],
            |r| r.get(0),
        )?;
        Ok(n)
    }
}

#[derive(Debug, Clone)]
pub struct Alert {
    pub id: i64,
    pub alert_type: String,
    pub token_address: Option<String>,
    pub message: String,
    pub confidence: i32,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct TokenSnapshot {
    pub token_address: String,
    pub top_holder_pct: f64,
    pub top5_pct: f64,
    pub top10_pct: f64,
    pub holder_count: i32,
    pub tx_rate: f64,
    pub velocity: f64,
    pub momentum: i32,
    pub distribution: i32,
    pub spring: i32,
    pub classification: String,
    pub confidence: i32,
    pub timestamp: i64,
    // Market-data fields (DexScreener). Zero when not reported.
    pub price_usd: f64,
    pub mcap_usd: f64,
    pub liquidity_usd: f64,
    pub volume_h1_usd: f64,
    pub buys_h1: i32,
    pub sells_h1: i32,
    pub price_change_h1: f64,
    pub pair_dex: String,
    // Launch forensics — populated by the async refresh task; 0 when
    // never measured. Surfaced in published call-detail JSON so the
    // public ledger shows what we knew about each token's structure.
    pub bundle_pct: f64,
    pub sniper_pct: f64,
    pub insider_pct: f64,
    pub smart_money_count: i32,
    pub forensics_computed_at: i64,
}

/// Snapshot of one public call for publisher/UI consumption. Mirrors the
/// `calls` table schema; entry fields are frozen at call-time.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CallRow {
    pub id: i64,
    pub mint: String,
    pub symbol: String,
    pub classification: String,
    pub confidence: i32,
    pub called_at: i64,
    pub entry_mcap_usd: f64,
    pub entry_price_usd: f64,
    pub entry_liquidity_usd: f64,
    pub entry_top_holder_pct: f64,
    pub entry_pair_dex: String,
    pub note: String,
    pub source: String,
    pub status: String,
    pub closed_at: Option<i64>,
    pub exit_price_usd: Option<f64>,
    pub exit_note: Option<String>,
    pub expires_at: Option<i64>,
    /// Transactions per minute at entry. Used by the settling phase's
    /// volume-collapse rule. Zero on legacy rows or when the entry
    /// path didn't have an analyzer snapshot to read from.
    pub entry_tx_rate: f64,
}

/// Column list shared by every `SELECT … FROM token_snapshots` — keeps the
/// row-mapping helper below in lockstep with the INSERT and struct layout.
/// Update this whenever columns are added.
const SNAPSHOT_COLS: &str =
    "token_address, top_holder_pct, top5_pct, top10_pct, holder_count, tx_rate, \
     velocity, momentum, distribution, spring, classification, confidence, timestamp, \
     price_usd, mcap_usd, liquidity_usd, volume_h1_usd, buys_h1, sells_h1, \
     price_change_h1, pair_dex, bundle_pct, sniper_pct, insider_pct, \
     smart_money_count, forensics_computed_at";

fn snapshot_from_row(row: &rusqlite::Row) -> rusqlite::Result<TokenSnapshot> {
    Ok(TokenSnapshot {
        token_address: row.get(0)?,
        top_holder_pct: row.get(1)?,
        top5_pct: row.get(2)?,
        top10_pct: row.get(3)?,
        holder_count: row.get(4)?,
        tx_rate: row.get(5)?,
        velocity: row.get(6)?,
        momentum: row.get(7)?,
        distribution: row.get(8)?,
        spring: row.get(9)?,
        classification: row.get(10)?,
        confidence: row.get(11)?,
        timestamp: row.get(12)?,
        price_usd: row.get(13)?,
        mcap_usd: row.get(14)?,
        liquidity_usd: row.get(15)?,
        volume_h1_usd: row.get(16)?,
        buys_h1: row.get(17)?,
        sells_h1: row.get(18)?,
        price_change_h1: row.get(19)?,
        pair_dex: row.get(20)?,
        bundle_pct: row.get::<_, f64>(21).unwrap_or(0.0),
        sniper_pct: row.get::<_, f64>(22).unwrap_or(0.0),
        insider_pct: row.get::<_, f64>(23).unwrap_or(0.0),
        smart_money_count: row.get::<_, i32>(24).unwrap_or(0),
        forensics_computed_at: row.get::<_, i64>(25).unwrap_or(0),
    })
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct TokenDelta {
    pub previous: TokenSnapshot,
    pub current: TokenSnapshot,
    pub top_holder_delta: f64,
    pub top5_delta: f64,
    pub holder_count_delta: i32,
    pub momentum_delta: i32,
    pub time_elapsed_seconds: i64,
    pub concentration_direction: String, // "distributing", "concentrating", "stable"
    pub classification_changed: bool,
}

#[derive(Debug, Clone)]
pub struct TelegramDelivery {
    pub id: i64,
    pub token_address: String,
    pub channel: String,
    pub message_id: i64,
    pub created_at: i64,
    pub last_edit_at: Option<i64>,
    pub edit_count: i32,
    pub status: String,
    pub snapshot_conf: i32,
    pub snapshot_class: String,
    pub snapshot_price: Option<f64>,
    pub snapshot_top_holder: Option<f64>,
    pub timeline_json: String,
}

#[derive(Debug, Clone)]
pub struct NearMiss {
    pub token_address: String,
    pub classification: String,
    pub effective_confidence: i32,
    pub top_holder_pct: f64,
    pub momentum_delta: Option<i32>,
    pub gate_that_failed: String,
    pub gap: String,
    pub timestamp: i64,
}

#[derive(Debug, Clone)]
pub struct TelegramDigest {
    pub id: i64,
    pub hour_bucket: i64,
    pub channel: String,
    pub message_id: i64,
    pub created_at: i64,
    pub last_edit_at: Option<i64>,
    pub edit_count: i32,
}
