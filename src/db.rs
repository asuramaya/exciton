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

            CREATE TABLE IF NOT EXISTS token_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token_address TEXT NOT NULL REFERENCES tokens(address),
                layer TEXT NOT NULL,
                signal_type TEXT NOT NULL,
                score INTEGER NOT NULL,
                details TEXT,
                timestamp INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS wallets (
                address TEXT PRIMARY KEY,
                label TEXT,
                win_rate REAL NOT NULL DEFAULT 0.0,
                avg_return REAL NOT NULL DEFAULT 0.0,
                trade_count INTEGER NOT NULL DEFAULT 0,
                last_seen INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS wallet_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wallet_address TEXT NOT NULL REFERENCES wallets(address),
                token_address TEXT NOT NULL,
                side TEXT NOT NULL,
                amount_sol REAL NOT NULL,
                token_amount REAL NOT NULL,
                timestamp INTEGER NOT NULL,
                tx_signature TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token_address TEXT NOT NULL,
                side TEXT NOT NULL,
                amount_sol REAL NOT NULL,
                token_amount REAL NOT NULL,
                entry_price REAL,
                exit_price REAL,
                pnl_sol REAL,
                confidence_at_entry INTEGER,
                signal_state TEXT,
                tx_signature TEXT,
                status TEXT NOT NULL DEFAULT 'open',
                opened_at INTEGER NOT NULL,
                closed_at INTEGER
            );

            CREATE TABLE IF NOT EXISTS regimes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                regime_type TEXT NOT NULL,
                confidence INTEGER NOT NULL,
                features TEXT NOT NULL,
                timestamp INTEGER NOT NULL
            );

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
                timestamp INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_snapshots_token ON token_snapshots(token_address);
            CREATE INDEX IF NOT EXISTS idx_snapshots_timestamp ON token_snapshots(timestamp);
            CREATE INDEX IF NOT EXISTS idx_snapshots_token_time ON token_snapshots(token_address, timestamp);

            CREATE INDEX IF NOT EXISTS idx_token_signals_token ON token_signals(token_address);
            CREATE INDEX IF NOT EXISTS idx_token_signals_timestamp ON token_signals(timestamp);
            CREATE INDEX IF NOT EXISTS idx_wallet_trades_wallet ON wallet_trades(wallet_address);
            CREATE INDEX IF NOT EXISTS idx_wallet_trades_token ON wallet_trades(token_address);
            CREATE INDEX IF NOT EXISTS idx_trades_token ON trades(token_address);
            CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);
            CREATE INDEX IF NOT EXISTS idx_alerts_timestamp ON alerts(timestamp);
            CREATE INDEX IF NOT EXISTS idx_regimes_timestamp ON regimes(timestamp);
        ",
        )?;
        Ok(())
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

    pub fn journal_mode(&self) -> Result<String> {
        let conn = self.conn.lock().unwrap();
        let mode: String = conn.pragma_query_value(None, "journal_mode", |row| row.get(0))?;
        Ok(mode)
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

    pub fn add_to_watchlist(&self, address: &str, classification: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = chrono::Utc::now().timestamp();
        conn.execute(
            "INSERT OR REPLACE INTO watchlist (token_address, classification, added_at, last_checked, active) \
             VALUES (?1, ?2, ?3, ?4, 1)",
            params![address, classification, now, 0],
        )?;
        Ok(())
    }

    pub fn get_watchlist_due(&self, check_interval_seconds: i64, limit: usize) -> Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let cutoff = chrono::Utc::now().timestamp() - check_interval_seconds;
        let mut stmt = conn.prepare(
            "SELECT token_address FROM watchlist \
             WHERE active = 1 AND last_checked < ?1 \
             ORDER BY last_checked ASC LIMIT ?2"
        )?;
        let addrs = stmt.query_map(params![cutoff, limit], |row| row.get(0))?
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

    pub fn deactivate_watchlist(&self, address: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE watchlist SET active = 0 WHERE token_address = ?1",
            params![address],
        )?;
        Ok(())
    }

    // -- Snapshot tracking (film, not frames) --

    pub fn insert_snapshot(&self, snap: &TokenSnapshot) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO token_snapshots (token_address, top_holder_pct, top5_pct, top10_pct, \
             holder_count, tx_rate, velocity, momentum, distribution, spring, classification, \
             confidence, timestamp) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
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
            ],
        )?;
        Ok(())
    }

    pub fn get_previous_snapshot(&self, address: &str) -> Result<Option<TokenSnapshot>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT token_address, top_holder_pct, top5_pct, top10_pct, holder_count, \
             tx_rate, velocity, momentum, distribution, spring, classification, confidence, timestamp \
             FROM token_snapshots WHERE token_address = ?1 ORDER BY timestamp DESC LIMIT 1"
        )?;
        let snap = stmt.query_row(params![address], |row| {
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
            })
        }).optional()?;
        Ok(snap)
    }

    pub fn get_snapshot_history(&self, address: &str, limit: usize) -> Result<Vec<TokenSnapshot>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT token_address, top_holder_pct, top5_pct, top10_pct, holder_count, \
             tx_rate, velocity, momentum, distribution, spring, classification, confidence, timestamp \
             FROM token_snapshots WHERE token_address = ?1 ORDER BY timestamp DESC LIMIT ?2"
        )?;
        let snaps = stmt.query_map(params![address, limit], |row| {
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
            })
        })?.collect::<Result<Vec<_>, _>>()?;
        Ok(snaps)
    }

    pub fn compute_delta(&self, address: &str, current: &TokenSnapshot) -> Result<Option<TokenDelta>> {
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

#[derive(Debug, Clone, serde::Serialize)]
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
