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
}
