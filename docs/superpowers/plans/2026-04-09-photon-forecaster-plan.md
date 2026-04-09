# Photon Signal Forecaster Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a working Rust binary that ingests Solana chain data, runs signal analysis, and exposes a 4-tool MCP interface — up to the point where the wallet is ready to fund.

**Architecture:** Single Rust binary, embedded SQLite (WAL mode), async tokio runtime. MCP server over stdio with 4 tools (scan, inspect, trade, status). RPC ingester connects to Solana WebSocket. Signal processors score tokens. Forecaster aggregates confidence.

**Tech Stack:** Rust, tokio, rmcp (MCP SDK), rusqlite (bundled), solana-sdk, solana-client, jupiter-swap-api-client, jito-sdk-rust, reqwest, serde, toml, tracing.

---

## File Structure

```
photon/
├── Cargo.toml
├── config.example.toml
├── src/
│   ├── main.rs
│   ├── config.rs
│   ├── db.rs
│   ├── ingester.rs
│   ├── signals/
│   │   ├── mod.rs
│   │   ├── onchain.rs
│   │   ├── microstructure.rs
│   │   ├── safety.rs
│   │   └── smartmoney.rs
│   ├── forecaster.rs
│   ├── execution.rs
│   └── mcp.rs
├── tests/
│   ├── config_test.rs
│   ├── db_test.rs
│   ├── signals_test.rs
│   ├── forecaster_test.rs
│   └── mcp_test.rs
└── Dockerfile
```

---

### Task 1: Project Scaffold and Config

**Files:**
- Create: `Cargo.toml`
- Create: `src/main.rs`
- Create: `src/config.rs`
- Create: `config.example.toml`
- Test: `tests/config_test.rs`

- [ ] **Step 1: Create Cargo.toml with all dependencies**

```toml
[package]
name = "photon"
version = "0.1.0"
edition = "2021"

[dependencies]
tokio = { version = "1", features = ["full"] }
rmcp = { version = "0.16", features = ["server", "transport-io"] }
rusqlite = { version = "0.32", features = ["bundled"] }
solana-sdk = "2"
solana-client = "2"
solana-transaction-status = "2"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
toml = "0.8"
reqwest = { version = "0.12", features = ["json"] }
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
anyhow = "1"
thiserror = "2"
chrono = { version = "0.4", features = ["serde"] }
bs58 = "0.5"
base64 = "0.22"

[dev-dependencies]
tempfile = "3"
```

- [ ] **Step 2: Create config.example.toml**

```toml
[rpc]
endpoints = ["wss://api.mainnet-beta.solana.com"]
# For production, use Helius/Triton with geyser support

[wallet]
# Public key only - private key loaded from OS keychain
public_key = ""

[risk]
max_position_pct = 15.0
default_position_pct = 0.5
high_confidence_threshold = 80
slippage_bps = 100
priority_fee_lamports = 10000

[tracking]
# Wallet addresses to track for smart money signals
wallets = []
max_active_tokens = 500

[alerts]
confidence_threshold = 70
stale_feed_seconds = 30
```

- [ ] **Step 3: Write failing config test**

```rust
// tests/config_test.rs
use photon::config::Config;
use std::path::PathBuf;

#[test]
fn test_load_config_from_file() {
    let config = Config::load(&PathBuf::from("config.example.toml")).unwrap();
    assert!(!config.rpc.endpoints.is_empty());
    assert_eq!(config.risk.max_position_pct, 15.0);
    assert_eq!(config.risk.high_confidence_threshold, 80);
}

#[test]
fn test_config_default_values() {
    let config = Config::default();
    assert_eq!(config.risk.default_position_pct, 0.5);
    assert_eq!(config.alerts.stale_feed_seconds, 30);
}
```

- [ ] **Step 4: Run test to verify it fails**

Run: `cargo test --test config_test`
Expected: FAIL — module `config` not found

- [ ] **Step 5: Implement config.rs**

```rust
// src/config.rs
use anyhow::Result;
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub rpc: RpcConfig,
    pub wallet: WalletConfig,
    pub risk: RiskConfig,
    pub tracking: TrackingConfig,
    pub alerts: AlertConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RpcConfig {
    pub endpoints: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WalletConfig {
    pub public_key: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RiskConfig {
    pub max_position_pct: f64,
    pub default_position_pct: f64,
    pub high_confidence_threshold: u8,
    pub slippage_bps: u16,
    pub priority_fee_lamports: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TrackingConfig {
    pub wallets: Vec<String>,
    pub max_active_tokens: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AlertConfig {
    pub confidence_threshold: u8,
    pub stale_feed_seconds: u64,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let contents = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&contents)?;
        Ok(config)
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            rpc: RpcConfig {
                endpoints: vec!["wss://api.mainnet-beta.solana.com".to_string()],
            },
            wallet: WalletConfig {
                public_key: String::new(),
            },
            risk: RiskConfig {
                max_position_pct: 15.0,
                default_position_pct: 0.5,
                high_confidence_threshold: 80,
                slippage_bps: 100,
                priority_fee_lamports: 10000,
            },
            tracking: TrackingConfig {
                wallets: vec![],
                max_active_tokens: 500,
            },
            alerts: AlertConfig {
                confidence_threshold: 70,
                stale_feed_seconds: 30,
            },
        }
    }
}
```

- [ ] **Step 6: Create src/main.rs and lib.rs exports**

```rust
// src/main.rs
use anyhow::Result;
use std::path::PathBuf;

mod config;
mod db;
mod ingester;
mod signals;
mod forecaster;
mod execution;
mod mcp;

pub use config::Config;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("photon=info".parse()?),
        )
        .init();

    let config_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("config.toml"));

    let config = Config::load(&config_path)?;
    tracing::info!("Photon Signal Forecaster starting");

    Ok(())
}
```

Note: For tests to access `photon::config`, we need a `src/lib.rs`:

```rust
// src/lib.rs
pub mod config;
pub mod db;
pub mod ingester;
pub mod signals;
pub mod forecaster;
pub mod execution;
pub mod mcp;
```

Create stub modules for everything that `lib.rs` exports so it compiles:

```rust
// src/db.rs — stub
// src/ingester.rs — stub
// src/forecaster.rs — stub
// src/execution.rs — stub
// src/mcp.rs — stub
// src/signals/mod.rs — stub
pub mod onchain;
pub mod microstructure;
pub mod safety;
pub mod smartmoney;

// src/signals/onchain.rs — stub
// src/signals/microstructure.rs — stub
// src/signals/safety.rs — stub
// src/signals/smartmoney.rs — stub
```

- [ ] **Step 7: Run tests, verify pass**

Run: `cargo test --test config_test`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "feat: project scaffold with config parsing"
```

---

### Task 2: Database Schema and Connection

**Files:**
- Modify: `src/db.rs`
- Test: `tests/db_test.rs`

- [ ] **Step 1: Write failing database tests**

```rust
// tests/db_test.rs
use photon::db::Db;
use tempfile::tempdir;

#[test]
fn test_db_initializes_schema() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let db = Db::open(&db_path).unwrap();

    // Verify all tables exist
    let tables = db.list_tables().unwrap();
    assert!(tables.contains(&"tokens".to_string()));
    assert!(tables.contains(&"token_signals".to_string()));
    assert!(tables.contains(&"wallets".to_string()));
    assert!(tables.contains(&"wallet_trades".to_string()));
    assert!(tables.contains(&"trades".to_string()));
    assert!(tables.contains(&"regimes".to_string()));
    assert!(tables.contains(&"alerts".to_string()));
    assert!(tables.contains(&"audit_log".to_string()));
}

#[test]
fn test_db_wal_mode_enabled() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let db = Db::open(&db_path).unwrap();
    assert_eq!(db.journal_mode().unwrap(), "wal");
}

#[test]
fn test_db_insert_and_query_token() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let db = Db::open(&db_path).unwrap();

    db.insert_token("So11111111111111111111111111111111111111112", 100).unwrap();
    let token = db.get_token("So11111111111111111111111111111111111111112").unwrap();
    assert!(token.is_some());
    assert_eq!(token.unwrap().safety_score, 100);
}

#[test]
fn test_audit_log_append_only() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let db = Db::open(&db_path).unwrap();

    db.audit_log("system", "startup", "Photon started").unwrap();
    let logs = db.get_audit_logs(10).unwrap();
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].action, "startup");
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --test db_test`
Expected: FAIL — Db struct not found

- [ ] **Step 3: Implement db.rs**

```rust
// src/db.rs
use anyhow::Result;
use rusqlite::{Connection, params};
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

        let db = Self { conn: Mutex::new(conn) };
        db.create_schema()?;
        Ok(db)
    }

    fn create_schema(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch("
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
        ")?;
        Ok(())
    }

    pub fn list_tables(&self) -> Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )?;
        let tables = stmt.query_map([], |row| row.get(0))?
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
        let mut stmt = conn.prepare(
            "SELECT address, first_seen, safety_score FROM tokens WHERE address = ?1"
        )?;
        let token = stmt.query_row(params![address], |row| {
            Ok(Token {
                address: row.get(0)?,
                first_seen: row.get(1)?,
                safety_score: row.get(2)?,
            })
        }).optional()?;
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
            "SELECT timestamp, actor, action, details FROM audit_log ORDER BY id DESC LIMIT ?1"
        )?;
        let logs = stmt.query_map(params![limit], |row| {
            Ok(AuditEntry {
                timestamp: row.get(0)?,
                actor: row.get(1)?,
                action: row.get(2)?,
                details: row.get(3)?,
            })
        })?.collect::<Result<Vec<_>, _>>()?;
        Ok(logs)
    }
}
```

Note: add `use rusqlite::OptionalExtension;` for the `.optional()` call.

- [ ] **Step 4: Run tests, verify pass**

Run: `cargo test --test db_test`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/db.rs tests/db_test.rs
git commit -m "feat: SQLite database with schema, WAL mode, audit log"
```

---

### Task 3: Signal Types and Trait

**Files:**
- Modify: `src/signals/mod.rs`
- Test: `tests/signals_test.rs`

- [ ] **Step 1: Write failing signal type tests**

```rust
// tests/signals_test.rs
use photon::signals::{Signal, SignalLayer, SignalScore, Confidence};

#[test]
fn test_signal_score_creation() {
    let score = SignalScore::new(SignalLayer::Safety, "honeypot_clear", 90, "Sell simulation passed");
    assert_eq!(score.layer, SignalLayer::Safety);
    assert_eq!(score.score, 90);
}

#[test]
fn test_confidence_from_scores() {
    let scores = vec![
        SignalScore::new(SignalLayer::Safety, "honeypot", 90, ""),
        SignalScore::new(SignalLayer::OnChain, "lp_locked", 80, ""),
        SignalScore::new(SignalLayer::Microstructure, "buy_pressure", 70, ""),
        SignalScore::new(SignalLayer::SmartMoney, "convergence", 85, ""),
    ];
    let confidence = Confidence::from_scores(&scores);
    assert!(confidence.total > 0);
    assert!(confidence.total <= 100);
    assert_eq!(confidence.layer_scores.len(), 4);
}

#[test]
fn test_confidence_degrades_with_missing_layers() {
    let full = vec![
        SignalScore::new(SignalLayer::Safety, "honeypot", 90, ""),
        SignalScore::new(SignalLayer::OnChain, "lp_locked", 80, ""),
        SignalScore::new(SignalLayer::Microstructure, "buy_pressure", 70, ""),
        SignalScore::new(SignalLayer::SmartMoney, "convergence", 85, ""),
    ];
    let partial = vec![
        SignalScore::new(SignalLayer::Safety, "honeypot", 90, ""),
        SignalScore::new(SignalLayer::OnChain, "lp_locked", 80, ""),
    ];
    let full_conf = Confidence::from_scores(&full);
    let partial_conf = Confidence::from_scores(&partial);
    assert!(partial_conf.total < full_conf.total);
    assert_eq!(partial_conf.coverage, 2);
}
```

- [ ] **Step 2: Run tests to verify failure**

Run: `cargo test --test signals_test`
Expected: FAIL

- [ ] **Step 3: Implement signal types in signals/mod.rs**

```rust
// src/signals/mod.rs
pub mod onchain;
pub mod microstructure;
pub mod safety;
pub mod smartmoney;

use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SignalLayer {
    OnChain,
    Microstructure,
    Safety,
    SmartMoney,
}

impl SignalLayer {
    pub fn all() -> &'static [SignalLayer] {
        &[
            SignalLayer::OnChain,
            SignalLayer::Microstructure,
            SignalLayer::Safety,
            SignalLayer::SmartMoney,
        ]
    }

    pub fn weight(&self) -> f64 {
        match self {
            SignalLayer::Safety => 0.30,
            SignalLayer::OnChain => 0.25,
            SignalLayer::Microstructure => 0.25,
            SignalLayer::SmartMoney => 0.20,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalScore {
    pub layer: SignalLayer,
    pub signal_type: String,
    pub score: i32,       // 0-100
    pub details: String,
    pub timestamp: i64,
}

impl SignalScore {
    pub fn new(layer: SignalLayer, signal_type: &str, score: i32, details: &str) -> Self {
        Self {
            layer,
            signal_type: signal_type.to_string(),
            score: score.clamp(0, 100),
            details: details.to_string(),
            timestamp: chrono::Utc::now().timestamp(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Confidence {
    pub total: i32,           // 0-100
    pub coverage: usize,      // how many layers contributed
    pub layer_scores: Vec<(SignalLayer, i32)>,
    pub reasoning: String,
}

impl Confidence {
    pub fn from_scores(scores: &[SignalScore]) -> Self {
        let mut layer_avgs: std::collections::HashMap<SignalLayer, Vec<i32>> =
            std::collections::HashMap::new();

        for s in scores {
            layer_avgs.entry(s.layer).or_default().push(s.score);
        }

        let mut layer_scores = Vec::new();
        let mut weighted_sum = 0.0;
        let mut total_weight = 0.0;

        for layer in SignalLayer::all() {
            if let Some(vals) = layer_avgs.get(layer) {
                let avg = vals.iter().sum::<i32>() as f64 / vals.len() as f64;
                layer_scores.push((*layer, avg as i32));
                weighted_sum += avg * layer.weight();
                total_weight += layer.weight();
            }
        }

        let coverage = layer_scores.len();
        // Penalize missing coverage: scale down if not all layers present
        let coverage_penalty = coverage as f64 / SignalLayer::all().len() as f64;
        let raw_total = if total_weight > 0.0 {
            weighted_sum / total_weight
        } else {
            0.0
        };
        let total = (raw_total * coverage_penalty) as i32;

        Self {
            total: total.clamp(0, 100),
            coverage,
            layer_scores,
            reasoning: format!(
                "{} of {} layers reporting, weighted score {}",
                coverage,
                SignalLayer::all().len(),
                total
            ),
        }
    }
}

/// Trait that all signal processors implement
pub trait Signal: Send + Sync {
    fn name(&self) -> &str;
    fn layer(&self) -> SignalLayer;
}
```

- [ ] **Step 4: Run tests, verify pass**

Run: `cargo test --test signals_test`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/signals/ tests/signals_test.rs
git commit -m "feat: signal types, confidence scoring with coverage penalty"
```

---

### Task 4: Safety Signal Processor

**Files:**
- Modify: `src/signals/safety.rs`
- Add to: `tests/signals_test.rs`

- [ ] **Step 1: Write failing safety signal tests**

Add to `tests/signals_test.rs`:

```rust
use photon::signals::safety::SafetyChecker;

#[test]
fn test_safety_flags_active_mint_authority() {
    let checker = SafetyChecker::new();
    let scores = checker.check_authorities(true, false, false);
    let mint_score = scores.iter().find(|s| s.signal_type == "mint_authority").unwrap();
    assert!(mint_score.score < 30, "Active mint authority should score low");
}

#[test]
fn test_safety_passes_renounced_authorities() {
    let checker = SafetyChecker::new();
    let scores = checker.check_authorities(false, false, false);
    let mint_score = scores.iter().find(|s| s.signal_type == "mint_authority").unwrap();
    assert!(mint_score.score >= 80);
}

#[test]
fn test_safety_flags_permanent_delegate() {
    let checker = SafetyChecker::new();
    let scores = checker.check_authorities(false, false, true);
    let delegate_score = scores.iter().find(|s| s.signal_type == "permanent_delegate").unwrap();
    assert_eq!(delegate_score.score, 0, "Permanent delegate = instant zero");
}

#[test]
fn test_safety_bundled_launch_detection() {
    let checker = SafetyChecker::new();
    // deployer and first buyer are same wallet
    let score = checker.check_bundled_launch("WalletA", &["WalletA", "WalletB", "WalletC"]);
    assert!(score.score < 20);
}

#[test]
fn test_safety_clean_launch() {
    let checker = SafetyChecker::new();
    let score = checker.check_bundled_launch("WalletA", &["WalletB", "WalletC", "WalletD"]);
    assert!(score.score >= 80);
}
```

- [ ] **Step 2: Run tests to verify failure**

Run: `cargo test --test signals_test`
Expected: FAIL

- [ ] **Step 3: Implement safety.rs**

```rust
// src/signals/safety.rs
use super::{Signal, SignalLayer, SignalScore};

pub struct SafetyChecker;

impl SafetyChecker {
    pub fn new() -> Self {
        Self
    }

    pub fn check_authorities(
        &self,
        mint_authority_active: bool,
        freeze_authority_active: bool,
        has_permanent_delegate: bool,
    ) -> Vec<SignalScore> {
        let mut scores = Vec::new();

        scores.push(SignalScore::new(
            SignalLayer::Safety,
            "mint_authority",
            if mint_authority_active { 10 } else { 90 },
            if mint_authority_active {
                "DANGER: Mint authority active — team can print tokens"
            } else {
                "Mint authority renounced"
            },
        ));

        scores.push(SignalScore::new(
            SignalLayer::Safety,
            "freeze_authority",
            if freeze_authority_active { 10 } else { 90 },
            if freeze_authority_active {
                "DANGER: Freeze authority active — team can freeze your account"
            } else {
                "Freeze authority renounced"
            },
        ));

        scores.push(SignalScore::new(
            SignalLayer::Safety,
            "permanent_delegate",
            if has_permanent_delegate { 0 } else { 95 },
            if has_permanent_delegate {
                "CRITICAL: Token-2022 Permanent Delegate — tokens can be burned from your wallet"
            } else {
                "No permanent delegate extension"
            },
        ));

        scores
    }

    pub fn check_bundled_launch(
        &self,
        deployer: &str,
        first_buyers: &[&str],
    ) -> SignalScore {
        let deployer_bought = first_buyers.iter().any(|b| *b == deployer);
        let deployer_in_top3 = first_buyers.iter().take(3).any(|b| *b == deployer);

        let (score, details) = if deployer_in_top3 {
            (5, "CRITICAL: Deployer is among first 3 buyers — likely bundled launch")
        } else if deployer_bought {
            (30, "WARNING: Deployer bought their own token")
        } else {
            (85, "Clean launch — deployer not among early buyers")
        };

        SignalScore::new(SignalLayer::Safety, "bundled_launch", score, details)
    }
}

impl Signal for SafetyChecker {
    fn name(&self) -> &str { "safety" }
    fn layer(&self) -> SignalLayer { SignalLayer::Safety }
}
```

- [ ] **Step 4: Run tests, verify pass**

Run: `cargo test --test signals_test`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/signals/safety.rs tests/signals_test.rs
git commit -m "feat: safety signal processor — authorities, bundled launch detection"
```

---

### Task 5: Forecaster with Regime Detection

**Files:**
- Modify: `src/forecaster.rs`
- Test: `tests/forecaster_test.rs`

- [ ] **Step 1: Write failing forecaster tests**

```rust
// tests/forecaster_test.rs
use photon::forecaster::{Forecaster, Regime};
use photon::signals::{SignalScore, SignalLayer, Confidence};

#[test]
fn test_forecaster_aggregate_scores() {
    let forecaster = Forecaster::new();
    let scores = vec![
        SignalScore::new(SignalLayer::Safety, "honeypot", 90, "clear"),
        SignalScore::new(SignalLayer::Safety, "mint_authority", 90, "renounced"),
        SignalScore::new(SignalLayer::OnChain, "lp_locked", 80, "locked 6mo"),
        SignalScore::new(SignalLayer::Microstructure, "buy_pressure", 75, "2:1 ratio"),
        SignalScore::new(SignalLayer::SmartMoney, "convergence", 85, "3 wallets in"),
    ];
    let confidence = forecaster.aggregate(&scores);
    assert!(confidence.total >= 60);
    assert_eq!(confidence.coverage, 4);
}

#[test]
fn test_forecaster_position_sizing() {
    let forecaster = Forecaster::new();

    assert!(forecaster.position_pct(95, 4) >= 10.0);
    assert!(forecaster.position_pct(95, 4) <= 15.0);

    assert!(forecaster.position_pct(50, 2) <= 2.0);

    assert!(forecaster.position_pct(30, 1) <= 0.5);
}

#[test]
fn test_regime_classification() {
    let forecaster = Forecaster::new();

    // High volume, many new tokens, high buy pressure = launch frenzy
    let regime = forecaster.classify_regime(500.0, 50, 3.0);
    assert_eq!(regime, Regime::LaunchFrenzy);

    // Low volume, few new tokens, balanced pressure = grind
    let regime = forecaster.classify_regime(10.0, 2, 1.1);
    assert_eq!(regime, Regime::LowActivityGrind);
}
```

- [ ] **Step 2: Run tests to verify failure**

Run: `cargo test --test forecaster_test`
Expected: FAIL

- [ ] **Step 3: Implement forecaster.rs**

```rust
// src/forecaster.rs
use crate::signals::{SignalScore, Confidence};
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Regime {
    LaunchFrenzy,
    WhaleAccumulation,
    LowActivityGrind,
    DumpCascade,
}

impl std::fmt::Display for Regime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Regime::LaunchFrenzy => write!(f, "Launch Frenzy"),
            Regime::WhaleAccumulation => write!(f, "Whale Accumulation"),
            Regime::LowActivityGrind => write!(f, "Low Activity Grind"),
            Regime::DumpCascade => write!(f, "Dump Cascade"),
        }
    }
}

pub struct Forecaster;

impl Forecaster {
    pub fn new() -> Self {
        Self
    }

    pub fn aggregate(&self, scores: &[SignalScore]) -> Confidence {
        Confidence::from_scores(scores)
    }

    /// Returns recommended position size as percentage of portfolio
    pub fn position_pct(&self, confidence: i32, coverage: usize) -> f64 {
        let base = match confidence {
            90..=100 => 15.0,
            80..=89 => 10.0,
            70..=79 => 5.0,
            50..=69 => 2.0,
            _ => 0.5,
        };
        // Scale down if not all layers reporting
        let coverage_factor = coverage as f64 / 4.0;
        base * coverage_factor
    }

    /// Classify current market regime based on aggregate metrics
    /// volume_rate: SOL volume per minute across tracked tokens
    /// new_token_count: new tokens in last 10 minutes
    /// buy_sell_ratio: aggregate buy volume / sell volume
    pub fn classify_regime(
        &self,
        volume_rate: f64,
        new_token_count: usize,
        buy_sell_ratio: f64,
    ) -> Regime {
        if volume_rate > 100.0 && new_token_count > 20 && buy_sell_ratio > 2.0 {
            Regime::LaunchFrenzy
        } else if buy_sell_ratio < 0.5 && volume_rate > 50.0 {
            Regime::DumpCascade
        } else if volume_rate > 50.0 && buy_sell_ratio > 1.5 {
            Regime::WhaleAccumulation
        } else {
            Regime::LowActivityGrind
        }
    }
}
```

- [ ] **Step 4: Run tests, verify pass**

Run: `cargo test --test forecaster_test`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/forecaster.rs tests/forecaster_test.rs
git commit -m "feat: forecaster with confidence aggregation, position sizing, regime detection"
```

---

### Task 6: MCP Server with Four Tools

**Files:**
- Modify: `src/mcp.rs`
- Modify: `src/main.rs`
- Test: `tests/mcp_test.rs`

This is the core interface. Each tool implements check → present → confirm → act → verify.

- [ ] **Step 1: Write MCP tool structure tests**

```rust
// tests/mcp_test.rs
use photon::mcp::PhotonServer;
use photon::db::Db;
use photon::config::Config;
use tempfile::tempdir;
use std::sync::Arc;

#[test]
fn test_server_creation() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Db::open(&dir.path().join("test.db")).unwrap());
    let config = Config::default();
    let server = PhotonServer::new(db, config);
    assert!(server.is_healthy());
}
```

- [ ] **Step 2: Run test to verify failure**

Run: `cargo test --test mcp_test`
Expected: FAIL

- [ ] **Step 3: Implement mcp.rs with all four tools**

```rust
// src/mcp.rs
use crate::config::Config;
use crate::db::Db;
use crate::forecaster::{Forecaster, Regime};
use crate::signals::{SignalLayer, SignalScore, Confidence};
use anyhow::Result;
use rmcp::{tool, ServerHandler, model::*};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone)]
pub struct PhotonServer {
    db: Arc<Db>,
    config: Config,
    forecaster: Forecaster,
}

#[derive(Debug, Serialize)]
struct ScanResult {
    healthy: bool,
    regime: String,
    opportunities: Vec<Opportunity>,
    alerts: Vec<AlertInfo>,
}

#[derive(Debug, Serialize)]
struct Opportunity {
    token: String,
    confidence: i32,
    coverage: usize,
    recommended_position_pct: f64,
    reasoning: String,
    layer_scores: Vec<(String, i32)>,
}

#[derive(Debug, Serialize)]
struct AlertInfo {
    alert_type: String,
    message: String,
    confidence: i32,
    timestamp: i64,
}

#[derive(Debug, Serialize)]
struct InspectResult {
    target: String,
    target_type: String,
    safety: Vec<SignalDetail>,
    signals: Vec<SignalDetail>,
    history: Vec<String>,
    risk_rating: String,
}

#[derive(Debug, Serialize)]
struct SignalDetail {
    layer: String,
    signal_type: String,
    score: i32,
    details: String,
}

#[derive(Debug, Serialize)]
struct TradePreview {
    action: String,
    token: String,
    amount_sol: f64,
    estimated_output: String,
    slippage_bps: u16,
    fees: String,
    confidence: i32,
    safety_checks: Vec<String>,
    requires_confirmation: bool,
}

#[derive(Debug, Serialize)]
struct StatusResult {
    system_health: SystemHealth,
    positions: Vec<Position>,
    total_balance_sol: f64,
    total_pnl_sol: f64,
    exposure_pct: f64,
}

#[derive(Debug, Serialize)]
struct SystemHealth {
    rpc_connected: bool,
    db_writable: bool,
    signal_layers_active: usize,
    ingester_lag_seconds: u64,
    data_freshness: String,
}

#[derive(Debug, Serialize)]
struct Position {
    token: String,
    amount_sol_in: f64,
    current_value_sol: f64,
    pnl_sol: f64,
    pnl_pct: f64,
    confidence_at_entry: i32,
}

impl PhotonServer {
    pub fn new(db: Arc<Db>, config: Config) -> Self {
        Self {
            db,
            config,
            forecaster: Forecaster::new(),
        }
    }

    pub fn is_healthy(&self) -> bool {
        self.db.list_tables().is_ok()
    }
}

#[tool(tool_box)]
impl PhotonServer {
    #[tool(description = "Scan the market: system health, current regime, top opportunities, active alerts. Flow: check health → query forecaster → present ranked results.")]
    async fn scan(&self) -> String {
        let result = ScanResult {
            healthy: self.is_healthy(),
            regime: Regime::LowActivityGrind.to_string(),
            opportunities: vec![],
            alerts: vec![],
        };
        serde_json::to_string_pretty(&result).unwrap_or_else(|e| format!("Error: {e}"))
    }

    #[tool(description = "Deep-dive investigation of a token or wallet. Flow: check existence → run all signal layers → pull history → safety checks → present full picture.")]
    async fn inspect(
        &self,
        #[tool(param, description = "Token mint address or wallet address to investigate")]
        address: String,
    ) -> String {
        let target_type = if address.len() > 40 { "token" } else { "wallet" };

        let result = InspectResult {
            target: address.clone(),
            target_type: target_type.to_string(),
            safety: vec![],
            signals: vec![],
            history: vec![],
            risk_rating: "unknown — no data yet".to_string(),
        };
        serde_json::to_string_pretty(&result).unwrap_or_else(|e| format!("Error: {e}"))
    }

    #[tool(description = "Execute a trade with full guardrails. Flow: safety checks → balance check → simulate → preview → WAIT FOR CONFIRMATION → sign → submit via Jito → verify → record.")]
    async fn trade(
        &self,
        #[tool(param, description = "Token mint address")]
        token: String,
        #[tool(param, description = "'buy' or 'sell'")]
        side: String,
        #[tool(param, description = "Amount in SOL (for buys) or token amount (for sells)")]
        amount: f64,
    ) -> String {
        let preview = TradePreview {
            action: side.clone(),
            token: token.clone(),
            amount_sol: amount,
            estimated_output: "Not yet connected to Jupiter".to_string(),
            slippage_bps: self.config.risk.slippage_bps,
            fees: "1% Photon fee + priority fee + Jito tip".to_string(),
            confidence: 0,
            safety_checks: vec!["System not yet connected to Solana RPC".to_string()],
            requires_confirmation: true,
        };
        serde_json::to_string_pretty(&preview).unwrap_or_else(|e| format!("Error: {e}"))
    }

    #[tool(description = "Portfolio status and system health. Flow: check all components → positions with live P&L → exposure vs risk limits → data freshness.")]
    async fn status(&self) -> String {
        let wallet_key = &self.config.wallet.public_key;

        let result = StatusResult {
            system_health: SystemHealth {
                rpc_connected: false,
                db_writable: self.is_healthy(),
                signal_layers_active: 0,
                ingester_lag_seconds: 0,
                data_freshness: "No data yet — awaiting RPC connection".to_string(),
            },
            positions: vec![],
            total_balance_sol: 0.0,
            total_pnl_sol: 0.0,
            exposure_pct: 0.0,
        };

        let mut output = serde_json::to_string_pretty(&result)
            .unwrap_or_else(|e| format!("Error: {e}"));

        if wallet_key.is_empty() {
            output.push_str("\n\n⚠ No wallet configured. Set wallet.public_key in config.toml");
        } else {
            output.push_str(&format!(
                "\n\nWallet: {}\nBalance: 0 SOL — wallet needs funding to begin trading",
                wallet_key
            ));
        }

        output
    }
}

#[tool(tool_box)]
impl ServerHandler for PhotonServer {}
```

- [ ] **Step 4: Wire MCP server into main.rs**

Update `src/main.rs`:

```rust
// src/main.rs
use anyhow::Result;
use std::path::PathBuf;
use std::sync::Arc;
use rmcp::ServiceExt;

mod config;
mod db;
mod ingester;
mod signals;
mod forecaster;
mod execution;
mod mcp;

use config::Config;
use db::Db;
use mcp::PhotonServer;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("photon=info".parse()?),
        )
        .stderr()
        .init();

    let config_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("config.toml"));

    let config = Config::load(&config_path)?;
    tracing::info!("Photon Signal Forecaster starting");

    let db_path = PathBuf::from("photon.db");
    let db = Arc::new(Db::open(&db_path)?);
    db.audit_log("system", "startup", "Photon Signal Forecaster started")?;
    tracing::info!("Database initialized at {:?}", db_path);

    let server = PhotonServer::new(db, config);
    tracing::info!("MCP server starting on stdio");

    let service = server.serve(rmcp::transport::stdio()).await?;
    service.waiting().await?;

    Ok(())
}
```

- [ ] **Step 5: Run test, verify pass**

Run: `cargo test --test mcp_test`
Expected: PASS

- [ ] **Step 6: Build full binary**

Run: `cargo build`
Expected: Compiles successfully

- [ ] **Step 7: Commit**

```bash
git add src/mcp.rs src/main.rs tests/mcp_test.rs
git commit -m "feat: MCP server with scan, inspect, trade, status tools over stdio"
```

---

### Task 7: Config File and Integration Test

**Files:**
- Create: `config.toml` (from example, with empty wallet)
- Modify: `src/lib.rs`

- [ ] **Step 1: Copy example config to live config**

```bash
cp config.example.toml config.toml
```

- [ ] **Step 2: Add .gitignore**

```
/target
photon.db
photon.db-wal
photon.db-shm
config.toml
```

- [ ] **Step 3: Verify cargo build succeeds**

Run: `cargo build --release`
Expected: Compiles

- [ ] **Step 4: Verify cargo test passes**

Run: `cargo test`
Expected: All tests pass

- [ ] **Step 5: Commit**

```bash
git add .gitignore config.example.toml src/lib.rs
git commit -m "feat: gitignore, example config, all tests passing"
```

---

### Task 8: Dockerfile

**Files:**
- Create: `Dockerfile`

- [ ] **Step 1: Create Dockerfile**

```dockerfile
FROM rust:1.82 AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/photon /usr/local/bin/photon
COPY config.example.toml /etc/photon/config.toml
VOLUME ["/data"]
ENV PHOTON_DB_PATH=/data/photon.db
ENTRYPOINT ["photon", "/etc/photon/config.toml"]
```

- [ ] **Step 2: Verify docker build**

Run: `docker build -t photon .`
Expected: Builds successfully

- [ ] **Step 3: Commit**

```bash
git add Dockerfile
git commit -m "feat: Dockerfile for single-container deployment"
```

---

### Task 9: Wire It All Together — End-to-End Smoke Test

**Files:**
- All existing files

- [ ] **Step 1: Run all tests**

Run: `cargo test`
Expected: All pass

- [ ] **Step 2: Build release binary**

Run: `cargo build --release`
Expected: Compiles

- [ ] **Step 3: Test MCP connection with Claude Code**

The binary should be registered in Claude Code's MCP config. Create the config entry:

```json
{
  "mcpServers": {
    "photon": {
      "command": "/path/to/photon/target/release/photon",
      "args": ["/path/to/photon/config.toml"]
    }
  }
}
```

- [ ] **Step 4: Call status tool — should show "wallet needs funding"**

This is the endpoint. The status tool returns:
- System health (DB writable, RPC not connected yet)
- Balance: 0 SOL
- Message: "wallet needs funding to begin trading"

- [ ] **Step 5: Final commit**

```bash
git add -A
git commit -m "feat: Photon Signal Forecaster v0.1 — MCP-ready, awaiting wallet funding"
```
