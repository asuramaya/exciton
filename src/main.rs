// Mirror lib.rs allowances; keep the bin and lib lint surfaces aligned.
#![allow(clippy::too_many_arguments)]
#![allow(clippy::type_complexity)]
#![allow(clippy::doc_lazy_continuation)]
#![allow(clippy::doc_overindented_list_items)]
#![allow(clippy::missing_safety_doc)]
#![allow(clippy::stable_sort_primitive)]
#![allow(clippy::unnecessary_sort_by)]
#![allow(clippy::explicit_counter_loop)]
#![allow(clippy::unnecessary_filter_map)]
#![allow(clippy::if_same_then_else)]
#![allow(dead_code)]

use anyhow::Result;
use rmcp::ServiceExt;
use std::path::PathBuf;
use std::sync::Arc;

mod bitquery;
mod bonding_curve;
mod bot;
mod chart_screenshot;
mod config;
mod holders;
mod image_gen;
mod db;
mod discovery;
mod discovery_pollers;
mod execution;
mod forecaster;
mod horizon;
mod ingester;
mod intel;
mod launch_forensics;
mod market;
mod mcp;
mod metadata;
mod notifier;
mod publisher;
mod wallet_cache;
mod pumpportal;
mod scanner;
mod wallet_observer;
mod scout;
mod signals;
mod templates;

use config::Config;
use db::Db;
use ingester::{resolve_endpoints, RpcRouter};
use mcp::ExcitonServer;
use scanner::BackgroundScanner;

async fn wait_for_shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut terminate = signal(SignalKind::terminate())?;
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = terminate.recv() => {},
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await?;
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env().add_directive("exciton=info".parse()?),
        )
        .with_writer(std::io::stderr)
        .init();

    let config_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("config.toml"));

    let mut config = Config::load(&config_path)?;
    if let Some(tg_cfg) = config.telegram.as_mut() {
        tg_cfg.bot_token = ingester::resolve_env_vars(&tg_cfg.bot_token);
        tg_cfg.dm_bot_token = ingester::resolve_env_vars(&tg_cfg.dm_bot_token);
        tg_cfg.signals_chat_id = ingester::resolve_env_vars(&tg_cfg.signals_chat_id);
        tg_cfg.ops_chat_id = ingester::resolve_env_vars(&tg_cfg.ops_chat_id);
        tg_cfg.lounge_chat_id = ingester::resolve_env_vars(&tg_cfg.lounge_chat_id);
        if tg_cfg.lounge_chat_id.trim().is_empty() {
            tg_cfg.lounge_chat_id = tg_cfg.ops_chat_id.clone();
        }
        tg_cfg.anthropic_api_key = ingester::resolve_env_vars(&tg_cfg.anthropic_api_key);
        tg_cfg.openai_api_key = ingester::resolve_env_vars(&tg_cfg.openai_api_key);
        tg_cfg.claw_api_secret = ingester::resolve_env_vars(&tg_cfg.claw_api_secret);
        tg_cfg.evolution_chat_id = ingester::resolve_env_vars(&tg_cfg.evolution_chat_id);
        if tg_cfg.evolution_chat_id.trim().is_empty() {
            tg_cfg.evolution_chat_id = tg_cfg.ops_chat_id.clone();
        }
    }
    if let Some(mp) = config.madapes.as_mut() {
        mp.repo_path = ingester::resolve_env_vars(&mp.repo_path);
        mp.cf_publish_url = ingester::resolve_env_vars(&mp.cf_publish_url);
        mp.cf_publish_secret = ingester::resolve_env_vars(&mp.cf_publish_secret);
        mp.r2_access_key_id = ingester::resolve_env_vars(&mp.r2_access_key_id);
        mp.r2_secret_access_key = ingester::resolve_env_vars(&mp.r2_secret_access_key);
        mp.recraft_api_key = ingester::resolve_env_vars(&mp.recraft_api_key);
    }
    tracing::info!("Exciton starting");

    let db_path = std::env::var("EXCITON_DB_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("exciton.db"));
    let db = Arc::new(Db::open(&db_path)?);
    db.audit_log("system", "startup", "Exciton started")?;
    tracing::info!("Database initialized at {:?}", db_path);

    // Autonomous alert-queue hygiene: every 60s, acknowledge any alert row
    // older than the notifier's STALE_SECS (30 min). Without this, `alerts`
    // grows unbounded whenever the MCP `scan()` tool isn't called, and the
    // UserPromptSubmit hook surfaces yesterday's winners as today's top-3.
    {
        let db_ack = db.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                tick.tick().await;
                match db_ack.acknowledge_stale_alerts(notifier::STALE_SECS) {
                    Ok(n) if n > 0 => {
                        tracing::info!("Auto-acked {} stale alerts (>{}s)", n, notifier::STALE_SECS)
                    }
                    Ok(_) => {}
                    Err(e) => tracing::warn!("acknowledge_stale_alerts failed: {}", e),
                }
            }
        });
    }

    // Smart-wallet auto-promotion. Hourly scan of wallet_observations
    // joined against token_snapshots peaks. Wallets that bought ≥3
    // tokens later running 1.5x with hit-rate ≥40% get inserted into
    // smart_wallets. Closes the dead loop where the observer was
    // collecting trades but nothing promoted them — leaving
    // smart_money_count stuck at 0 in every forensics readout.
    {
        let db_promote = db.clone();
        tokio::spawn(async move {
            // Skip the immediate first tick; let the scanner warm up.
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(3600));
            tick.tick().await;
            loop {
                tick.tick().await;
                let res = tokio::task::spawn_blocking({
                    let db = db_promote.clone();
                    move || db.promote_smart_wallets(3, 5, 1.5, 40.0)
                })
                .await;
                match res {
                    Ok(Ok((considered, promoted))) => {
                        if promoted > 0 || considered > 0 {
                            tracing::info!(
                                "smart_wallet promotion: {} considered, {} promoted",
                                considered,
                                promoted
                            );
                        }
                    }
                    Ok(Err(e)) => tracing::warn!("smart_wallet promotion failed: {}", e),
                    Err(e) => tracing::warn!("smart_wallet promotion panic: {}", e),
                }
            }
        });
    }

    // Resolve env vars in endpoint URLs and create RPC router
    let resolved_endpoints = resolve_endpoints(&config.rpc.endpoints);
    let rpc = Arc::new(RpcRouter::new(&resolved_endpoints)?);
    tracing::info!(
        "RPC router ready: {} endpoints configured",
        rpc.endpoint_count()
    );

    // Test connectivity
    match rpc.check_connection().await {
        Ok(true) => tracing::info!("RPC connection verified"),
        Ok(false) => tracing::warn!("RPC connection check failed — will retry on demand"),
        Err(e) => tracing::warn!("RPC connection error: {} — will retry on demand", e),
    }

    // Start background scanner (with optional Telegram notifier attached)
    let mut scanner = BackgroundScanner::new(
        db.clone(),
        rpc.clone(),
        15,                                        // scan every 15 seconds
        config.alerts.confidence_threshold as i32, // alert threshold from config
        config.tracking.max_active_tokens,         // bound the re-analysis shortlist
    );
    // Shared kick handle: state-changing components (settling phase,
    // auto-call insert, manual /call + /close_call) signal the publisher
    // to run an immediate snapshot via this. Always created so notifier
    // can hold a reference even when no publisher is configured (kick
    // becomes a no-op).
    let publish_kick: publisher::PublishKick = Arc::new(tokio::sync::Notify::new());

    // Trade-execution context. Built once at boot when both EXCITON_PRIVATE_KEY
    // env var is present AND [execution] config block exists. Either missing
    // means exciton stays paper-only (no real swaps fire from auto-call paths).
    // The cfg.enabled flag gates further — present keypair + config but
    // enabled=false means the bundle is constructed for future use but
    // calls.spawn_buy / settle.execute_sell short-circuit.
    let executor_arc: Option<Arc<execution::ExecutionCtx>> = match config.execution.clone() {
        Some(exec_cfg) => {
            match execution::ExecutionCtx::from_env(
                db.clone(),
                rpc.clone(),
                exec_cfg.clone(),
                config.risk.priority_fee_lamports,
                config.risk.jito_tip_lamports,
            ) {
                Ok(ctx) => {
                    tracing::info!(
                        "execution context loaded (enabled={}, wallet={}, daily_cap={:.4} SOL, max_open={})",
                        exec_cfg.enabled,
                        ctx.wallet_pubkey,
                        exec_cfg.max_daily_position_sol,
                        exec_cfg.max_simultaneous_positions
                    );
                    if !exec_cfg.enabled {
                        tracing::warn!("execution disabled in config — paper-only mode");
                    }
                    Some(Arc::new(ctx))
                }
                Err(e) => {
                    tracing::warn!(
                        "execution context init failed: {} — continuing in paper-only mode",
                        e
                    );
                    None
                }
            }
        }
        None => {
            tracing::info!("[execution] block absent — paper-only mode");
            None
        }
    };

    let mut notifier_arc: Option<Arc<notifier::Notifier>> = None;
    if let Some(tg_cfg) = config.telegram.clone() {
        let cfg = tg_cfg;
        match notifier::Notifier::new(cfg.clone(), db.clone(), Some(publish_kick.clone())) {
            Ok(mut n) => {
                let enabled = cfg.enabled;
                tracing::info!("Telegram notifier configured (enabled={})", enabled);
                let helius_key = config.rpc.helius_api_key();
                if !helius_key.is_empty() {
                    tracing::info!("wallet_observer: Helius key wired (len={})", helius_key.len());
                    n = n.with_helius_api_key(helius_key);
                }
                if let Some(exec) = executor_arc.as_ref() {
                    n = n.with_executor(exec.clone());
                }
                let arc = Arc::new(n);
                scanner = scanner.with_notifier(arc.clone());
                if let Some(exec) = executor_arc.as_ref() {
                    scanner = scanner.with_executor(exec.clone());
                }
                notifier_arc = Some(arc);

                // Telegram bot surfaces — two long-polls, one per surface.
                // Private hosts operator+claw; Public hosts read-only
                // intel + per-user state.
                // Public surface enforces a 1/min global ceiling on
                // RPC-heavy lookup commands; the gate is shared across
                // every public user and persists for the bot's lifetime.
                if cfg.dm_enabled {
                    let public_gate = bot::new_public_lookup_gate();
                    let notifier_clone = notifier_arc.as_ref().unwrap().clone();

                    // Private surface — requires dm_bot_token and at least
                    // one admin. We refuse to fall back to bot_token here
                    // (would 409 on getUpdates with the public bot).
                    if !cfg.dm_bot_token.is_empty() {
                        match bot::DmBot::private(
                            cfg.clone(),
                            db.clone(),
                            rpc.clone(),
                            notifier_clone.clone(),
                            14,
                        ) {
                            Ok(b) => {
                                let admins = cfg.admin_user_ids.len();
                                tracing::info!(
                                    "Private bot enabled (admin_user_ids={})",
                                    admins
                                );
                                Arc::new(b).start();
                            }
                            Err(e) => tracing::warn!(
                                "Private bot init failed: {} — operator surface offline",
                                e
                            ),
                        }
                    } else {
                        tracing::info!(
                            "dm_bot_token empty — private operator bot disabled (set dm_bot_token to enable)"
                        );
                    }

                    // Public surface — bot_token is also the channel poster,
                    // so this adds a long-poll on it. Only ever one process
                    // does getUpdates per token; if you run two exciton
                    // instances against the same bot_token you'll see 409s.
                    if !cfg.bot_token.is_empty() {
                        match bot::DmBot::public(
                            cfg.clone(),
                            db.clone(),
                            rpc.clone(),
                            notifier_clone,
                            14,
                            public_gate,
                        ) {
                            Ok(b) => {
                                tracing::info!("Public bot enabled");
                                Arc::new(b).start();
                            }
                            Err(e) => tracing::warn!(
                                "Public bot init failed: {} — intel surface offline",
                                e
                            ),
                        }
                    }
                }
            }
            Err(e) => tracing::warn!("Telegram notifier init failed: {} — continuing without", e),
        }
    }
    // PumpPortal client — the missing data source. Replaces RPC-based
    // sig-walking for new-token discovery + graduation events. Phase 2 / 2b
    // in the scanner gate on this client's freshness; when stale they fall
    // back to the existing RPC walks. No feature flag — connectivity is
    // the gate.
    // Read smart wallets at boot for AccountTrade subscription. The
    // hourly promotion loop adds new wallets over time; those get picked
    // up on the next exciton restart. PumpPortal's docs only document
    // re-subscribing on connect, not modifying an active subscription —
    // a process restart is the cleanest path for adding new wallets and
    // the promotion cadence is hourly, so the staleness window matches.
    let smart_wallets: Vec<String> = db
        .list_active_smart_wallets()
        .unwrap_or_default()
        .into_iter()
        .map(|(addr, _label)| addr)
        .collect();
    let mut pp_subs = vec![
        pumpportal::Subscription::NewToken,
        pumpportal::Subscription::Migration,
    ];
    if !smart_wallets.is_empty() {
        tracing::info!(
            "pumpportal: subscribing AccountTrade for {} smart wallets",
            smart_wallets.len()
        );
        pp_subs.push(pumpportal::Subscription::AccountTrade(smart_wallets));
    }
    let pp_client = pumpportal::spawn(pp_subs);
    let pp_health = pp_client.health.clone();
    scanner = scanner.with_pumpportal_health(pp_health.clone());
    // Spawn the event sink. New-token events insert into `tokens`
    // (lightweight — defers full analyze_token to Phase 4 reingest
    // where DexScreener data is cheaper than per-token RPC reads).
    // Migration events log raw until 8.4 captures the shape from a
    // real graduation; until then Phase 2b sig-walk continues to
    // provide graduation detection.
    let sink_db = db.clone();
    tokio::spawn(async move {
        let mut events = pp_client.events;
        while let Some(ev) = events.recv().await {
            match ev {
                pumpportal::PumpEvent::NewToken(token) => {
                    // safety_score=0 placeholder — Phase 4 reingest's
                    // analyze_token overwrites with the real value.
                    if let Err(e) = sink_db.insert_token(&token.mint, 0) {
                        tracing::warn!("pumpportal-sink: insert_token {} failed: {}", token.mint, e);
                    }
                    // Capture the actual deployer (creator) wallet — the
                    // PumpPortal new-token event carries `traderPublicKey`
                    // which IS the wallet that called the create
                    // instruction on pump.fun. This is the cluster key
                    // for deployer-history scoring. Earlier code stored
                    // the top-1 holder here which is a different concept
                    // (current owner ≠ creator) and produced 100%
                    // one-shot deployer rows in the DB.
                    if let Some(trader) = token.trader_public_key.as_deref() {
                        if !trader.is_empty() {
                            let initial = token.initial_buy.unwrap_or(0.0);
                            let _ = sink_db.set_deployer_if_empty(&token.mint, trader, initial);
                        }
                    }
                    // Audit-log spam removed 2026-04-30: 68k of 130k
                    // audit_log rows were "pumpportal/new_token" entries
                    // from this hot path (~30/min). The DB tokens table
                    // is the canonical record of new mints; audit_log
                    // is reserved for state changes and human/MCP actions.
                }
                pumpportal::PumpEvent::Migration(m) => {
                    // The token just graduated to an AMM (pump-amm or
                    // raydium). Add it to the watchlist so Phase 1's
                    // re-analysis loop picks it up immediately with
                    // post-graduation DexScreener pricing — no need
                    // to wait for Phase 4 reingest's stale-discovered
                    // poll. STAIRCASE is a placeholder; Phase 1's
                    // first read overwrites with the real class.
                    let pool = m.pool.as_deref().unwrap_or("?");
                    if let Err(e) = sink_db.add_to_watchlist(&m.mint, "STAIRCASE") {
                        tracing::warn!(
                            "pumpportal-sink: add_to_watchlist {} failed: {}",
                            m.mint, e
                        );
                    }
                    let _ = sink_db.audit_log(
                        "pumpportal",
                        "migration",
                        &format!("{} pool={}", m.mint, pool),
                    );
                    tracing::info!(
                        "pumpportal: migration {} → {} (sig {})",
                        m.mint,
                        pool,
                        m.signature.as_deref().unwrap_or("?")
                    );
                }
                pumpportal::PumpEvent::AccountTrade(t) => {
                    // A promoted smart wallet just traded. Two leverage
                    // points: (1) seed the mint into the watchlist if
                    // we haven't seen it yet — smart-wallet entry is
                    // strong "look here" signal for fresh mints; (2)
                    // record the buy into wallet_observations so the
                    // promotion loop keeps scoring the wallet's
                    // ongoing hit rate. Sells just log; selling
                    // signals are weaker and noisy until we score
                    // peak-vs-sell-price properly.
                    let trader = t.trader_public_key.as_deref().unwrap_or("?");
                    let kind = t.tx_type.as_deref().unwrap_or("?");
                    let sol = t.sol_amount.unwrap_or(0.0);
                    if t.tx_type.as_deref() == Some("buy") {
                        let _ = sink_db.insert_token(&t.mint, 0);
                        let _ = sink_db.add_to_watchlist(&t.mint, "DEVELOPING");
                        if !trader.is_empty() && trader != "?" {
                            let _ = sink_db.insert_wallet_observation(
                                trader,
                                &t.mint,
                                0,
                                chrono::Utc::now().timestamp(),
                                sol,
                            );
                        }
                    }
                    tracing::info!(
                        "pumpportal: smart-wallet {} {} {} ({:.3} SOL)",
                        trader,
                        kind,
                        t.mint,
                        sol
                    );
                }
                pumpportal::PumpEvent::Raw(value) => {
                    // Unknown event shape — surface loudly so we notice
                    // schema drift and add typed handling.
                    let s = value.to_string();
                    let preview = if s.len() > 240 { &s[..240] } else { &s[..] };
                    tracing::info!(target: "pumpportal::raw", "{}", preview);
                }
            }
        }
    });

    // Bitquery streaming — second mint-discovery feed, redundancy for
    // PumpPortal drops. Uses a different vendor with different gaps,
    // so a missed event on one is likely covered by the other. Emits
    // the same downstream contract as PumpPortal sink: insert into
    // `tokens` and seed watchlist. No-op when BITQUERY_API_TOKEN is
    // unset.
    let bq_sink_db = db.clone();
    let mut bq_client = bitquery::spawn();
    tokio::spawn(async move {
        while let Some(ev) = bq_client.events.recv().await {
            if let Err(e) = bq_sink_db.insert_token(&ev.mint, 0) {
                tracing::warn!(
                    "bitquery-sink: insert_token {} failed: {}",
                    ev.mint,
                    e
                );
                continue;
            }
            if let Err(e) = bq_sink_db.add_to_watchlist(&ev.mint, "DEVELOPING") {
                tracing::warn!(
                    "bitquery-sink: add_to_watchlist {} failed: {}",
                    ev.mint,
                    e
                );
            }
            tracing::debug!(
                "bitquery: trade {} ({}) sig {}",
                ev.mint,
                ev.symbol.as_deref().unwrap_or("?"),
                ev.signature.as_deref().unwrap_or("?")
            );
        }
        tracing::info!("pumpportal-sink: event stream ended");
    });

    let scanner_handle = scanner.start();
    tracing::info!("Background scanner started");

    // Free-tier discovery pollers — broadens coverage beyond PumpPortal
    // (pump.fun-only). DexScreener round-robin across token-profiles +
    // token-boosts catches non-pump.fun launchpads (Raydium-direct,
    // Moonshot, Believe) and any pump.fun events PumpPortal dropped.
    // 3 HTTPS calls/min total, no auth, no key.
    discovery_pollers::DiscoveryPoller::new(db.clone()).spawn();

    // Publisher — pushes data/*.json snapshots to a target git repo
    // on a fixed interval. Notes under thoughts/ are left alone (append-only,
    // manual). Non-fatal on failures: a transient git/RPC error doesn't
    // interrupt the main scanner.
    if let Some(mp) = config.madapes.clone() {
        if mp.enabled {
            // Wallet snapshot cache — refreshed ambient by a slow loop.
            // Reserves Solana RPC budget for the scanner/scout decision
            // path; the publisher reads from this cache and never blocks
            // on RPC for our own wallet state.
            let wallet_cache = wallet_cache::new_cache();
            wallet_cache::spawn_refresh(
                wallet_cache.clone(),
                rpc.clone(),
                config.wallet.public_key.clone(),
                300, // 5 min default — staleness here just delays the
                     // page's bag/featured holding values, never drops a
                     // publisher tick
            );
            let pub_instance = Arc::new(publisher::Publisher::new(
                mp.clone(),
                config.wallet.public_key.clone(),
                rpc.clone(),
                db.clone(),
                wallet_cache,
            ));
            pub_instance.clone().spawn(publish_kick.clone());
            // Scout loop — per-call evidence bundles, whale traces, and
            // call detail JSON. Off the publisher critical path so RPC
            // degradation only causes scout staleness, never publish drops.
            pub_instance.clone().spawn_scout_loop();
            // Image-gen loop runs decoupled from the publisher tick so a
            // 30s Recraft render never threatens the 60s publisher budget.
            // Self-disables if any R2/Recraft credential is empty.
            image_gen::spawn(Arc::new(mp.clone()));
        } else {
            tracing::info!("publisher configured but disabled");
        }
    }

    // Claw HTTP API — POST /api/claw for the website chat widget.
    // Only starts when claw_api_secret is configured.
    if let Some(tg_cfg) = config.telegram.as_ref() {
        if !tg_cfg.claw_api_secret.is_empty() {
            let port = tg_cfg.claw_api_port;
            let secret = tg_cfg.claw_api_secret.clone();
            let api_key = tg_cfg.anthropic_api_key.clone();
            let openai_key = tg_cfg.openai_api_key.clone();
            let db_api = db.clone();
            tokio::spawn(async move {
                if let Err(e) = crate::bot::serve_claw_api(port, secret, api_key, openai_key, db_api).await {
                    tracing::warn!("Claw HTTP API error: {}", e);
                }
            });
            tracing::info!("Claw HTTP API starting on port {}", port);
        }
    }

    let disable_mcp = std::env::var("EXCITON_DISABLE_MCP")
        .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false);
    // Two-mode runtime:
    //   stdio  — legacy parent-as-MCP-client (zeroclaw-spawns-exciton).
    //            Foreground; daemon dies. Set via PHOTON_MCP_TRANSPORT=stdio.
    //   http   — bidirectional MCP over Streamable-HTTP (SSE for server→
    //            client streams, POST for client→server JSON-RPC). Runs
    //            alongside the daemon. Default when MCP isn't disabled.
    //   off    — EXCITON_DISABLE_MCP=1; daemon-only.
    let mcp_transport = std::env::var("PHOTON_MCP_TRANSPORT")
        .unwrap_or_else(|_| "http".to_string())
        .to_lowercase();
    let mcp_port: u16 = std::env::var("EXCITON_MCP_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8082);

    if disable_mcp {
        tracing::info!("MCP server disabled by EXCITON_DISABLE_MCP; running daemon-only mode");
        wait_for_shutdown_signal().await?;
    } else if mcp_transport == "stdio" {
        // Foreground stdio mode — kept for backward compat with zeroclaw
        // setups that spawn exciton as a child process. NOTE: this exits
        // the daemon (scanner/settling/publisher all stop the moment
        // serve_directly takes over). Don't pick this in production.
        let server = ExcitonServer::new(
            db,
            config,
            rpc,
            resolved_endpoints,
            notifier_arc.clone(),
        );
        tracing::warn!("MCP transport=stdio — daemon will halt; use 'http' for production");
        let service = server.serve(rmcp::transport::stdio()).await?;
        service.waiting().await?;
    } else {
        // HTTP mode — runs the MCP server as an axum service on
        // EXCITON_MCP_PORT (default 8082) at the `/mcp` path. The
        // daemon (scanner/settling/publisher) keeps running because
        // we spawn the listener as a background task and then wait
        // for shutdown signal as before.
        let server_db = db.clone();
        let server_config = config.clone();
        let server_rpc = rpc.clone();
        let server_endpoints = resolved_endpoints.clone();
        let server_notifier = notifier_arc.clone();
        let mcp_service = rmcp::transport::streamable_http_server::tower::StreamableHttpService::new(
            move || {
                Ok(ExcitonServer::new(
                    server_db.clone(),
                    server_config.clone(),
                    server_rpc.clone(),
                    server_endpoints.clone(),
                    server_notifier.clone(),
                ))
            },
            std::sync::Arc::new(
                rmcp::transport::streamable_http_server::session::local::LocalSessionManager::default(),
            ),
            rmcp::transport::streamable_http_server::tower::StreamableHttpServerConfig::default(),
        );

        // Optional bearer-token gate. When EXCITON_MCP_TOKEN is set,
        // every /mcp request must carry `Authorization: Bearer <token>`
        // matching it (constant-time compared). When unset, the
        // service runs unauthenticated — only safe behind a loopback
        // bind or trusted Docker network. /health bypasses the gate
        // so monitoring stays simple.
        let mcp_token = std::env::var("EXCITON_MCP_TOKEN").unwrap_or_default();
        if mcp_token.is_empty() {
            tracing::warn!(
                "MCP bearer auth DISABLED — set EXCITON_MCP_TOKEN to require Authorization header"
            );
        } else {
            tracing::info!("MCP bearer auth enabled");
        }
        let mcp_token_arc = std::sync::Arc::new(mcp_token);
        let auth_layer = {
            let token = mcp_token_arc.clone();
            axum::middleware::from_fn(move |req: axum::extract::Request, next: axum::middleware::Next| {
                let token = token.clone();
                async move {
                    if token.is_empty() {
                        return Ok(next.run(req).await);
                    }
                    let header = req
                        .headers()
                        .get("authorization")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or_default();
                    let presented = header.strip_prefix("Bearer ").unwrap_or("");
                    if !ct_eq_bytes(presented.as_bytes(), token.as_bytes()) {
                        return Err(axum::http::StatusCode::UNAUTHORIZED);
                    }
                    Ok(next.run(req).await)
                }
            })
        };

        let app = axum::Router::new()
            .nest_service("/mcp", mcp_service)
            .layer(auth_layer)
            .route(
                "/health",
                axum::routing::get(|| async { "ok" }),
            );

        // Bind to 127.0.0.1 inside the container by default — the host
        // docker-compose `127.0.0.1:8082:8082` mapping prevents external
        // exposure; binding to 0.0.0.0 inside the container would still
        // be reachable from other containers on the docker bridge
        // network. Override via PHOTON_MCP_BIND for setups that need
        // to expose to other containers (set to "0.0.0.0").
        let bind_host = std::env::var("PHOTON_MCP_BIND")
            .unwrap_or_else(|_| "0.0.0.0".to_string());
        let bind_addr = format!("{}:{}", bind_host, mcp_port);
        tracing::info!("MCP server listening on http://{}/mcp (Streamable HTTP)", bind_addr);
        let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
        // axum::serve runs the HTTP server until the future completes.
        // Spawn so the main task can also wait on the shutdown signal
        // and the daemon keeps running concurrently.
        tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, app).await {
                tracing::error!("MCP HTTP server exited: {}", e);
            }
        });
        wait_for_shutdown_signal().await?;
    }

    // Cleanup
    scanner_handle.stop();

    Ok(())
}

/// Constant-time byte comparison for the MCP bearer-token gate.
/// Always walks the longer slice so an attacker can't binary-search
/// the secret via response timing on length or position.
fn ct_eq_bytes(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        let mut acc: u8 = 1;
        let n = a.len().max(b.len());
        for i in 0..n {
            let av = *a.get(i).unwrap_or(&0);
            let bv = *b.get(i).unwrap_or(&0);
            acc |= av ^ bv;
        }
        std::hint::black_box(acc);
        return false;
    }
    let mut acc: u8 = 0;
    for (av, bv) in a.iter().zip(b.iter()) {
        acc |= av ^ bv;
    }
    acc == 0
}
