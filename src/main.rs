use anyhow::Result;
use rmcp::ServiceExt;
use std::path::PathBuf;
use std::sync::Arc;

mod bonding_curve;
mod bot;
mod config;
mod db;
mod discovery;
mod discovery_pollers;
mod execution;
mod forecaster;
mod horizon;
mod image_gen;
mod ingester;
mod intel;
mod launch_forensics;
mod market;
mod mcp;
mod metadata;
mod notifier;
mod publisher;
mod pumpportal;
mod scanner;
mod scout;
mod signals;
mod templates;
mod thought_images;

use config::Config;
use db::Db;
use ingester::{resolve_endpoints, RpcRouter};
use mcp::PhotonServer;
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
            tracing_subscriber::EnvFilter::from_default_env().add_directive("photon=info".parse()?),
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
        tg_cfg.anthropic_api_key = ingester::resolve_env_vars(&tg_cfg.anthropic_api_key);
        tg_cfg.openai_api_key = ingester::resolve_env_vars(&tg_cfg.openai_api_key);
        tg_cfg.claw_api_secret = ingester::resolve_env_vars(&tg_cfg.claw_api_secret);
    }
    if let Some(mp) = config.madapes.as_mut() {
        mp.repo_path = ingester::resolve_env_vars(&mp.repo_path);
        mp.recraft_api_key = ingester::resolve_env_vars(&mp.recraft_api_key);
    }
    tracing::info!("Photon Signal Forecaster starting");

    let db_path = std::env::var("PHOTON_DB_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("photon.db"));
    let db = Arc::new(Db::open(&db_path)?);
    db.audit_log("system", "startup", "Photon Signal Forecaster started")?;
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

    // Trade-execution context. Built once at boot when both PHOTON_PRIVATE_KEY
    // env var is present AND [execution] config block exists. Either missing
    // means photon stays paper-only (no real swaps fire from auto-call paths).
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
                if let Some(exec) = executor_arc.as_ref() {
                    n = n.with_executor(exec.clone());
                }
                let arc = Arc::new(n);
                scanner = scanner.with_notifier(arc.clone());
                if let Some(exec) = executor_arc.as_ref() {
                    scanner = scanner.with_executor(exec.clone());
                }
                notifier_arc = Some(arc);

                // DM bot (long-poll) — started alongside the notifier when dm_enabled.
                if cfg.dm_enabled {
                    match bot::DmBot::new(
                        cfg.clone(),
                        db.clone(),
                        rpc.clone(),
                        notifier_arc.as_ref().unwrap().clone(),
                        14, // call_expiry_days default
                    ) {
                        Ok(b) => {
                            let admins = cfg.admin_user_ids.len();
                            tracing::info!("DM bot enabled (admin_user_ids={})", admins);
                            Arc::new(b).start();
                        }
                        Err(e) => tracing::warn!("DM bot init failed: {} — continuing without", e),
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
    let pp_client = pumpportal::spawn(vec![
        pumpportal::Subscription::NewToken,
        pumpportal::Subscription::Migration,
    ]);
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
                pumpportal::PumpEvent::Raw(value) => {
                    // Unknown event shape — surface loudly so we notice
                    // schema drift and add typed handling.
                    let s = value.to_string();
                    let preview = if s.len() > 240 { &s[..240] } else { &s[..] };
                    tracing::info!(target: "pumpportal::raw", "{}", preview);
                }
            }
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

    // MadApes.ai publisher — pushes data/*.json snapshots to the public repo
    // on a fixed interval. Notes under thoughts/ are left alone (append-only,
    // manual). Non-fatal on failures: a transient git/RPC error doesn't
    // interrupt the main scanner.
    if let Some(mp) = config.madapes.clone() {
        if mp.enabled {
            let pub_instance = Arc::new(publisher::Publisher::new(
                mp.clone(),
                config.wallet.public_key.clone(),
                rpc.clone(),
                db.clone(),
            ));
            pub_instance.spawn(publish_kick.clone());

            // Thought-image processor runs beside the publisher. Only starts
            // when a Recraft key is present — no accidental API burn on
            // local dev without credentials.
            if !mp.recraft_api_key.is_empty() {
                let img_proc = Arc::new(thought_images::ThoughtImageProcessor::new(
                    PathBuf::from(&mp.repo_path),
                    mp.image_interval_seconds,
                    mp.recraft_api_key.clone(),
                ));
                img_proc.spawn();
            }
        } else {
            tracing::info!("MadApes publisher configured but disabled");
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

    let disable_mcp = std::env::var("PHOTON_DISABLE_MCP")
        .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false);
    // Two-mode runtime:
    //   stdio  — legacy parent-as-MCP-client (zeroclaw-spawns-photon).
    //            Foreground; daemon dies. Set via PHOTON_MCP_TRANSPORT=stdio.
    //   http   — bidirectional MCP over Streamable-HTTP (SSE for server→
    //            client streams, POST for client→server JSON-RPC). Runs
    //            alongside the daemon. Default when MCP isn't disabled.
    //   off    — PHOTON_DISABLE_MCP=1; daemon-only.
    let mcp_transport = std::env::var("PHOTON_MCP_TRANSPORT")
        .unwrap_or_else(|_| "http".to_string())
        .to_lowercase();
    let mcp_port: u16 = std::env::var("PHOTON_MCP_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8082);

    if disable_mcp {
        tracing::info!("MCP server disabled by PHOTON_DISABLE_MCP; running daemon-only mode");
        wait_for_shutdown_signal().await?;
    } else if mcp_transport == "stdio" {
        // Foreground stdio mode — kept for backward compat with zeroclaw
        // setups that spawn photon as a child process. NOTE: this exits
        // the daemon (scanner/settling/publisher all stop the moment
        // serve_directly takes over). Don't pick this in production.
        let server = PhotonServer::new(
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
        // PHOTON_MCP_PORT (default 8082) at the `/mcp` path. The
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
                Ok(PhotonServer::new(
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

        let app = axum::Router::new()
            .nest_service("/mcp", mcp_service)
            .route(
                "/health",
                axum::routing::get(|| async { "ok" }),
            );

        let bind_addr = format!("0.0.0.0:{}", mcp_port);
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
