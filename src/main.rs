use anyhow::Result;
use rmcp::ServiceExt;
use std::path::PathBuf;
use std::sync::Arc;

mod bot;
mod config;
mod db;
mod discovery;
mod execution;
mod forecaster;
mod image_gen;
mod ingester;
mod intel;
mod market;
mod mcp;
mod metadata;
mod notifier;
mod publisher;
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
    let mut notifier_arc: Option<Arc<notifier::Notifier>> = None;
    if let Some(tg_cfg) = config.telegram.clone() {
        let cfg = tg_cfg;
        match notifier::Notifier::new(cfg.clone(), db.clone()) {
            Ok(n) => {
                let enabled = cfg.enabled;
                tracing::info!("Telegram notifier configured (enabled={})", enabled);
                let arc = Arc::new(n);
                scanner = scanner.with_notifier(arc.clone());
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
    let scanner_handle = scanner.start();
    tracing::info!("Background scanner started");

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
            pub_instance.spawn();

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

    if disable_mcp {
        tracing::info!("MCP server disabled by PHOTON_DISABLE_MCP; running daemon-only mode");
        wait_for_shutdown_signal().await?;
    } else {
        // Start MCP server
        let server = PhotonServer::new(db, config, rpc, resolved_endpoints, notifier_arc.clone());
        tracing::info!("MCP server starting on stdio");

        let service = server.serve(rmcp::transport::stdio()).await?;
        service.waiting().await?;
    }

    // Cleanup
    scanner_handle.stop();

    Ok(())
}
