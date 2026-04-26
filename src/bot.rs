//! Direct-message bot — long-poll interface for interactive commands.
//!
//! Feels like a Telegram bot, not an AI. Users DM @MadApesAIBot, type slash
//! commands, get structured responses. Multi-user by default; per-user state
//! (watchlist, mutes) persisted in SQLite. Admin tier gated via config.
//!
//! Architecture: single tokio task polls getUpdates with long timeout, dispatches
//! each incoming update to a command handler. Handlers reuse notifier/templates
//! rendering — single source of truth for message formatting.

use crate::config::TelegramConfig;
use crate::db::Db;
use crate::ingester::RpcRouter;
use crate::metadata;
use crate::notifier::Notifier;
use crate::signals;
use crate::templates::{self, Template};
use anyhow::{anyhow, Result};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub struct DmBot {
    cfg: TelegramConfig,
    db: Arc<Db>,
    rpc: Arc<RpcRouter>,
    notifier: Arc<Notifier>,
    call_expiry_days: i64,
    http: reqwest::Client,
    running: Arc<AtomicBool>,
}

impl DmBot {
    pub fn new(
        cfg: TelegramConfig,
        db: Arc<Db>,
        rpc: Arc<RpcRouter>,
        notifier: Arc<Notifier>,
        call_expiry_days: i64,
    ) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()?;
        Ok(Self {
            cfg,
            db,
            rpc,
            notifier,
            call_expiry_days,
            http,
            running: Arc::new(AtomicBool::new(false)),
        })
    }

    pub fn start(self: Arc<Self>) {
        self.running.store(true, Ordering::SeqCst);
        let me = self.clone();
        tokio::spawn(async move {
            // Seed admin users from config on startup
            for uid in &me.cfg.admin_user_ids {
                let _ = me.db.set_admin(*uid, true);
            }
            // Drop any webhook so getUpdates works
            let _ = me
                .http
                .post(format!(
                    "https://api.telegram.org/bot{}/deleteWebhook",
                    me.cfg.bot_token
                ))
                .send()
                .await;
            // Register commands with Telegram for client-side autocomplete
            let _ = me.register_commands().await;

            tracing::info!("DM bot started, long-polling @MadApesAIBot");
            me.poll_loop().await;
        });
    }

    async fn register_commands(&self) -> Result<()> {
        let commands = [
            ("help", "List available commands"),
            ("scan", "Queue overview — top 5 by effective confidence"),
            ("inspect", "Deep dive on a token: /inspect <addr>"),
            ("status", "System health, wallet, active signals"),
            ("signals", "Currently active signal cards"),
            ("traps", "Hour's trap report: /traps [hours_ago]"),
            (
                "top",
                "Top tokens in a class: /top staircase|grinder|spring",
            ),
            ("regime", "Current market regime"),
            ("why", "Classification reasoning: /why <addr>"),
            ("safety", "Safety signals only: /safety <addr>"),
            ("watch", "Track a token: /watch <addr> [note]"),
            ("unwatch", "Stop tracking: /unwatch <addr>"),
            ("watchlist", "Your tracked tokens"),
            ("mute", "Silence a token: /mute <addr>"),
            ("unmute", "Unmute a token"),
            ("muted", "Your muted tokens"),
            ("nearmisses", "Recent tokens that almost fired a signal"),
            (
                "watch_wallet",
                "Track a smart wallet: /watch_wallet <addr> [label]",
            ),
            ("unwatch_wallet", "Stop tracking: /unwatch_wallet <addr>"),
            ("wallets", "Your smart-wallet watchlist"),
            (
                "scout",
                "Pull deployer profile + website for a mint: /scout <addr>",
            ),
            ("call", "Fire a public call: /call <mint> [short|long] [note]"),
            (
                "close_call",
                "Close an active call: /close_call <mint> [outcome]",
            ),
            ("calls", "Active calls + recent history"),
            (
                "whales",
                "Trace top-10 holder flow for a mint: /whales <addr>",
            ),
            ("lp", "Check LP lock/burn/program status: /lp <addr>"),
            ("deployer", "Deployer's past launches: /deployer <addr>"),
            (
                "ref_mint",
                "Add a known-winner reference mint: /ref_mint <addr> [label]",
            ),
            ("unref_mint", "Remove: /unref_mint <addr>"),
            ("refs", "List reference mints"),
            ("menu", "Show the quick-action reply keyboard (opt-in)"),
        ];
        let json = serde_json::json!({
            "commands": commands.iter().map(|(c, d)| {
                serde_json::json!({"command": c, "description": d})
            }).collect::<Vec<_>>()
        });
        self.http
            .post(format!(
                "https://api.telegram.org/bot{}/setMyCommands",
                self.cfg.bot_token
            ))
            .json(&json)
            .send()
            .await?;
        Ok(())
    }

    async fn poll_loop(&self) {
        let mut offset: i64 = 0;
        while self.running.load(Ordering::SeqCst) {
            match self.get_updates(offset).await {
                Ok(updates) => {
                    for update in updates {
                        if let Some(id) = update.get("update_id").and_then(|v| v.as_i64()) {
                            offset = id + 1;
                        }
                        let me = self;
                        // Dispatch — tolerate errors; never let one bad update kill the loop
                        if let Err(e) = me.handle_update(&update).await {
                            tracing::warn!("handle_update error: {}", e);
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("getUpdates failed: {} — backing off", e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn get_updates(&self, offset: i64) -> Result<Vec<serde_json::Value>> {
        let url = format!(
            "https://api.telegram.org/bot{}/getUpdates",
            self.cfg.bot_token
        );
        let resp = self
            .http
            .post(&url)
            .form(&[
                ("offset", offset.to_string()),
                ("timeout", "25".to_string()),
                (
                    "allowed_updates",
                    r#"["message","callback_query"]"#.to_string(),
                ),
            ])
            .send()
            .await?;
        let body: serde_json::Value = resp.json().await?;
        if body["ok"].as_bool() != Some(true) {
            return Err(anyhow!("getUpdates returned ok=false: {}", body));
        }
        Ok(body["result"].as_array().cloned().unwrap_or_default())
    }

    async fn handle_update(&self, update: &serde_json::Value) -> Result<()> {
        if let Some(msg) = update.get("message") {
            self.handle_message(msg).await?;
        } else if let Some(cb) = update.get("callback_query") {
            self.handle_callback(cb).await?;
        }
        Ok(())
    }

    async fn handle_message(&self, msg: &serde_json::Value) -> Result<()> {
        let chat_id = msg["chat"]["id"]
            .as_i64()
            .ok_or_else(|| anyhow!("no chat_id"))?;
        let chat_type = msg["chat"]["type"].as_str().unwrap_or("");
        // Only respond in private chats — ignore groups/channels where the bot is a member
        if chat_type != "private" {
            return Ok(());
        }

        let user = msg.get("from").ok_or_else(|| anyhow!("no from"))?;
        let user_id = user["id"].as_i64().ok_or_else(|| anyhow!("no user_id"))?;
        let username = user["username"].as_str();
        let first_name = user["first_name"].as_str();
        let text = msg
            .get("text")
            .and_then(|t| t.as_str())
            .unwrap_or("")
            .trim();

        let first_time = self.db.touch_user(user_id, username, first_name)?;

        // Parse command + args
        let (cmd, args) = parse_command(text);
        let cmd = match cmd {
            Some(c) => c,
            None => {
                self.send(chat_id, "Type /help to see available commands.", None)
                    .await?;
                return Ok(());
            }
        };

        // Rate limit — drop bombs early
        if self.cfg.dm_rate_limit_per_minute > 0 {
            let since = chrono::Utc::now().timestamp() - 60;
            let count = self.db.command_count_since(user_id, since)?;
            if count >= self.cfg.dm_rate_limit_per_minute {
                self.send(
                    chat_id,
                    "⏸ Rate limit reached — try again in a minute.",
                    None,
                )
                .await?;
                return Ok(());
            }
        }

        let start = std::time::Instant::now();
        let is_admin = self.db.is_admin(user_id)?;

        let result = match cmd.as_str() {
            "start" => self.cmd_start(chat_id, first_time).await,
            "help" => self.cmd_help(chat_id, is_admin).await,
            "menu" => self.cmd_menu(chat_id).await,
            "scan" => self.cmd_scan(chat_id).await,
            "status" => self.cmd_status(chat_id).await,
            "regime" => self.cmd_regime(chat_id).await,
            "signals" => self.cmd_signals(chat_id).await,
            "traps" => self.cmd_traps(chat_id, &args).await,
            "top" => self.cmd_top(chat_id, &args).await,
            "inspect" => self.cmd_inspect(chat_id, &args).await,
            "why" => self.cmd_why(chat_id, &args).await,
            "safety" => self.cmd_safety(chat_id, &args).await,
            "watch" => self.cmd_watch(chat_id, user_id, &args).await,
            "unwatch" => self.cmd_unwatch(chat_id, user_id, &args).await,
            "watchlist" => self.cmd_watchlist(chat_id, user_id).await,
            "mute" => self.cmd_mute(chat_id, user_id, &args).await,
            "unmute" => self.cmd_unmute(chat_id, user_id, &args).await,
            "muted" => self.cmd_muted(chat_id, user_id).await,
            "nearmisses" => self.cmd_nearmisses(chat_id).await,
            "call" => self.cmd_call(chat_id, is_admin, &args).await,
            "close_call" => self.cmd_close_call(chat_id, is_admin, &args).await,
            "calls" => self.cmd_list_calls(chat_id).await,
            "watch_wallet" => self.cmd_watch_wallet(chat_id, is_admin, &args).await,
            "unwatch_wallet" => self.cmd_unwatch_wallet(chat_id, is_admin, &args).await,
            "wallets" => self.cmd_list_wallets(chat_id).await,
            "scout" => self.cmd_scout(chat_id, &args).await,
            "whales" => self.cmd_whales(chat_id, &args).await,
            "lp" => self.cmd_lp(chat_id, &args).await,
            "deployer" => self.cmd_deployer_history(chat_id, &args).await,
            "ref_mint" => self.cmd_ref_mint(chat_id, is_admin, &args).await,
            "unref_mint" => self.cmd_unref_mint(chat_id, is_admin, &args).await,
            "refs" => self.cmd_list_refs(chat_id).await,
            // Admin commands
            "halt" => self.cmd_admin_halt(chat_id, is_admin).await,
            "resume" => self.cmd_admin_resume(chat_id, is_admin).await,
            "threshold" => self.cmd_admin_threshold(chat_id, is_admin, &args).await,
            "stats" => self.cmd_admin_stats(chat_id, is_admin).await,
            "claw" => self.cmd_claw(chat_id, username, &args).await,
            other => {
                self.send(
                    chat_id,
                    &format!("Unknown command /{}. Type /help.", html_escape(other)),
                    None,
                )
                .await?;
                Ok(())
            }
        };
        let duration_ms = start.elapsed().as_millis() as i64;
        let _ = self.db.log_command(
            user_id,
            &cmd,
            if args.is_empty() { None } else { Some(&args) },
            duration_ms,
        );
        result
    }

    async fn handle_callback(&self, cb: &serde_json::Value) -> Result<()> {
        // Lightweight callback handler: only support inspect drill-down for now.
        // Callback data shape: "i:<addr>" = inspect that address.
        let user = cb.get("from").ok_or_else(|| anyhow!("no from"))?;
        let user_id = user["id"].as_i64().ok_or_else(|| anyhow!("no user_id"))?;
        let message = cb.get("message").ok_or_else(|| anyhow!("no message"))?;
        let chat_id = message["chat"]["id"]
            .as_i64()
            .ok_or_else(|| anyhow!("no chat_id"))?;
        let data = cb.get("data").and_then(|d| d.as_str()).unwrap_or("");
        let cb_id = cb.get("id").and_then(|d| d.as_str()).unwrap_or("");

        // Acknowledge the callback (removes the loading spinner on the button)
        let _ = self
            .http
            .post(format!(
                "https://api.telegram.org/bot{}/answerCallbackQuery",
                self.cfg.bot_token
            ))
            .form(&[("callback_query_id", cb_id.to_string())])
            .send()
            .await;

        self.db.touch_user(user_id, None, None)?;

        if let Some(addr) = data.strip_prefix("i:") {
            self.cmd_inspect(chat_id, addr).await?;
        } else if let Some(addr) = data.strip_prefix("s:") {
            self.cmd_safety(chat_id, addr).await?;
        } else if let Some(addr) = data.strip_prefix("w:") {
            self.cmd_watch(chat_id, user_id, addr).await?;
        }
        Ok(())
    }

    // -- API wrappers -------------------------------------------------------

    async fn send(&self, chat_id: i64, text: &str, reply_markup: Option<&str>) -> Result<i64> {
        let mut form = vec![
            ("chat_id", chat_id.to_string()),
            ("text", text.to_string()),
            ("parse_mode", "HTML".to_string()),
            (
                "link_preview_options",
                r#"{"is_disabled":true}"#.to_string(),
            ),
        ];
        if let Some(kb) = reply_markup {
            form.push(("reply_markup", kb.to_string()));
        }
        let resp = self
            .http
            .post(format!(
                "https://api.telegram.org/bot{}/sendMessage",
                self.cfg.bot_token
            ))
            .form(&form)
            .send()
            .await?;
        let body: serde_json::Value = resp.json().await?;
        if body["ok"].as_bool() != Some(true) {
            return Err(anyhow!("sendMessage failed: {}", body));
        }
        Ok(body["result"]["message_id"].as_i64().unwrap_or(0))
    }

    // -- Command handlers --------------------------------------------------

    async fn cmd_start(&self, chat_id: i64, first_time: bool) -> Result<()> {
        let greeting = if first_time {
            "🤖 <b>Welcome</b>"
        } else {
            "🤖 <b>Back at it</b>"
        };
        let body = format!(
            "{greeting}\n<i>Why: /start</i>\n\n\
             DM interface to Photon — Solana signal forecaster. Type / to autocomplete.\n\n\
             Quick: /scan · /status · /signals · /traps · /help"
        );
        // Clear any stale persistent reply keyboard from prior versions.
        self.send(chat_id, &body, Some(&clear_keyboard())).await?;
        Ok(())
    }

    async fn cmd_help(&self, chat_id: i64, is_admin: bool) -> Result<()> {
        // Compact help — one line per command, grouped in a blockquote so it
        // stays glanceable and collapsible.
        let mut body = String::from(
            "📖 <b>Commands</b>\n\n\
             <blockquote expandable><b>Query</b>\n\
             /scan · /status · /signals · /regime\n\
             /traps [N] · /top &lt;class&gt;\n\n\
             <b>Token</b>\n\
             /inspect &lt;addr&gt; · /why &lt;addr&gt; · /safety &lt;addr&gt;\n\n\
             <b>State</b>\n\
             /watch &lt;addr&gt; [note] · /unwatch · /watchlist\n\
             /calls · /mute &lt;addr&gt; · /unmute · /muted\n\n\
             <b>UI</b>\n\
             /menu — show quick-action keyboard",
        );
        if is_admin {
            body.push_str(
                "\n\n<b>Admin</b>\n\
                 /call &lt;mint&gt; [short|long] [note]\n\
                 /close_call &lt;mint&gt; [note]\n\
                 /calls — active calls + history\n\
                 /halt · /resume · /threshold &lt;N&gt; · /stats\n\
                 /watch_wallet · /unwatch_wallet · /wallets\n\
                 /ref_mint · /unref_mint · /refs",
            );
        }
        body.push_str("</blockquote>");
        self.send(chat_id, &body, None).await?;
        Ok(())
    }

    async fn cmd_menu(&self, chat_id: i64) -> Result<()> {
        self.send(
            chat_id,
            "Tap any button, then message me freely again.",
            Some(&reply_menu()),
        )
        .await?;
        Ok(())
    }

    async fn cmd_scan(&self, chat_id: i64) -> Result<()> {
        let now = chrono::Utc::now().timestamp();
        let _ = self.db.acknowledge_stale_alerts(1800);
        let pending = self.db.get_signal_alerts(20).unwrap_or_default();
        let total = self.db.pending_alert_count().unwrap_or(0) as usize;
        let ops_count = total.saturating_sub(pending.len());

        if pending.is_empty() {
            let ops_note = if ops_count > 0 {
                format!(" · {} ops alerts", ops_count)
            } else {
                String::new()
            };
            self.send(chat_id, &format!("🔍 No buy signals.{}", ops_note), None)
                .await?;
            return Ok(());
        }

        let ops_suffix = if ops_count > 0 {
            format!(" · {} ops", ops_count)
        } else {
            String::new()
        };
        let mut lines = vec![format!(
            "🔍 <b>{} signals</b> · top 5 by eff{}",
            pending.len(),
            ops_suffix
        )];

        // Score + sort
        let mut scored: Vec<(i32, &crate::db::Alert)> = pending
            .iter()
            .map(|a| {
                let age = (now - a.timestamp).max(0);
                (crate::notifier::effective_confidence(a.confidence, age), a)
            })
            .collect();
        scored.sort_by(|a, b| b.0.cmp(&a.0));

        let mut keyboard_rows: Vec<Vec<serde_json::Value>> = Vec::new();
        for (idx, (eff, alert)) in scored.iter().take(5).enumerate() {
            let addr = alert.token_address.clone().unwrap_or_default();
            let short = if addr.len() > 14 {
                format!("{}…{}", &addr[..6], &addr[addr.len() - 4..])
            } else {
                addr.clone()
            };
            lines.push(format!(
                "{}. <code>{}</code>  eff <b>{}</b> · {}",
                idx + 1,
                short,
                eff,
                html_escape(&alert.alert_type)
            ));
            if !addr.is_empty() {
                keyboard_rows.push(vec![serde_json::json!({
                    "text": format!("{}. inspect", idx + 1),
                    "callback_data": format!("i:{}", addr),
                })]);
            }
        }

        let kb = serde_json::json!({ "inline_keyboard": keyboard_rows }).to_string();
        self.send(chat_id, &lines.join("\n"), Some(&kb)).await?;
        Ok(())
    }

    async fn cmd_status(&self, chat_id: i64) -> Result<()> {
        let rpc_connected = self.rpc.check_connection().await.unwrap_or(false);
        let slot = if rpc_connected {
            self.rpc.get_slot().await.ok()
        } else {
            None
        };
        let pending = self.db.pending_alert_count().unwrap_or(0);
        let signals_active = self.db.active_delivery_count("winners").unwrap_or(0);
        let halted = self.notifier.halted();
        let threshold_override = self.notifier.signal_threshold_override();

        let body = format!(
            "📟 <b>Status</b>\n\
             <b>RPC</b> {} · <b>slot</b> {}\n\
             <b>Queue</b> {} pending · <b>Signals live</b> {}\n\
             <b>Notifier</b> {}\n\
             <b>Threshold</b> {}",
            if rpc_connected {
                "✓ connected"
            } else {
                "✗ disconnected"
            },
            slot.map(|s| s.to_string())
                .unwrap_or_else(|| "?".to_string()),
            pending,
            signals_active,
            if halted {
                "🔴 HALTED"
            } else {
                "🟢 running"
            },
            if threshold_override > 0 {
                format!("{} (override)", threshold_override)
            } else {
                format!(
                    "{} (default)",
                    crate::notifier::SIGNAL_MIN_EFFECTIVE_CONFIDENCE
                )
            },
        );
        self.send(chat_id, &body, None).await?;
        Ok(())
    }

    async fn cmd_regime(&self, chat_id: i64) -> Result<()> {
        let dist = self.db.get_regime_distribution(3600).unwrap_or_default();
        if dist.is_empty() {
            self.send(chat_id, "🌦 <b>Regime</b> — no data yet", None)
                .await?;
            return Ok(());
        }
        let count = |class: &str| -> i64 {
            dist.iter()
                .find(|(c, _)| c == class)
                .map(|(_, n)| *n)
                .unwrap_or(0)
        };
        let staircase = count("STAIRCASE");
        let spring = count("SPRING");
        let grinder = count("GRINDER");
        let developing = count("DEVELOPING");
        let crashing = count("CRASHING");

        let label = if crashing > (staircase + spring + grinder) {
            "📉 Broad Decline"
        } else if staircase + spring > grinder + developing {
            "🚀 Momentum Expansion"
        } else if grinder >= staircase + spring {
            "⚙️ Low Activity Grind"
        } else {
            "🔄 Mixed"
        };

        let mut lines = vec![format!("🌦 <b>{}</b>", label)];
        for (class, n) in &dist {
            lines.push(format!("  {} · {}", n, html_escape(class)));
        }
        self.send(chat_id, &lines.join("\n"), None).await?;
        Ok(())
    }

    async fn cmd_signals(&self, chat_id: i64) -> Result<()> {
        let count = self.db.active_delivery_count("winners").unwrap_or(0);
        let body = if count == 0 {
            "📊 No active signals.".to_string()
        } else {
            format!(
                "📊 <b>{}</b> active signal card(s) — full cards in the signals channel.",
                count
            )
        };
        self.send(chat_id, &body, None).await?;
        Ok(())
    }

    async fn cmd_traps(&self, chat_id: i64, args: &str) -> Result<()> {
        let now = chrono::Utc::now().timestamp();
        let hours_ago: i64 = args.trim().parse().unwrap_or(0);
        let current_hour = now - (now % 3600);
        let target_hour = current_hour - (hours_ago * 3600);
        let hour_label = chrono::DateTime::from_timestamp(target_hour, 0)
            .map(|d| d.format("%H:00 UTC").to_string())
            .unwrap_or_default();

        let traps = self.notifier.render_hour_traps(target_hour, 8).await?;
        if traps.is_empty() {
            self.send(
                chat_id,
                &format!(
                    "💥 <b>Traps — hour {}</b>\n\nNo collapses above severity floor.",
                    hour_label
                ),
                None,
            )
            .await?;
            return Ok(());
        }
        let header = format!(
            "💥 <b>Trap report — hour {hour}</b>\n\
             <i>Ranked by peak→current delta</i>",
            hour = hour_label,
        );
        let body = format!("{}{}", header, traps);
        self.send(chat_id, &body, None).await?;
        Ok(())
    }

    async fn cmd_top(&self, chat_id: i64, args: &str) -> Result<()> {
        let class = args.trim().to_uppercase();
        let class = if class.is_empty() {
            "STAIRCASE".to_string()
        } else {
            class
        };
        let now = chrono::Utc::now().timestamp();
        let pending = self.db.get_pending_alerts(200).unwrap_or_default();

        let mut scored: Vec<(i32, &crate::db::Alert)> = pending
            .iter()
            .filter(|a| a.message.to_uppercase().contains(&class))
            .map(|a| {
                let age = (now - a.timestamp).max(0);
                (crate::notifier::effective_confidence(a.confidence, age), a)
            })
            .collect();
        scored.sort_by(|a, b| b.0.cmp(&a.0));

        if scored.is_empty() {
            self.send(
                chat_id,
                &format!("📈 <b>Top {}</b> — none in queue.", class),
                None,
            )
            .await?;
            return Ok(());
        }
        let mut lines = vec![format!("📈 <b>Top {} · by effective conf</b>", class)];
        let mut kb: Vec<Vec<serde_json::Value>> = Vec::new();
        for (i, (eff, a)) in scored.iter().take(5).enumerate() {
            let addr = a.token_address.clone().unwrap_or_default();
            let short = if addr.len() > 14 {
                format!("{}…{}", &addr[..6], &addr[addr.len() - 4..])
            } else {
                addr.clone()
            };
            lines.push(format!(
                "{}. <code>{}</code>  eff <b>{}</b>",
                i + 1,
                short,
                eff
            ));
            if !addr.is_empty() {
                kb.push(vec![serde_json::json!({
                    "text": format!("{}. inspect", i + 1),
                    "callback_data": format!("i:{}", addr),
                })]);
            }
        }
        let kb_json = serde_json::json!({ "inline_keyboard": kb }).to_string();
        self.send(chat_id, &lines.join("\n"), Some(&kb_json))
            .await?;
        Ok(())
    }

    async fn cmd_inspect(&self, chat_id: i64, addr: &str) -> Result<()> {
        let addr = addr.trim();
        if addr.is_empty() {
            self.send(chat_id, "Usage: /inspect &lt;token_address&gt;", None)
                .await?;
            return Ok(());
        }
        match signals::analyze_token(&self.rpc, addr, Some(&self.db), None).await {
            Ok(analysis) => {
                let meta = metadata::fetch(addr).await.ok().flatten();
                let mut html = templates::render(&analysis, meta.as_ref(), Template::Inspect);
                // Truncate to 4000 chars to stay under Telegram's 4096 cap
                if html.len() > 4000 {
                    html.truncate(4000);
                    html.push_str("\n…[truncated]");
                }

                let kb = serde_json::json!({
                    "inline_keyboard": [[
                        { "text": "🔒 Safety only", "callback_data": format!("s:{}", addr) },
                        { "text": "👁 Watch", "callback_data": format!("w:{}", addr) },
                    ]]
                })
                .to_string();
                self.send(chat_id, &html, Some(&kb)).await?;
            }
            Err(e) => {
                self.send(
                    chat_id,
                    &format!("Inspect failed: {}", html_escape(&e.to_string())),
                    None,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn cmd_why(&self, chat_id: i64, addr: &str) -> Result<()> {
        let addr = addr.trim();
        if addr.is_empty() {
            self.send(chat_id, "Usage: /why &lt;token_address&gt;", None)
                .await?;
            return Ok(());
        }
        match signals::analyze_token(&self.rpc, addr, Some(&self.db), None).await {
            Ok(a) => {
                // Decompose the score: show EXACTLY how total was built from
                // the weighted components plus any distribution bonus.
                let mom_contrib = a.confidence.momentum as f64 * 0.40;
                let dist_contrib = a.confidence.distribution as f64 * 0.30;
                let spring_contrib = a.confidence.spring as f64 * 0.30;
                let bonus_txt = if a.confidence.top_holder_bonus_pct > 0 {
                    format!(
                        " +{}% bonus (top {:.2}%)",
                        a.confidence.top_holder_bonus_pct, a.top_holder_pct
                    )
                } else {
                    String::new()
                };
                let body = format!(
                    "🧭 <code>{addr}</code>\n\
                     <b>{cls}</b> · conf <b>{total}</b> · base {base}{bonus}\n\n\
                     <b>mom</b> {mom} × 0.40 = {mc:.1}\n\
                     <b>dist</b> {d} × 0.30 = {dc:.1}\n\
                     <b>spring</b> {s} × 0.30 = {sc:.1}\n\
                     <b>sum</b> = {sum:.1} → base {base}\n\n\
                     <i>{reason}</i>",
                    addr = html_escape(&a.address),
                    cls = a.confidence.classification,
                    total = a.confidence.total,
                    base = a.confidence.base_total,
                    bonus = bonus_txt,
                    mom = a.confidence.momentum,
                    mc = mom_contrib,
                    d = a.confidence.distribution,
                    dc = dist_contrib,
                    s = a.confidence.spring,
                    sc = spring_contrib,
                    sum = mom_contrib + dist_contrib + spring_contrib,
                    reason = html_escape(&a.confidence.reasoning),
                );
                self.send(chat_id, &body, None).await?;
            }
            Err(e) => {
                self.send(
                    chat_id,
                    &format!("Analysis failed: {}", html_escape(&e.to_string())),
                    None,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn cmd_safety(&self, chat_id: i64, addr: &str) -> Result<()> {
        let addr = addr.trim();
        if addr.is_empty() {
            self.send(chat_id, "Usage: /safety &lt;token_address&gt;", None)
                .await?;
            return Ok(());
        }
        match signals::analyze_token(&self.rpc, addr, Some(&self.db), None).await {
            Ok(a) => {
                let mut lines = vec![format!(
                    "🔒 <b>Safety — {}</b>",
                    if addr.len() > 14 {
                        format!("{}…{}", &addr[..6], &addr[addr.len() - 4..])
                    } else {
                        addr.to_string()
                    }
                )];
                for s in a
                    .scores
                    .iter()
                    .filter(|s| s.layer == signals::SignalLayer::Safety)
                {
                    lines.push(format!(
                        "  [{:2}] {} — <i>{}</i>",
                        s.score,
                        html_escape(&s.signal_type),
                        html_escape(&s.details)
                    ));
                }
                self.send(chat_id, &lines.join("\n"), None).await?;
            }
            Err(e) => {
                self.send(
                    chat_id,
                    &format!("Analysis failed: {}", html_escape(&e.to_string())),
                    None,
                )
                .await?;
            }
        }
        Ok(())
    }

    // User state commands

    async fn cmd_watch(&self, chat_id: i64, user_id: i64, args: &str) -> Result<()> {
        let args = args.trim();
        if args.is_empty() {
            self.send(chat_id, "Usage: /watch &lt;token_address&gt; [note]", None)
                .await?;
            return Ok(());
        }
        let (addr, note) = args
            .split_once(' ')
            .map(|(a, n)| (a, Some(n.trim())))
            .unwrap_or((args, None));
        self.db.add_user_watch(user_id, addr, note)?;
        let n = self.db.get_user_watches(user_id)?.len();
        self.send(
            chat_id,
            &format!(
                "👁 watching <code>{}</code> · {} total",
                html_escape(addr),
                n
            ),
            None,
        )
        .await?;
        Ok(())
    }

    async fn cmd_unwatch(&self, chat_id: i64, user_id: i64, addr: &str) -> Result<()> {
        let addr = addr.trim();
        if addr.is_empty() {
            self.send(chat_id, "Usage: /unwatch &lt;token_address&gt;", None)
                .await?;
            return Ok(());
        }
        let removed = self.db.remove_user_watch(user_id, addr)?;
        let msg = if removed {
            "removed from your watchlist"
        } else {
            "wasn't on your watchlist"
        };
        self.send(
            chat_id,
            &format!("{} — <code>{}</code>", msg, html_escape(addr)),
            None,
        )
        .await?;
        Ok(())
    }

    async fn cmd_watchlist(&self, chat_id: i64, user_id: i64) -> Result<()> {
        let watches = self.db.get_user_watches(user_id)?;
        if watches.is_empty() {
            self.send(
                chat_id,
                "👁 Watchlist empty. Add with <code>/watch &lt;addr&gt;</code>.",
                None,
            )
            .await?;
            return Ok(());
        }
        let mut lines = vec![format!("👁 <b>Watchlist · {} tokens</b>", watches.len())];
        for (addr, note, _) in watches.iter().take(20) {
            let short = if addr.len() > 14 {
                format!("{}…{}", &addr[..6], &addr[addr.len() - 4..])
            } else {
                addr.clone()
            };
            let n = note
                .as_deref()
                .map(|s| format!(" — <i>{}</i>", html_escape(s)))
                .unwrap_or_default();
            lines.push(format!("• <code>{}</code>{}", short, n));
        }
        self.send(chat_id, &lines.join("\n"), None).await?;
        Ok(())
    }

    // -- Public calls ------------------------------------------------------

    // (short_mint helper defined at the bottom of this file — used by the
    //  /call and /calls renderers below)
    // Manual `/call` is admin-only so the Calls channel stays curated.
    // Once fired, the call is immutable until someone explicitly closes it —
    // mirrors to data/calls.json on the next publisher tick.

    async fn cmd_call(&self, chat_id: i64, is_admin: bool, args: &str) -> Result<()> {
        if !is_admin {
            self.send(chat_id, "Admin only.", None).await?;
            return Ok(());
        }
        let args = args.trim();
        if args.is_empty() {
            self.send(
                chat_id,
                "Usage: /call &lt;mint&gt; [short|long] [note]",
                None,
            )
            .await?;
            return Ok(());
        }

        // Parse: /call <mint> [short|long] [free-text note]
        let mut parts = args.splitn(3, ' ');
        let mint = parts.next().unwrap_or("").trim();
        let rest = parts.next().unwrap_or("").trim();
        let (term, note) = match rest.to_lowercase().as_str() {
            "short" | "long" => {
                let t = rest.to_uppercase();
                let n = parts.next().unwrap_or("").trim().to_string();
                (t, n)
            }
            _ => {
                // No explicit term — rest is note, default to SHORT
                let n = if parts.next().is_some() {
                    format!("{} {}", rest, args.splitn(3, ' ').nth(2).unwrap_or(""))
                } else {
                    rest.to_string()
                };
                ("SHORT".to_string(), n)
            }
        };

        if self.db.has_active_call(mint).unwrap_or(false) {
            self.send(
                chat_id,
                &format!(
                    "already have an active call on <code>{}</code> — close it first",
                    html_escape(mint)
                ),
                None,
            )
            .await?;
            return Ok(());
        }

        // Embed horizon in note so the publisher and website can display it.
        let full_note = if note.is_empty() {
            format!("horizon={}", term)
        } else {
            format!("{} · horizon={}", note, term)
        };

        // Pull current market + distribution state to freeze the entry snapshot.
        let market = crate::market::get_market(mint).await.ok().flatten();
        let (sym, mcap, price, liq, dex) = match &market {
            Some(m) => (
                m.symbol.clone(),
                m.mcap_usd,
                m.price_usd,
                m.liquidity_usd,
                m.pair_dex.clone(),
            ),
            None => (String::new(), 0.0, 0.0, 0.0, String::new()),
        };
        // Top-holder % via live RPC — keeps the entry state as real as possible.
        let top_pct = match self.rpc.get_token_largest_accounts(mint).await {
            Ok(holders) if !holders.is_empty() => {
                let supply_ui = self
                    .rpc
                    .get_token_supply(mint)
                    .await
                    .map(|s| s.ui_amount)
                    .unwrap_or(1_000_000_000.0);
                if supply_ui > 0.0 {
                    holders[0].ui_amount / supply_ui * 100.0
                } else {
                    0.0
                }
            }
            _ => 0.0,
        };

        let called_at = chrono::Utc::now().timestamp();
        let inserted = self.db.insert_call(
            mint, &sym, "MANUAL", 0, called_at, mcap, price, liq, top_pct, &dex, &full_note, "dm",
        )?;

        if inserted.is_none() {
            self.send(
                chat_id,
                "call already active for this mint — no change",
                None,
            )
            .await?;
            return Ok(());
        }

        let expiry_days = self.call_expiry_days.max(0);
        let expires_at = if expiry_days > 0 {
            Some(called_at + expiry_days * 86_400)
        } else {
            None
        };
        let _ = self.db.set_call_expiration(mint, expires_at);

        // Post the call card to the public channel.
        let notifier = self.notifier.clone();
        let mint_owned = mint.to_string();
        let note_owned = full_note.clone();
        tokio::spawn(async move {
            if let Err(e) = notifier.fire_call_card(&mint_owned, &note_owned, mcap).await {
                tracing::warn!("fire_call_card failed for {}: {}", mint_owned, e);
            }
        });

        let sym_display = if sym.is_empty() { short_mint(mint) } else { html_escape(&sym) };
        let body = format!(
            "📣 <b>Call fired</b> — ${} · <b>{}</b>\n\
             mcap ${:.0}k · liq ${:.0}k · top {:.1}% · {}\n\
             <a href=\"https://dexscreener.com/solana/{}\">chart</a>\n\
             <i>Posted to channel.</i>",
            sym_display,
            term,
            mcap / 1000.0,
            liq / 1000.0,
            top_pct,
            html_escape(&dex),
            html_escape(mint),
        );
        self.send(chat_id, &body, None).await?;
        Ok(())
    }

    async fn cmd_close_call(&self, chat_id: i64, is_admin: bool, args: &str) -> Result<()> {
        if !is_admin {
            self.send(chat_id, "Admin only.", None).await?;
            return Ok(());
        }
        let args = args.trim();
        if args.is_empty() {
            self.send(
                chat_id,
                "Usage: /close_call &lt;mint&gt; [outcome_note]",
                None,
            )
            .await?;
            return Ok(());
        }
        let (mint, note) = args
            .split_once(' ')
            .map(|(m, n)| (m.trim(), n.trim()))
            .unwrap_or((args, ""));

        let market = crate::market::get_market(mint).await.ok().flatten();
        let exit_price = market.as_ref().map(|m| m.price_usd).unwrap_or(0.0);

        // Fetch the call entry to compute PnL for the exit note.
        let entry_price = self
            .db
            .list_calls(true, 50)
            .unwrap_or_default()
            .into_iter()
            .find(|c| c.mint == mint)
            .map(|c| c.entry_price_usd)
            .unwrap_or(0.0);
        let exit_pct = if entry_price > 0.0 && exit_price > 0.0 {
            Some((exit_price / entry_price - 1.0) * 100.0)
        } else {
            None
        };
        let pnl_str = exit_pct
            .map(|p| format!("{:+.1}%", p))
            .unwrap_or_else(|| "?".to_string());

        let closed = self.db.close_call(mint, exit_price, note)?;
        if !closed {
            self.send(chat_id, "no active call found for that mint", None)
                .await?;
            return Ok(());
        }

        // Update the channel card to show the outcome.
        let notifier = self.notifier.clone();
        let mint_owned = mint.to_string();
        let exit_note = if note.is_empty() {
            format!("closed · {}", pnl_str)
        } else {
            format!("{} · {}", note, pnl_str)
        };
        let exit_note_owned = exit_note.clone();
        tokio::spawn(async move {
            if let Err(e) = notifier.update_call_outcome(&mint_owned, "withdrew", exit_pct, &exit_note_owned).await {
                tracing::warn!("update_call_outcome failed for {}: {}", mint_owned, e);
            }
        });

        self.send(
            chat_id,
            &format!(
                "✓ call closed on <code>{}</code> · {}\n<i>Channel card updated.</i>",
                html_escape(mint),
                html_escape(&exit_note),
            ),
            None,
        )
        .await?;
        Ok(())
    }

    async fn cmd_list_calls(&self, chat_id: i64) -> Result<()> {
        let active = self.db.list_calls(true, 20).unwrap_or_default();
        if active.is_empty() {
            self.send(chat_id, "📣 no active calls", None).await?;
            return Ok(());
        }
        let now = chrono::Utc::now().timestamp();

        // Batch-fetch current market data for all active calls in one request.
        let mints: Vec<&str> = active.iter().map(|c| c.mint.as_str()).collect();
        let markets = crate::market::get_market_batch(&mints).await.unwrap_or_default();

        let mut lines = vec![format!("📣 <b>Active calls · {}</b>", active.len())];
        for c in &active {
            let sym = if c.symbol.is_empty() {
                short_mint(&c.mint)
            } else {
                format!("${}", c.symbol)
            };
            let age_h = (now - c.called_at) / 3600;

            // Parse horizon term from stored note ("horizon=SHORT" / "horizon=LONG")
            let term = if c.note.contains("horizon=LONG") {
                " LONG"
            } else {
                " SHORT"
            };

            let pnl_str = if let Some(m) = markets.get(&c.mint) {
                let current_mcap = m.mcap_usd;
                if c.entry_mcap_usd > 0.0 && current_mcap > 0.0 {
                    let pct = (current_mcap / c.entry_mcap_usd - 1.0) * 100.0;
                    if pct >= 0.0 {
                        format!(" · <b>+{:.0}%</b>", pct)
                    } else {
                        format!(" · <b>{:.0}%</b>", pct)
                    }
                } else {
                    String::new()
                }
            } else {
                String::new()
            };
            lines.push(format!(
                "<b>{}</b>{} · entry ${:.0}k · top {:.1}%{} · {}h",
                html_escape(&sym),
                term,
                c.entry_mcap_usd / 1000.0,
                c.entry_top_holder_pct,
                pnl_str,
                age_h,
            ));
        }
        self.send(chat_id, &lines.join("\n"), None).await?;
        Ok(())
    }

    // -- Smart-wallet tracking --------------------------------------------
    // Admin-gated: smart-wallet `buy` alerts fire into the shared channel,
    // so curation has to stay with operators, not per-user state.

    async fn cmd_watch_wallet(&self, chat_id: i64, is_admin: bool, args: &str) -> Result<()> {
        if !is_admin {
            self.send(chat_id, "Admin only.", None).await?;
            return Ok(());
        }
        let args = args.trim();
        if args.is_empty() {
            self.send(
                chat_id,
                "Usage: /watch_wallet &lt;solana_address&gt; [label]",
                None,
            )
            .await?;
            return Ok(());
        }
        let (addr, label) = args
            .split_once(' ')
            .map(|(a, l)| (a.trim(), l.trim()))
            .unwrap_or((args, ""));
        self.db.add_smart_wallet(addr, label)?;
        let n = self.db.list_active_smart_wallets()?.len();
        let label_html = if label.is_empty() {
            String::new()
        } else {
            format!(" — <i>{}</i>", html_escape(label))
        };
        self.send(
            chat_id,
            &format!(
                "🕵 tracking <code>{}</code>{} · {} total",
                html_escape(addr),
                label_html,
                n
            ),
            None,
        )
        .await?;
        Ok(())
    }

    async fn cmd_unwatch_wallet(&self, chat_id: i64, is_admin: bool, args: &str) -> Result<()> {
        if !is_admin {
            self.send(chat_id, "Admin only.", None).await?;
            return Ok(());
        }
        let addr = args.trim();
        if addr.is_empty() {
            self.send(chat_id, "Usage: /unwatch_wallet &lt;address&gt;", None)
                .await?;
            return Ok(());
        }
        self.db.remove_smart_wallet(addr)?;
        self.send(
            chat_id,
            &format!("stopped tracking <code>{}</code>", html_escape(addr)),
            None,
        )
        .await?;
        Ok(())
    }

    async fn cmd_scout(&self, chat_id: i64, args: &str) -> Result<()> {
        let mint = args.trim();
        if mint.is_empty() {
            self.send(chat_id, "Usage: /scout &lt;mint_address&gt;", None)
                .await?;
            return Ok(());
        }
        let report = match crate::scout::scout(mint, &self.rpc, &self.db).await {
            Ok(r) => r,
            Err(e) => {
                self.send(
                    chat_id,
                    &format!("scout failed: {}", html_escape(&e.to_string())),
                    None,
                )
                .await?;
                return Ok(());
            }
        };

        let mut lines: Vec<String> = Vec::new();
        let short = |a: &str| {
            if a.len() > 12 {
                format!("{}…{}", &a[..6], &a[a.len() - 4..])
            } else {
                a.to_string()
            }
        };

        let header = if !report.symbol.is_empty() {
            format!(
                "🕵 <b>Scout · ${} </b><i>{}</i>",
                html_escape(&report.symbol),
                html_escape(&report.name)
            )
        } else {
            format!(
                "🕵 <b>Scout · <code>{}</code></b>",
                html_escape(&short(&report.mint))
            )
        };
        lines.push(header);
        lines.push(format!(
            "<b>mcap</b> ${:.0}k · <b>liq</b> ${:.0}k · <b>dex</b> {}",
            report.mcap_usd / 1000.0,
            report.liquidity_usd / 1000.0,
            if report.pair_dex.is_empty() {
                "—"
            } else {
                &report.pair_dex
            },
        ));

        match &report.deployer {
            Some(d) => {
                let since = d
                    .seconds_since_last_tx
                    .map(|s| {
                        if s < 3600 {
                            format!("{}m ago", s / 60)
                        } else if s < 86_400 {
                            format!("{}h ago", s / 3600)
                        } else {
                            format!("{}d ago", s / 86_400)
                        }
                    })
                    .unwrap_or_else(|| "never".to_string());
                lines.push(format!(
                    "<b>deployer</b> <code>{}</code> · sold <b>{:.1}%</b> · 24h tx {} · 7d tx {} · last {} · {}",
                    short(&d.deployer_address),
                    d.pct_sold,
                    d.tx_count_24h,
                    d.tx_count_7d,
                    since,
                    if d.active { "active" } else { "dormant" },
                ));
            }
            None => lines.push("<b>deployer</b> not recorded (mint predates tracking)".to_string()),
        }

        if !report.socials.is_empty() {
            let socials: Vec<String> = report
                .socials
                .iter()
                .map(|(t, u)| format!("<a href=\"{}\">{}</a>", html_escape(u), html_escape(t)))
                .collect();
            lines.push(format!("<b>socials</b> {}", socials.join(" · ")));
        }

        if let Some(w) = &report.website {
            let excerpt = if w.text.len() > 1400 {
                format!("{}…", &w.text[..1400])
            } else {
                w.text.clone()
            };
            lines.push(format!(
                "\n<b>website</b> <a href=\"{}\">{}</a> ({})",
                html_escape(&w.url),
                html_escape(&w.url),
                w.status
            ));
            if !excerpt.is_empty() {
                lines.push(format!(
                    "<blockquote expandable>{}</blockquote>",
                    html_escape(&excerpt)
                ));
            }
        } else {
            lines.push("\n<b>website</b> none registered".to_string());
        }

        self.send(chat_id, &lines.join("\n"), None).await?;
        Ok(())
    }

    // -- Deep-scout DM commands --------------------------------------------

    async fn cmd_whales(&self, chat_id: i64, args: &str) -> Result<()> {
        let mint = args.trim();
        if mint.is_empty() {
            self.send(chat_id, "Usage: /whales &lt;mint_address&gt;", None)
                .await?;
            return Ok(());
        }
        self.send(chat_id, "🐋 tracing whales…", None).await?;
        let moves = crate::scout::whale_trace(mint, &self.rpc)
            .await
            .unwrap_or_default();
        if moves.is_empty() {
            self.send(chat_id, "no whales found", None).await?;
            return Ok(());
        }
        let mut lines = vec!["🐋 <b>Top-10 whale flow</b>".to_string()];
        for w in moves {
            let owner_short = if w.owner.is_empty() {
                "?".to_string()
            } else {
                format!("{}…", &w.owner[..8.min(w.owner.len())])
            };
            let tag = match w.action.as_str() {
                "accumulating" => "📈 accum",
                "distributing" => "📉 distrib",
                "idle" => "— idle",
                "owner_unresolved" => "? unresolved",
                _ => "?",
            };
            lines.push(format!(
                "<code>{:>5.2}%</code> {} {} · 24h <b>{:+.0}</b> · 7d <b>{:+.0}</b> · tx24 {} · tx7 {}",
                w.pct_of_supply,
                owner_short,
                tag,
                w.net_change_24h_ui,
                w.net_change_7d_ui,
                w.tx_count_24h,
                w.tx_count_7d,
            ));
        }
        self.send(chat_id, &lines.join("\n"), None).await?;
        Ok(())
    }

    async fn cmd_lp(&self, chat_id: i64, args: &str) -> Result<()> {
        let mint = args.trim();
        if mint.is_empty() {
            self.send(chat_id, "Usage: /lp &lt;mint_address&gt;", None)
                .await?;
            return Ok(());
        }
        let market = crate::market::get_market(mint).await.ok().flatten();
        let Some(m) = market else {
            self.send(chat_id, "no DexScreener pair — can't locate pool", None)
                .await?;
            return Ok(());
        };
        let status = match crate::scout::lp_check(&m.pair_address, &m.pair_dex, &self.rpc).await {
            Ok(s) => s,
            Err(e) => {
                self.send(
                    chat_id,
                    &format!("lp_check failed: {}", html_escape(&e.to_string())),
                    None,
                )
                .await?;
                return Ok(());
            }
        };
        let verdict_icon = match status.verdict.as_str() {
            "burnt" => "🔥",
            "locked" => "🔒",
            "pool_program_native" => "🏛",
            "held_by_wallet" => "⚠️",
            _ => "❓",
        };
        let msg = format!(
            "{} <b>LP · {}</b>\n<b>verdict</b> {}\n<b>detail</b> {}\n<b>lp_mint</b> <code>{}</code>\n<b>supply</b> {:.0} · <b>top holder</b> {:.1}%",
            verdict_icon,
            html_escape(&m.pair_dex),
            html_escape(&status.verdict),
            html_escape(&status.detail),
            html_escape(&status.lp_mint),
            status.lp_supply_ui,
            status.top_lp_holder_pct,
        );
        self.send(chat_id, &msg, None).await?;
        Ok(())
    }

    async fn cmd_deployer_history(&self, chat_id: i64, args: &str) -> Result<()> {
        let mint = args.trim();
        if mint.is_empty() {
            self.send(chat_id, "Usage: /deployer &lt;mint_address&gt;", None)
                .await?;
            return Ok(());
        }
        let Some((deployer, _)) = self.db.get_deployer(mint)? else {
            self.send(
                chat_id,
                "no deployer recorded for this mint (predates tracking)",
                None,
            )
            .await?;
            return Ok(());
        };
        if deployer.is_empty() {
            self.send(chat_id, "deployer field empty", None).await?;
            return Ok(());
        }
        self.send(chat_id, "🛠 pulling deployer track record…", None)
            .await?;
        let launches = crate::scout::deployer_history(&deployer, &self.db)
            .await
            .unwrap_or_default();
        let mut lines = vec![format!(
            "🛠 <b>Deployer</b> <code>{}</code> · {} launches tracked",
            html_escape(&deployer),
            launches.len()
        )];
        for l in launches.iter().take(15) {
            let mcap_str = if l.current_mcap_usd > 0.0 {
                format!("${:.0}k", l.current_mcap_usd / 1000.0)
            } else {
                "—".to_string()
            };
            let liq_str = if l.current_liquidity_usd > 0.0 {
                format!("${:.0}k", l.current_liquidity_usd / 1000.0)
            } else {
                "—".to_string()
            };
            let sym = if l.symbol.is_empty() {
                format!("{}…", &l.mint[..6.min(l.mint.len())])
            } else {
                l.symbol.clone()
            };
            lines.push(format!(
                "• <b>{}</b> mcap {} · liq {} · {}",
                html_escape(&sym),
                mcap_str,
                liq_str,
                html_escape(&l.current_pair_dex)
            ));
        }
        self.send(chat_id, &lines.join("\n"), None).await?;
        Ok(())
    }

    async fn cmd_ref_mint(&self, chat_id: i64, is_admin: bool, args: &str) -> Result<()> {
        if !is_admin {
            self.send(chat_id, "Admin only.", None).await?;
            return Ok(());
        }
        let args = args.trim();
        if args.is_empty() {
            self.send(chat_id, "Usage: /ref_mint &lt;mint&gt; [label]", None)
                .await?;
            return Ok(());
        }
        let (mint, label) = args
            .split_once(' ')
            .map(|(m, l)| (m.trim(), l.trim()))
            .unwrap_or((args, ""));
        self.db.add_reference_mint(mint, label)?;
        let n = self.db.list_reference_mints()?.len();
        self.send(
            chat_id,
            &format!("added ref <code>{}</code> · {} total", html_escape(mint), n),
            None,
        )
        .await?;
        Ok(())
    }

    async fn cmd_unref_mint(&self, chat_id: i64, is_admin: bool, args: &str) -> Result<()> {
        if !is_admin {
            self.send(chat_id, "Admin only.", None).await?;
            return Ok(());
        }
        let mint = args.trim();
        if mint.is_empty() {
            self.send(chat_id, "Usage: /unref_mint &lt;mint&gt;", None)
                .await?;
            return Ok(());
        }
        self.db.remove_reference_mint(mint)?;
        self.send(
            chat_id,
            &format!("removed ref <code>{}</code>", html_escape(mint)),
            None,
        )
        .await?;
        Ok(())
    }

    async fn cmd_list_refs(&self, chat_id: i64) -> Result<()> {
        let refs = self.db.list_reference_mints_with_label()?;
        if refs.is_empty() {
            self.send(
                chat_id,
                "no reference mints. Add with <code>/ref_mint &lt;addr&gt; [label]</code>.",
                None,
            )
            .await?;
            return Ok(());
        }
        let mut lines = vec![format!("🎯 <b>Reference mints · {}</b>", refs.len())];
        for (m, l) in refs.iter().take(30) {
            let short = if m.len() > 14 {
                format!("{}…{}", &m[..6], &m[m.len() - 4..])
            } else {
                m.clone()
            };
            let tag = if l.is_empty() {
                String::new()
            } else {
                format!(" — <i>{}</i>", html_escape(l))
            };
            lines.push(format!("• <code>{}</code>{}", short, tag));
        }
        self.send(chat_id, &lines.join("\n"), None).await?;
        Ok(())
    }

    async fn cmd_list_wallets(&self, chat_id: i64) -> Result<()> {
        let wallets = self.db.list_active_smart_wallets()?;
        if wallets.is_empty() {
            self.send(
                chat_id,
                "🕵 No smart wallets tracked. Add with <code>/watch_wallet &lt;addr&gt; [label]</code>.",
                None,
            )
            .await?;
            return Ok(());
        }
        let mut lines = vec![format!("🕵 <b>Smart wallets · {}</b>", wallets.len())];
        for (addr, label) in wallets.iter().take(30) {
            let short = if addr.len() > 14 {
                format!("{}…{}", &addr[..6], &addr[addr.len() - 4..])
            } else {
                addr.clone()
            };
            let tag = if label.is_empty() {
                String::new()
            } else {
                format!(" — <i>{}</i>", html_escape(label))
            };
            lines.push(format!("• <code>{}</code>{}", short, tag));
        }
        self.send(chat_id, &lines.join("\n"), None).await?;
        Ok(())
    }

    async fn cmd_mute(&self, chat_id: i64, user_id: i64, addr: &str) -> Result<()> {
        let addr = addr.trim();
        if addr.is_empty() {
            self.send(chat_id, "Usage: /mute &lt;token_address&gt;", None)
                .await?;
            return Ok(());
        }
        self.db.add_user_mute(user_id, addr)?;
        self.send(
            chat_id,
            &format!("🔕 muted <code>{}</code>", html_escape(addr)),
            None,
        )
        .await?;
        Ok(())
    }

    async fn cmd_unmute(&self, chat_id: i64, user_id: i64, addr: &str) -> Result<()> {
        let addr = addr.trim();
        if addr.is_empty() {
            self.send(chat_id, "Usage: /unmute &lt;token_address&gt;", None)
                .await?;
            return Ok(());
        }
        let removed = self.db.remove_user_mute(user_id, addr)?;
        let msg = if removed { "unmuted" } else { "wasn't muted" };
        self.send(
            chat_id,
            &format!("{} — <code>{}</code>", msg, html_escape(addr)),
            None,
        )
        .await?;
        Ok(())
    }

    async fn cmd_nearmisses(&self, chat_id: i64) -> Result<()> {
        let rows = self.db.get_recent_near_misses(10).unwrap_or_default();
        if rows.is_empty() {
            self.send(chat_id, "🎯 No recent near-misses.", None)
                .await?;
            return Ok(());
        }
        let mut lines = vec![
            format!("🎯 <b>Near-misses · {} shown</b>", rows.len()),
            "<i>Tokens that qualified on class but failed one gate</i>".to_string(),
        ];
        let now = chrono::Utc::now().timestamp();
        for m in &rows {
            let age = {
                let s = (now - m.timestamp).max(0);
                if s < 3600 {
                    format!("{}m", s / 60)
                } else if s < 86400 {
                    format!("{:.1}h", s as f64 / 3600.0)
                } else {
                    format!("{:.1}d", s as f64 / 86400.0)
                }
            };
            let short = if m.token_address.len() > 14 {
                format!(
                    "{}…{}",
                    &m.token_address[..6],
                    &m.token_address[m.token_address.len() - 4..]
                )
            } else {
                m.token_address.clone()
            };
            lines.push(format!(
                "<code>{short}</code>  <b>{cls}</b>  {gap}  <i>{age} ago</i>",
                short = short,
                cls = m.classification,
                gap = html_escape(&m.gap),
                age = age,
            ));
        }
        self.send(chat_id, &lines.join("\n"), None).await?;
        Ok(())
    }

    async fn cmd_muted(&self, chat_id: i64, user_id: i64) -> Result<()> {
        let mutes = self.db.get_user_mutes(user_id)?;
        if mutes.is_empty() {
            self.send(chat_id, "🔕 No muted tokens.", None).await?;
            return Ok(());
        }
        let mut lines = vec![format!("🔕 <b>Muted · {}</b>", mutes.len())];
        for (addr, _) in mutes.iter().take(20) {
            let short = if addr.len() > 14 {
                format!("{}…{}", &addr[..6], &addr[addr.len() - 4..])
            } else {
                addr.clone()
            };
            lines.push(format!("• <code>{}</code>", short));
        }
        self.send(chat_id, &lines.join("\n"), None).await?;
        Ok(())
    }

    // Admin commands — require is_admin flag on the user record

    async fn cmd_admin_halt(&self, chat_id: i64, is_admin: bool) -> Result<()> {
        if !is_admin {
            self.send(chat_id, "🚫 <i>Admin command — access denied</i>", None)
                .await?;
            return Ok(());
        }
        self.notifier.set_halted(true);
        self.send(chat_id, "🔴 <b>HALTED</b>  <i>alert promotions paused</i>", None)
            .await?;
        Ok(())
    }

    async fn cmd_admin_resume(&self, chat_id: i64, is_admin: bool) -> Result<()> {
        if !is_admin {
            self.send(chat_id, "🚫 <i>Admin command — access denied</i>", None)
                .await?;
            return Ok(());
        }
        self.notifier.set_halted(false);
        self.send(chat_id, "🟢 <b>RUNNING</b>", None).await?;
        Ok(())
    }

    async fn cmd_admin_threshold(&self, chat_id: i64, is_admin: bool, args: &str) -> Result<()> {
        if !is_admin {
            self.send(chat_id, "🚫 <i>Admin command — access denied</i>", None)
                .await?;
            return Ok(());
        }
        let n: i32 = match args.trim().parse() {
            Ok(v) if (0..=100).contains(&v) => v,
            _ => {
                self.send(
                    chat_id,
                    "Usage: /threshold &lt;0-100&gt;  (0 = default)",
                    None,
                )
                .await?;
                return Ok(());
            }
        };
        self.notifier.set_signal_threshold_override(n);
        let desc = if n == 0 {
            format!(
                "reverted to default {}",
                crate::notifier::SIGNAL_MIN_EFFECTIVE_CONFIDENCE
            )
        } else {
            format!("set to {}", n)
        };
        self.send(
            chat_id,
            &format!("🎚 <b>Threshold {}</b>  <i>applies to live signal promotions</i>", desc),
            None,
        )
        .await?;
        Ok(())
    }

    async fn cmd_admin_stats(&self, chat_id: i64, is_admin: bool) -> Result<()> {
        if !is_admin {
            self.send(chat_id, "🚫 <i>Admin command — access denied</i>", None)
                .await?;
            return Ok(());
        }
        let users = self.db.total_users().unwrap_or(0);
        let since = chrono::Utc::now().timestamp() - 86400;
        let cmds_24h = self.db.total_commands_since(since).unwrap_or(0);
        let body = format!(
            "📊 <b>Bot stats</b>\n<b>Users</b> {} · <b>Cmds 24h</b> {}",
            users, cmds_24h
        );
        self.send(chat_id, &body, None).await?;
        Ok(())
    }

    // -- Claw (AI interface) -----------------------------------------------

    async fn cmd_claw(&self, chat_id: i64, username: Option<&str>, args: &str) -> Result<()> {
        let allowed = &self.cfg.claw_username;
        let is_allowed = !allowed.is_empty()
            && username
                .map(|u| u.eq_ignore_ascii_case(allowed.as_str()))
                .unwrap_or(false);
        if !is_allowed {
            // Silent rejection — looks like the command doesn't exist
            self.send(chat_id, "Unknown command /claw. Type /help.", None)
                .await?;
            return Ok(());
        }
        let q = args.trim();
        if q.is_empty() {
            self.send(
                chat_id,
                "What do you want to know?\n\n\
                 <code>/claw what's the market like</code>\n\
                 <code>/claw [CA] break this down</code>\n\
                 <code>/claw what are we holding right now</code>",
                None,
            )
            .await?;
            return Ok(());
        }
        // Typing indicator — non-fatal
        let _ = self
            .http
            .post(format!(
                "https://api.telegram.org/bot{}/sendChatAction",
                self.cfg.bot_token
            ))
            .form(&[
                ("chat_id", chat_id.to_string()),
                ("action", "typing".to_string()),
            ])
            .send()
            .await;

        let ctx = self.build_claw_context(q).await;
        // Primary: route through zeroclaw (ChatGPT OAuth subscription).
        // Falls back to raw API keys when zeroclaw is unreachable.
        let result = match claw_ask_zeroclaw(&self.http, &ctx, q).await {
            Ok(r) => Ok(r),
            Err(e) => {
                tracing::warn!("zeroclaw unavailable: {} — trying API key fallback", e);
                let anthropic_key = &self.cfg.anthropic_api_key;
                let openai_key = &self.cfg.openai_api_key;
                if !anthropic_key.is_empty() {
                    claw_ask(&self.http, anthropic_key, &ctx, q).await
                } else if !openai_key.is_empty() {
                    claw_ask_openai(&self.http, openai_key, &ctx, q).await
                } else {
                    Err(e)
                }
            }
        };
        match result {
            Ok(reply) => {
                self.send(chat_id, &html_escape(&reply), None).await?;
            }
            Err(e) => {
                tracing::warn!("claw: API error: {}", e);
                self.send(
                    chat_id,
                    "🐾 <i>Claw didn't respond — try again in a moment.</i>",
                    None,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn build_claw_context(&self, question: &str) -> String {
        let now = chrono::Utc::now().timestamp();
        let mut ctx = String::new();

        // Active calls
        let calls = self.db.list_calls(true, 10).unwrap_or_default();
        if calls.is_empty() {
            ctx.push_str("ACTIVE CALLS: none\n\n");
        } else {
            ctx.push_str("ACTIVE CALLS:\n");
            for c in &calls {
                let sym = if c.symbol.is_empty() {
                    short_mint(&c.mint)
                } else {
                    format!("${}", c.symbol)
                };
                let age_h = (now - c.called_at) / 3600;
                ctx.push_str(&format!(
                    "  {sym}: entry_mcap=${:.0}k top={:.1}% class={} conf={} age={}h\n",
                    c.entry_mcap_usd / 1000.0,
                    c.entry_top_holder_pct,
                    c.classification,
                    c.confidence,
                    age_h
                ));
                if !c.note.is_empty() {
                    ctx.push_str(&format!("    note: {}\n", c.note));
                }
            }
            ctx.push('\n');
        }

        // Recent call history (last 3 closed)
        let history = self.db.list_calls(false, 20).unwrap_or_default();
        let closed: Vec<_> = history
            .into_iter()
            .filter(|c| c.status != "active")
            .take(3)
            .collect();
        if !closed.is_empty() {
            ctx.push_str("RECENT CLOSED CALLS:\n");
            for c in &closed {
                let sym = if c.symbol.is_empty() {
                    short_mint(&c.mint)
                } else {
                    format!("${}", c.symbol)
                };
                ctx.push_str(&format!(
                    "  {sym}: status={} entry=${:.8} exit=${:.8}\n",
                    c.status,
                    c.entry_price_usd,
                    c.exit_price_usd.unwrap_or(0.0)
                ));
                if let Some(note) = &c.exit_note {
                    ctx.push_str(&format!("    exit: {}\n", note));
                }
            }
            ctx.push('\n');
        }

        // Hot signal queue — buy signals only, no ops noise
        let alerts = self.db.get_signal_alerts(5).unwrap_or_default();
        if !alerts.is_empty() {
            ctx.push_str("PENDING SIGNALS (not yet called):\n");
            for a in &alerts {
                let addr = a.token_address.as_deref().unwrap_or("?");
                ctx.push_str(&format!(
                    "  {} conf={} class={}\n",
                    short_mint(addr),
                    a.confidence,
                    a.alert_type
                ));
            }
            ctx.push('\n');
        }

        // If a Solana CA is embedded in the question, pull live market data
        if let Some(addr) = extract_solana_address(question) {
            if let Ok(Some(meta)) = crate::metadata::fetch(&addr).await {
                ctx.push_str(&format!(
                    "TOKEN: {} (${}) — {}\n",
                    meta.name,
                    meta.symbol,
                    meta.dex_id.as_deref().unwrap_or("unknown dex")
                ));
                if let Some(p) = meta.price_usd {
                    ctx.push_str(&format!("  price: ${:.8}\n", p));
                }
                if let Some(m) = meta.market_cap_usd {
                    ctx.push_str(&format!("  mcap: ${:.1}k\n", m / 1000.0));
                }
                if let Some(l) = meta.liquidity_usd {
                    ctx.push_str(&format!("  liq: ${:.1}k\n", l / 1000.0));
                }
                if let Some(v) = meta.volume_1h_usd {
                    ctx.push_str(&format!("  vol_1h: ${:.1}k\n", v / 1000.0));
                }
                if let Some(c) = meta.price_change_5m {
                    ctx.push_str(&format!("  Δ5m: {:.1}%\n", c));
                }
                if let Some(c) = meta.price_change_1h {
                    ctx.push_str(&format!("  Δ1h: {:.1}%\n", c));
                }
                if let Some(age) = meta.age_human() {
                    ctx.push_str(&format!("  age: {}\n", age));
                }
                if let Some(url) = &meta.pair_url {
                    ctx.push_str(&format!("  pair: {}\n", url));
                }
                ctx.push('\n');
            }
        }

        ctx
    }
}

// -- Claw helpers -----------------------------------------------------------

async fn claw_ask(
    http: &reqwest::Client,
    api_key: &str,
    context: &str,
    question: &str,
) -> Result<String> {
    let system = format!(
        "You are the Claw — the AI embedded in MadApe, an autonomous Solana signal system.\n\
         You scan the chain in real time, classify tokens, and manage open positions called calls.\n\
         You operate in the DeFi trenches. Speak directly. No disclaimers, no hedging.\n\
         Give your actual read on the market, positions, or a specific token.\n\
         Keep it tight — 2 to 4 short paragraphs. Plain text only, no markdown, no asterisks.\n\n\
         Live system state:\n{}",
        context
    );
    let body = serde_json::json!({
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": 600,
        "system": system,
        "messages": [{ "role": "user", "content": question }]
    });
    let resp = http
        .post("https://api.anthropic.com/v1/messages")
        .header("x-api-key", api_key)
        .header("anthropic-version", "2023-06-01")
        .json(&body)
        .send()
        .await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!("Anthropic {} — {}", status, &text[..text.len().min(200)]));
    }
    let data: serde_json::Value = resp.json().await?;
    let text = data["content"][0]["text"]
        .as_str()
        .unwrap_or("(no response)")
        .to_string();
    Ok(text)
}

async fn claw_ask_openai(
    http: &reqwest::Client,
    api_key: &str,
    context: &str,
    question: &str,
) -> Result<String> {
    let system = format!(
        "You are the Claw — the AI embedded in MadApe, an autonomous Solana signal system.\n\
         You scan the chain in real time, classify tokens, and manage open positions called calls.\n\
         You operate in the DeFi trenches. Speak directly. No disclaimers, no hedging.\n\
         Give your actual read on the market, positions, or a specific token.\n\
         Keep it tight — 2 to 4 short paragraphs. Plain text only, no markdown, no asterisks.\n\n\
         Live system state:\n{}",
        context
    );
    let body = serde_json::json!({
        "model": "gpt-4o-mini",
        "max_tokens": 600,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": question}
        ]
    });
    let resp = http
        .post("https://api.openai.com/v1/chat/completions")
        .header("Authorization", format!("Bearer {}", api_key))
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!("OpenAI {} — {}", status, &text[..text.len().min(200)]));
    }
    let data: serde_json::Value = resp.json().await?;
    let text = data["choices"][0]["message"]["content"]
        .as_str()
        .unwrap_or("(no response)")
        .to_string();
    Ok(text.trim().to_string())
}

async fn claw_ask_zeroclaw(http: &reqwest::Client, context: &str, question: &str) -> Result<String> {
    let message = format!(
        "You are the Claw — the AI embedded in MadApe, an autonomous Solana signal system.\n\
         You scan the chain in real time, classify tokens, and manage open positions called calls.\n\
         You operate in the DeFi trenches. Speak directly. No disclaimers, no hedging.\n\
         Give your actual read on the market, positions, or a specific token.\n\
         Keep it tight — 2 to 4 short paragraphs. Plain text only, no markdown, no JSON, no asterisks.\n\
         No tools are available — do NOT generate <tool_call> blocks or any function call syntax.\n\
         Reason from the context below and answer directly in prose.\n\n\
         Live system state:\n{context}\n\
         Question: {question}"
    );
    let body = serde_json::json!({ "message": message });
    let resp = http
        .post("http://zeroclaw:42617/webhook")
        .timeout(std::time::Duration::from_secs(115))
        .json(&body)
        .send()
        .await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!(
            "zeroclaw {} — {}",
            status,
            &text[..text.len().min(200)]
        ));
    }
    let data: serde_json::Value = resp.json().await?;
    let raw = data["response"].as_str().unwrap_or("").to_string();
    let text = strip_tool_blocks(&raw);
    if text.is_empty() {
        return Err(anyhow::anyhow!("zeroclaw returned only tool calls, no prose"));
    }
    Ok(text)
}

/// Strip spontaneous <tool_call>/<tool_result> blocks that gpt-5.4-mini generates
/// even when tools=None — the Codex fine-tune has strong tool-call priors.
fn strip_tool_blocks(text: &str) -> String {
    let tags = [
        ("<tool_call>", "</tool_call>"),
        ("<tool-call>", "</tool-call>"),
        ("<function_calls>", "</function_calls>"),
        ("<tool_result>", "</tool_result>"),
    ];
    let mut s = text.trim().to_string();
    let mut changed = true;
    while changed {
        changed = false;
        for (open, close) in &tags {
            if let Some(start) = s.find(open) {
                changed = true;
                if let Some(rel) = s[start..].find(close) {
                    let end = start + rel + close.len();
                    s = format!("{}{}", s[..start].trim_end(), s[end..].trim_start());
                } else {
                    s = s[..start].trim_end().to_string();
                }
            }
        }
    }
    s.trim().to_string()
}

fn extract_solana_address(text: &str) -> Option<String> {
    // Solana addresses: base58, 32-44 chars, no 0/O/I/l
    for word in text.split_whitespace() {
        let w = word.trim_matches(|c: char| !c.is_alphanumeric());
        if w.len() >= 32
            && w.len() <= 44
            && w.chars()
                .all(|c| matches!(c, '1'..='9' | 'A'..='H' | 'J'..='N' | 'P'..='Z' | 'a'..='k' | 'm'..='z'))
        {
            return Some(w.to_string());
        }
    }
    None
}

/// HTTP API server for the website chat widget.
/// POST /api/claw { "message": "..." } — requires X-Claw-Secret header.
/// Accepts either an Anthropic key (anthropic_api_key) or an OpenAI key
/// (openai_api_key). Anthropic is preferred when both are provided.
pub async fn serve_claw_api(
    port: u16,
    secret: String,
    anthropic_key: String,
    openai_key: String,
    db: std::sync::Arc<crate::db::Db>,
) -> Result<()> {
    use std::net::SocketAddr;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("Claw API listening on {}", addr);

    let secret = std::sync::Arc::new(secret);
    let anthropic_key = std::sync::Arc::new(anthropic_key);
    let openai_key = std::sync::Arc::new(openai_key);
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(60))
        .build()?;
    let http = std::sync::Arc::new(http);

    loop {
        let (mut stream, _) = listener.accept().await?;
        let secret = secret.clone();
        let anthropic_key = anthropic_key.clone();
        let openai_key = openai_key.clone();
        let http = http.clone();
        let db = db.clone();

        tokio::spawn(async move {
            // Accumulate the full HTTP request, handling TCP fragmentation.
            // We read until we have "\r\n\r\n" and then the advertised body.
            let mut raw_bytes: Vec<u8> = Vec::with_capacity(4096);
            let mut tmp = vec![0u8; 4096];
            let mut content_length: usize = 0;
            let mut header_end: usize = 0;
            loop {
                let n = match stream.read(&mut tmp).await {
                    Ok(0) | Err(_) => return,
                    Ok(n) => n,
                };
                raw_bytes.extend_from_slice(&tmp[..n]);
                if raw_bytes.len() > 131072 {
                    let _ = stream.write_all(b"HTTP/1.1 413 Payload Too Large\r\n\r\n").await;
                    return;
                }
                // Find header/body boundary
                if let Some(pos) = raw_bytes
                    .windows(4)
                    .position(|w| w == b"\r\n\r\n")
                {
                    header_end = pos;
                    let cl = raw_bytes[..pos]
                        .split(|&b| b == b'\n')
                        .find(|l| l.to_ascii_lowercase().starts_with(b"content-length:"))
                        .and_then(|l| std::str::from_utf8(l).ok())
                        .and_then(|l| l.splitn(2, ':').nth(1))
                        .and_then(|v| v.trim().parse::<usize>().ok())
                        .unwrap_or(0);
                    content_length = cl;
                    if raw_bytes.len() >= pos + 4 + content_length {
                        break;
                    }
                }
            }
            let raw = String::from_utf8_lossy(&raw_bytes);

            // Parse bare-minimum HTTP: method, path, headers, body
            let (headers_part, body_part) = match raw.split_once("\r\n\r\n") {
                Some(p) => p,
                None => {
                    let _ = stream.write_all(b"HTTP/1.1 400 Bad Request\r\n\r\n").await;
                    return;
                }
            };
            let _ = (header_end, content_length); // consumed above
            let first_line = headers_part.lines().next().unwrap_or("");
            if !first_line.starts_with("POST /api/claw") {
                let resp = if first_line.starts_with("OPTIONS") {
                    "HTTP/1.1 200 OK\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Headers: Content-Type, X-Claw-Secret\r\nAccess-Control-Allow-Methods: POST, OPTIONS\r\nContent-Length: 0\r\n\r\n"
                } else {
                    "HTTP/1.1 404 Not Found\r\n\r\n"
                };
                let _ = stream.write_all(resp.as_bytes()).await;
                return;
            }
            // Check secret header
            let header_secret = headers_part
                .lines()
                .find(|l| l.to_lowercase().starts_with("x-claw-secret:"))
                .map(|l| l.splitn(2, ':').nth(1).unwrap_or("").trim().to_string())
                .unwrap_or_default();
            if header_secret != *secret {
                let _ = stream.write_all(b"HTTP/1.1 401 Unauthorized\r\n\r\n").await;
                return;
            }
            // Parse JSON body
            let msg_val: serde_json::Value = match serde_json::from_str(body_part) {
                Ok(v) => v,
                Err(_) => {
                    let _ = stream.write_all(b"HTTP/1.1 400 Bad Request\r\n\r\n").await;
                    return;
                }
            };
            let question = msg_val["message"].as_str().unwrap_or("").trim().to_string();
            if question.is_empty() {
                let _ = stream
                    .write_all(b"HTTP/1.1 400 Bad Request\r\ncontent-type: application/json\r\n\r\n{\"error\":\"empty message\"}")
                    .await;
                return;
            }
            // Build context using a minimal DmBot-like helper
            let context = build_context_for_api(&db, &question).await;
            let ai_result = match claw_ask_zeroclaw(&http, &context, &question).await {
                Ok(r) => Ok(r),
                Err(e) => {
                    tracing::warn!("claw API: zeroclaw unavailable: {} — trying fallback", e);
                    if !anthropic_key.is_empty() {
                        claw_ask(&http, &anthropic_key, &context, &question).await
                    } else if !openai_key.is_empty() {
                        claw_ask_openai(&http, &openai_key, &context, &question).await
                    } else {
                        Err(e)
                    }
                }
            };
            let reply = match ai_result {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!("claw API: ask failed: {}", e);
                    "The claw is unavailable right now.".to_string()
                }
            };
            let json = serde_json::json!({ "response": reply }).to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\n\
                 Content-Type: application/json\r\n\
                 Access-Control-Allow-Origin: *\r\n\
                 Content-Length: {}\r\n\r\n{}",
                json.len(),
                json
            );
            let _ = stream.write_all(response.as_bytes()).await;
        });
    }
}

async fn build_context_for_api(db: &crate::db::Db, question: &str) -> String {
    let now = chrono::Utc::now().timestamp();
    let mut ctx = String::new();
    let calls = db.list_calls(true, 8).unwrap_or_default();
    if calls.is_empty() {
        ctx.push_str("ACTIVE CALLS: none\n\n");
    } else {
        ctx.push_str("ACTIVE CALLS:\n");
        for c in &calls {
            let sym = if c.symbol.is_empty() {
                short_mint(&c.mint)
            } else {
                format!("${}", c.symbol)
            };
            let age_h = (now - c.called_at) / 3600;
            ctx.push_str(&format!(
                "  {sym}: entry_mcap=${:.0}k top={:.1}% class={} conf={} age={}h\n",
                c.entry_mcap_usd / 1000.0,
                c.entry_top_holder_pct,
                c.classification,
                c.confidence,
                age_h
            ));
        }
        ctx.push('\n');
    }
    let alerts = db.get_signal_alerts(4).unwrap_or_default();
    if !alerts.is_empty() {
        ctx.push_str("PENDING SIGNALS:\n");
        for a in &alerts {
            let addr = a.token_address.as_deref().unwrap_or("?");
            ctx.push_str(&format!("  {} conf={}\n", short_mint(addr), a.confidence));
        }
        ctx.push('\n');
    }
    if let Some(addr) = extract_solana_address(question) {
        if let Ok(Some(meta)) = crate::metadata::fetch(&addr).await {
            ctx.push_str(&format!("TOKEN: {} (${}) price=${:.8} mcap=${:.1}k\n\n",
                meta.name, meta.symbol,
                meta.price_usd.unwrap_or(0.0),
                meta.market_cap_usd.unwrap_or(0.0) / 1000.0));
        }
    }
    ctx
}

// -- Helpers ----------------------------------------------------------------

fn parse_command(text: &str) -> (Option<String>, String) {
    if !text.starts_with('/') {
        return (None, String::new());
    }
    let rest = &text[1..];
    // Handle /cmd@BotName syntax
    let (head, tail) = rest
        .split_once(' ')
        .map(|(h, t)| (h, t.to_string()))
        .unwrap_or((rest, String::new()));
    let cmd = head.split('@').next().unwrap_or(head).to_lowercase();
    (Some(cmd), tail)
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn short_mint(m: &str) -> String {
    if m.len() < 10 {
        m.to_string()
    } else {
        format!("{}…{}", &m[..4], &m[m.len() - 4..])
    }
}

/// Strip any persistent reply keyboard — used when we want the chat to feel
/// like a normal conversation without the menu eating half the screen.
fn clear_keyboard() -> String {
    serde_json::json!({ "remove_keyboard": true }).to_string()
}

/// Opt-in reply keyboard (only shown via /menu). Compact 2×2 layout.
fn reply_menu() -> String {
    serde_json::json!({
        "keyboard": [
            [{ "text": "/scan" }, { "text": "/signals" }],
            [{ "text": "/status" }, { "text": "/traps" }]
        ],
        "resize_keyboard": true,
        "one_time_keyboard": true,
        "is_persistent": false
    })
    .to_string()
}
