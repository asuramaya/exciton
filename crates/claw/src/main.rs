//! Claw — Exciton's autonomous agent runtime.
//!
//! Three subcommands cover the v0 lifecycle:
//!   - `claw login` — completes the OpenAI Codex OAuth flow and stores
//!     an encrypted auth profile under `~/.exciton/auth.json`.
//!   - `claw whoami` — prints the active auth profile (provider, account,
//!     token validity) without making a network call.
//!   - `claw review --once` — runs a single self-review cycle: pulls the
//!     last N days of closed-call outcomes via MCP, asks the LLM to
//!     propose at most one strategy tune with full evidence, and
//!     (in `commit` mode) commits + posts the resulting evolution event.
//!
//! `claw run` (the always-on tick loop) lands in a later phase. v0 is a
//! cron-driven binary; the agent's autonomy comes from the schedule, not
//! from a persistent process.

use anyhow::Result;
use clap::{Parser, Subcommand};

mod auth;
mod provider;
mod runtime;

#[derive(Parser)]
#[command(name = "claw", version, about = "Autonomous agent runtime for Exciton")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Complete the OpenAI Codex OAuth flow + store the encrypted profile.
    /// Re-run any time to refresh the token before its access window
    /// expires (typically 1 hour for Codex).
    Login(auth::LoginArgs),
    /// Print the active auth profile + provider state. Read-only.
    Whoami,
    /// Run a self-review cycle. Reads outcomes via MCP, proposes a
    /// strategy tune, optionally commits + posts the evolution event.
    Review(runtime::review::ReviewArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("claw=info".parse()?)
                .add_directive("exciton_claw=info".parse()?),
        )
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Login(args) => auth::login(args).await,
        Cmd::Whoami => auth::whoami().await,
        Cmd::Review(args) => runtime::review::run(args).await,
    }
}
