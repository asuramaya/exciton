//! Self-review cycle.
//!
//! 1. Connect to the running exciton MCP server (HTTP-streamable).
//! 2. Build the system prompt (bundled `prompts/system.md`) + a brief
//!    user kickoff that hands the agent the cycle's parameters.
//! 3. Drive the agent loop:
//!    - `provider.complete(messages, tools)`
//!    - Execute every tool call against MCP, append the result.
//!    - Loop up to MAX_TURNS until the model returns content with no
//!      tool calls (final message), or hits the turn cap.
//! 4. Print the final agent text + a summary of any tool calls made.
//!
//! v0 caveats:
//!   - Only OpenAI raw-API path runs end-to-end. Codex transport is
//!     stubbed (returns Unauthorized → fallback to api-key). B2 wired
//!     the OAuth flow; the `/backend-api/codex/responses` request shape
//!     port lands in a follow-up.
//!   - No streaming. Each `provider.complete` is a full request/response.
//!   - The four autonomy tools are advertised by hardcoded JSON schema.
//!     A future pass can introspect via MCP `tools/list` instead.

use crate::provider::{Message, Provider, ProviderError, ToolCall, ToolSpec};
use crate::runtime::{mcp_client::McpClient, selection::CascadingProvider};
use anyhow::{anyhow, Context, Result};
use clap::Args;
use serde_json::json;

/// Hard cap on agent turns per review. Each turn = one provider.complete.
/// In practice the agent should land in 3–6 turns: list_tunes →
/// analyze_outcomes → (optional second analyze with include_raw) →
/// propose_tune → commit_tune → final message.
const MAX_TURNS: usize = 12;

const SYSTEM_PROMPT: &str = include_str!("../../prompts/system.md");

#[derive(Args, Debug)]
pub struct ReviewArgs {
    /// One-shot mode — run a single cycle and exit. v0 only supports
    /// this; an always-on tick loop ships later.
    #[arg(long)]
    pub once: bool,
    /// Trailing window in seconds. Default: 30 days.
    #[arg(long, default_value_t = 30 * 24 * 60 * 60)]
    pub window_secs: i64,
    /// `propose` (the agent stops after `propose_tune` and leaves the
    /// proposal pending) or `commit` (the agent also calls `commit_tune`
    /// when a proposal lands cleanly). Default: propose.
    #[arg(long, default_value = "propose")]
    pub mode: String,
}

pub async fn run(args: ReviewArgs) -> Result<()> {
    if !args.once {
        eprintln!("claw: only --once is wired in v0; default tick loop ships later");
    }
    if args.mode != "propose" && args.mode != "commit" {
        anyhow::bail!("--mode must be 'propose' or 'commit'");
    }

    let mcp = McpClient::from_env()?;
    let provider = CascadingProvider::from_env()?;

    let now = chrono::Utc::now().timestamp();
    let since = now - args.window_secs;
    let user_kickoff = format!(
        "Review cycle starting now ({}). Window: last {} seconds (since={}). \
         Operating mode: {}. \
         Begin by calling `list_tunes` to see prior proposals, then `analyze_outcomes(since={})`. \
         If you find a bucket with strong evidence, call `propose_tune` and (if mode=commit) \
         follow up with `commit_tune`. If nothing meets the publishable bar, \
         stop with a brief explanation of what you looked at.",
        now, args.window_secs, since, args.mode, since,
    );

    let mut messages = vec![
        Message {
            role: "system".into(),
            content: SYSTEM_PROMPT.to_string(),
            tool_calls: vec![],
            tool_call_id: None,
        },
        Message {
            role: "user".into(),
            content: user_kickoff,
            tool_calls: vec![],
            tool_call_id: None,
        },
    ];

    let tools = autonomy_tools();
    let mut tool_call_count = 0usize;

    for turn in 1..=MAX_TURNS {
        let completion = match provider.complete(&messages, &tools).await {
            Ok(c) => c,
            Err(ProviderError::UsageLimitReached { provider, detail }) => {
                anyhow::bail!("provider {provider} quota exhausted: {detail}");
            }
            Err(ProviderError::Unauthorized { provider, detail }) => {
                anyhow::bail!("provider {provider} unauthorized: {detail}");
            }
            Err(other) => return Err(anyhow!(other.to_string())),
        };

        if completion.tool_calls.is_empty() {
            // Agent finished without invoking more tools — print the
            // closing message and stop. Even a stop-with-reason is a
            // valid review outcome.
            println!(
                "\n=== claw review · final message (turn {}) ===\n{}",
                turn,
                completion.content.trim()
            );
            println!(
                "\n=== claw review · summary ===\n  turns: {}\n  tool_calls: {}",
                turn, tool_call_count
            );
            return Ok(());
        }

        // Push the assistant turn that emitted these tool calls.
        messages.push(Message {
            role: "assistant".into(),
            content: completion.content.clone(),
            tool_calls: completion.tool_calls.clone(),
            tool_call_id: None,
        });

        for call in &completion.tool_calls {
            tool_call_count += 1;
            let result = run_tool(&mcp, call).await;
            let result_text = match result {
                Ok(text) => text,
                Err(e) => {
                    json!({
                        "ok": false,
                        "error": "tool_dispatch_failed",
                        "message": e.to_string(),
                    })
                    .to_string()
                }
            };
            tracing::info!(
                "tool_call {}({}) → {}",
                call.name,
                truncate(&call.arguments, 200),
                truncate(&result_text, 200)
            );
            messages.push(Message {
                role: "tool".into(),
                content: result_text,
                tool_calls: vec![],
                tool_call_id: Some(call.id.clone()),
            });
        }
    }

    eprintln!(
        "claw: hit MAX_TURNS ({MAX_TURNS}) without a final message — stopping. tool_calls={tool_call_count}"
    );
    Ok(())
}

/// Dispatch a tool call to the MCP server. The agent is restricted by
/// prompt to only call tools in the autonomy surface, but we route any
/// name through `call_tool` — the server will reject anything not
/// registered.
async fn run_tool(mcp: &McpClient, call: &ToolCall) -> Result<String> {
    let args: serde_json::Value =
        serde_json::from_str(&call.arguments).with_context(|| {
            format!(
                "tool {} returned arguments that don't parse as JSON: {}",
                call.name, call.arguments
            )
        })?;
    mcp.call_tool(&call.name, args).await
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…[+{}]", &s[..max], s.len() - max)
    }
}

/// The four autonomy tools advertised to the model. Schemas mirror the
/// param structs in `mcp.rs` (ProposeTuneParams etc.) — keep these in
/// sync with the server. Future iteration: have claw introspect the
/// server's `tools/list` instead of carrying schemas here.
fn autonomy_tools() -> Vec<ToolSpec> {
    vec![
        ToolSpec {
            name: "analyze_outcomes".into(),
            description: "Read closed-call outcomes bucketed by classification × horizon. \
                          Returns aggregates (n, win_rate_pct, mean/median/p25/p75 PnL, mean hold, \
                          verdict breakdown). Pass `include_raw=true` to also receive the raw \
                          call list capped by `limit`."
                .into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "classification": { "type": "string", "description": "STAIRCASE | GRINDER | SPRING etc. Omit for all." },
                    "horizon": { "type": "string", "description": "SHORT | LONG | MOONSHOT | SCALP. Omit for all." },
                    "since": { "type": "integer", "description": "Earliest called_at to include (epoch seconds)." },
                    "include_raw": { "type": "boolean", "description": "Include raw call list. Default false." },
                    "limit": { "type": "integer", "description": "Cap on raw rows when include_raw is true. Default 50." }
                }
            }),
        },
        ToolSpec {
            name: "list_tunes".into(),
            description: "List prior tune proposals (pending / committed / rejected / reverted). \
                          Use this BEFORE proposing to avoid duplicates and to see what's already \
                          been reverted (don't re-fight that battle without new evidence)."
                .into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "status": { "type": "string", "description": "Filter: pending | committed | rejected | reverted. Omit for all." },
                    "limit": { "type": "integer", "description": "Default 50, max 500." }
                }
            }),
        },
        ToolSpec {
            name: "propose_tune".into(),
            description: "Submit a strategy tune proposal. Validators reject anything that doesn't \
                          meet n>=10, effect_size>=5%, holdout positive, narrative present. Returns \
                          a proposal_id to be passed to commit_tune."
                .into(),
            parameters: json!({
                "type": "object",
                "required": ["field", "scope", "old_value", "new_value", "evidence_json", "narrative"],
                "properties": {
                    "field": { "type": "string", "description": "min_effective_confidence | max_top_holder_pct | min_liquidity_usd | min_volume_24h_usd | min_token_age_secs" },
                    "scope": { "type": "string", "description": "global | class:STAIRCASE | class:GRINDER | class:SPRING" },
                    "old_value": { "type": "string", "description": "Stringified current value" },
                    "new_value": { "type": "string", "description": "Stringified proposed value" },
                    "evidence_json": { "type": "string", "description": "JSON string with current{n,mean_pnl_pct}, proposed{mean_pnl_pct}, holdout{n,mean_pnl_pct}, method." },
                    "narrative": { "type": "string", "description": "3–4 sentence trader-voice explanation. Min 40 chars." },
                    "proposed_by": { "type": "string", "description": "claw (default) | operator" }
                }
            }),
        },
        ToolSpec {
            name: "commit_tune".into(),
            description: "Activate a pending proposal. Writes the runtime override + creates a \
                          public diary entry. body_md must be 200..=4000 chars and reference the \
                          field, old_value, or new_value somewhere — soft check that the diary is \
                          actually about the change."
                .into(),
            parameters: json!({
                "type": "object",
                "required": ["proposal_id", "body_md"],
                "properties": {
                    "proposal_id": { "type": "integer" },
                    "body_md": { "type": "string", "description": "Agent-authored markdown for the diary entry." },
                    "summary": { "type": "string", "description": "Optional one-line headline. Auto-derived if omitted." }
                }
            }),
        },
    ]
}
