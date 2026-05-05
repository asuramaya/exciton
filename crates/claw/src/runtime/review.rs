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

/// Hard cap on agent turns per review. With the D1+D2+D3 surface the
/// happy path is review_log → list_tunes → rank_tune_candidates →
/// propose_tune → review_log_write → final message ≈ 5-6 turns. 10
/// leaves headroom for one diagnostic detour without runaway loops.
const MAX_TURNS: usize = 10;

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

    // Refresh the access token if it's within 5 minutes of expiry. No-op
    // when there's no profile (api-key fallback) or when the token is
    // still fresh. Avoids mid-cycle 401s from a token that ages out.
    match crate::auth::refresh_if_needed(std::time::Duration::from_secs(300)).await {
        Ok(_) => {}
        Err(e) => tracing::warn!("auth refresh skipped: {}", e),
    }

    let mcp = McpClient::from_env()?;
    let provider = CascadingProvider::from_env()?;

    let now = chrono::Utc::now().timestamp();
    let since = now - args.window_secs;
    let user_kickoff = format!(
        "Review cycle starting now (started_at={}). Window: last {}s (since={}). Mode: {}.\n\n\
         Suggested flow:\n\
         1. `review_log(limit=5)` — check what you concluded last cycle. Don't restate yesterday's diagnosis; only act on new evidence.\n\
         2. `list_tunes(limit=20)` — check prior moves still on file.\n\
         3. `rank_tune_candidates(top_k=5, since={})` — pre-validated menu. n≥10, effect≥5%, holdout≥0 already enforced.\n\
         4. If a row clears your robustness judgment, call `propose_tune` with that row's `evidence_json` verbatim. {}.\n\
         5. **ALWAYS** finish by calling `review_log_write(started_at={}, mode=\"{}\", outcome=..., summary=..., proposal_id=...?, turns=..., tool_calls=...)`. This is how future cycles see what you did.\n\
         6. Then output your final message and stop.\n\n\
         Do not call `analyze_outcomes` or `sweep_threshold` unless `rank_tune_candidates` returned weak — the ranker already covers the deterministic search. The diagnostic tools (`analyze_drift`, `analyze_failure_modes`, `compare_periods`) are for narrating *why*, not for finding candidates.",
        now,
        args.window_secs,
        since,
        args.mode,
        since,
        if args.mode == "commit" {
            "Then call `commit_tune(proposal_id, body_md=...)` with your authored diary entry"
        } else {
            "Stop after propose; no commit this cycle"
        },
        now,
        args.mode,
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

/// Autonomy tools advertised to the model. Schemas mirror the param
/// structs in `mcp.rs` — keep them in sync. Future iteration: have
/// claw introspect via MCP `tools/list` instead of carrying schemas
/// here.
fn autonomy_tools() -> Vec<ToolSpec> {
    vec![
        ToolSpec {
            name: "rank_tune_candidates".into(),
            description: "PRIMARY DECISION TOOL. Server runs the candidate grid \
                          across every (scope × field × value), pre-validates \
                          each against the floors (n>=10, effect>=5%, holdout \
                          n>=1 and mean>=0), ranks by `effect × √n`, and \
                          returns top-K rows with validator-ready evidence_json \
                          attached. The agent picks one row and feeds the \
                          embedded evidence_json straight into propose_tune. \
                          No math required from the agent."
                .into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "top_k": { "type": "integer", "description": "Default 5, max 20." },
                    "scopes": { "type": "array", "items": {"type":"string"}, "description": "Optional. e.g. [\"class:GRINDER\", \"global\"]. Default: all valid scopes per field." },
                    "fields": { "type": "array", "items": {"type":"string"}, "description": "Optional. Default: all sweepable fields." },
                    "since": { "type": "integer", "description": "Earliest called_at to include. Default: now-30d." },
                    "holdout_pct": { "type": "integer", "description": "Holdout split percent. Default 25." }
                }
            }),
        },
        ToolSpec {
            name: "sweep_threshold".into(),
            description: "Per-knob threshold curve when you want to interrogate \
                          a specific (field, scope) yourself. Same shape as one \
                          row of rank_tune_candidates. Use only when the ranker \
                          missed something obvious."
                .into(),
            parameters: json!({
                "type": "object",
                "required": ["field", "scope", "candidates"],
                "properties": {
                    "field": { "type": "string" },
                    "scope": { "type": "string" },
                    "candidates": { "type": "array", "items": {"type":"string"} },
                    "since": { "type": "integer" },
                    "holdout_pct": { "type": "integer" }
                }
            }),
        },
        ToolSpec {
            name: "describe_signal_filters".into(),
            description: "Tunable knob inventory: name, current effective value, \
                          compile-time default, valid range, supported scopes, \
                          whether it can be swept. Read this when you need to \
                          confirm what's tunable, but typically you can rely on \
                          rank_tune_candidates to know the surface."
                .into(),
            parameters: json!({"type": "object", "properties": {}}),
        },
        ToolSpec {
            name: "list_overrides".into(),
            description: "Active runtime overrides (committed tunes, current \
                          values). Read counterpart to commit_tune writes."
                .into(),
            parameters: json!({"type": "object", "properties": {}}),
        },
        ToolSpec {
            name: "review_log".into(),
            description: "Read recent claw review cycles. Use this at the \
                          start of a cycle to know what you concluded last \
                          time; helps avoid restating the same diagnosis."
                .into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "limit": { "type": "integer", "description": "Default 10, max 200." }
                }
            }),
        },
        ToolSpec {
            name: "review_log_write".into(),
            description: "Write this cycle's ledger row at the end. ALWAYS \
                          call this exactly once per cycle, after you've \
                          either proposed/committed or stopped — that's how \
                          future cycles know what you did."
                .into(),
            parameters: json!({
                "type": "object",
                "required": ["started_at", "mode", "outcome", "summary"],
                "properties": {
                    "started_at": { "type": "integer", "description": "Epoch seconds the cycle started — use the value from the kickoff message." },
                    "mode": { "type": "string", "description": "propose | commit" },
                    "outcome": { "type": "string", "description": "proposed | committed | stopped | failed" },
                    "summary": { "type": "string", "description": "One paragraph note-to-future-self." },
                    "proposal_id": { "type": "integer", "description": "Optional. Set if this cycle produced one." },
                    "turns": { "type": "integer" },
                    "tool_calls": { "type": "integer" }
                }
            }),
        },
        ToolSpec {
            name: "analyze_drift".into(),
            description: "Time-windowed performance check on a scope. \
                          Slices the closed-call universe into N windows \
                          and reports per-window stats so you can spot \
                          rotting buckets analyze_outcomes smoothes over."
                .into(),
            parameters: json!({
                "type": "object",
                "required": ["scope"],
                "properties": {
                    "scope": { "type": "string", "description": "global | class:STAIRCASE | class:GRINDER | class:SPRING | class:DEVELOPING" },
                    "window_count": { "type": "integer", "description": "Default 4. Range 2..=12." },
                    "window_secs": { "type": "integer", "description": "Total span in seconds. Default 30 days." }
                }
            }),
        },
        ToolSpec {
            name: "analyze_failure_modes".into(),
            description: "Failure-mode taxonomy for closed calls in scope. \
                          Buckets non-winners into rug_or_fast_dump, \
                          slow_bleed, drawdown_cap, timeout, void, flat, \
                          winner; returns share + mean PnL per bucket."
                .into(),
            parameters: json!({
                "type": "object",
                "required": ["scope"],
                "properties": {
                    "scope": { "type": "string" },
                    "since": { "type": "integer" }
                }
            }),
        },
        ToolSpec {
            name: "list_evolution_events".into(),
            description: "Read the public diary so you don't repeat \
                          yourself. Returns kind/summary/committed_at; \
                          set include_body=true to see your prior voice."
                .into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "kind": { "type": "string", "description": "strategy | tool | site. Omit for all." },
                    "limit": { "type": "integer", "description": "Default 20, max 200." },
                    "include_body": { "type": "boolean", "description": "Include the full body_md. Default false." }
                }
            }),
        },
        ToolSpec {
            name: "compare_periods".into(),
            description: "Before/after comparison anchored at a timestamp. \
                          Use with anchor_at = a prior tune's committed_at \
                          to answer 'did my last tune work?'."
                .into(),
            parameters: json!({
                "type": "object",
                "required": ["scope"],
                "properties": {
                    "scope": { "type": "string" },
                    "anchor_at": { "type": "integer", "description": "Pivot timestamp. Default now." },
                    "before_secs": { "type": "integer", "description": "Default 30 days." },
                    "after_secs": { "type": "integer", "description": "Default = (now - anchor_at)." }
                }
            }),
        },
        ToolSpec {
            name: "describe_classifications".into(),
            description: "Glossary: classification names, horizons, voice \
                          framing, default floors. Read once if you need \
                          to refresh on what each label means."
                .into(),
            parameters: json!({"type": "object", "properties": {}}),
        },
        ToolSpec {
            name: "simulate_overrides".into(),
            description: "Counterfactual replay applying multiple overrides \
                          at once. For testing combined effects (tighten \
                          confidence AND liquidity simultaneously) without \
                          asking the LLM to do the math."
                .into(),
            parameters: json!({
                "type": "object",
                "required": ["overrides"],
                "properties": {
                    "overrides": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["field", "scope", "new_value"],
                            "properties": {
                                "field": { "type": "string" },
                                "scope": { "type": "string" },
                                "new_value": { "type": "string" }
                            }
                        }
                    },
                    "since": { "type": "integer" }
                }
            }),
        },
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
