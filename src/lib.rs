// Crate-level lint allowances. These are stylistic warnings that are
// intentional in this codebase: too_many_arguments fires on the call-
// state insert/update functions where every column is a positional
// arg by design; dead_code fires on staged accessors waiting for a
// caller; type_complexity flags some intentional Result<...> shapes
// inside settle. Tightening any of these is on the post-v0.1.0
// backlog when the call-row API can be refactored to a builder.
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

pub mod bitquery;
pub mod bonding_curve;
pub mod bot;
pub mod chart_screenshot;
pub mod holders;
pub mod image_gen;
pub mod config;
pub mod db;
pub mod discovery;
pub mod discovery_pollers;
pub mod execution;
pub mod forecaster;
pub mod horizon;
pub mod ingester;
pub mod intel;
pub mod launch_forensics;
pub mod market;
pub mod mcp;
pub mod metadata;
pub mod notifier;
pub mod publisher;
pub mod pumpportal;
pub mod scanner;
pub mod wallet_cache;
pub mod wallet_observer;
pub mod scout;
pub mod signals;
pub mod templates;
