//! Runtime: review cycle, MCP transport, and the provider-selection
//! cascade that makes subscription billing the default with raw-API
//! fallback on quota exhaustion.

pub mod mcp_client;
pub mod review;
pub mod selection;
