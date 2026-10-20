//! The arbiter's harness plugin, `bmo-arbiter` (docs/api.md, 「The clients and the harness plugin」).
//!
//! `bmo-arbiter mcp` starts this crate's MCP server, one tool for each route a harness takes a turn on a card
//! or reads the cluster through, each a thin call of the arbiter client; the skill beside this crate tells a
//! harness when and how to use it. The crate also generates the arbiter's OpenAPI document, which carries the
//! tools under `x-tools`.

mod openapi;
mod server;
mod tools;

pub use openapi::{openapi, openapi_json};
pub use server::ArbiterServer;
pub use tools::{Answered, ArbiterTool, Clients, Program, ToolFailure, tools};
