//! The arbiter's client (docs/api.md): every body the master's and the node arbiter's routes take and answer
//! and every event the master publishes as a type, and a call for every route. The master answers in these
//! types, and the OpenAPI document the harness plugin generates is drawn from them, so the document is what
//! both programs serve.

mod call;
pub mod card;
pub mod cluster;
pub mod demands;
pub mod descriptions;
mod error;
pub mod events;
mod page;
pub mod placements;

pub use call::{ArbiterClient, CallError, CallResult};
pub use error::{ErrorCode, WireError};
