//! The card master's integration tests, one module to an area, built as one test binary: each binary
//! links every dependency whole, so one of them rather than one per area is what a build makes and
//! keeps.

mod common;
#[path = "../support/contract.rs"]
mod contract_check;

mod contract;
mod placement;
