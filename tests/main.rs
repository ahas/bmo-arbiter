//! The crate's one test harness (bmo-chat's docs/conventions.md, 「Rust builds」): each file of tests a
//! module of it, sharing the process.

mod fixtures;
mod support;

mod card;
mod card_wire;
mod dependencies;
mod hands;
mod hangul;
mod install;
mod integration;
mod layout;
mod master;
mod mcp;
mod openapi;
mod placement;
mod plan;
mod reach;
mod rules;
mod tool;
mod verbs;
mod yaml;
