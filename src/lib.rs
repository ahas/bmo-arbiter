//! The card arbiter's master (docs/placement.md): the placement rules, the planner that reads them, the
//! node reach the master drives the slaves through, the master itself with its picture of the cards and
//! its state API, the node arbiter's install, and the verbs that check the rules and push them.

pub mod picture;
pub mod plan;
pub mod rules;
pub mod yaml;
