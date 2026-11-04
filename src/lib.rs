//! The card arbiter's master (docs/placement.md): the placement rules, the planner that reads them, the
//! node reach the master drives the slaves through, the master itself with its picture of the cards and
//! its API, placing the services, the node arbiter's install, and the verbs that check the rules and push
//! them.

pub mod client;
pub mod demands;
pub mod hands;
pub mod install;
pub mod machine;
pub mod master;
pub mod picture;
pub mod placement;
pub mod plan;
pub mod reach;
pub mod rules;
pub mod serve;
pub mod usage;
pub mod verbs;
pub mod yaml;
