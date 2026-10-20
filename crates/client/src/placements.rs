//! Placements (docs/api.md, 「Placements」): a service put on a node or taken off one, asked, run one at a
//! time, and followed by its lines.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// What a placement does.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum PlacementKind {
  Place,
  Remove,
}

impl PlacementKind {
  pub fn word(self) -> &'static str {
    match self {
      Self::Place => "place",
      Self::Remove => "remove",
    }
  }
}

/// Where a placement stands in its life.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum PlacementState {
  Queued,
  Running,
  Done,
  Failed,
}

impl PlacementState {
  /// Whether it has ended, done or failed.
  pub fn ended(self) -> bool {
    matches!(self, Self::Done | Self::Failed)
  }
}

/// A placement asked for.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PlacementAsk {
  pub kind: PlacementKind,
  pub service_name: String,
  pub node_name: String,
}

/// A placement as every route and event answers it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Placement {
  pub id: i64,
  pub kind: PlacementKind,
  pub service_name: String,
  pub node_name: String,
  pub state: PlacementState,
  /// What the master did, one English sentence each, in order.
  pub lines: Vec<String>,
  /// Why it failed, with the step it stopped at.
  pub failure: Option<String>,
  /// When it was asked, in epoch milliseconds.
  pub created_at: i64,
  /// When it ended, in epoch milliseconds.
  pub ended_at: Option<i64>,
}
