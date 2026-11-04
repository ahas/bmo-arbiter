//! The card master's use of every card (docs/api.md, 「Usage」): what each node's card was doing at the
//! master's last reading, the service or turn holding it, the engine resident on it with its memory, and
//! the waiters in its queue. It names services and units alone, never a process, a pid or a person.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::card::{Claim, UnitState};
use crate::cluster::CardTurn;

/// One card's use, and what stands on it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UsageCard {
  pub index: u32,
  pub name: Option<String>,
  #[serde(rename = "totalMiB")]
  pub total_mib: Option<f64>,
  #[serde(rename = "usedMiB")]
  pub used_mib: Option<f64>,
  #[serde(rename = "freeMiB")]
  pub free_mib: Option<f64>,
  /// The card's use as the driver reports it, 0 to 100, or null where it gives no rate.
  #[serde(rename = "utilizationPercent")]
  pub utilization_percent: Option<u32>,
  /// The turn standing on the card, whether or not it is in use.
  pub turn: Option<CardTurn>,
  /// The claim standing on the card.
  pub claim: Option<Claim>,
  /// Every unit on the card, with the place each stands in.
  pub units: Vec<UsageUnit>,
  /// Every ticket waiting in the card's line, in line order.
  pub waiters: Vec<UsageWaiter>,
}

/// A unit on a card and the memory it holds.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UsageUnit {
  /// `<service>/<part>@<node>`.
  pub name: String,
  pub service_name: String,
  pub part: String,
  #[serde(rename = "vramMiB")]
  pub vram_mib: f64,
  #[serde(rename = "ramMiB")]
  pub ram_mib: f64,
  pub state: UnitState,
}

/// A ticket waiting for a card's turn.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UsageWaiter {
  pub ticket_id: i64,
  pub service_name: String,
  pub position: Option<i64>,
  pub waited_ms: i64,
  /// The unit it asks for.
  pub unit_name: Option<String>,
  #[serde(rename = "vramMiB")]
  pub vram_mib: Option<f64>,
}

/// One node, whether the master reaches it, and its cards.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UsageNode {
  pub node_name: String,
  /// Whether the master reached the node's slave at its last reading.
  pub connected: bool,
  /// When the slave was last read.
  pub read_at: Option<i64>,
  /// Why the last read gave no card, where it gave none.
  pub error: Option<String>,
  pub cards: Vec<UsageCard>,
}

/// Every node's card as the master last read it, and when it answered.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Usage {
  /// When the master answered, in epoch milliseconds.
  pub at: i64,
  pub nodes: Vec<UsageNode>,
}
