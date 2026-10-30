//! Demands (docs/api.md, 「Demands」): cards of a service standing on several nodes asked of the master, which
//! decides which node answers each, and given back by their caller alone.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::page::page;

/// A demand asked for.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DemandAsk {
  pub service_name: String,
  /// How many cards; one where absent.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub count: Option<u32>,
  /// The id of a granted demand of the caller's this one is asked within.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub within: Option<i64>,
  /// The key of the work it continues.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub work_key: Option<String>,
  /// A reference; shown.
  #[serde(default, rename = "ref", skip_serializing_if = "Option::is_none")]
  pub reference: Option<String>,
  /// How long to hold the answer open.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub wait_ms: Option<u64>,
}

/// How a demand is asked after.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DemandQuery {
  /// How long to hold the answer open until a card is granted, granted again, lent, or taken by a lower
  /// tier, or the time runs out.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub wait_ms: Option<u64>,
}

/// Where a demand stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum DemandState {
  /// No card of it is granted.
  Waiting,
  /// Every card it asks is granted.
  Granted,
  /// Some of its cards are granted and others wait.
  Partly,
  /// Its card is lent to a demand asked within it.
  Lent,
}

/// One card of a demand granted: its node, and where the service's adapter reports them, the address its
/// callers reach the instance at and the file its key is read from.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DemandCard {
  pub node_name: String,
  pub address: Option<String>,
  pub key_file: Option<String>,
}

/// What a card of a demand waits on.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum DemandWait {
  /// The service is starting on a card.
  Starting,
  /// Every card it may have is in use.
  InUse,
  /// A claim or a ticket of a lower tier holds the cards it may have.
  Held,
  /// The service's `start` failed; tried again at the next round.
  StartFailed,
}

/// Why a card of a demand waits: its code, and an English sentence for a person.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DemandReason {
  pub code: DemandWait,
  pub text: String,
}

/// A demand as every route answers it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Demand {
  pub id: i64,
  pub service_name: String,
  pub count: u32,
  /// Each card granted.
  pub cards: Vec<DemandCard>,
  pub state: DemandState,
  /// The claimant or lower-tier occupant that stopped the service on one of its cards.
  pub taken: Option<String>,
  /// While a card waits, what it waits on.
  pub reason: Option<DemandReason>,
  /// How long it has stood.
  pub waited_ms: i64,
  pub within: Option<i64>,
  pub work_key: Option<String>,
  #[serde(rename = "ref")]
  pub reference: Option<String>,
}

page!(DemandPage, Demand, "A page of demands.");
