//! The events the master publishes on its one stream (docs/api.md, 「Events」): each named by its resource
//! and its change, numbered by the master, its data the member as a GET answers it.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::page::page;

/// A kind of event: a resource and its change.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
pub enum EventKind {
  #[serde(rename = "cluster.changed")]
  ClusterChanged,
  #[serde(rename = "placement.created")]
  PlacementCreated,
  #[serde(rename = "placement.changed")]
  PlacementChanged,
}

impl EventKind {
  /// Every kind.
  pub const ALL: [Self; 3] = [Self::ClusterChanged, Self::PlacementCreated, Self::PlacementChanged];

  /// The kind as a stream names it.
  pub fn name(self) -> &'static str {
    match self {
      Self::ClusterChanged => "cluster.changed",
      Self::PlacementCreated => "placement.created",
      Self::PlacementChanged => "placement.changed",
    }
  }

  /// The kind a name names, or none where it names none.
  pub fn of_name(name: &str) -> Option<Self> {
    Self::ALL.into_iter().find(|kind| kind.name() == name)
  }
}

/// One event as the stream carries it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Event {
  /// The master's event number, which only grows.
  pub id: i64,
  pub kind: EventKind,
  /// When it was published, in epoch milliseconds.
  pub at: i64,
  /// The member as a GET answers it; the document's `x-events` gives the schema of each kind's.
  #[schemars(with = "Map<String, Value>")]
  pub data: Value,
}

page!(EventPage, Event, "A page of events, in the order they were published.");

/// Where a stream starts and what it carries.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EventsQuery {
  /// The number of the last event already read; the stream starts at the moment it is opened where none is
  /// given.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub after: Option<i64>,
  /// The comma-separated kinds; every kind where none is given.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub kinds: Option<String>,
  /// The most events a page answers, for a reader asking `Accept: application/json`.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub limit: Option<u32>,
}

/// Whether a program stands ready, and its version.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Health {
  pub ready: bool,
  /// Null when ready, else why not.
  pub reason: Option<String>,
  pub version: String,
}
