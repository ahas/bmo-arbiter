//! The cluster as the master pictures it (docs/api.md, 「The cluster」): the master itself, the groups the
//! rules choose, every node read with its card, units and queue, the units the rules declare, the services
//! the master places, the placement under way, and the picture's last notes.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::card::{Claim, Process, Ram, Store, UnitState};
use crate::descriptions::UnitDetail;
use crate::placements::PlacementKind;

/// The master: its version, its start, the rules it reads and how often it reads the nodes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MasterInfo {
  pub version: String,
  pub started_at: i64,
  pub rules_file: String,
  pub rules_loaded_at: Option<i64>,
  pub rules_error: Option<String>,
  pub read_every_ms: i64,
}

/// A group of nodes the rules choose, and the memory its cards hold and have free.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Group {
  pub name: String,
  pub pool: bool,
  pub node_names: Vec<String>,
  #[serde(rename = "totalMiB")]
  pub total_mib: f64,
  #[serde(rename = "freeMiB")]
  pub free_mib: f64,
}

/// Whether the master reaches a node's slave.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum LinkState {
  Connected,
  Lost,
}

/// The master's link to a node's slave.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodeLink {
  pub state: LinkState,
  /// When the link came into its state.
  pub since_at: Option<i64>,
  /// When the slave was last read.
  pub read_at: Option<i64>,
  /// How long the last read took.
  pub last_read_ms: Option<i64>,
  pub error: Option<String>,
}

/// The rules as a slave last read them.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SlaveRules {
  pub loaded_at: Option<i64>,
  pub error: Option<String>,
}

/// A unit's kind as the rules give it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum UnitKind {
  Resident,
  OnDemand,
  Fill,
}

/// A unit on a node's card, as its slave said it, with the kind the rules give it and what its service says
/// it holds.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ClusterUnit {
  /// `<service>/<part>@<node>`.
  pub name: String,
  pub service_name: String,
  pub part: String,
  #[serde(rename = "vramMiB")]
  pub vram_mib: f64,
  pub state: UnitState,
  #[serde(rename = "ramMiB")]
  pub ram_mib: f64,
  pub since_at: Option<i64>,
  pub evictable: bool,
  pub reason: Option<String>,
  pub load_ms: Option<i64>,
  pub unload_ms: Option<i64>,
  pub kind: Option<UnitKind>,
  pub detail: Option<UnitDetail>,
}

/// The turn standing on a card.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CardTurn {
  pub service_name: String,
  pub ticket_id: i64,
  pub waited_ms: i64,
  /// Whether the turn is in use, where the slave says.
  pub in_use: Option<bool>,
}

/// A ticket waiting in a card's line.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueuedTicket {
  pub ticket_id: i64,
  pub service_name: String,
  pub position: Option<i64>,
  pub waited_ms: i64,
  pub wait_ms: Option<i64>,
  /// The unit it asks for.
  pub unit_name: Option<String>,
  #[serde(rename = "vramMiB")]
  pub vram_mib: Option<f64>,
}

/// Whether the card's lent engine waits at its stage, and for how long.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CardStage {
  pub waiting: bool,
  pub waiting_ms: i64,
}

/// One card of a node.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ClusterCard {
  pub index: u32,
  pub name: Option<String>,
  #[serde(rename = "totalMiB")]
  pub total_mib: Option<f64>,
  #[serde(rename = "usedMiB")]
  pub used_mib: Option<f64>,
  #[serde(rename = "freeMiB")]
  pub free_mib: Option<f64>,
  pub units: Vec<ClusterUnit>,
  pub turn: Option<CardTurn>,
  pub queue: Vec<QueuedTicket>,
  pub claim: Option<Claim>,
  pub ram: Ram,
  pub stage: CardStage,
  pub store: Option<Store>,
  /// Every process using the card, or null where the slave cannot read them.
  pub processes: Option<Vec<Process>>,
}

/// One node the master reads.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ClusterNode {
  pub name: String,
  /// The services the node is reserved for, joined by `+`.
  pub role: Option<String>,
  pub desktop: bool,
  /// Its slave's version.
  pub version: Option<String>,
  pub rules: Option<SlaveRules>,
  pub occupants: Vec<String>,
  pub cards: Vec<ClusterCard>,
  pub link: NodeLink,
}

/// A unit the rules declare, so one asked for only on demand is known while no card holds it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DeclaredUnit {
  /// A unit's name or a pattern.
  pub unit: String,
  pub service_name: String,
  pub kind: UnitKind,
  #[serde(rename = "vramMiB")]
  pub vram_mib: Option<f64>,
}

/// How many nodes a service stands on.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum Spread {
  One,
  Many,
}

/// How a service yields its card: by its turn, or by its process going down and coming back.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum YieldBy {
  Turn,
  Process,
}

/// An occupant's tier: the lower takes the card first.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OccupantTier {
  pub name: String,
  pub tier: f64,
}

/// A service the master places, with its declaration and its nodes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PlacedService {
  pub service_name: String,
  #[serde(rename = "cardMiB")]
  pub card_mib: f64,
  pub priority: f64,
  /// The tier its declaration gives, where it gives one.
  pub tier: Option<f64>,
  pub yield_by: YieldBy,
  pub spread: Spread,
  pub alone: bool,
  pub node_names: Vec<String>,
  /// Why its placement is unsettled.
  pub unsettled: Option<String>,
}

/// The placement under way and the step it stands at.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PlacementUnderWay {
  pub kind: PlacementKind,
  pub service_name: String,
  pub node_name: String,
  pub step: Option<String>,
}

/// One note of the picture: when, on which node, the code a reader translates by and the English text a
/// person reads.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Note {
  pub at: i64,
  pub node_name: Option<String>,
  pub code: String,
  pub text: String,
}

/// The whole picture.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Cluster {
  pub master: MasterInfo,
  pub groups: Vec<Group>,
  pub nodes: Vec<ClusterNode>,
  pub declared: Vec<DeclaredUnit>,
  /// Every occupant's tier as the nodes are handed it, lower first.
  pub tiers: Vec<OccupantTier>,
  pub services: Vec<PlacedService>,
  pub placement: Option<PlacementUnderWay>,
  /// The picture's last notes, newest first.
  pub events: Vec<Note>,
}
