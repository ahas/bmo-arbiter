//! The node arbiter's card, its tickets and its card units (docs/api.md, 「The card」, 「Tickets」 and
//! 「Card units」): each as every route answers it, and the bodies and queries that ask for a turn, rest it and
//! move a unit.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::page::page;

/// Where a unit stands: on the card, out in its owner's host memory, out in the dump store, or some of each.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum UnitState {
  Resident,
  Ram,
  Evicted,
  Mixed,
}

impl UnitState {
  /// The state a word names, or none where it names none.
  pub fn of_word(word: &str) -> Option<Self> {
    match word {
      "resident" => Some(Self::Resident),
      "ram" => Some(Self::Ram),
      "evicted" => Some(Self::Evicted),
      "mixed" => Some(Self::Mixed),
      _ => None,
    }
  }
}

/// The claim standing on a card: the claimant that takes it outright, and since when.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Claim {
  pub claimant_name: String,
  /// When the claim began, in epoch milliseconds.
  pub claimed_at: Option<i64>,
}

/// The host memory the dumps take, and what the node's rules let them take.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Ram {
  #[serde(rename = "dumpsMiB")]
  pub dumps_mib: f64,
  #[serde(rename = "availableMiB")]
  pub available_mib: Option<f64>,
  #[serde(rename = "maxMiB")]
  pub max_mib: Option<f64>,
  #[serde(rename = "floorMiB")]
  pub floor_mib: Option<f64>,
  /// Where a unit goes when it is unloaded and no rule names its part: `ram` or `disk`.
  pub dump: Option<String>,
}

/// The dump store on the node's NVMe.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Store {
  pub path: String,
  pub bytes: i64,
  pub extents: Option<i64>,
  pub slots_used: i64,
  pub bytes_used: i64,
  pub error: Option<String>,
}

/// A process using the card, its memory as the driver counts it, and the service it is known as.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Process {
  pub pid: i64,
  pub name: String,
  pub service_name: Option<String>,
  #[serde(rename = "usedMiB")]
  pub used_mib: f64,
}

/// The card as its driver says it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Device {
  pub name: String,
  #[serde(rename = "totalMiB")]
  pub total_mib: f64,
  #[serde(rename = "usedMiB")]
  pub used_mib: f64,
  #[serde(rename = "freeMiB")]
  pub free_mib: f64,
  /// Every process on the card, or null where the driver gives no count per process.
  pub processes: Option<Vec<Process>>,
}

/// An owner's door: whether a use of the card waits there with its units out, how long, and the waits so far.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Door {
  pub waiting: bool,
  pub waiting_ms: i64,
  pub graphs: i64,
  pub waits: i64,
  pub waited_ms: i64,
}

/// When something was last read, in epoch milliseconds, and why it could not be.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RulesRead {
  pub loaded_at: Option<i64>,
  pub error: Option<String>,
}

/// One buffer of a unit as its owner lists it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Buffer {
  pub id: i64,
  pub bytes: i64,
  /// Where it is: `resident`, `ram` or `evicted`.
  pub state: String,
  /// Whether its slot in the dump store holds its contents.
  pub clean: bool,
  pub ram_bytes: i64,
  /// Where its contents are kept in the dump store.
  pub slot: Option<Slot>,
}

/// A buffer's place in the dump store.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Slot {
  pub offset: i64,
  pub length: i64,
}

/// One move of a unit: how long it took, how much it moved, when, and where it went.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UnitTiming {
  pub took_ms: i64,
  pub bytes: i64,
  /// When it ended, in epoch milliseconds.
  pub at: i64,
  /// Where an unload went: `ram` or `disk`.
  pub to: Option<String>,
}

/// One unit of a card's memory, as the node arbiter keeps it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CardUnit {
  /// `<service>/<part>`.
  pub name: String,
  /// The service whose engine holds it.
  pub holder_name: String,
  pub part: String,
  pub state: UnitState,
  pub evictable: bool,
  /// When it came into its state, in epoch milliseconds.
  pub since_at: Option<i64>,
  #[serde(rename = "vramMiB")]
  pub vram_mib: f64,
  #[serde(rename = "ramMiB")]
  pub ram_mib: f64,
  /// Why it is out.
  pub reason: Option<String>,
  pub buffers: Vec<Buffer>,
  pub last_unload: Option<UnitTiming>,
  pub last_load: Option<UnitTiming>,
  pub last_lower: Option<UnitTiming>,
  /// Its own owner's door, for an owner besides the lent one.
  pub door: Option<Door>,
}

page!(
  CardUnitPage,
  CardUnit,
  "A page of card units: the lent owner's, then every other owner's."
);

/// Where a move takes a unit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum UnitMove {
  /// Off the card, into its owner's host memory or the dump store as the node's rules place it.
  Evicted,
  /// Back onto the card.
  Resident,
}

/// A move of one unit.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CardUnitChanges {
  pub state: UnitMove,
  /// Why it moves, shown on the unit while it is out.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub reason: Option<String>,
  /// More of the same holder's units go out after this one: its hold keeps its lock and turn until the last
  /// of them is out, rather than letting them go once this one is.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub keeps_turn: Option<bool>,
}

/// What a ticket says it needs: shown, weighing nothing, but for the unit, which the master places by the
/// rules.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Need {
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub gpus: Option<i64>,
  #[serde(default, rename = "vramMiB", skip_serializing_if = "Option::is_none")]
  pub vram_mib: Option<i64>,
  /// The unit asked for, `<service>/<part>`.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub unit_name: Option<String>,
}

/// A ticket for one turn on the card, as every route answers it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Ticket {
  pub id: i64,
  pub service_name: String,
  /// Its turn is given.
  pub turn: bool,
  /// Its turn is given and does not rest.
  pub in_use: bool,
  pub resting: bool,
  /// It is told to let go.
  pub give_way: bool,
  /// The occupant or claimant it lets go for.
  pub give_way_for: Option<String>,
  /// Its place in line, while it waits.
  pub position: Option<i64>,
  /// How long it is counted to wait, while it waits.
  pub wait_ms: Option<i64>,
  /// The claimant whose claim stands.
  pub claimant_name: Option<String>,
  /// How long it has stood, backdated by the wait it was asked with.
  pub waited_ms: i64,
  pub need: Option<Need>,
  #[serde(rename = "ref")]
  pub reference: Option<String>,
  /// It was asked only to rest on the card once it is free.
  pub rest: bool,
  /// Whether the turn carries on to the next request, where `nextWaitedMs` asked.
  pub keep: Option<bool>,
}

/// A ticket asked for.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TicketAsk {
  pub service_name: String,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub need: Option<Need>,
  /// A reference such as a task id; shown.
  #[serde(default, rename = "ref", skip_serializing_if = "Option::is_none")]
  pub reference: Option<String>,
  /// How long it takes to let go once told, more than zero; a turn asked without it holds to its end.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub gives_way_ms: Option<i64>,
  /// How long the request it stands for has waited.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub waited_ms: Option<i64>,
  /// How long to hold the answer open for the turn.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub wait_ms: Option<i64>,
  /// No request in hand: only to rest on the card once it is free.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub rest: Option<bool>,
}

/// How a ticket is asked after.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TicketQuery {
  /// How long to hold the answer open until the turn is given, it is told to give way, or the time runs
  /// out.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub wait_ms: Option<u64>,
  /// How long the next request has waited, asking whether the turn carries on to it.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub next_waited_ms: Option<u64>,
}

/// A change to a ticket: its turn resting from now.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TicketChanges {
  /// `true` rests the turn; a rest ends only by `nextWaitedMs`.
  pub resting: bool,
}

/// The card as the node arbiter keeps it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Card {
  pub version: String,
  pub desktop: bool,
  /// The names that may be on the card, highest first, the claimants standing first first.
  pub occupants: Vec<String>,
  /// The same names grouped by rank, which are the tiers where the rules carry them.
  pub ranks: Vec<Vec<String>>,
  /// Each occupant's tier as the node's part of the rules carries it; empty where it carries none.
  #[serde(default)]
  pub tiers: BTreeMap<String, f64>,
  pub claim: Option<Claim>,
  /// The pids holding the card lock.
  pub lock_held_by: Vec<i64>,
  pub busy: bool,
  /// Every ticket, in line order.
  pub queue: Vec<Ticket>,
  /// By service, how long its turns have held the card in use.
  pub typical_hold_ms: BTreeMap<String, i64>,
  pub units: Vec<CardUnit>,
  pub device: Option<Device>,
  /// The lent owner's door.
  pub stage: Door,
  pub store: Option<Store>,
  pub ram: Ram,
  /// The node's part of the rules as last read.
  pub rules: RulesRead,
}
