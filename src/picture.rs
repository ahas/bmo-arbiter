//! The picture of every card the master keeps (docs/placement.md, 「The console」): one node per slave read,
//! its card, the units on it and where those out are, the host memory their dumps take, whose turn it is,
//! the queue, the claim standing and a stage waiting, as the slave's `GET /card` said them; and what changed
//! between two readings, said among the notes.

use std::collections::BTreeMap;
use std::path::Path;

pub use bmo_arbiter_client::card::UnitState as UnitPlace;
use bmo_arbiter_client::card::{Card, Claim, Process, Ram, Store};
pub use bmo_arbiter_client::cluster::LinkState;
pub use bmo_arbiter_client::descriptions::{Blocks, UnitDetail};
use serde_json::Value;

use crate::plan::{PictureNode, PictureUnit, UnitState};
use crate::rules::{NodeFacts, Num};

/// A unit as its slave said it, named with its node.
#[derive(Debug, Clone, PartialEq)]
pub struct UnitView {
  /// `<service>/<part>@<node>`.
  pub name: String,
  pub service: String,
  pub part: String,
  pub vram_mib: Num,
  /// On the card, out in its owner's host memory, out in the dump store, or some of each.
  pub state: UnitPlace,
  /// The host memory it takes while kept there.
  pub ram_mib: Num,
  /// When it came into its state, in epoch milliseconds.
  pub since_at: Option<i64>,
  pub evictable: bool,
  pub reason: Option<String>,
  pub load_ms: Option<i64>,
  pub unload_ms: Option<i64>,
}

/// A ticket in a card's line, its id as the master's reasons write it.
#[derive(Debug, Clone, PartialEq)]
pub struct TicketView {
  pub ticket: String,
  pub service: String,
  pub turn: bool,
  pub position: Option<i64>,
  pub waited_ms: i64,
  pub wait_ms: Option<i64>,
  pub unit: Option<String>,
  pub vram_mib: Option<Num>,
  /// A turn's: whether it is in use, and the service it gives way to now, if any.
  pub in_use: Option<bool>,
  pub gives_way_to: Option<String>,
}

/// The turn standing on a card.
#[derive(Debug, Clone, PartialEq)]
pub struct Turn {
  pub service: String,
  pub ticket: String,
  pub waited_ms: i64,
  /// Whether the turn is in use (given and not resting).
  pub in_use: Option<bool>,
  /// The service the turn gives way to now, where it does.
  pub gives_way_to: Option<String>,
}

impl Turn {
  /// Whether the card's turn may pass to a ticket of `service`, so another occupant's units may be unloaded for
  /// it (docs/placement.md, 「The engine's turn」): a turn of its own service, one not in use, or one that
  /// gives way to it.
  pub fn passes_to(&self, service: &str) -> bool {
    self.service == service || self.in_use != Some(true) || self.gives_way_to.as_deref() == Some(service)
  }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Stage {
  pub waiting: bool,
  pub waiting_ms: i64,
}

/// One card's view; a unit's own fields carry what the master adds to it (`U`).
#[derive(Debug, Clone, PartialEq)]
pub struct CardView<U = UnitView> {
  pub index: u32,
  pub name: Option<String>,
  pub total_mib: Option<Num>,
  pub used_mib: Option<Num>,
  pub free_mib: Option<Num>,
  pub units: Vec<U>,
  pub turn: Option<Turn>,
  pub queue: Vec<TicketView>,
  /// The occupant whose claim on the card stands, and since when.
  pub claim: Option<Claim>,
  pub ram: Ram,
  pub stage: Stage,
  pub store: Option<Store>,
  /// Every process using the card, or none where the slave cannot read them.
  pub processes: Option<Vec<Process>>,
}

/// When the master's link to a slave came into its state and was last read, in the master's clock's seconds.
#[derive(Debug, Clone, PartialEq)]
pub struct Link {
  pub state: LinkState,
  pub since: Option<Num>,
  pub read_at: Option<Num>,
  pub last_read_ms: Option<Num>,
  pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SlaveRules {
  pub loaded_at: Option<i64>,
  pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NodeView<U = UnitView> {
  pub name: String,
  pub role: Option<String>,
  pub desktop: bool,
  pub version: Option<String>,
  pub rules: Option<SlaveRules>,
  pub occupants: Vec<String>,
  pub cards: Vec<CardView<U>>,
  pub link: Link,
}

/// The slave's card read into one card's view.
pub fn card_from(card: &Card, node: &str) -> CardView {
  let units = card
    .units
    .iter()
    .map(|unit| UnitView {
      name: format!("{}@{node}", unit.name),
      service: unit.holder_name.clone(),
      part: unit.part.clone(),
      vram_mib: Num(unit.vram_mib),
      state: unit.state,
      ram_mib: Num(unit.ram_mib),
      since_at: unit.since_at,
      evictable: unit.evictable,
      reason: unit.reason.clone(),
      load_ms: unit.last_load.as_ref().map(|load| load.took_ms),
      unload_ms: unit.last_unload.as_ref().map(|unload| unload.took_ms),
    })
    .collect();
  let queue: Vec<TicketView> = card
    .queue
    .iter()
    .map(|ticket| {
      let need = ticket.need.as_ref();
      TicketView {
        ticket: ticket.id.to_string(),
        service: ticket.service_name.clone(),
        turn: ticket.turn,
        position: ticket.position,
        waited_ms: ticket.waited_ms,
        wait_ms: ticket.wait_ms,
        unit: need.and_then(|need| need.unit_name.clone()),
        vram_mib: need.and_then(|need| need.vram_mib).map(|mib| Num(mib as f64)),
        in_use: Some(ticket.in_use),
        gives_way_to: ticket.give_way.then(|| ticket.give_way_for.clone()).flatten(),
      }
    })
    .collect();
  let turn = queue.iter().find(|ticket| ticket.turn).map(|holder| Turn {
    service: holder.service.clone(),
    ticket: holder.ticket.clone(),
    waited_ms: holder.waited_ms,
    in_use: holder.in_use,
    gives_way_to: holder.gives_way_to.clone(),
  });
  let device = card.device.as_ref();
  CardView {
    index: 0,
    name: device.map(|device| device.name.clone()),
    total_mib: device.map(|device| Num(device.total_mib)),
    used_mib: device.map(|device| Num(device.used_mib)),
    free_mib: device.map(|device| Num(device.free_mib)),
    units,
    turn,
    queue: queue.into_iter().filter(|ticket| !ticket.turn).collect(),
    claim: card.claim.clone(),
    ram: card.ram.clone(),
    stage: Stage {
      waiting: card.stage.waiting,
      waiting_ms: card.stage.waiting_ms,
    },
    store: card.store.clone(),
    processes: device.and_then(|device| device.processes.clone()),
  }
}

/// A node as the planner reads it: its facts, its card's free memory and its units.
pub fn plannable(node: &NodeView) -> Option<PictureNode> {
  let card = node.cards.first()?;
  Some(PictureNode {
    facts: NodeFacts {
      name: node.name.clone(),
      role: node.role.clone(),
      desktop: node.desktop,
    },
    free_mib: card.free_mib?.0,
    total_mib: card.total_mib?.0,
    claimed_by: card.claim.as_ref().map(|claim| claim.claimant_name.clone()),
    units: card
      .units
      .iter()
      .map(|unit| PictureUnit {
        name: unit.name.clone(),
        service: unit.service.clone(),
        vram_mib: unit.vram_mib.0,
        state: match unit.state {
          UnitPlace::Resident => UnitState::Resident,
          UnitPlace::Ram => UnitState::Ram,
          UnitPlace::Evicted => UnitState::Evicted,
          UnitPlace::Mixed => UnitState::Mixed,
        },
        evictable: unit.evictable,
        load_s: unit.load_ms.map(|ms| ms as f64 / 1000.0),
        unload_s: unit.unload_ms.map(|ms| ms as f64 / 1000.0),
      })
      .collect(),
  })
}

/// One change of a node for the notes: the code a reader translates by, and the English text a person
/// reads (docs/api.md, 「Messages and codes」).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
  pub code: &'static str,
  pub text: String,
}

impl Change {
  fn new(code: &'static str, text: impl Into<String>) -> Self {
    Self {
      code,
      text: text.into(),
    }
  }
}

fn claimant(card: &CardView) -> Option<&str> {
  card.claim.as_ref().map(|claim| claim.claimant_name.as_str())
}

/// What changed between two views of a node, each said once for the notes.
pub fn changes(before: Option<&NodeView>, after: &NodeView) -> Vec<Change> {
  let mut said = Vec::new();
  let Some(before) = before else { return said };
  if before.link.state != after.link.state {
    said.push(match after.link.state {
      LinkState::Connected => Change::new("node-reconnected", "the master reaches the node again"),
      LinkState::Lost => match after.link.error.as_deref().filter(|error| !error.is_empty()) {
        Some(error) => Change::new("node-lost", format!("the master lost the node ({error})")),
        None => Change::new("node-lost", "the master lost the node"),
      },
    });
  }
  let (Some(was), Some(now)) = (before.cards.first(), after.cards.first()) else {
    return said;
  };
  if after.link.state != LinkState::Connected {
    return said;
  }
  if claimant(was) != claimant(now) {
    said.push(match (claimant(now), claimant(was)) {
      (Some(by), _) => Change::new("card-claimed", format!("{by} claimed the card")),
      (None, Some(was_by)) => Change::new("card-given-back", format!("{was_by} gave the card back")),
      (None, None) => unreachable!("the claims differ"),
    });
  }
  if was.stage.waiting != now.stage.waiting {
    said.push(if now.stage.waiting {
      Change::new("stage-waiting", "the engine waits at this card's stage")
    } else {
      Change::new(
        "stage-resumed",
        format!(
          "the engine goes on after waiting {} s at this card's stage",
          crate::yaml::js_number((was.stage.waiting_ms as f64 / 1000.0).round())
        ),
      )
    });
  }
  let states: BTreeMap<&str, UnitPlace> =
    was.units.iter().map(|unit| (unit.name.as_str(), unit.state)).collect();
  for unit in &now.units {
    let Some(old) = states.get(unit.name.as_str()).copied() else { continue };
    if old == unit.state {
      continue;
    }
    let why = unit
      .reason
      .as_deref()
      .filter(|reason| !reason.is_empty())
      .map(|reason| format!(" ({reason})"))
      .unwrap_or_default();
    let (code, moved) = if old == UnitPlace::Ram && unit.state == UnitPlace::Evicted {
      ("unit-lowered", "moved from RAM to NVMe".to_owned())
    } else {
      match unit.state {
        UnitPlace::Evicted => ("unit-unloaded-to-disk", format!("unloaded to NVMe{why}")),
        UnitPlace::Ram => ("unit-unloaded-to-ram", format!("unloaded to RAM{why}")),
        UnitPlace::Resident => ("unit-loaded", "loaded".to_owned()),
        UnitPlace::Mixed => ("unit-partly-unloaded", "partly unloaded".to_owned()),
      }
    };
    said.push(Change::new(code, format!("{} {moved}", unit.name)));
  }
  said
}

/// The folder's name beside the placement rules.
pub const DESCRIPTIONS_FOLDER: &str = "card-units";

fn count(value: Option<&Value>) -> Option<u64> {
  value.and_then(Value::as_u64)
}

/// One unit's detail as a file or a body gives it, or why it cannot be taken.
pub fn detail_from(value: &Value) -> Result<UnitDetail, String> {
  if !value.is_object() {
    return Err("a unit's description is not an object".into());
  }
  let blocks = match value.get("blocks").filter(|one| !one.is_null()) {
    None => None,
    Some(range) => match (count(range.get("from")), count(range.get("to"))) {
      (Some(from), Some(to)) if to >= from => Some(Blocks { from, to }),
      _ => return Err("blocks must be two block numbers, from ≤ to".into()),
    },
  };
  let full = value.get("fullAttention").filter(|one| !one.is_null());
  if full.is_some() && count(full).is_none() {
    return Err("fullAttention must be a number of blocks".into());
  }
  Ok(UnitDetail {
    blocks,
    output: value.get("output") == Some(&Value::Bool(true)),
    mtp: value.get("mtp") == Some(&Value::Bool(true)),
    full_attention: count(full),
  })
}

fn json_names(folder: &Path) -> Vec<String> {
  let Ok(read) = std::fs::read_dir(folder) else { return Vec::new() };
  let mut names: Vec<String> = read
    .flatten()
    .filter_map(|entry| entry.file_name().into_string().ok())
    .filter(|name| name.ends_with(".json"))
    .collect();
  names.sort();
  names
}

/// Every unit's detail out of the folder, by its full name, and each file that could not be taken with
/// why; a folder that does not exist describes nothing.
pub fn read_descriptions(folder: &Path) -> (BTreeMap<String, UnitDetail>, Vec<String>) {
  let mut details = BTreeMap::new();
  let mut faults = Vec::new();
  for name in json_names(folder) {
    let read: Value = match std::fs::read_to_string(folder.join(&name))
      .map_err(|fault| fault.to_string())
      .and_then(|source| serde_json::from_str(&source).map_err(|fault| fault.to_string()))
    {
      Ok(read) => read,
      Err(fault) => {
        faults.push(format!("{name}: {fault}"));
        continue;
      }
    };
    let Some(units) = read.get("units").and_then(Value::as_object) else {
      faults.push(format!("{name}: it holds no units object"));
      continue;
    };
    for (unit, value) in units {
      match detail_from(value) {
        Ok(detail) => {
          details.insert(unit.clone(), detail);
        }
        Err(fault) => faults.push(format!("{name}: {unit} — {fault}")),
      }
    }
  }
  (details, faults)
}

/// The folder's files and when each last changed, to tell whether it needs reading again.
pub fn descriptions_mark(folder: &Path) -> String {
  json_names(folder)
    .into_iter()
    .map(|name| {
      let changed = std::fs::metadata(folder.join(&name)).and_then(|meta| meta.modified()).ok();
      format!("{name}:{changed:?}")
    })
    .collect::<Vec<_>>()
    .join("|")
}
