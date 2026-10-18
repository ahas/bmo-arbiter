//! The picture of every card the master keeps and its state API answers (docs/placement.md,
//! 「The console」): one node per slave read, its card, the units on it and where those out are, the
//! host memory their dumps take, whose turn it is, the queue, the claim standing and a stage waiting, as
//! the slave's `GET /units` said them. The field names and their order are the console's contract.

use std::collections::BTreeMap;
use std::path::Path;

use serde::Serialize;
use serde_json::Value;

use crate::plan::{PictureNode, PictureUnit, UnitState};
use crate::rules::{NodeFacts, Num};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum UnitPlace {
  Resident,
  Ram,
  Evicted,
  Mixed,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UnitView {
  pub name: String,
  pub service: String,
  pub part: String,
  #[serde(rename = "vramMiB")]
  pub vram_mib: Num,
  /// On the card, out in its owner's host memory, out in the dump store, or some of each.
  pub state: UnitPlace,
  /// The host memory it takes while kept there.
  #[serde(rename = "ramMiB")]
  pub ram_mib: Num,
  pub since: Option<Num>,
  pub evictable: bool,
  pub reason: Option<String>,
  pub load_ms: Option<Num>,
  pub unload_ms: Option<Num>,
}

/// A process using a card, its memory as the driver counts it, and the service its slave knows it as.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessView {
  pub pid: Num,
  pub name: String,
  pub service: Option<String>,
  #[serde(rename = "usedMiB")]
  pub used_mib: Num,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TicketView {
  pub ticket: String,
  pub service: String,
  pub turn: bool,
  pub position: Option<Num>,
  pub waited_s: Num,
  pub wait_s: Option<Num>,
  pub unit: Option<String>,
  #[serde(rename = "vramMiB")]
  pub vram_mib: Option<Num>,
  /// A turn's: whether it is in use, where the slave says, and the service it gives way to now, if any.
  #[serde(skip)]
  pub in_use: Option<bool>,
  #[serde(skip)]
  pub gives_way_to: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Turn {
  pub service: String,
  pub ticket: String,
  pub waited_s: Num,
  /// Whether the turn is in use (given and not resting); none from a slave that does not say.
  pub in_use: Option<bool>,
  /// The service the turn gives way to now, where it does.
  #[serde(skip)]
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

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Claim {
  pub by: Option<String>,
  pub since: Option<Num>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Ram {
  #[serde(rename = "dumpsMiB")]
  pub dumps_mib: Num,
  #[serde(rename = "availableMiB")]
  pub available_mib: Option<Num>,
  #[serde(rename = "maxMiB")]
  pub max_mib: Option<Num>,
  #[serde(rename = "floorMiB")]
  pub floor_mib: Option<Num>,
  pub dump: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Stage {
  pub waiting: bool,
  pub waiting_s: Num,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Store {
  pub path: String,
  pub bytes: Num,
  pub extents: Option<Num>,
  pub slots_used: Num,
  pub error: Option<String>,
}

/// One card's view; a unit's own fields carry what the master adds to it (`U`).
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CardView<U = UnitView> {
  pub index: u32,
  pub name: Option<String>,
  #[serde(rename = "totalMiB")]
  pub total_mib: Option<Num>,
  #[serde(rename = "usedMiB")]
  pub used_mib: Option<Num>,
  #[serde(rename = "freeMiB")]
  pub free_mib: Option<Num>,
  pub units: Vec<U>,
  pub turn: Option<Turn>,
  pub queue: Vec<TicketView>,
  /// The occupant whose claim on the card stands, and since when.
  pub claim: Claim,
  pub ram: Ram,
  pub stage: Stage,
  pub store: Option<Store>,
  /// Every process using the card, or none where the slave cannot read them.
  pub processes: Option<Vec<ProcessView>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LinkState {
  Connected,
  Lost,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Link {
  pub state: LinkState,
  pub since: Option<Num>,
  pub read_at: Option<Num>,
  pub last_read_ms: Option<Num>,
  pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SlaveRules {
  pub loaded_at: Option<Value>,
  pub error: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
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

fn number(value: Option<&Value>) -> Option<Num> {
  value.and_then(Value::as_f64).filter(|one| one.is_finite()).map(Num)
}

fn text(value: Option<&Value>) -> Option<String> {
  value.and_then(Value::as_str).map(str::to_owned)
}

fn items(value: Option<&Value>) -> &[Value] {
  value.and_then(Value::as_array).map(Vec::as_slice).unwrap_or_default()
}

/// The slave's answer read into one card's view; whatever is missing is shown as unknown.
pub fn card_from(answer: &Value, node: &str) -> CardView {
  let card = answer.get("card");
  let at = |value: Option<&Value>, key: &str| value.and_then(|one| one.get(key)).cloned();
  let units = items(answer.get("units"))
    .iter()
    .map(|unit| {
      let state = match unit.get("state").and_then(Value::as_str) {
        Some("evicted") => UnitPlace::Evicted,
        Some("ram") => UnitPlace::Ram,
        Some("mixed") => UnitPlace::Mixed,
        _ => UnitPlace::Resident,
      };
      UnitView {
        name: format!("{}@{node}", text(unit.get("name")).unwrap_or_else(|| "?".into())),
        service: text(unit.get("service")).unwrap_or_else(|| "?".into()),
        part: text(unit.get("part")).unwrap_or_default(),
        vram_mib: number(unit.get("vramMiB")).unwrap_or(Num(0.0)),
        state,
        ram_mib: number(unit.get("ramMiB")).unwrap_or(Num(0.0)),
        since: number(unit.get("since")),
        evictable: unit.get("evictable") == Some(&Value::Bool(true)),
        reason: text(unit.get("reason")),
        load_ms: number(at(unit.get("lastLoad"), "ms").as_ref()),
        unload_ms: number(at(unit.get("lastUnload"), "ms").as_ref()),
      }
    })
    .collect();
  let queue: Vec<TicketView> = items(answer.get("queue"))
    .iter()
    .map(|ticket| {
      let need = ticket.get("need");
      TicketView {
        ticket: text(ticket.get("ticket")).unwrap_or_else(|| "?".into()),
        service: text(ticket.get("service")).unwrap_or_else(|| "?".into()),
        turn: ticket.get("turn") == Some(&Value::Bool(true)),
        position: number(ticket.get("position")),
        waited_s: number(ticket.get("waitedS")).unwrap_or(Num(0.0)),
        wait_s: number(ticket.get("waitS")),
        unit: text(at(need, "unit").as_ref()),
        vram_mib: number(at(need, "vramMiB").as_ref()),
        in_use: ticket.get("inUse").and_then(Value::as_bool),
        gives_way_to: (ticket.get("giveWay") == Some(&Value::Bool(true)))
          .then(|| text(ticket.get("for")))
          .flatten(),
      }
    })
    .collect();
  let turn = queue.iter().find(|ticket| ticket.turn).map(|holder| Turn {
    service: holder.service.clone(),
    ticket: holder.ticket.clone(),
    waited_s: holder.waited_s,
    in_use: holder.in_use,
    gives_way_to: holder.gives_way_to.clone(),
  });
  let claim = answer.get("claim");
  let ram = answer.get("ram");
  let stage = answer.get("stage");
  let store = answer.get("store").filter(|one| !one.is_null()).map(|store| Store {
    path: text(store.get("path")).unwrap_or_default(),
    bytes: number(store.get("bytes")).unwrap_or(Num(0.0)),
    extents: number(store.get("extents")),
    slots_used: number(store.get("slotsUsed")).unwrap_or(Num(0.0)),
    error: text(store.get("error")),
  });
  let processes = card.and_then(|one| one.get("processes")).and_then(Value::as_array).map(|processes| {
    processes
      .iter()
      .map(|process| ProcessView {
        pid: number(process.get("pid")).unwrap_or(Num(0.0)),
        name: text(process.get("name")).unwrap_or_else(|| "?".into()),
        service: text(process.get("service")),
        used_mib: number(process.get("usedMiB")).unwrap_or(Num(0.0)),
      })
      .collect()
  });
  CardView {
    index: 0,
    name: text(at(card, "name").as_ref()),
    total_mib: number(at(card, "totalMiB").as_ref()),
    used_mib: number(at(card, "usedMiB").as_ref()),
    free_mib: number(at(card, "freeMiB").as_ref()),
    units,
    turn,
    queue: queue.into_iter().filter(|ticket| !ticket.turn).collect(),
    claim: Claim {
      by: text(at(claim, "by").as_ref()),
      since: number(at(claim, "since").as_ref()),
    },
    ram: Ram {
      dumps_mib: number(at(ram, "dumpsMiB").as_ref()).unwrap_or(Num(0.0)),
      available_mib: number(at(ram, "availableMiB").as_ref()),
      max_mib: number(at(ram, "maxMiB").as_ref()),
      floor_mib: number(at(ram, "floorMiB").as_ref()),
      dump: text(at(ram, "dump").as_ref()),
    },
    stage: Stage {
      waiting: at(stage, "waiting") == Some(Value::Bool(true)),
      waiting_s: number(at(stage, "waitingS").as_ref()).unwrap_or(Num(0.0)),
    },
    store,
    processes,
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
    claimed_by: card.claim.by.clone(),
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
        load_s: unit.load_ms.map(|ms| ms.0 / 1000.0),
        unload_s: unit.unload_ms.map(|ms| ms.0 / 1000.0),
      })
      .collect(),
  })
}

/// One change of a node for the events: the code a console reads it by, and the English text a person
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

/// What changed between two views of a node, each said once for the events.
pub fn changes(before: Option<&NodeView>, after: &NodeView) -> Vec<Change> {
  let mut said = Vec::new();
  let Some(before) = before else { return said };
  if before.link.state != after.link.state {
    said.push(match after.link.state {
      LinkState::Connected => Change::new("node_reconnected", "the master reaches the node again"),
      LinkState::Lost => match after.link.error.as_deref().filter(|error| !error.is_empty()) {
        Some(error) => Change::new("node_lost", format!("the master lost the node ({error})")),
        None => Change::new("node_lost", "the master lost the node"),
      },
    });
  }
  let (Some(was), Some(now)) = (before.cards.first(), after.cards.first()) else {
    return said;
  };
  if after.link.state != LinkState::Connected {
    return said;
  }
  if was.claim.by != now.claim.by {
    said.push(match (&now.claim.by, &was.claim.by) {
      (Some(by), _) => Change::new("card_claimed", format!("{by} claimed the card")),
      (None, Some(was_by)) => Change::new("card_given_back", format!("{was_by} gave the card back")),
      (None, None) => unreachable!("the claims differ"),
    });
  }
  if was.stage.waiting != now.stage.waiting {
    said.push(if now.stage.waiting {
      Change::new("stage_waiting", "the engine waits at this card's stage")
    } else {
      Change::new(
        "stage_resumed",
        format!(
          "the engine goes on after waiting {} s at this card's stage",
          crate::yaml::js_number(was.stage.waiting_s.0.round())
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
      ("unit_lowered", "moved from RAM to NVMe".to_owned())
    } else {
      match unit.state {
        UnitPlace::Evicted => ("unit_unloaded_to_disk", format!("unloaded to NVMe{why}")),
        UnitPlace::Ram => ("unit_unloaded_to_ram", format!("unloaded to RAM{why}")),
        UnitPlace::Resident => ("unit_loaded", "loaded".to_owned()),
        UnitPlace::Mixed => ("unit_partly_unloaded", "partly unloaded".to_owned()),
      }
    };
    said.push(Change::new(code, format!("{} {moved}", unit.name)));
  }
  said
}

/// What a unit of a layered model holds on its card (docs/placement.md, 「What a service says of
/// its units」).
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UnitDetail {
  pub blocks: Option<Blocks>,
  pub output: bool,
  pub mtp: bool,
  pub full_attention: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Blocks {
  pub from: u64,
  pub to: u64,
}

/// The folder's name beside the placement rules.
pub const DESCRIPTIONS_FOLDER: &str = "card-units";

fn count(value: Option<&Value>) -> Option<u64> {
  value.and_then(Value::as_u64)
}

/// One unit's detail as a file gives it, or why it cannot be taken.
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
