//! The placement rules the card master reads (docs/placement.md, 「The rules」): the occupants
//! that take the card outright and how each is seen, groups of nodes chosen by their row in the node
//! list, the units, where each goes when it is unloaded and what it costs to move, and the rules that
//! say what goes when a unit is asked for. Every value is checked as it is read, and a fault names the
//! file and the place in it, in the words the people who write the file read.
//!
//! What a node is pushed is this same shape as JSON, read by the slave on the node, so the names and
//! the order of the fields are the slave's contract.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde::ser::Serializer;

use crate::yaml::{self, Yaml};

pub const RULES_FILE: &str = "card-rules.yaml";

// The default rank of a unit the rules give none.
pub const DEFAULT_PRIORITY: f64 = 50.0;

/// The tier of an occupant neither the rules nor its declaration give one (docs/placement.md, 「Tiers」).
pub const DEFAULT_TIER: f64 = 2.0;

/// The tier of a claimant whose entry gives none: it takes the card outright.
pub const CLAIMANT_TIER: f64 = 0.0;

/// The rules file under a data root's placements.
pub fn rules_path(data_root: &Path) -> PathBuf {
  data_root.join("placements").join(RULES_FILE)
}

/// A number as the file gave it, written back the way a script writes one: whole numbers without a point.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Num(pub f64);

impl Serialize for Num {
  fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
    if self.0.fract() == 0.0 && self.0.abs() < 9.0e15 {
      serializer.serialize_i64(self.0 as i64)
    } else {
      serializer.serialize_f64(self.0)
    }
  }
}

impl std::fmt::Display for Num {
  fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
    formatter.write_str(&yaml::js_number(self.0))
  }
}

/// What chooses nodes: the service their row reserves them for, and whether the machine is a desktop.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct Selector {
  #[serde(skip_serializing_if = "Option::is_none")]
  pub role: Option<String>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub desktop: Option<bool>,
}

/// Where a unit goes when it is unloaded: kept in its owner's host memory, or written to the dump store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Dump {
  Ram,
  Disk,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeGroup {
  pub name: String,
  pub select: Selector,
  /// The group's cards counted as one when asking whether a unit fits anywhere.
  pub pool: bool,
  #[serde(rename = "reserveMiB")]
  pub reserve_mib: Num,
  /// Where its nodes' units go when unloaded, unless a unit's own entry says otherwise.
  pub dump: Option<Dump>,
  /// The most host memory the dumps kept there may take together on one node.
  #[serde(rename = "ramMaxGiB")]
  pub ram_max_gib: Option<Num>,
  /// The size of each of its nodes' dump store; none leaves the install's own default.
  #[serde(rename = "storeGiB")]
  pub store_gib: Option<Num>,
}

/// How a claim of a claimant is seen: its own tickets, its running containers named by a prefix, and
/// its processes on the card. Nothing else that runs on a node claims for it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SeenBy {
  /// A ticket asked under its name.
  pub tickets: bool,
  /// A unit whose processes on the card are the claimant's; for a claimant neither of the others sees.
  pub cgroup: Option<String>,
  /// The prefix of the names of its containers.
  pub containers: Option<String>,
}

/// The host memory a claimant keeps available: a size in GiB, or every dump in host memory written out.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RamFree {
  GiB(Num),
  All,
}

impl Serialize for RamFree {
  fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
    match self {
      RamFree::GiB(size) => size.serialize(serializer),
      RamFree::All => serializer.serialize_str("all"),
    }
  }
}

/// An occupant that takes the card outright when it comes: the slave holds the lock for it and
/// everything below it gives way.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Claimant {
  pub name: String,
  pub claims: bool,
  /// Standing first on every node whatever the node's own order says.
  pub first: bool,
  pub seen_by: SeenBy,
  pub ram_free: RamFree,
  pub tier: Num,
}

/// What one entry of the rules' `occupants` says: a claimant, or an occupant's tier alone.
enum OccupantEntry {
  Claimant(Claimant),
  Tier(String, Num),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum UnitKind {
  Resident,
  OnDemand,
  Fill,
}

impl From<UnitKind> for bmo_arbiter_client::cluster::UnitKind {
  fn from(kind: UnitKind) -> Self {
    match kind {
      UnitKind::Resident => Self::Resident,
      UnitKind::OnDemand => Self::OnDemand,
      UnitKind::Fill => Self::Fill,
    }
  }
}

impl UnitKind {
  pub fn word(self) -> &'static str {
    match self {
      UnitKind::Resident => "resident",
      UnitKind::OnDemand => "on-demand",
      UnitKind::Fill => "fill",
    }
  }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Hold {
  pub allowed: bool,
  pub max_wait_s: Num,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UnitSpec {
  /// A unit name without its node, or a pattern with `*`.
  pub unit: String,
  pub service: String,
  #[serde(rename = "where")]
  pub place: Option<Selector>,
  pub kind: UnitKind,
  pub priority: Num,
  pub hold: Hold,
  pub group: Option<String>,
  pub apart: Vec<String>,
  #[serde(rename = "vramMiB")]
  pub vram_mib: Option<Num>,
  /// Measured times, or none where moving it is counted by its size.
  pub load_s: Option<Num>,
  pub unload_s: Option<Num>,
  pub measured_at: Option<String>,
  /// Where it goes when unloaded, over its node group's.
  pub dump: Option<Dump>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Eviction {
  List(Vec<String>),
  Node,
  Cheapest(Vec<String>),
  Below(String),
}

impl Serialize for Eviction {
  fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
    use serde::ser::SerializeMap;
    let mut map = serializer.serialize_map(Some(1))?;
    match self {
      Eviction::List(units) => map.serialize_entry("list", units)?,
      Eviction::Node => map.serialize_entry("node", &true)?,
      Eviction::Cheapest(units) => map.serialize_entry("cheapest", units)?,
      Eviction::Below(occupant) => map.serialize_entry("below", occupant)?,
    }
    map.end()
  }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct When {
  #[serde(skip_serializing_if = "Option::is_none")]
  pub request: Option<String>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub desktop: Option<&'static str>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub on: Option<Selector>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Rule {
  pub name: String,
  pub when: When,
  pub evict: Option<Eviction>,
  pub place: Option<Selector>,
  pub restore: bool,
}

/// How a service takes turns with one of its rank (docs/node-arbiter.md, 「One rank taking turns」), as its
/// declaration says it and its node is handed it.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnValues {
  pub yield_after_s: Num,
  pub can_pause: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct Rules {
  pub occupants: Vec<Claimant>,
  pub groups: Vec<NodeGroup>,
  pub units: Vec<UnitSpec>,
  pub rules: Vec<Rule>,
  /// Every declared service's turn values, keyed by service: no rules file names them, the master writes
  /// them from the declarations, and every node is handed them whole.
  #[serde(skip_serializing_if = "BTreeMap::is_empty")]
  pub turns: BTreeMap<String, TurnValues>,
  /// Every occupant's tier, keyed by name (docs/placement.md, 「Tiers」): read from the file the tiers its
  /// `occupants` give, and with the declarations every declared service's; every node is handed them whole.
  #[serde(skip_serializing_if = "BTreeMap::is_empty")]
  pub tiers: BTreeMap<String, Num>,
}

/// A rules file that will not read: the file and what is wrong where.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RulesError(pub String);

impl std::fmt::Display for RulesError {
  fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
    formatter.write_str(&self.0)
  }
}

impl std::error::Error for RulesError {}

type Read<T> = Result<T, RulesError>;

struct Reader<'a> {
  file: &'a str,
}

impl Reader<'_> {
  fn fail<T>(&self, why: String) -> Read<T> {
    Err(RulesError(format!("{}: {why}", self.file)))
  }

  fn object<'v>(&self, value: &'v Yaml, place: &str) -> Read<&'v [(String, Yaml)]> {
    match value {
      Yaml::Map(entries) => Ok(entries),
      _ => self.fail(format!("{place} must be a map of names to values")),
    }
  }

  fn list<'v>(&self, value: Option<&'v Yaml>, place: &str) -> Read<&'v [Yaml]> {
    match value {
      None | Some(Yaml::Null) => Ok(&[]),
      Some(Yaml::List(items)) => Ok(items),
      Some(_) => self.fail(format!("{place} must be a list")),
    }
  }

  fn positive(&self, value: Option<&Yaml>, place: &str) -> Read<Num> {
    match value {
      Some(Yaml::Number(one)) if one.is_finite() && *one > 0.0 => Ok(Num(*one)),
      _ => self.fail(format!("{place} must be a number above 0")),
    }
  }

  fn number(&self, value: Option<&Yaml>, place: &str, fallback: f64) -> Read<Num> {
    match value {
      None | Some(Yaml::Null) => Ok(Num(fallback)),
      Some(Yaml::Number(one)) if one.is_finite() && *one >= 0.0 => Ok(Num(*one)),
      Some(_) => self.fail(format!("{place} must be a number of 0 or more")),
    }
  }

  fn text(&self, value: Option<&Yaml>, place: &str) -> Read<String> {
    match value {
      Some(Yaml::Text(one)) if !one.trim().is_empty() => Ok(one.trim().to_owned()),
      _ => self.fail(format!("{place} must be text that is not empty")),
    }
  }

  fn dump(&self, value: Option<&Yaml>, place: &str) -> Read<Option<Dump>> {
    match value {
      None | Some(Yaml::Null) => Ok(None),
      Some(Yaml::Text(one)) if one == "ram" => Ok(Some(Dump::Ram)),
      Some(Yaml::Text(one)) if one == "disk" => Ok(Some(Dump::Disk)),
      Some(_) => self.fail(format!("{place} must be ram or disk")),
    }
  }

  fn flag(&self, value: Option<&Yaml>, place: &str, fallback: bool) -> Read<bool> {
    match value {
      None | Some(Yaml::Null) => Ok(fallback),
      Some(Yaml::Bool(one)) => Ok(*one),
      Some(_) => self.fail(format!("{place} must be true or false")),
    }
  }

  fn occupant(&self, value: &Yaml, place: &str) -> Read<OccupantEntry> {
    let keys = self.object(value, place)?;
    let name = self.text(value.get("name"), &format!("{place}.name"))?;
    if !occupant_word(&name) {
      return self.fail(format!(
        "{place}.name must be a word an occupants file holds (lower-case letters, digits and -)"
      ));
    }
    let claims = self.flag(value.get("claims"), &format!("{place}.claims"), false)?;
    if !claims {
      if let Some((key, _)) =
        keys.iter().find(|(key, _)| !["name", "tier", "claims"].contains(&key.as_str()))
      {
        return self.fail(format!(
          "{place}.{key} belongs to an occupant that takes the whole card (claims: true); one that does not says its tier alone"
        ));
      }
      let tier = self.number(value.get("tier"), &format!("{place}.tier"), DEFAULT_TIER)?;
      return Ok(OccupantEntry::Tier(name, tier));
    }
    let empty = Yaml::Map(Vec::new());
    let seen = match value.get("seen_by") {
      None => &empty,
      Some(given) => {
        self.object(given, &format!("{place}.seen_by"))?;
        given
      }
    };
    let ram_free = match value.get("ram_free") {
      None | Some(Yaml::Null) => RamFree::GiB(Num(0.0)),
      Some(Yaml::Text(word)) if word == "all" => RamFree::All,
      Some(Yaml::Number(size)) if size.is_finite() && *size >= 0.0 => RamFree::GiB(Num(*size)),
      Some(_) => {
        return self.fail(format!("{place}.ram_free must be a number of GiB of 0 or more, or all"));
      }
    };
    let optional_text = |key: &str| -> Read<Option<String>> {
      match seen.get(key) {
        None => Ok(None),
        given => self.text(given, &format!("{place}.seen_by.{key}")).map(Some),
      }
    };
    Ok(OccupantEntry::Claimant(Claimant {
      name,
      claims: true,
      first: self.flag(value.get("first"), &format!("{place}.first"), true)?,
      seen_by: SeenBy {
        tickets: self.flag(seen.get("tickets"), &format!("{place}.seen_by.tickets"), true)?,
        cgroup: optional_text("cgroup")?,
        containers: optional_text("containers")?,
      },
      ram_free,
      tier: self.number(value.get("tier"), &format!("{place}.tier"), CLAIMANT_TIER)?,
    }))
  }

  fn selector(&self, value: Option<&Yaml>, place: &str) -> Read<Selector> {
    let given = match value {
      Some(one) => one,
      None => return self.fail(format!("{place} must be a map of names to values")),
    };
    let mut chosen = Selector::default();
    for (name, one) in self.object(given, place)? {
      match name.as_str() {
        "role" => chosen.role = Some(self.text(Some(one), &format!("{place}.role"))?),
        "desktop" => match one {
          Yaml::Bool(flag) => chosen.desktop = Some(*flag),
          _ => return self.fail(format!("{place}.desktop must be true or false")),
        },
        other => {
          return self
            .fail(format!("{place} chooses nodes by role and desktop alone; {other} is no name it knows"));
        }
      }
    }
    Ok(chosen)
  }

  fn eviction(&self, value: Option<&Yaml>, place: &str) -> Read<Option<Eviction>> {
    let given = match value {
      None | Some(Yaml::Null) => return Ok(None),
      Some(one) => one,
    };
    if *given == Yaml::Text("node".into()) {
      return Ok(Some(Eviction::Node));
    }
    if let Yaml::List(items) = given {
      let named = items
        .iter()
        .enumerate()
        .map(|(at, one)| self.text(Some(one), &format!("{place}[{at}]")))
        .collect::<Read<Vec<_>>>()?;
      return Ok(Some(Eviction::List(named)));
    }
    self.object(given, place)?;
    if let Some(cheapest) = given.get("cheapest") {
      let named = self
        .list(Some(cheapest), &format!("{place}.cheapest"))?
        .iter()
        .enumerate()
        .map(|(at, one)| self.text(Some(one), &format!("{place}.cheapest[{at}]")))
        .collect::<Read<Vec<_>>>()?;
      return Ok(Some(Eviction::Cheapest(named)));
    }
    if let Some(below) = given.get("below") {
      return Ok(Some(Eviction::Below(self.text(Some(below), &format!("{place}.below"))?)));
    }
    self.fail(format!(
      "{place} must be node, a list of units, {{cheapest: […]}} or {{below: <occupant>}}"
    ))
  }
}

/// Whether a word may stand in a node's occupants file: lowercase letters, digits and hyphens.
fn occupant_word(name: &str) -> bool {
  let mut chars = name.chars();
  chars.next().is_some_and(|first| first.is_ascii_lowercase() || first.is_ascii_digit())
    && chars.all(|one| one.is_ascii_lowercase() || one.is_ascii_digit() || one == '-')
}

/// The rules out of the file's text, every value checked; a fault names the file and what is wrong.
pub fn parse_rules(source: &str, file: &str) -> Result<Rules, RulesError> {
  let reader = Reader { file };
  let read = match yaml::parse(source) {
    Ok(Yaml::Null) => Yaml::Map(Vec::new()),
    Ok(read) => read,
    Err(fault) => return reader.fail(format!("it does not read as YAML: {fault}")),
  };
  for (name, _) in reader.object(&read, "the file")? {
    if !["occupants", "nodes", "units", "rules"].contains(&name.as_str()) {
      return reader.fail(format!(
        "the top level holds occupants, nodes, units and rules alone; {name} is no name it knows"
      ));
    }
  }

  let mut occupants: Vec<Claimant> = Vec::new();
  let mut tiers: BTreeMap<String, Num> = BTreeMap::new();
  for (at, one) in reader.list(read.get("occupants"), "occupants")?.iter().enumerate() {
    let (name, tier) = match reader.occupant(one, &format!("occupants[{at}]"))? {
      OccupantEntry::Claimant(claimant) => {
        let named = (claimant.name.clone(), claimant.tier);
        occupants.push(claimant);
        named
      }
      OccupantEntry::Tier(name, tier) => (name, tier),
    };
    if tiers.insert(name.clone(), tier).is_some() {
      return reader.fail(format!("occupants names {name} twice"));
    }
  }

  let mut groups = Vec::new();
  for (at, one) in reader.list(read.get("nodes"), "nodes")?.iter().enumerate() {
    let place = format!("nodes[{at}]");
    reader.object(one, &place)?;
    groups.push(NodeGroup {
      name: match one.get("name") {
        None => format!("group-{}", at + 1),
        given => reader.text(given, &format!("{place}.name"))?,
      },
      select: reader.selector(one.get("select"), &format!("{place}.select"))?,
      pool: one.get("pool") == Some(&Yaml::Bool(true)),
      reserve_mib: reader.number(one.get("reserve_mib"), &format!("{place}.reserve_mib"), 0.0)?,
      dump: reader.dump(one.get("dump"), &format!("{place}.dump"))?,
      ram_max_gib: match one.get("ram_max_gib") {
        None => None,
        given => Some(reader.number(given, &format!("{place}.ram_max_gib"), 0.0)?),
      },
      store_gib: match one.get("store_gib") {
        None => None,
        given => Some(reader.positive(given, &format!("{place}.store_gib"))?),
      },
    });
  }

  let mut units = Vec::new();
  for (at, one) in reader.list(read.get("units"), "units")?.iter().enumerate() {
    let place = format!("units[{at}]");
    reader.object(one, &place)?;
    let kind = match one.get("kind") {
      None | Some(Yaml::Null) => UnitKind::OnDemand,
      Some(Yaml::Text(word)) if word == "resident" => UnitKind::Resident,
      Some(Yaml::Text(word)) if word == "on-demand" => UnitKind::OnDemand,
      Some(Yaml::Text(word)) if word == "fill" => UnitKind::Fill,
      Some(_) => return reader.fail(format!("{place}.kind must be resident, on-demand or fill")),
    };
    let empty = Yaml::Map(Vec::new());
    let hold = match one.get("hold") {
      None => &empty,
      Some(given) => {
        reader.object(given, &format!("{place}.hold"))?;
        given
      }
    };
    let unit = reader.text(one.get("unit"), &format!("{place}.unit"))?;
    units.push(UnitSpec {
      service: match one.get("service") {
        None => unit.split('/').next().unwrap_or_default().to_owned(),
        given => reader.text(given, &format!("{place}.service"))?,
      },
      unit,
      place: match one.get("where") {
        None => None,
        given => Some(reader.selector(given, &format!("{place}.where"))?),
      },
      kind,
      priority: reader.number(one.get("priority"), &format!("{place}.priority"), DEFAULT_PRIORITY)?,
      hold: Hold {
        allowed: hold.get("allowed") != Some(&Yaml::Bool(false)),
        max_wait_s: reader.number(hold.get("max_wait_s"), &format!("{place}.hold.max_wait_s"), 300.0)?,
      },
      group: match one.get("group") {
        None => None,
        given => Some(reader.text(given, &format!("{place}.group"))?),
      },
      apart: reader
        .list(one.get("apart"), &format!("{place}.apart"))?
        .iter()
        .enumerate()
        .map(|(k, name)| reader.text(Some(name), &format!("{place}.apart[{k}]")))
        .collect::<Read<Vec<_>>>()?,
      vram_mib: optional_number(&reader, one, "vram_mib", &place)?,
      load_s: optional_number(&reader, one, "load_s", &place)?,
      unload_s: optional_number(&reader, one, "unload_s", &place)?,
      measured_at: one.get("measured_at").map(Yaml::as_words),
      dump: reader.dump(one.get("dump"), &format!("{place}.dump"))?,
    });
  }

  let mut rules = Vec::new();
  for (at, one) in reader.list(read.get("rules"), "rules")?.iter().enumerate() {
    let place = format!("rules[{at}]");
    reader.object(one, &place)?;
    let when_place = format!("{place}.when");
    let when = match one.get("when") {
      Some(given) => given,
      None => return reader.fail(format!("{when_place} must be a map of names to values")),
    };
    for (name, _) in reader.object(when, &when_place)? {
      if !["request", "desktop", "on"].contains(&name.as_str()) {
        return reader.fail(format!(
          "{when_place} holds request, desktop and on alone; {name} is no name it knows (an occupant that takes the whole card stands in occupants)"
        ));
      }
    }
    let mut said = When::default();
    if let Some(request) = when.get("request") {
      said.request = Some(reader.text(Some(request), &format!("{when_place}.request"))?);
    }
    if let Some(desktop) = when.get("desktop") {
      if *desktop != Yaml::Text("busy".into()) {
        return reader.fail(format!("{when_place}.desktop must be busy"));
      }
      said.desktop = Some("busy");
    }
    if let Some(on) = when.get("on") {
      said.on = Some(reader.selector(Some(on), &format!("{when_place}.on"))?);
    }
    if said.request.is_none() && said.desktop.is_none() {
      return reader.fail(format!("{when_place} must hold request or desktop"));
    }
    let evict = reader.eviction(one.get("evict"), &format!("{place}.evict"))?;
    // What stands below an occupant gives way while the desktop is busy, which the slave carries; a unit
    // asked for has no occupant to stand below, so a request rule that says it is refused rather than
    // planned as if it said nothing.
    if said.request.is_some() && matches!(evict, Some(Eviction::Below(_))) {
      return reader.fail(format!(
        "{place}.evict may say below only when the desktop is busy; a request rule says node, a list of units or {{cheapest: […]}}"
      ));
    }
    rules.push(Rule {
      name: match one.get("name") {
        None => format!("rule-{}", at + 1),
        given => reader.text(given, &format!("{place}.name"))?,
      },
      when: said,
      evict,
      place: match one.get("place") {
        None => None,
        given => Some(reader.selector(given, &format!("{place}.place"))?),
      },
      restore: one.get("restore") == Some(&Yaml::Bool(true)),
    });
  }

  Ok(Rules {
    occupants,
    groups,
    units,
    rules,
    turns: BTreeMap::new(),
    tiers,
  })
}

fn optional_number(reader: &Reader, one: &Yaml, key: &str, place: &str) -> Read<Option<Num>> {
  match one.get(key) {
    None => Ok(None),
    given => reader.number(given, &format!("{place}.{key}"), 0.0).map(Some),
  }
}

/// The rules file read: none where there is no file, an error where it is malformed.
pub fn read_rules(path: &Path) -> Result<Option<Rules>, RulesError> {
  match std::fs::read_to_string(path) {
    Ok(source) => parse_rules(&source, &path.display().to_string()).map(Some),
    Err(fault) if fault.kind() == std::io::ErrorKind::NotFound => Ok(None),
    Err(fault) => Err(RulesError(format!("{}: {fault}", path.display()))),
  }
}

/// Whether a unit name (with or without its node) matches a pattern of the rules, `*` standing for any run.
pub fn matches(pattern: &str, name: &str) -> bool {
  let bare = name.split('@').next().unwrap_or_default();
  let pieces: Vec<&str> = pattern.split('*').collect();
  let (first, rest) = pieces.split_first().expect("split yields at least one piece");
  let Some(mut left) = bare.strip_prefix(first) else { return false };
  let Some((last, middle)) = rest.split_last() else { return left.is_empty() };
  for piece in middle {
    match left.find(piece) {
      Some(at) => left = &left[at + piece.len()..],
      None => return false,
    }
  }
  left.len() >= last.len() && left.ends_with(last)
}

/// What the rules know of a node: its name, its row's role, and whether its machine is a desktop.
#[derive(Debug, Clone, PartialEq)]
pub struct NodeFacts {
  pub name: String,
  pub role: Option<String>,
  pub desktop: bool,
}

/// The services a node's role names: one, or several joined by `+` (docs/api.md, 「The node service」).
pub fn role_services(role: Option<&str>) -> Vec<&str> {
  role.map(|one| one.split('+').collect()).unwrap_or_default()
}

pub fn selects(chosen: Option<&Selector>, node: &NodeFacts) -> bool {
  let Some(chosen) = chosen else { return true };
  chosen
    .role
    .as_deref()
    .is_none_or(|role| role_services(node.role.as_deref()).contains(&role))
    && chosen.desktop.is_none_or(|desktop| desktop == node.desktop)
}

impl Rules {
  /// The group a node answers to: the first that selects it, or none.
  pub fn group_of(&self, node: &NodeFacts) -> Option<&NodeGroup> {
    self.groups.iter().find(|group| selects(Some(&group.select), node))
  }

  /// The spec a unit answers to: the first whose pattern names it.
  pub fn spec_of(&self, name: &str) -> Option<&UnitSpec> {
    self.units.iter().find(|spec| matches(&spec.unit, name))
  }

  /// An occupant's tier as the rules carry it, the default where they name none (docs/placement.md,
  /// 「Tiers」).
  pub fn tier_of(&self, occupant: &str) -> f64 {
    self.tiers.get(occupant).map_or(DEFAULT_TIER, |tier| tier.0)
  }

  /// The part of the rules a node's slave is pushed: the groups that choose it, the units that may stand
  /// there, and the rules that can touch it.
  pub fn for_node(&self, node: &NodeFacts) -> Rules {
    Rules {
      occupants: self.occupants.clone(),
      groups: self.groups.iter().filter(|group| selects(Some(&group.select), node)).cloned().collect(),
      units: self.units.iter().filter(|spec| selects(spec.place.as_ref(), node)).cloned().collect(),
      rules: self
        .rules
        .iter()
        .filter(|rule| {
          selects(rule.when.on.as_ref(), node)
            && (rule.place.is_none()
              || selects(rule.place.as_ref(), node)
              || selects(rule.when.on.as_ref(), node))
        })
        .cloned()
        .collect(),
      turns: self.turns.clone(),
      tiers: self.tiers.clone(),
    }
  }

  /// The JSON a node is pushed, written as the slave has always been handed it: one space of indent and
  /// a closing newline.
  pub fn node_text(&self) -> String {
    let mut out = Vec::new();
    let formatter = serde_json::ser::PrettyFormatter::with_indent(b" ");
    let mut serializer = serde_json::Serializer::with_formatter(&mut out, formatter);
    self.serialize(&mut serializer).expect("rules serialize");
    format!("{}\n", String::from_utf8(out).expect("serde_json writes UTF-8"))
  }
}
