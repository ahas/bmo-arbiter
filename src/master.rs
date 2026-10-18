//! The card master (docs/placement.md, 「Master and slaves」). Every round it reads
//! the slaves of the nodes its rules' groups choose, through each node's shell; now and then it reads the
//! node registry again and asks every node whether a slave answers there. A ticket in a slave's queue that
//! names a unit is placed by the rules: what makes room is unloaded on that node, and what was unloaded
//! to come back is loaded back once that ticket is gone, as the slaves say it. It holds no lock and no
//! card.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime};

use futures_util::future::join_all;
use serde::Serialize;
use serde_json::Value;
use tokio::task::JoinSet;

use crate::picture::{
  CardView, DESCRIPTIONS_FOLDER, Link, LinkState, NodeView, SlaveRules, TicketView, Turn, UnitDetail,
  UnitPlace, UnitView, card_from, changes, descriptions_mark, plannable, read_descriptions,
};
use crate::placement::{Board, RequestView, ServiceView};
use crate::plan::{Decision, Request};
pub use crate::reach::BoxFuture;
use crate::reach::{NodeShell, RunResult};
use crate::rules::{NodeFacts, Num, Rules, UnitKind, read_rules, selects};

/// Where the slave answers on each node's loopback.
pub const SLAVE: &str = "http://127.0.0.1:7439";
const EVENTS_KEPT: usize = 200;

// How the master's own unloads say, to the slave and through it to whoever reads its units, which ticket
// the room is made for and that the unit comes back once that ticket is done.
const REASON_PREFIX: &str = "card-master: ";
const ASKED_FOR: &str = " asked for ";
const BACK_ONCE: &str = " — back once ";
const IS_DONE: &str = " is done";

/// A node's shell, as far as the master uses it: a command run there.
pub trait Shell: Send + Sync {
  fn exec<'a>(&'a self, command: &'a str) -> BoxFuture<'a, RunResult>;
}

impl Shell for NodeShell {
  fn exec<'a>(&'a self, command: &'a str) -> BoxFuture<'a, RunResult> {
    Box::pin(NodeShell::exec(self, command))
  }
}

/// A node reached: its role in the node registry and its shell.
#[derive(Clone)]
pub struct Reached {
  pub role: Option<String>,
  pub shell: Arc<dyn Shell>,
}

/// What the master needs of the world, replaced in tests.
pub trait World: Send + Sync {
  /// Every registered node's name.
  fn list_nodes(&self) -> BoxFuture<'_, Vec<String>>;
  /// A node's role and shell, or why it cannot be reached.
  fn reach<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Result<Reached, String>>;
  /// Seconds since the epoch.
  fn clock(&self) -> f64;
  fn log(&self, line: &str);
  /// The release of the arbiter's code the master installs now.
  fn version(&self) -> String;
}

/// One event of the picture: when, on which node, the code a console reads it by and the English text a
/// person reads (docs/api.md, 「Messages and codes」).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Event {
  pub at: Num,
  pub node: Option<String>,
  pub code: &'static str,
  pub text: String,
}

/// A unit as the state API gives it: as its slave said, with the kind the rules give it and what its
/// service says it holds.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StateUnit {
  #[serde(flatten)]
  pub unit: UnitView,
  pub kind: Option<UnitKind>,
  pub detail: Option<UnitDetail>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MasterInfo {
  pub version: String,
  pub started_at: Num,
  pub rules_file: String,
  pub rules_loaded_at: Option<Num>,
  pub rules_error: Option<String>,
  pub read_every_s: Num,
}

#[derive(Debug, Clone, Serialize)]
pub struct GroupState {
  pub name: String,
  pub pool: bool,
  pub nodes: Vec<String>,
  #[serde(rename = "totalMiB")]
  pub total_mib: Num,
  #[serde(rename = "freeMiB")]
  pub free_mib: Num,
}

#[derive(Debug, Clone, Serialize)]
pub struct Declared {
  pub unit: String,
  pub service: String,
  pub kind: UnitKind,
  #[serde(rename = "vramMiB")]
  pub vram_mib: Option<Num>,
}

/// The whole picture the state API answers.
#[derive(Debug, Clone, Serialize)]
pub struct MasterState {
  pub master: MasterInfo,
  pub groups: Vec<GroupState>,
  pub nodes: Vec<NodeView<StateUnit>>,
  /// The units the rules declare, so one asked for only on demand is known while no card holds it.
  pub declared: Vec<Declared>,
  pub events: Vec<Event>,
  /// The services the master places, each with its declaration and its nodes.
  pub services: Vec<ServiceView>,
  /// The placement request under way, or none.
  pub request: Option<RequestView>,
}

#[derive(Serialize)]
struct Move<'a> {
  units: Vec<&'a str>,
  reason: &'a str,
}

/// The reason the master hands a slave with an unload, naming where the unit comes back.
pub fn unload_reason(service: &str, unit: &str, rule: Option<&str>, ticket: &str, restore: bool) -> String {
  let rule = rule.map(|name| format!(" ({name})")).unwrap_or_default();
  let base = format!("{REASON_PREFIX}{service}{ASKED_FOR}{unit}{rule}");
  if restore { format!("{base}{BACK_ONCE}{ticket}{IS_DONE}") } else { base }
}

/// The ticket an unload of the master's waits on before its unit comes back, read out of its reason;
/// none for a unit the master did not unload to come back.
pub fn awaited_ticket(reason: &str) -> Option<&str> {
  let (_, tail) = reason.strip_prefix(REASON_PREFIX)?.rsplit_once(BACK_ONCE)?;
  tail.strip_suffix(IS_DONE)
}

/// The service whose ticket an unload of the master's made room for, read out of its reason.
pub fn asker_of(reason: &str) -> Option<&str> {
  reason.strip_prefix(REASON_PREFIX)?.split_once(ASKED_FOR).map(|(service, _)| service)
}

fn bare(name: &str) -> &str {
  name.split('@').next().unwrap_or(name)
}

pub struct Master {
  world: Arc<dyn World>,
  rules_file: PathBuf,
  read_every_s: f64,
  started_at: f64,
  nodes: HashMap<String, NodeView>,
  reached: HashMap<String, Reached>,
  known: Vec<String>,
  answering: HashSet<String>,
  rules: Option<Rules>,
  rules_error: Option<String>,
  rules_loaded_at: Option<f64>,
  rules_mtime: Option<Option<SystemTime>>,
  details: BTreeMap<String, UnitDetail>,
  details_mark: Option<String>,
  // The tickets already placed, by node, left unplanned while what was unloaded for them stays out; a ticket on
  // hold is not among them and is planned again.
  placed: HashSet<(String, String)>,
  // Why each ticket on hold waits, said once and again only where it changes.
  held: HashMap<(String, String), String>,
  // The restores that failed, said once until one goes through.
  unrestored: HashSet<(String, String)>,
  // The restores waiting for the asker's own units to leave the card, said once.
  restore_waits: HashSet<(String, String)>,
  // Why each unload that failed did, said once and again only where it changes.
  unloads_failed: HashMap<(String, String), String>,
  events: VecDeque<Event>,
  revision: u64,
  board: Option<Arc<Mutex<Board>>>,
}

impl Master {
  /// The placement's board, whose services and request the picture carries and whose words it says.
  pub fn attach(&mut self, board: Arc<Mutex<Board>>) {
    self.board = Some(board);
  }

  /// What the placement had to say since the last round, said among the events.
  fn take_board_words(&mut self) {
    let said = match &self.board {
      Some(board) => std::mem::take(&mut board.lock().unwrap().said),
      None => return,
    };
    for one in said {
      self.say(one.node.as_deref(), one.code, one.text);
    }
  }

  pub fn new(world: Arc<dyn World>, rules_file: PathBuf, read_every_s: f64) -> Self {
    let started_at = world.clock();
    Master {
      world,
      rules_file,
      read_every_s,
      started_at,
      nodes: HashMap::new(),
      reached: HashMap::new(),
      known: Vec::new(),
      answering: HashSet::new(),
      rules: None,
      rules_error: None,
      rules_loaded_at: None,
      rules_mtime: None,
      details: BTreeMap::new(),
      details_mark: None,
      placed: HashSet::new(),
      held: HashMap::new(),
      unrestored: HashSet::new(),
      restore_waits: HashSet::new(),
      unloads_failed: HashMap::new(),
      events: VecDeque::new(),
      revision: 0,
      board: None,
    }
  }

  /// How many times the picture has changed: the event stream sends a picture whenever it moves.
  pub fn revision(&self) -> u64 {
    self.revision
  }

  /// Whether no restore is owed: none tried and failed, and none waiting for its asker's units to leave.
  pub fn owes_no_restore(&self) -> bool {
    self.unrestored.is_empty() && self.restore_waits.is_empty()
  }

  fn say(&mut self, node: Option<&str>, code: &'static str, text: String) {
    self.world.log(&format!("{}: {text}", node.unwrap_or("master")));
    self.events.push_front(Event {
      at: Num(self.world.clock()),
      node: node.map(str::to_owned),
      code,
      text,
    });
    self.events.truncate(EVENTS_KEPT);
    self.revision += 1;
  }

  /// The rules read again where the file changed, the change kept in the log beside it.
  pub fn read_rules_if_changed(&mut self) {
    let mtime = std::fs::metadata(&self.rules_file).and_then(|meta| meta.modified()).ok();
    if self.rules_mtime == Some(mtime) && self.rules_loaded_at.is_some() {
      return;
    }
    self.rules_mtime = Some(mtime);
    self.rules_loaded_at = Some(self.world.clock());
    match read_rules(&self.rules_file) {
      Err(fault) => {
        self.rules_error = Some(fault.0.clone());
        self.say(
          None,
          "rules_unreadable",
          format!("the rules file does not read, so the rules read before stand: {}", fault.0),
        );
      }
      Ok(read) => {
        self.rules_error = None;
        let (code, said) = match &read {
          None => ("rules_absent", "no rules file stands, so the default places the units".to_owned()),
          Some(rules) => (
            "rules_read",
            format!(
              "the rules are read again ({} groups, {} units, {} rules)",
              rules.groups.len(),
              rules.units.len(),
              rules.rules.len()
            ),
          ),
        };
        self.rules = read;
        self.say(None, code, said);
      }
    }
    let stamp = chrono::DateTime::from_timestamp_millis((self.world.clock() * 1000.0) as i64)
      .map(|at| at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
      .unwrap_or_default();
    let line = match &self.rules_error {
      None => format!("{stamp} master read\n"),
      Some(error) => format!("{stamp} master not read: {error}\n"),
    };
    // The log is a record; a folder that cannot take it leaves the rules as read.
    if let Some(folder) = self.rules_file.parent() {
      let _ = append(&folder.join("card-rules.log"), &line);
    }
  }

  /// The master's store of the units' descriptions, beside the rules file.
  pub fn descriptions_folder(&self) -> PathBuf {
    self.rules_file.parent().unwrap_or(Path::new(".")).join(DESCRIPTIONS_FOLDER)
  }

  /// What the services say of their units read again where a file of theirs changed; a file that cannot
  /// be taken is said once and leaves the rest as read.
  pub fn read_descriptions_if_changed(&mut self) {
    let folder = self.descriptions_folder();
    let mark = descriptions_mark(&folder);
    if self.details_mark.as_deref() == Some(mark.as_str()) {
      return;
    }
    self.details_mark = Some(mark);
    let (details, faults) = read_descriptions(&folder);
    self.details = details;
    for fault in faults {
      self.say(None, "description_unreadable", format!("a unit description does not read: {fault}"));
    }
    self.revision += 1;
  }

  /// Which nodes are read every round: every node whose slave answered at the last discovery, and every
  /// node a group of the rules chooses by its role, so one whose slave stopped answering shows as lost.
  pub fn members(&self) -> Vec<String> {
    let groups = self.rules.as_ref().map(|rules| rules.groups.as_slice()).unwrap_or_default();
    self
      .known
      .iter()
      .filter(|name| {
        if self.answering.contains(*name) {
          return true;
        }
        let facts = NodeFacts {
          name: (*name).clone(),
          role: self.reached.get(*name).and_then(|reached| reached.role.clone()),
          desktop: self.nodes.get(*name).is_some_and(|node| node.desktop),
        };
        groups
          .iter()
          .any(|group| group.select.role.is_some() && selects(Some(&group.select), &facts))
      })
      .cloned()
      .collect()
  }

  /// The node list read again and every node asked whether its slave answers.
  pub async fn discover(&mut self) {
    self.known = self.world.list_nodes().await;
    let mut asking = JoinSet::new();
    for name in self.known.clone() {
      let world = self.world.clone();
      asking.spawn(async move {
        let reached = world.reach(&name).await;
        let asked = match &reached {
          Ok(reached) => Some(reached.shell.exec(&format!("curl -s -m 3 {SLAVE}/units")).await),
          Err(_) => None,
        };
        (name, reached, asked)
      });
    }
    while let Some(Ok((name, reached, asked))) = asking.join_next().await {
      let Ok(reached) = reached else {
        self.answering.remove(&name);
        continue;
      };
      self.reached.insert(name.clone(), reached);
      match asked.and_then(|asked| answer_of(&asked)) {
        None => {
          self.answering.remove(&name);
        }
        Some(answer) => {
          self.answering.insert(name.clone());
          self.take(&name, Some(answer), 0.0, None);
        }
      }
    }
  }

  /// One slave's answer taken into the picture, or the node marked lost where it gave none.
  fn take(&mut self, name: &str, answer: Option<Value>, read_ms: f64, error: Option<String>) {
    let before = self.nodes.get(name).cloned();
    let now = Num(self.world.clock());
    let role = self.reached.get(name).and_then(|reached| reached.role.clone());
    let view = match answer {
      None => match &before {
        None => NodeView {
          name: name.to_owned(),
          role,
          desktop: false,
          version: None,
          rules: None,
          occupants: Vec::new(),
          cards: Vec::new(),
          link: Link {
            state: LinkState::Lost,
            since: Some(now),
            read_at: None,
            last_read_ms: None,
            error,
          },
        },
        Some(before) => NodeView {
          link: Link {
            state: LinkState::Lost,
            since: if before.link.state == LinkState::Lost { before.link.since } else { Some(now) },
            error,
            ..before.link.clone()
          },
          ..before.clone()
        },
      },
      Some(said) => {
        let rules = said.get("rules").filter(|one| !one.is_null()).map(|rules| SlaveRules {
          loaded_at: rules.get("loadedAt").cloned(),
          error: rules.get("error").cloned(),
        });
        NodeView {
          name: name.to_owned(),
          role,
          desktop: said.get("desktop") == Some(&Value::Bool(true)),
          version: said.get("version").and_then(Value::as_str).map(str::to_owned),
          rules,
          occupants: said
            .get("occupants")
            .and_then(Value::as_array)
            .map(|all| all.iter().filter_map(Value::as_str).map(str::to_owned).collect())
            .unwrap_or_default(),
          cards: vec![card_from(&said, name)],
          link: Link {
            state: LinkState::Connected,
            since: match &before {
              Some(before) if before.link.state == LinkState::Connected => before.link.since,
              _ => Some(now),
            },
            read_at: Some(now),
            last_read_ms: Some(Num(read_ms.round())),
            error: None,
          },
        }
      }
    };
    for change in changes(before.as_ref(), &view) {
      self.say(Some(name), change.code, change.text);
    }
    if before.as_ref() != Some(&view) {
      self.revision += 1;
    }
    self.nodes.insert(name.to_owned(), view);
  }

  /// Every member's slave read once, then what the reads call for done.
  pub async fn poll(&mut self) {
    self.take_board_words();
    self.read_rules_if_changed();
    self.read_descriptions_if_changed();
    let mut asking = JoinSet::new();
    for name in self.members() {
      let Some(reached) = self.reached.get(&name).cloned() else { continue };
      asking.spawn(async move {
        let began = Instant::now();
        let asked = reached.shell.exec(&format!("curl -s -m 3 {SLAVE}/units")).await;
        (name, asked, began.elapsed().as_secs_f64() * 1000.0)
      });
    }
    while let Some(Ok((name, asked, read_ms))) = asking.join_next().await {
      let answer = answer_of(&asked);
      let error = answer.is_none().then(|| {
        let said = asked.stderr.trim();
        let said = if said.is_empty() { format!("exit {}", asked.code) } else { said.to_owned() };
        tail(&said, 200).to_owned()
      });
      self.take(&name, answer, read_ms, error);
    }
    self.act().await;
  }

  async fn move_units(&self, node: &str, verb: &str, units: &[&str], reason: &str) -> (bool, String) {
    let Some(reached) = self.reached.get(node) else {
      return (false, "unreachable".into());
    };
    let body = serde_json::to_string(&Move {
      units: units.iter().map(|unit| bare(unit)).collect(),
      reason,
    })
    .expect("move serializes")
    .replace('\'', "'\\''");
    let ran = reached
      .shell
      .exec(&format!(
        "curl -s -m 300 -o /dev/stdout -w '\\n%{{http_code}}' -X POST -H 'content-type: application/json' -d '{body}' {SLAVE}/units/{verb}"
      ))
      .await;
    let (body, status) = ran.stdout.trim().rsplit_once('\n').unwrap_or(("", ran.stdout.trim()));
    let ok = ran.code == 0 && status == "200";
    let why = serde_json::from_str::<Value>(body)
      .ok()
      .and_then(|answer| answer.get("error").and_then(Value::as_str).map(str::to_owned));
    match why {
      Some(why) if !ok => (false, format!("{status}: {why}")),
      _ => (ok, status.to_owned()),
    }
  }

  /// What the master unloaded to come back loaded back once its ticket is gone, and every ticket naming a
  /// unit placed by the rules.
  pub async fn act(&mut self) {
    self.restore_what_is_done().await;
    self.place_tickets().await;
  }

  async fn restore_what_is_done(&mut self) {
    let mut due: Vec<(String, String, Vec<String>)> = Vec::new();
    // (node, the ticket, its units, the asker whose units still fill the card)
    let mut waiting: Vec<(String, String, Vec<String>, String)> = Vec::new();
    for view in self.nodes.values() {
      let Some(card) = view.cards.first() else { continue };
      if view.link.state != LinkState::Connected || card.claim.by.is_some() {
        continue;
      }
      let standing: HashSet<&str> = card
        .queue
        .iter()
        .map(|ticket| ticket.ticket.as_str())
        .chain(card.turn.as_ref().map(|turn| turn.ticket.as_str()))
        .collect();
      // a service standing in the card's line has its own turn bring its units back
      let in_line: HashSet<&str> = card
        .queue
        .iter()
        .map(|ticket| ticket.service.as_str())
        .chain(card.turn.as_ref().map(|turn| turn.service.as_str()))
        .collect();
      let on_card = |service: &str| {
        card.units.iter().any(|unit| {
          unit.service == service && matches!(unit.state, UnitPlace::Resident | UnitPlace::Mixed)
        })
      };
      let mut by_ticket: BTreeMap<&str, Vec<String>> = BTreeMap::new();
      let mut held: BTreeMap<(&str, &str), Vec<String>> = BTreeMap::new();
      for unit in card.units.iter().filter(|unit| unit.state != UnitPlace::Resident) {
        let Some(reason) = unit.reason.as_deref() else { continue };
        let Some(ticket) = awaited_ticket(reason).filter(|ticket| !standing.contains(ticket)) else {
          continue;
        };
        if in_line.contains(unit.service.as_str()) {
          continue;
        }
        // a holder that let its ticket go before its own units left the card is waited for
        match asker_of(reason).filter(|asker| on_card(asker)) {
          Some(asker) => held.entry((ticket, asker)).or_default().push(unit.name.clone()),
          None => by_ticket.entry(ticket).or_default().push(unit.name.clone()),
        }
      }
      due.extend(
        by_ticket
          .into_iter()
          .map(|(ticket, units)| (view.name.clone(), ticket.to_owned(), units)),
      );
      waiting.extend(
        held
          .into_iter()
          .map(|((ticket, asker), units)| (view.name.clone(), ticket.to_owned(), units, asker.to_owned())),
      );
    }
    for (node, ticket, units, asker) in waiting {
      if self.restore_waits.insert((node.clone(), ticket)) {
        self.say(
          Some(&node),
          "restore_waiting",
          format!("{}: back once {asker}'s units leave the card", units.join(", ")),
        );
      }
    }
    // every node's loads at once, so a group spread over several nodes has its last card back with its first
    let master = &*self;
    let moved = join_all(due.iter().map(|(node, _, units)| async move {
      let names: Vec<&str> = units.iter().map(String::as_str).collect();
      master.move_units(node, "restore", &names, "the asker is done").await
    }))
    .await;
    for ((node, ticket, units), (ok, status)) in due.into_iter().zip(moved) {
      let key = (node.clone(), ticket);
      if ok {
        self.unrestored.remove(&key);
        self.restore_waits.remove(&key);
        self.say(Some(&node), "units_restored", format!("{} loaded back", units.join(", ")));
      } else if self.unrestored.insert(key) {
        self.say(
          Some(&node),
          "restore_failed",
          format!("{} not loaded back yet ({status}); trying again", units.join(", ")),
        );
      }
    }
  }

  async fn place_tickets(&mut self) {
    let rules = self.rules.clone().unwrap_or_default();
    let mut asked = Vec::new();
    let mut standing: HashSet<(String, String)> = HashSet::new();
    let mut replanned: Vec<(String, String)> = Vec::new();
    for view in self.nodes.values() {
      let Some(card) = view.cards.first() else { continue };
      if view.link.state != LinkState::Connected {
        // A node not read this round keeps what the master knows of its tickets.
        standing.extend(
          self
            .placed
            .iter()
            .chain(self.held.keys())
            .filter(|(node, _)| *node == view.name)
            .cloned(),
        );
        continue;
      }
      for ticket in card
        .queue
        .iter()
        .map(|one| &one.ticket)
        .chain(card.turn.as_ref().map(|turn| &turn.ticket))
      {
        standing.insert((view.name.clone(), ticket.clone()));
      }
      let Some(node) = plannable(view) else { continue };
      for ticket in &card.queue {
        let Some(unit) = &ticket.unit else { continue };
        let key = (view.name.clone(), ticket.ticket.clone());
        if self.placed.contains(&key) {
          // a placement is done only while what it unloaded stays out for it; one that unloaded nothing, or
          // whose units came back while its ticket stands, is planned again (docs/placement.md,
          // 「Master and slaves」)
          if awaits(Some(view), &ticket.ticket) {
            continue;
          }
          replanned.push(key);
        }
        let request = Request {
          unit: unit.clone(),
          vram_mib: ticket.vram_mib.map(|n| n.0),
          on: None,
        };
        let decision =
          turn_gate(card.turn.as_ref(), &ticket.service, rules.plan(std::slice::from_ref(&node), &request));
        asked.push((view.name.clone(), ticket.clone(), decision));
      }
    }
    for key in &replanned {
      self.placed.remove(key);
    }
    let asked = group_gate(&rules, &self.nodes, &self.placed, asked);
    self.placed.retain(|key| standing.contains(key));
    self.held.retain(|key, _| standing.contains(key));
    self.unloads_failed.retain(|key, _| standing.contains(key));
    let nodes = &self.nodes;
    self.unrestored.retain(|(node, ticket)| awaits(nodes.get(node), ticket));
    self.restore_waits.retain(|(node, ticket)| awaits(nodes.get(node), ticket));

    // (node, the unit asked for, what goes, why, its cost)
    // (node, the unit asked for, what goes, why, its cost, the ticket)
    let mut unloads: Vec<(String, String, Vec<String>, String, f64, String)> = Vec::new();
    for (node, ticket, decision) in asked {
      let key = (node.clone(), ticket.ticket.clone());
      let unit = ticket.unit.clone().unwrap_or_default();
      match decision {
        Decision::Hold { reason, .. } => {
          // Held, and planned again at the next read: only a new reason is said.
          if self.held.get(&key) != Some(&reason) {
            self.say(Some(&node), "ticket_held", format!("{unit} held: {reason}"));
            self.held.insert(key, reason);
          }
        }
        Decision::Place {
          evict,
          restore,
          cost_s,
          rule,
          ..
        } => {
          self.held.remove(&key);
          self.placed.insert(key);
          if evict.is_empty() {
            continue;
          }
          let reason = unload_reason(&ticket.service, &unit, rule.as_deref(), &ticket.ticket, restore);
          unloads.push((node, unit, evict, reason, cost_s, ticket.ticket.clone()));
        }
      }
    }
    // every node's unloads at once, so a group asking on several nodes has its last card free with its first;
    // one node's own, where it has more than one, in turn
    let mut by_node: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
    for (at, (node, ..)) in unloads.iter().enumerate() {
      by_node.entry(node.as_str()).or_default().push(at);
    }
    let master = &*self;
    let unloads_ref = &unloads;
    let per_node = join_all(by_node.into_values().map(|ats| async move {
      let mut moved = Vec::new();
      for at in ats {
        let (node, _, evict, reason, _, _) = &unloads_ref[at];
        let names: Vec<&str> = evict.iter().map(String::as_str).collect();
        moved.push((at, master.move_units(node, "evict", &names, reason).await));
      }
      moved
    }))
    .await;
    let mut moved: Vec<(usize, (bool, String))> = per_node.into_iter().flatten().collect();
    moved.sort_by_key(|(at, _)| *at);
    for ((node, unit, evict, _, cost_s, ticket), (_, (ok, status))) in unloads.into_iter().zip(moved) {
      let key = (node.clone(), ticket);
      if ok {
        self.unloads_failed.remove(&key);
        self.say(
          Some(&node),
          "units_unloaded",
          format!(
            "{} unloaded to load {unit} (cost {} s)",
            evict.join(", "),
            crate::yaml::js_number(cost_s)
          ),
        );
        continue;
      }
      // an unload that failed leaves its ticket on hold, planned again at the next read; its reason is said once
      // and again only where it changes
      self.placed.remove(&key);
      if self.unloads_failed.get(&key) != Some(&status) {
        self.say(
          Some(&node),
          "unload_failed",
          format!("no room was made to load {unit} ({status}); trying again"),
        );
        self.unloads_failed.insert(key, status);
      }
    }
  }

  /// The whole picture the state API answers.
  pub fn state(&self) -> MasterState {
    let nodes: Vec<&NodeView> = self.known.iter().filter_map(|name| self.nodes.get(name)).collect();
    let rules = self.rules.as_ref();
    let groups = rules
      .map(|rules| rules.groups.as_slice())
      .unwrap_or_default()
      .iter()
      .map(|group| {
        let chosen: Vec<&&NodeView> = nodes
          .iter()
          .filter(|node| {
            let facts = NodeFacts {
              name: node.name.clone(),
              role: node.role.clone(),
              desktop: node.desktop,
            };
            selects(Some(&group.select), &facts)
          })
          .collect();
        let sum = |pick: fn(&CardView) -> Option<Num>| {
          Num(chosen.iter().map(|node| node.cards.first().and_then(pick).map_or(0.0, |n| n.0)).sum())
        };
        GroupState {
          name: group.name.clone(),
          pool: group.pool,
          nodes: chosen.iter().map(|node| node.name.clone()).collect(),
          total_mib: sum(|card| card.total_mib),
          free_mib: sum(|card| card.free_mib),
        }
      })
      .collect();
    let kind_of = |name: &str| rules.and_then(|rules| rules.spec_of(name)).map(|spec| spec.kind);
    MasterState {
      master: MasterInfo {
        version: self.world.version(),
        started_at: Num(self.started_at),
        rules_file: self.rules_file.display().to_string(),
        rules_loaded_at: self.rules_loaded_at.map(Num),
        rules_error: self.rules_error.clone(),
        read_every_s: Num(self.read_every_s),
      },
      groups,
      nodes: nodes
        .into_iter()
        .map(|node| NodeView {
          name: node.name.clone(),
          role: node.role.clone(),
          desktop: node.desktop,
          version: node.version.clone(),
          rules: node.rules.clone(),
          occupants: node.occupants.clone(),
          cards: node
            .cards
            .iter()
            .map(|card| CardView {
              units: card
                .units
                .iter()
                .map(|unit| StateUnit {
                  unit: unit.clone(),
                  kind: kind_of(&unit.name),
                  detail: self.details.get(&unit.name).cloned(),
                })
                .collect(),
              index: card.index,
              name: card.name.clone(),
              total_mib: card.total_mib,
              used_mib: card.used_mib,
              free_mib: card.free_mib,
              turn: card.turn.clone(),
              queue: card.queue.clone(),
              claim: card.claim.clone(),
              ram: card.ram.clone(),
              stage: card.stage.clone(),
              store: card.store.clone(),
              processes: card.processes.clone(),
            })
            .collect(),
          link: node.link.clone(),
        })
        .collect(),
      declared: rules
        .map(|rules| rules.units.as_slice())
        .unwrap_or_default()
        .iter()
        .map(|spec| Declared {
          unit: spec.unit.clone(),
          service: spec.service.clone(),
          kind: spec.kind,
          vram_mib: spec.vram_mib,
        })
        .collect(),
      events: self.events.iter().cloned().collect(),
      services: self
        .board
        .as_ref()
        .map(|board| board.lock().unwrap().services.clone())
        .unwrap_or_default(),
      request: self.board.as_ref().and_then(|board| board.lock().unwrap().request.clone()),
    }
  }
}

/// A slave's answer, where the call came back whole with one.
fn answer_of(asked: &RunResult) -> Option<Value> {
  if asked.code != 0 {
    return None;
  }
  serde_json::from_str::<Value>(&asked.stdout).ok().filter(|answer| !answer.is_null())
}

/// A ticket's plan that unloads another occupant's units held while the card's turn may not pass to it
/// (docs/placement.md, 「The engine's turn」).
fn turn_gate(turn: Option<&Turn>, service: &str, decision: Decision) -> Decision {
  match (turn, &decision) {
    (Some(turn), Decision::Place { evict, rule, .. }) if !evict.is_empty() && !turn.passes_to(service) => {
      Decision::Hold {
        reason: format!("{}'s turn is in use", turn.service),
        rule: rule.clone(),
      }
    }
    _ => decision,
  }
}

/// A group's tickets planned together (docs/placement.md, 「The engine's turn」): a ticket naming a unit of a
/// group is held until every node where a unit of that group stands has such a ticket planned now or placed
/// already, or has that unit on its card.
fn group_gate(
  rules: &Rules,
  nodes: &HashMap<String, NodeView>,
  placed: &HashSet<(String, String)>,
  asked: Vec<(String, TicketView, Decision)>,
) -> Vec<(String, TicketView, Decision)> {
  let group_of = |unit: &str| {
    rules
      .spec_of(unit)
      .and_then(|spec| spec.group.clone())
      .filter(|group| !group.is_empty())
  };
  let mut ready: HashSet<(String, String)> = HashSet::new();
  for (node, ticket, decision) in &asked {
    if let (Some(group), Decision::Place { .. }) = (ticket.unit.as_deref().and_then(group_of), decision) {
      ready.insert((group, node.clone()));
    }
  }
  for view in nodes.values() {
    let Some(card) = view.cards.first() else { continue };
    for ticket in &card.queue {
      if let Some(group) = ticket.unit.as_deref().and_then(group_of)
        && placed.contains(&(view.name.clone(), ticket.ticket.clone()))
      {
        ready.insert((group, view.name.clone()));
      }
    }
  }
  let waiting_on = |group: &str| -> Vec<String> {
    let mut waiting: Vec<String> = nodes
      .values()
      // a node whose slave was not read this round counts as it was last read, so the group never moves on part
      // of its nodes
      .filter_map(|view| {
        let card = view.cards.first()?;
        let stands: Vec<&UnitView> = card
          .units
          .iter()
          .filter(|unit| group_of(&unit.name).as_deref() == Some(group))
          .collect();
        let on_card = stands.iter().all(|unit| unit.state == UnitPlace::Resident);
        let asked = ready.contains(&(group.to_owned(), view.name.clone()));
        (!stands.is_empty() && !on_card && !asked).then(|| view.name.clone())
      })
      .collect();
    waiting.sort();
    waiting
  };
  asked
    .into_iter()
    .map(|(node, ticket, decision)| {
      let group = ticket.unit.as_deref().and_then(group_of);
      let (Some(group), Decision::Place { rule, .. }) = (group, &decision) else {
        return (node, ticket, decision);
      };
      let waiting = waiting_on(&group);
      if waiting.is_empty() {
        return (node, ticket, decision);
      }
      let held = Decision::Hold {
        reason: format!("its group {group} waits for {}", waiting.join(", ")),
        rule: rule.clone(),
      };
      (node, ticket, held)
    })
    .collect()
}

/// Whether a unit on the node still out waits on the ticket to come back: one back on the card, by the
/// master's restore or its slave's own, waits on nothing.
fn awaits(view: Option<&NodeView>, ticket: &str) -> bool {
  view.and_then(|view| view.cards.first()).is_some_and(|card| {
    card.units.iter().any(|unit| {
      unit.state != UnitPlace::Resident && unit.reason.as_deref().and_then(awaited_ticket) == Some(ticket)
    })
  })
}

fn tail(text: &str, chars: usize) -> &str {
  let count = text.chars().count();
  if count <= chars {
    return text;
  }
  let start = text.char_indices().nth(count - chars).map_or(0, |(at, _)| at);
  &text[start..]
}

fn append(path: &Path, line: &str) -> std::io::Result<()> {
  use std::io::Write;
  std::fs::OpenOptions::new()
    .create(true)
    .append(true)
    .open(path)?
    .write_all(line.as_bytes())
}
