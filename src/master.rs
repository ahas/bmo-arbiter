//! The card master (docs/placement.md, 「Master and slaves」). Every round it reads
//! the slaves of the nodes its rules' groups choose, through each node's shell; now and then it reads the
//! node service again and asks every node whether a slave answers there. A ticket in a slave's queue that
//! names a unit is placed by the rules: what makes room is unloaded on that node, and what was unloaded
//! to come back is loaded back once that ticket is gone, as the slaves say it. It holds no lock and no
//! card.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime};

use bmo_arbiter_client::card::{Card, CardUnitChanges, UnitMove};
use bmo_arbiter_client::cluster::ServiceInstance;
use bmo_arbiter_client::cluster::{
  CardStage, CardTurn, Cluster, ClusterCard, ClusterNode, ClusterUnit, DeclaredUnit, Group, MasterInfo,
  NodeLink, Note, OccupantTier, QueuedTicket,
};
use bmo_arbiter_client::demands::{DemandCard, DemandPool, DemandReason, DemandWait};
use futures_util::FutureExt;
use futures_util::future::join_all;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};

use crate::demands::{Book, Demands, Held, Seat};

use crate::picture::{
  CardView, DESCRIPTIONS_FOLDER, Link, LinkState, NodeView, SlaveRules, TicketView, Turn, UnitDetail,
  UnitPlace, UnitView, card_from, changes, descriptions_mark, plannable, read_descriptions,
};
use crate::placement::{AdapterAnswer, Board, Declaration, Placements, YieldBy, tiers_of, write_whole};
use crate::plan::{Decision, Request};
pub use crate::reach::BoxFuture;
use crate::reach::{NodeShell, RunResult};
use crate::rules::{NodeFacts, Num, Rules, read_rules, selects};

/// Where the slave answers on each node's loopback.
pub const SLAVE: &str = "http://127.0.0.1:7439";
const EVENTS_KEPT: usize = 200;

// How the master's own unloads say, to the slave and through it to whoever reads its units, which ticket
// the room is made for and that the unit comes back once that ticket is done.
const REASON_PREFIX: &str = "card-master: ";
const ASKED_FOR: &str = " asked for ";
const BACK_ONCE: &str = " — back once ";
const IS_DONE: &str = " is done";

// The master's record of the occupants it stopped to yield their card, beside the rules
// (docs/placement.md, 「Yielding by the process」).
const PROCESS_YIELDS: &str = "process-yields.json";

/// A node's shell, as far as the master uses it: a command run there.
pub trait Shell: Send + Sync {
  fn exec<'a>(&'a self, command: &'a str) -> BoxFuture<'a, RunResult>;
}

impl Shell for NodeShell {
  fn exec<'a>(&'a self, command: &'a str) -> BoxFuture<'a, RunResult> {
    Box::pin(NodeShell::exec(self, command))
  }
}

/// A node reached: its role in the node service and its shell.
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
  /// The services' declarations as they stand now; none where the world reads none.
  fn declarations(&self) -> Result<Vec<Declaration>, String> {
    Ok(Vec::new())
  }
  /// A service's adapter run with one verb for one node, its nodes as placed: what it answered.
  fn adapter<'a>(
    &'a self,
    declaration: &'a Declaration,
    verb: &'a str,
    node: &'a str,
    placed: &'a [String],
  ) -> BoxFuture<'a, AdapterAnswer> {
    let _ = (declaration, verb, node, placed);
    Box::pin(async { AdapterAnswer::refused("no adapter is run here") })
  }
}

/// What the master stopped to yield a card, kept beside the rules so a master restarted meanwhile still
/// brings back what is to come back: each service and node, with why it went down and how.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ProcessYields {
  pub stopped: Vec<StoppedOccupant>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StoppedOccupant {
  pub service_name: String,
  pub node_name: String,
  pub reason: String,
  #[serde(default)]
  pub kind: StopKind,
}

/// How an occupant that yields by its process went down (docs/placement.md, 「Yielding by the process」).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StopKind {
  /// To yield, brought back once nothing that made it yield stands.
  #[default]
  Yield,
  /// Handed over to another occupant of its tier, never brought back by that one going.
  Handover,
}

/// One stop the master keeps: why, and how.
#[derive(Debug, Clone, PartialEq)]
struct Stop {
  reason: String,
  kind: StopKind,
}

/// Where a service stands on a node, as its adapter's `health` says: the address its callers reach it at
/// and the file its key is read from.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Instance {
  pub address: Option<String>,
  pub key_file: Option<String>,
}

/// The instance an adapter's `health` answers standing, or none where it does not answer `running`.
fn instance_of(answer: &AdapterAnswer) -> Option<Instance> {
  (answer.ok && answer.state.as_deref() == Some("running")).then(|| Instance {
    address: answer.address.clone(),
    key_file: answer.key_file.clone(),
  })
}

/// An adapter verb running beside the rounds: the service, the node and the verb.
type RunKey = (String, String, &'static str);

/// What a verb run beside the rounds is for, which says what its answer does once a round takes it.
enum Purpose {
  /// A `stop` that yields the card or hands it over, with why.
  Down(Stop),
  /// A `start` once nothing that made it yield stands, its `health` read after it.
  Back,
  /// A `health` read of a placed instance.
  Health,
}

impl Purpose {
  fn verb(&self) -> &'static str {
    match self {
      Purpose::Down(_) => "stop",
      Purpose::Back => "start",
      Purpose::Health => "health",
    }
  }
}

/// A verb's answer handed back to the rounds: whose and where, what it was for, what the adapter answered,
/// and for a `start` that answered, the `health` read after it.
struct Answered {
  service: String,
  node: String,
  purpose: Purpose,
  answer: AdapterAnswer,
  health: Option<AdapterAnswer>,
}

/// Each card of a service's placement by node, with what it is to a demand of it.
type Openings = Vec<(String, Opening)>;

/// A card of a demand granted on a node, where the instance standing there answers.
fn card_at(node: &str, instance: &Instance) -> DemandCard {
  DemandCard {
    node_name: node.to_owned(),
    address: instance.address.clone(),
    key_file: instance.key_file.clone(),
  }
}

/// A service being brought up on a node for a demand, the occupants it takes the card from handed over
/// first.
struct Bringing {
  service: String,
  demand: i64,
  handle: JoinHandle<Brought>,
}

/// What bringing a service up came to: each occupant stopped and what its adapter answered, and the
/// instance standing or why it does not.
struct Brought {
  stopped: Vec<(String, AdapterAnswer)>,
  started: Result<Instance, String>,
}

/// What one card of a service's placement is to a demand of it (docs/placement.md, 「Demands」).
#[derive(Debug, Clone)]
enum Opening {
  /// The service stands there with no demand on it.
  Stands(Instance),
  /// Free, or its standing occupants of the service's tier idle: they are handed over and the service
  /// brought up.
  Bring(Vec<Declaration>),
  /// In use by the services named.
  InUse(String),
  /// The service's own `start` runs there beside the rounds.
  Starting,
  /// Held by a claim or a ticket of a lower tier, named.
  Held(String),
  /// Open, but a card the service does not use while it uses as many as its ceiling lets it.
  Ceiling,
  /// Open, but a card the demand's pool does not use while it uses as many as its limit lets it.
  Pool,
  /// Not a card the service can be had on now.
  Closed,
}

/// What holds a demand back from a card it does not use yet: the cards its service uses and, where it stands
/// in a pool, that pool with the cards it uses of the service.
struct Limits {
  used: BTreeSet<String>,
  pool: Option<(DemandPool, BTreeSet<String>)>,
}

impl Limits {
  /// Whether the card on the node would put the demand's pool above its limit.
  fn above_pool(&self, node: &str) -> bool {
    self
      .pool
      .as_ref()
      .is_some_and(|(pool, using)| !using.contains(node) && using.len() >= pool.cards as usize)
  }
}

/// One note of the picture: when, in the master's clock's seconds, on which node, the code a reader
/// translates by and the English text a person reads (docs/api.md, 「Messages and codes」).
#[derive(Debug, Clone, PartialEq)]
pub struct Event {
  pub at: Num,
  pub node: Option<String>,
  pub code: &'static str,
  pub text: String,
}

/// Seconds of the master's clock as epoch milliseconds.
pub fn millis(seconds: f64) -> i64 {
  (seconds * 1000.0).round() as i64
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
  // The services' declarations as last read, whose tiers stand beside the rules' and whose adapters a
  // yield by the process runs.
  declared: Vec<Declaration>,
  // The occupants stopped to yield their card, by service and node, with why and how; and why a stop or
  // start that failed did, said once.
  stopped: BTreeMap<(String, String), Stop>,
  yields_failed: HashMap<(String, String, &'static str), String>,
  // The demands the routes and the rounds share, and the book's version the picture last took in.
  demands: Option<Arc<Demands>>,
  demands_seen: u64,
  // Where each service stands now, by service and node, as its adapter's health last said; and what moved
  // since a health read of it began, which that read's answer leaves alone.
  instances: BTreeMap<(String, String), Instance>,
  touched: HashSet<(String, String)>,
  // The adapter verbs running beside the rounds, by service, node and verb, and the way their answers come
  // back to the rounds.
  runs: HashSet<RunKey>,
  answers_out: mpsc::UnboundedSender<Answered>,
  answers_in: mpsc::UnboundedReceiver<Answered>,
  // A service being brought up for a demand, by node; the node each work key was last granted on; and why
  // a demand's last bring failed, until another begins.
  bringing: HashMap<String, Bringing>,
  work_cards: HashMap<String, String>,
  start_failed: HashMap<i64, String>,
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

  /// What the placement had to say since the last round, said among the events, and every service it
  /// stood taken as standing there.
  fn take_board_words(&mut self) {
    let (said, stood) = match &self.board {
      Some(board) => {
        let mut board = board.lock().unwrap();
        (std::mem::take(&mut board.said), std::mem::take(&mut board.stood))
      }
      None => return,
    };
    for one in said {
      self.say(one.node.as_deref(), one.code, one.text);
    }
    for one in stood {
      let key = (one.service, one.node);
      self.touched.insert(key.clone());
      if let Some(instance) = instance_of(&one.health) {
        self.standing_again(key, instance, "a placement stood it");
      }
    }
  }

  /// A service standing on the node, its instance kept; a stop the master kept of it there let go, as it
  /// stands again whatever brought it up (docs/placement.md, 「Yielding by the process」).
  fn standing_again(&mut self, key: (String, String), instance: Instance, how: &str) {
    self.yields_failed.remove(&(key.0.clone(), key.1.clone(), "start"));
    if self.stopped.remove(&key).is_some() {
      self.save_yields();
      self.say(
        Some(&key.1),
        "occupant-standing",
        format!("{} stands again, {how}: its stop is let go", key.0),
      );
    }
    self.instances.insert(key, instance);
  }

  pub fn new(world: Arc<dyn World>, rules_file: PathBuf, read_every_s: f64) -> Self {
    let started_at = world.clock();
    let (answers_out, answers_in) = mpsc::unbounded_channel();
    let mut master = Master {
      world,
      rules_file,
      read_every_s,
      started_at,
      nodes: HashMap::new(),
      reached: HashMap::new(),
      known: Vec::new(),
      answering: HashSet::new(),
      rules: None,
      declared: Vec::new(),
      stopped: BTreeMap::new(),
      yields_failed: HashMap::new(),
      demands: None,
      demands_seen: 0,
      instances: BTreeMap::new(),
      touched: HashSet::new(),
      runs: HashSet::new(),
      answers_out,
      answers_in,
      bringing: HashMap::new(),
      work_cards: HashMap::new(),
      start_failed: HashMap::new(),
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
    };
    master.read_yields();
    master
  }

  /// The rules as the file stands, with every occupant's tier the declarations add (docs/placement.md,
  /// 「Tiers」).
  fn rules_now(&self) -> Rules {
    let rules = self.rules.clone().unwrap_or_default();
    Rules {
      tiers: tiers_of(&rules.tiers, &self.declared),
      ..rules
    }
  }

  fn yields_file(&self) -> PathBuf {
    self.rules_file.parent().unwrap_or(Path::new(".")).join(PROCESS_YIELDS)
  }

  /// The occupants a master before this one stopped, read back at its start so it brings them back.
  pub fn read_yields(&mut self) {
    let Ok(text) = std::fs::read_to_string(self.yields_file()) else { return };
    match serde_json::from_str::<ProcessYields>(&text) {
      Ok(read) => {
        self.stopped = read
          .stopped
          .into_iter()
          .map(|one| {
            let stop = Stop {
              reason: one.reason,
              kind: one.kind,
            };
            ((one.service_name, one.node_name), stop)
          })
          .collect();
      }
      Err(fault) => self.world.log(&format!("master: {PROCESS_YIELDS} does not read: {fault}")),
    }
  }

  fn save_yields(&self) {
    let kept = ProcessYields {
      stopped: self
        .stopped
        .iter()
        .map(|((service, node), stop)| StoppedOccupant {
          service_name: service.clone(),
          node_name: node.clone(),
          reason: stop.reason.clone(),
          kind: stop.kind,
        })
        .collect(),
    };
    let text = serde_json::to_string_pretty(&kept).expect("process yields serialize");
    if let Err(fault) = write_whole(&self.yields_file(), &text) {
      self.world.log(&format!("master: {PROCESS_YIELDS} was not written: {fault}"));
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
          "rules-unreadable",
          format!("the rules file does not read, so the rules read before stand: {}", fault.0),
        );
      }
      Ok(read) => {
        self.rules_error = None;
        let (code, said) = match &read {
          None => ("rules-absent", "no rules file stands, so the default places the units".to_owned()),
          Some(rules) => (
            "rules-read",
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
      self.say(None, "description-unreadable", format!("a unit description does not read: {fault}"));
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
    match self.world.declarations() {
      Ok(read) => {
        if read != self.declared {
          self.revision += 1;
        }
        self.declared = read;
      }
      Err(fault) => self.world.log(&format!("master: the declarations were not read: {fault}")),
    }
    self.known = self.world.list_nodes().await;
    let mut asking = JoinSet::new();
    for name in self.known.clone() {
      let world = self.world.clone();
      asking.spawn(async move {
        let reached = world.reach(&name).await;
        let asked = match &reached {
          Ok(reached) => Some(reached.shell.exec(&format!("curl -s {SLAVE}/card")).await),
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
      match asked.and_then(|asked| card_of(&asked).ok()) {
        None => {
          self.answering.remove(&name);
        }
        Some(answer) => {
          self.answering.insert(name.clone());
          self.take(&name, Some(answer), 0.0, None);
        }
      }
    }
    self.sweep_health();
  }

  /// One slave's answer taken into the picture, or the node marked lost where it gave none.
  fn take(&mut self, name: &str, answer: Option<Card>, read_ms: f64, error: Option<String>) {
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
          role,
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
        let rules = SlaveRules {
          loaded_at: said.rules.loaded_at,
          error: said.rules.error.clone(),
        };
        NodeView {
          name: name.to_owned(),
          role,
          desktop: said.desktop,
          version: Some(said.version.clone()),
          rules: Some(rules),
          occupants: said.occupants.clone(),
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
        let asked = reached.shell.exec(&format!("curl -s {SLAVE}/card")).await;
        (name, asked, began.elapsed().as_secs_f64() * 1000.0)
      });
    }
    while let Some(Ok((name, asked, read_ms))) = asking.join_next().await {
      match card_of(&asked) {
        Ok(answer) => self.take(&name, Some(answer), read_ms, None),
        Err(error) => self.take(&name, None, read_ms, Some(error)),
      }
    }
    self.act().await;
  }

  /// The units named moved on the node, one `PATCH /card-units/{unitName}` each, in order: whether every one
  /// moved, and the last status, or the first refusal with its status and why.
  async fn move_units(&self, node: &str, to: UnitMove, units: &[&str], reason: &str) -> (bool, String) {
    let Some(reached) = self.reached.get(node) else {
      return (false, "unreachable".into());
    };
    let mut status = String::new();
    for (at, unit) in units.iter().enumerate() {
      // A hold unloaded for a ticket keeps its lock and turn until the last of its units asked for is out.
      let holder = |name: &str| bare(name).split_once('/').map(|(service, _)| service.to_owned());
      let more = to == UnitMove::Evicted && units[at + 1..].iter().any(|next| holder(next) == holder(unit));
      let body = serde_json::to_string(&CardUnitChanges {
        state: to,
        reason: Some(reason.to_owned()),
        keeps_turn: more.then_some(true),
      })
      .expect("a move serializes")
      .replace('\'', "'\\''");
      let path = bare(unit).replace('/', "%2F");
      let ran = reached
        .shell
        .exec(&format!(
          "curl -s -o /dev/stdout -w '\\n%{{http_code}}' -X PATCH -H 'content-type: application/json' -d '{body}' {SLAVE}/card-units/{path}"
        ))
        .await;
      let (said, code) = ran.stdout.trim().rsplit_once('\n').unwrap_or(("", ran.stdout.trim()));
      if ran.code != 0 || code != "200" {
        let why = serde_json::from_str::<Value>(said)
          .ok()
          .and_then(|answer| answer.get("error").and_then(Value::as_str).map(str::to_owned));
        return match why {
          Some(why) => (false, format!("{code}: {why}")),
          None => (false, code.to_owned()),
        };
      }
      status = code.to_owned();
    }
    (true, status)
  }

  /// The answers of the verbs run beside the rounds that have come taken in, what the master unloaded to
  /// come back loaded back once its ticket is gone, every ticket naming a unit placed by the rules, the
  /// occupants that yield by their process stopped and started beside the rounds, and the demands' cards
  /// granted.
  pub async fn act(&mut self) {
    self.take_answers();
    self.restore_what_is_done().await;
    self.place_tickets().await;
    self.yield_processes();
    self.serve_demands();
  }

  /// An adapter verb run beside the rounds for the service on the node, its answer handed back to the
  /// first round after it comes; none where that verb of the service runs there already. The verb runs to
  /// its own end (docs/api.md, 「Adapters」).
  fn run(&mut self, declaration: &Declaration, node: &str, placed: Vec<String>, purpose: Purpose) {
    let verb = purpose.verb();
    if !self.runs.insert((declaration.service.clone(), node.to_owned(), verb)) {
      return;
    }
    let world = self.world.clone();
    let declaration = declaration.clone();
    let node = node.to_owned();
    let out = self.answers_out.clone();
    // The end of a start or a stop has the next round come at once.
    let wake = self.demands.clone().filter(|_| verb != "health");
    tokio::spawn(async move {
      let ran = async {
        let answer = world.adapter(&declaration, verb, &node, &placed).await;
        // where a service started answers now, which a demand granted there hands on
        let health = match purpose {
          Purpose::Back if answer.ok => Some(world.adapter(&declaration, "health", &node, &placed).await),
          _ => None,
        };
        (answer, health)
      };
      let (answer, health) = AssertUnwindSafe(ran)
        .catch_unwind()
        .await
        .unwrap_or_else(|_| (AdapterAnswer::refused("the adapter's run broke off"), None));
      let answered = Answered {
        service: declaration.service,
        node,
        purpose,
        answer,
        health,
      };
      // A master that has ended takes no answer.
      let _ = out.send(answered);
      if let Some(demands) = wake {
        demands.wake.notify_one();
      }
    });
  }

  /// The services whose `start` or `stop` runs beside the rounds on the node, each with the verb.
  fn moving_on<'a>(&'a self, node: &'a str) -> impl Iterator<Item = (&'a str, &'static str)> + 'a {
    self
      .runs
      .iter()
      .filter(move |(_, at, verb)| at == node && *verb != "health")
      .map(|(service, _, verb)| (service.as_str(), *verb))
  }

  /// Whether a `start` or a `stop` of the service runs beside the rounds on the node.
  fn moving(&self, service: &str, node: &str) -> bool {
    self.moving_on(node).any(|(one, _)| one == service)
  }

  /// Every answer of a verb run beside the rounds that has come, taken in and acted on.
  fn take_answers(&mut self) {
    let before = self.instances.clone();
    while let Ok(answered) = self.answers_in.try_recv() {
      self.take_answer(answered);
    }
    if self.instances != before {
      self.revision += 1;
    }
  }

  /// Every `start` and `stop` running beside the rounds waited for to its answer, and each answer taken: what
  /// a master read only once does before it ends.
  pub async fn finish_runs(&mut self) {
    while self.runs.iter().any(|(_, _, verb)| *verb != "health") {
      let Some(answered) = self.answers_in.recv().await else { return };
      self.take_answer(answered);
    }
  }

  /// One verb's answer acted on: a health read adopted where nothing moved the instance since it began, an
  /// occupant's stop or start said and kept, or its failure said once and tried again at the next round.
  fn take_answer(&mut self, answered: Answered) {
    let Answered {
      service,
      node,
      purpose,
      answer,
      health,
    } = answered;
    self.runs.remove(&(service.clone(), node.clone(), purpose.verb()));
    let key = (service, node);
    match purpose {
      Purpose::Health => {
        if self.touched.contains(&key) || self.bringing.contains_key(&key.1) {
          return;
        }
        match instance_of(&answer) {
          Some(instance) => self.standing_again(key, instance, "its health answering running"),
          None => {
            self.instances.remove(&key);
          }
        }
      }
      Purpose::Down(why) => {
        let failed = (key.0.clone(), key.1.clone(), "stop");
        if answer.ok {
          self.yields_failed.remove(&failed);
          let how = match why.kind {
            StopKind::Yield => "to yield the card",
            StopKind::Handover => "to hand the card over",
          };
          self.say(Some(&key.1), "occupant-stopped", format!("{} stopped {how}: {}", key.0, why.reason));
          self.instances.remove(&key);
          self.touched.insert(key.clone());
          self.stopped.insert(key, why);
          self.save_yields();
        } else if self.yields_failed.get(&failed) != Some(&answer.detail) {
          self.say(
            Some(&key.1),
            "occupant-stop-failed",
            format!("{} was not stopped ({}); trying again", key.0, answer.detail),
          );
          self.yields_failed.insert(failed, answer.detail);
        }
      }
      Purpose::Back => {
        let failed = (key.0.clone(), key.1.clone(), "start");
        if answer.ok {
          self.yields_failed.remove(&failed);
          self.say(
            Some(&key.1),
            "occupant-started",
            format!("{} started again: the card is its turn", key.0),
          );
          self.stopped.remove(&key);
          self.save_yields();
          self.touched.insert(key.clone());
          match health.as_ref().and_then(instance_of) {
            Some(instance) => self.instances.insert(key, instance),
            None => self.instances.remove(&key),
          };
        } else if self.yields_failed.get(&failed) != Some(&answer.detail) {
          self.say(
            Some(&key.1),
            "occupant-start-failed",
            format!("{} was not started ({}); trying again", key.0, answer.detail),
          );
          self.yields_failed.insert(failed, answer.detail);
        }
      }
    }
  }

  /// Each service's nodes as the master's placement keeps them.
  fn placements(&self) -> Placements {
    self
      .rules_file
      .parent()
      .and_then(|folder| std::fs::read_to_string(folder.join("services.json")).ok())
      .and_then(|text| serde_json::from_str(&text).ok())
      .unwrap_or_default()
  }

  fn declaration(&self, service: &str) -> Option<&Declaration> {
    self.declared.iter().find(|one| one.service == service)
  }

  /// How many demands keep the service's turn on the node in use.
  fn demands_on(&self, service: &str, node: &str) -> usize {
    self
      .demands
      .as_ref()
      .map_or(0, |demands| demands.book.lock().unwrap().on(service, node))
  }

  /// The nodes whose card the service uses now (docs/placement.md, 「A service's ceiling」): each card one of
  /// its demands holds, a card lent counting for the demand it is lent to, and each card of a node the master
  /// reaches where its turn is in use. The demand `own` names is counted by the seats given for it rather than
  /// by the book's.
  fn cards_used(
    &self,
    service: &str,
    book: Option<&Book>,
    own: Option<(i64, &[Seat])>,
  ) -> BTreeSet<String> {
    let mut used: BTreeSet<String> = self
      .nodes
      .values()
      .filter(|view| view.link.state == LinkState::Connected)
      .filter(|view| {
        view
          .cards
          .first()
          .and_then(|card| card.turn.as_ref())
          .is_some_and(|turn| turn.service == service && turn.in_use == Some(true))
      })
      .map(|view| view.name.clone())
      .collect();
    let others = book
      .into_iter()
      .flat_map(|book| book.held.values())
      .filter(|held| held.service == service && own.is_none_or(|(id, _)| held.id != id))
      .flat_map(|held| held.seats.iter());
    let mine = own.into_iter().flat_map(|(_, seats)| seats.iter());
    used.extend(others.chain(mine).filter_map(Seat::uses).map(str::to_owned));
    used
  }

  /// What holds the demand back: its service's cards and its pool's, the demand's own counted by `seats`.
  fn limits(&self, held: &Held, book: &Book, seats: &[Seat]) -> Limits {
    let used = self.cards_used(&held.service, Some(book), Some((held.id, seats)));
    let pool = held.pool.clone().map(|pool| {
      let using = book
        .held
        .values()
        .filter(|one| one.id != held.id && one.service == held.service)
        .filter(|one| one.pool.as_ref().is_some_and(|theirs| theirs.key == pool.key))
        .flat_map(|one| one.seats.iter())
        .chain(seats.iter())
        .filter_map(Seat::uses)
        .map(str::to_owned)
        .collect();
      (pool, using)
    });
    Limits { used, pool }
  }

  /// Whether the card on the node is one the service may not take: one it does not use while it uses as many
  /// as its declaration's ceiling lets it.
  fn above_ceiling(declaration: &Declaration, used: &BTreeSet<String>, node: &str) -> bool {
    declaration
      .cards
      .is_some_and(|ceiling| !used.contains(node) && used.len() >= ceiling as usize)
  }

  /// What a ticket of the service on the node is held for where the service is at its ceiling and does not
  /// use that card; none where it is below its ceiling, uses the card already, or declares none.
  fn ceiling_holds(&self, service: &str, node: &str) -> Option<String> {
    let declaration = self.declaration(service)?;
    declaration.cards?;
    let book = self.demands.as_ref().map(|demands| demands.book.lock().unwrap());
    let used = self.cards_used(service, book.as_deref(), None);
    Self::above_ceiling(declaration, &used, node).then(|| ceiling_text(declaration, &used))
  }

  /// The demands the routes ask and the rounds grant (docs/placement.md, 「Demands」).
  pub fn attach_demands(&mut self, demands: Arc<Demands>) {
    demands.book.lock().unwrap().placed = self.placements().services;
    self.demands = Some(demands);
  }

  /// Where the service stands on the node now: on every node it is placed on for a service with no
  /// adapter, else where its health last answered running and the master has neither stopped it nor is
  /// stopping it.
  fn stands(&self, declaration: &Declaration, node: &str) -> Option<Instance> {
    if declaration.adapter.is_none() {
      return Some(Instance::default());
    }
    let key = (declaration.service.clone(), node.to_owned());
    if self.stopped.contains_key(&key) || self.runs.contains(&(key.0.clone(), key.1.clone(), "stop")) {
      return None;
    }
    self.instances.get(&key).cloned()
  }

  /// The claim or the ticket of an occupant of a lower tier than `tier` that holds the node's card, named.
  fn held_by(&self, rules: &Rules, tier: f64, node: &str) -> Option<String> {
    let card = self.nodes.get(node)?.cards.first()?;
    card
      .claim
      .as_ref()
      .filter(|claim| rules.tier_of(&claim.claimant_name) < tier)
      .map(|claim| claim.claimant_name.clone())
      .or_else(|| {
        card
          .turn
          .as_ref()
          .filter(|turn| rules.tier_of(&turn.service) < tier)
          .map(|turn| turn.service.clone())
      })
      .or_else(|| {
        card
          .queue
          .iter()
          .find(|ticket| !ticket.rest && rules.tier_of(&ticket.service) < tier)
          .map(|ticket| ticket.service.clone())
      })
  }

  /// Whether the service fits on the node's card beside what stands there (docs/placement.md, 「Services that
  /// fit share a card」): the card's free memory as last read, less its group's reserve, its own processes'
  /// memory counted free, and less the declared memory of every other service standing there whose process
  /// that reading does not show yet, holds what it declares. A card with no free memory read fits nothing.
  fn fits(&self, rules: &Rules, declaration: &Declaration, node: &str) -> bool {
    let Some(view) = self.nodes.get(node) else { return false };
    let Some(card) = view.cards.first() else { return false };
    let Some(free) = card.free_mib else { return false };
    let service = declaration.service.as_str();
    let facts = NodeFacts {
      name: node.to_owned(),
      role: view.role.clone(),
      desktop: view.desktop,
    };
    let reserve = rules.group_of(&facts).map_or(0.0, |group| group.reserve_mib.0);
    let processes = card.processes.as_deref().unwrap_or_default();
    let shown = |name: &str| processes.iter().any(|one| one.service_name.as_deref() == Some(name));
    let own: f64 = processes
      .iter()
      .filter(|one| one.service_name.as_deref() == Some(service))
      .map(|one| one.used_mib)
      .sum();
    let unshown: f64 = self
      .instances
      .keys()
      .filter(|(other, at)| at == node && other != service && !shown(other))
      .filter(|(other, at)| !self.stopped.contains_key(&(other.clone(), at.clone())))
      .filter_map(|(other, _)| self.declaration(other))
      .map(|other| other.card_mib as f64)
      .sum();
    free.0 + own - reserve - unshown >= declaration.card_mib as f64
  }

  /// What a card of the service's placement is to a demand of it, `seated` being the services each node's
  /// demands hold it for.
  fn opening(
    &self,
    rules: &Rules,
    declaration: &Declaration,
    node: &str,
    seated: &HashMap<String, Vec<String>>,
  ) -> Opening {
    let service = declaration.service.as_str();
    let tier = rules.tier_of(service);
    let Some(card) = self
      .nodes
      .get(node)
      .filter(|view| view.link.state == LinkState::Connected)
      .and_then(|view| view.cards.first())
    else {
      return Opening::Closed;
    };
    if let Some(by) = self.held_by(rules, tier, node) {
      return Opening::Held(by);
    }
    // a card holds one demand of a service. Demands of others keep it from a service that yields by its
    // process only where theirs do not stand there yielding by their process too, as on a card lent or coming
    // back, or where it neither stands nor fits there; from any other service always.
    let using = seated.get(node).filter(|using| !using.is_empty());
    let beside = |one: &String| {
      one != service
        && self
          .declaration(one)
          .is_some_and(|other| other.yield_by == YieldBy::Process && self.stands(other, node).is_some())
    };
    if let Some(using) =
      using.filter(|using| declaration.yield_by != YieldBy::Process || !using.iter().all(beside))
    {
      return Opening::InUse(using.join(", "));
    }
    if let Some(bring) = self.bringing.get(node) {
      return Opening::InUse(bring.service.clone());
    }
    // a start or a stop running there: its own start has the service starting, another's keeps the card
    let moving: Vec<(&str, &str)> = self.moving_on(node).collect();
    if moving.contains(&(service, "start")) {
      return Opening::Starting;
    }
    if let Some((other, _)) = moving.iter().find(|(one, _)| *one != service) {
      return Opening::InUse((*other).to_owned());
    }
    if !moving.is_empty() {
      return Opening::Closed;
    }
    // a turn of its tier or a lower one in use; an occupant yielding by its process is in use by its demands
    if let Some(turn) =
      card.turn.as_ref().filter(|turn| turn.service != service && turn.in_use == Some(true))
      && rules.tier_of(&turn.service) <= tier
      && self.declaration(&turn.service).is_none_or(|one| one.yield_by != YieldBy::Process)
    {
      return Opening::InUse(turn.service.clone());
    }
    if let Some(instance) = self.stands(declaration, node) {
      return Opening::Stands(instance);
    }
    if declaration.yield_by != YieldBy::Process {
      return Opening::Closed;
    }
    if self.fits(rules, declaration, node) {
      return Opening::Bring(Vec::new());
    }
    if let Some(using) = using {
      return Opening::InUse(using.join(", "));
    }
    let mut handover = Vec::new();
    for (other, at) in self.instances.keys() {
      if at != node || other == service || self.stopped.contains_key(&(other.clone(), at.clone())) {
        continue;
      }
      let Some(standing) = self.declaration(other).filter(|one| one.yield_by == YieldBy::Process) else {
        continue;
      };
      let theirs = rules.tier_of(other);
      if theirs < tier {
        return Opening::InUse(other.clone());
      }
      if theirs == tier {
        handover.push(standing.clone());
      }
    }
    Opening::Bring(handover)
  }

  /// The card a demand of the service is granted next by the one rule — where it stands idle, else where it
  /// can be brought up, the card its work key was last granted on first among those equally open, then the
  /// placement's order, never a card that would put it above its ceiling or its pool above its limit —
  /// with every card's opening, which says what it waits on where none is open.
  #[allow(clippy::too_many_arguments)]
  fn choose(
    &self,
    rules: &Rules,
    declaration: &Declaration,
    work_key: Option<&str>,
    seated: &HashMap<String, Vec<String>>,
    limits: &Limits,
    placements: &Placements,
  ) -> (Option<(String, Opening)>, Openings) {
    let openings: Openings = placements
      .nodes_of(&declaration.service)
      .iter()
      .map(|node| {
        let opening = match self.opening(rules, declaration, node, seated) {
          Opening::Stands(_) | Opening::Bring(_) if limits.above_pool(node) => Opening::Pool,
          Opening::Stands(_) | Opening::Bring(_)
            if Self::above_ceiling(declaration, &limits.used, node) =>
          {
            Opening::Ceiling
          }
          opening => opening,
        };
        (node.clone(), opening)
      })
      .collect();
    let last = work_key.and_then(|key| self.work_cards.get(key));
    let chosen = openings
      .iter()
      .filter_map(|(node, opening)| {
        let open = match opening {
          Opening::Stands(_) => 0,
          Opening::Bring(_) => 1,
          _ => return None,
        };
        Some(((open, u8::from(Some(node) != last)), (node, opening)))
      })
      .min_by_key(|(rank, _)| *rank)
      .map(|(_, (node, opening))| (node.clone(), opening.clone()));
    (chosen, openings)
  }

  /// A card taken for a demand: granted where the service stands, else the service brought up there.
  fn take_card(
    &mut self,
    held: &Held,
    declaration: &Declaration,
    node: &str,
    opening: Opening,
    placements: &Placements,
    seated: &mut HashMap<String, Vec<String>>,
  ) -> Seat {
    seated.entry(node.to_owned()).or_default().push(held.service.clone());
    match opening {
      Opening::Bring(handover) => {
        self.bring(held.id, declaration, node, handover, placements);
        Seat::Starting(node.to_owned())
      }
      Opening::Stands(instance) => self.grant(held, node, &instance),
      Opening::InUse(_)
      | Opening::Starting
      | Opening::Held(_)
      | Opening::Ceiling
      | Opening::Pool
      | Opening::Closed => Seat::Waiting,
    }
  }

  /// The card a waiting demand takes from demands of a worse rank (docs/placement.md, 「A demand's rank」), where
  /// one may be taken: every demand on it at a worse rank and holding it plainly, within the demand's limits,
  /// and the service to be had there were those demands gone. Their worst rank first; then a card where the
  /// service stands, so no engine stops; then its work key's; then the one whose demand was asked last.
  #[allow(clippy::too_many_arguments)]
  fn outrank(
    &self,
    rules: &Rules,
    declaration: &Declaration,
    held: &Held,
    book: &Book,
    seated: &HashMap<String, Vec<String>>,
    limits: &Limits,
    placements: &Placements,
  ) -> Option<(String, Opening)> {
    let last = held.work_key.as_ref().and_then(|key| self.work_cards.get(key));
    placements
      .nodes_of(&declaration.service)
      .iter()
      .filter(|node| !limits.above_pool(node) && !Self::above_ceiling(declaration, &limits.used, node))
      .filter_map(|node| {
        let on: Vec<&Held> = book
          .held
          .values()
          .filter(|one| one.seats.iter().any(|seat| seat.node() == Some(node.as_str())))
          .collect();
        let plain = |seat: &Seat| {
          seat.node() != Some(node.as_str()) || matches!(seat, Seat::Granted(_) | Seat::Lent(_))
        };
        if on.is_empty() || !on.iter().all(|one| one.rank > held.rank && one.seats.iter().all(plain)) {
          return None;
        }
        let mut gone = seated.clone();
        gone.remove(node);
        let opening = self.opening(rules, declaration, node, &gone);
        let stands = match opening {
          Opening::Stands(_) => 0,
          Opening::Bring(_) => 1,
          _ => return None,
        };
        let worst = on.iter().map(|one| one.rank).max()?;
        let latest = on.iter().map(|one| one.id).max()?;
        let rank = (
          std::cmp::Reverse(worst),
          stands,
          u8::from(Some(node) != last),
          std::cmp::Reverse(latest),
        );
        Some((rank, (node.clone(), opening)))
      })
      .min_by_key(|(rank, _)| *rank)
      .map(|(_, chosen)| chosen)
  }

  /// Every demand on the node put back in the line, its seat there given up to the demand of a better rank
  /// that takes the card; each said, so how often a card is taken can be read.
  fn take_seats(&mut self, book: &mut Book, node: &str, taker: &Held) {
    let by = taker.reference.clone().unwrap_or_else(|| format!("a demand of rank {}", taker.rank));
    let losing: Vec<Held> = book
      .held
      .values()
      .filter(|one| one.seats.iter().any(|seat| seat.node() == Some(node)))
      .cloned()
      .collect();
    for one in losing {
      let seats = one
        .seats
        .iter()
        .map(|seat| {
          if seat.node() == Some(node) {
            Seat::Outranked { by: by.clone() }
          } else {
            seat.clone()
          }
        })
        .collect();
      book.seat(one.id, seats, one.reason.clone());
      let named = one.reference.as_deref().unwrap_or("a demand");
      self.say(
        Some(node),
        "card-outranked",
        format!(
          "{named} of {} (rank {}) gave its card to {by} (rank {})",
          one.service, one.rank, taker.rank
        ),
      );
    }
  }

  /// A card granted, the node kept as the one its work key was last granted on.
  fn grant(&mut self, held: &Held, node: &str, instance: &Instance) -> Seat {
    if let Some(key) = &held.work_key {
      self.work_cards.insert(key.clone(), node.to_owned());
    }
    Seat::Granted(card_at(node, instance))
  }

  /// The service brought up on the node for a demand, apart from the rounds: the occupants of its tier
  /// standing there handed over first, then its adapter's `start` where it yields by its process, then its
  /// `health`, which says where it answers.
  fn bring(
    &mut self,
    demand: i64,
    declaration: &Declaration,
    node: &str,
    handover: Vec<Declaration>,
    placements: &Placements,
  ) {
    for one in &handover {
      let key = (one.service.clone(), node.to_owned());
      self.instances.remove(&key);
      self.touched.insert(key.clone());
      let stop = Stop {
        reason: format!("{} takes the card for a demand", declaration.service),
        kind: StopKind::Handover,
      };
      self.stopped.insert(key, stop);
    }
    let key = (declaration.service.clone(), node.to_owned());
    self.touched.insert(key.clone());
    if self.stopped.remove(&key).is_some() || !handover.is_empty() {
      self.save_yields();
    }
    let stops: Vec<(Declaration, Vec<String>)> = handover
      .into_iter()
      .map(|one| {
        let placed = placements.nodes_of(&one.service).to_vec();
        (one, placed)
      })
      .collect();
    let placed = placements.nodes_of(&declaration.service).to_vec();
    let world = self.world.clone();
    let service = declaration.clone();
    let at = node.to_owned();
    let wake = self.demands.clone();
    let bring = async move {
      let mut stopped = Vec::new();
      for (one, placed) in &stops {
        let answer = world.adapter(one, "stop", &at, placed).await;
        let ok = answer.ok;
        stopped.push((one.service.clone(), answer));
        if !ok {
          let started = Err(format!("{} was not stopped", one.service));
          return Brought { stopped, started };
        }
      }
      if service.yield_by == YieldBy::Process {
        let answer = world.adapter(&service, "start", &at, &placed).await;
        if !answer.ok {
          return Brought {
            stopped,
            started: Err(answer.detail),
          };
        }
      }
      let health = world.adapter(&service, "health", &at, &placed).await;
      let started = instance_of(&health).ok_or_else(|| {
        format!(
          "its health answered {} ({})",
          health.state.as_deref().unwrap_or("nothing"),
          health.detail
        )
      });
      Brought { stopped, started }
    };
    // The bring's end has the next round come at once, so its grant waits on no round's rest.
    let handle = tokio::spawn(async move {
      let brought = bring.await;
      if let Some(demands) = wake {
        demands.wake.notify_one();
      }
      brought
    });
    self.start_failed.remove(&demand);
    let bringing = Bringing {
      service: declaration.service.clone(),
      demand,
      handle,
    };
    self.bringing.insert(node.to_owned(), bringing);
  }

  /// Every bring that has ended, taken off the nodes it held.
  fn collect_brought(&mut self) -> Vec<(String, String, i64, Brought)> {
    let ended: Vec<String> = self
      .bringing
      .iter()
      .filter(|(_, bring)| bring.handle.is_finished())
      .map(|(node, _)| node.clone())
      .collect();
    ended
      .into_iter()
      .filter_map(|node| {
        let bring = self.bringing.remove(&node)?;
        let brought = bring.handle.now_or_never()?.unwrap_or_else(|fault| Brought {
          stopped: Vec::new(),
          started: Err(fault.to_string()),
        });
        Some((node, bring.service, bring.demand, brought))
      })
      .collect()
  }

  /// What a bring came to, said, and written into its demand's seats: granted where the service stands, back
  /// in the line where it does not, the card it was lent from owed back where nothing else holds it.
  fn settle(&mut self, book: &mut Book, node: String, service: String, demand: i64, brought: Brought) {
    let mut kept_changed = false;
    for (other, answer) in brought.stopped {
      if answer.ok {
        self.say(
          Some(&node),
          "occupant-stopped",
          format!("{other} stopped to hand the card over: {service} takes it for a demand"),
        );
      } else {
        self.stopped.remove(&(other.clone(), node.clone()));
        kept_changed = true;
        self.say(
          Some(&node),
          "occupant-stop-failed",
          format!("{other} was not stopped ({}); {service} is not brought up there", answer.detail),
        );
      }
    }
    if kept_changed {
      self.save_yields();
    }
    let key = (service.clone(), node.clone());
    self.touched.insert(key.clone());
    let started = match brought.started {
      Ok(instance) => {
        self.say(Some(&node), "occupant-started", format!("{service} started for a demand"));
        self.yields_failed.remove(&(service.clone(), node.clone(), "start"));
        self.instances.insert(key, instance.clone());
        Ok(instance)
      }
      Err(why) => {
        self.say(
          Some(&node),
          "occupant-start-failed",
          format!("{service} was not started ({why}); trying again"),
        );
        self.instances.remove(&key);
        Err(why)
      }
    };
    if let Some(held) = book.held.get(&demand).cloned() {
      let mut seats = held.seats.clone();
      for seat in seats.iter_mut() {
        let here = matches!(seat, Seat::Starting(at) | Seat::Returning(at) if *at == node);
        match (&started, here) {
          (Ok(instance), true) => *seat = self.grant(&held, &node, instance),
          (Err(_), true) if matches!(seat, Seat::Starting(_)) => *seat = Seat::Waiting,
          _ => {}
        }
      }
      if let Err(why) = &started {
        self.start_failed.insert(demand, why.clone());
      }
      book.seat(demand, seats, held.reason.clone());
    }
    // a card lent for a bring that failed, or for a demand given back meanwhile, held by nothing else now
    let holds: usize = book
      .held
      .values()
      .flat_map(|held| &held.seats)
      .filter(|seat| matches!(seat, Seat::Starting(at) | Seat::Granted(DemandCard { node_name: at, .. }) if *at == node))
      .count();
    if holds == 0 {
      let owed: Vec<(i64, Vec<Seat>, Option<DemandReason>)> = book
        .held
        .values()
        .filter(|held| held.seats.iter().any(|seat| matches!(seat, Seat::Lent(at) if *at == node)))
        .map(|held| {
          let seats = held
            .seats
            .iter()
            .map(|seat| match seat {
              Seat::Lent(at) if *at == node => Seat::Returning(at.clone()),
              other => other.clone(),
            })
            .collect();
          (held.id, seats, held.reason.clone())
        })
        .collect();
      for (id, seats, reason) in owed {
        book.seat(id, seats, reason);
      }
    }
  }

  /// Every placed instance's health read beside the rounds, one read at a time for each service and node,
  /// none where the service is being brought up, started or stopped there; each answer is taken by the
  /// round after it comes.
  fn sweep_health(&mut self) {
    let placements = self.placements();
    let mut asks: Vec<(Declaration, String, Vec<String>)> = Vec::new();
    for one in self.declared.iter().filter(|one| one.adapter.is_some()) {
      let placed = placements.nodes_of(&one.service).to_vec();
      for node in &placed {
        let reading = self.runs.contains(&(one.service.clone(), node.clone(), "health"));
        if !reading && !self.bringing.contains_key(node) && !self.moving(&one.service, node) {
          asks.push((one.clone(), node.clone(), placed.clone()));
        }
      }
    }
    for (one, node, placed) in asks {
      self.touched.remove(&(one.service.clone(), node.clone()));
      self.run(&one, &node, placed, Purpose::Health);
    }
  }

  /// Each demand's cards granted by the one rule (docs/placement.md, 「Demands」), in the line's order:
  /// a card where the service stands idle, else one it can be brought up on, else the line; a demand asked
  /// within another granted beside it on its card for the same service, and lent that card for another
  /// where no other is open; a card a lower tier took granted again once the service stands there again or
  /// another opens. A demand whose lease ran out is given back first; nothing else here waits on time.
  fn serve_demands(&mut self) {
    let Some(demands) = self.demands.clone() else { return };
    let rules = self.rules_now();
    let placements = self.placements();
    let brought = self.collect_brought();
    let mut book = demands.book.lock().unwrap();
    book.placed = placements.services.clone();
    for lapsed in book.lapse(millis(self.world.clock())) {
      let named = lapsed.reference.as_deref().unwrap_or("no reference");
      self.say(
        None,
        "demand-lapsed",
        format!(
          "{}: demand {} ({named}) given back, its lease run out with no call on it",
          lapsed.service, lapsed.id
        ),
      );
    }
    let mut failed_now: HashSet<i64> = HashSet::new();
    for (node, service, demand, brought) in brought {
      if brought.started.is_err() {
        failed_now.insert(demand);
      }
      self.settle(&mut book, node, service, demand, brought);
    }
    self.start_failed.retain(|demand, _| book.held.contains_key(demand));
    let mut seated: HashMap<String, Vec<String>> = HashMap::new();
    for held in book.held.values() {
      for node in held.seats.iter().filter_map(Seat::node) {
        seated.entry(node.to_owned()).or_default().push(held.service.clone());
      }
    }
    // the line: by rank, then by the order demands were asked in
    let mut line: Vec<(u32, i64)> = book.held.values().map(|held| (held.rank, held.id)).collect();
    line.sort_unstable();
    for (_, id) in line {
      let Some(held) = book.held.get(&id).cloned() else { continue };
      let Some(declaration) = self.declaration(&held.service).cloned() else {
        let reason = DemandReason {
          code: DemandWait::InUse,
          text: format!("{} is declared nowhere", held.service),
        };
        book.seat(id, held.seats.clone(), Some(reason));
        continue;
      };
      let tier = rules.tier_of(&held.service);
      let outer = held.within.and_then(|outer| book.held.get(&outer)).cloned();
      let mut seats = held.seats.clone();
      let mut openings: Option<Openings> = None;
      for at in 0..seats.len() {
        // the cards its service uses but for this seat's, which is what is being decided
        let others: Vec<Seat> = seats
          .iter()
          .enumerate()
          .filter(|(one, _)| *one != at)
          .map(|(_, seat)| seat.clone())
          .collect();
        let limits = self.limits(&held, &book, &others);
        match seats[at].clone() {
          Seat::Granted(card) => {
            if let Some(by) = self.held_by(&rules, tier, &card.node_name) {
              seats[at] = Seat::Taken {
                node: card.node_name,
                by,
              };
            } else if let Some(instance) = self.stands(&declaration, &card.node_name) {
              seats[at] = Seat::Granted(card_at(&card.node_name, &instance));
            }
          }
          Seat::Taken { node, by } => {
            let holder = self.held_by(&rules, tier, &node);
            if holder.is_none()
              && let Some(instance) = self.stands(&declaration, &node)
            {
              seats[at] = self.grant(&held, &node, &instance);
              continue;
            }
            let (chosen, seen) =
              self.choose(&rules, &declaration, held.work_key.as_deref(), &seated, &limits, &placements);
            openings = Some(seen);
            if let Some((to, opening)) = chosen {
              if let Some(using) = seated.get_mut(&node)
                && let Some(mine) = using.iter().position(|one| *one == held.service)
              {
                using.remove(mine);
              }
              seats[at] = self.take_card(&held, &declaration, &to, opening, &placements, &mut seated);
            } else if let Some(holder) = holder.filter(|holder| *holder != by) {
              seats[at] = Seat::Taken { node, by: holder };
            }
          }
          Seat::Waiting | Seat::Outranked { .. } if !failed_now.contains(&id) => {
            let outer_card = outer.as_ref().and_then(|outer| {
              outer.seats.iter().find_map(|seat| match seat {
                Seat::Granted(card) => Some(card.clone()),
                _ => None,
              })
            });
            // asked within a demand on the service it stands on: beside it, on its own card
            if let (Some(outer), Some(card)) = (&outer, &outer_card)
              && outer.service == held.service
              && !seats.iter().any(|seat| seat.node() == Some(card.node_name.as_str()))
            {
              seated.entry(card.node_name.clone()).or_default().push(held.service.clone());
              let instance = Instance {
                address: card.address.clone(),
                key_file: card.key_file.clone(),
              };
              seats[at] = self.grant(&held, &card.node_name, &instance);
              continue;
            }
            let (chosen, seen) =
              self.choose(&rules, &declaration, held.work_key.as_deref(), &seated, &limits, &placements);
            openings = Some(seen);
            if let Some((to, opening)) = chosen {
              seats[at] = self.take_card(&held, &declaration, &to, opening, &placements, &mut seated);
              continue;
            }
            // no other card open: the caller's own card lent to it, where only the caller's demand holds it
            if let (Some(outer), Some(card)) = (&outer, &outer_card)
              && outer.service != held.service
              && seated.get(&card.node_name).is_some_and(|using| using.len() == 1)
              && !self.bringing.contains_key(&card.node_name)
              && self.moving_on(&card.node_name).next().is_none()
              && self.held_by(&rules, tier, &card.node_name).is_none()
              && !Self::above_ceiling(&declaration, &limits.used, &card.node_name)
              && !limits.above_pool(&card.node_name)
              && (declaration.yield_by == YieldBy::Process
                || self.stands(&declaration, &card.node_name).is_some())
            {
              let node = card.node_name.clone();
              let mut lent = outer.seats.clone();
              if let Some(seat) = lent
                .iter_mut()
                .find(|seat| matches!(seat, Seat::Granted(mine) if mine.node_name == node))
              {
                *seat = Seat::Lent(node.clone());
              }
              book.seat(outer.id, lent, None);
              let handover: Vec<Declaration> = self
                .declaration(&outer.service)
                .filter(|one| one.yield_by == YieldBy::Process)
                .cloned()
                .into_iter()
                .collect();
              seated.entry(node.clone()).or_default().push(held.service.clone());
              self.bring(id, &declaration, &node, handover, &placements);
              seats[at] = Seat::Starting(node);
              continue;
            }
            // no card open and none to lend: one taken from demands of a worse rank
            if let Some((to, opening)) =
              self.outrank(&rules, &declaration, &held, &book, &seated, &limits, &placements)
            {
              self.take_seats(&mut book, &to, &held);
              seated.remove(&to);
              seats[at] = self.take_card(&held, &declaration, &to, opening, &placements, &mut seated);
            }
          }
          // its card back from the demand it was lent to, once its service is below its ceiling
          Seat::Returning(node)
            if !self.bringing.contains_key(&node)
              && self.moving_on(&node).next().is_none()
              && !limits.above_pool(&node)
              && !Self::above_ceiling(&declaration, &limits.used, &node) =>
          {
            let handover: Vec<Declaration> = self
              .instances
              .keys()
              .filter(|(other, on)| *on == node && *other != held.service)
              .filter_map(|(other, _)| self.declaration(other))
              .filter(|one| one.yield_by == YieldBy::Process)
              .cloned()
              .collect();
            self.bring(id, &declaration, &node, handover, &placements);
          }
          _ => {}
        }
      }
      let limits = self.limits(&held, &book, &seats);
      let reason =
        self.reason(&held, &seats, openings, &rules, &declaration, &seated, &limits, &placements);
      book.seat(id, seats, reason);
    }
    let version = book.version;
    drop(book);
    if version != self.demands_seen {
      self.demands_seen = version;
      self.revision += 1;
      demands.tell();
    }
  }

  /// What a demand's cards wait on, one reason at a time; none where every card is granted or lent.
  #[allow(clippy::too_many_arguments)]
  fn reason(
    &self,
    held: &Held,
    seats: &[Seat],
    openings: Option<Openings>,
    rules: &Rules,
    declaration: &Declaration,
    seated: &HashMap<String, Vec<String>>,
    limits: &Limits,
    placements: &Placements,
  ) -> Option<DemandReason> {
    let service = &held.service;
    if seats.iter().all(|seat| matches!(seat, Seat::Granted(_) | Seat::Lent(_))) {
      return None;
    }
    if let Some(why) = self.start_failed.get(&held.id) {
      return Some(DemandReason {
        code: DemandWait::StartFailed,
        text: format!("{service} did not start ({why}); it is tried again at the next round"),
      });
    }
    let used = &limits.used;
    let ceiling = || DemandReason {
      code: DemandWait::Ceiling,
      text: ceiling_text(declaration, used),
    };
    // a card coming back that no bring took up this round waits on its service's ceiling
    if seats
      .iter()
      .any(|seat| matches!(seat, Seat::Returning(node) if !self.bringing.contains_key(node)))
    {
      return Some(ceiling());
    }
    if let Some(node) = seats.iter().find_map(|seat| match seat {
      Seat::Starting(node) | Seat::Returning(node) => Some(node),
      _ => None,
    }) {
      return Some(DemandReason {
        code: DemandWait::Starting,
        text: format!("{service} is starting on {node}"),
      });
    }
    if let Some((node, by)) = seats.iter().find_map(|seat| match seat {
      Seat::Taken { node, by } => Some((node, by)),
      _ => None,
    }) {
      return Some(DemandReason {
        code: DemandWait::Held,
        text: format!("{by} holds the card {service} was granted on {node}"),
      });
    }
    // a start that keeps failing on a node of its placement, which a demand would otherwise read as starting
    let failing = placements.nodes_of(service).iter().find_map(|node| {
      let reached = self.nodes.get(node).is_some_and(|view| view.link.state == LinkState::Connected);
      let why = self.yields_failed.get(&(service.clone(), node.clone(), "start")).filter(|_| reached)?;
      Some((node, why))
    });
    if let Some((node, why)) = failing {
      return Some(DemandReason {
        code: DemandWait::StartFailed,
        text: format!("{service} did not start on {node} ({why}); it is tried again at the next round"),
      });
    }
    let openings = openings.unwrap_or_else(|| {
      self.choose(rules, declaration, held.work_key.as_deref(), seated, limits, placements).1
    });
    let named = |pick: fn(&Opening) -> Option<&String>| -> Vec<String> {
      openings
        .iter()
        .filter_map(|(node, opening)| pick(opening).map(|who| format!("{node} by {who}")))
        .collect()
    };
    let in_use = named(|opening| match opening {
      Opening::InUse(who) => Some(who),
      _ => None,
    });
    let held_by = named(|opening| match opening {
      Opening::Held(who) => Some(who),
      _ => None,
    });
    let starting = openings
      .iter()
      .find(|(_, opening)| matches!(opening, Opening::Starting))
      .map(|(node, _)| node);
    // at its ceiling, no card it does not use is open to it, whatever else holds them
    let capped = declaration.cards.is_some_and(|ceiling| used.len() >= ceiling as usize)
      || openings.iter().any(|(_, opening)| matches!(opening, Opening::Ceiling));
    let pooled = limits.pool.as_ref().filter(|(pool, using)| {
      using.len() >= pool.cards as usize
        || openings.iter().any(|(_, opening)| matches!(opening, Opening::Pool))
    });
    Some(if let Some((pool, using)) = pooled {
      let names: Vec<&str> = using.iter().map(String::as_str).collect();
      DemandReason {
        code: DemandWait::Pool,
        text: format!(
          "the pool {} uses {} cards of {service} and its limit is {}: {}",
          pool.key,
          using.len(),
          pool.cards,
          names.join(", ")
        ),
      }
    } else if capped {
      ceiling()
    } else if let Some(node) = starting {
      DemandReason {
        code: DemandWait::Starting,
        text: format!("{service} is starting on {node}"),
      }
    } else if !in_use.is_empty() {
      DemandReason {
        code: DemandWait::InUse,
        text: format!("every card {service} may have is in use: {}", in_use.join(", ")),
      }
    } else if !held_by.is_empty() {
      DemandReason {
        code: DemandWait::Held,
        text: format!("every card {service} may have is held by a lower tier: {}", held_by.join(", ")),
      }
    } else {
      DemandReason {
        code: DemandWait::InUse,
        text: format!("no card {service} may have answers now"),
      }
    })
  }

  /// Every occupant that yields by its process stopped on a node where something of a lower tier, or a
  /// request of its own tier while no demand keeps its turn in use, wants the card, and started again once
  /// nothing does; one stopped for an occupant of its tier that yields by its process too hands the card
  /// over and is not brought back (docs/placement.md, 「Yielding by the process」). Each stop and start runs
  /// beside the rounds, none of a service on a node while one of it runs there.
  fn yield_processes(&mut self) {
    let rules = self.rules_now();
    let placements = self.placements();
    let yields_by_process = |service: &str| {
      self
        .declared
        .iter()
        .any(|one| one.service == service && one.yield_by == YieldBy::Process)
    };
    let mut down: Vec<(Declaration, String, Stop)> = Vec::new();
    let mut up: Vec<(Declaration, String)> = Vec::new();
    for declaration in self.declared.iter().filter(|one| one.yield_by == YieldBy::Process) {
      let service = declaration.service.as_str();
      let tier = rules.tier_of(service);
      for node in placements.nodes_of(service) {
        let Some(view) = self.nodes.get(node).filter(|view| view.link.state == LinkState::Connected) else {
          continue;
        };
        let Some(card) = view.cards.first() else { continue };
        let kept = self.stopped.get(&(service.to_owned(), node.clone()));
        let stopped = kept.is_some();
        // A service being brought up there for a demand is the bring's to settle.
        if self.bringing.get(node).is_some_and(|bring| bring.service == service) {
          continue;
        }
        let in_use = self.demands_on(service, node) > 0;
        let facts = NodeFacts {
          name: node.clone(),
          role: view.role.clone(),
          desktop: view.desktop,
        };
        let reserve = rules.group_of(&facts).map_or(0.0, |group| group.reserve_mib.0);
        let room = card.free_mib.map_or(0.0, |free| free.0) - reserve;
        // How it goes down for another occupant that wants the card: a hand-over to one of its own tier that
        // yields by its process as well, a yield to be brought back from to any other. One of its own tier
        // makes it go down only while no demand keeps its turn in use.
        let stop_for = |other: &str| -> Option<StopKind> {
          let theirs = rules.tier_of(other);
          if theirs < tier {
            Some(StopKind::Yield)
          } else if theirs == tier && !in_use {
            Some(if yields_by_process(other) { StopKind::Handover } else { StopKind::Yield })
          } else {
            None
          }
        };
        // what makes it go down: a claim of a lower tier, a turn of a lower or the same tier in use, and a
        // request of either waiting that the card's free memory does not hold; kept down while any stands.
        // A rest is not a use: the process it was stopped for comes back and waits for the lock, and the
        // rest gives way to it (`docs/node-arbiter.md`, 「Resting」).
        let mut why: Option<Stop> = card
          .claim
          .as_ref()
          .filter(|claim| rules.tier_of(&claim.claimant_name) < tier)
          .map(|claim| Stop {
            reason: format!("{} claims the card", claim.claimant_name),
            kind: StopKind::Yield,
          });
        if why.is_none()
          && let Some(turn) = card.turn.as_ref().filter(|turn| turn.service != service)
          && turn.in_use == Some(true)
          && let Some(kind) = stop_for(&turn.service)
        {
          why = Some(Stop {
            reason: format!("{}'s turn holds the card", turn.service),
            kind,
          });
        }
        if why.is_none() {
          why = card
            .queue
            .iter()
            .filter(|ticket| ticket.service != service && !ticket.rest)
            .filter(|ticket| stopped || ticket.vram_mib.is_none_or(|need| need.0 > room))
            .filter(|ticket| self.ceiling_holds(&ticket.service, node).is_none())
            .find_map(|ticket| {
              stop_for(&ticket.service).map(|kind| Stop {
                reason: format!("{} asks for the card", ticket.service),
                kind,
              })
            });
        }
        match (why, kept.map(|stop| stop.kind)) {
          (Some(why), None) => down.push((declaration.clone(), node.clone(), why)),
          (None, Some(StopKind::Yield)) => up.push((declaration.clone(), node.clone())),
          _ => {}
        }
      }
    }
    // a stop kept for a service no longer declared to yield by its process, or no longer placed there, is
    // started all the same where it was a yield, and forgotten where it was a hand-over
    let mut forgotten = Vec::new();
    for ((service, node), stop) in &self.stopped {
      let yields = self
        .declared
        .iter()
        .find(|one| &one.service == service && one.yield_by == YieldBy::Process);
      if let Some(declaration) = yields
        && !placements.nodes_of(service).contains(node)
      {
        match stop.kind {
          StopKind::Yield => up.push((declaration.clone(), node.clone())),
          StopKind::Handover => forgotten.push((service.clone(), node.clone())),
        }
      }
    }
    if !forgotten.is_empty() {
      for key in forgotten {
        self.stopped.remove(&key);
      }
      self.save_yields();
    }
    for (declaration, node, why) in down {
      if self.moving(&declaration.service, &node) {
        continue;
      }
      let placed = placements.nodes_of(&declaration.service).to_vec();
      self.touched.insert((declaration.service.clone(), node.clone()));
      self.run(&declaration, &node, placed, Purpose::Down(why));
    }
    for (declaration, node) in up {
      if self.moving(&declaration.service, &node) {
        continue;
      }
      let placed = placements.nodes_of(&declaration.service).to_vec();
      self.touched.insert((declaration.service.clone(), node.clone()));
      self.run(&declaration, &node, placed, Purpose::Back);
    }
  }

  async fn restore_what_is_done(&mut self) {
    let mut due: Vec<(String, String, Vec<String>)> = Vec::new();
    // (node, the ticket, its units, the asker whose units still fill the card)
    let mut waiting: Vec<(String, String, Vec<String>, String)> = Vec::new();
    for view in self.nodes.values() {
      let Some(card) = view.cards.first() else { continue };
      if view.link.state != LinkState::Connected || card.claim.is_some() {
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
          "restore-waiting",
          format!("{}: back once {asker}'s units leave the card", units.join(", ")),
        );
      }
    }
    // every node's loads at once, so a group spread over several nodes has its last card back with its first
    let master = &*self;
    let moved = join_all(due.iter().map(|(node, _, units)| async move {
      let names: Vec<&str> = units.iter().map(String::as_str).collect();
      master.move_units(node, UnitMove::Resident, &names, "the asker is done").await
    }))
    .await;
    for ((node, ticket, units), (ok, status)) in due.into_iter().zip(moved) {
      let key = (node.clone(), ticket);
      if ok {
        self.unrestored.remove(&key);
        self.restore_waits.remove(&key);
        self.say(Some(&node), "units-restored", format!("{} loaded back", units.join(", ")));
      } else if self.unrestored.insert(key) {
        self.say(
          Some(&node),
          "restore-failed",
          format!("{} not loaded back yet ({status}); trying again", units.join(", ")),
        );
      }
    }
  }

  async fn place_tickets(&mut self) {
    let rules = self.rules_now();
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
        // no room made for a ticket that would put its service above its ceiling (docs/placement.md,
        // 「A service's ceiling」)
        let decision = match self.ceiling_holds(&ticket.service, &view.name) {
          Some(reason) => Decision::Hold { reason, rule: None },
          None => turn_gate(
            card.turn.as_ref(),
            &ticket.service,
            rules.plan(std::slice::from_ref(&node), &request),
          ),
        };
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
            self.say(Some(&node), "ticket-held", format!("{unit} held: {reason}"));
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
        moved.push((at, master.move_units(node, UnitMove::Evicted, &names, reason).await));
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
          "units-unloaded",
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
          "unload-failed",
          format!("no room was made to load {unit} ({status}); trying again"),
        );
        self.unloads_failed.insert(key, status);
      }
    }
  }

  /// The whole picture `GET /cluster` answers.
  pub fn state(&self) -> Cluster {
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
          chosen.iter().map(|node| node.cards.first().and_then(pick).map_or(0.0, |n| n.0)).sum()
        };
        Group {
          name: group.name.clone(),
          pool: group.pool,
          node_names: chosen.iter().map(|node| node.name.clone()).collect(),
          total_mib: sum(|card| card.total_mib),
          free_mib: sum(|card| card.free_mib),
        }
      })
      .collect();
    let kind_of = |name: &str| rules.and_then(|rules| rules.spec_of(name)).map(|spec| spec.kind.into());
    let mut tiers: Vec<OccupantTier> = self
      .rules_now()
      .tiers
      .into_iter()
      .map(|(name, tier)| OccupantTier { name, tier: tier.0 })
      .collect();
    tiers.sort_by(|a, b| a.tier.total_cmp(&b.tier).then(a.name.cmp(&b.name)));
    let board = self.board.as_ref().map(|board| board.lock().unwrap());
    let book = self.demands.as_ref().map(|demands| demands.book.lock().unwrap());
    let services = board
      .as_ref()
      .map(|board| board.services.clone())
      .unwrap_or_default()
      .into_iter()
      .map(|mut placed| {
        placed.cards_in_use =
          self.cards_used(&placed.service_name, book.as_deref(), None).into_iter().collect();
        if let Some(declaration) = self.declaration(&placed.service_name) {
          placed.instances = placed
            .node_names
            .iter()
            .filter_map(|node| {
              let instance = self.stands(declaration, node)?;
              Some(ServiceInstance {
                node_name: node.clone(),
                address: instance.address,
                key_file: instance.key_file,
                demands: book.as_ref().map_or(0, |book| book.granted_on(&placed.service_name, node)) as u32,
              })
            })
            .collect();
        }
        placed
      })
      .collect();
    Cluster {
      master: MasterInfo {
        version: self.world.version(),
        started_at: millis(self.started_at),
        rules_file: self.rules_file.display().to_string(),
        rules_loaded_at: self.rules_loaded_at.map(millis),
        rules_error: self.rules_error.clone(),
        read_every_ms: millis(self.read_every_s),
      },
      groups,
      nodes: nodes
        .into_iter()
        .map(|node| ClusterNode {
          name: node.name.clone(),
          role: node.role.clone(),
          desktop: node.desktop,
          version: node.version.clone(),
          rules: node.rules.as_ref().map(|rules| bmo_arbiter_client::cluster::SlaveRules {
            loaded_at: rules.loaded_at,
            error: rules.error.clone(),
          }),
          occupants: node.occupants.clone(),
          cards: node.cards.iter().map(|card| cluster_card(card, &kind_of, &self.details)).collect(),
          link: NodeLink {
            state: node.link.state,
            since_at: node.link.since.map(|at| millis(at.0)),
            read_at: node.link.read_at.map(|at| millis(at.0)),
            last_read_ms: node.link.last_read_ms.map(|ms| ms.0.round() as i64),
            error: node.link.error.clone(),
          },
        })
        .collect(),
      declared: rules
        .map(|rules| rules.units.as_slice())
        .unwrap_or_default()
        .iter()
        .map(|spec| DeclaredUnit {
          unit: spec.unit.clone(),
          service_name: spec.service.clone(),
          kind: spec.kind.into(),
          vram_mib: spec.vram_mib.map(|mib| mib.0),
        })
        .collect(),
      tiers,
      services,
      placement: board.as_ref().and_then(|board| board.request.clone()),
      demands: book.as_ref().map(|book| book.answers(millis(self.world.clock()))).unwrap_or_default(),
      events: self
        .events
        .iter()
        .map(|event| Note {
          at: millis(event.at.0),
          node_name: event.node.clone(),
          code: event.code.to_owned(),
          text: event.text.clone(),
        })
        .collect(),
    }
  }
}

/// What a demand or a ticket waits on where its service is at its ceiling: the cards it uses.
fn ceiling_text(declaration: &Declaration, used: &BTreeSet<String>) -> String {
  let names: Vec<&str> = used.iter().map(String::as_str).collect();
  format!(
    "{} uses {} cards and its ceiling is {}: {}",
    declaration.service,
    used.len(),
    declaration.cards.unwrap_or_default(),
    names.join(", ")
  )
}

/// One card as `GET /cluster` answers it: its units with the kind the rules give each and what its service
/// says it holds.
fn cluster_card(
  card: &CardView,
  kind_of: &dyn Fn(&str) -> Option<bmo_arbiter_client::cluster::UnitKind>,
  details: &BTreeMap<String, UnitDetail>,
) -> ClusterCard {
  let ticket_id = |ticket: &str| ticket.parse::<i64>().unwrap_or_default();
  ClusterCard {
    index: card.index,
    name: card.name.clone(),
    total_mib: card.total_mib.map(|mib| mib.0),
    used_mib: card.used_mib.map(|mib| mib.0),
    free_mib: card.free_mib.map(|mib| mib.0),
    utilization_percent: card.utilization_percent,
    units: card
      .units
      .iter()
      .map(|unit| ClusterUnit {
        name: unit.name.clone(),
        service_name: unit.service.clone(),
        part: unit.part.clone(),
        vram_mib: unit.vram_mib.0,
        state: unit.state,
        ram_mib: unit.ram_mib.0,
        since_at: unit.since_at,
        evictable: unit.evictable,
        reason: unit.reason.clone(),
        load_ms: unit.load_ms,
        unload_ms: unit.unload_ms,
        kind: kind_of(&unit.name),
        detail: details.get(&unit.name).cloned(),
      })
      .collect(),
    turn: card.turn.as_ref().map(|turn| CardTurn {
      service_name: turn.service.clone(),
      ticket_id: ticket_id(&turn.ticket),
      waited_ms: turn.waited_ms,
      in_use: turn.in_use,
    }),
    queue: card
      .queue
      .iter()
      .map(|ticket| QueuedTicket {
        ticket_id: ticket_id(&ticket.ticket),
        service_name: ticket.service.clone(),
        position: ticket.position,
        waited_ms: ticket.waited_ms,
        wait_ms: ticket.wait_ms,
        unit_name: ticket.unit.clone(),
        vram_mib: ticket.vram_mib.map(|mib| mib.0),
      })
      .collect(),
    claim: card.claim.clone(),
    ram: card.ram.clone(),
    stage: CardStage {
      waiting: card.stage.waiting,
      waiting_ms: card.stage.waiting_ms,
    },
    store: card.store.clone(),
    processes: card.processes.clone(),
  }
}

/// The card a slave answered, or why the read gave none: the shell's error where the command failed, else
/// what the slave said where that is no card, as an arbiter of another release answers a route it does not
/// serve.
fn card_of(asked: &RunResult) -> Result<Card, String> {
  if asked.code != 0 {
    let said = asked.stderr.trim();
    let said = if said.is_empty() { format!("exit {}", asked.code) } else { said.to_owned() };
    return Err(tail(&said, 200).to_owned());
  }
  let said = asked.stdout.trim();
  if said.is_empty() {
    return Err("the slave answered nothing".into());
  }
  serde_json::from_str::<Card>(said)
    .map_err(|fault| format!("the slave's answer is no card ({fault}): {}", tail(said, 200)))
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
