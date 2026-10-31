//! Placing the services (docs/placement.md, 「Placing the services」): which service stands on
//! which node, in what order, kept by the master alone; each node's role and occupants derived from it;
//! each service's own placement kept in step through the adapter it declares. A request is journaled
//! step by step and undone in reverse where it fails, or where the master stopped before it ended.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use bmo_arbiter_client::cluster::{PlacedService, PlacementUnderWay};
use bmo_arbiter_client::placements::PlacementKind;
use serde::{Deserialize, Serialize};

use crate::reach::BoxFuture;
use crate::rules::{DEFAULT_TIER, Num, RULES_FILE, read_rules, role_services};

/// Where a declaration stands among a root's files: `<service>.card.toml`.
pub const DECLARATION_SUFFIX: &str = ".card.toml";

/// A tool's manifest, in the tool's folder under a root's `tools/`.
pub const MANIFEST: &str = "tool.toml";

/// The folder of a tool's folder its placed manifest stands in.
pub const PLACED: &str = ".placed";

/// The folder of a root's `tools/` that holds shared helpers, each a tool folder of its own.
const SHARED: &str = "_shared";

/// The setting naming the declaration roots (docs/api.md, 「Declarations」).
pub const DECLARATIONS: &str = "CARD_DECLARATIONS";

/// The declaration roots `CARD_DECLARATIONS` names, separated by `:`; none where it names none.
pub fn declaration_roots_from_env() -> Vec<PathBuf> {
  std::env::var_os(DECLARATIONS)
    .map(|named| std::env::split_paths(&named).filter(|root| !root.as_os_str().is_empty()).collect())
    .unwrap_or_default()
}

/// How a service stands across nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Spread {
  /// On a single node, moved whole.
  One,
  /// On several nodes.
  Many,
}

/// How a service yields its card (docs/node-arbiter.md, 「Priority and yielding」).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum YieldBy {
  /// By its turn: giving way, its units unloaded, or letting go at its request boundary.
  #[default]
  Turn,
  /// By its process: its adapter's `stop`, and its `start` once its turn comes again.
  Process,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeclarationFile {
  service: String,
  card_mib: u64,
  priority: i64,
  tier: Option<f64>,
  yield_by: Option<YieldBy>,
  spread: Spread,
  alone: bool,
  adapter: Option<Vec<String>>,
  cards: Option<u32>,
  /// A `[turn]` table, accepted and meaning nothing: no number of seconds decides how a service takes turns
  /// with one of its rank (docs/api.md, 「Declarations」).
  #[serde(rename = "turn")]
  _turn: Option<serde::de::IgnoredAny>,
}

/// A service as it declares itself, and the directory its adapter runs from.
#[derive(Debug, Clone, PartialEq)]
pub struct Declaration {
  pub service: String,
  pub card_mib: u64,
  pub priority: i64,
  /// Its tier where the rules name none for it (docs/placement.md, 「Tiers」); none for the default.
  pub tier: Option<Num>,
  pub yield_by: YieldBy,
  pub spread: Spread,
  pub alone: bool,
  pub adapter: Option<Vec<String>>,
  /// The most cards it uses across the cluster at once (docs/placement.md, 「A service's ceiling」); none for
  /// no ceiling.
  pub cards: Option<u32>,
  pub dir: PathBuf,
}

/// One declaration out of its file's text, or why it cannot be one.
pub fn parse_declaration(text: &str, path: &Path) -> Result<Declaration, String> {
  let read: DeclarationFile =
    toml::from_str(text).map_err(|fault| format!("{}: {fault}", path.display()))?;
  let named = path
    .file_name()
    .and_then(|name| name.to_str())
    .and_then(|name| name.strip_suffix(DECLARATION_SUFFIX));
  if named != Some(read.service.as_str()) {
    return Err(format!(
      "{}: the file must be named after its service, {}{DECLARATION_SUFFIX}",
      path.display(),
      read.service
    ));
  }
  held(read, path, path.parent().map(Path::to_owned).unwrap_or_default())
}

/// A tool's manifest as far as the master reads it: its `[card]` table alone, every other table being the
/// tool host's.
#[derive(Debug, Deserialize)]
struct ToolManifest {
  card: Option<DeclarationFile>,
}

/// The declaration in the `[card]` table of the manifest placed for the tool folder `tool`, its adapter run
/// from that folder; none where the tool declares no card.
pub fn tool_declaration(tool: &Path) -> Result<Option<Declaration>, String> {
  let path = tool.join(PLACED).join(MANIFEST);
  let text = std::fs::read_to_string(&path).map_err(|fault| format!("{}: {fault}", path.display()))?;
  let manifest: ToolManifest =
    toml::from_str(&text).map_err(|fault| format!("{} [card]: {}", path.display(), fault.message()))?;
  match manifest.card {
    None => Ok(None),
    Some(read) => held(read, &path, tool.to_owned()).map(Some),
  }
}

/// Every tool folder a root's `tools/` holds, by its folder under `tools/`: each folder whose name starts
/// with neither `_` nor `.`, then each folder of `_shared/`, each in order of name.
fn tool_folders(tools: &Path) -> Vec<String> {
  let names = |dir: &Path| -> Vec<String> {
    let mut found: Vec<String> = std::fs::read_dir(dir)
      .into_iter()
      .flatten()
      .flatten()
      .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
      .filter_map(|entry| entry.file_name().into_string().ok())
      .collect();
    found.sort();
    found
  };
  let mut folders: Vec<String> =
    names(tools).into_iter().filter(|name| !name.starts_with(['_', '.'])).collect();
  folders.extend(names(&tools.join(SHARED)).into_iter().map(|name| format!("{SHARED}/{name}")));
  folders
}

// A declaration as it was read, held to the rules wherever it stood.
fn held(read: DeclarationFile, path: &Path, dir: PathBuf) -> Result<Declaration, String> {
  if !occupant_word(&read.service) {
    return Err(format!("{}: service must be lower-case letters, digits and hyphens", path.display()));
  }
  if read.adapter.as_ref().is_some_and(|argv| argv.is_empty()) {
    return Err(format!("{}: adapter must be a command that is not empty", path.display()));
  }
  if read.tier.is_some_and(|tier| !tier.is_finite() || tier < 0.0) {
    return Err(format!("{}: tier must be a number of zero or more", path.display()));
  }
  if read.cards == Some(0) {
    return Err(format!("{}: cards must be one or more", path.display()));
  }
  if read.yield_by == Some(YieldBy::Process) && read.adapter.is_none() {
    return Err(format!(
      "{}: yield_by = \"process\" needs an adapter to stop and start it",
      path.display()
    ));
  }
  Ok(Declaration {
    service: read.service,
    card_mib: read.card_mib,
    priority: read.priority,
    tier: read.tier.map(Num),
    yield_by: read.yield_by.unwrap_or_default(),
    spread: read.spread,
    alone: read.alone,
    adapter: read.adapter,
    cards: read.cards,
    dir,
  })
}

/// Every occupant's tier (docs/placement.md, 「Tiers」): the ones the rules' `occupants` give, and for each
/// declared service the rules name none for, its declaration's, else the default.
pub fn tiers_of(given: &BTreeMap<String, Num>, declarations: &[Declaration]) -> BTreeMap<String, Num> {
  let mut tiers = given.clone();
  for one in declarations {
    tiers.entry(one.service.clone()).or_insert(one.tier.unwrap_or(Num(DEFAULT_TIER)));
  }
  tiers
}

fn occupant_word(name: &str) -> bool {
  let mut chars = name.chars();
  chars.next().is_some_and(|first| first.is_ascii_lowercase() || first.is_ascii_digit())
    && chars.all(|one| one.is_ascii_lowercase() || one.is_ascii_digit() || one == '-')
}

/// Whether a root stands in a git work tree, so its declarations are the ones its index holds.
fn in_work_tree(root: &Path) -> Result<bool, String> {
  let asked = std::process::Command::new("git")
    .arg("-C")
    .arg(root)
    .args(["rev-parse", "--is-inside-work-tree"])
    .stdin(std::process::Stdio::null())
    .output()
    .map_err(|fault| format!("git rev-parse: {fault}"))?;
  Ok(asked.status.success() && String::from_utf8_lossy(&asked.stdout).trim() == "true")
}

/// The declaration files the root's git index holds, a checked-out submodule's among them.
fn indexed_declarations(root: &Path) -> Result<Vec<PathBuf>, String> {
  let listed = std::process::Command::new("git")
    .arg("-C")
    .arg(root)
    .args(["ls-files", "-z", "--recurse-submodules", "--", &format!("*{DECLARATION_SUFFIX}")])
    .stdin(std::process::Stdio::null())
    .output()
    .map_err(|fault| format!("git ls-files: {fault}"))?;
  if !listed.status.success() {
    return Err(format!(
      "git ls-files in {}: {}",
      root.display(),
      String::from_utf8_lossy(&listed.stderr).trim()
    ));
  }
  Ok(
    String::from_utf8_lossy(&listed.stdout)
      .split('\0')
      .filter(|name| !name.is_empty())
      .map(|name| root.join(name))
      .collect(),
  )
}

/// The declaration files found under a root that is no git tree, in order of path: hidden folders left
/// out, and whatever the root's own `.gitignore` and `.ignore` files exclude.
fn walked_declarations(root: &Path) -> Result<Vec<PathBuf>, String> {
  let mut found: Vec<PathBuf> = Vec::new();
  let walk = ignore::WalkBuilder::new(root)
    .require_git(false)
    .parents(false)
    .git_global(false)
    .git_exclude(false)
    .build();
  for entry in walk {
    let entry = entry.map_err(|fault| format!("{}: {fault}", root.display()))?;
    let named = entry.file_name().to_str().is_some_and(|name| name.ends_with(DECLARATION_SUFFIX));
    if named && entry.file_type().is_some_and(|kind| kind.is_file()) {
      found.push(entry.into_path());
    }
  }
  found.sort();
  Ok(found)
}

/// Every declaration of one root: each one the root's git index holds, a checked-out submodule's among
/// them, or each found under a root that is no git tree, found by its name; and each in the manifest
/// placed for a tool folder of its `tools/`.
fn root_declarations(root: &Path) -> Result<Vec<Declaration>, String> {
  let files = if in_work_tree(root)? {
    indexed_declarations(root)?
  } else {
    walked_declarations(root)?
  };
  let mut read: Vec<Declaration> = Vec::new();
  for path in files {
    let text = std::fs::read_to_string(&path).map_err(|fault| format!("{}: {fault}", path.display()))?;
    read.push(parse_declaration(&text, &path)?);
  }
  let tools = root.join("tools");
  for folder in tool_folders(&tools) {
    let tool = tools.join(&folder);
    if tool.join(PLACED).join(MANIFEST).is_file() {
      read.extend(tool_declaration(&tool)?);
    }
  }
  Ok(read)
}

/// Every declaration of the roots, in order of service (docs/api.md, 「Declarations」); no list of services
/// is kept, and two declarations of one service across all roots are refused.
pub fn declarations(roots: &[PathBuf]) -> Result<Vec<Declaration>, String> {
  let mut read: Vec<Declaration> = Vec::new();
  for root in roots {
    read.extend(root_declarations(root)?);
  }
  let mut found: Vec<Declaration> = Vec::new();
  for declaration in read {
    if found.iter().any(|one| one.service == declaration.service) {
      return Err(format!("two files declare {}", declaration.service));
    }
    found.push(declaration);
  }
  found.sort_by(|a, b| a.service.cmp(&b.service));
  Ok(found)
}

/// The master's own state: each service's nodes in order, and the services whose first adoption found
/// their placement and the roles disagreeing, with why.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Placements {
  pub services: BTreeMap<String, Vec<String>>,
  #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
  pub unsettled: BTreeMap<String, String>,
}

impl Placements {
  pub fn nodes_of(&self, service: &str) -> &[String] {
    self.services.get(service).map(Vec::as_slice).unwrap_or_default()
  }

  /// The services standing on a node.
  pub fn on(&self, node: &str) -> Vec<&str> {
    self
      .services
      .iter()
      .filter(|(_, nodes)| nodes.iter().any(|one| one == node))
      .map(|(service, _)| service.as_str())
      .collect()
  }
}

fn priority_of(service: &str, declarations: &[Declaration]) -> i64 {
  declarations.iter().find(|one| one.service == service).map_or(0, |one| one.priority)
}

fn by_priority<'a>(services: &[&'a str], declarations: &[Declaration]) -> Vec<&'a str> {
  let priority = |service: &str| priority_of(service, declarations);
  let mut ordered = services.to_vec();
  ordered.sort_by(|a, b| priority(b).cmp(&priority(a)).then(a.cmp(b)));
  ordered
}

fn stands_alone(service: &str, declarations: &[Declaration]) -> bool {
  declarations.iter().any(|one| one.service == service && one.alone)
}

/// A node's role as the placement gives it: the services standing alone there, highest first, then
/// those standing beneath them without standing alone; none where nothing stands alone.
pub fn role_of(node: &str, placements: &Placements, declarations: &[Declaration]) -> Option<String> {
  let (alone, beneath): (Vec<&str>, Vec<&str>) =
    placements.on(node).into_iter().partition(|service| stands_alone(service, declarations));
  (!alone.is_empty()).then(|| {
    by_priority(&alone, declarations)
      .into_iter()
      .chain(by_priority(&beneath, declarations))
      .collect::<Vec<_>>()
      .join("+")
  })
}

/// A node's occupant order as the placement gives it: the desktop where the machine is one, the services
/// standing there by tier, lower first, those of one tier one word joined by `=` since they stand at one
/// rank, and `*` on a node reserved for no service.
pub fn occupants_of(
  node: &str,
  placements: &Placements,
  declarations: &[Declaration],
  tiers: &BTreeMap<String, Num>,
  desktop: bool,
) -> String {
  let tier = |service: &str| tiers.get(service).map_or(DEFAULT_TIER, |tier| tier.0);
  let mut words: Vec<String> = Vec::new();
  if desktop {
    words.push("desktop".into());
  }
  let mut standing = placements.on(node);
  standing.sort_by(|a, b| tier(a).total_cmp(&tier(b)).then(a.cmp(b)));
  let mut rank: Vec<&str> = Vec::new();
  for service in standing {
    if rank.last().is_some_and(|last| tier(last) != tier(service)) {
      words.push(rank.join("="));
      rank.clear();
    }
    rank.push(service);
  }
  if !rank.is_empty() {
    words.push(rank.join("="));
  }
  if role_of(node, placements, declarations).is_none() {
    words.push("*".into());
  }
  words.join(" ")
}

/// A node as the node list gives it: its name and its role.
#[derive(Debug, Clone, PartialEq)]
pub struct NodeRow {
  pub name: String,
  pub role: Option<String>,
}

/// What an adapter answers, the same shape for every verb.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AdapterAnswer {
  pub ok: bool,
  pub node: Option<String>,
  pub state: Option<String>,
  #[serde(default)]
  pub detail: String,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub nodes: Option<Vec<String>>,
  /// For `health` answering `running`: where the instance answers its callers.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub address: Option<String>,
  /// For `health` answering `running`: the file its key is read from on the master's host.
  #[serde(default, rename = "keyFile", skip_serializing_if = "Option::is_none")]
  pub key_file: Option<String>,
}

impl AdapterAnswer {
  pub fn refused(detail: impl Into<String>) -> Self {
    AdapterAnswer {
      ok: false,
      detail: detail.into(),
      ..AdapterAnswer::default()
    }
  }
}

/// How what a request does is said on its way.
pub type Say = Arc<dyn Fn(String) + Send + Sync>;

/// What the master needs of the world to place a service, replaced in tests.
pub trait Hands: Send + Sync {
  /// The services declared in the tree the hands install from, read as it stands now.
  fn declarations(&self) -> Result<Vec<Declaration>, String>;
  fn nodes(&self) -> BoxFuture<'_, Result<Vec<NodeRow>, String>>;
  /// Whether the node's machine is a person's desktop, as its arbiter says.
  fn desktop<'a>(&'a self, node: &'a str) -> BoxFuture<'a, bool>;
  fn set_role<'a>(&'a self, node: &'a str, role: Option<&'a str>) -> BoxFuture<'a, Result<(), String>>;
  /// The node's occupants file as it stands, or none where it has none.
  fn occupants<'a>(&'a self, node: &'a str) -> BoxFuture<'a, Result<Option<String>, String>>;
  /// The node's occupants written whole, or the file taken away.
  fn write_occupants<'a>(
    &'a self,
    node: &'a str,
    order: Option<&'a str>,
  ) -> BoxFuture<'a, Result<(), String>>;
  /// The node's arbiter stood or brought up to date, and its part of the rules written.
  fn arbiter<'a>(&'a self, node: &'a str, say: &'a Say) -> BoxFuture<'a, Result<(), String>>;
  fn adapter<'a>(
    &'a self,
    declaration: &'a Declaration,
    verb: &'a str,
    node: Option<&'a str>,
    placed: &'a [String],
    say: &'a Say,
  ) -> BoxFuture<'a, AdapterAnswer>;
}

/// The steps of a request, each journaled before it runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Step {
  Arbiter,
  Roles,
  Occupants,
  Placement,
  Adapter,
  Health,
  Narrowed,
}

impl Step {
  fn word(self) -> &'static str {
    match self {
      Step::Arbiter => "arbiter",
      Step::Roles => "roles",
      Step::Occupants => "occupants",
      Step::Placement => "placement",
      Step::Adapter => "adapter",
      Step::Health => "health",
      Step::Narrowed => "narrowed",
    }
  }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Verb {
  Place,
  Remove,
}

impl Verb {
  pub fn word(self) -> &'static str {
    match self {
      Verb::Place => "place",
      Verb::Remove => "remove",
    }
  }
}

/// A request under way, with what stood before it, so it can be undone.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Journal {
  pub verb: Verb,
  pub service: String,
  pub node: String,
  pub before: Placements,
  pub roles: BTreeMap<String, Option<String>>,
  pub occupants: BTreeMap<String, Option<String>>,
  pub done: Vec<Step>,
}

impl From<Spread> for bmo_arbiter_client::cluster::Spread {
  fn from(spread: Spread) -> Self {
    match spread {
      Spread::One => Self::One,
      Spread::Many => Self::Many,
    }
  }
}

impl From<YieldBy> for bmo_arbiter_client::cluster::YieldBy {
  fn from(yield_by: YieldBy) -> Self {
    match yield_by {
      YieldBy::Turn => Self::Turn,
      YieldBy::Process => Self::Process,
    }
  }
}

impl From<Verb> for PlacementKind {
  fn from(verb: Verb) -> Self {
    match verb {
      Verb::Place => Self::Place,
      Verb::Remove => Self::Remove,
    }
  }
}

impl From<PlacementKind> for Verb {
  fn from(kind: PlacementKind) -> Self {
    match kind {
      PlacementKind::Place => Self::Place,
      PlacementKind::Remove => Self::Remove,
    }
  }
}

/// One thing the placement has to say among the master's events: the node it is of, the code a console
/// reads it by, and its English text (docs/api.md, 「Messages and codes」).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Said {
  pub node: Option<String>,
  pub code: &'static str,
  pub text: String,
}

/// A service a placement stood on a node: its service and node, and its `health` answering `running`.
#[derive(Debug, Clone, PartialEq)]
pub struct Stood {
  pub service: String,
  pub node: String,
  pub health: AdapterAnswer,
}

/// What the placement shows the master's picture, what it has to say among its events, and the services
/// it stood since the master last looked, which stand there whatever the master stopped before.
#[derive(Debug, Default)]
pub struct Board {
  pub services: Vec<PlacedService>,
  pub request: Option<PlacementUnderWay>,
  pub said: Vec<Said>,
  pub stood: Vec<Stood>,
}

/// The placement, its declarations read again through its hands as each request is taken.
pub struct Placer {
  hands: Arc<dyn Hands>,
  declarations: Mutex<Arc<Vec<Declaration>>>,
  rules_file: PathBuf,
  state_file: PathBuf,
  journal_file: PathBuf,
  board: Arc<Mutex<Board>>,
}

/// Why a request stopped, at which step, and whether what it did was undone.
#[derive(Debug, Clone, PartialEq)]
pub struct Stopped {
  pub step: Option<Step>,
  pub why: String,
}

impl std::fmt::Display for Stopped {
  fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
    match self.step {
      Some(step) => write!(formatter, "stopped at {}: {}", step.word(), self.why),
      None => write!(formatter, "{}", self.why),
    }
  }
}

fn refuse(why: String) -> Stopped {
  Stopped { step: None, why }
}

impl Placer {
  pub fn new(hands: Arc<dyn Hands>, placements_dir: &Path, board: Arc<Mutex<Board>>) -> Self {
    Placer {
      hands,
      declarations: Mutex::default(),
      rules_file: placements_dir.join(RULES_FILE),
      state_file: placements_dir.join("services.json"),
      journal_file: placements_dir.join("services-request.json"),
      board,
    }
  }

  pub fn board(&self) -> Arc<Mutex<Board>> {
    self.board.clone()
  }

  /// The declarations as the hands read them now, kept for the request taking them.
  fn read_declarations(&self) -> Result<(), String> {
    let read = self
      .hands
      .declarations()
      .map_err(|why| format!("could not read the service declarations: {why}"))?;
    *self.declarations.lock().unwrap() = Arc::new(read);
    Ok(())
  }

  /// The declarations the request under way took.
  fn declared(&self) -> Arc<Vec<Declaration>> {
    self.declarations.lock().unwrap().clone()
  }

  fn declaration(&self, service: &str) -> Option<Declaration> {
    self.declared().iter().find(|one| one.service == service).cloned()
  }

  /// Every occupant's tier, from the rules as the file stands now and the declarations the request took; a
  /// file that does not read gives none of its own.
  fn tiers(&self) -> BTreeMap<String, Num> {
    let given = read_rules(&self.rules_file).ok().flatten().map(|rules| rules.tiers).unwrap_or_default();
    tiers_of(&given, &self.declared())
  }

  /// The master's state as it stands; none before the first adoption.
  pub fn state(&self) -> Option<Placements> {
    let text = std::fs::read_to_string(&self.state_file).ok()?;
    serde_json::from_str(&text).ok()
  }

  fn save_state(&self, placements: &Placements) -> Result<(), String> {
    write_whole(
      &self.state_file,
      &serde_json::to_string_pretty(placements).expect("placements serialize"),
    )?;
    self.show(placements);
    Ok(())
  }

  fn journal(&self) -> Option<Journal> {
    serde_json::from_str(&std::fs::read_to_string(&self.journal_file).ok()?).ok()
  }

  fn save_journal(&self, journal: &Journal) -> Result<(), String> {
    write_whole(&self.journal_file, &serde_json::to_string_pretty(journal).expect("journal serializes"))
  }

  fn clear_journal(&self) {
    let _ = std::fs::remove_file(&self.journal_file);
  }

  /// The services as the state API shows them.
  fn show(&self, placements: &Placements) {
    let services = self
      .declared()
      .iter()
      .map(|one| PlacedService {
        service_name: one.service.clone(),
        card_mib: one.card_mib as f64,
        priority: one.priority as f64,
        tier: one.tier.map(|tier| tier.0),
        yield_by: one.yield_by.into(),
        spread: one.spread.into(),
        alone: one.alone,
        cards: one.cards,
        cards_in_use: Vec::new(),
        node_names: placements.nodes_of(&one.service).to_vec(),
        instances: Vec::new(),
        unsettled: placements.unsettled.get(&one.service).cloned(),
      })
      .collect();
    self.board.lock().unwrap().services = services;
  }

  fn tell(&self, node: Option<&str>, code: &'static str, text: String) {
    self.board.lock().unwrap().said.push(Said {
      node: node.map(str::to_owned),
      code,
      text,
    });
  }

  fn at_step(&self, journal: &Journal, step: Option<Step>) {
    self.board.lock().unwrap().request = Some(PlacementUnderWay {
      kind: journal.verb.into(),
      service_name: journal.service.clone(),
      node_name: journal.node.clone(),
      step: step.map(|step| step.word().to_owned()),
    });
  }

  /// The first start's state, out of what stands: each service's nodes from its adapter, else from the
  /// roles that name it; a service whose adapter and roles disagree is unsettled.
  pub async fn adopt(&self, say: &Say) -> Result<Placements, String> {
    self.read_declarations()?;
    if let Some(standing) = self.state() {
      self.show(&standing);
      return Ok(standing);
    }
    let rows = self.hands.nodes().await?;
    let mut placements = Placements::default();
    for declaration in self.declared().iter() {
      let by_role: Vec<String> = rows
        .iter()
        .filter(|row| role_services(row.role.as_deref()).contains(&declaration.service.as_str()))
        .map(|row| row.name.clone())
        .collect();
      let nodes = match &declaration.adapter {
        None => by_role.clone(),
        Some(_) => {
          let answer = self.hands.adapter(declaration, "placed", None, &[], say).await;
          match answer.nodes {
            Some(nodes) if answer.ok => nodes,
            _ => {
              placements.unsettled.insert(
                declaration.service.clone(),
                format!("the adapter did not say where the service stands: {}", answer.detail),
              );
              by_role.clone()
            }
          }
        }
      };
      if declaration.alone && !placements.unsettled.contains_key(&declaration.service) {
        let placed: BTreeSet<&String> = nodes.iter().collect();
        let roled: BTreeSet<&String> = by_role.iter().collect();
        if placed != roled {
          placements.unsettled.insert(
            declaration.service.clone(),
            format!(
              "the adapter places it on {}, and the node service reserves {} for it",
              words(&nodes),
              words(&by_role)
            ),
          );
        }
      }
      placements.services.insert(declaration.service.clone(), nodes);
    }
    for (service, why) in &placements.unsettled {
      self.tell(
        None,
        "placement-unsettled",
        format!("{service}'s placement is unsettled: {why}; a place settles it"),
      );
    }
    self.save_state(&placements)?;
    Ok(placements)
  }

  /// What differs between the master's state and what stands, each said once and none of it followed.
  pub async fn drift(&self, say: &Say) -> Result<Vec<String>, String> {
    let Some(placements) = self.state() else { return Ok(Vec::new()) };
    self.read_declarations()?;
    let declarations = self.declared();
    let mut found = Vec::new();
    for declaration in declarations.iter().filter(|one| one.adapter.is_some()) {
      let answer = self.hands.adapter(declaration, "placed", None, &[], say).await;
      let ours = placements.nodes_of(&declaration.service);
      match answer.nodes {
        Some(theirs) if answer.ok && theirs != ours => found.push(format!(
          "{}: the adapter places it on {}, the master on {}",
          declaration.service,
          words(&theirs),
          words(ours)
        )),
        Some(_) if answer.ok => {}
        _ => found.push(format!(
          "{}: the adapter did not say where the service stands: {}",
          declaration.service, answer.detail
        )),
      }
    }
    for row in self.hands.nodes().await? {
      let role = role_of(&row.name, &placements, &declarations);
      if role != row.role {
        found.push(format!(
          "{}: the node service's role is {}, the placement gives {}",
          row.name,
          row.role.as_deref().unwrap_or("none"),
          role.as_deref().unwrap_or("none")
        ));
      }
      if placements.on(&row.name).is_empty() {
        continue;
      }
      let derived = occupants_of(
        &row.name,
        &placements,
        &declarations,
        &self.tiers(),
        self.hands.desktop(&row.name).await,
      );
      match self.hands.occupants(&row.name).await {
        Ok(standing) if standing.as_deref().map(str::trim) == Some(derived.as_str()) => {}
        Ok(standing) => found.push(format!(
          "{}: the occupants are {}, the placement gives {}",
          row.name,
          standing.as_deref().map_or("(no file)", str::trim),
          derived
        )),
        Err(fault) => found.push(format!("{}: the occupants were not read: {fault}", row.name)),
      }
    }
    for line in &found {
      self.tell(None, "placement-drift", format!("drift: {line}"));
    }
    Ok(found)
  }

  /// A request a master stopped in the middle of, undone from its journal.
  pub async fn recover(&self, say: &Say) {
    let Some(journal) = self.journal() else { return };
    if let Err(why) = self.read_declarations() {
      say(why);
    }
    let text = format!(
      "undoing a request left unfinished: {} {} {}",
      journal.verb.word(),
      journal.service,
      journal.node
    );
    say(text.clone());
    self.tell(Some(&journal.node), "request-undone", text);
    self.undo(&journal, say).await;
    self.clear_journal();
  }

  async fn undo(&self, journal: &Journal, say: &Say) {
    if journal.done.contains(&Step::Narrowed) {
      say("the request had narrowed its placement, so nothing is undone".into());
      return;
    }
    let Some(declaration) = self.declaration(&journal.service) else { return };
    for step in journal.done.iter().rev() {
      match step {
        Step::Adapter => {
          let placed = journal.before.nodes_of(&journal.service).to_vec();
          let verb = if journal.verb == Verb::Place { "uninstall" } else { "install" };
          if declaration.adapter.is_some() {
            let answer = self.hands.adapter(&declaration, verb, Some(&journal.node), &placed, say).await;
            if !answer.ok {
              say(format!("{}: the {verb} that undoes it failed: {}", journal.node, answer.detail));
            }
          }
        }
        Step::Placement => {
          if let Err(fault) = self.save_state(&journal.before) {
            say(format!("the placement was not put back: {fault}"));
          }
        }
        Step::Roles => {
          for (node, role) in &journal.roles {
            if let Err(fault) = self.hands.set_role(node, role.as_deref()).await {
              say(format!("{node}: the role was not put back: {fault}"));
            }
          }
        }
        Step::Occupants => {
          for (node, order) in &journal.occupants {
            if let Err(fault) = self.hands.write_occupants(node, order.as_deref()).await {
              say(format!("{node}: the occupants were not put back: {fault}"));
            }
          }
        }
        Step::Arbiter | Step::Health | Step::Narrowed => {}
      }
    }
    say("the steps taken were undone in reverse".into());
  }

  /// One step journaled before it runs.
  fn begin(&self, journal: &mut Journal, step: Step, say: &Say) -> Result<(), Stopped> {
    journal.done.push(step);
    self.save_journal(journal).map_err(|why| Stopped {
      step: Some(step),
      why,
    })?;
    self.at_step(journal, Some(step));
    say(format!("— {}", step.word()));
    Ok(())
  }

  async fn finish_with_undo(&self, journal: &Journal, stopped: Stopped, say: &Say) -> Stopped {
    say(format!("{stopped}; undoing"));
    self.undo(journal, say).await;
    self.clear_journal();
    stopped
  }

  /// A request run whole, or why it stopped; what it did undone where it stopped short.
  pub async fn run(&self, verb: Verb, service: &str, node: &str, say: &Say) -> Result<(), Stopped> {
    let outcome = match self.read_declarations() {
      Err(why) => Err(refuse(why)),
      Ok(()) if verb == Verb::Place => self.place(service, node, say).await,
      Ok(()) => self.remove(service, node, say).await,
    };
    self.board.lock().unwrap().request = None;
    match &outcome {
      Ok(()) => self.tell(Some(node), "request-done", format!("{} {service}: done", verb.word())),
      Err(stopped) => {
        self.tell(Some(node), "request-failed", format!("{} {service}: {stopped}", verb.word()))
      }
    }
    outcome
  }

  async fn rows(&self) -> Result<Vec<NodeRow>, Stopped> {
    self
      .hands
      .nodes()
      .await
      .map_err(|why| refuse(format!("the node service was not read: {why}")))
  }

  async fn place(&self, service: &str, node: &str, say: &Say) -> Result<(), Stopped> {
    let declarations = self.declared();
    let declaration = self.declaration(service).ok_or_else(|| {
      refuse(format!("no {service}{DECLARATION_SUFFIX} or [card] table declares {service}"))
    })?;
    let rows = self.rows().await?;
    let Some(row) = rows.iter().find(|row| row.name == node) else {
      return Err(refuse(format!("the node service holds no {node}")));
    };
    let before = self.state().unwrap_or_default();
    // Services standing alone at one priority share a node as equals, its reservation naming them all.
    let shares = |other: &str| {
      declaration.alone
        && self
          .declaration(other)
          .is_some_and(|one| one.alone && one.priority == declaration.priority)
    };
    // A service that does not stand alone may stand beneath every service standing alone on a node: it
    // gives way to each of them on the card, so the node stays theirs first.
    let beneath = |other: &str| {
      !declaration.alone
        && self
          .declaration(other)
          .is_some_and(|one| !one.alone || one.priority > declaration.priority)
    };
    let below = |other: &str| {
      self
        .declaration(other)
        .is_some_and(|one| !one.alone && one.priority < declaration.priority)
    };
    let roles = role_services(row.role.as_deref());
    // A service standing alone joins a reservation whose services standing alone stand at its priority, the
    // ones standing beneath them standing below it too.
    let joins = roles.iter().all(|one| shares(one) || (declaration.alone && below(one)));
    let reserved_above = roles.iter().any(|one| self.declaration(one).is_some_and(|one| one.alone))
      && roles.iter().all(|one| beneath(one));
    if !roles.is_empty() && !roles.contains(&service) && !joins && !reserved_above {
      return Err(refuse(format!(
        "{node} is reserved for {} in the node service, so {service} cannot stand there",
        row.role.as_deref().unwrap_or("")
      )));
    }
    let others: Vec<&str> = before.on(node).into_iter().filter(|one| *one != service).collect();
    if declaration.alone && !others.iter().all(|one| shares(one) || below(one)) {
      return Err(refuse(format!(
        "{} stands on {node}, so {service} cannot stand there alone",
        others.join(", ")
      )));
    }

    let old: Vec<String> = before.nodes_of(service).iter().filter(|one| *one != node).cloned().collect();
    let mut after = before.clone();
    after.unsettled.remove(service);
    let mut list = old.clone();
    list.push(node.to_owned());
    after.services.insert(service.to_owned(), list.clone());
    let touched: Vec<String> = std::iter::once(node.to_owned())
      .chain(if declaration.spread == Spread::One { old.clone() } else { Vec::new() })
      .collect();
    let mut journal = Journal {
      verb: Verb::Place,
      service: service.to_owned(),
      node: node.to_owned(),
      before: before.clone(),
      roles: BTreeMap::new(),
      occupants: BTreeMap::new(),
      done: Vec::new(),
    };
    for one in &touched {
      journal
        .roles
        .insert(one.clone(), rows.iter().find(|row| row.name == *one).and_then(|row| row.role.clone()));
      let standing = self
        .hands
        .occupants(one)
        .await
        .map_err(|why| refuse(format!("{one}: the occupants were not read: {why}")))?;
      journal.occupants.insert(one.clone(), standing);
    }
    self.save_journal(&journal).map_err(refuse)?;

    match self.place_steps(&declaration, &mut journal, &after, &list, say).await {
      Ok(()) => {}
      Err(stopped) => return Err(self.finish_with_undo(&journal, stopped, say).await),
    }

    if declaration.spread == Spread::One && !old.is_empty() {
      self.begin(&mut journal, Step::Narrowed, say)?;
      let mut narrowed = after.clone();
      narrowed.services.insert(service.to_owned(), vec![node.to_owned()]);
      if let Err(why) = self.save_state(&narrowed) {
        say(format!("the placement was not narrowed to {node}: {why}"));
      }
      for gone in &old {
        if declaration.adapter.is_some() {
          let answer =
            self.hands.adapter(&declaration, "uninstall", Some(gone), &[node.to_owned()], say).await;
          if !answer.ok {
            say(format!("{gone}: {service} was not uninstalled: {}", answer.detail));
            self.tell(
              Some(gone),
              "service-not-uninstalled",
              format!("{service} was not uninstalled: {}", answer.detail),
            );
          }
        }
        let role = role_of(gone, &narrowed, &declarations);
        if journal.roles.get(gone).cloned().flatten() != role
          && let Err(why) = self.hands.set_role(gone, role.as_deref()).await
        {
          say(format!("{gone}: the role was not changed: {why}"));
        }
        let order =
          occupants_of(gone, &narrowed, &declarations, &self.tiers(), self.hands.desktop(gone).await);
        if let Err(why) = self.hands.write_occupants(gone, Some(&order)).await {
          say(format!("{gone}: the occupants were not written: {why}"));
        }
      }
    }
    self.clear_journal();
    say(format!(
      "{service} stands on {}",
      words(self.state().unwrap_or_default().nodes_of(service))
    ));
    Ok(())
  }

  async fn place_steps(
    &self,
    declaration: &Declaration,
    journal: &mut Journal,
    after: &Placements,
    list: &[String],
    say: &Say,
  ) -> Result<(), Stopped> {
    let node = journal.node.clone();
    let service = journal.service.clone();
    let declarations = self.declared();

    self.begin(journal, Step::Arbiter, say)?;
    self.hands.arbiter(&node, say).await.map_err(|why| Stopped {
      step: Some(Step::Arbiter),
      why,
    })?;

    let role = role_of(&node, after, &declarations);
    if journal.roles.get(&node).cloned().flatten() != role {
      self.begin(journal, Step::Roles, say)?;
      self.hands.set_role(&node, role.as_deref()).await.map_err(|why| Stopped {
        step: Some(Step::Roles),
        why,
      })?;
    }

    self.begin(journal, Step::Occupants, say)?;
    let order = occupants_of(&node, after, &declarations, &self.tiers(), self.hands.desktop(&node).await);
    self.hands.write_occupants(&node, Some(&order)).await.map_err(|why| Stopped {
      step: Some(Step::Occupants),
      why,
    })?;
    say(format!("{node}: occupants {order}"));

    self.begin(journal, Step::Placement, say)?;
    self.save_state(after).map_err(|why| Stopped {
      step: Some(Step::Placement),
      why,
    })?;

    if declaration.adapter.is_some() {
      self.begin(journal, Step::Adapter, say)?;
      let answer = self.hands.adapter(declaration, "install", Some(&node), list, say).await;
      if !answer.ok {
        return Err(Stopped {
          step: Some(Step::Adapter),
          why: answer.detail,
        });
      }
      self.begin(journal, Step::Health, say)?;
      let answer = self.hands.adapter(declaration, "health", Some(&node), list, say).await;
      if !answer.ok || answer.state.as_deref() != Some("running") {
        return Err(Stopped {
          step: Some(Step::Health),
          why: format!(
            "{service} is not running on {node} ({}): {}",
            answer.state.as_deref().unwrap_or("?"),
            answer.detail
          ),
        });
      }
      self.board.lock().unwrap().stood.push(Stood {
        service: service.clone(),
        node: node.clone(),
        health: answer,
      });
    }
    Ok(())
  }

  async fn remove(&self, service: &str, node: &str, say: &Say) -> Result<(), Stopped> {
    let declarations = self.declared();
    let declaration = self.declaration(service).ok_or_else(|| {
      refuse(format!("no {service}{DECLARATION_SUFFIX} or [card] table declares {service}"))
    })?;
    let before = self.state().unwrap_or_default();
    if let Some(why) = before.unsettled.get(service) {
      return Err(refuse(format!("{service}'s placement is unsettled ({why}); a place settles it first")));
    }
    if !before.nodes_of(service).iter().any(|one| one == node) {
      return Err(refuse(format!("{service} does not stand on {node}")));
    }
    let rows = self.rows().await?;
    let mut after = before.clone();
    let list: Vec<String> = before.nodes_of(service).iter().filter(|one| *one != node).cloned().collect();
    after.services.insert(service.to_owned(), list.clone());
    let mut journal = Journal {
      verb: Verb::Remove,
      service: service.to_owned(),
      node: node.to_owned(),
      before: before.clone(),
      roles: BTreeMap::from([(
        node.to_owned(),
        rows.iter().find(|row| row.name == node).and_then(|row| row.role.clone()),
      )]),
      occupants: BTreeMap::from([(
        node.to_owned(),
        self
          .hands
          .occupants(node)
          .await
          .map_err(|why| refuse(format!("{node}: the occupants were not read: {why}")))?,
      )]),
      done: Vec::new(),
    };
    self.save_journal(&journal).map_err(refuse)?;
    let steps = async {
      if declaration.adapter.is_some() {
        self.begin(&mut journal, Step::Adapter, say)?;
        let answer = self.hands.adapter(&declaration, "uninstall", Some(node), &list, say).await;
        if !answer.ok {
          return Err(Stopped {
            step: Some(Step::Adapter),
            why: answer.detail,
          });
        }
      }
      self.begin(&mut journal, Step::Placement, say)?;
      self.save_state(&after).map_err(|why| Stopped {
        step: Some(Step::Placement),
        why,
      })?;
      let role = role_of(node, &after, &declarations);
      if journal.roles.get(node).cloned().flatten() != role {
        self.begin(&mut journal, Step::Roles, say)?;
        self.hands.set_role(node, role.as_deref()).await.map_err(|why| Stopped {
          step: Some(Step::Roles),
          why,
        })?;
      }
      self.begin(&mut journal, Step::Occupants, say)?;
      let order = occupants_of(node, &after, &declarations, &self.tiers(), self.hands.desktop(node).await);
      self.hands.write_occupants(node, Some(&order)).await.map_err(|why| Stopped {
        step: Some(Step::Occupants),
        why,
      })?;
      Ok(())
    };
    match steps.await {
      Ok(()) => {
        self.clear_journal();
        say(format!("{service} is taken off {node}"));
        Ok(())
      }
      Err(stopped) => Err(self.finish_with_undo(&journal, stopped, say).await),
    }
  }
}

fn words(nodes: &[String]) -> String {
  if nodes.is_empty() { "(none)".to_owned() } else { nodes.join(", ") }
}

/// A file written beside its name and renamed over it, so no reader sees half of it.
pub fn write_whole(path: &Path, text: &str) -> Result<(), String> {
  if let Some(dir) = path.parent() {
    std::fs::create_dir_all(dir).map_err(|fault| format!("{}: {fault}", dir.display()))?;
  }
  let partial = path.with_extension("partial");
  std::fs::write(&partial, format!("{text}\n"))
    .map_err(|fault| format!("{}: {fault}", partial.display()))?;
  std::fs::rename(&partial, path).map_err(|fault| format!("{}: {fault}", path.display()))
}
