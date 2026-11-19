//! The rules checked, pushed to the nodes they choose, or handed out for one node
//! (docs/placement.md, 「The rules」): what a person or an install runs, apart from the master
//! that serves.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::placement::{declarations, tiers_of};
use crate::reach::{NodeRegistry, NodeShell};
use crate::rules::{Dump, NodeFacts, Num, RamFree, Rules, read_rules, rules_path, selects};

/// Where the master's slave answers on each node's loopback.
pub const SLAVE_UNITS: &str = "curl -s http://127.0.0.1:7439/card";

// Where a node keeps the rules it was pushed, under its account's home.
const NODE_RULES: &str = ".config/bmo-card/rules.json";
const NODE_CONFIG: &str = ".config/bmo-card";

/// What the verbs stand on (docs/api.md, 「Settings」): the data root the rules are under, the arbiter's own
/// tree, the roots the services' declarations are read from, and the node service, or why none is named.
pub struct Roots {
  pub data_root: PathBuf,
  pub code_root: PathBuf,
  pub declaration_roots: Vec<PathBuf>,
  pub registry: Result<NodeRegistry, String>,
}

impl Roots {
  /// The node service, or why the verb that needs it cannot reach the nodes.
  pub fn registry(&self) -> Result<&NodeRegistry, String> {
    self.registry.as_ref().map_err(Clone::clone)
  }
}

/// The arbiter's own tree the master and the verbs read where none is named: the one
/// `BMO_DEPLOY_DIR` names; else, where the binary `<tree>/bin/bmo-arbiter` stands in a release beside
/// the deploy link's, the link itself, never resolved, so each read finds the release live then; else
/// the tree the binary stands in (docs/placement.md, 「Master and slaves」).
pub fn code_root(deploy_dir: Option<PathBuf>, exe: Option<&Path>, link: &Path) -> PathBuf {
  if let Some(named) = deploy_dir.filter(|named| !named.as_os_str().is_empty()) {
    return named;
  }
  let Some(tree) = exe.and_then(|exe| Some(exe.parent()?.parent()?.to_owned())) else {
    return PathBuf::from(".");
  };
  let store_of = |path: &Path| std::fs::canonicalize(path).ok()?.parent().map(Path::to_owned);
  match store_of(link) {
    Some(releases) if store_of(&tree).as_ref() == Some(&releases) => link.to_owned(),
    _ => tree,
  }
}

/// A node as the rules see it, with its shell, whether its slave answered and its card's memory as the
/// slave gave it.
pub struct ReachedNode {
  pub facts: NodeFacts,
  pub answers: bool,
  pub card_mib: Option<f64>,
  pub shell: NodeShell,
}

/// The memory of the card the slave's answer gives; none where it did not answer or gave no card.
pub fn slave_card_mib(answer: &str) -> Option<f64> {
  let read: serde_json::Value = serde_json::from_str(answer).ok()?;
  read.get("device")?.get("totalMiB")?.as_f64()
}

/// Whether the slave's answer says its machine is a desktop; none where it did not answer.
pub fn slave_desktop(answer: &str) -> Option<bool> {
  let read: serde_json::Value = serde_json::from_str(answer).ok()?;
  Some(read.get("desktop") == Some(&serde_json::Value::Bool(true)))
}

/// Each node named, or each the node service lists, with its role and whether the machine is a desktop, as
/// its slave says. A node the node service cannot give is said and passed by; with no node service named,
/// there are no nodes to reach.
pub async fn reach_nodes(roots: &Roots, named: &[String]) -> Result<Vec<ReachedNode>, String> {
  let registry = roots.registry()?;
  let names = if named.is_empty() { registry.listed_nodes().await } else { named.to_vec() };
  let mut reached = Vec::new();
  for name in names {
    let connection = match registry.read_connection(&name).await {
      Ok(connection) => connection,
      Err(fault) => {
        eprintln!("bmo-arbiter: {fault}");
        continue;
      }
    };
    let shell = NodeShell { connection };
    let asked = shell.exec(SLAVE_UNITS).await;
    let desktop = slave_desktop(&asked.stdout);
    reached.push(ReachedNode {
      facts: NodeFacts {
        name,
        role: shell.connection.role.clone(),
        desktop: desktop.unwrap_or(false),
      },
      answers: desktop.is_some(),
      card_mib: slave_card_mib(&asked.stdout),
      shell,
    });
  }
  Ok(reached)
}

/// The rules as the file stands, with the turn values and the tiers the services' declarations carry, or
/// why there are none to go by.
pub fn standing_rules(roots: &Roots) -> Result<Rules, String> {
  let path = rules_path(&roots.data_root);
  match read_rules(&path) {
    Ok(Some(rules)) => with_declared(rules, &roots.declaration_roots),
    Ok(None) => Err(format!("{} does not stand", path.display())),
    Err(fault) => Err(fault.0),
  }
}

/// The rules with the `tiers` part every node is handed, read off the declarations the roots carry
/// (docs/placement.md, 「The rules」, 「Tiers」).
pub fn with_declared(rules: Rules, declaration_roots: &[PathBuf]) -> Result<Rules, String> {
  let declared = declarations(declaration_roots)?;
  Ok(Rules {
    tiers: tiers_of(&rules.tiers, &declared),
    ..rules
  })
}

fn dump_word(dump: Dump) -> &'static str {
  match dump {
    Dump::Ram => "RAM",
    Dump::Disk => "NVMe",
  }
}

/// What the rules would place where, line by line, writing nothing.
pub fn check_lines(rules: &Rules, nodes: &[NodeFacts]) -> Vec<String> {
  let mut lines = Vec::new();
  for one in &rules.occupants {
    let seen: Vec<String> = [
      one.seen_by.tickets.then(|| "tickets".to_owned()),
      one
        .seen_by
        .containers
        .as_ref()
        .filter(|prefix| !prefix.is_empty())
        .map(|prefix| format!("{prefix} containers")),
      one
        .seen_by
        .cgroup
        .as_ref()
        .filter(|cgroup| !cgroup.is_empty())
        .map(|cgroup| format!("{cgroup} processes on the card")),
    ]
    .into_iter()
    .flatten()
    .collect();
    let seen = match seen.as_slice() {
      [] => "seen by nothing".to_owned(),
      [one] => format!("seen by {one}"),
      [rest @ .., last] => format!("seen by {} and {last}", rest.join(", ")),
    };
    let ram = match one.ram_free {
      RamFree::All => "every dump written out".to_owned(),
      RamFree::GiB(size) => format!("{size} GiB kept free"),
    };
    let first = if one.first { ", first on every node" } else { "" };
    lines.push(format!(
      "occupant {}: takes the whole card{first}, {seen}, RAM {ram}, waits behind earlier demands at most {} s",
      one.name, one.waits_behind_s
    ));
  }
  for group in &rules.groups {
    let chosen: Vec<&str> = nodes
      .iter()
      .filter(|node| selects(Some(&group.select), node))
      .map(|node| node.name.as_str())
      .collect();
    let dump = match group.dump {
      None => String::new(),
      Some(dump) => {
        let ram = group.ram_max_gib.map(|most| format!(" (RAM at most {most} GiB)")).unwrap_or_default();
        format!(", unloads to {}{ram}", dump_word(dump))
      }
    };
    let store = group.store_gib.map(|size| format!(", dump store {size} GiB")).unwrap_or_default();
    let pool = if group.pool { " (one pool)" } else { "" };
    let chosen = if chosen.is_empty() { "no node chosen".to_owned() } else { chosen.join(", ") };
    lines.push(format!("group {}{pool}{dump}{store}: {chosen}", group.name));
  }
  for spec in &rules.units {
    let vram = spec.vram_mib.map(|size| format!(", {size} MiB")).unwrap_or_default();
    let group = spec
      .group
      .as_ref()
      .filter(|group| !group.is_empty())
      .map(|group| format!(", group {group}"))
      .unwrap_or_default();
    let dump = spec.dump.map(|dump| format!(", unloads to {}", dump_word(dump))).unwrap_or_default();
    lines.push(format!(
      "unit {}: {}, priority {}{vram}{group}{dump}",
      spec.unit,
      spec.kind.word(),
      spec.priority
    ));
  }
  for rule in &rules.rules {
    let what = rule.when.request.clone().unwrap_or_else(|| "the desktop busy".to_owned());
    let evict = match &rule.evict {
      None => "the default".to_owned(),
      Some(evict) => serde_json::to_string(evict).expect("eviction serializes"),
    };
    let restore = if rule.restore { ", restored once done" } else { "" };
    lines.push(format!("rule {}: {what} → {evict}{restore}", rule.name));
  }
  let mut tiers: Vec<(&String, &Num)> = rules.tiers.iter().collect();
  tiers.sort_by(|a, b| a.1.0.total_cmp(&b.1.0).then(a.0.cmp(b.0)));
  for (name, tier) in tiers {
    lines.push(format!("tier {name}: {tier}"));
  }
  lines
}

/// Each unit whose measured size is more than any card it may stand on holds once its group's reserve is
/// kept back, said as a fault; the cards are the nodes whose slave gave one, and a unit none of them may
/// hold is not judged.
pub fn misfits(rules: &Rules, cards: &[(NodeFacts, f64)]) -> Vec<String> {
  let mut faults = Vec::new();
  for spec in &rules.units {
    let Some(vram) = spec.vram_mib else { continue };
    let widest = cards
      .iter()
      .filter(|(node, _)| selects(spec.place.as_ref(), node))
      .map(|(node, total)| {
        let reserve = rules.group_of(node).map_or(0.0, |group| group.reserve_mib.0);
        (node.name.as_str(), total - reserve)
      })
      .max_by(|a, b| a.1.total_cmp(&b.1));
    if let Some((node, room)) = widest
      && room < vram.0
    {
      faults.push(format!(
        "unit {}'s {vram} MiB fits no card it may stand on; the widest, its reserve kept back, is {node}'s {} MiB",
        spec.unit,
        Num(room)
      ));
    }
  }
  faults
}

/// The first twelve hex digits of the text's SHA-256: how a push names what it wrote.
pub fn text_hash(text: &str) -> String {
  let digest = Sha256::digest(text.as_bytes());
  digest.iter().take(6).map(|byte| format!("{byte:02x}")).collect()
}

/// A node's part of the rules carried to it whole: beside its file and renamed over it, so its slave never
/// reads half of it. The answer is the text's hash, or why it did not land.
pub async fn carry_rules(shell: &NodeShell, part: &Rules) -> Result<String, String> {
  let text = part.node_text();
  let scratch = tempfile::Builder::new()
    .prefix("bmo-card-rules-")
    .tempdir()
    .map_err(|fault| fault.to_string())?;
  let local = scratch.path().join("rules.json");
  std::fs::write(&local, &text).map_err(|fault| fault.to_string())?;
  let carried = shell.upload(&local, "/tmp/bmo-card-rules.json").await;
  if !carried.ok() {
    return Err(carried.fault());
  }
  let placed = shell
    .exec(&format!(
      "mkdir -p ~/{NODE_CONFIG} && mv /tmp/bmo-card-rules.json ~/{NODE_RULES}.partial && mv ~/{NODE_RULES}.partial ~/{NODE_RULES}"
    ))
    .await;
  if !placed.ok() {
    return Err(placed.fault());
  }
  Ok(text_hash(&text))
}

/// One push written down beside the rules file.
pub fn log_push(data_root: &Path, node: &str, hash: &str) {
  let path = data_root.join("placements").join("card-rules.log");
  let line = format!(
    "{} push {node} {hash}\n",
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
  );
  if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
    let _ = file.write_all(line.as_bytes());
  }
}
