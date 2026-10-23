//! Placing the services (docs/placement.md, 「Placing the services」): requests journaled and undone,
//! reservations, derived roles and occupants, drift, adoption, and the declarations read from their roots.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use bmo_arbiter::master::BoxFuture;
use bmo_arbiter::placement::{
  AdapterAnswer, Board, Declaration, Hands, Journal, NodeRow, Placements, Placer, Say, Spread, Step, Verb,
  YieldBy, declarations, occupants_of, parse_declaration, role_of, tiers_of, tool_declaration, turns_of,
};
use bmo_arbiter::rules::{Num, TurnValues};

/// A world of nodes in memory: their roles, their occupants files, whether each is a desktop, and every
/// adapter verb asked, answered as the test sets.
#[derive(Default)]
struct Stand {
  roles: Mutex<BTreeMap<String, Option<String>>>,
  occupants: Mutex<BTreeMap<String, Option<String>>>,
  desktops: Vec<String>,
  asked: Mutex<Vec<String>>,
  placed: Mutex<BTreeMap<String, Vec<String>>>,
  failing: Mutex<Option<String>>,
  declared: Mutex<Vec<Declaration>>,
}

impl Stand {
  fn with(nodes: &[(&str, Option<&str>)]) -> Stand {
    Stand {
      roles: Mutex::new(
        nodes.iter().map(|(name, role)| (name.to_string(), role.map(str::to_owned))).collect(),
      ),
      occupants: Mutex::new(nodes.iter().map(|(name, _)| (name.to_string(), None)).collect()),
      ..Stand::default()
    }
  }

  fn asked(&self) -> Vec<String> {
    self.asked.lock().unwrap().clone()
  }

  fn role(&self, node: &str) -> Option<String> {
    self.roles.lock().unwrap().get(node).cloned().flatten()
  }

  fn order(&self, node: &str) -> Option<String> {
    self.occupants.lock().unwrap().get(node).cloned().flatten()
  }
}

struct StandHands(Arc<Stand>);

impl Hands for StandHands {
  fn declarations(&self) -> Result<Vec<Declaration>, String> {
    Ok(self.0.declared.lock().unwrap().clone())
  }

  fn nodes(&self) -> BoxFuture<'_, Result<Vec<NodeRow>, String>> {
    let rows = self
      .0
      .roles
      .lock()
      .unwrap()
      .iter()
      .map(|(name, role)| NodeRow {
        name: name.clone(),
        role: role.clone(),
      })
      .collect();
    Box::pin(async move { Ok(rows) })
  }

  fn desktop<'a>(&'a self, node: &'a str) -> BoxFuture<'a, bool> {
    let desktop = self.0.desktops.iter().any(|one| one == node);
    Box::pin(async move { desktop })
  }

  fn set_role<'a>(&'a self, node: &'a str, role: Option<&'a str>) -> BoxFuture<'a, Result<(), String>> {
    self.0.asked.lock().unwrap().push(format!("role {node} {}", role.unwrap_or("none")));
    self.0.roles.lock().unwrap().insert(node.to_owned(), role.map(str::to_owned));
    Box::pin(async { Ok(()) })
  }

  fn occupants<'a>(&'a self, node: &'a str) -> BoxFuture<'a, Result<Option<String>, String>> {
    let standing = self.0.order(node);
    Box::pin(async move { Ok(standing) })
  }

  fn write_occupants<'a>(
    &'a self,
    node: &'a str,
    order: Option<&'a str>,
  ) -> BoxFuture<'a, Result<(), String>> {
    self
      .0
      .asked
      .lock()
      .unwrap()
      .push(format!("occupants {node} {}", order.unwrap_or("(none)")));
    self.0.occupants.lock().unwrap().insert(node.to_owned(), order.map(str::to_owned));
    Box::pin(async { Ok(()) })
  }

  fn arbiter<'a>(&'a self, node: &'a str, _say: &'a Say) -> BoxFuture<'a, Result<(), String>> {
    self.0.asked.lock().unwrap().push(format!("arbiter {node}"));
    Box::pin(async { Ok(()) })
  }

  fn adapter<'a>(
    &'a self,
    declaration: &'a Declaration,
    verb: &'a str,
    node: Option<&'a str>,
    placed: &'a [String],
    _say: &'a Say,
  ) -> BoxFuture<'a, AdapterAnswer> {
    let node = node.map(str::to_owned);
    let call = format!(
      "{} {verb} {} [{}]",
      declaration.service,
      node.as_deref().unwrap_or("-"),
      placed.join(",")
    );
    self.0.asked.lock().unwrap().push(call.clone());
    let failing = self.0.failing.lock().unwrap().clone();
    let theirs = self.0.placed.lock().unwrap().get(&declaration.service).cloned().unwrap_or_default();
    if verb == "install" || verb == "uninstall" {
      self.0.placed.lock().unwrap().insert(declaration.service.clone(), placed.to_vec());
    }
    Box::pin(async move {
      if failing.as_deref() == Some(call.split(' ').take(2).collect::<Vec<_>>().join(" ").as_str()) {
        return AdapterAnswer::refused("the stand-in was told to fail here");
      }
      match verb {
        "placed" => AdapterAnswer {
          ok: true,
          nodes: Some(theirs),
          ..AdapterAnswer::default()
        },
        "health" => AdapterAnswer {
          ok: true,
          node,
          state: Some("running".into()),
          ..AdapterAnswer::default()
        },
        _ => AdapterAnswer {
          ok: true,
          node,
          state: Some("running".into()),
          ..AdapterAnswer::default()
        },
      }
    })
  }
}

fn declaration(service: &str, priority: i64, spread: Spread, alone: bool) -> Declaration {
  Declaration {
    service: service.into(),
    card_mib: 6000,
    priority,
    tier: None,
    yield_by: YieldBy::Turn,
    spread,
    alone,
    adapter: Some(vec!["true".into()]),
    turn: None,
    dir: PathBuf::from("."),
  }
}

fn declared() -> Vec<Declaration> {
  vec![
    declaration("voice", 70, Spread::One, true),
    declaration("router", 60, Spread::One, false),
    declaration("model", 50, Spread::Many, true),
    declaration("router-train", 20, Spread::Many, false),
    Declaration {
      adapter: None,
      ..declaration("song", 30, Spread::One, false)
    },
  ]
}

fn placer(stand: &Arc<Stand>, dir: &Path) -> Placer {
  placer_of(stand, declared(), dir)
}

/// A placer whose hands declare the services given.
fn placer_of(stand: &Arc<Stand>, declarations: Vec<Declaration>, dir: &Path) -> Placer {
  *stand.declared.lock().unwrap() = declarations;
  Placer::new(Arc::new(StandHands(stand.clone())), dir, Arc::new(Mutex::new(Board::default())))
}

fn quiet() -> Say {
  Arc::new(|_| {})
}

fn standing(dir: &Path, services: &[(&str, &[&str])]) {
  let placements = Placements {
    services: services
      .iter()
      .map(|(service, nodes)| (service.to_string(), nodes.iter().map(|one| one.to_string()).collect()))
      .collect(),
    unsettled: BTreeMap::new(),
  };
  std::fs::write(dir.join("services.json"), serde_json::to_string(&placements).unwrap()).unwrap();
}

#[tokio::test]
async fn a_place_on_a_one_service_narrows_and_uninstalls_the_old_node() {
  let dir = tempfile::tempdir().unwrap();
  standing(dir.path(), &[("voice", &["a"])]);
  let stand = Arc::new(Stand::with(&[("a", Some("voice")), ("b", None)]));
  let placer = placer(&stand, dir.path());

  placer.run(Verb::Place, "voice", "b", &quiet()).await.unwrap();

  assert_eq!(placer.state().unwrap().nodes_of("voice"), ["b"]);
  let asked = stand.asked();
  let at = |call: &str| {
    asked
      .iter()
      .position(|one| one == call)
      .unwrap_or_else(|| panic!("{call} never asked: {asked:#?}"))
  };
  assert!(at("arbiter b") < at("voice install b [a,b]"));
  assert!(at("voice install b [a,b]") < at("voice health b [a,b]"));
  assert!(at("voice health b [a,b]") < at("voice uninstall a [b]"));
  assert_eq!((stand.role("b"), stand.role("a")), (Some("voice".into()), None));
  assert_eq!((stand.order("b"), stand.order("a")), (Some("voice".into()), Some("*".into())));
  assert!(!dir.path().join("services-request.json").exists());
}

#[tokio::test]
async fn a_failure_at_install_undoes_the_role_the_occupants_and_the_placement() {
  let dir = tempfile::tempdir().unwrap();
  standing(dir.path(), &[("voice", &["a"])]);
  let stand = Arc::new(Stand::with(&[("a", Some("voice")), ("b", None)]));
  stand.occupants.lock().unwrap().insert("b".into(), Some("desktop *".into()));
  *stand.failing.lock().unwrap() = Some("voice install".into());
  let placer = placer(&stand, dir.path());

  let stopped = placer.run(Verb::Place, "voice", "b", &quiet()).await.unwrap_err();

  assert_eq!(stopped.step, Some(Step::Adapter));
  assert_eq!(placer.state().unwrap().nodes_of("voice"), ["a"]);
  assert_eq!(stand.role("b"), None);
  assert_eq!(stand.order("b"), Some("desktop *".into()));
  assert!(stand.asked().contains(&"voice uninstall b [a]".to_owned()), "{:#?}", stand.asked());
  assert!(!stand.asked().iter().any(|call| call.starts_with("voice uninstall a")));
  assert!(!dir.path().join("services-request.json").exists());
}

#[tokio::test]
async fn a_master_restarted_mid_request_undoes_it_from_the_journal() {
  let dir = tempfile::tempdir().unwrap();
  standing(dir.path(), &[("router", &["a", "b"])]);
  let before = Placements {
    services: BTreeMap::from([("router".into(), vec!["a".into()])]),
    unsettled: BTreeMap::new(),
  };
  let journal = Journal {
    verb: Verb::Place,
    service: "router".into(),
    node: "b".into(),
    before,
    roles: BTreeMap::from([("b".into(), None)]),
    occupants: BTreeMap::from([("b".into(), Some("*".into()))]),
    done: vec![Step::Arbiter, Step::Occupants, Step::Placement, Step::Adapter],
  };
  std::fs::write(dir.path().join("services-request.json"), serde_json::to_string(&journal).unwrap())
    .unwrap();
  let stand = Arc::new(Stand::with(&[("a", None), ("b", None)]));
  stand.occupants.lock().unwrap().insert("b".into(), Some("router *".into()));

  placer(&stand, dir.path()).recover(&quiet()).await;

  assert!(stand.asked().contains(&"router uninstall b [a]".to_owned()), "{:#?}", stand.asked());
  assert_eq!(placer(&stand, dir.path()).state().unwrap().nodes_of("router"), ["a"]);
  assert_eq!(stand.order("b"), Some("*".into()));
  assert!(!dir.path().join("services-request.json").exists());
}

#[tokio::test]
async fn drift_is_named_and_not_followed() {
  let dir = tempfile::tempdir().unwrap();
  standing(dir.path(), &[("voice", &["a"]), ("model", &["k"])]);
  let stand = Arc::new(Stand::with(&[("a", Some("voice")), ("k", None)]));
  stand.placed.lock().unwrap().insert("voice".into(), vec!["b".into()]);
  stand.placed.lock().unwrap().insert("model".into(), vec!["k".into()]);
  stand.occupants.lock().unwrap().insert("a".into(), Some("voice".into()));
  stand.occupants.lock().unwrap().insert("k".into(), Some("router *".into()));
  let placer = placer(&stand, dir.path());

  let found = placer.drift(&quiet()).await.unwrap();

  assert!(
    found
      .iter()
      .any(|line| line.starts_with("voice: the adapter places it on b, the master on a")),
    "{found:#?}"
  );
  assert!(
    found
      .iter()
      .any(|line| line.starts_with("k: the node service's role is none, the placement gives model")),
    "{found:#?}"
  );
  assert!(
    found
      .iter()
      .any(|line| line.starts_with("k: the occupants are router *, the placement gives model")),
    "{found:#?}"
  );
  assert_eq!(found.len(), 3, "{found:#?}");
  assert!(stand.asked().iter().all(|call| call.ends_with("placed - []")), "{:#?}", stand.asked());
  assert_eq!(placer.state().unwrap().nodes_of("voice"), ["a"]);
  assert_eq!(placer.board().lock().unwrap().said.len(), 3);
}

#[tokio::test]
async fn a_node_reserved_for_another_service_is_refused_before_anything_changes() {
  let dir = tempfile::tempdir().unwrap();
  standing(dir.path(), &[("voice", &["a"])]);
  let stand = Arc::new(Stand::with(&[("a", Some("voice")), ("k", Some("model"))]));
  let placer = placer(&stand, dir.path());

  let stopped = placer.run(Verb::Place, "voice", "k", &quiet()).await.unwrap_err();

  assert_eq!(stopped.step, None);
  assert!(stopped.why.contains("reserved for model"), "{}", stopped.why);
  assert!(stand.asked().is_empty(), "{:#?}", stand.asked());
  assert_eq!(placer.state().unwrap().nodes_of("voice"), ["a"]);
}

fn equals() -> Vec<Declaration> {
  vec![
    declaration("voice", 70, Spread::One, true),
    Declaration {
      adapter: None,
      ..declaration("song", 70, Spread::One, true)
    },
    declaration("router", 70, Spread::One, false),
    declaration("model", 50, Spread::Many, true),
  ]
}

#[tokio::test]
async fn a_service_standing_alone_at_the_same_priority_joins_the_reservation_and_no_other_does() {
  let dir = tempfile::tempdir().unwrap();
  standing(dir.path(), &[("voice", &["a"]), ("model", &["k"])]);
  let stand = Arc::new(Stand::with(&[("a", Some("voice")), ("k", Some("model"))]));
  let placer = placer_of(&stand, equals(), dir.path());

  placer.run(Verb::Place, "song", "a", &quiet()).await.unwrap();

  assert_eq!(placer.state().unwrap().nodes_of("song"), ["a"]);
  assert_eq!(
    (stand.role("a"), stand.order("a")),
    (Some("song+voice".into()), Some("song=voice".into()))
  );
  let not_alone = placer.run(Verb::Place, "router", "a", &quiet()).await.unwrap_err();
  assert!(not_alone.why.contains("reserved for song+voice"), "{}", not_alone.why);
  let lower = placer.run(Verb::Place, "model", "a", &quiet()).await.unwrap_err();
  assert!(lower.why.contains("reserved for song+voice"), "{}", lower.why);
  let onto_lower = placer.run(Verb::Place, "song", "k", &quiet()).await.unwrap_err();
  assert!(onto_lower.why.contains("reserved for model"), "{}", onto_lower.why);
  assert_eq!(placer.state().unwrap().nodes_of("song"), ["a"]);
}

#[tokio::test]
async fn a_service_below_every_reserving_one_stands_beneath_them_and_they_are_placed_again_beside_it() {
  let dir = tempfile::tempdir().unwrap();
  standing(dir.path(), &[("voice", &["a"]), ("song", &["a"])]);
  let stand = Arc::new(Stand::with(&[("a", Some("song+voice"))]));
  let mut declarations = equals();
  // Below them in priority, so the reservation takes it; ahead of them on the card by the tier it declares.
  declarations.push(Declaration {
    tier: Some(Num(0.5)),
    ..declaration("router-train", 20, Spread::Many, false)
  });
  let placer = placer_of(&stand, declarations, dir.path());

  placer.run(Verb::Place, "router-train", "a", &quiet()).await.unwrap();

  assert_eq!(placer.state().unwrap().nodes_of("router-train"), ["a"]);
  assert_eq!(
    (stand.role("a"), stand.order("a")),
    (Some("song+voice+router-train".into()), Some("router-train song=voice".into()))
  );
  let level = placer.run(Verb::Place, "router", "a", &quiet()).await.unwrap_err();
  assert!(level.why.contains("song+voice+router-train"), "{}", level.why);
  placer.run(Verb::Place, "voice", "a", &quiet()).await.unwrap();
  assert_eq!(stand.role("a"), Some("song+voice+router-train".into()));
}

#[tokio::test]
async fn each_request_reads_the_declarations_its_hands_give_then() {
  let dir = tempfile::tempdir().unwrap();
  let stand = Arc::new(Stand::with(&[("a", None)]));
  let placer = placer(&stand, dir.path());

  let undeclared = placer.run(Verb::Place, "extra", "a", &quiet()).await.unwrap_err();
  assert!(undeclared.why.contains("extra.card.toml"), "{}", undeclared.why);
  stand.declared.lock().unwrap().push(declaration("extra", 40, Spread::Many, false));
  placer.run(Verb::Place, "extra", "a", &quiet()).await.unwrap();

  assert_eq!(placer.state().unwrap().nodes_of("extra"), ["a"]);
  assert!(stand.asked().contains(&"extra install a [a]".to_owned()), "{:#?}", stand.asked());
}

#[test]
fn services_of_one_tier_stand_at_one_rank_and_a_lower_tier_before_them() {
  let placements = Placements {
    services: BTreeMap::from([
      ("voice".into(), vec!["node-a".into()]),
      ("song".into(), vec!["node-a".into(), "desktop-pc".into()]),
      ("router".into(), vec!["desktop-pc".into()]),
      ("model".into(), vec!["desktop-pc".into()]),
    ]),
    unsettled: BTreeMap::new(),
  };
  let declared = tiers_of(&BTreeMap::new(), &equals());
  assert_eq!(occupants_of("node-a", &placements, &equals(), &declared, false), "song=voice");
  assert_eq!(role_of("node-a", &placements, &equals()).as_deref(), Some("song+voice"));
  assert_eq!(
    occupants_of("desktop-pc", &placements, &equals(), &declared, true),
    "desktop model=router=song"
  );
  // The rules' tiers stand over the declarations': the voice before the song, the model before both.
  let given = BTreeMap::from([("voice".to_owned(), Num(1.0)), ("model".to_owned(), Num(0.5))]);
  let tiers = tiers_of(&given, &equals());
  assert_eq!(occupants_of("node-a", &placements, &equals(), &tiers, false), "voice song");
  assert_eq!(
    occupants_of("desktop-pc", &placements, &equals(), &tiers, true),
    "desktop model router=song"
  );
}

#[test]
fn occupants_come_out_for_a_reserved_a_desktop_and_a_shared_node() {
  let declarations = declared();
  let placements = Placements {
    services: BTreeMap::from([
      ("voice".into(), vec!["node-a".into()]),
      ("model".into(), vec!["node-b".into()]),
      ("router".into(), vec!["desktop-pc".into()]),
      ("song".into(), vec!["desktop-pc".into()]),
      ("router-train".into(), vec!["host-a".into(), "desktop-pc".into()]),
    ]),
    unsettled: BTreeMap::new(),
  };
  let tiers = tiers_of(
    &BTreeMap::from([("router-train".to_owned(), Num(0.5)), ("voice".to_owned(), Num(1.0))]),
    &declarations,
  );
  assert_eq!(occupants_of("node-a", &placements, &declarations, &tiers, false), "voice");
  assert_eq!(role_of("node-a", &placements, &declarations).as_deref(), Some("voice"));
  assert_eq!(
    occupants_of("desktop-pc", &placements, &declarations, &tiers, true),
    "desktop router-train router=song *"
  );
  assert_eq!(role_of("desktop-pc", &placements, &declarations), None);
  assert_eq!(occupants_of("host-a", &placements, &declarations, &tiers, false), "router-train *");
  assert_eq!(occupants_of("idle", &placements, &declarations, &tiers, false), "*");
}

#[tokio::test]
async fn the_first_start_adopts_what_stands_and_marks_a_disagreement_unsettled() {
  let dir = tempfile::tempdir().unwrap();
  let stand =
    Arc::new(Stand::with(&[("node-a", Some("voice")), ("node-b", Some("model")), ("node-c", None)]));
  stand.placed.lock().unwrap().insert("voice".into(), vec!["node-a".into()]);
  stand
    .placed
    .lock()
    .unwrap()
    .insert("model".into(), vec!["node-b".into(), "node-c".into()]);
  stand.placed.lock().unwrap().insert("router".into(), vec!["node-c".into()]);
  let placer = placer(&stand, dir.path());

  let adopted = placer.adopt(&quiet()).await.unwrap();

  assert_eq!(adopted.nodes_of("voice"), ["node-a"]);
  assert_eq!(adopted.nodes_of("router"), ["node-c"]);
  assert!(adopted.unsettled.contains_key("model"));
  assert!(!adopted.unsettled.contains_key("voice"));
  assert_eq!(placer.state(), Some(adopted.clone()));
  let refused = placer.run(Verb::Remove, "model", "node-b", &quiet()).await.unwrap_err();
  assert!(refused.why.contains("unsettled"), "{}", refused.why);
}

#[tokio::test]
async fn a_remove_takes_the_node_off_and_releases_its_reservation() {
  let dir = tempfile::tempdir().unwrap();
  standing(dir.path(), &[("model", &["node-b", "node-c"])]);
  let stand = Arc::new(Stand::with(&[("node-b", Some("model")), ("node-c", Some("model"))]));
  let placer = placer(&stand, dir.path());

  placer.run(Verb::Remove, "model", "node-c", &quiet()).await.unwrap();

  assert_eq!(placer.state().unwrap().nodes_of("model"), ["node-b"]);
  assert!(stand.asked().contains(&"model uninstall node-c [node-b]".to_owned()));
  assert_eq!(stand.role("node-c"), None);
  assert_eq!(stand.order("node-c"), Some("*".into()));
}

#[test]
fn declarations_are_found_through_the_git_index_a_submodule_s_among_them_and_nothing_lists_them() {
  let tree = tempfile::tempdir().unwrap();
  let pooled = tempfile::tempdir().unwrap();
  let git_in = |at: &Path, args: &[&str]| {
    let done = std::process::Command::new("git")
      .args(["-c", "user.name=tester", "-c", "user.email=tester", "-c", "protocol.file.allow=always"])
      .arg("-C")
      .arg(at)
      .args(args)
      .output()
      .unwrap();
    assert!(done.status.success(), "{}", String::from_utf8_lossy(&done.stderr));
  };
  let git = |args: &[&str]| git_in(tree.path(), args);
  git(&["init", "-q"]);
  git_in(pooled.path(), &["init", "-q"]);
  std::fs::write(
    pooled.path().join("pooled.card.toml"),
    "service = \"pooled\"\ncard_mib = 1\npriority = 50\nspread = \"many\"\nalone = false\n",
  )
  .unwrap();
  git_in(pooled.path(), &["add", "pooled.card.toml"]);
  git_in(pooled.path(), &["commit", "-qm", "pooled"]);
  git(&["submodule", "add", "-q", &pooled.path().to_string_lossy(), "pooled"]);
  std::fs::create_dir_all(tree.path().join("gate")).unwrap();
  std::fs::write(
    tree.path().join("gate/router.card.toml"),
    "service = \"router\"\ncard_mib = 7000\npriority = 60\nspread = \"one\"\nalone = false\n",
  )
  .unwrap();
  std::fs::write(
    tree.path().join("model.card.toml"),
    "service = \"model\"\ncard_mib = 1\npriority = 30\nspread = \"many\"\nalone = false\n",
  )
  .unwrap();
  std::fs::write(tree.path().join("stray.card.toml"), "not toml at all [").unwrap();
  git(&["add", "gate/router.card.toml", "model.card.toml"]);

  let found = declarations(&[tree.path().to_owned()]).unwrap();

  assert_eq!(
    found.iter().map(|one| one.service.as_str()).collect::<Vec<_>>(),
    ["model", "pooled", "router"]
  );
  assert_eq!(found[2].dir, tree.path().join("gate"));
  assert_eq!(found[1].dir, tree.path().join("pooled"));
}

#[test]
fn a_root_that_is_no_git_tree_declares_every_file_under_it_its_ignore_files_and_hidden_folders_leave_out() {
  let tree = tempfile::tempdir().unwrap();
  let write = |name: &str, text: &str| {
    let path = tree.path().join(name);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, text).unwrap();
  };
  write(
    "gate/router.card.toml",
    "service = \"router\"\ncard_mib = 7000\npriority = 60\nspread = \"one\"\nalone = false\n",
  );
  write(
    "model.card.toml",
    "service = \"model\"\ncard_mib = 1\npriority = 30\nspread = \"many\"\nalone = false\n",
  );
  write(".gitignore", "node_modules/\n");
  write("node_modules/copy/router.card.toml", "not toml at all [");
  write(".worktrees/old/model.card.toml", "not toml at all [");
  write(
    "tools/speaker/.placed/tool.toml",
    "[card]\nservice = \"speaker\"\ncard_mib = 1\npriority = 70\nspread = \"one\"\nalone = true\n",
  );

  let found = declarations(&[tree.path().to_owned()]).unwrap();

  assert_eq!(
    found.iter().map(|one| one.service.as_str()).collect::<Vec<_>>(),
    ["model", "router", "speaker"]
  );
  assert_eq!(found[1].dir, tree.path().join("gate"));
  assert_eq!(found[2].dir, tree.path().join("tools/speaker"));
}

#[test]
fn the_declarations_of_every_root_are_read_together_and_one_service_declared_in_two_roots_is_refused() {
  let root = |file: &str, text: &str| {
    let tree = tempfile::tempdir().unwrap();
    std::fs::write(tree.path().join(file), text).unwrap();
    for args in [&["init", "-q"][..], &["add", file]] {
      let done = std::process::Command::new("git")
        .arg("-C")
        .arg(tree.path())
        .args(args)
        .output()
        .unwrap();
      assert!(done.status.success(), "{}", String::from_utf8_lossy(&done.stderr));
    }
    tree
  };
  let one = root(
    "router.card.toml",
    "service = \"router\"\ncard_mib = 1\npriority = 60\nspread = \"one\"\nalone = false\n",
  );
  let two = root(
    "voice.card.toml",
    "service = \"voice\"\ncard_mib = 1\npriority = 70\nspread = \"one\"\nalone = true\n",
  );
  let again = root(
    "router.card.toml",
    "service = \"router\"\ncard_mib = 2\npriority = 60\nspread = \"one\"\nalone = false\n",
  );
  let roots = |trees: &[&tempfile::TempDir]| -> Vec<PathBuf> {
    trees.iter().map(|tree| tree.path().to_owned()).collect()
  };

  let found = declarations(&roots(&[&two, &one])).unwrap();
  assert_eq!(found.iter().map(|one| one.service.as_str()).collect::<Vec<_>>(), ["router", "voice"]);
  assert_eq!(found[0].dir, one.path());
  assert!(declarations(&[]).unwrap().is_empty());
  assert!(declarations(&roots(&[&one, &again])).unwrap_err().contains("router"));
}

// A tool folder of a tree's `tools/` whose manifest declares `card`, placed where `placed` says and its source
// manifest beside it.
fn tool_with_card(tree: &Path, folder: &str, card: &str, placed: bool) {
  let manifest = format!(
    "[tool]\nname = \"{folder}\"\nprotocol = 1\nlanguage = \"typescript\"\nrun = [\"bun\", \"run.ts\"]\ntimeout = 10\n\n[[entry]]\nnamespace = \"bmo\"\nname = \"{folder}\"\ndescription = \"Makes one.\"\ninput = \"input.json\"\ncapabilities = [\"room.tools\"]\n\n[card]\n{card}"
  );
  let tool = tree.join("tools").join(folder);
  let mut dirs = vec![tool.clone()];
  if placed {
    dirs.push(tool.join(".placed"));
  }
  for dir in dirs {
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("tool.toml"), &manifest).unwrap();
    std::fs::write(dir.join("input.json"), "{ \"type\": \"object\" }").unwrap();
  }
}

#[test]
fn a_placed_tool_declares_its_service_in_its_manifest_s_card_table_its_adapter_run_from_its_folder() {
  let tree = tempfile::tempdir().unwrap();
  let git = std::process::Command::new("git")
    .arg("-C")
    .arg(tree.path())
    .args(["init", "-q"])
    .output();
  assert!(git.unwrap().status.success());
  tool_with_card(
    tree.path(),
    "bench",
    "service = \"bench\"\ncard_mib = 10000\npriority = 70\nspread = \"one\"\nalone = true\nadapter = [\"bun\", \"scripts/bench.ts\", \"adapter\"]\n",
    true,
  );
  // A tool not yet placed declares nothing, whatever its source manifest says.
  tool_with_card(
    tree.path(),
    "unplaced",
    "service = \"unplaced\"\ncard_mib = 1\npriority = 1\nspread = \"one\"\nalone = false\n",
    false,
  );

  let found = declarations(&[tree.path().to_owned()]).unwrap();

  assert_eq!(found.iter().map(|one| one.service.as_str()).collect::<Vec<_>>(), ["bench"]);
  assert_eq!(found[0].dir, tree.path().join("tools/bench"));
  assert_eq!((found[0].card_mib, found[0].priority, found[0].spread), (10000, 70, Spread::One));
  assert_eq!(
    found[0].adapter.as_deref(),
    Some(&["bun".to_owned(), "scripts/bench.ts".to_owned(), "adapter".to_owned()][..])
  );

  // A service declared both by a placed manifest and by a file is refused, and so is a card table out of shape.
  std::fs::write(
    tree.path().join("bench.card.toml"),
    "service = \"bench\"\ncard_mib = 1\npriority = 1\nspread = \"one\"\nalone = false\n",
  )
  .unwrap();
  let added = std::process::Command::new("git")
    .arg("-C")
    .arg(tree.path())
    .args(["add", "bench.card.toml"])
    .output();
  assert!(added.unwrap().status.success());
  assert!(declarations(&[tree.path().to_owned()]).unwrap_err().contains("bench"));

  tool_with_card(tree.path(), "odd", "service = \"odd\"\ncard_mib = 1\n", true);
  assert!(tool_declaration(&tree.path().join("tools/odd")).unwrap_err().contains("[card]"));
}

#[test]
fn a_declaration_is_read_and_named_by_its_file() {
  let path = Path::new("/tree/gate/router.card.toml");
  let read = parse_declaration(
    "service = \"router\"\ncard_mib = 7000\npriority = 60\nspread = \"one\"\nalone = false\nadapter = [\"bun\", \"scripts/router-adapter.ts\"]\n",
    path,
  )
  .unwrap();
  assert_eq!(
    (read.service.as_str(), read.spread, read.dir.as_path()),
    ("router", Spread::One, Path::new("/tree/gate"))
  );
  let misnamed = parse_declaration(
    "service = \"voice\"\ncard_mib = 1\npriority = 1\nspread = \"one\"\nalone = true\n",
    path,
  );
  assert!(misnamed.unwrap_err().contains("voice.card.toml"));
  assert!(
    parse_declaration(
      "service = \"router\"\ncard_mib = 1\npriority = 1\nspread = \"two\"\nalone = true\n",
      path
    )
    .is_err()
  );
}

#[test]
fn a_turn_table_is_read_into_the_turns_part_every_node_is_handed_and_one_not_understood_is_refused() {
  let path = Path::new("/tree/gate/model.card.toml");
  let head = "service = \"model\"\ncard_mib = 1\npriority = 50\nspread = \"many\"\nalone = true\n";
  let model =
    parse_declaration(&format!("{head}[turn]\nyield_after_s = 300\ncan_pause = false\n"), path).unwrap();
  assert_eq!(
    model.turn,
    Some(TurnValues {
      yield_after_s: Num(300.0),
      can_pause: false
    })
  );
  assert_eq!(parse_declaration(head, path).unwrap().turn, None);
  for wrong in [
    "[turn]\nyield_after_s = 300\n",
    "[turn]\nyield_after_s = -1\ncan_pause = true\n",
    "[turn]\nyield_after_s = 60\ncan_pause = true\nsometimes = 1\n",
    "[turn]\nyield_after_s = \"60\"\ncan_pause = true\n",
  ] {
    assert!(parse_declaration(&format!("{head}{wrong}"), path).is_err(), "{wrong}");
  }

  let mut declarations = declared();
  declarations[2].turn = model.turn;
  declarations[0].turn = Some(TurnValues {
    yield_after_s: Num(7.5),
    can_pause: true,
  });
  let turns = turns_of(&declarations);
  assert_eq!(turns.keys().collect::<Vec<_>>(), ["model", "voice"]);
  assert_eq!(turns["voice"].yield_after_s, Num(7.5));
}
