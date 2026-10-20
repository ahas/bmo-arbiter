//! The node arbiter's install, its removal, the lend step and a node's occupants (docs/node-arbiter.md,
//! 「Installing」, 「Lending the card」, 「The occupants」), driven against a node that answers as a
//! healthy one would, on a paused clock.

use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use bmo_arbiter::install::{
  CARD_FILES, InstallOptions, Outcome, Step, Timing, card_settings, held_units, install, lend, parse_store,
  read_occupants, remove_command, store_command,
};
use bmo_arbiter::machine::{Card, Machine, parse_machine};
use bmo_arbiter::reach::{BoxFuture, NodeCalls, RunResult};

const DESKTOP_PROBE: &str =
  "MACHINE_WSL=1\nMACHINE_CARD_MIB=10240\nMACHINE_CARD_NAME=NVIDIA GeForce RTX 3080\n";
const RIVAL: &str = "older-arbiter.service";

fn answer(stdout: &str, code: i32) -> RunResult {
  RunResult {
    code,
    stdout: stdout.to_owned(),
    attempts: 1,
    ..RunResult::default()
  }
}

/// What a node is changed in from a healthy one.
#[derive(Default)]
struct Changes {
  probe: Option<&'static str>,
  rival: Option<&'static str>,
  pids: Option<Vec<&'static str>>,
  store: Option<RunResult>,
  units: Option<String>,
  installed: Option<bool>,
  occupants: Option<&'static str>,
}

/// A node that answers the arbiter's install as a healthy one would, with what is changed; every command
/// it runs and every file carried to it, with its text and mode, kept.
struct Node {
  changes: Changes,
  pids: Mutex<Vec<&'static str>>,
  commands: Mutex<Vec<String>>,
  carried: Mutex<HashMap<String, (String, u32)>>,
}

impl Node {
  fn new(changes: Changes) -> Self {
    let pids = changes.pids.clone().unwrap_or_else(|| vec!["4242", "4242"]);
    Self {
      changes,
      pids: Mutex::new(pids),
      commands: Mutex::new(Vec::new()),
      carried: Mutex::new(HashMap::new()),
    }
  }

  fn commands(&self) -> Vec<String> {
    self.commands.lock().unwrap().clone()
  }

  fn ran(&self, part: &str) -> bool {
    self.commands().iter().any(|command| command.contains(part))
  }

  fn carried(&self, remote: &str) -> Option<(String, u32)> {
    self.carried.lock().unwrap().get(remote).cloned()
  }

  fn answer(&self, command: &str) -> RunResult {
    let changes = &self.changes;
    if command.starts_with("echo HOME") {
      answer("HOME=/srv/node\n", 0)
    } else if command.contains("MACHINE_WSL") {
      answer(changes.probe.unwrap_or(DESKTOP_PROBE), 0)
    } else if command.contains(&format!("is-active {RIVAL}")) {
      answer(changes.rival.unwrap_or("inactive\n"), 0)
    } else if command.contains("setup.log") {
      answer("CARD-SETUP-DONE ok\n", 0)
    } else if command.contains("7439/card") {
      answer(changes.units.as_deref().unwrap_or(""), 0)
    } else if command.contains("units.store") {
      changes.store.clone().unwrap_or_else(|| answer("STORE 3\n", 0))
    } else if command.contains("MainPID") {
      let mut pids = self.pids.lock().unwrap();
      let pid = if pids.is_empty() { "0" } else { pids.remove(0) };
      answer(&format!("{pid}\n"), 0)
    } else if command.contains("CARD-INSTALLED") {
      answer(if changes.installed == Some(false) { "" } else { "CARD-INSTALLED\n" }, 0)
    } else if command.starts_with("cat ~/.config/bmo-card/occupants") {
      answer(changes.occupants.unwrap_or(""), 0)
    } else {
      answer("", 0)
    }
  }
}

impl NodeCalls for Node {
  fn exec<'a>(&'a self, command: &'a str) -> BoxFuture<'a, RunResult> {
    self.commands.lock().unwrap().push(command.to_owned());
    let answered = self.answer(command);
    Box::pin(async move { answered })
  }

  fn upload<'a>(&'a self, local: &'a Path, remote: &'a str) -> BoxFuture<'a, RunResult> {
    self.commands.lock().unwrap().push(format!("upload {remote}"));
    if let Ok(text) = std::fs::read_to_string(local) {
      let mode = std::fs::metadata(local).unwrap().permissions().mode() & 0o777;
      self.carried.lock().unwrap().insert(remote.to_owned(), (text, mode));
    }
    Box::pin(async { answer("", 0) })
  }
}

fn options(role: Option<&str>, store_gib: Option<f64>) -> InstallOptions {
  InstallOptions {
    role: role.map(str::to_owned),
    code_root: PathBuf::from("/checkout"),
    store_gib,
    rival_units: vec![RIVAL.to_owned()],
    timing: Timing::default(),
  }
}

async fn installed(node: &str, at: &Node, options: &InstallOptions) -> Outcome {
  install(node, at, options, &|_| {}).await
}

#[test]
fn a_wsl_desktop_and_its_card() {
  assert_eq!(
    parse_machine(DESKTOP_PROBE),
    Some(Machine {
      wsl: true,
      card: Some(Card {
        name: "NVIDIA GeForce RTX 3080".into(),
        total_mib: 10240
      })
    })
  );
}

#[test]
fn a_machine_whose_driver_answers_for_no_card_has_none_and_an_answer_that_is_not_the_probes_is_nothing() {
  assert_eq!(
    parse_machine("MACHINE_WSL=0\nMACHINE_CARD_MIB=\nMACHINE_CARD_NAME=\n"),
    Some(Machine {
      wsl: false,
      card: None
    })
  );
  assert_eq!(parse_machine("bash: nvidia-smi: command not found"), None);
}

#[test]
fn the_lock_stands_in_the_nodes_own_home_and_the_desktop_rule_on_wsl() {
  let card = Some(Card {
    name: "x".into(),
    total_mib: 1,
  });
  let desktop = Machine {
    wsl: true,
    card: card.clone(),
  };
  let node = Machine { wsl: false, card };
  assert_eq!(card_settings("/srv/node", &desktop), "CARD_LOCK=/srv/node/.gpu.lock\nCARD_DESKTOP=1\n");
  assert_eq!(card_settings("/srv/other", &node), "CARD_LOCK=/srv/other/.gpu.lock\n");
}

#[tokio::test(start_paused = true)]
async fn the_install_runs_its_steps_in_order_and_leaves_the_unit_standing() {
  let at = Node::new(Changes::default());

  let outcome = installed("desktop-pc", &at, &options(Some("router"), None)).await;

  assert!(outcome.ok, "{:?}", outcome.fault);
  assert_eq!(
    outcome.done,
    [
      Step::Home,
      Step::Rival,
      Step::Machine,
      Step::Env,
      Step::Upload,
      Step::Store,
      Step::Setup,
      Step::Units,
      Step::Running
    ]
  );
  assert_eq!(
    at.carried("/srv/node/.config/bmo-card/env"),
    Some(("CARD_LOCK=/srv/node/.gpu.lock\nCARD_DESKTOP=1\n".to_owned(), 0o600))
  );
  assert!(at.commands().contains(&"chmod 600 /srv/node/.config/bmo-card/env".to_owned()));
  for name in CARD_FILES {
    assert!(at.commands().contains(&format!("upload /srv/node/bmo-card/{name}")), "{name}");
  }
  assert!(at.ran("systemctl --user enable bmo-card.service"));
  // The lend settings, the occupants and the node's rules are none of this install's: an existing lend
  // file is left as it stands, and nothing writes the order or the rules.
  assert!(!at.commands().iter().any(|command| command.ends_with("lend.env")));
  assert!(!at.ran(&format!("disable --now {RIVAL}")));
  assert!(!at.ran("bmo-card/occupants") && !at.ran("rules.json"));
}

#[tokio::test(start_paused = true)]
async fn the_dump_store_is_made_at_the_size_the_master_hands_it_else_at_the_cards_memory_and_never_on_a_spinning_disk()
 {
  let fallocated = |at: &Node| {
    let command = at.commands().into_iter().find(|command| command.contains("units.store")).unwrap();
    let after = command.split("fallocate -l ").nth(1).unwrap();
    after.split_whitespace().next().unwrap().to_owned()
  };
  let bare = Node::new(Changes::default());
  installed("desktop-pc", &bare, &options(None, None)).await;
  assert_eq!(fallocated(&bare), (10240u64 << 20).to_string());
  let asked = Node::new(Changes::default());
  installed("desktop-pc", &asked, &options(None, Some(3.0))).await;
  assert_eq!(fallocated(&asked), (3u64 << 30).to_string());
  assert_eq!(parse_store("STORE 68\n"), Some(68));

  assert!(!store_command(1, true, false).contains("ROTA"));
  assert!(store_command(1, false, false).contains("ROTA"));
  let spinning = Node::new(Changes {
    store: Some(answer("STORE-ON-SPINNING-DISK /dev/sda1\n", 4)),
    ..Changes::default()
  });
  let outcome = installed("desktop-pc", &spinning, &options(None, None)).await;
  assert_eq!((outcome.ok, outcome.step), (false, Some(Step::Store)));
  assert!(outcome.fault.unwrap().contains("spinning disk"));
  assert!(!spinning.ran("systemctl --user enable"));
}

#[tokio::test(start_paused = true)]
async fn a_store_at_another_size_is_made_again_only_while_no_unit_is_out_in_it() {
  let out = serde_json::json!({ "units": [
    { "name": "router/weights", "service": "router", "part": "weights", "state": "evicted" },
    { "name": "router/compute", "service": "router", "part": "compute", "state": "resident" },
    { "name": "voice/store", "service": "voice", "part": "store", "state": "mixed" },
  ] })
  .to_string();
  assert_eq!(held_units(&out), ["router/weights", "voice/store"]);
  assert!(held_units("").is_empty());

  let home = tempfile::tempdir().unwrap();
  let file = home.path().join(".local/share/bmo-card/store/units.store");
  let run = |command: String| {
    std::process::Command::new("bash")
      .args(["-c", &command])
      .env("HOME", home.path())
      .stdin(std::process::Stdio::null())
      .output()
      .unwrap()
  };
  assert!(run(store_command(4096, true, false)).status.success());
  assert_eq!(std::fs::metadata(&file).unwrap().len(), 4096);
  let refused = run(store_command(8192, true, true));
  assert_eq!(
    (refused.status.code(), String::from_utf8_lossy(&refused.stdout).trim().to_owned()),
    (Some(5), "STORE-HOLDS-UNITS 4096".to_owned())
  );
  assert_eq!(std::fs::metadata(&file).unwrap().len(), 4096);
  assert!(run(store_command(4096, true, true)).status.success());
  assert!(run(store_command(8192, true, false)).status.success());
  assert_eq!(std::fs::metadata(&file).unwrap().len(), 8192);

  let holding = Node::new(Changes {
    units: Some(out),
    store: Some(answer("STORE-HOLDS-UNITS 17179869184\n", 5)),
    ..Changes::default()
  });
  let outcome = installed("desktop-pc", &holding, &options(None, None)).await;
  assert_eq!((outcome.ok, outcome.step), (false, Some(Step::Store)));
  assert!(outcome.fault.unwrap().contains("router/weights, voice/store"));
  let store = holding.commands().into_iter().find(|command| command.contains("units.store")).unwrap();
  assert!(store.contains("STORE-HOLDS-UNITS"));
}

#[test]
fn removing_takes_the_unit_off_and_the_store_with_it_unless_it_is_kept() {
  assert!(remove_command(false).contains("disable --now bmo-card.service"));
  assert!(remove_command(false).contains("rm -rf ~/.local/share/bmo-card/store;"));
  assert!(!remove_command(true).contains("bmo-card/store"));
}

#[tokio::test(start_paused = true)]
async fn the_lend_step_writes_its_settings_privately_replaces_the_rival_unit_and_restarts_the_arbiter() {
  let at = Node::new(Changes {
    rival: Some("active\n"),
    ..Changes::default()
  });
  let settings = "LLM_KEY=k\nCARD_ADDRESS=192.0.2.10\nCARD_RPC_SERVER={home}/rpc\n";

  lend("node-a", &at, Some(settings), &[RIVAL.to_owned()], &Timing::default(), &|_| {})
    .await
    .unwrap();

  assert_eq!(
    at.carried("/srv/node/.config/bmo-card/lend.env"),
    Some(("LLM_KEY=k\nCARD_ADDRESS=192.0.2.10\nCARD_RPC_SERVER=/srv/node/rpc\n".to_owned(), 0o600))
  );
  assert!(at.commands().iter().any(|command| command.contains(&format!("disable --now {RIVAL}"))
    && command.contains("restart bmo-card.service")));
}

#[tokio::test(start_paused = true)]
async fn the_lend_step_takes_its_settings_off_and_refuses_a_node_with_no_arbiter() {
  let lent = Node::new(Changes::default());
  lend("node-b", &lent, None, &[], &Timing::default(), &|_| {}).await.unwrap();
  assert!(lent.commands().contains(&"rm -f ~/.config/bmo-card/lend.env".to_owned()));

  let bare = Node::new(Changes {
    installed: Some(false),
    ..Changes::default()
  });
  let fault = lend("node-b", &bare, None, &[], &Timing::default(), &|_| {}).await.unwrap_err();
  assert!(fault.contains("bmo-arbiter place"), "{fault}");
  assert!(!bare.ran("restart"));
}

#[tokio::test(start_paused = true)]
async fn a_node_where_a_rival_unit_still_keeps_the_lock_is_refused() {
  let at = Node::new(Changes {
    rival: Some("active\n"),
    ..Changes::default()
  });

  let outcome = installed("node-a", &at, &options(None, None)).await;

  assert_eq!(outcome.step, Some(Step::Rival));
  assert!(outcome.fault.unwrap().contains(RIVAL));
  assert!(!at.commands().iter().any(|command| command.starts_with("upload")));
}

#[tokio::test(start_paused = true)]
async fn a_machine_with_no_card_is_refused_before_anything_is_carried() {
  let at = Node::new(Changes {
    probe: Some("MACHINE_WSL=0\nMACHINE_CARD_MIB=\nMACHINE_CARD_NAME=\n"),
    ..Changes::default()
  });

  let outcome = installed("example-host", &at, &options(None, None)).await;

  assert_eq!(outcome.step, Some(Step::Machine));
  assert!(!at.commands().iter().any(|command| command.starts_with("upload")));
}

#[tokio::test(start_paused = true)]
async fn a_unit_that_keeps_restarting_is_not_taken_for_one_standing() {
  let at = Node::new(Changes {
    pids: Some(vec!["4242", "4300"]),
    ..Changes::default()
  });

  let outcome = installed("desktop-pc", &at, &options(None, None)).await;

  assert_eq!(outcome.step, Some(Step::Running));
  assert!(outcome.fault.unwrap().contains("journalctl --user -u bmo-card.service"));
}

#[tokio::test(start_paused = true)]
async fn a_nodes_occupants_order_is_read_back_with_the_arbiters_history() {
  let at = Node::new(Changes {
    occupants: Some("jobs desktop *\n--history--\nT jobs * -> jobs desktop *\n"),
    ..Changes::default()
  });

  let view = read_occupants(&at, 5).await.unwrap();

  assert_eq!(view.order, "jobs desktop *");
  assert_eq!(view.history, ["T jobs * -> jobs desktop *"]);
}
