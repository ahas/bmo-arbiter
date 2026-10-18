//! The master's rounds (docs/placement.md, 「Master and slaves」, 「The engine's turn」): tickets planned by the
//! rules over a world of stand-in slaves, unloads and restores asked of the slaves, holds said once, and
//! groups planned together.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use bmo_arbiter::master::{
  BoxFuture, Master, Reached, Shell, World, asker_of, awaited_ticket, unload_reason,
};
use bmo_arbiter::reach::RunResult;
use serde_json::{Value, json};

const RULES: &str = r#"
nodes:
  - name: compute
    select: { role: model }
    pool: true
units:
  - unit: "model/*"
    kind: resident
    priority: 50
  - unit: "voice/*"
    priority: 60
    vram_mib: 6000
  - unit: "coder/*"
    kind: on-demand
    priority: 50
    group: coder/pipeline
    vram_mib: 6000
rules:
  - when: { request: "voice/*" }
    evict: { cheapest: ["model/*"] }
    restore: true
  - when: { request: "coder/*" }
    evict: { cheapest: ["model/*"] }
    restore: true
"#;

/// A world of nodes whose slaves all answer what the test sets, every command written down. Where `meet` is
/// set, a move of its verb answers only once that many are asked at once, and fails where they never are.
struct Stand {
  answer: Mutex<Value>,
  /// What a node's slave answers in place of `answer`, where the test sets it.
  answers: Mutex<HashMap<String, Value>>,
  commands: Mutex<Vec<String>>,
  nodes: Vec<String>,
  meet: Option<(&'static str, tokio::sync::Barrier)>,
  /// Every move refused, as a slave whose owner cannot unload answers it.
  refuse_moves: AtomicBool,
}

struct StandShell(Arc<Stand>, String);

impl Shell for StandShell {
  fn exec<'a>(&'a self, command: &'a str) -> BoxFuture<'a, RunResult> {
    Box::pin(async move {
      self.0.commands.lock().unwrap().push(command.to_owned());
      if let Some((_, meet)) =
        self.0.meet.as_ref().filter(|(verb, _)| command.contains(&format!("/units/{verb}")))
        && tokio::time::timeout(std::time::Duration::from_secs(2), meet.wait()).await.is_err()
      {
        return RunResult {
          code: 1,
          attempts: 1,
          ..RunResult::default()
        };
      }
      let stdout = if command.contains("/units/") && self.0.refuse_moves.load(Ordering::SeqCst) {
        "{\"error\":\"no room\"}\n409".to_owned()
      } else if command.contains("/units/") {
        "{\"results\":[]}\n200".to_owned()
      } else {
        let own = self.0.answers.lock().unwrap().get(&self.1).cloned();
        own.unwrap_or_else(|| self.0.answer.lock().unwrap().clone()).to_string()
      };
      RunResult {
        code: 0,
        stdout,
        attempts: 1,
        ..RunResult::default()
      }
    })
  }
}

struct StandWorld(Arc<Stand>);

impl World for StandWorld {
  fn list_nodes(&self) -> BoxFuture<'_, Vec<String>> {
    Box::pin(async { self.0.nodes.clone() })
  }

  fn reach<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Result<Reached, String>> {
    let shell: Arc<dyn Shell> = Arc::new(StandShell(self.0.clone(), name.to_owned()));
    Box::pin(async move {
      Ok(Reached {
        role: Some("model".into()),
        shell,
      })
    })
  }

  fn clock(&self) -> f64 {
    1000.0
  }

  fn log(&self, _line: &str) {}

  fn version(&self) -> String {
    "test".into()
  }
}

fn slave(free: f64, store: Value, claim: Option<&str>, queue: Value) -> Value {
  json!({
    "version": "abc123", "desktop": false, "occupants": ["tester", "*"],
    "card": { "name": "RTX 4070", "totalMiB": 11875, "usedMiB": 11875.0 - free, "freeMiB": free },
    "units": [
      { "name": "model/weights", "service": "model", "part": "weights", "evictable": true, "state": "resident", "vramMiB": 4000 },
      store,
    ],
    "stage": { "waiting": false, "waitingS": 0 },
    "claim": { "by": claim, "since": null },
    "queue": queue,
  })
}

fn store_resident() -> Value {
  json!({ "name": "model/store", "service": "model", "part": "store", "evictable": true, "state": "resident", "vramMiB": 4608 })
}

fn asking(ticket: &str) -> Value {
  json!([{ "ticket": ticket, "service": "voice", "turn": false, "position": 1, "waitedS": 1, "need": { "unit": "voice/large" } }])
}

fn master(answer: Value) -> (Master, Arc<Stand>) {
  master_of(answer, &["a"], None)
}

fn master_of(
  answer: Value,
  nodes: &[&str],
  meet: Option<(&'static str, tokio::sync::Barrier)>,
) -> (Master, Arc<Stand>) {
  let dir = tempfile::tempdir().unwrap().keep();
  let rules_file = dir.join("card-rules.yaml");
  std::fs::write(&rules_file, RULES).unwrap();
  let stand = Arc::new(Stand {
    answer: Mutex::new(answer),
    answers: Mutex::new(HashMap::new()),
    commands: Mutex::new(Vec::new()),
    nodes: nodes.iter().map(|node| node.to_string()).collect(),
    meet,
    refuse_moves: AtomicBool::new(false),
  });
  (Master::new(Arc::new(StandWorld(stand.clone())), rules_file, 2.0), stand)
}

fn sent(stand: &Stand, verb: &str) -> Vec<String> {
  stand
    .commands
    .lock()
    .unwrap()
    .iter()
    .filter(|command| command.contains(&format!("/units/{verb}")))
    .cloned()
    .collect()
}

#[tokio::test]
async fn a_ticket_held_is_planned_again_once_room_can_be_made() {
  // A claim stands on the only card: the ticket is held.
  let (mut master, stand) = master(slave(1500.0, store_resident(), Some("tester"), asking("t1")));
  master.discover().await;
  master.poll().await;
  assert!(sent(&stand, "evict").is_empty());
  assert!(master.state().events.iter().any(|event| event.text.starts_with("voice/large held")));

  // The claim is gone and the same ticket still waits: it is planned again and room is made.
  *stand.answer.lock().unwrap() = slave(1500.0, store_resident(), None, asking("t1"));
  master.poll().await;
  let evicted = sent(&stand, "evict");
  assert_eq!(evicted.len(), 1);
  assert!(evicted[0].contains(r#""units":["model/store"]"#), "{evicted:?}");
  assert!(evicted[0].contains("back once t1 is done"), "{evicted:?}");
}

#[tokio::test]
async fn a_hold_is_said_once_while_its_reason_stands() {
  let (mut master, _stand) = master(slave(1500.0, store_resident(), Some("tester"), asking("t1")));
  master.discover().await;
  master.poll().await;
  master.poll().await;
  let holds = master
    .state()
    .events
    .iter()
    .filter(|event| event.text.starts_with("voice/large held"))
    .count();
  assert_eq!(holds, 1);
}

#[tokio::test]
async fn a_master_started_after_the_unload_restores_once_the_ticket_is_gone() {
  let reason = unload_reason("voice", "voice/large", Some("rule-1"), "t1", true);
  let evicted = json!({ "name": "model/store", "service": "model", "part": "store", "evictable": true,
    "state": "evicted", "vramMiB": 4608, "reason": reason });
  // A master that never saw the unload, while the ticket still holds the room: nothing comes back.
  let (mut master, stand) = master(slave(6100.0, evicted.clone(), None, asking("t1")));
  master.discover().await;
  master.poll().await;
  assert!(sent(&stand, "restore").is_empty());

  // The ticket is gone: what was unloaded for it comes back, read off the slave alone.
  *stand.answer.lock().unwrap() = slave(6100.0, evicted, None, json!([]));
  master.poll().await;
  let restored = sent(&stand, "restore");
  assert_eq!(restored.len(), 1);
  assert!(restored[0].contains(r#""units":["model/store"]"#), "{restored:?}");
  assert!(master.state().events.iter().any(|event| event.text == "model/store@a loaded back"));
}

#[tokio::test]
async fn every_nodes_restore_is_asked_at_once() {
  let reason = unload_reason("coder", "coder/stage", Some("rule-1"), "t1", true);
  let evicted = json!({ "name": "model/store", "service": "model", "part": "store", "evictable": true,
    "state": "evicted", "vramMiB": 4608, "reason": reason });
  // each restore answers only once the other node's is asked too: one node after another would fail both
  let (mut master, stand) = master_of(
    slave(6100.0, evicted, None, json!([])),
    &["a", "b"],
    Some(("restore", tokio::sync::Barrier::new(2))),
  );
  master.discover().await;
  master.poll().await;
  assert_eq!(sent(&stand, "restore").len(), 2);
  let events = master.state().events;
  for node in ["a", "b"] {
    let said = format!("model/store@{node} loaded back");
    assert!(events.iter().any(|event| event.text == said), "{said}");
  }
}

#[tokio::test]
async fn every_nodes_unload_for_its_ticket_is_asked_at_once() {
  // each unload answers only once the other node's is asked too: one node after another would fail both
  let (mut master, stand) = master_of(
    slave(1500.0, store_resident(), None, asking("t1")),
    &["a", "b"],
    Some(("evict", tokio::sync::Barrier::new(2))),
  );
  master.discover().await;
  master.poll().await;
  assert_eq!(sent(&stand, "evict").len(), 2);
  let events = master.state().events;
  for node in ["a", "b"] {
    let said = events
      .iter()
      .find(|event| event.node.as_deref() == Some(node) && event.text.contains("unloaded to load"));
    assert!(
      said.is_some(),
      "{node}: {:?}",
      events.iter().map(|event| &event.text).collect::<Vec<_>>()
    );
  }
}

#[tokio::test]
async fn a_unit_a_claim_unloaded_is_left_where_the_claim_put_it() {
  let evicted = json!({ "name": "model/store", "service": "model", "part": "store", "evictable": true,
    "state": "evicted", "vramMiB": 4608, "reason": "tester" });
  let (mut master, stand) = master(slave(6100.0, evicted, None, json!([])));
  master.discover().await;
  master.poll().await;
  assert!(sent(&stand, "restore").is_empty());
}

/// A queue with the text model's turn on the card, in use or not and giving way to `gives_way_to`, and `waiting` behind it.
fn behind_model(in_use: bool, gives_way_to: Option<&str>, waiting: Value) -> Value {
  let mut turn =
    json!({ "ticket": "k1", "service": "model", "turn": true, "inUse": in_use, "waitedS": 30 });
  if let Some(service) = gives_way_to {
    turn["giveWay"] = json!(true);
    turn["for"] = json!(service);
  }
  let mut queue = vec![turn];
  queue.extend(waiting.as_array().unwrap().iter().cloned());
  Value::Array(queue)
}

#[tokio::test]
async fn nothing_is_unloaded_for_a_ticket_while_the_turn_on_its_card_is_in_use_and_does_not_give_way_to_it()
{
  let (mut master, stand) =
    master(slave(1500.0, store_resident(), None, behind_model(true, None, asking("t1"))));
  master.discover().await;
  master.poll().await;
  assert!(sent(&stand, "evict").is_empty());
  let events = master.state().events;
  assert!(
    events.iter().any(
      |event| event.text.starts_with("voice/large") && event.text.ends_with(": model's turn is in use")
    ),
    "{events:?}"
  );

  // the turn gives way to the voice, which stands above it: room is made at once
  *stand.answer.lock().unwrap() =
    slave(1500.0, store_resident(), None, behind_model(true, Some("voice"), asking("t1")));
  master.poll().await;
  assert_eq!(sent(&stand, "evict").len(), 1);
}

#[tokio::test]
async fn a_slave_that_says_nothing_of_its_turns_use_is_planned_for_as_before() {
  let mut queue = asking("t1");
  queue
    .as_array_mut()
    .unwrap()
    .insert(0, json!({ "ticket": "k1", "service": "model", "turn": true, "waitedS": 30 }));
  let (mut master, stand) = master(slave(1500.0, store_resident(), None, queue));
  master.discover().await;
  master.poll().await;
  assert_eq!(sent(&stand, "evict").len(), 1);
}

/// A node where the text model's units stand on the card and coder's stage is dumped, with `queue`.
fn shared_node(queue: Value) -> Value {
  let mut answer = slave(1500.0, store_resident(), None, queue);
  answer["units"]
    .as_array_mut()
    .unwrap()
    .push(json!({ "name": "coder/stage", "service": "coder",
    "part": "stage", "evictable": true, "state": "ram", "vramMiB": 6000 }));
  answer
}

fn stage_asking(ticket: &str) -> Value {
  json!([{ "ticket": ticket, "service": "coder", "turn": false, "position": 1, "waitedS": 1,
    "need": { "unit": "coder/stage" } }])
}

#[tokio::test]
async fn a_groups_tickets_are_planned_together_once_every_node_it_stands_on_has_one() {
  let (mut master, stand) =
    master_of(shared_node(behind_model(false, Some("coder"), stage_asking("s1"))), &["a", "b"], None);
  stand
    .answers
    .lock()
    .unwrap()
    .insert("b".into(), shared_node(behind_model(false, None, json!([]))));
  master.discover().await;
  master.poll().await;
  assert!(sent(&stand, "evict").is_empty());
  let events = master.state().events;
  let said = ": its group coder/pipeline waits for b";
  assert!(events.iter().any(|event| event.text.ends_with(said)), "{events:?}");

  // b asks too, but the text model's turn there is in use: nothing goes on either node
  let in_use_on_b = shared_node(behind_model(true, None, stage_asking("s2")));
  stand.answers.lock().unwrap().insert("b".into(), in_use_on_b);
  master.poll().await;
  assert!(sent(&stand, "evict").is_empty());

  // the text model rests on b as well: both nodes' text-model units go in the same read
  let resting_on_b = shared_node(behind_model(false, Some("coder"), stage_asking("s2")));
  stand.answers.lock().unwrap().insert("b".into(), resting_on_b);
  master.poll().await;
  let evicted = sent(&stand, "evict");
  assert_eq!(evicted.len(), 2, "{evicted:?}");
}

#[tokio::test]
async fn a_group_waits_for_a_node_its_master_could_not_read_as_that_node_was_last_read() {
  let (mut master, stand) = master_of(shared_node(behind_model(false, None, json!([]))), &["a", "b"], None);
  master.discover().await;
  master.poll().await;
  // b's slave stops answering; a asks with the text model resting: nothing goes on a alone
  stand.answers.lock().unwrap().insert("b".into(), Value::Null);
  *stand.answer.lock().unwrap() = shared_node(behind_model(false, Some("coder"), stage_asking("s1")));
  master.poll().await;
  assert!(sent(&stand, "evict").is_empty());
  let said = ": its group coder/pipeline waits for b";
  assert!(master.state().events.iter().any(|event| event.text.ends_with(said)));
}

#[tokio::test]
async fn an_unload_that_fails_leaves_its_ticket_to_be_planned_again_its_reason_said_once() {
  let (mut master, stand) = master(slave(1500.0, store_resident(), None, asking("t1")));
  stand.refuse_moves.store(true, Ordering::SeqCst);
  master.discover().await;
  master.poll().await;
  master.poll().await;
  assert_eq!(sent(&stand, "evict").len(), 2);
  let failed = master
    .state()
    .events
    .iter()
    .filter(|event| event.text.contains("(409: no room)"))
    .count();
  assert_eq!(failed, 1);
  stand.refuse_moves.store(false, Ordering::SeqCst);
  master.poll().await;
  assert_eq!(sent(&stand, "evict").len(), 3);
}

#[test]
fn an_unload_reason_names_the_ticket_it_waits_on_only_where_it_comes_back() {
  assert_eq!(awaited_ticket(&unload_reason("voice", "voice/large", None, "t-9", true)), Some("t-9"));
  assert_eq!(awaited_ticket(&unload_reason("voice", "voice/large", None, "t-9", false)), None);
  assert_eq!(awaited_ticket("tester"), None);
  assert_eq!(
    asker_of(&unload_reason("voice", "voice/large", Some("rule-1"), "t-9", true)),
    Some("voice")
  );
  assert_eq!(asker_of("tester"), None);
}

/// A node where the text model's store went out for coder's ticket `t1`, with coder's stage `stage` and `queue`.
fn out_for_the_stage(stage: &str, queue: Value) -> Value {
  let reason = unload_reason("coder", "coder/stage", Some("rule-2"), "t1", true);
  let evicted = json!({ "name": "model/store", "service": "model", "part": "store", "evictable": true,
    "state": "evicted", "vramMiB": 4608, "reason": reason });
  let mut answer = slave(6100.0, evicted, None, queue);
  answer["units"]
    .as_array_mut()
    .unwrap()
    .push(json!({ "name": "coder/stage", "service": "coder",
    "part": "stage", "evictable": true, "state": stage, "vramMiB": 6000 }));
  answer
}

#[tokio::test]
async fn a_restore_waits_for_the_askers_own_unit_to_leave_the_card_once_its_ticket_is_gone() {
  // the ticket is gone while the stage still fills the card: a restore now would be answered 409
  let (mut master, stand) = master(out_for_the_stage("resident", json!([])));
  master.discover().await;
  master.poll().await;
  master.poll().await;
  assert!(sent(&stand, "restore").is_empty());
  let said = "model/store@a: back once coder's units leave the card";
  let waits = master.state().events.iter().filter(|event| event.text == said).count();
  assert_eq!(waits, 1);

  // part of the stage has left: still waited for
  *stand.answer.lock().unwrap() = out_for_the_stage("mixed", json!([]));
  master.poll().await;
  assert!(sent(&stand, "restore").is_empty());

  // the stage is out: the text model's store comes back
  *stand.answer.lock().unwrap() = out_for_the_stage("ram", json!([]));
  master.poll().await;
  let restored = sent(&stand, "restore");
  assert_eq!(restored.len(), 1);
  assert!(restored[0].contains(r#""units":["model/store"]"#), "{restored:?}");
}

#[tokio::test]
async fn a_unit_its_slave_brings_back_on_its_own_turn_is_never_restored_by_the_master() {
  // the text model's hold stands in the line on its own WANT while coder's ticket is gone: its own turn loads it
  let model_in_line =
    json!([{ "ticket": "k2", "service": "model", "turn": false, "position": 1, "waitedS": 4 }]);
  let (mut master, stand) = master(out_for_the_stage("ram", model_in_line.clone()));
  master.discover().await;
  master.poll().await;
  assert!(sent(&stand, "restore").is_empty());

  // its turn came and the slave brought the store back, while coder's next ticket stands: it is back
  let mut back = out_for_the_stage("ram", stage_asking("t2"));
  back["units"][1] = store_resident();
  *stand.answer.lock().unwrap() = back.clone();
  master.poll().await;
  assert!(sent(&stand, "restore").is_empty());

  // and stays back once that ticket ends too: nothing waits on any ticket
  back["queue"] = json!([]);
  *stand.answer.lock().unwrap() = back;
  master.poll().await;
  assert!(sent(&stand, "restore").is_empty());
  assert!(master.owes_no_restore());
}

#[tokio::test]
async fn a_target_whose_turn_is_in_use_is_never_unloaded_for_one_of_its_rank_whatever_a_rule_names() {
  // the coder stands at the text model's rank and a rule names the text model's units for it: the text model's turn in use holds it
  let (mut master, stand) = master(shared_node(behind_model(true, None, stage_asking("s1"))));
  master.discover().await;
  master.poll().await;
  master.poll().await;
  assert!(sent(&stand, "evict").is_empty());
  let held = master
    .state()
    .events
    .iter()
    .filter(|event| {
      event.text.starts_with("coder/stage ") && event.text.ends_with(": model's turn is in use")
    })
    .count();
  assert_eq!(held, 1);

  // the text model's turn rests, at its PASS and QUIET or after its last computation: room is made
  *stand.answer.lock().unwrap() = shared_node(behind_model(false, Some("coder"), stage_asking("s1")));
  master.poll().await;
  let evicted = sent(&stand, "evict");
  assert_eq!(evicted.len(), 1);
  assert!(evicted[0].contains("back once s1 is done"), "{evicted:?}");
}

/// A shared node whose card stands empty for one read: the text model's store out with no ticket of the master's,
/// coder's stage dumped, room for the stage, and `queue`.
fn emptied(queue: Value) -> Value {
  let out = json!({ "name": "model/store", "service": "model", "part": "store", "evictable": true,
    "state": "evicted", "vramMiB": 4608 });
  let mut answer = slave(6100.0, out, None, queue);
  answer["units"]
    .as_array_mut()
    .unwrap()
    .push(json!({ "name": "coder/stage", "service": "coder",
    "part": "stage", "evictable": true, "state": "ram", "vramMiB": 6000 }));
  answer
}

#[tokio::test]
async fn a_ticket_placed_on_an_empty_card_is_planned_again_once_the_other_engine_comes_back_resting() {
  let (mut master, stand) = master(emptied(stage_asking("s1")));
  master.discover().await;
  master.poll().await;
  assert!(sent(&stand, "evict").is_empty());

  // the text model's units came back before the stage's turn did, and its turn rests: they go for the ticket
  *stand.answer.lock().unwrap() = shared_node(behind_model(false, Some("coder"), stage_asking("s1")));
  master.poll().await;
  let evicted = sent(&stand, "evict");
  assert_eq!(evicted.len(), 1);
  assert!(evicted[0].contains("back once s1 is done"), "{evicted:?}");

  // what went out stays out for the ticket: nothing is unloaded twice for it
  let mut out = shared_node(behind_model(false, Some("coder"), stage_asking("s1")));
  out["units"][1]["state"] = json!("evicted");
  out["units"][1]["reason"] = json!(unload_reason("coder", "coder/stage", None, "s1", true));
  out["card"]["freeMiB"] = json!(6108.0);
  *stand.answer.lock().unwrap() = out;
  master.poll().await;
  master.poll().await;
  assert_eq!(sent(&stand, "evict").len(), 1);
}

#[tokio::test]
async fn a_ticket_placed_on_an_empty_card_is_held_while_the_other_engine_comes_back_in_use() {
  let (mut master, stand) = master(emptied(stage_asking("s1")));
  master.discover().await;
  master.poll().await;

  *stand.answer.lock().unwrap() = shared_node(behind_model(true, None, stage_asking("s1")));
  master.poll().await;
  master.poll().await;
  assert!(sent(&stand, "evict").is_empty());
  let held = master
    .state()
    .events
    .iter()
    .filter(|event| {
      event.text.starts_with("coder/stage ") && event.text.ends_with(": model's turn is in use")
    })
    .count();
  assert_eq!(held, 1);

  // the text model's turn rests: room is made for the ticket that stood all along
  *stand.answer.lock().unwrap() = shared_node(behind_model(false, Some("coder"), stage_asking("s1")));
  master.poll().await;
  assert_eq!(sent(&stand, "evict").len(), 1);
}

#[tokio::test]
async fn a_group_placed_on_empty_cards_unloads_every_node_at_once_once_the_other_engine_rests_on_all() {
  let nodes = ["node-a", "node-b", "node-c", "node-d"];
  let (mut master, stand) = master_of(emptied(json!([])), &nodes, None);
  let ticket = |node: &str| format!("s-{node}");
  for node in nodes {
    stand.answers.lock().unwrap().insert(node.into(), emptied(stage_asking(&ticket(node))));
  }
  master.discover().await;
  master.poll().await;
  assert!(sent(&stand, "evict").is_empty());

  // the text model comes back on every node, its turn still in use on node-d: nothing goes on any node
  for node in nodes {
    let in_use = node == "node-d";
    let gives_way = (!in_use).then_some("coder");
    let answer = shared_node(behind_model(in_use, gives_way, stage_asking(&ticket(node))));
    stand.answers.lock().unwrap().insert(node.into(), answer);
  }
  master.poll().await;
  assert!(sent(&stand, "evict").is_empty());

  // it rests on node-d too: all four nodes' text-model units go in the same read
  let resting = shared_node(behind_model(false, Some("coder"), stage_asking(&ticket("node-d"))));
  stand.answers.lock().unwrap().insert("node-d".into(), resting);
  master.poll().await;
  let evicted = sent(&stand, "evict");
  assert_eq!(evicted.len(), 4, "{evicted:?}");
}
