//! The master's rounds (docs/placement.md, 「Master and slaves」, 「The engine's turn」): tickets planned by the
//! rules over a world of stand-in slaves, unloads and restores asked of the slaves, holds said once, and
//! groups planned together.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use bmo_arbiter::demands::Demands;
use bmo_arbiter::master::{
  BoxFuture, Master, Reached, Shell, World, asker_of, awaited_ticket, unload_reason,
};
use bmo_arbiter::placement::{AdapterAnswer, Board, Declaration, Placements, Spread, Stood, YieldBy};
use bmo_arbiter::reach::RunResult;
use bmo_arbiter::rules::Num;
use bmo_arbiter_client::cluster::{self as wire, LinkState, PlacedService};
use bmo_arbiter_client::demands::{Demand, DemandAsk, DemandPool, DemandState, DemandWait};
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
  - when: { request: "model/*" }
    evict: { cheapest: ["coder/*"] }
    restore: true
"#;

/// A world of nodes whose slaves all answer what the test sets, every command written down. Where `meet` is
/// set, a move of its verb answers only once that many are asked at once, and fails where they never are.
struct Stand {
  answer: Mutex<Value>,
  /// What a node's slave answers in place of `answer`, where the test sets it.
  answers: Mutex<HashMap<String, Value>>,
  /// A node's role in the node service in place of `model`, where the test sets it.
  roles: Mutex<HashMap<String, String>>,
  commands: Mutex<Vec<String>>,
  nodes: Vec<String>,
  meet: Option<(&'static str, tokio::sync::Barrier)>,
  /// Every move refused, as a slave whose owner cannot unload answers it.
  refuse_moves: AtomicBool,
  /// Every command with the node it ran on.
  ran_on: Mutex<Vec<(String, String)>>,
  /// The folder the rules and the placements stand in.
  dir: PathBuf,
  /// The services declared, and every adapter verb run but `health`, as `<service> <verb> <node>`.
  declared: Mutex<Vec<Declaration>>,
  adapted: Mutex<Vec<String>>,
  /// The instances standing, as `<service> <node>`: `start` stands one, `stop` takes it down, and `health`
  /// answers `running` with its address and key file for one that stands.
  running: Mutex<HashSet<String>>,
  /// The verbs the test holds, as `<service> <verb> <node>`: such a verb answers only once its gate opens.
  gates: Mutex<HashMap<String, Arc<tokio::sync::Notify>>>,
  /// The verbs that fail, as `<service> <verb> <node>`.
  refused: Mutex<HashSet<String>>,
}

struct StandShell(Arc<Stand>, String);

impl Shell for StandShell {
  fn exec<'a>(&'a self, command: &'a str) -> BoxFuture<'a, RunResult> {
    Box::pin(async move {
      self.0.commands.lock().unwrap().push(command.to_owned());
      self.0.ran_on.lock().unwrap().push((self.1.clone(), command.to_owned()));
      if let Some((_, meet)) = self.0.meet.as_ref().filter(|(verb, _)| moves(command, verb))
        && tokio::time::timeout(std::time::Duration::from_secs(2), meet.wait()).await.is_err()
      {
        return RunResult {
          code: 1,
          attempts: 1,
          ..RunResult::default()
        };
      }
      let stdout = if command.contains("/card-units/") && self.0.refuse_moves.load(Ordering::SeqCst) {
        "{\"code\":\"conflict\",\"error\":\"no room\"}\n409".to_owned()
      } else if command.contains("/card-units/") {
        "{}\n200".to_owned()
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
    let role = self.0.roles.lock().unwrap().get(name).cloned().unwrap_or_else(|| "model".into());
    Box::pin(async move {
      Ok(Reached {
        role: Some(role),
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

  fn declarations(&self) -> Result<Vec<Declaration>, String> {
    Ok(self.0.declared.lock().unwrap().clone())
  }

  fn adapter<'a>(
    &'a self,
    declaration: &'a Declaration,
    verb: &'a str,
    node: &'a str,
    _placed: &'a [String],
  ) -> BoxFuture<'a, AdapterAnswer> {
    let call = format!("{} {verb} {node}", declaration.service);
    if verb != "health" {
      self.0.adapted.lock().unwrap().push(call.clone());
    }
    let gate = self.0.gates.lock().unwrap().get(&call).cloned();
    let refused = self.0.refused.lock().unwrap().contains(&call);
    Box::pin(async move {
      if let Some(gate) = gate {
        gate.notified().await;
      }
      if refused {
        return AdapterAnswer::refused("the helper is missing");
      }
      let instance = format!("{} {node}", declaration.service);
      let mut running = self.0.running.lock().unwrap();
      let state = match verb {
        "start" => {
          running.insert(instance.clone());
          "running"
        }
        "stop" => {
          running.remove(&instance);
          "stopped"
        }
        _ if running.contains(&instance) => "running",
        _ => "stopped",
      };
      let standing = state == "running";
      AdapterAnswer {
        ok: true,
        node: Some(node.to_owned()),
        state: Some(state.to_owned()),
        detail: "done".into(),
        address: standing.then(|| format!("http://{node}.test/{}", declaration.service)),
        key_file: standing.then(|| format!("keys/{node}.key")),
        ..AdapterAnswer::default()
      }
    })
  }
}

/// A unit of the card as its slave answers it.
fn unit(name: &str, state: &str, vram_mib: f64, reason: Option<&str>) -> Value {
  let (service, part) = name.split_once('/').unwrap();
  json!({ "name": name, "holderName": service, "part": part, "state": state, "evictable": true, "sinceAt": null,
    "vramMiB": vram_mib, "ramMiB": 0, "reason": reason, "buffers": [], "lastUnload": null, "lastLoad": null,
    "lastLower": null, "door": null })
}

/// A ticket in the card's line as its slave answers it, waiting first in line or holding the turn.
fn ticket(id: i64, service: &str, turn: bool, unit: Option<&str>) -> Value {
  json!({ "id": id, "serviceName": service, "turn": turn, "inUse": false, "resting": false, "giveWay": false,
    "giveWayFor": null, "position": if turn { Value::Null } else { json!(1) }, "waitMs": null, "claimantName": null,
    "waitedMs": 1000, "need": unit.map(|unit| json!({ "unitName": unit })), "ref": null, "rest": false, "keep": null })
}

fn slave(free: f64, store: Value, claim: Option<&str>, queue: Value) -> Value {
  json!({
    "version": "abc123", "desktop": false, "occupants": ["tester", "*"], "ranks": [["tester"], ["*"]],
    "claim": claim.map(|name| json!({ "claimantName": name, "claimedAt": null })),
    "lockHeldBy": [], "busy": false, "queue": queue, "typicalHoldMs": {},
    "units": [unit("model/weights", "resident", 4000.0, None), store],
    "device": { "name": "RTX 4070", "totalMiB": 11875, "usedMiB": 11875.0 - free, "freeMiB": free, "processes": null },
    "stage": { "waiting": false, "waitingMs": 0, "graphs": 0, "waits": 0, "waitedMs": 0 },
    "store": null,
    "ram": { "dumpsMiB": 0, "availableMiB": null, "maxMiB": null, "floorMiB": null, "dump": "disk" },
    "rules": { "loadedAt": null, "error": null },
  })
}

fn store_resident() -> Value {
  unit("model/store", "resident", 4608.0, None)
}

fn asking(id: i64) -> Value {
  json!([ticket(id, "voice", false, Some("voice/large"))])
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
    roles: Mutex::new(HashMap::new()),
    commands: Mutex::new(Vec::new()),
    nodes: nodes.iter().map(|node| node.to_string()).collect(),
    meet,
    refuse_moves: AtomicBool::new(false),
    ran_on: Mutex::new(Vec::new()),
    dir: dir.clone(),
    declared: Mutex::new(Vec::new()),
    adapted: Mutex::new(Vec::new()),
    running: Mutex::new(HashSet::new()),
    gates: Mutex::new(HashMap::new()),
    refused: Mutex::new(HashSet::new()),
  });
  (Master::new(Arc::new(StandWorld(stand.clone())), rules_file, 2.0), stand)
}

/// Whether a command moves a unit by `verb`: `evict` off the card, `restore` back onto it.
fn moves(command: &str, verb: &str) -> bool {
  let state = if verb == "evict" { "evicted" } else { "resident" };
  command.contains("/card-units/") && command.contains(&format!(r#""state":"{state}""#))
}

fn sent(stand: &Stand, verb: &str) -> Vec<String> {
  stand
    .commands
    .lock()
    .unwrap()
    .iter()
    .filter(|command| moves(command, verb))
    .cloned()
    .collect()
}

#[tokio::test]
async fn a_ticket_held_is_planned_again_once_room_can_be_made() {
  // A claim stands on the only card: the ticket is held.
  let (mut master, stand) = master(slave(1500.0, store_resident(), Some("tester"), asking(1)));
  master.discover().await;
  master.poll().await;
  assert!(sent(&stand, "evict").is_empty());
  assert!(master.state().events.iter().any(|event| event.text.starts_with("voice/large held")));

  // The claim is gone and the same ticket still waits: it is planned again and room is made.
  *stand.answer.lock().unwrap() = slave(1500.0, store_resident(), None, asking(1));
  master.poll().await;
  let evicted = sent(&stand, "evict");
  assert_eq!(evicted.len(), 1);
  assert!(evicted[0].contains("/card-units/model%2Fstore"), "{evicted:?}");
  assert!(evicted[0].contains("back once 1 is done"), "{evicted:?}");
}

#[tokio::test]
async fn a_hold_is_said_once_while_its_reason_stands() {
  let (mut master, _stand) = master(slave(1500.0, store_resident(), Some("tester"), asking(1)));
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
  let reason = unload_reason("voice", "voice/large", Some("rule-1"), "1", true);
  let evicted = unit("model/store", "evicted", 4608.0, Some(&reason));
  // A master that never saw the unload, while the ticket still holds the room: nothing comes back.
  let (mut master, stand) = master(slave(6100.0, evicted.clone(), None, asking(1)));
  master.discover().await;
  master.poll().await;
  assert!(sent(&stand, "restore").is_empty());

  // The ticket is gone: what was unloaded for it comes back, read off the slave alone.
  *stand.answer.lock().unwrap() = slave(6100.0, evicted, None, json!([]));
  master.poll().await;
  let restored = sent(&stand, "restore");
  assert_eq!(restored.len(), 1);
  assert!(restored[0].contains("/card-units/model%2Fstore"), "{restored:?}");
  assert!(master.state().events.iter().any(|event| event.text == "model/store@a loaded back"));
}

#[tokio::test]
async fn every_nodes_restore_is_asked_at_once() {
  let reason = unload_reason("coder", "coder/stage", Some("rule-1"), "1", true);
  let evicted = unit("model/store", "evicted", 4608.0, Some(&reason));
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
    slave(1500.0, store_resident(), None, asking(1)),
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
      .find(|event| event.node_name.as_deref() == Some(node) && event.text.contains("unloaded to load"));
    assert!(
      said.is_some(),
      "{node}: {:?}",
      events.iter().map(|event| &event.text).collect::<Vec<_>>()
    );
  }
}

#[tokio::test]
async fn a_unit_a_claim_unloaded_is_left_where_the_claim_put_it() {
  let evicted = unit("model/store", "evicted", 4608.0, Some("tester"));
  let (mut master, stand) = master(slave(6100.0, evicted, None, json!([])));
  master.discover().await;
  master.poll().await;
  assert!(sent(&stand, "restore").is_empty());
}

/// A queue with the text model's turn on the card, in use or not and giving way to `gives_way_to`, and `waiting` behind it.
fn behind_model(in_use: bool, gives_way_to: Option<&str>, waiting: Value) -> Value {
  let mut turn = ticket(11, "model", true, None);
  turn["inUse"] = json!(in_use);
  if let Some(service) = gives_way_to {
    turn["giveWay"] = json!(true);
    turn["giveWayFor"] = json!(service);
  }
  let mut queue = vec![turn];
  queue.extend(waiting.as_array().unwrap().iter().cloned());
  Value::Array(queue)
}

#[tokio::test]
async fn nothing_is_unloaded_for_a_ticket_while_the_turn_on_its_card_is_in_use_and_does_not_give_way_to_it()
{
  let (mut master, stand) =
    master(slave(1500.0, store_resident(), None, behind_model(true, None, asking(1))));
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
    slave(1500.0, store_resident(), None, behind_model(true, Some("voice"), asking(1)));
  master.poll().await;
  assert_eq!(sent(&stand, "evict").len(), 1);
}

/// A node where the text model's units stand on the card and coder's stage is dumped, with `queue`.
fn shared_node(queue: Value) -> Value {
  let mut answer = slave(1500.0, store_resident(), None, queue);
  answer["units"].as_array_mut().unwrap().push(unit("coder/stage", "ram", 6000.0, None));
  answer
}

fn stage_asking(id: i64) -> Value {
  json!([ticket(id, "coder", false, Some("coder/stage"))])
}

#[tokio::test]
async fn a_groups_tickets_are_planned_together_once_every_node_it_stands_on_has_one() {
  let (mut master, stand) =
    master_of(shared_node(behind_model(false, Some("coder"), stage_asking(21))), &["a", "b"], None);
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
  let in_use_on_b = shared_node(behind_model(true, None, stage_asking(22)));
  stand.answers.lock().unwrap().insert("b".into(), in_use_on_b);
  master.poll().await;
  assert!(sent(&stand, "evict").is_empty());

  // the text model rests on b as well: both nodes' text-model units go in the same read
  let resting_on_b = shared_node(behind_model(false, Some("coder"), stage_asking(22)));
  stand.answers.lock().unwrap().insert("b".into(), resting_on_b);
  master.poll().await;
  let evicted = sent(&stand, "evict");
  assert_eq!(evicted.len(), 2, "{evicted:?}");
}

/// A ticket the lending engine's `WANT` raises waiting behind the coder's turn on a card the coder fills.
fn lending_engine_waiting(gives_way_to: Option<&str>) -> Value {
  let mut turn = ticket(31, "coder", true, None);
  turn["inUse"] = json!(true);
  if let Some(service) = gives_way_to {
    turn["giveWay"] = json!(true);
    turn["giveWayFor"] = json!(service);
  }
  let mut waiting = ticket(41, "model", false, Some("model/weights"));
  waiting["need"] = json!({ "unitName": "model/weights", "vramMiB": 4608 });
  let mut answer = slave(1500.0, store_resident(), None, json!([turn, waiting]));
  answer["units"]
    .as_array_mut()
    .unwrap()
    .push(unit("coder/stage", "resident", 6000.0, None));
  answer
}

#[tokio::test]
async fn a_lending_engines_waiting_ticket_is_unloaded_for_by_a_rule_only_once_the_turn_passes_to_it() {
  // the coder's turn is in use and does not give way: the engine's ticket is held with that reason
  let (mut master, stand) = master(lending_engine_waiting(None));
  master.discover().await;
  master.poll().await;
  assert!(sent(&stand, "evict").is_empty());
  let held = master
    .state()
    .events
    .iter()
    .filter(|event| {
      event.text.starts_with("model/weights ") && event.text.ends_with(": coder's turn is in use")
    })
    .count();
  assert_eq!(held, 1);

  // the coder gives way to the engine: the pipeline's unit is unloaded for the engine's ticket
  *stand.answer.lock().unwrap() = lending_engine_waiting(Some("model"));
  master.poll().await;
  let evicted = sent(&stand, "evict");
  assert_eq!(evicted.len(), 1, "{evicted:?}");
  assert!(evicted[0].contains("/card-units/coder%2Fstage"), "{evicted:?}");
  assert!(evicted[0].contains("back once 41 is done"), "{evicted:?}");
}

#[tokio::test]
async fn a_group_waits_for_a_node_its_master_could_not_read_as_that_node_was_last_read() {
  let (mut master, stand) = master_of(shared_node(behind_model(false, None, json!([]))), &["a", "b"], None);
  master.discover().await;
  master.poll().await;
  // b's slave stops answering; a asks with the text model resting: nothing goes on a alone
  stand.answers.lock().unwrap().insert("b".into(), Value::Null);
  *stand.answer.lock().unwrap() = shared_node(behind_model(false, Some("coder"), stage_asking(21)));
  master.poll().await;
  assert!(sent(&stand, "evict").is_empty());
  let said = ": its group coder/pipeline waits for b";
  assert!(master.state().events.iter().any(|event| event.text.ends_with(said)));
}

#[tokio::test]
async fn an_unload_that_fails_leaves_its_ticket_to_be_planned_again_its_reason_said_once() {
  let (mut master, stand) = master(slave(1500.0, store_resident(), None, asking(1)));
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
  let reason = unload_reason("coder", "coder/stage", Some("rule-2"), "1", true);
  let evicted = unit("model/store", "evicted", 4608.0, Some(&reason));
  let mut answer = slave(6100.0, evicted, None, queue);
  answer["units"].as_array_mut().unwrap().push(unit("coder/stage", stage, 6000.0, None));
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
  assert!(restored[0].contains("/card-units/model%2Fstore"), "{restored:?}");
}

#[tokio::test]
async fn a_unit_its_slave_brings_back_on_its_own_turn_is_never_restored_by_the_master() {
  // the text model's hold stands in the line on its own WANT while coder's ticket is gone: its own turn loads it
  let model_in_line = json!([ticket(12, "model", false, None)]);
  let (mut master, stand) = master(out_for_the_stage("ram", model_in_line.clone()));
  master.discover().await;
  master.poll().await;
  assert!(sent(&stand, "restore").is_empty());

  // its turn came and the slave brought the store back, while coder's next ticket stands: it is back
  let mut back = out_for_the_stage("ram", stage_asking(2));
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
  let (mut master, stand) = master(shared_node(behind_model(true, None, stage_asking(21))));
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
  *stand.answer.lock().unwrap() = shared_node(behind_model(false, Some("coder"), stage_asking(21)));
  master.poll().await;
  let evicted = sent(&stand, "evict");
  assert_eq!(evicted.len(), 1);
  assert!(evicted[0].contains("back once 21 is done"), "{evicted:?}");
}

/// A shared node whose card stands empty for one read: the text model's store out with no ticket of the master's,
/// coder's stage dumped, room for the stage, and `queue`.
fn emptied(queue: Value) -> Value {
  let out = unit("model/store", "evicted", 4608.0, None);
  let mut answer = slave(6100.0, out, None, queue);
  answer["units"].as_array_mut().unwrap().push(unit("coder/stage", "ram", 6000.0, None));
  answer
}

#[tokio::test]
async fn a_ticket_placed_on_an_empty_card_is_planned_again_once_the_other_engine_comes_back_resting() {
  let (mut master, stand) = master(emptied(stage_asking(21)));
  master.discover().await;
  master.poll().await;
  assert!(sent(&stand, "evict").is_empty());

  // the text model's units came back before the stage's turn did, and its turn rests: they go for the ticket
  *stand.answer.lock().unwrap() = shared_node(behind_model(false, Some("coder"), stage_asking(21)));
  master.poll().await;
  let evicted = sent(&stand, "evict");
  assert_eq!(evicted.len(), 1);
  assert!(evicted[0].contains("back once 21 is done"), "{evicted:?}");

  // what went out stays out for the ticket: nothing is unloaded twice for it
  let mut out = shared_node(behind_model(false, Some("coder"), stage_asking(21)));
  out["units"][1]["state"] = json!("evicted");
  out["units"][1]["reason"] = json!(unload_reason("coder", "coder/stage", None, "21", true));
  out["device"]["freeMiB"] = json!(6108.0);
  *stand.answer.lock().unwrap() = out;
  master.poll().await;
  master.poll().await;
  assert_eq!(sent(&stand, "evict").len(), 1);
}

#[tokio::test]
async fn a_ticket_placed_on_an_empty_card_is_held_while_the_other_engine_comes_back_in_use() {
  let (mut master, stand) = master(emptied(stage_asking(21)));
  master.discover().await;
  master.poll().await;

  *stand.answer.lock().unwrap() = shared_node(behind_model(true, None, stage_asking(21)));
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
  *stand.answer.lock().unwrap() = shared_node(behind_model(false, Some("coder"), stage_asking(21)));
  master.poll().await;
  assert_eq!(sent(&stand, "evict").len(), 1);
}

#[tokio::test]
async fn a_group_placed_on_empty_cards_unloads_every_node_at_once_once_the_other_engine_rests_on_all() {
  let nodes = ["node-a", "node-b", "node-c", "node-d"];
  let (mut master, stand) = master_of(emptied(json!([])), &nodes, None);
  let ticket = |node: &str| 100 + nodes.iter().position(|one| *one == node).unwrap() as i64;
  for node in nodes {
    stand.answers.lock().unwrap().insert(node.into(), emptied(stage_asking(ticket(node))));
  }
  master.discover().await;
  master.poll().await;
  assert!(sent(&stand, "evict").is_empty());

  // the text model comes back on every node, its turn still in use on node-d: nothing goes on any node
  for node in nodes {
    let in_use = node == "node-d";
    let gives_way = (!in_use).then_some("coder");
    let answer = shared_node(behind_model(in_use, gives_way, stage_asking(ticket(node))));
    stand.answers.lock().unwrap().insert(node.into(), answer);
  }
  master.poll().await;
  assert!(sent(&stand, "evict").is_empty());

  // it rests on node-d too: all four nodes' text-model units go in the same read
  let resting = shared_node(behind_model(false, Some("coder"), stage_asking(ticket("node-d"))));
  stand.answers.lock().unwrap().insert("node-d".into(), resting);
  master.poll().await;
  let evicted = sent(&stand, "evict");
  assert_eq!(evicted.len(), 4, "{evicted:?}");
}

#[tokio::test]
async fn a_slave_answering_no_card_is_lost_with_its_answer_and_shows_the_role_read_last() {
  // An arbiter of another release answers the probe with a route it does not serve.
  let (mut master, stand) = master(json!({ "error": "no such path" }));
  master.discover().await;
  master.poll().await;
  let node = |master: &Master| master.state().nodes.into_iter().find(|node| node.name == "a").unwrap();
  let lost = node(&master);
  assert_eq!(lost.link.state, LinkState::Lost);
  let error = lost.link.error.unwrap();
  assert!(error.contains("is no card") && error.contains("no such path"), "{error}");
  let said = master.state().events;
  assert!(
    said
      .iter()
      .any(|event| event.code == "node-lost" && event.node_name.as_deref() == Some("a")),
    "a node never reached is said lost: {said:?}"
  );

  // The node service gives the node another role while its slave still gives no card.
  stand.roles.lock().unwrap().insert("a".into(), "model+train".into());
  master.discover().await;
  master.poll().await;
  assert_eq!(node(&master).role.as_deref(), Some("model+train"));

  // A slave of this release stands there: the next read connects it.
  *stand.answer.lock().unwrap() = slave(1500.0, store_resident(), None, json!([]));
  master.poll().await;
  let connected = node(&master);
  assert_eq!(connected.link.state, LinkState::Connected);
  assert_eq!(connected.role.as_deref(), Some("model+train"));
  assert!(connected.link.error.is_none());
}

/// A service as it declares itself, at the tier given, yielding by its process where it says so.
fn declared(service: &str, tier: f64, yield_by: YieldBy) -> Declaration {
  Declaration {
    service: service.into(),
    card_mib: 1000,
    priority: 10,
    tier: Some(Num(tier)),
    yield_by,
    spread: Spread::One,
    alone: false,
    adapter: Some(vec!["true".into()]),
    cards: None,
    dir: PathBuf::from("."),
  }
}

fn placed(stand: &Stand, services: &[(&str, &[&str])]) {
  let placements = Placements {
    services: services
      .iter()
      .map(|(service, nodes)| (service.to_string(), nodes.iter().map(|node| node.to_string()).collect()))
      .collect(),
    unsettled: BTreeMap::new(),
  };
  std::fs::write(stand.dir.join("services.json"), serde_json::to_string(&placements).unwrap()).unwrap();
}

fn waiting(id: i64, service: &str, vram_mib: Option<u64>) -> Value {
  let mut one = ticket(id, service, false, None);
  if let Some(vram) = vram_mib {
    one["need"] = json!({ "vramMiB": vram });
  }
  one
}

#[tokio::test]
async fn a_two_card_occupant_yields_only_the_card_a_one_card_request_asks_for_and_keeps_the_other() {
  let (mut master, stand) = master_of(slave(1500.0, store_resident(), None, json!([])), &["a", "b"], None);
  stand
    .answers
    .lock()
    .unwrap()
    .insert("a".into(), slave(1500.0, store_resident(), None, asking(1)));
  master.discover().await;
  master.poll().await;
  let ran_on = stand.ran_on.lock().unwrap().clone();
  let unloads: Vec<&(String, String)> =
    ran_on.iter().filter(|(_, command)| moves(command, "evict")).collect();
  assert_eq!(unloads.len(), 1, "{unloads:?}");
  assert_eq!(unloads[0].0, "a");
  assert!(unloads[0].1.contains("/card-units/model%2Fstore"), "{unloads:?}");
}

#[tokio::test]
async fn an_occupant_of_a_later_tier_is_unloaded_by_default_and_one_of_an_earlier_tier_never() {
  let (mut master, stand) = master(slave(
    1500.0,
    store_resident(),
    None,
    json!([ticket(1, "voice", false, Some("voice/large"))]),
  ));
  std::fs::write(
    stand.dir.join("card-rules.yaml"),
    "occupants:\n  - name: model\n    tier: 0.5\n  - name: voice\n    tier: 1\nunits:\n  - unit: \"voice/*\"\n    vram_mib: 6000\n",
  )
  .unwrap();
  master.discover().await;
  master.poll().await;
  // The model stands at a lower tier than the voice: the voice's request waits, nothing of the model goes.
  assert!(sent(&stand, "evict").is_empty());
  let tiers: Vec<(String, f64)> =
    master.state().tiers.into_iter().map(|one| (one.name, one.tier)).collect();
  assert_eq!(tiers, [("model".to_owned(), 0.5), ("voice".to_owned(), 1.0)]);

  std::fs::write(
    stand.dir.join("card-rules.yaml"),
    "occupants:\n  - name: model\n    tier: 2\n  - name: voice\n    tier: 1\nunits:\n  - unit: \"voice/*\"\n    vram_mib: 6000\n",
  )
  .unwrap();
  // The file is read again where its time of change moved; a second's step makes sure it does.
  std::thread::sleep(std::time::Duration::from_millis(1100));
  master.poll().await;
  let evicted = sent(&stand, "evict");
  assert_eq!(evicted.len(), 1, "{evicted:?}");
  assert!(evicted[0].contains("/card-units/model%2Fstore"), "{evicted:?}");
}

#[tokio::test]
async fn an_occupant_that_yields_by_its_process_goes_down_for_an_earlier_tier_and_comes_back_after() {
  let (mut master, stand) =
    master(slave(1500.0, store_resident(), None, json!([waiting(1, "trainer", None)])));
  *stand.declared.lock().unwrap() = vec![
    declared("translator", 1.0, YieldBy::Process),
    declared("trainer", 0.5, YieldBy::Turn),
    declared("song", 2.0, YieldBy::Turn),
  ];
  placed(&stand, &[("translator", &["a"]), ("trainer", &["a"]), ("song", &["a"])]);
  master.discover().await;
  // The stop runs beside the rounds; the round after its answer comes takes it.
  rounds(&mut master, 2).await;
  assert_eq!(*stand.adapted.lock().unwrap(), ["translator stop a"]);
  assert!(master.state().events.iter().any(|event| event.code == "occupant-stopped"));
  let kept = std::fs::read_to_string(stand.dir.join("process-yields.json")).unwrap();
  assert!(kept.contains("\"serviceName\": \"translator\""), "{kept}");

  // A master started meanwhile reads the stop back, and brings it back once the trainer's ticket is gone.
  let mut again = Master::new(Arc::new(StandWorld(stand.clone())), stand.dir.join("card-rules.yaml"), 2.0);
  *stand.answer.lock().unwrap() = slave(9000.0, store_resident(), None, json!([]));
  again.discover().await;
  rounds(&mut again, 2).await;
  assert_eq!(*stand.adapted.lock().unwrap(), ["translator stop a", "translator start a"]);
  assert!(again.state().events.iter().any(|event| event.code == "occupant-started"));
  assert!(
    !std::fs::read_to_string(stand.dir.join("process-yields.json"))
      .unwrap()
      .contains("translator")
  );
}

#[tokio::test]
async fn a_resting_turn_does_not_keep_a_process_yield_occupant_down() {
  let (mut master, stand) =
    master(slave(1500.0, store_resident(), None, json!([waiting(1, "voice", None)])));
  *stand.declared.lock().unwrap() =
    vec![declared("translator", 1.0, YieldBy::Process), declared("voice", 1.0, YieldBy::Turn)];
  placed(&stand, &[("translator", &["a"]), ("voice", &["a"])]);
  master.discover().await;
  rounds(&mut master, 1).await;
  assert_eq!(*stand.adapted.lock().unwrap(), ["translator stop a"]);
  // The voice has spoken and rests on the card with its turn kept: the translator comes back and waits for
  // the lock, and the node's rest gives way to it (docs/node-arbiter.md, 「Resting」).
  let mut resting = ticket(1, "voice", true, None);
  resting["resting"] = json!(true);
  *stand.answer.lock().unwrap() = slave(9000.0, store_resident(), None, json!([resting]));
  rounds(&mut master, 2).await;
  assert_eq!(*stand.adapted.lock().unwrap(), ["translator stop a", "translator start a"]);
  assert!(
    !std::fs::read_to_string(stand.dir.join("process-yields.json"))
      .unwrap()
      .contains("translator")
  );
}

#[tokio::test]
async fn an_occupant_that_yields_by_its_process_stays_for_a_later_tier_and_a_request_the_card_holds() {
  let (mut master, stand) = master(slave(
    7000.0,
    store_resident(),
    None,
    json!([waiting(1, "song", None), waiting(2, "voice", Some(6000))]),
  ));
  *stand.declared.lock().unwrap() = vec![
    declared("translator", 1.0, YieldBy::Process),
    declared("voice", 1.0, YieldBy::Turn),
    declared("song", 2.0, YieldBy::Turn),
  ];
  placed(&stand, &[("translator", &["a"])]);
  master.discover().await;
  rounds(&mut master, 1).await;
  assert_eq!(master.state().nodes[0].link.state, LinkState::Connected);
  // The song stands at a later tier; the voice's request of its own tier fits in the free memory.
  assert!(stand.adapted.lock().unwrap().is_empty());
  // The same request that the free memory does not hold makes it yield.
  *stand.answer.lock().unwrap() =
    slave(5000.0, store_resident(), None, json!([waiting(2, "voice", Some(6000))]));
  rounds(&mut master, 1).await;
  assert_eq!(*stand.adapted.lock().unwrap(), ["translator stop a"]);
}

/// A master over the nodes given, with the services declared and placed as given, the instances standing
/// that `running` names, and the demands its rounds grant.
async fn demanding(
  nodes: &[&str],
  services: Vec<Declaration>,
  placements: &[(&str, &[&str])],
  running: &[&str],
) -> (Master, Arc<Stand>, Arc<Demands>) {
  let (mut master, stand) = master_of(slave(9000.0, store_resident(), None, json!([])), nodes, None);
  *stand.declared.lock().unwrap() = services;
  placed(&stand, placements);
  stand.running.lock().unwrap().extend(running.iter().map(|one| one.to_string()));
  let demands = Arc::new(Demands::default());
  master.attach_demands(demands.clone());
  master.discover().await;
  rounds(&mut master, 2).await;
  (master, stand, demands)
}

/// Rounds of the master, each letting what it brings up apart from them run between.
async fn rounds(master: &mut Master, count: usize) {
  for _ in 0..count {
    master.poll().await;
    for _ in 0..4 {
      tokio::task::yield_now().await;
    }
  }
}

fn ask(demands: &Demands, service: &str, within: Option<i64>, work_key: Option<&str>) -> i64 {
  let ask = DemandAsk {
    service_name: service.into(),
    count: None,
    within,
    work_key: work_key.map(str::to_owned),
    reference: None,
    rank: None,
    pool: None,
    wait_ms: None,
  };
  demands.book.lock().unwrap().ask(&ask, 1000).unwrap().id
}

/// A demand asked at a rank, in a pool where one is named, its reference naming it.
fn ask_ranked(
  demands: &Demands,
  service: &str,
  rank: u32,
  pool: Option<(&str, u32)>,
  reference: &str,
) -> i64 {
  let ask = DemandAsk {
    service_name: service.into(),
    count: None,
    within: None,
    work_key: None,
    reference: Some(reference.into()),
    rank: Some(rank),
    pool: pool.map(|(key, cards)| DemandPool {
      key: key.into(),
      cards,
    }),
    wait_ms: None,
  };
  demands.book.lock().unwrap().ask(&ask, 1000).unwrap().id
}

fn demand(demands: &Demands, id: i64) -> Demand {
  demands.book.lock().unwrap().held[&id].answer(1000)
}

fn granted_on(demands: &Demands, id: i64) -> Vec<String> {
  demand(demands, id).cards.into_iter().map(|card| card.node_name).collect()
}

/// Whether the master said the code of the service.
fn said(master: &Master, code: &str, service: &str) -> bool {
  master
    .state()
    .events
    .iter()
    .any(|event| event.code == code && event.text.starts_with(service))
}

fn adapted(stand: &Stand) -> Vec<String> {
  stand.adapted.lock().unwrap().clone()
}

/// Two engines of one tier that yield by their process, placed on both nodes.
fn two_engines() -> Vec<Declaration> {
  vec![declared("lab-arena", 2.0, YieldBy::Process), declared("coder", 2.0, YieldBy::Process)]
}

#[tokio::test]
async fn a_demand_is_granted_where_its_service_stands_idle_else_where_it_comes_up_else_waits_in_line() {
  let both: &[&str] = &["a", "b"];
  let (mut master, stand, demands) =
    demanding(both, two_engines(), &[("lab-arena", both), ("coder", both)], &["lab-arena a", "coder b"]).await;

  // Standing idle on a: granted there, nothing started or stopped, the adapter's address handed on.
  let first = ask(&demands, "lab-arena", None, None);
  rounds(&mut master, 1).await;
  let answered = demand(&demands, first);
  assert_eq!(answered.state, DemandState::Granted);
  assert_eq!(answered.cards[0].node_name, "a");
  assert_eq!(answered.cards[0].address.as_deref(), Some("http://a.test/lab-arena"));
  assert_eq!(answered.cards[0].key_file.as_deref(), Some("keys/a.key"));
  assert!(adapted(&stand).is_empty());

  // No idle instance left: b, whose coder of the same tier stands idle, hands the card over to it.
  let second = ask(&demands, "lab-arena", None, None);
  rounds(&mut master, 1).await;
  assert_eq!(demand(&demands, second).reason.unwrap().code, DemandWait::Starting);
  rounds(&mut master, 2).await;
  assert_eq!(granted_on(&demands, second), ["b"]);
  assert_eq!(adapted(&stand), ["coder stop b", "lab-arena start b"]);
  let kept = std::fs::read_to_string(stand.dir.join("process-yields.json")).unwrap();
  assert!(kept.contains("\"kind\": \"handover\""), "{kept}");

  // Every card in use: the next demand waits in line however many rounds pass, takes nothing, and the
  // engine that handed its card over is never brought back for it.
  let third = ask(&demands, "coder", None, None);
  rounds(&mut master, 20).await;
  let waiting = demand(&demands, third);
  assert_eq!(waiting.state, DemandState::Waiting);
  assert_eq!(waiting.reason.unwrap().code, DemandWait::InUse);
  assert_eq!(adapted(&stand), ["coder stop b", "lab-arena start b"]);
  let cluster = master.state();
  assert_eq!(cluster.demands.len(), 3);

  // The first is given back at its caller's boundary: its card goes to the one waiting, handed over.
  assert!(demands.book.lock().unwrap().give_back(first));
  rounds(&mut master, 3).await;
  assert_eq!(granted_on(&demands, third), ["a"]);
  assert_eq!(adapted(&stand), ["coder stop b", "lab-arena start b", "lab-arena stop a", "coder start a"]);
}

#[tokio::test]
async fn the_end_of_a_bring_has_the_next_round_come_at_once() {
  let both: &[&str] = &["a", "b"];
  let (mut master, stand, demands) =
    demanding(both, two_engines(), &[("lab-arena", both), ("coder", both)], &["coder a", "coder b"]).await;
  let asked = ask(&demands, "lab-arena", None, None);
  master.poll().await;
  // The bring runs apart from the rounds; its end wakes the round loop rather than waiting out its rest.
  tokio::time::timeout(std::time::Duration::from_secs(5), demands.wake.notified())
    .await
    .expect("the bring's end woke the rounds");
  master.poll().await;
  assert_eq!(demand(&demands, asked).state, DemandState::Granted);
  assert_eq!(adapted(&stand), ["coder stop a", "lab-arena start a"]);
}

#[tokio::test]
async fn a_demand_within_another_runs_beside_it_elsewhere_or_on_its_lent_card_which_comes_back() {
  let both: &[&str] = &["a", "b"];
  let (mut master, stand, demands) =
    demanding(both, two_engines(), &[("lab-arena", both), ("coder", both)], &["lab-arena a"]).await;
  let outer = ask(&demands, "lab-arena", None, Some("session-1"));
  rounds(&mut master, 1).await;
  assert_eq!(granted_on(&demands, outer), ["a"]);

  // Work on its own engine runs on its own card, beside it.
  let beside = ask(&demands, "lab-arena", Some(outer), None);
  rounds(&mut master, 1).await;
  assert_eq!(granted_on(&demands, beside), ["a"]);
  demands.book.lock().unwrap().give_back(beside);

  // Work on the other engine goes to another card while one is open: b is free.
  let elsewhere = ask(&demands, "coder", Some(outer), None);
  rounds(&mut master, 3).await;
  assert_eq!(granted_on(&demands, elsewhere), ["b"]);
  assert_eq!(demand(&demands, outer).state, DemandState::Granted);
  demands.book.lock().unwrap().give_back(elsewhere);
  let other = ask(&demands, "lab-arena", None, None);
  rounds(&mut master, 3).await;
  assert_eq!(granted_on(&demands, other), ["b"]);

  // No other card open: the caller's own card is lent, its engine handed over.
  stand.adapted.lock().unwrap().clear();
  let lent = ask(&demands, "coder", Some(outer), None);
  rounds(&mut master, 3).await;
  assert_eq!(granted_on(&demands, lent), ["a"]);
  assert_eq!(demand(&demands, outer).state, DemandState::Lent);
  assert_eq!(adapted(&stand), ["lab-arena stop a", "coder start a"]);

  // Given back: the lent engine yields and the caller's own comes back on the same card.
  demands.book.lock().unwrap().give_back(lent);
  rounds(&mut master, 3).await;
  assert_eq!(adapted(&stand), ["lab-arena stop a", "coder start a", "coder stop a", "lab-arena start a"]);
  let back = demand(&demands, outer);
  assert_eq!((back.state, granted_on(&demands, outer)), (DemandState::Granted, vec!["a".to_owned()]));
}

#[tokio::test]
async fn the_work_key_prefers_the_card_that_last_served_it() {
  let both: &[&str] = &["a", "b"];
  let (mut master, _stand, demands) =
    demanding(both, two_engines(), &[("lab-arena", both)], &["lab-arena a", "lab-arena b"]).await;
  let holding = ask(&demands, "lab-arena", None, None);
  let keyed = ask(&demands, "lab-arena", None, Some("session-7"));
  rounds(&mut master, 1).await;
  assert_eq!(
    (granted_on(&demands, holding), granted_on(&demands, keyed)),
    (vec!["a".to_owned()], vec!["b".to_owned()])
  );
  demands.book.lock().unwrap().give_back(holding);
  demands.book.lock().unwrap().give_back(keyed);
  rounds(&mut master, 1).await;
  // Both cards stand idle, a first in the placement's order: the key goes back to b.
  let again = ask(&demands, "lab-arena", None, Some("session-7"));
  let plain = ask(&demands, "lab-arena", None, None);
  rounds(&mut master, 1).await;
  assert_eq!(
    (granted_on(&demands, again), granted_on(&demands, plain)),
    (vec!["b".to_owned()], vec!["a".to_owned()])
  );
}

#[tokio::test]
async fn an_engine_in_use_is_never_stopped_for_its_own_tier_and_a_lower_tier_takes_its_card_and_gives_it_back()
 {
  let services = vec![
    declared("lab-arena", 2.0, YieldBy::Process),
    declared("voice", 2.0, YieldBy::Turn),
    declared("bench", 1.0, YieldBy::Turn),
  ];
  let (mut master, stand, demands) =
    demanding(&["a"], services, &[("lab-arena", &["a"])], &["lab-arena a"]).await;
  let held = ask(&demands, "lab-arena", None, None);
  rounds(&mut master, 1).await;
  assert_eq!(granted_on(&demands, held), ["a"]);

  // A request of its own tier waits on the card for as long as the demand stands.
  *stand.answer.lock().unwrap() = slave(9000.0, store_resident(), None, json!([waiting(1, "voice", None)]));
  rounds(&mut master, 10).await;
  assert!(adapted(&stand).is_empty());
  assert_eq!(demand(&demands, held).state, DemandState::Granted);

  // The bench's ticket, a lower tier, stops it; the demand is told who took its card.
  *stand.answer.lock().unwrap() = slave(9000.0, store_resident(), None, json!([waiting(2, "bench", None)]));
  rounds(&mut master, 1).await;
  assert_eq!(adapted(&stand), ["lab-arena stop a"]);
  let taken = demand(&demands, held);
  assert_eq!((taken.state, taken.taken.as_deref()), (DemandState::Waiting, Some("bench")));
  assert_eq!(taken.reason.unwrap().code, DemandWait::Held);

  // The bench gone, what it stopped comes back and the demand is granted there again: the round that takes
  // the stop's answer runs the start, and the round after takes the start's.
  *stand.answer.lock().unwrap() = slave(9000.0, store_resident(), None, json!([]));
  rounds(&mut master, 2).await;
  assert_eq!(adapted(&stand), ["lab-arena stop a", "lab-arena start a"]);
  let back = demand(&demands, held);
  assert_eq!((back.state, back.taken), (DemandState::Granted, None));
  assert_eq!(granted_on(&demands, held), ["a"]);
}

#[tokio::test]
async fn a_service_that_yields_by_its_turn_is_granted_where_it_stands_idle_and_never_started() {
  let both: &[&str] = &["a", "b"];
  let services = vec![declared("voice", 2.0, YieldBy::Turn)];
  let (mut master, stand, demands) =
    demanding(both, services, &[("voice", both)], &["voice a", "voice b"]).await;
  let first = ask(&demands, "voice", None, None);
  let second = ask(&demands, "voice", None, None);
  let third = ask(&demands, "voice", None, None);
  rounds(&mut master, 3).await;
  assert_eq!(
    (granted_on(&demands, first), granted_on(&demands, second)),
    (vec!["a".to_owned()], vec!["b".to_owned()])
  );
  assert_eq!(demand(&demands, third).state, DemandState::Waiting);
  assert!(adapted(&stand).is_empty());
  let instances = &master.state().services;
  assert!(instances.is_empty(), "no placement board is attached here: {instances:?}");
  demands.book.lock().unwrap().give_back(second);
  rounds(&mut master, 1).await;
  assert_eq!(granted_on(&demands, third), ["b"]);
}

#[tokio::test]
async fn a_hand_over_is_not_given_back_and_a_lower_tier_brings_back_only_what_it_stopped() {
  let mut services = two_engines();
  services.push(declared("bench", 1.0, YieldBy::Turn));
  let (mut master, stand, demands) =
    demanding(&["a"], services, &[("lab-arena", &["a"]), ("coder", &["a"])], &["lab-arena a"]).await;
  // The coder comes up on a, as a placement starts it, and asks the card: the idle lab-arena hands it over.
  *stand.answer.lock().unwrap() = slave(9000.0, store_resident(), None, json!([waiting(1, "coder", None)]));
  rounds(&mut master, 1).await;
  assert_eq!(adapted(&stand), ["lab-arena stop a"]);
  stand.running.lock().unwrap().insert("coder a".into());
  let coder = ask(&demands, "coder", None, None);
  *stand.answer.lock().unwrap() = slave(9000.0, store_resident(), None, json!([]));
  master.discover().await;
  rounds(&mut master, 2).await;
  assert_eq!(granted_on(&demands, coder), ["a"]);
  // The coder's ticket gone, the lab-arena stays down.
  assert_eq!(adapted(&stand), ["lab-arena stop a"]);
  // The bench stops the coder in use, and once gone brings back the coder alone.
  *stand.answer.lock().unwrap() = slave(9000.0, store_resident(), None, json!([waiting(2, "bench", None)]));
  rounds(&mut master, 1).await;
  *stand.answer.lock().unwrap() = slave(9000.0, store_resident(), None, json!([]));
  rounds(&mut master, 2).await;
  assert_eq!(adapted(&stand), ["lab-arena stop a", "coder stop a", "coder start a"]);
  assert_eq!(granted_on(&demands, coder), ["a"]);
}

#[tokio::test]
async fn a_stop_is_let_go_once_its_service_stands_again_by_its_health_or_by_a_placement() {
  let both: &[&str] = &["a", "b"];
  let (mut master, stand, demands) =
    demanding(both, two_engines(), &[("lab-arena", both), ("coder", both)], &["lab-arena a", "lab-arena b"]).await;
  let board = Arc::new(Mutex::new(Board::default()));
  master.attach(board.clone());
  // The coder asks both cards: each idle lab-arena hands its card over, and the hand-overs are kept once the
  // round after the stops answer takes them.
  *stand.answer.lock().unwrap() = slave(9000.0, store_resident(), None, json!([waiting(1, "coder", None)]));
  rounds(&mut master, 2).await;
  assert_eq!(adapted(&stand), ["lab-arena stop a", "lab-arena stop b"]);
  *stand.answer.lock().unwrap() = slave(9000.0, store_resident(), None, json!([]));
  let kept = || std::fs::read_to_string(stand.dir.join("process-yields.json")).unwrap();
  assert_eq!(kept().matches("\"kind\": \"handover\"").count(), 2, "{}", kept());

  // Brought up on a by something else, its health answers running there: the stop is let go and it stands.
  stand.running.lock().unwrap().insert("lab-arena a".into());
  master.discover().await;
  rounds(&mut master, 2).await;
  assert!(!kept().contains("\"nodeName\": \"a\""), "{}", kept());
  assert!(kept().contains("\"nodeName\": \"b\""), "{}", kept());
  assert!(master.state().events.iter().any(|event| event.code == "occupant-standing"));
  let first = ask(&demands, "lab-arena", None, None);
  rounds(&mut master, 1).await;
  assert_eq!(granted_on(&demands, first), ["a"]);

  // A placement stands it on b: its stop there is let go, and a demand is granted on it as it stands.
  stand.running.lock().unwrap().insert("lab-arena b".into());
  board.lock().unwrap().stood.push(Stood {
    service: "lab-arena".into(),
    node: "b".into(),
    health: AdapterAnswer {
      ok: true,
      node: Some("b".into()),
      state: Some("running".into()),
      address: Some("http://b.test/lab-arena".into()),
      ..AdapterAnswer::default()
    },
  });
  let second = ask(&demands, "lab-arena", None, None);
  rounds(&mut master, 1).await;
  assert!(!kept().contains("lab-arena"), "{}", kept());
  assert_eq!(granted_on(&demands, second), ["b"]);
  assert_eq!(demand(&demands, second).cards[0].address.as_deref(), Some("http://b.test/lab-arena"));
  assert_eq!(adapted(&stand), ["lab-arena stop a", "lab-arena stop b"]);
}

/// A service as it declares itself, with the ceiling given.
fn capped(service: &str, yield_by: YieldBy, cards: u32) -> Declaration {
  Declaration {
    cards: Some(cards),
    ..declared(service, 2.0, yield_by)
  }
}

/// The service as the cluster shows it, its placement board standing in for the placer's.
fn shown(board: &Mutex<Board>, master: &Master, service: &str) -> PlacedService {
  if board.lock().unwrap().services.iter().all(|one| one.service_name != service) {
    board.lock().unwrap().services.push(PlacedService {
      service_name: service.into(),
      card_mib: 1000.0,
      priority: 10.0,
      tier: Some(2.0),
      yield_by: wire::YieldBy::Process,
      spread: wire::Spread::Many,
      alone: false,
      cards: None,
      cards_in_use: Vec::new(),
      node_names: Vec::new(),
      instances: Vec::new(),
      unsettled: None,
    });
  }
  master.state().services.into_iter().find(|one| one.service_name == service).unwrap()
}

#[tokio::test]
async fn a_service_at_its_ceiling_takes_no_other_card_and_its_demands_wait_in_line_until_one_of_its_own_frees()
 {
  let all: &[&str] = &["a", "b", "c"];
  let services = vec![capped("lab-arena", YieldBy::Process, 2), declared("coder", 2.0, YieldBy::Process)];
  let (mut master, stand, demands) =
    demanding(all, services, &[("lab-arena", all), ("coder", all)], &["lab-arena a", "lab-arena b", "lab-arena c"])
      .await;
  let board = Arc::new(Mutex::new(Board::default()));
  master.attach(board.clone());
  let first = ask(&demands, "lab-arena", None, None);
  let second = ask(&demands, "lab-arena", None, None);
  rounds(&mut master, 1).await;
  assert_eq!(
    (granted_on(&demands, first), granted_on(&demands, second)),
    (vec!["a".to_owned()], vec!["b".to_owned()])
  );

  // Two cards in use, its ceiling: c, where it stands idle, is not granted it, and the line keeps its order.
  let third = ask(&demands, "lab-arena", None, None);
  let fourth = ask(&demands, "lab-arena", None, None);
  rounds(&mut master, 10).await;
  for id in [third, fourth] {
    let waiting = demand(&demands, id);
    assert_eq!(waiting.state, DemandState::Waiting);
    let reason = waiting.reason.unwrap();
    assert_eq!(reason.code, DemandWait::Ceiling);
    assert!(reason.text.contains("ceiling is 2: a, b"), "{}", reason.text);
  }
  assert!(adapted(&stand).is_empty());
  assert_eq!(shown(&board, &master, "lab-arena").cards_in_use, ["a", "b"]);

  // The card it leaves is the other engine's to take, its idle lab-arena handed over.
  let coder = ask(&demands, "coder", None, None);
  rounds(&mut master, 3).await;
  assert_eq!(granted_on(&demands, coder), ["c"]);
  assert_eq!(adapted(&stand), ["lab-arena stop c", "coder start c"]);
  assert_eq!(shown(&board, &master, "coder").cards_in_use, ["c"]);

  // One of its own given back: the next in line takes that card, the one after it still waits.
  assert!(demands.book.lock().unwrap().give_back(first));
  rounds(&mut master, 2).await;
  assert_eq!(granted_on(&demands, third), ["a"]);
  assert_eq!(demand(&demands, fourth).reason.unwrap().code, DemandWait::Ceiling);
  assert_eq!(adapted(&stand), ["lab-arena stop c", "coder start c"]);
}

#[tokio::test]
async fn a_better_rank_takes_a_card_of_its_own_engine_with_no_engine_stopped_and_the_loser_comes_back() {
  let (mut master, stand, demands) = demanding(
    &["a"],
    vec![capped("lab-arena", YieldBy::Process, 3)],
    &[("lab-arena", &["a"])],
    &["lab-arena a"],
  )
  .await;
  let low = ask_ranked(&demands, "lab-arena", 1, None, "low");
  rounds(&mut master, 1).await;
  assert_eq!(granted_on(&demands, low), ["a"]);

  let high = ask_ranked(&demands, "lab-arena", 0, None, "high");
  rounds(&mut master, 1).await;
  assert_eq!(granted_on(&demands, high), ["a"]);
  let lost = demand(&demands, low);
  assert_eq!(
    (lost.state, lost.taken.as_deref(), lost.outranked),
    (DemandState::Waiting, Some("high"), true)
  );
  assert!(adapted(&stand).is_empty(), "the engine stays up: {:?}", adapted(&stand));
  let said = master.state().events;
  assert!(
    said.iter().any(|note| note.code == "card-outranked" && note.text.contains("low")),
    "{said:?}"
  );

  // Nothing more moves while the better rank holds it.
  rounds(&mut master, 5).await;
  assert_eq!(granted_on(&demands, high), ["a"]);
  assert_eq!(demand(&demands, low).reason.unwrap().code, DemandWait::InUse);

  assert!(demands.book.lock().unwrap().give_back(high));
  rounds(&mut master, 1).await;
  let back = demand(&demands, low);
  assert_eq!((back.state, back.taken, back.outranked), (DemandState::Granted, None, false));
  assert!(adapted(&stand).is_empty());
}

#[tokio::test]
async fn a_better_rank_for_the_other_engine_stops_the_one_standing_as_a_hand_over() {
  let (mut master, stand, demands) =
    demanding(&["a"], two_engines(), &[("lab-arena", &["a"]), ("coder", &["a"])], &["lab-arena a"]).await;
  let low = ask_ranked(&demands, "lab-arena", 1, None, "low");
  rounds(&mut master, 1).await;
  let high = ask_ranked(&demands, "coder", 0, None, "high");
  rounds(&mut master, 3).await;
  assert_eq!(adapted(&stand), ["lab-arena stop a", "coder start a"]);
  assert_eq!(granted_on(&demands, high), ["a"]);
  assert_eq!(demand(&demands, low).taken.as_deref(), Some("high"));

  // Once it is given back the loser's engine is brought up again for it, its seat in the line kept.
  assert!(demands.book.lock().unwrap().give_back(high));
  rounds(&mut master, 3).await;
  assert_eq!(adapted(&stand), ["lab-arena stop a", "coder start a", "coder stop a", "lab-arena start a"]);
  assert_eq!(granted_on(&demands, low), ["a"]);
}

#[tokio::test]
async fn demands_of_one_rank_wait_in_the_order_asked_and_nothing_is_taken_between_them() {
  let (mut master, stand, demands) = demanding(
    &["a"],
    vec![declared("lab-arena", 2.0, YieldBy::Process)],
    &[("lab-arena", &["a"])],
    &["lab-arena a"],
  )
  .await;
  let first = ask_ranked(&demands, "lab-arena", 1, None, "first");
  rounds(&mut master, 1).await;
  let second = ask(&demands, "lab-arena", None, None);
  rounds(&mut master, 3).await;
  assert_eq!(granted_on(&demands, first), ["a"]);
  assert_eq!(demand(&demands, first).taken, None);
  assert_eq!(demand(&demands, second).reason.unwrap().code, DemandWait::InUse);
  assert_eq!(demand(&demands, second).rank, 1, "a demand naming no rank stands at one");
  assert!(adapted(&stand).is_empty());
}

#[tokio::test]
async fn no_rank_takes_a_card_a_claim_or_the_bench_holds_and_a_claim_takes_a_card_from_any_rank() {
  let services = vec![declared("lab-arena", 2.0, YieldBy::Process), declared("bench", 1.0, YieldBy::Turn)];
  let both: &[&str] = &["a", "b"];
  let (mut master, stand, demands) =
    demanding(both, services, &[("lab-arena", both)], &["lab-arena a", "lab-arena b"]).await;
  let on_a = ask_ranked(&demands, "lab-arena", 0, None, "best-on-a");
  rounds(&mut master, 1).await;
  assert_eq!(granted_on(&demands, on_a), ["a"]);

  // A claim stops the engine under a demand of the best rank, as ever; so does the bench's ticket.
  stand
    .answers
    .lock()
    .unwrap()
    .insert("a".into(), slave(9000.0, store_resident(), Some("trainer"), json!([])));
  std::fs::write(
    stand.dir.join("card-rules.yaml"),
    format!("{RULES}\noccupants:\n  - name: trainer\n    tier: 0\n    claims: true\n"),
  )
  .ok();
  rounds(&mut master, 1).await;
  assert_eq!(adapted(&stand), ["lab-arena stop a"]);
  assert_eq!(demand(&demands, on_a).taken.as_deref(), Some("trainer"));
  assert!(!demand(&demands, on_a).outranked, "a claim is no rank's");

  // b is held by the bench's ticket: a demand of the best rank is granted neither card.
  stand
    .answers
    .lock()
    .unwrap()
    .insert("b".into(), slave(9000.0, store_resident(), None, json!([waiting(2, "bench", None)])));
  let best = ask_ranked(&demands, "lab-arena", 0, None, "best");
  rounds(&mut master, 2).await;
  assert!(granted_on(&demands, best).is_empty());
  assert_eq!(demand(&demands, best).reason.unwrap().code, DemandWait::Held);
  assert_eq!(demand(&demands, on_a).taken.as_deref(), Some("trainer"));
}

#[tokio::test]
async fn a_better_rank_at_its_ceiling_takes_a_card_its_service_uses_and_never_a_further_one() {
  let all: &[&str] = &["a", "b", "c"];
  let services = vec![capped("lab-arena", YieldBy::Process, 2)];
  let (mut master, stand, demands) =
    demanding(all, services, &[("lab-arena", all)], &["lab-arena a", "lab-arena b", "lab-arena c"]).await;
  let first = ask_ranked(&demands, "lab-arena", 1, None, "first");
  let second = ask_ranked(&demands, "lab-arena", 2, None, "second");
  rounds(&mut master, 1).await;
  let high = ask_ranked(&demands, "lab-arena", 0, None, "high");
  rounds(&mut master, 1).await;
  // the worst rank loses, on a card its service uses, and c stays above the ceiling
  assert_eq!(granted_on(&demands, high), ["b"]);
  assert_eq!(granted_on(&demands, first), ["a"]);
  assert_eq!(demand(&demands, second).taken.as_deref(), Some("high"));
  assert!(adapted(&stand).is_empty());
}

#[tokio::test]
async fn a_pool_holds_its_demands_to_its_limit_of_each_service_and_no_other_demand_waits_for_it() {
  let all: &[&str] = &["a", "b", "c"];
  let services = vec![declared("lab-arena", 2.0, YieldBy::Process), declared("coder", 2.0, YieldBy::Process)];
  let (mut master, stand, demands) =
    demanding(all, services, &[("lab-arena", all), ("coder", all)], &["lab-arena a", "lab-arena b", "lab-arena c"])
      .await;
  let first = ask_ranked(&demands, "lab-arena", 1, Some(("mine", 1)), "first");
  let second = ask_ranked(&demands, "lab-arena", 1, Some(("mine", 1)), "second");
  let other = ask_ranked(&demands, "lab-arena", 1, None, "other");
  rounds(&mut master, 1).await;
  assert_eq!(granted_on(&demands, first), ["a"]);
  assert!(granted_on(&demands, second).is_empty());
  let reason = demand(&demands, second).reason.unwrap();
  assert_eq!(reason.code, DemandWait::Pool);
  assert!(
    reason.text.contains("the pool mine uses 1 cards of lab-arena and its limit is 1: a"),
    "{}",
    reason.text
  );
  assert_eq!(granted_on(&demands, other), ["b"], "a demand of no pool is not held back");

  // The limit is of each service: the other engine's demand of the pool has a card of its own.
  let on_coder = ask_ranked(&demands, "coder", 1, Some(("mine", 1)), "on-coder");
  rounds(&mut master, 2).await;
  assert_eq!(granted_on(&demands, on_coder), ["c"]);
  assert_eq!(adapted(&stand), ["lab-arena stop c", "coder start c"]);

  // A card of the pool given back opens the limit to the next of it.
  assert!(demands.book.lock().unwrap().give_back(first));
  rounds(&mut master, 1).await;
  assert_eq!(granted_on(&demands, second), ["a"]);
}

#[tokio::test]
async fn a_demand_asked_within_takes_its_outers_rank_and_pool_and_loses_its_shared_card_with_it() {
  let (mut master, stand, demands) = demanding(
    &["a"],
    vec![declared("lab-arena", 2.0, YieldBy::Process)],
    &[("lab-arena", &["a"])],
    &["lab-arena a"],
  )
  .await;
  let outer = ask_ranked(&demands, "lab-arena", 2, Some(("mine", 1)), "outer");
  rounds(&mut master, 1).await;
  let beside = ask(&demands, "lab-arena", Some(outer), None);
  rounds(&mut master, 1).await;
  assert_eq!(granted_on(&demands, beside), ["a"]);
  let told = demand(&demands, beside);
  assert_eq!((told.rank, told.pool.map(|pool| pool.key)), (2, Some("mine".to_owned())));

  // Both lose the card to a better rank, the card holding every demand on it to the same rank.
  let high = ask_ranked(&demands, "lab-arena", 1, None, "high");
  rounds(&mut master, 1).await;
  assert_eq!(granted_on(&demands, high), ["a"]);
  assert_eq!(demand(&demands, outer).taken.as_deref(), Some("high"));
  assert_eq!(demand(&demands, beside).taken.as_deref(), Some("high"));
  assert!(adapted(&stand).is_empty());
}

#[tokio::test]
async fn a_demand_outranked_uses_no_card_of_its_pool_or_its_ceiling() {
  let (mut master, _stand, demands) = demanding(
    &["a", "b"],
    vec![capped("lab-arena", YieldBy::Process, 1)],
    &[("lab-arena", &["a", "b"])],
    &["lab-arena a", "lab-arena b"],
  )
  .await;
  let board = Arc::new(Mutex::new(Board::default()));
  master.attach(board.clone());
  let low = ask_ranked(&demands, "lab-arena", 2, Some(("mine", 1)), "low");
  rounds(&mut master, 1).await;
  let high = ask_ranked(&demands, "lab-arena", 1, None, "high");
  rounds(&mut master, 1).await;
  assert_eq!(granted_on(&demands, high), ["a"]);
  assert_eq!(shown(&board, &master, "lab-arena").cards_in_use, ["a"]);
  // The next of its pool may have the card its pool no longer holds; the ceiling keeps it waiting on it.
  let next = ask_ranked(&demands, "lab-arena", 2, Some(("mine", 1)), "next");
  rounds(&mut master, 1).await;
  assert_eq!(demand(&demands, next).reason.unwrap().code, DemandWait::Ceiling);
  assert_eq!(demand(&demands, low).reason.unwrap().code, DemandWait::Ceiling);
}

#[tokio::test]
async fn a_return_start_that_keeps_failing_is_the_reason_a_demand_reads_rather_than_starting() {
  let (mut master, stand, demands) = demanding(
    &["a"],
    vec![declared("lab-arena", 2.0, YieldBy::Process)],
    &[("lab-arena", &["a"])],
    &["lab-arena a"],
  )
  .await;
  // the bench's ticket stops it, and once it is gone the start back fails
  *stand.answer.lock().unwrap() = slave(9000.0, store_resident(), None, json!([waiting(2, "bench", None)]));
  *stand.declared.lock().unwrap() =
    vec![declared("lab-arena", 2.0, YieldBy::Process), declared("bench", 1.0, YieldBy::Turn)];
  rounds(&mut master, 2).await;
  assert_eq!(adapted(&stand), ["lab-arena stop a"]);
  stand.refused.lock().unwrap().insert("lab-arena start a".into());
  *stand.answer.lock().unwrap() = slave(9000.0, store_resident(), None, json!([]));
  rounds(&mut master, 3).await;
  let waiting_one = ask(&demands, "lab-arena", None, None);
  rounds(&mut master, 3).await;
  let reason = demand(&demands, waiting_one).reason.unwrap();
  assert_eq!(reason.code, DemandWait::StartFailed);
  assert!(reason.text.contains("the helper is missing"), "{}", reason.text);

  // Once it starts the demand is granted and the failure is forgotten.
  stand.refused.lock().unwrap().clear();
  rounds(&mut master, 3).await;
  assert_eq!(granted_on(&demands, waiting_one), ["a"]);
}

#[tokio::test]
async fn a_lost_nodes_last_reading_counts_toward_no_ceiling_and_the_node_counts_again_once_it_is_read() {
  let all: &[&str] = &["a", "b", "c"];
  let services = vec![capped("lab-arena", YieldBy::Process, 2)];
  let (mut master, stand, demands) =
    demanding(all, services, &[("lab-arena", all)], &["lab-arena a", "lab-arena b", "lab-arena c"]).await;
  let board = Arc::new(Mutex::new(Board::default()));
  master.attach(board.clone());
  let mut using = ticket(1, "lab-arena", true, None);
  using["inUse"] = json!(true);
  let in_use_on_c = slave(9000.0, store_resident(), None, json!([using]));
  stand.answers.lock().unwrap().insert("c".into(), in_use_on_c.clone());
  rounds(&mut master, 1).await;
  assert_eq!(shown(&board, &master, "lab-arena").cards_in_use, ["c"]);

  // c is lost with its turn in use in its last reading: that card counts no more, so both others are open.
  let gone = json!({ "error": "no answer" });
  stand.answers.lock().unwrap().insert("c".into(), gone.clone());
  rounds(&mut master, 1).await;
  assert_eq!(shown(&board, &master, "lab-arena").cards_in_use, Vec::<String>::new());
  let first = ask(&demands, "lab-arena", None, None);
  let second = ask(&demands, "lab-arena", None, None);
  let third = ask(&demands, "lab-arena", None, None);
  rounds(&mut master, 1).await;
  assert_eq!(granted_on(&demands, first), ["a"]);
  assert_eq!(granted_on(&demands, second), ["b"]);
  let reason = demand(&demands, third).reason.unwrap();
  assert_eq!(reason.code, DemandWait::Ceiling);
  assert!(reason.text.contains("uses 2 cards and its ceiling is 2: a, b"), "{}", reason.text);

  // c comes back still holding it: above its ceiling, nothing is taken, and it is given only what it uses.
  stand.answers.lock().unwrap().insert("c".into(), in_use_on_c);
  rounds(&mut master, 1).await;
  assert_eq!(shown(&board, &master, "lab-arena").cards_in_use, ["a", "b", "c"]);
  assert_eq!(granted_on(&demands, first), ["a"]);
  assert_eq!(granted_on(&demands, second), ["b"]);
  assert_eq!(granted_on(&demands, third), ["c"]);
  assert!(adapted(&stand).is_empty());
  assert!(demands.book.lock().unwrap().give_back(third));
  let fourth = ask(&demands, "lab-arena", None, None);
  assert!(demands.book.lock().unwrap().give_back(first));
  rounds(&mut master, 1).await;
  assert_eq!(granted_on(&demands, fourth), ["c"]);

  // A demand's seat is the master's own word: it counts on a node lost with it granted there.
  stand.answers.lock().unwrap().insert("b".into(), gone);
  rounds(&mut master, 1).await;
  assert!(shown(&board, &master, "lab-arena").cards_in_use.contains(&"b".to_owned()));
}

#[tokio::test]
async fn a_lent_card_counts_for_the_service_it_is_lent_to_and_comes_back_once_the_callers_is_below_its_ceiling()
 {
  let all: &[&str] = &["a", "b", "c"];
  let services = vec![
    capped("lab-arena", YieldBy::Process, 1),
    capped("coder", YieldBy::Process, 2),
    declared("voice", 2.0, YieldBy::Process),
  ];
  let (mut master, stand, demands) = demanding(
    all,
    services,
    &[("lab-arena", all), ("coder", all), ("voice", all)],
    &["lab-arena a", "coder b", "voice c"],
  )
  .await;
  let voice = ask(&demands, "voice", None, None);
  let coder = ask(&demands, "coder", None, None);
  let outer = ask(&demands, "lab-arena", None, None);
  rounds(&mut master, 1).await;
  assert_eq!(
    [granted_on(&demands, voice), granted_on(&demands, coder), granted_on(&demands, outer)],
    [["c"], ["b"], ["a"]]
  );

  // Every card in use and the coder below its ceiling: the caller's card is lent to it.
  let inner = ask(&demands, "coder", Some(outer), None);
  rounds(&mut master, 3).await;
  assert_eq!(granted_on(&demands, inner), ["a"]);
  assert_eq!(demand(&demands, outer).state, DemandState::Lent);
  assert_eq!(adapted(&stand), ["lab-arena stop a", "coder start a"]);

  // c freed: the lent card counts for the coder, at its ceiling of two, and no longer for the lab-arena, which
  // takes c though asked after the coder.
  demands.book.lock().unwrap().give_back(voice);
  let later = ask(&demands, "coder", None, None);
  let lab-arena = ask(&demands, "lab-arena", None, None);
  rounds(&mut master, 3).await;
  let waiting = demand(&demands, later);
  assert_eq!(waiting.reason.unwrap().code, DemandWait::Ceiling);
  assert_eq!(granted_on(&demands, lab-arena), ["c"]);
  assert_eq!(adapted(&stand), ["lab-arena stop a", "coder start a", "voice stop c", "lab-arena start c"]);

  // The lent demand given back: the lab-arena uses c, its ceiling, so the caller's card waits on it.
  demands.book.lock().unwrap().give_back(inner);
  rounds(&mut master, 3).await;
  let owed = demand(&demands, outer);
  assert_eq!(owed.state, DemandState::Lent);
  assert_eq!(owed.reason.unwrap().code, DemandWait::Ceiling);
  assert_eq!(adapted(&stand).len(), 4);

  // c given back: the caller's card comes back to it, and the coder waiting takes c.
  demands.book.lock().unwrap().give_back(lab-arena);
  rounds(&mut master, 3).await;
  assert_eq!(
    (demand(&demands, outer).state, granted_on(&demands, outer)),
    (DemandState::Granted, vec!["a".to_owned()])
  );
  assert_eq!(granted_on(&demands, later), ["c"]);
  let moved = adapted(&stand);
  for one in ["coder stop a", "lab-arena start a", "lab-arena stop c", "coder start c"] {
    assert!(moved[4..].iter().any(|done| done == one), "{moved:?}");
  }
}

#[tokio::test]
async fn the_master_makes_no_room_for_a_ticket_that_would_put_its_service_above_its_ceiling() {
  let (mut master, stand) = master_of(slave(1500.0, store_resident(), None, json!([])), &["a", "b"], None);
  *stand.declared.lock().unwrap() = vec![capped("voice", YieldBy::Turn, 1)];
  let mut using = ticket(1, "voice", true, None);
  using["inUse"] = json!(true);
  stand
    .answers
    .lock()
    .unwrap()
    .insert("a".into(), slave(1500.0, store_resident(), None, json!([using])));
  stand
    .answers
    .lock()
    .unwrap()
    .insert("b".into(), slave(1500.0, store_resident(), None, asking(2)));
  master.discover().await;
  master.poll().await;
  master.poll().await;
  assert!(sent(&stand, "evict").is_empty());
  let said = master.state().events;
  assert!(
    said
      .iter()
      .any(|event| event.code == "ticket-held" && event.text.contains("its ceiling is 1: a")),
    "{said:?}"
  );

  // Its turn on a rests: below its ceiling, b is made room on.
  stand
    .answers
    .lock()
    .unwrap()
    .insert("a".into(), slave(1500.0, store_resident(), None, json!([])));
  master.poll().await;
  let evicted = sent(&stand, "evict");
  assert_eq!(evicted.len(), 1, "{evicted:?}");
  assert!(evicted[0].contains("/card-units/model%2Fstore"), "{evicted:?}");
}

#[tokio::test]
async fn a_start_that_never_answers_holds_no_round_and_a_claim_meanwhile_stops_the_occupant_there() {
  let (_, stand) = master_of(slave(9000.0, store_resident(), None, json!([])), &["a"], None);
  *stand.declared.lock().unwrap() = vec![
    declared("lab-arena", 2.0, YieldBy::Process),
    declared("coder", 2.0, YieldBy::Process),
    declared("trainer", 0.5, YieldBy::Turn),
  ];
  placed(&stand, &[("lab-arena", &["a"]), ("coder", &["a"])]);
  stand.running.lock().unwrap().insert("lab-arena a".into());
  // The coder was stopped to yield a, and nothing that made it yield stands: its start is held by the test.
  let yields = json!({ "stopped": [
    { "serviceName": "coder", "nodeName": "a", "reason": "trainer claims the card", "kind": "yield" },
  ] });
  std::fs::write(stand.dir.join("process-yields.json"), yields.to_string()).unwrap();
  let gate = Arc::new(tokio::sync::Notify::new());
  stand.gates.lock().unwrap().insert("coder start a".into(), gate.clone());
  let mut master = Master::new(Arc::new(StandWorld(stand.clone())), stand.dir.join("card-rules.yaml"), 2.0);
  let demands = Arc::new(Demands::default());
  master.attach_demands(demands.clone());
  master.discover().await;
  let reads = || {
    stand
      .commands
      .lock()
      .unwrap()
      .iter()
      .filter(|command| command.ends_with("/card"))
      .count()
  };
  rounds(&mut master, 1).await;
  let coder = ask(&demands, "coder", None, None);

  // Every round reads the node while the start hangs, the start is run once, and a demand of the coder waits
  // on it, starting there.
  let before = reads();
  rounds(&mut master, 5).await;
  assert_eq!(reads(), before + 5);
  assert_eq!(adapted(&stand), ["coder start a"]);
  let waiting = demand(&demands, coder);
  assert_eq!(waiting.state, DemandState::Waiting);
  assert_eq!(waiting.reason.unwrap().code, DemandWait::Starting);

  // A claim of a lower tier comes while it hangs: the lab-arena standing there is stopped for it.
  *stand.answer.lock().unwrap() = slave(9000.0, store_resident(), Some("trainer"), json!([]));
  rounds(&mut master, 2).await;
  assert_eq!(reads(), before + 7);
  assert_eq!(adapted(&stand), ["coder start a", "lab-arena stop a"]);
  assert!(said(&master, "occupant-stopped", "lab-arena"));
  assert!(!said(&master, "occupant-started", "coder"));

  // The start answers at last: the round after takes it, and the coder, standing now, yields to the claim
  // as well.
  gate.notify_one();
  rounds(&mut master, 3).await;
  assert!(said(&master, "occupant-started", "coder"));
  assert!(said(&master, "occupant-stopped", "coder"));
  assert_eq!(adapted(&stand), ["coder start a", "lab-arena stop a", "coder stop a"]);
}
