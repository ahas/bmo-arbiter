//! The hosted tool that reads the card master for a supervisor (docs/api.md, 「Usage」): its manifest, its
//! owner's reading, and its answer when the master is up or down.

use crate::integration::common::{World, connected, slave};

use std::path::Path;

use bmo_arbiter_client::card::UnitState;
use bmo_arbiter_client::cluster::CardTurn;
use bmo_arbiter_client::usage::{Usage, UsageCard, UsageNode, UsageUnit, UsageWaiter};
use gpu_tool::{ENTRY, Handed, answer_with, lines, usage_at};
use serde_json::{Value, json};

fn manifest() -> toml::Table {
  let text = std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("tools/gpu/tool.toml"))
    .expect("the manifest reads");
  text.parse().expect("the manifest is TOML")
}

fn asking() -> Handed {
  Handed {
    entry: ENTRY.to_owned(),
    arguments: json!({}),
  }
}

#[test]
fn the_manifest_declares_the_entry_the_executable_answers() {
  let manifest = manifest();
  let entries = manifest["entry"].as_array().expect("one entry");

  assert_eq!(entries.len(), 1);
  assert_eq!(
    (
      entries[0]["namespace"].as_str(),
      entries[0]["name"].as_str(),
      entries[0]["hints"]["read_only"].as_bool()
    ),
    (Some("bmo"), Some("gpu"), Some(true))
  );
  assert_eq!(
    format!("{}/{}", entries[0]["namespace"].as_str().unwrap(), entries[0]["name"].as_str().unwrap()),
    ENTRY
  );
}

#[test]
fn every_schema_the_manifest_names_is_an_object() {
  let manifest = manifest();
  let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tools/gpu");

  for entry in manifest["entry"].as_array().unwrap() {
    for key in ["input", "output"] {
      let Some(file) = entry.get(key).and_then(toml::Value::as_str) else { continue };
      let text = std::fs::read_to_string(dir.join(file)).unwrap_or_else(|why| panic!("{file}: {why}"));
      let schema: Value = serde_json::from_str(&text).unwrap();

      assert!(schema.is_object(), "{file} is no schema object");
      assert!(schema.get("type").and_then(Value::as_str) == Some("object"), "{file} is no object");
    }
  }
}

#[test]
fn the_reading_puts_each_cards_use_and_holder_first() {
  let usage = Usage {
    at: 1000,
    nodes: vec![UsageNode {
      node_name: "n2".to_owned(),
      connected: true,
      read_at: Some(900),
      error: None,
      cards: vec![UsageCard {
        index: 0,
        name: Some("RTX 4090".to_owned()),
        total_mib: Some(24564.0),
        used_mib: Some(21000.0),
        free_mib: Some(3564.0),
        utilization_percent: Some(87),
        turn: Some(CardTurn {
          service_name: "model".to_owned(),
          ticket_id: 123,
          waited_ms: 0,
          in_use: Some(true),
        }),
        claim: None,
        units: vec![UsageUnit {
          name: "model/weights@n2".to_owned(),
          service_name: "model".to_owned(),
          part: "weights".to_owned(),
          vram_mib: 18000.0,
          ram_mib: 0.0,
          state: UnitState::Resident,
        }],
        waiters: vec![UsageWaiter {
          ticket_id: 124,
          service_name: "voice".to_owned(),
          position: Some(1),
          waited_ms: 1000,
          unit_name: Some("voice/large".to_owned()),
          vram_mib: Some(6000.0),
        }],
      }],
    }],
  };

  let said = lines(&usage);

  assert_eq!(said[0], "1 card on 1 node: 1 in use, 0 idle, 1 waiting.");
  assert_eq!(
    said[1],
    "n2 card 0 (RTX 4090): 87% used, 21000 of 24564 MiB, 3564 free; turn model (ticket 123) in use"
  );
  assert_eq!(said[2], "  engine: model/weights@n2 18000 MiB resident");
  assert_eq!(said[3], "  waiting: voice ticket 124, position 1, waited 1000 ms, voice/large 6000 MiB");
}

#[tokio::test]
async fn the_tool_answers_the_masters_cards_when_it_is_up() {
  let holding = json!({ "id": 7, "serviceName": "model", "turn": true, "inUse": true, "resting": false,
    "giveWay": false, "giveWayFor": null, "position": null, "waitMs": null, "claimantName": null, "waitedMs": 0,
    "need": null, "ref": null, "rest": false, "keep": null });
  let world = World::new(
    json!({ "a": "model" }),
    &[("a", slave(json!({ "free": 500.0, "utilizationPercent": 42, "queue": [holding] })))],
  )
  .start();
  world.state_where("node a read", |state| connected(state, "a")).await;

  let usage = usage_at(&world.base(), None).await.expect("the master answers");
  assert_eq!(usage.nodes[0].cards[0].utilization_percent, Some(42));

  let answer = answer_with(&world.base(), None, &asking()).await;
  assert!(!answer.is_error, "{}", answer.text());
  assert!(answer.text().contains("42% used"), "{}", answer.text());
  assert!(answer.text().contains("turn model"), "{}", answer.text());
  assert_eq!(answer.structured.as_ref().unwrap()["nodes"][0]["cards"][0]["utilizationPercent"], 42);
}

#[tokio::test]
async fn the_tool_says_the_master_does_not_answer_when_it_is_down() {
  let mut world = World::new(json!({ "a": "model" }), &[("a", slave(json!({})))]).start();
  world.stop_master();

  let answer = answer_with(&world.base(), None, &asking()).await;

  assert!(answer.is_error, "{}", answer.text());
  assert!(answer.structured.is_none(), "{answer:?}");
  assert!(answer.text().contains("does not answer"), "{}", answer.text());
}
