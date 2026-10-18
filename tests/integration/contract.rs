//! The card master's wire contract: started as a process on a free port over a world of stand-ins
//! (`common`), and asked through its state API as the console asks it.

use crate::common;

use std::time::Duration;

use common::{World, card, codes, connected, events, nodes, slave};
use serde_json::{Value, json};

#[tokio::test]
async fn reads_the_nodes_its_groups_choose_and_shows_each_card() {
  let world = World::new(json!({ "a": "model", "desk": null }), &[("a", slave(json!({})))]).start();
  let state = world.state_where("node a read", |state| connected(state, "a")).await;
  assert_eq!(
    state["groups"],
    json!([{ "name": "compute", "pool": true, "nodes": ["a"], "totalMiB": 11875, "freeMiB": 1300 }])
  );
  assert_eq!(nodes(&state).iter().map(|node| node["name"].clone()).collect::<Vec<_>>(), [json!("a")]);
  let units: Vec<(Value, Value)> = card(&state)["units"]
    .as_array()
    .unwrap()
    .iter()
    .map(|unit| (unit["name"].clone(), unit["kind"].clone()))
    .collect();
  assert_eq!(
    units,
    [
      (json!("model/weights@a"), json!("resident")),
      (json!("model/store@a"), json!("resident"))
    ]
  );
  assert_eq!(
    state["declared"],
    json!([
      { "unit": "model/*", "service": "model", "kind": "resident", "vramMiB": null },
      { "unit": "voice/*", "service": "voice", "kind": "on-demand", "vramMiB": 6000 },
    ])
  );
  assert_eq!(nodes(&state)[0]["version"], "abc123");

  let (status, body) = world.get("/nowhere").await.unwrap();
  assert_eq!(
    (status, serde_json::from_str::<Value>(&body).unwrap()),
    (404, json!({ "error": "no such path" }))
  );
}

#[tokio::test]
async fn the_picture_streams_as_server_sent_events() {
  let world = World::new(json!({ "a": "model" }), &[("a", slave(json!({})))]).start();
  world.state_where("node a read", |state| connected(state, "a")).await;
  let (kind, event, picture) = world.first_event().await;
  assert_eq!(kind, "text/event-stream");
  assert_eq!(event.as_deref(), Some("state"));
  assert_eq!(picture["nodes"][0]["name"], "a");
}

#[tokio::test]
async fn a_cards_processes_come_through_as_its_slave_read_them_and_none_from_a_slave_that_does_not_say_them()
 {
  let processes = json!([{ "pid": 41, "name": "rpc-server", "service": "model", "usedMiB": 9100 }, { "pid": 7, "name": "Xorg", "service": null, "usedMiB": 120 }]);
  let world =
    World::new(json!({ "a": "model" }), &[("a", slave(json!({ "processes": processes })))]).start();
  let state = world
    .state_where("the processes", |state| connected(state, "a") && !card(state)["processes"].is_null())
    .await;
  assert_eq!(card(&state)["processes"], processes);
  world.set_slave("a", Some(slave(json!({}))));
  world
    .state_where("no processes", |state| connected(state, "a") && card(state)["processes"].is_null())
    .await;
}

#[tokio::test]
async fn what_a_service_says_of_its_units_stands_beside_them_read_again_when_its_file_changes_and_a_bad_file_is_said()
 {
  let world = World::new(json!({ "a": "model" }), &[("a", slave(json!({})))]);
  let folder = world.root().join("data/placements/card-units");
  let block = |unit: &str, mtp: bool| json!({ "units": { unit: { "blocks": { "from": 0, "to": 17 }, "output": false, "mtp": mtp, "fullAttention": if mtp { json!(4) } else { Value::Null } } } });
  std::fs::write(folder.join("model.json"), block("model/weights@a", false).to_string()).unwrap();
  std::fs::write(folder.join("voice.json"), "{ not json").unwrap();
  let world = world.start();
  let blocks = |state: &Value| -> Vec<(Value, Value)> {
    card(state)["units"]
      .as_array()
      .unwrap()
      .iter()
      .map(|unit| (unit["name"].clone(), unit["detail"]["blocks"].clone()))
      .collect()
  };
  let state = world
    .state_where("the weights' blocks", |state| {
      connected(state, "a") && blocks(state)[0].1 == json!({ "from": 0, "to": 17 })
    })
    .await;
  assert_eq!(blocks(&state)[1].1, Value::Null);
  assert!(
    events(&state)
      .iter()
      .any(|text| text.starts_with("a unit description does not read: voice.json")),
    "{:?}",
    events(&state)
  );

  let file = folder.join("model.json");
  std::fs::write(&file, block("model/store@a", true).to_string()).unwrap();
  std::fs::File::options()
    .write(true)
    .open(&file)
    .unwrap()
    .set_modified(std::time::UNIX_EPOCH + Duration::from_secs(5))
    .unwrap();
  world
    .state_where("the store's blocks", |state| {
      blocks(state)
        == [
          (json!("model/weights@a"), Value::Null),
          (json!("model/store@a"), json!({ "from": 0, "to": 17 })),
        ]
    })
    .await;
}

#[tokio::test]
async fn a_slave_that_stops_answering_is_lost_and_coming_back_is_an_event() {
  let world = World::new(json!({ "a": "model" }), &[("a", slave(json!({})))]).start();
  world.state_where("node a read", |state| connected(state, "a")).await;
  world.set_slave("a", None);
  world
    .state_where("node a lost", |state| nodes(state)[0]["link"]["state"] == "lost")
    .await;
  world.set_slave("a", Some(slave(json!({}))));
  let state = world.state_where("node a back", |state| connected(state, "a")).await;
  let said = events(&state);
  assert_eq!(said[0], "the master reaches the node again");
  assert!(said[1].contains("lost the node"), "{said:?}");
  assert_eq!(codes(&state)[..2], ["node_reconnected", "node_lost"]);
}

#[tokio::test]
async fn a_ticket_asking_for_a_unit_has_the_cheapest_units_unloaded_and_loaded_back_once_it_is_gone() {
  let queue = json!([{ "ticket": "t1", "service": "voice", "turn": false, "position": 1, "waitedS": 1, "need": { "unit": "voice/large" } }]);
  // 6,000 MiB asked with 1,500 free: the store (4,608) alone makes room, the weights (4,000) do not.
  let world =
    World::new(json!({ "a": "model" }), &[("a", slave(json!({ "queue": queue, "free": 1500 })))]).start();
  let evicted = world.command_where("an unload", |command| command.contains("/units/evict")).await;
  assert!(evicted.contains(r#""units":["model/store"]"#), "{evicted}");

  // The slave unloads it as asked, keeping the reason it was given, and the ticket goes.
  let body = evicted.split_once("-d '").unwrap().1.split_once("' http").unwrap().0;
  let reason = serde_json::from_str::<Value>(body).unwrap()["reason"].clone();
  let store = json!({ "name": "model/store", "service": "model", "part": "store", "evictable": true, "state": "evicted", "vramMiB": 4608, "since": 2, "reason": reason });
  world.set_slave("a", Some(slave(json!({ "store": store, "free": 6108 }))));
  let restored = world.command_where("a restore", |command| command.contains("/units/restore")).await;
  assert!(restored.contains(r#""units":["model/store"]"#), "{restored}");
  world
    .state_where("the restore said", |state| {
      events(state).iter().any(|text| text.contains("loaded back"))
    })
    .await;
}

#[tokio::test]
async fn a_claimant_taking_the_card_is_shown_and_said_by_its_own_name() {
  let world = World::new(json!({ "a": "model" }), &[("a", slave(json!({})))]).start();
  world.state_where("node a read", |state| connected(state, "a")).await;
  world.set_slave("a", Some(slave(json!({ "claim": "tester" }))));
  let state = world.state_where("the claim", |state| card(state)["claim"]["by"] == "tester").await;
  assert_eq!(events(&state)[0], "tester claimed the card");
  assert_eq!(codes(&state)[0], "card_claimed");
}

#[tokio::test]
async fn a_service_hands_in_its_units_description_which_the_master_keeps_and_shows_and_a_bad_one_is_refused()
 {
  let world = World::new(json!({ "a": "model" }), &[("a", slave(json!({})))]).start();
  world.state_where("node a read", |state| connected(state, "a")).await;
  let description = json!({ "units": { "model/weights@a": { "blocks": { "from": 0, "to": 17 }, "output": true, "mtp": false, "fullAttention": null } } });

  let (status, body) = world.put("/descriptions/model", &description.to_string()).await.unwrap();

  assert_eq!(status, 204, "{body}");
  let kept: Value = serde_json::from_str(&world.read("data/placements/card-units/model.json")).unwrap();
  assert_eq!(kept, description);
  world
    .state_where("the weights' detail", |state| {
      card(state)["units"][0]["detail"]["blocks"] == json!({ "from": 0, "to": 17 })
    })
    .await;
  for (path, body, code) in [
    ("/descriptions/Bad_Name", description.to_string(), "service_name_invalid"),
    ("/descriptions/model", "{ not json".to_owned(), "description_invalid"),
    ("/descriptions/voice", description.to_string(), "description_invalid"),
  ] {
    let (status, said) = world.put(path, &body).await.unwrap();
    let said: Value = serde_json::from_str(&said).unwrap();
    assert_eq!((status, said["code"].as_str()), (400, Some(code)), "{path}: {said}");
  }
  assert!(!world.root().join("data/placements/card-units/voice.json").exists());
}

#[tokio::test]
async fn the_node_lists_names_are_read_from_its_listing() {
  let world =
    World::new(json!({ "a": "model", "b": null }), &[("a", slave(json!({}))), ("b", slave(json!({})))])
      .start();
  let state = world
    .state_where("both nodes", |state| connected(state, "a") && connected(state, "b"))
    .await;
  let names: Vec<Value> = nodes(&state).iter().map(|node| node["name"].clone()).collect();
  assert_eq!(names, [json!("a"), json!("b")]);
}
