//! The master's wire contract: started as a process on a free port over a world of stand-ins (`common`), and
//! asked through its API as a reader asks it, every answer held to docs/openapi.json.

use crate::integration::common;

use std::time::Duration;

use bmo_arbiter_client::events::EventKind;
use common::{World, card, codes, connected, events, nodes, slave, unit, waiting};
use reqwest::Method;
use serde_json::{Value, json};

use crate::support::contract::holds_to_contract;

/// A route asked once, its answer held to the document, its status and its JSON.
async fn asked(world: &World, method: Method, path: &str, body: Option<&str>) -> (u16, Value) {
  let (status, said) = world.send(method.clone(), path, body).await.expect("the master answers");
  let said: Value = if said.is_empty() { Value::Null } else { serde_json::from_str(&said).unwrap() };
  holds_to_contract(method.as_str(), path, status, &said);
  (status, said)
}

#[tokio::test]
async fn reads_the_nodes_its_groups_choose_and_shows_each_card() {
  let world = World::new(json!({ "a": "model", "desk": null }), &[("a", slave(json!({})))]).start();
  let state = world.state_where("node a read", |state| connected(state, "a")).await;
  assert_eq!(
    state["groups"],
    json!([{ "name": "compute", "pool": true, "nodeNames": ["a"], "totalMiB": 11875.0, "freeMiB": 1300.0 }])
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
      { "unit": "model/*", "serviceName": "model", "kind": "resident", "vramMiB": null },
      { "unit": "voice/*", "serviceName": "voice", "kind": "on-demand", "vramMiB": 6000.0 },
    ])
  );
  assert_eq!(nodes(&state)[0]["version"], "abc123");

  assert_eq!(asked(&world, Method::GET, "/cluster", None).await.0, 200);
  let (status, health) = asked(&world, Method::GET, "/health", None).await;
  assert_eq!(
    (status, health["ready"].clone(), health["reason"].clone()),
    (200, json!(true), Value::Null)
  );
  for (method, path) in [(Method::GET, "/nowhere"), (Method::GET, "/state"), (Method::POST, "/cluster")] {
    let (status, body) = asked(&world, method, path, None).await;
    assert_eq!((status, body["code"].as_str()), (404, Some("no-route")), "{path}: {body}");
  }
}

#[tokio::test]
async fn the_cluster_streams_as_numbered_events_resumed_within_the_window_or_read_as_a_page() {
  let world = World::new(json!({ "a": "model" }), &[("a", slave(json!({})))]).start();
  world.state_where("node a read", |state| connected(state, "a")).await;
  let event = world.first_event("cluster.changed").await;
  assert_eq!(
    (event.kind, event.data["nodes"][0]["name"].clone()),
    (EventKind::ClusterChanged, json!("a"))
  );
  holds_to_contract("get", "/cluster", 200, &event.data);

  // the same events as a page to a reader that polls, resumed after the one read
  let polled = reqwest::Client::new()
    .get(format!("{}/events?after={}&limit=1", world.base(), event.id - 1))
    .header("accept", "application/json")
    .send()
    .await
    .unwrap();
  let polled: Value = polled.json().await.unwrap();
  holds_to_contract("get", "/events", 200, &polled);
  assert_eq!(polled["items"][0]["id"], event.id);
  // a number this run never gave is outside the window
  let (status, gone) = asked(&world, Method::GET, "/events?after=1", None).await;
  assert_eq!((status, gone["code"].as_str()), (410, Some("gone")));
  let (status, wrong) = asked(&world, Method::GET, "/events?kinds=nothing.changed", None).await;
  assert_eq!((status, wrong["code"].as_str()), (400, Some("bad-request")));
}

#[tokio::test]
async fn a_cards_processes_come_through_as_its_slave_read_them_and_none_from_a_slave_that_does_not_say_them()
 {
  let processes = json!([{ "pid": 41, "name": "rpc-server", "serviceName": "model", "usedMiB": 9100.0 }, { "pid": 7, "name": "Xorg", "serviceName": null, "usedMiB": 120.0 }]);
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
async fn the_usage_reads_each_cards_use_holder_engine_and_waiters() {
  let holding = json!({ "id": 7, "serviceName": "model", "turn": true, "inUse": true, "resting": false,
    "giveWay": false, "giveWayFor": null, "position": null, "waitMs": null, "claimantName": null, "waitedMs": 0,
    "need": null, "ref": null, "rest": false, "keep": null });
  let world = World::new(
    json!({ "a": "model" }),
    &[(
      "a",
      slave(json!({ "free": 500.0, "utilizationPercent": 42,
        "queue": [holding, waiting(8, "voice", "voice/large")] })),
    )],
  )
  .start();
  world.state_where("node a read", |state| connected(state, "a")).await;
  let (status, usage) = asked(&world, Method::GET, "/usage", None).await;
  assert_eq!(status, 200);
  let node = &usage["nodes"][0];
  assert_eq!((node["nodeName"].as_str(), node["connected"].as_bool()), (Some("a"), Some(true)));
  let card = &node["cards"][0];
  assert_eq!(card["utilizationPercent"], 42);
  assert_eq!(
    (
      card["turn"]["serviceName"].as_str(),
      card["turn"]["ticketId"].as_i64(),
      card["turn"]["inUse"].as_bool()
    ),
    (Some("model"), Some(7), Some(true))
  );
  let units: Vec<&str> = card["units"]
    .as_array()
    .unwrap()
    .iter()
    .map(|unit| unit["name"].as_str().unwrap())
    .collect();
  assert_eq!(units, ["model/weights@a", "model/store@a"]);
  assert_eq!(
    (card["waiters"][0]["serviceName"].as_str(), card["waiters"][0]["position"].as_i64()),
    (Some("voice"), Some(1))
  );
  // No process, pid or name of a process stands in the owner's reading.
  assert!(card.get("processes").is_none(), "{card}");
}

#[tokio::test]
async fn the_usage_says_a_node_the_master_does_not_reach() {
  let world = World::new(json!({ "a": "model" }), &[]).start();
  world
    .state_where("the node lost", |state| {
      nodes(state).iter().any(|node| node["name"] == "a" && node["link"]["state"] == "lost")
    })
    .await;
  let (status, usage) = asked(&world, Method::GET, "/usage", None).await;
  assert_eq!(status, 200);
  let node = &usage["nodes"][0];
  assert_eq!((node["nodeName"].as_str(), node["connected"].as_bool()), (Some("a"), Some(false)));
  assert!(node["error"].as_str().is_some_and(|error| !error.is_empty()), "{node}");
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
  // The node's own link events, newest first; the drift the placement names at its start may fall between.
  let link: Vec<(String, String)> = codes(&state)
    .into_iter()
    .zip(events(&state))
    .filter(|(code, _)| code.starts_with("node-"))
    .collect();
  assert_eq!(link[0], ("node-reconnected".to_owned(), "the master reaches the node again".to_owned()));
  assert_eq!(link[1].0, "node-lost");
  assert!(link[1].1.contains("lost the node"), "{link:?}");
}

#[tokio::test]
async fn a_ticket_asking_for_a_unit_has_the_cheapest_units_unloaded_and_loaded_back_once_it_is_gone() {
  let queue = json!([waiting(1, "voice", "voice/large")]);
  // 6,000 MiB asked with 1,500 free: the store (4,608) alone makes room, the weights (4,000) do not.
  let world =
    World::new(json!({ "a": "model" }), &[("a", slave(json!({ "queue": queue, "free": 1500 })))]).start();
  let evicted = world
    .command_where("an unload", |command| {
      command.contains("/card-units/") && command.contains(r#""evicted""#)
    })
    .await;
  assert!(evicted.contains("/card-units/model%2Fstore"), "{evicted}");

  // The slave unloads it as asked, keeping the reason it was given, and the ticket goes.
  let body = evicted.split_once("-d '").unwrap().1.split_once("' http").unwrap().0;
  let reason = serde_json::from_str::<Value>(body).unwrap()["reason"].clone();
  let store = unit("model/store", "evicted", 4608.0, reason.as_str());
  world.set_slave("a", Some(slave(json!({ "store": store, "free": 6108 }))));
  let restored = world
    .command_where("a restore", |command| {
      command.contains("/card-units/") && command.contains(r#""resident""#)
    })
    .await;
  assert!(restored.contains("/card-units/model%2Fstore"), "{restored}");
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
  let state = world
    .state_where("the claim", |state| card(state)["claim"]["claimantName"] == "tester")
    .await;
  assert_eq!(events(&state)[0], "tester claimed the card");
  assert_eq!(codes(&state)[0], "card-claimed");
}

#[tokio::test]
async fn a_service_hands_in_its_units_description_which_the_master_keeps_and_shows_and_a_bad_one_is_refused()
 {
  let world = World::new(json!({ "a": "model" }), &[("a", slave(json!({})))]).start();
  world.state_where("node a read", |state| connected(state, "a")).await;
  let description = json!({ "units": { "model/weights@a": { "blocks": { "from": 0, "to": 17 }, "output": true, "mtp": false, "fullAttention": null } } });

  let (status, body) =
    asked(&world, Method::PUT, "/descriptions/model", Some(&description.to_string())).await;

  assert_eq!((status, &body), (201, &description));
  let kept: Value = serde_json::from_str(&world.read("data/placements/card-units/model.json")).unwrap();
  assert_eq!(kept, description);
  assert_eq!(
    asked(&world, Method::PUT, "/descriptions/model", Some(&description.to_string()))
      .await
      .0,
    200
  );
  world
    .state_where("the weights' detail", |state| {
      card(state)["units"][0]["detail"]["blocks"] == json!({ "from": 0, "to": 17 })
    })
    .await;
  for (path, body, answer) in [
    ("/descriptions/Bad_Name", description.to_string(), (422, "name-rule")),
    ("/descriptions/model", "{ not json".to_owned(), (400, "bad-request")),
    ("/descriptions/voice", description.to_string(), (422, "bad-field")),
  ] {
    let (status, said) = asked(&world, Method::PUT, path, Some(&body)).await;
    assert_eq!((status, said["code"].as_str().unwrap()), answer, "{path}: {said}");
  }
  assert!(!world.root().join("data/placements/card-units/voice.json").exists());
}

#[tokio::test]
async fn the_node_services_nodes_are_read_in_its_lists_order() {
  let world =
    World::new(json!({ "a": "model", "b": null }), &[("a", slave(json!({}))), ("b", slave(json!({})))])
      .start();
  let state = world
    .state_where("both nodes", |state| connected(state, "a") && connected(state, "b"))
    .await;
  let names: Vec<Value> = nodes(&state).iter().map(|node| node["name"].clone()).collect();
  assert_eq!(names, [json!("a"), json!("b")]);
}

#[tokio::test]
async fn a_placement_is_answered_as_queued_and_followed_on_the_member_and_the_stream() {
  let world = World::new(json!({ "a": "model" }), &[("a", slave(json!({})))]).start();
  world.state_where("node a read", |state| connected(state, "a")).await;
  let ask = json!({ "kind": "place", "serviceName": "absent", "nodeName": "a" }).to_string();

  let (status, made) = asked(&world, Method::POST, "/placements", Some(&ask)).await;

  assert_eq!(
    (status, made["state"].clone(), made["lines"].clone()),
    (201, json!("queued"), json!([]))
  );
  let id = made["id"].as_i64().unwrap();
  let until = std::time::Instant::now() + std::time::Duration::from_secs(30);
  let ended = loop {
    let (status, now) = asked(&world, Method::GET, &format!("/placements/{id}"), None).await;
    assert_eq!(status, 200);
    if now["state"] == "failed" || now["state"] == "done" {
      break now;
    }
    assert!(std::time::Instant::now() < until, "the placement never ended: {now}");
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
  };
  // no service is declared in this world, so the placement fails and says why
  assert_eq!(ended["state"], "failed");
  assert!(ended["failure"].as_str().is_some_and(|why| !why.is_empty()), "{ended}");
  assert!(ended["endedAt"].as_i64().is_some(), "{ended}");
  let (status, missing) = asked(&world, Method::GET, &format!("/placements/{}", id + 1000), None).await;
  assert_eq!((status, missing["code"].as_str()), (404, Some("not-found")));
  let (status, wrong) = asked(&world, Method::POST, "/placements", Some(r#"{"kind":"move"}"#)).await;
  assert_eq!((status, wrong["code"].as_str()), (400, Some("bad-request")));
}

#[tokio::test]
async fn a_master_with_a_token_answers_every_route_only_to_a_caller_holding_it() {
  let world = World::new(json!({ "a": "model" }), &[("a", slave(json!({})))])
    .with_token("secret")
    .start();
  for path in ["/health", "/cluster"] {
    let (status, said) = asked(&world, Method::GET, path, None).await;
    assert_eq!((status, said["code"].as_str()), (401, Some("unauthorized")), "{path}");
  }
  let held = reqwest::Client::new()
    .get(format!("{}/cluster", world.base()))
    .bearer_auth("secret")
    .send()
    .await
    .unwrap();
  assert_eq!(held.status(), 200);
}

#[tokio::test]
async fn a_demand_is_granted_waits_in_line_while_its_card_is_in_use_and_is_given_back_by_its_caller() {
  let demo = "service = \"demo\"\ncard_mib = 6000\npriority = 40\nspread = \"many\"\nalone = true\nyield_by = \"process\"\nadapter = [\"bun\", \"adapter.ts\"]\n";
  let world = World::new(json!({ "a": "demo" }), &[("a", slave(json!({})))])
    .with_service(demo, &["a"])
    .start();
  world
    .state_where("demo adopted on a", |state| {
      connected(state, "a")
        && state["services"]
          .as_array()
          .is_some_and(|all| all.iter().any(|one| one["nodeNames"] == json!(["a"])))
    })
    .await;
  let (status, refused) =
    asked(&world, Method::POST, "/demands", Some(r#"{"serviceName":"absent"}"#)).await;
  assert_eq!((status, refused["code"].as_str()), (422, Some("no-place")));
  let (status, refused) =
    asked(&world, Method::POST, "/demands", Some(r#"{"serviceName":"demo","within":7}"#)).await;
  assert_eq!((status, refused["code"].as_str()), (404, Some("not-found")));

  for pool in [r#"{"key":"","cards":1}"#, r#"{"key":"k","cards":0}"#] {
    let body = format!(r#"{{"serviceName":"demo","pool":{pool}}}"#);
    let (status, refused) = asked(&world, Method::POST, "/demands", Some(&body)).await;
    assert_eq!((status, refused["code"].as_str()), (400, Some("bad-request")), "{pool}");
  }

  // The service is brought up on a for the first; the answer is held open until it is granted.
  let (status, first) = asked(
    &world,
    Method::POST,
    "/demands",
    Some(r#"{"serviceName":"demo","workKey":"k","waitMs":10000}"#),
  )
  .await;
  let first_id = first["id"].as_i64().unwrap();
  let first = if status == 201 {
    first
  } else {
    asked(&world, Method::GET, &format!("/demands/{first_id}?waitMs=10000"), None).await.1
  };
  assert_eq!(
    (first["state"].as_str(), first["cards"][0]["nodeName"].as_str()),
    (Some("granted"), Some("a"))
  );
  assert!(std::fs::read_to_string(world.root().join("adapter.log")).unwrap().contains("start a"));
  // Granted, it is answered at once however long the read asks to be held.
  let began = std::time::Instant::now();
  let (status, again) =
    asked(&world, Method::GET, &format!("/demands/{first_id}?waitMs=10000"), None).await;
  assert_eq!((status, again["state"].as_str()), (200, Some("granted")));
  assert!(began.elapsed() < Duration::from_secs(1), "{:?}", began.elapsed());
  assert_eq!((again["rank"].as_u64(), again["pool"].as_str()), (Some(1), None));
  // A caller standing on the card asks to be held until it moves.
  let began = std::time::Instant::now();
  let (status, watched) =
    asked(&world, Method::GET, &format!("/demands/{first_id}?waitMs=300&watch=true"), None).await;
  assert_eq!((status, watched["state"].as_str()), (200, Some("granted")));
  assert!(began.elapsed() >= Duration::from_millis(300), "{:?}", began.elapsed());

  // The one card is in use: the next waits, its answer held only as long as asked, and it stands.
  let (status, second) = asked(&world, Method::POST, "/demands", Some(r#"{"serviceName":"demo"}"#)).await;
  assert_eq!((status, second["state"].as_str()), (202, Some("waiting")));
  let second_id = second["id"].as_i64().unwrap();
  let began = std::time::Instant::now();
  let (status, still) = asked(&world, Method::GET, &format!("/demands/{second_id}?waitMs=300"), None).await;
  assert!(began.elapsed() >= Duration::from_millis(300), "{:?}", began.elapsed());
  assert_eq!(
    (status, still["state"].as_str(), still["reason"]["code"].as_str()),
    (200, Some("waiting"), Some("in-use"))
  );
  let (status, all) = asked(&world, Method::GET, "/demands", None).await;
  assert_eq!((status, all["items"].as_array().map(Vec::len)), (200, Some(2)));

  // Given back by its caller, the card goes to the one waiting.
  assert_eq!(asked(&world, Method::DELETE, &format!("/demands/{first_id}"), None).await.0, 204);
  let (status, granted) =
    asked(&world, Method::GET, &format!("/demands/{second_id}?waitMs=10000"), None).await;
  assert_eq!((status, granted["state"].as_str()), (200, Some("granted")));
  let (status, gone) = asked(&world, Method::DELETE, &format!("/demands/{first_id}"), None).await;
  assert_eq!((status, gone["code"].as_str()), (404, Some("not-found")));
  let state = world
    .state_where("the demand shown", |state| {
      state["demands"].as_array().is_some_and(|all| all.len() == 1)
    })
    .await;
  assert_eq!(state["services"][0]["instances"][0]["demands"], 1);
}
