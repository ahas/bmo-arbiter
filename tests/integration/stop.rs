//! A stop of the master and its next start (docs/api.md, 「A stop」): the master's binary sent the signal a
//! stop of its unit sends while calls, a stream and a placement are in hand, and started again over the same
//! world on the same port, as a restart of its unit does.

use crate::integration::common;

use std::time::{Duration, Instant};

use common::{World, connected, slave};
use reqwest::Method;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::support::contract::holds_to_contract;

const DEMO: &str = "service = \"demo\"\ncard_mib = 6000\npriority = 40\nspread = \"one\"\nalone = true\nadapter = [\"bun\", \"adapter.ts\"]\n";

const DEMANDED: &str = "service = \"demo\"\ncard_mib = 6000\npriority = 40\nspread = \"many\"\nalone = true\nyield_by = \"process\"\nadapter = [\"bun\", \"adapter.ts\"]\n";

/// A route asked once, its status and its JSON.
async fn asked(world: &World, method: Method, path: &str, body: Option<&str>) -> (u16, Value) {
  let (status, said) = world.send(method, path, body).await.expect("the master answers");
  let said: Value = if said.is_empty() { Value::Null } else { serde_json::from_str(&said).unwrap() };
  (status, said)
}

/// One answer read off a bare connection: its status, its headers lower-cased, and its body, read to the
/// length it names.
async fn answer_on(connection: &mut TcpStream) -> (u16, Vec<(String, String)>, Vec<u8>) {
  let mut read = Vec::new();
  let mut chunk = [0u8; 4096];
  loop {
    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut answer = httparse::Response::new(&mut headers);
    if let httparse::Status::Complete(head) = answer.parse(&read).expect("the answer reads as HTTP") {
      let status = answer.code.expect("a complete head has a status");
      let named: Vec<(String, String)> = answer
        .headers
        .iter()
        .map(|header| {
          (header.name.to_ascii_lowercase(), String::from_utf8_lossy(header.value).into_owned())
        })
        .collect();
      let length = named
        .iter()
        .find(|(name, _)| name == "content-length")
        .map_or(0, |(_, value)| value.parse::<usize>().unwrap());
      while read.len() < head + length {
        let got = connection.read(&mut chunk).await.unwrap();
        assert!(got > 0, "the connection closed inside the answer's body");
        read.extend_from_slice(&chunk[..got]);
      }
      return (status, named, read[head..head + length].to_vec());
    }
    let got = tokio::time::timeout(Duration::from_secs(30), connection.read(&mut chunk))
      .await
      .expect("an answer comes")
      .unwrap();
    assert!(got > 0, "the connection closed before an answer; it read {read:?}");
    read.extend_from_slice(&chunk[..got]);
  }
}

/// Whether the master closes the connection with nothing more said, within a bound.
async fn closed(connection: &mut TcpStream) -> bool {
  let mut chunk = [0u8; 256];
  match tokio::time::timeout(Duration::from_secs(30), connection.read(&mut chunk)).await {
    Ok(Ok(0)) | Ok(Err(_)) => true,
    Ok(Ok(_)) | Err(_) => false,
  }
}

/// A demand asked on a bare connection whose head the master has read and whose body it is waiting on: the
/// head sent asking to be told to go on, the master's `100 Continue` read, and the first `sent` bytes of the
/// body sent after it.
async fn call_in_hand(address: &str, body: &[u8], sent: usize) -> TcpStream {
  let mut held = TcpStream::connect(address).await.unwrap();
  let head = format!(
    "POST /demands HTTP/1.1\r\nhost: master\r\ncontent-type: application/json\r\ncontent-length: {}\r\nexpect: 100-continue\r\n\r\n",
    body.len()
  );
  held.write_all(head.as_bytes()).await.unwrap();
  assert_eq!(answer_on(&mut held).await.0, 100, "the master reads the body");
  held.write_all(&body[..sent]).await.unwrap();
  held
}

#[tokio::test]
async fn a_stopping_master_listens_no_more_closes_an_idle_connection_and_finishes_the_call_in_hand() {
  let mut world = World::new(json!({ "a": "model" }), &[("a", slave(json!({})))]).start();
  let address = format!("127.0.0.1:{}", world.port);
  // A connection kept open after a call it made.
  let mut idle = TcpStream::connect(&address).await.unwrap();
  idle.write_all(b"GET /health HTTP/1.1\r\nhost: master\r\n\r\n").await.unwrap();
  assert_eq!(answer_on(&mut idle).await.0, 200);
  let body = br#"{"serviceName":"absent"}"#;
  let mut held = call_in_hand(&address, body, 5).await;

  world.terminate_master();

  let until = Instant::now() + Duration::from_secs(30);
  while TcpStream::connect(&address).await.is_ok() {
    assert!(Instant::now() < until, "the stopping master still listens");
    tokio::time::sleep(Duration::from_millis(25)).await;
  }
  assert!(closed(&mut idle).await, "the idle connection is closed at the stop");
  assert!(world.master_running(), "the call in hand holds the master");
  held.write_all(&body[5..]).await.unwrap();
  let (status, headers, said) = answer_on(&mut held).await;
  let said: Value = serde_json::from_slice(&said).unwrap();
  holds_to_contract("post", "/demands", status, &said);
  assert_eq!((status, said["code"].as_str()), (422, Some("no-place")));
  assert!(headers.contains(&("connection".into(), "close".into())), "{headers:?}");
  assert!(closed(&mut held).await, "the connection is closed once its call is answered");
  assert_eq!(world.master_ended(), 0);
}

#[tokio::test]
async fn a_call_still_in_hand_when_the_drain_runs_out_is_cut_and_the_master_ends() {
  let mut world = World::new(json!({ "a": "model" }), &[("a", slave(json!({})))])
    .with_drain_ms(100)
    .start();
  let address = format!("127.0.0.1:{}", world.port);
  let mut held = call_in_hand(&address, br#"{"serviceName":"absent"}"#, 5).await;

  world.signal_master(libc::SIGINT);

  assert!(closed(&mut held).await, "the call in hand is cut with no answer");
  assert_eq!(world.master_ended(), 0);
  assert!(
    world.read("master.err").contains("after the stop were cut"),
    "{}",
    world.read("master.err")
  );
}

#[tokio::test]
async fn a_stopped_master_answers_the_calls_it_holds_ends_its_streams_and_its_callers_ask_the_next_one_again()
 {
  let mut world = World::new(json!({ "a": "demo" }), &[("a", slave(json!({})))])
    .with_service(DEMANDED, &["a"])
    .start();
  world
    .state_where("demo adopted on a", |state| {
      connected(state, "a")
        && state["services"]
          .as_array()
          .is_some_and(|all| all.iter().any(|one| one["nodeNames"] == json!(["a"])))
    })
    .await;
  // The one card granted, so the next demand waits and its answer is held open.
  let (status, first) = asked(
    &world,
    Method::POST,
    "/demands",
    Some(r#"{"serviceName":"demo","workKey":"k","waitMs":10000}"#),
  )
  .await;
  let first_id = first["id"].as_i64().unwrap();
  if status != 201 {
    let (_, granted) = asked(&world, Method::GET, &format!("/demands/{first_id}?waitMs=10000"), None).await;
    assert_eq!(granted["state"].as_str(), Some("granted"));
  }
  let read = world.first_event("cluster.changed").await;

  // A stream open and a demand's answer held for ten minutes when the stop comes.
  let mut stream = reqwest::Client::new()
    .get(format!("{}/events", world.base()))
    .send()
    .await
    .expect("the stream answers");
  assert_eq!(stream.status().as_u16(), 200);
  let base = world.base();
  let held = tokio::spawn(async move {
    let answer = reqwest::Client::new()
      .post(format!("{base}/demands"))
      .header("content-type", "application/json")
      .body(r#"{"serviceName":"demo","waitMs":600000}"#)
      .send()
      .await
      .expect("the held call is answered, not cut");
    let status = answer.status().as_u16();
    (status, answer.json::<Value>().await.unwrap())
  });
  let until = Instant::now() + Duration::from_secs(30);
  loop {
    let (_, all) = asked(&world, Method::GET, "/demands", None).await;
    if all["items"].as_array().is_some_and(|items| items.len() == 2) {
      break;
    }
    assert!(Instant::now() < until, "the second demand was never asked; the master holds {all}");
    tokio::time::sleep(Duration::from_millis(25)).await;
  }

  world.terminate_master();
  let (status, second) = tokio::time::timeout(Duration::from_secs(30), held)
    .await
    .expect("the held call is answered at the stop")
    .unwrap();
  holds_to_contract("post", "/demands", status, &second);
  assert_eq!((status, second["state"].as_str()), (202, Some("waiting")));
  tokio::time::timeout(Duration::from_secs(30), async {
    while stream.chunk().await.expect("the stream ends, not cut").is_some() {}
  })
  .await
  .expect("the stream ends at the stop");
  assert_eq!(world.master_ended(), 0);
  assert!(world.get("/health").await.is_none(), "a stopped master answers no new call");

  // The next master holds none of it: the caller finds its demand gone and asks again with its work key,
  // and the reader resuming its stream is told to read the collections again.
  world.restart_master();
  let (status, gone) = asked(&world, Method::GET, &format!("/demands/{first_id}"), None).await;
  assert_eq!((status, gone["code"].as_str()), (404, Some("not-found")));
  let (status, again) = asked(
    &world,
    Method::POST,
    "/demands",
    Some(r#"{"serviceName":"demo","workKey":"k","waitMs":10000}"#),
  )
  .await;
  let again = if status == 201 {
    again
  } else {
    let id = again["id"].as_i64().unwrap();
    asked(&world, Method::GET, &format!("/demands/{id}?waitMs=10000"), None).await.1
  };
  assert_eq!(
    (again["state"].as_str(), again["cards"][0]["nodeName"].as_str()),
    (Some("granted"), Some("a"))
  );
  let resumed = reqwest::Client::new()
    .get(format!("{}/events", world.base()))
    .header("last-event-id", read.id.to_string())
    .send()
    .await
    .unwrap();
  assert_eq!(resumed.status().as_u16(), 410);
}

#[tokio::test]
async fn a_master_stopped_in_the_middle_of_a_placement_undoes_it_at_its_next_start() {
  let mut world =
    World::new(json!({ "a": "demo", "b": null }), &[("a", slave(json!({}))), ("b", slave(json!({})))])
      .with_service(DEMO, &["a"])
      .start();
  world
    .state_where("demo adopted", |state| state["services"][0]["nodeNames"] == json!(["a"]))
    .await;
  std::fs::write(world.root().join("hold-install"), "").unwrap();
  let (status, placement) = asked(
    &world,
    Method::POST,
    "/placements",
    Some(r#"{"kind":"place","serviceName":"demo","nodeName":"b"}"#),
  )
  .await;
  assert_eq!(status, 201, "{placement}");
  let until = Instant::now() + Duration::from_secs(30);
  while !world.read("adapter.log").contains("holding install b") {
    assert!(
      Instant::now() < until,
      "the install never began; the adapter was asked {}",
      world.read("adapter.log")
    );
    tokio::time::sleep(Duration::from_millis(25)).await;
  }

  assert_eq!(world.nodes_file(), json!({ "a": "demo", "b": "demo" }), "the placement had changed b");

  world.terminate_master();
  assert_eq!(world.master_ended(), 0);
  std::fs::remove_file(world.root().join("hold-install")).unwrap();
  world.restart_master();

  world
    .state_where("demo back on a alone", |state| state["services"][0]["nodeNames"] == json!(["a"]))
    .await;
  let until = Instant::now() + Duration::from_secs(30);
  while world.nodes_file() != json!({ "a": "demo", "b": null }) {
    assert!(
      Instant::now() < until,
      "b's role was never given back; the nodes are {}",
      world.nodes_file()
    );
    tokio::time::sleep(Duration::from_millis(25)).await;
  }
  assert!(world.read("adapter.log").contains("uninstall b a\n"), "{}", world.read("adapter.log"));
  assert!(
    world.read("master.log").contains("undoing a request left unfinished: place demo b"),
    "{}",
    world.read("master.log")
  );
  let id = placement["id"].as_i64().unwrap();
  let (status, gone) = asked(&world, Method::GET, &format!("/placements/{id}"), None).await;
  assert_eq!((status, gone["code"].as_str()), (404, Some("not-found")));
}
