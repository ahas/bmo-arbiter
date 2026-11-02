//! The harness plugin: `bmo-arbiter mcp --master <url> --card <url>` starts an MCP server on standard input
//! and output that lists exactly the document's seven tools, and each call reaches the program its route is
//! served by through the client, a refusal coming back as a tool error with its code. The command runs as
//! its own process, spoken to over its pipes, beside stand-ins of the master and the node arbiter served in
//! the test, and is taken down before the test ends.

use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::support::contract::CONTRACT;
use axum::Router;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

/// How long the server may take to answer one request before the test fails; a guard, never a wait.
const PATIENCE: Duration = Duration::from_secs(30);

fn cluster() -> Value {
  json!({
    "master": { "version": "0.1.0-rev.2", "startedAt": 1000, "rulesFile": "rules.yaml", "rulesLoadedAt": null,
      "rulesError": null, "readEveryMs": 2000 },
    "groups": [], "nodes": [], "declared": [], "tiers": [], "services": [], "placement": null, "demands": [],
    "events": [],
  })
}

fn card() -> Value {
  json!({
    "version": "0.1.0-rev.2", "desktop": false, "occupants": ["voice"], "ranks": [["voice"]], "claim": null,
    "lockHeldBy": [], "busy": false, "queue": [], "typicalHoldMs": {}, "units": [], "device": null,
    "stage": { "waiting": false, "waitingMs": 0, "graphs": 0, "waits": 0, "waitedMs": 0 }, "store": null,
    "ram": { "dumpsMiB": 0, "availableMiB": null, "maxMiB": null, "floorMiB": null, "dump": "disk" },
    "rules": { "loadedAt": null, "error": null },
  })
}

fn ticket(service: &str, waited_ms: i64) -> Value {
  json!({ "id": 7, "serviceName": service, "turn": true, "inUse": true, "resting": false, "giveWay": false,
    "giveWayFor": null, "position": null, "waitMs": null, "claimantName": null, "waitedMs": waited_ms, "need": null,
    "ref": null, "rest": false, "keep": null })
}

fn refused(status: StatusCode, code: &str) -> Response {
  (status, axum::Json(json!({ "code": code, "error": "the stand-in refuses" }))).into_response()
}

/// What the stand-ins were asked, each as its method, path and query.
type Heard = Arc<Mutex<Vec<String>>>;

/// The master and the node arbiter standing in on ports of their own, answering in the document's shapes.
async fn stand_ins(heard: Heard) -> (String, String) {
  let master = Router::new().route("/cluster", get(|| async { axum::Json(cluster()) })).route(
    "/placements/{placementId}",
    get(|Path(id): Path<i64>| async move { refused(StatusCode::NOT_FOUND, &format!("no-{id}")) }),
  );
  let node = Router::new()
    .route("/card", get(|| async { axum::Json(card()) }))
    .route(
      "/tickets",
      axum::routing::post(|State(heard): State<Heard>, axum::Json(asked): axum::Json<Value>| async move {
        heard.lock().unwrap().push(format!("POST /tickets {asked}"));
        (StatusCode::CREATED, axum::Json(ticket(asked["serviceName"].as_str().unwrap_or("?"), 0)))
          .into_response()
      }),
    )
    .route(
      "/tickets/{ticketId}",
      get(|State(heard): State<Heard>, Path(id): Path<i64>, uri: axum::http::Uri| async move {
        heard
          .lock()
          .unwrap()
          .push(format!("GET /tickets/{id}?{}", uri.query().unwrap_or_default()));
        axum::Json(ticket("voice", 1500))
      })
      .delete(|Path(id): Path<i64>| async move {
        if id == 7 {
          StatusCode::NO_CONTENT.into_response()
        } else {
          refused(StatusCode::NOT_FOUND, "not-found")
        }
      }),
    )
    .with_state(heard);
  let serve = |router: Router| async move {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, router).await });
    address
  };
  (serve(master).await, serve(node).await)
}

/// The command, spoken to line by line.
struct Server {
  child: Child,
  input: ChildStdin,
  output: Lines<BufReader<ChildStdout>>,
  next: u64,
}

impl Server {
  fn start(master: &str, card: &str) -> Self {
    let mut child = Command::new(env!("CARGO_BIN_EXE_bmo-arbiter"))
      .args(["mcp", "--master", master, "--card", card])
      .env_remove("CARD_MASTER_TOKEN")
      .stdin(Stdio::piped())
      .stdout(Stdio::piped())
      .stderr(Stdio::null())
      .kill_on_drop(true)
      .spawn()
      .expect("the command starts");
    let input = child.stdin.take().expect("the command's input is piped");
    let output = BufReader::new(child.stdout.take().expect("the command's output is piped")).lines();
    Self {
      child,
      input,
      output,
      next: 1,
    }
  }

  async fn send(&mut self, message: Value) {
    let mut line = message.to_string();
    line.push('\n');
    self.input.write_all(line.as_bytes()).await.expect("the server reads its input");
    self.input.flush().await.expect("the server reads its input");
  }

  /// One request, answered with the result of the answer bearing its id.
  async fn ask(&mut self, method: &str, params: Value) -> Value {
    let id = self.next;
    self.next += 1;
    self
      .send(json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }))
      .await;
    loop {
      let line = tokio::time::timeout(PATIENCE, self.output.next_line())
        .await
        .expect("the server answers in time")
        .expect("the server's output reads")
        .expect("the server answers before it closes its output");
      let answer: Value = serde_json::from_str(&line).expect("the server writes JSON lines");
      if answer["id"] == id {
        assert!(answer["error"].is_null(), "{method} is answered without a protocol error: {answer}");
        return answer["result"].clone();
      }
    }
  }

  async fn call(&mut self, tool: &str, arguments: Value) -> Value {
    self.ask("tools/call", json!({ "name": tool, "arguments": arguments })).await
  }

  async fn stop(mut self) {
    drop(self.input);
    self.child.kill().await.expect("the command is taken down");
  }
}

/// A tool error's content, as the server writes it.
fn said(answer: &Value) -> Value {
  assert_eq!(answer["isError"], json!(true), "{answer}");
  serde_json::from_str(answer["content"][0]["text"].as_str().unwrap()).unwrap()
}

#[tokio::test]
async fn the_server_lists_exactly_the_documented_tools_and_each_calls_its_programs_route() {
  let heard: Heard = Arc::default();
  let (master, card_address) = stand_ins(heard.clone()).await;
  let mut server = Server::start(&master, &card_address);
  let started = server
    .ask(
      "initialize",
      json!({ "protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": { "name": "tester", "version": "1" } }),
    )
    .await;
  assert_eq!(
    (started["serverInfo"]["name"].clone(), started["serverInfo"]["version"].clone()),
    (json!("bmo-arbiter"), json!(env!("CARGO_PKG_VERSION")))
  );
  server.send(json!({ "jsonrpc": "2.0", "method": "notifications/initialized" })).await;

  let listed = server.ask("tools/list", json!({})).await;
  let names: Vec<&str> = listed["tools"]
    .as_array()
    .unwrap()
    .iter()
    .map(|tool| tool["name"].as_str().unwrap())
    .collect();
  let documented: Vec<&str> = CONTRACT["x-tools"]
    .as_array()
    .unwrap()
    .iter()
    .map(|tool| tool["name"].as_str().unwrap())
    .collect();
  assert_eq!(
    names,
    [
      "arbiter_cluster",
      "arbiter_placement_request",
      "arbiter_placement",
      "arbiter_card",
      "arbiter_ticket_take",
      "arbiter_ticket",
      "arbiter_ticket_return"
    ]
  );
  assert_eq!(names, documented);
  assert!(
    listed["tools"]
      .as_array()
      .unwrap()
      .iter()
      .all(|tool| tool["outputSchema"]["type"] == "object")
  );

  let cluster = server.call("arbiter_cluster", json!({})).await;
  assert_eq!(cluster["structuredContent"]["master"]["readEveryMs"], 2000);
  let card = server.call("arbiter_card", json!({})).await;
  assert_eq!(card["structuredContent"]["occupants"], json!(["voice"]));

  let taken = server
    .call("arbiter_ticket_take", json!({ "body": { "serviceName": "voice", "waitMs": 5000 } }))
    .await;
  assert_eq!(
    (taken["structuredContent"]["id"].clone(), taken["structuredContent"]["turn"].clone()),
    (json!(7), json!(true))
  );
  let waited = server.call("arbiter_ticket", json!({ "ticketId": 7, "waitMs": 2000 })).await;
  assert_eq!(waited["structuredContent"]["waitedMs"], 1500);
  assert_eq!(
    *heard.lock().unwrap(),
    [r#"POST /tickets {"serviceName":"voice","waitMs":5000}"#, "GET /tickets/7?waitMs=2000"]
  );
  let returned = server.call("arbiter_ticket_return", json!({ "ticketId": 7 })).await;
  assert_eq!(returned["structuredContent"], json!({ "ticketId": 7 }));

  let gone = said(&server.call("arbiter_ticket_return", json!({ "ticketId": 8 })).await);
  assert_eq!((gone["status"].clone(), gone["code"].clone()), (json!(404), json!("not-found")));
  let missing = said(&server.call("arbiter_placement", json!({ "placementId": 3 })).await);
  assert_eq!(missing["status"], 404);
  let wrong = said(&server.call("arbiter_ticket", json!({ "ticket": 7 })).await);
  assert_eq!(wrong["code"], "bad-request");
  server.stop().await;
}
