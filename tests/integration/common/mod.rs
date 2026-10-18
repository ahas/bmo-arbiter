//! A world of stand-ins the card master's binary is started over, as a process on a free port: the node
//! registry's command, the console answering each node's shell out of files the test writes (the node
//! arbiter's install among them), the arbiter's own tree, and a service's adapter. The master is asked
//! through its own API, as a console and a person ask it, and taken down with everything it started.

#![allow(dead_code)]

use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use axum::body::Bytes;
use futures_util::StreamExt;
use http_body_util::{BodyExt, Empty};
use hyper::Request;
use hyper::body::Incoming;
use hyper::header::HOST;
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use sse_stream::SseStream;
use tokio::net::TcpStream;

pub const RULES: &str = r#"
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
rules:
  - when: { request: "voice/*" }
    evict: { cheapest: ["model/*"] }
    restore: true
"#;

// The node registry's command, standing in: the listing with lines that name no node among those that
// do, one node's connection as the last line of its answer, and a role set the way the registry sets it.
const NODES: &str = r#"
import { appendFileSync, readFileSync, writeFileSync } from "node:fs";
const dir = process.env.STANDIN_DIR;
const nodes = JSON.parse(readFileSync(`${dir}/nodes.json`, "utf8"));
const args = process.argv.slice(2);
if (args[0] === "--target") {
  console.log("reading the node list");
  console.log(JSON.stringify({ kind: "hub", hubUrl: "http://127.0.0.1:1", tokenFile: "~/none", agent: args[1], role: nodes[args[1]] ?? null }));
} else if (args[0] === "--set") {
  nodes[args[1]] = args[3] === "none" ? null : args[3];
  writeFileSync(`${dir}/nodes.json`, JSON.stringify(nodes));
  appendFileSync(`${dir}/commands.log`, `nodes: --set ${args[1]} --role ${args[3]}\n`);
} else {
  for (const name of Object.keys(nodes)) {
    console.log(`${name} [hub] ${name} — ${name} @ http://127.0.0.1:1`);
    console.log("    connected · checked");
  }
  console.log("\nhubs:\nhub http://127.0.0.1:1");
}
"#;

// The console, standing in: every command written down; a file carried kept under the node's own folder;
// the home, the occupants file and its move answered as a node answers them; the node arbiter's install
// answered as a healthy node answers it; a move of units answered as a slave answers one; and a read of the
// slave answered with the node's slave file, or failed where the node has none.
const CONSOLE: &str = r#"
import { appendFileSync, copyFileSync, existsSync, mkdirSync, readFileSync, renameSync } from "node:fs";
import { basename } from "node:path";
const dir = process.env.STANDIN_DIR;
const args = process.argv.slice(2);
if (args[0] === "upload") {
  const [, local, remote, , agent] = args;
  appendFileSync(`${dir}/commands.log`, `${agent}: upload ${remote}\n`);
  mkdirSync(`${dir}/files/${agent}`, { recursive: true });
  copyFileSync(local, `${dir}/files/${agent}/${basename(remote)}`);
  process.exit(0);
}
const [, , agent, command] = args;
appendFileSync(`${dir}/commands.log`, `${agent}: ${command}\n`);
const kept = (name) => `${dir}/files/${agent}/${name}`;
if (command.startsWith("echo HOME")) {
  process.stdout.write("HOME=/home/stand\n");
} else if (command.includes("/units/")) {
  process.stdout.write('{"results":[]}\n200');
} else if (command.includes("MACHINE_WSL")) {
  process.stdout.write("MACHINE_WSL=0\nMACHINE_CARD_MIB=11875\nMACHINE_CARD_NAME=RTX 4070\n");
} else if (command.includes("units.store")) {
  process.stdout.write("STORE 1\n");
} else if (command.includes("setup.log")) {
  process.stdout.write("CARD-SETUP-DONE ok\n");
} else if (command.includes("MainPID")) {
  process.stdout.write("4242\n");
} else if (command.startsWith("cat ~/.config/bmo-card/occupants")) {
  process.stdout.write(existsSync(kept("occupants")) ? readFileSync(kept("occupants"), "utf8") : "__BMO_NO_OCCUPANTS__\n");
} else if (command.startsWith("mv ~/.config/bmo-card/occupants.partial")) {
  renameSync(kept("occupants.partial"), kept("occupants"));
} else if (command.startsWith("mkdir -p") || command.startsWith("rm -f")) {
  process.exit(0);
} else {
  const file = `${dir}/slaves/${agent}.json`;
  if (!existsSync(file)) process.exit(7);
  process.stdout.write(readFileSync(file, "utf8"));
}
"#;

/// The node arbiter's files the install carries, standing in: each names itself.
const CARD_FILES: [&str; 7] = [
  "card.py",
  "desktop.py",
  "units.py",
  "turn.py",
  "pyproject.toml",
  "node-setup.sh",
  "bmo-card.service",
];

// A service's adapter, standing in: every verb written down, its own placement kept in a file, and an
// install that fails where the test asks it to.
const ADAPTER: &str = r#"
import { appendFileSync, existsSync, readFileSync, writeFileSync } from "node:fs";
const dir = process.env.STANDIN_DIR;
const [verb, ...rest] = process.argv.slice(2);
const value = (flag) => { const at = rest.indexOf(flag); return at === -1 ? null : rest[at + 1]; };
const node = value("--node");
const placed = (value("--placed") ?? "").split(",").filter((one) => one !== "");
appendFileSync(`${dir}/adapter.log`, `${verb} ${node ?? "-"} ${placed.join(",")}\n`);
const own = `${dir}/demo-placed.json`;
const answer = (said) => { console.log(JSON.stringify(said)); process.exit(said.ok ? 0 : 1); };
if (verb === "placed") answer({ ok: true, node: null, state: null, detail: "", nodes: existsSync(own) ? JSON.parse(readFileSync(own, "utf8")) : [] });
if (verb === "install" && existsSync(`${dir}/fail-install`)) answer({ ok: false, node, state: null, detail: "the stand-in was told to fail" });
console.log(`doing ${verb} on ${node}`);
if (verb === "install") writeFileSync(own, JSON.stringify(placed.includes(node) ? placed : [...placed, node]));
if (verb === "uninstall") writeFileSync(own, JSON.stringify(placed.filter((one) => one !== node)));
answer({ ok: true, node, state: verb === "uninstall" || verb === "stop" ? "stopped" : "running", detail: "" });
"#;

pub fn slave(over: Value) -> Value {
  let free = over.get("free").and_then(Value::as_f64).unwrap_or(1300.0);
  let mut card =
    json!({ "name": "RTX 4070", "totalMiB": 11875, "usedMiB": 11875.0 - free, "freeMiB": free });
  if let Some(processes) = over.get("processes") {
    card["processes"] = processes.clone();
  }
  let store = over.get("store").cloned().unwrap_or_else(
    || json!({ "name": "model/store", "service": "model", "part": "store", "evictable": true, "state": "resident", "vramMiB": 4608, "since": 1 }),
  );
  json!({
    "version": "abc123", "desktop": false, "occupants": ["tester", "*"],
    "card": card,
    "units": [
      { "name": "model/weights", "service": "model", "part": "weights", "evictable": true, "state": "resident", "vramMiB": 4000, "since": 1 },
      store,
    ],
    "stage": { "waiting": false, "waitingS": 0 },
    "claim": { "by": over.get("claim").cloned().unwrap_or(Value::Null), "since": null },
    "queue": over.get("queue").cloned().unwrap_or_else(|| json!([])),
    "rules": { "loadedAt": null, "error": null },
  })
}

pub struct World {
  dir: tempfile::TempDir,
  master: Option<Child>,
  pub port: u16,
}

pub fn binary() -> &'static str {
  env!("CARGO_BIN_EXE_card-master")
}

impl World {
  pub fn new(nodes: Value, slaves: &[(&str, Value)]) -> World {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::create_dir_all(root.join("data/placements/card-units")).unwrap();
    std::fs::create_dir_all(root.join("code/card")).unwrap();
    std::fs::create_dir_all(root.join("slaves")).unwrap();
    std::fs::write(root.join("data/placements/card-rules.yaml"), RULES).unwrap();
    std::fs::write(root.join("nodes.ts"), NODES).unwrap();
    // The arbiter's own tree: its version and the node arbiter's files.
    std::fs::write(root.join("code/VERSION"), "0.1.0\n").unwrap();
    for name in CARD_FILES {
      std::fs::write(root.join("code/card").join(name), format!("{name} one\n")).unwrap();
    }
    std::fs::write(root.join("console.ts"), CONSOLE).unwrap();
    std::fs::write(root.join("nodes.json"), nodes.to_string()).unwrap();
    std::fs::write(root.join("commands.log"), "").unwrap();
    let world = World {
      dir,
      master: None,
      port: 0,
    };
    for (name, state) in slaves {
      world.set_slave(name, Some(state.clone()));
    }
    world
  }

  /// A service declared in the scratch tree's git index, its adapter the stand-in, standing on the nodes
  /// given as its own placement says.
  pub fn with_service(self, declaration: &str, placed: &[&str]) -> World {
    let code = self.root().join("code");
    std::fs::create_dir_all(code.join("svc")).unwrap();
    std::fs::write(code.join("svc/demo.card.toml"), declaration).unwrap();
    std::fs::write(code.join("svc/adapter.ts"), ADAPTER).unwrap();
    std::fs::write(self.root().join("demo-placed.json"), serde_json::to_string(placed).unwrap()).unwrap();
    let git = |args: &[&str]| {
      let done = Command::new("git").arg("-C").arg(&code).args(args).output().unwrap();
      assert!(done.status.success(), "{}", String::from_utf8_lossy(&done.stderr));
    };
    git(&["init", "-q"]);
    git(&["add", "svc/demo.card.toml", "svc/adapter.ts"]);
    self
  }

  /// The scratch tree made a committed release, and a deploy link naming it the master's code root.
  pub fn behind_link(self) -> World {
    let releases = self.root().join("releases");
    std::fs::create_dir_all(&releases).unwrap();
    std::fs::rename(self.root().join("code"), releases.join("one")).unwrap();
    commit(&releases.join("one"));
    std::os::unix::fs::symlink(releases.join("one"), self.root().join("deploy")).unwrap();
    self
  }

  /// A release copied from the one the deploy link names, changed as given and committed, and the link
  /// moved onto it whole, as a deployment moves it.
  pub fn move_link(&self, release: &str, change: impl FnOnce(&Path)) {
    let link = self.root().join("deploy");
    let new = self.root().join("releases").join(release);
    let copied = Command::new("cp")
      .arg("-a")
      .arg(std::fs::canonicalize(&link).unwrap())
      .arg(&new)
      .output()
      .unwrap();
    assert!(copied.status.success(), "{}", String::from_utf8_lossy(&copied.stderr));
    change(&new);
    commit(&new);
    let next = self.root().join("deploy.next");
    std::os::unix::fs::symlink(&new, &next).unwrap();
    std::fs::rename(next, link).unwrap();
  }

  pub fn root(&self) -> &Path {
    self.dir.path()
  }

  fn environment(&self, command: &mut Command) {
    let root = self.root();
    let link = root.join("deploy");
    let tree = if link.exists() { link } else { root.join("code") };
    command
      .env("BMO_DATA_DIR", root.join("data"))
      .env("BMO_CODE_ROOT", &tree)
      .env("CARD_DECLARATIONS", &tree)
      .env(
        "CARD_NODES_COMMAND",
        format!("'{}' '{}'", bmo_arbiter::reach::bun(), root.join("nodes.ts").display()),
      )
      .env("BMO_HUB_CONSOLE", root.join("console.ts"))
      .env("CARD_INSTALL_POLL_MS", "10")
      .env_remove("CARD_RIVAL_UNITS")
      .env("STANDIN_DIR", root)
      .env("CARD_MASTER_PORT", self.port.to_string())
      .env("CARD_MASTER_READ_EVERY_MS", "50")
      .env("CARD_MASTER_DISCOVER_EVERY_MS", "600000");
  }

  /// The master started over the world, on a port of its own choosing, which it names as it starts
  /// answering — so no other process can take the port between its choosing and its binding.
  pub fn start(mut self) -> World {
    let mut command = Command::new(binary());
    self.environment(&mut command);
    let log = std::fs::File::create(self.root().join("master.log")).unwrap();
    self.master = Some(
      command
        .stdin(Stdio::null())
        .stdout(log)
        .stderr(Stdio::null())
        .process_group(0)
        .spawn()
        .expect("the master starts"),
    );
    let ready = Instant::now() + Duration::from_secs(30);
    self.port = loop {
      let said = self.read("master.log");
      let heard = said.lines().find_map(|line| {
        line.strip_prefix("card-master: answering on 127.0.0.1:")?.trim().parse::<u16>().ok()
      });
      if let Some(port) = heard {
        break port;
      }
      assert!(Instant::now() < ready, "the master never answered; it said {said}");
      std::thread::sleep(Duration::from_millis(20));
    };
    self
  }

  /// The binary run once as a person runs it beside the standing master, with what it printed.
  pub fn run(&self, args: &[&str]) -> (i32, String) {
    let mut command = Command::new(binary());
    self.environment(&mut command);
    let done = command.args(args).stdin(Stdio::null()).output().unwrap();
    (
      done.status.code().unwrap_or(-1),
      format!("{}{}", String::from_utf8_lossy(&done.stdout), String::from_utf8_lossy(&done.stderr)),
    )
  }

  /// A node's slave answering as given, or not at all; written whole so no read sees half of it.
  pub fn set_slave(&self, name: &str, state: Option<Value>) {
    let file = self.root().join("slaves").join(format!("{name}.json"));
    match state {
      Some(state) => {
        let partial = file.with_extension("partial");
        std::fs::write(&partial, state.to_string()).unwrap();
        std::fs::rename(partial, file).unwrap();
      }
      None => {
        let _ = std::fs::remove_file(file);
      }
    }
  }

  pub fn read(&self, path: &str) -> String {
    std::fs::read_to_string(self.root().join(path)).unwrap_or_default()
  }

  pub fn commands(&self) -> Vec<String> {
    self.read("commands.log").lines().map(str::to_owned).collect()
  }

  pub fn nodes_file(&self) -> Value {
    serde_json::from_str(&self.read("nodes.json")).unwrap()
  }

  pub fn kept(&self, node: &str, name: &str) -> PathBuf {
    self.root().join("files").join(node).join(name)
  }

  async fn ask(&self, path: &str) -> Option<(u16, hyper::Response<Incoming>)> {
    let stream = TcpStream::connect(("127.0.0.1", self.port)).await.ok()?;
    let (mut sender, connection) =
      hyper::client::conn::http1::handshake(TokioIo::new(stream)).await.ok()?;
    tokio::spawn(connection);
    let request = Request::get(path).header(HOST, "127.0.0.1").body(Empty::<Bytes>::new()).ok()?;
    let answer = sender.send_request(request).await.ok()?;
    Some((answer.status().as_u16(), answer))
  }

  /// A path asked once, its status and its whole body.
  pub async fn get(&self, path: &str) -> Option<(u16, String)> {
    let (status, answer) = self.ask(path).await?;
    let body = answer.into_body().collect().await.ok()?.to_bytes();
    Some((status, String::from_utf8_lossy(&body).into_owned()))
  }

  /// A body put to a path once, the answer's status and its whole body.
  pub async fn put(&self, path: &str, body: &str) -> Option<(u16, String)> {
    let stream = TcpStream::connect(("127.0.0.1", self.port)).await.ok()?;
    let (mut sender, connection) =
      hyper::client::conn::http1::handshake(TokioIo::new(stream)).await.ok()?;
    tokio::spawn(connection);
    let request = Request::put(path)
      .header(HOST, "127.0.0.1")
      .header(hyper::header::CONTENT_TYPE, "application/json")
      .body(http_body_util::Full::new(Bytes::from(body.to_owned())))
      .ok()?;
    let answer = sender.send_request(request).await.ok()?;
    let status = answer.status().as_u16();
    let said = answer.into_body().collect().await.ok()?.to_bytes();
    Some((status, String::from_utf8_lossy(&said).into_owned()))
  }

  /// The first event of the event stream, its name, its data, and the stream's content type.
  pub async fn first_event(&self) -> (String, Option<String>, Value) {
    let (_, answer) = self.ask("/events").await.expect("the stream answers");
    let kind = answer
      .headers()
      .get("content-type")
      .map(|value| value.to_str().unwrap().to_owned())
      .unwrap_or_default();
    let mut events = SseStream::new(answer.into_body());
    let first = tokio::time::timeout(Duration::from_secs(10), events.next())
      .await
      .unwrap()
      .unwrap()
      .unwrap();
    (kind, first.event, serde_json::from_str(first.data.as_deref().unwrap()).unwrap())
  }

  pub async fn state(&self) -> Option<Value> {
    let (status, body) = self.get("/state").await?;
    if status != 200 {
      return None;
    }
    serde_json::from_str(&body).ok()
  }

  /// The state once it holds what the test waits for, within a bound.
  pub async fn state_where(&self, what: &str, holds: impl Fn(&Value) -> bool) -> Value {
    let until = Instant::now() + Duration::from_secs(30);
    let mut last = None;
    while Instant::now() < until {
      if let Some(state) = self.state().await {
        if holds(&state) {
          return state;
        }
        last = Some(state);
      }
      tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("the master never came to show {what}; it last showed {last:#?}");
  }

  /// The commands the nodes were sent once one matches, within a bound.
  pub async fn command_where(&self, what: &str, holds: impl Fn(&str) -> bool) -> String {
    let until = Instant::now() + Duration::from_secs(30);
    while Instant::now() < until {
      if let Some(found) = self.commands().into_iter().find(|command| holds(command)) {
        return found;
      }
      tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("no node was ever sent {what}; they were sent {:#?}", self.commands());
  }
}

impl Drop for World {
  fn drop(&mut self) {
    // The master and every console call it started, as one process group.
    if let Some(master) = &mut self.master {
      unsafe {
        libc::kill(-(master.id() as i32), libc::SIGKILL);
      }
      let _ = master.wait();
    }
  }
}

/// Everything in a tree committed, as a release stands committed.
fn commit(tree: &Path) {
  for args in [&["add", "-A"][..], &["commit", "-q", "-m", "release"]] {
    let done = Command::new("git")
      .arg("-C")
      .arg(tree)
      .args([
        "-c",
        "user.name=tester",
        "-c",
        "user.email=tester@example.com",
        "-c",
        "commit.gpgsign=false",
        "-c",
        "core.hooksPath=/dev/null",
      ])
      .args(args)
      .output()
      .unwrap();
    assert!(done.status.success(), "{}", String::from_utf8_lossy(&done.stderr));
  }
}

pub fn connected(state: &Value, node: &str) -> bool {
  nodes(state)
    .iter()
    .any(|one| one["name"] == node && one["link"]["state"] == "connected")
}

pub fn nodes(state: &Value) -> Vec<Value> {
  state["nodes"].as_array().cloned().unwrap_or_default()
}

/// The events' codes, newest first.
pub fn codes(state: &Value) -> Vec<String> {
  state["events"]
    .as_array()
    .into_iter()
    .flatten()
    .filter_map(|event| event["code"].as_str().map(str::to_owned))
    .collect()
}

pub fn events(state: &Value) -> Vec<String> {
  state["events"]
    .as_array()
    .into_iter()
    .flatten()
    .filter_map(|event| event["text"].as_str().map(str::to_owned))
    .collect()
}

pub fn card(state: &Value) -> Value {
  nodes(state)[0]["cards"][0].clone()
}
