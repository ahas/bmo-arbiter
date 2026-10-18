//! The master standing: its rounds, the live world it reads the nodes through, and its API on the
//! loopback (docs/api.md, 「What the master serves」) — the whole picture, the same as Server-Sent Events
//! whenever it changes and at least every five seconds, the placement requests, and the units'
//! descriptions the services hand in.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{Path as Named, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use http_body_util::channel::Channel;
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::{mpsc, watch};

use crate::master::{BoxFuture, Master, Reached, World};
use crate::picture::detail_from;
use crate::placement::{Placer, Say, Verb, write_whole};
use crate::reach::{NodeRegistry, NodeShell};

/// The address the state API answers on, and how often the rounds go.
pub struct Settings {
  pub port: u16,
  pub read_every: Duration,
  pub discover_every: Duration,
}

impl Settings {
  /// As the environment sets them: `CARD_MASTER_PORT`, `CARD_MASTER_READ_EVERY_MS` and
  /// `CARD_MASTER_DISCOVER_EVERY_MS`, else 7440, two seconds and a minute.
  pub fn from_env() -> Self {
    let number = |name: &str| {
      std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|n| *n > 0)
    };
    // A port of 0 is one the system chooses, which the master names as it starts answering.
    let port = std::env::var("CARD_MASTER_PORT")
      .ok()
      .and_then(|value| value.trim().parse::<u16>().ok());
    Settings {
      port: port.unwrap_or(7440),
      read_every: Duration::from_millis(number("CARD_MASTER_READ_EVERY_MS").unwrap_or(2_000)),
      discover_every: Duration::from_millis(number("CARD_MASTER_DISCOVER_EVERY_MS").unwrap_or(60_000)),
    }
  }
}

/// The nodes as the node registry gives them, reached through their own shells, and the version of the
/// arbiter's own tree.
pub struct LiveWorld {
  registry: Result<NodeRegistry, String>,
  code_root: PathBuf,
  // The release the code root last named, and its version.
  release: Mutex<Option<(PathBuf, String)>>,
}

impl LiveWorld {
  pub fn new(registry: Result<NodeRegistry, String>, code_root: PathBuf) -> Self {
    LiveWorld {
      registry,
      code_root,
      release: Mutex::default(),
    }
  }
}

impl World for LiveWorld {
  /// The registry's nodes; none where no registry is named, which the master's start says.
  fn list_nodes(&self) -> BoxFuture<'_, Vec<String>> {
    Box::pin(async move {
      match &self.registry {
        Ok(registry) => registry.listed_nodes().await,
        Err(_) => Vec::new(),
      }
    })
  }

  fn reach<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Result<Reached, String>> {
    Box::pin(async move {
      let registry = self.registry.as_ref().map_err(Clone::clone)?;
      let connection = registry.read_connection(name).await?;
      Ok(Reached {
        role: connection.role.clone(),
        shell: Arc::new(NodeShell { connection }),
      })
    })
  }

  fn clock(&self) -> f64 {
    std::time::SystemTime::now()
      .duration_since(std::time::UNIX_EPOCH)
      .map_or(0.0, |since| since.as_secs_f64())
  }

  fn log(&self, line: &str) {
    println!("{line}");
  }

  /// The version of the release the code root names now, read again only where that release moved.
  fn version(&self) -> String {
    let release = std::fs::canonicalize(&self.code_root).unwrap_or_else(|_| self.code_root.clone());
    let mut known = self.release.lock().unwrap();
    match &*known {
      Some((read, named)) if *read == release => named.clone(),
      _ => {
        let read = crate::install::version(&self.code_root);
        *known = Some((release, read.clone()));
        read
      }
    }
  }
}

/// One placement request, and where what it does is said to its asker.
pub struct Asked {
  verb: Verb,
  service: String,
  node: String,
  lines: mpsc::UnboundedSender<String>,
}

/// The line a placement's answer ends with: `== done`, or `== failed: <why>`.
pub const DONE_LINE: &str = "== done";
pub const FAILED_LINE: &str = "== failed: ";

#[derive(Clone)]
struct App {
  seen: watch::Receiver<Snapshot>,
  requests: mpsc::UnboundedSender<Asked>,
  /// The master's store of the units' descriptions.
  descriptions: Arc<PathBuf>,
}

#[derive(Deserialize)]
struct Asking {
  service: String,
  node: String,
}

/// The placement's own work, one request after another in arrival order: first a request a stopped
/// master left unfinished, undone; then the first start's adoption and the drift named; then what is asked.
async fn place_requests(placer: Placer, mut waiting: mpsc::UnboundedReceiver<Asked>) {
  let log: Say = Arc::new(|line: String| println!("placement: {line}"));
  placer.recover(&log).await;
  if let Err(fault) = placer.adopt(&log).await {
    println!("placement: the standing placement was not adopted: {fault}");
  }
  if let Err(fault) = placer.drift(&log).await {
    println!("placement: the drift was not read: {fault}");
  }
  while let Some(asked) = waiting.recv().await {
    let lines = asked.lines.clone();
    let say: Say = Arc::new(move |line: String| {
      println!("placement: {line}");
      let _ = lines.send(line);
    });
    let ended = match placer.run(asked.verb, &asked.service, &asked.node, &say).await {
      Ok(()) => DONE_LINE.to_owned(),
      Err(stopped) => format!("{FAILED_LINE}{stopped}"),
    };
    let _ = asked.lines.send(ended);
  }
}

/// A request taken in, and what it does sent back line by line as it does it.
fn ask(app: App, verb: Verb, asking: Asking) -> Response {
  let (lines, mut said) = mpsc::unbounded_channel::<String>();
  let (mut sender, body) = Channel::<Bytes, std::convert::Infallible>::new(16);
  let _ = app.requests.send(Asked {
    verb,
    service: asking.service,
    node: asking.node,
    lines,
  });
  tokio::spawn(async move {
    while let Some(line) = said.recv().await {
      let last = line.starts_with(DONE_LINE) || line.starts_with(FAILED_LINE);
      if sender.send_data(Bytes::from(format!("{line}\n"))).await.is_err() || last {
        break;
      }
    }
  });
  Response::builder()
    .header(header::CONTENT_TYPE, "text/plain;charset=utf-8")
    .body(Body::new(body))
    .expect("response builds")
}

async fn place(State(app): State<App>, axum::Json(asking): axum::Json<Asking>) -> Response {
  ask(app, Verb::Place, asking)
}

async fn remove(State(app): State<App>, axum::Json(asking): axum::Json<Asking>) -> Response {
  ask(app, Verb::Remove, asking)
}

/// What the state API serves: the picture as JSON and the revision it was taken at.
#[derive(Clone)]
struct Snapshot {
  revision: u64,
  json: Arc<String>,
}

fn snapshot(master: &Master) -> Snapshot {
  Snapshot {
    revision: master.revision(),
    json: Arc::new(serde_json::to_string(&master.state()).expect("state serializes")),
  }
}

async fn state(State(app): State<App>) -> Response {
  let json = app.seen.borrow().json.clone();
  ([(header::CONTENT_TYPE, "application/json;charset=utf-8")], json.as_ref().clone()).into_response()
}

async fn events(State(app): State<App>) -> Response {
  let seen = app.seen;
  let (mut sender, body) = Channel::<Bytes, std::convert::Infallible>::new(4);
  tokio::spawn(async move {
    let mut shown: Option<u64> = None;
    let mut sent_at = Instant::now();
    let mut every = tokio::time::interval(Duration::from_millis(500));
    loop {
      every.tick().await;
      let now = seen.borrow().clone();
      if shown == Some(now.revision) && sent_at.elapsed() < Duration::from_secs(5) {
        continue;
      }
      shown = Some(now.revision);
      sent_at = Instant::now();
      let frame = format!("event: state\ndata: {}\n\n", now.json);
      // A listener gone is a send that fails, and ends this stream.
      if sender.send_data(Bytes::from(frame)).await.is_err() {
        break;
      }
    }
  });
  Response::builder()
    .header(header::CONTENT_TYPE, "text/event-stream")
    .header(header::CACHE_CONTROL, "no-store")
    .body(Body::new(body))
    .expect("response builds")
}

/// A refusal of a description, with its code and its English reason.
fn refused(code: &str, error: String) -> Response {
  (
    StatusCode::BAD_REQUEST,
    [(header::CONTENT_TYPE, "application/json;charset=utf-8")],
    serde_json::json!({ "code": code, "error": error }).to_string(),
  )
    .into_response()
}

/// Whether a word may name a service: lower-case letters, digits and hyphens.
fn service_word(name: &str) -> bool {
  let mut chars = name.chars();
  chars.next().is_some_and(|first| first.is_ascii_lowercase() || first.is_ascii_digit())
    && chars.all(|one| one.is_ascii_lowercase() || one.is_ascii_digit() || one == '-')
}

/// What a service hands in of its units, held to its shape before it is written: a `units` object whose
/// every name is the service's own and whose every detail reads.
fn description(service: &str, body: &[u8]) -> Result<Value, String> {
  let read: Value =
    serde_json::from_slice(body).map_err(|fault| format!("the body does not read as JSON: {fault}"))?;
  let units = read.get("units").and_then(Value::as_object).ok_or("the body holds no units object")?;
  for (unit, detail) in units {
    if !unit.starts_with(&format!("{service}/")) {
      return Err(format!("{unit} is not a unit of {service}"));
    }
    detail_from(detail).map_err(|fault| format!("{unit}: {fault}"))?;
  }
  Ok(serde_json::json!({ "units": units }))
}

/// A service's description of its units written whole into the master's store, replacing what it said
/// before (docs/api.md, 「Unit descriptions」); the master reads it again within its next round.
async fn describe(State(app): State<App>, Named(service): Named<String>, body: Bytes) -> Response {
  if !service_word(&service) {
    return refused(
      "service_name_invalid",
      format!("{service} is not a service's name: lower-case letters, digits and hyphens"),
    );
  }
  let read = match description(&service, &body) {
    Ok(read) => read,
    Err(why) => return refused("description_invalid", why),
  };
  let path = app.descriptions.join(format!("{service}.json"));
  let text = serde_json::to_string_pretty(&read).expect("a description serializes");
  match write_whole(&path, &text) {
    Ok(()) => StatusCode::NO_CONTENT.into_response(),
    Err(fault) => (
      StatusCode::INTERNAL_SERVER_ERROR,
      [(header::CONTENT_TYPE, "application/json;charset=utf-8")],
      serde_json::json!({ "code": "description_not_written", "error": fault }).to_string(),
    )
      .into_response(),
  }
}

async fn unknown() -> Response {
  (
    StatusCode::NOT_FOUND,
    [(header::CONTENT_TYPE, "application/json;charset=utf-8")],
    r#"{"error":"no such path"}"#,
  )
    .into_response()
}

/// The master read once, its picture printed, for `--once`.
pub async fn once(mut master: Master) -> String {
  master.read_rules_if_changed();
  master.discover().await;
  master.poll().await;
  let mut out = Vec::new();
  let formatter = serde_json::ser::PrettyFormatter::with_indent(b" ");
  let mut serializer = serde_json::Serializer::with_formatter(&mut out, formatter);
  serde::Serialize::serialize(&master.state(), &mut serializer).expect("state serializes");
  String::from_utf8(out).expect("serde_json writes UTF-8")
}

/// The master standing: the state API up, then a round every `read_every` and the node list read again
/// every `discover_every`.
pub async fn serve(mut master: Master, settings: Settings, placer: Placer) -> std::io::Result<()> {
  master.read_rules_if_changed();
  master.attach(placer.board());
  let descriptions = Arc::new(master.descriptions_folder());
  let (requests, waiting) = mpsc::unbounded_channel();
  tokio::spawn(place_requests(placer, waiting));
  master.discover().await;
  let (publish, seen) = watch::channel(snapshot(&master));
  let app = Router::new()
    .route("/state", get(state))
    .route("/events", get(events))
    .route("/place", post(place))
    .route("/remove", post(remove))
    .route("/descriptions/{service}", put(describe))
    .fallback(unknown)
    .with_state(App {
      seen,
      requests,
      descriptions,
    });
  let listener = tokio::net::TcpListener::bind(("127.0.0.1", settings.port)).await?;
  println!("card-master: answering on 127.0.0.1:{}", listener.local_addr()?.port());
  tokio::spawn(async move {
    if let Err(fault) = axum::serve(listener, app).await {
      eprintln!("card-master: the state API stopped — {fault}");
    }
  });
  let mut discovered = Instant::now();
  loop {
    let began = Instant::now();
    if discovered.elapsed() > settings.discover_every {
      master.discover().await;
      discovered = began;
    }
    master.poll().await;
    publish.send_replace(snapshot(&master));
    let rest = settings
      .read_every
      .saturating_sub(began.elapsed())
      .max(Duration::from_millis(200).min(settings.read_every));
    tokio::time::sleep(rest).await;
  }
}
