//! The master standing: its rounds, the live world it reads the nodes through, and its API on the loopback
//! (docs/api.md, 「What the master serves」) — its health, its one event stream, the cluster, the placements
//! run one at a time, the demands its rounds grant, and the units' descriptions the services hand in.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Bytes;
use axum::extract::rejection::QueryRejection;
use axum::extract::{Path as Named, Query, Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use bmo_arbiter_client::cluster::Cluster;
use bmo_arbiter_client::demands::{DemandAsk, DemandPage, DemandQuery, DemandState};
use bmo_arbiter_client::descriptions::Description;
use bmo_arbiter_client::events::{Event, EventKind, EventPage, EventsQuery, Health};
use bmo_arbiter_client::placements::{Placement, PlacementAsk, PlacementState};
use bmo_arbiter_client::{ErrorCode, WireError};
use futures_util::Stream;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::{broadcast, mpsc};

use crate::demands::Demands;
use crate::master::{BoxFuture, Master, Reached, World};
use crate::picture::detail_from;
use crate::placement::{AdapterAnswer, Declaration, Hands, Placer, Say, write_whole};
use crate::reach::{NodeRegistry, NodeShell};
use crate::usage::usage_of;

/// The address the API answers on, the token it takes, and how often the rounds go.
pub struct Settings {
  pub port: u16,
  pub token: Option<String>,
  pub read_every: Duration,
  pub discover_every: Duration,
}

/// The port the master answers on: `CARD_MASTER_PORT`, else 7440; 0 is one the system chooses.
pub fn master_port() -> u16 {
  std::env::var("CARD_MASTER_PORT")
    .ok()
    .and_then(|value| value.trim().parse::<u16>().ok())
    .unwrap_or(7440)
}

/// The master's token, `CARD_MASTER_TOKEN`, where one is set.
pub fn master_token() -> Option<String> {
  std::env::var("CARD_MASTER_TOKEN")
    .ok()
    .map(|token| token.trim().to_owned())
    .filter(|token| !token.is_empty())
}

impl Settings {
  /// As the environment sets them: `CARD_MASTER_PORT`, `CARD_MASTER_TOKEN`, `CARD_MASTER_READ_EVERY_MS` and
  /// `CARD_MASTER_DISCOVER_EVERY_MS`, else 7440, no token, two seconds and a minute.
  pub fn from_env() -> Self {
    let number = |name: &str| {
      std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|n| *n > 0)
    };
    Settings {
      port: master_port(),
      token: master_token(),
      read_every: Duration::from_millis(number("CARD_MASTER_READ_EVERY_MS").unwrap_or(2_000)),
      discover_every: Duration::from_millis(number("CARD_MASTER_DISCOVER_EVERY_MS").unwrap_or(60_000)),
    }
  }
}

/// The nodes as the node service gives them, reached through their own shells, and the version of the
/// arbiter's own tree.
pub struct LiveWorld {
  registry: Result<NodeRegistry, String>,
  code_root: PathBuf,
  // The placement's hands, which read the declarations and run the adapters; none where nothing is placed.
  hands: Option<Arc<dyn Hands>>,
  // The release the code root last named, and its version.
  release: Mutex<Option<(PathBuf, String)>>,
}

impl LiveWorld {
  pub fn new(registry: Result<NodeRegistry, String>, code_root: PathBuf) -> Self {
    LiveWorld {
      registry,
      code_root,
      hands: None,
      release: Mutex::default(),
    }
  }

  /// The world reading the declarations and running the adapters through the placement's hands.
  pub fn with_hands(self, hands: Arc<dyn Hands>) -> Self {
    LiveWorld {
      hands: Some(hands),
      ..self
    }
  }
}

impl World for LiveWorld {
  /// The node service's nodes; none where no node service is named, which the master's start says.
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
    now_seconds()
  }

  fn declarations(&self) -> Result<Vec<Declaration>, String> {
    match &self.hands {
      Some(hands) => hands.declarations(),
      None => Ok(Vec::new()),
    }
  }

  fn adapter<'a>(
    &'a self,
    declaration: &'a Declaration,
    verb: &'a str,
    node: &'a str,
    placed: &'a [String],
  ) -> BoxFuture<'a, AdapterAnswer> {
    Box::pin(async move {
      let Some(hands) = &self.hands else {
        return AdapterAnswer::refused("no adapter is run here");
      };
      let say: Say = Arc::new(|line: String| println!("{line}"));
      hands.adapter(declaration, verb, Some(node), placed, &say).await
    })
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

fn now_seconds() -> f64 {
  std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH)
    .map_or(0.0, |since| since.as_secs_f64())
}

fn now_ms() -> i64 {
  (now_seconds() * 1000.0) as i64
}

/// How many events and placements the master keeps in its memory.
const EVENTS_KEPT: usize = 256;
const PLACEMENTS_KEPT: usize = 256;

/// The master's events: the window kept for a reader that resumes, and the live ones for every open
/// stream (docs/api.md, 「Events」).
struct Events {
  kept: VecDeque<Event>,
  next_id: i64,
  live: broadcast::Sender<Event>,
}

impl Events {
  fn new() -> Self {
    Self {
      kept: VecDeque::new(),
      // The first number of a run is the milliseconds at its start, so no number of an earlier run is this
      // run's.
      next_id: now_ms().max(1),
      live: broadcast::channel(EVENTS_KEPT).0,
    }
  }

  fn publish(&mut self, kind: EventKind, data: Value) {
    let event = Event {
      id: self.next_id,
      kind,
      at: now_ms(),
      data,
    };
    self.next_id += 1;
    self.kept.push_back(event.clone());
    while self.kept.len() > EVENTS_KEPT {
      self.kept.pop_front();
    }
    // A send with no stream open reaches nobody, which is no fault.
    let _ = self.live.send(event);
  }

  /// Whether a reader that read up to `after` can resume: no event it has not read has left the window, and
  /// the number is one this run gave.
  fn resumes(&self, after: i64) -> bool {
    let lowest = self.kept.front().map_or(self.next_id, |event| event.id);
    after >= lowest - 1 && after < self.next_id
  }
}

/// The placements the master keeps, the newest last.
struct Placements {
  kept: VecDeque<Placement>,
  last_id: i64,
}

impl Placements {
  /// A new id: larger than the last, and never below the milliseconds at which it is given.
  fn next_id(&mut self) -> i64 {
    self.last_id = (self.last_id + 1).max(now_ms());
    self.last_id
  }

  fn get(&self, id: i64) -> Option<&Placement> {
    self.kept.iter().find(|placement| placement.id == id)
  }

  fn change(&mut self, id: i64, change: impl FnOnce(&mut Placement)) -> Option<Placement> {
    let placement = self.kept.iter_mut().find(|placement| placement.id == id)?;
    change(placement);
    Some(placement.clone())
  }
}

#[derive(Clone)]
struct App {
  cluster: Arc<Mutex<Cluster>>,
  events: Arc<Mutex<Events>>,
  placements: Arc<Mutex<Placements>>,
  requests: mpsc::UnboundedSender<i64>,
  /// The demands the routes ask and the master's rounds grant.
  demands: Arc<Demands>,
  /// The master's store of the units' descriptions.
  descriptions: Arc<PathBuf>,
  token: Option<Arc<str>>,
}

impl App {
  fn publish(&self, kind: EventKind, data: Value) {
    self.events.lock().unwrap().publish(kind, data);
  }

  fn publish_placement(&self, kind: EventKind, placement: &Placement) {
    self.publish(kind, serde_json::to_value(placement).expect("a placement serializes"));
  }

  /// A placement changed and the change published.
  fn change_placement(&self, id: i64, change: impl FnOnce(&mut Placement)) {
    let changed = self.placements.lock().unwrap().change(id, change);
    if let Some(changed) = changed {
      self.publish_placement(EventKind::PlacementChanged, &changed);
    }
  }
}

/// An answer in the wire format's error shape.
fn refused(code: ErrorCode, error: impl Into<String>) -> Response {
  let status = StatusCode::from_u16(code.status()).expect("every code's status is a status");
  (status, axum::Json(WireError::new(code, error))).into_response()
}

/// A refusal not yet answered: its code and its sentence.
struct Refusal(ErrorCode, String);

impl IntoResponse for Refusal {
  fn into_response(self) -> Response {
    refused(self.0, self.1)
  }
}

fn answered<T: Serialize>(status: StatusCode, body: &T) -> Response {
  (status, axum::Json(body)).into_response()
}

/// Every route takes the master's token where one is set.
async fn authorized(State(app): State<App>, request: Request, next: Next) -> Response {
  if let Some(token) = &app.token {
    let given = request
      .headers()
      .get(header::AUTHORIZATION)
      .and_then(|value| value.to_str().ok())
      .and_then(|value| value.strip_prefix("Bearer "));
    if given != Some(token.as_ref()) {
      return refused(ErrorCode::Unauthorized, "the master's token is missing or wrong");
    }
  }
  next.run(request).await
}

async fn health() -> Response {
  answered(
    StatusCode::OK,
    &Health {
      ready: true,
      reason: None,
      version: env!("CARGO_PKG_VERSION").to_owned(),
    },
  )
}

async fn cluster(State(app): State<App>) -> Response {
  let cluster = app.cluster.lock().unwrap().clone();
  answered(StatusCode::OK, &cluster)
}

/// What every card was doing at the master's last reading (docs/api.md, 「Usage」): the owner's read of the
/// cluster, with no process, pid or person's name in it.
async fn usage(State(app): State<App>) -> Response {
  let usage = {
    let cluster = app.cluster.lock().unwrap();
    usage_of(&cluster, now_ms())
  };
  answered(StatusCode::OK, &usage)
}

/// The placement's own work, one placement after another in the order they were asked: first one a stopped
/// master left unfinished, undone; then the first start's adoption and the drift named; then what is asked.
async fn place_requests(placer: Placer, app: App, mut waiting: mpsc::UnboundedReceiver<i64>) {
  let log: Say = Arc::new(|line: String| println!("placement: {line}"));
  placer.recover(&log).await;
  if let Err(fault) = placer.adopt(&log).await {
    println!("placement: the standing placement was not adopted: {fault}");
  }
  if let Err(fault) = placer.drift(&log).await {
    println!("placement: the drift was not read: {fault}");
  }
  while let Some(id) = waiting.recv().await {
    let Some(asked) = app.placements.lock().unwrap().get(id).cloned() else { continue };
    app.change_placement(id, |placement| placement.state = PlacementState::Running);
    let telling = app.clone();
    let say: Say = Arc::new(move |line: String| {
      println!("placement: {line}");
      telling.change_placement(id, |placement| placement.lines.push(line));
    });
    let ended = placer.run(asked.kind.into(), &asked.service_name, &asked.node_name, &say).await;
    app.change_placement(id, |placement| {
      placement.ended_at = Some(now_ms());
      match ended {
        Ok(()) => placement.state = PlacementState::Done,
        Err(stopped) => {
          placement.state = PlacementState::Failed;
          placement.failure = Some(stopped.to_string());
        }
      }
    });
  }
}

/// A body read as the route's type, or the refusal naming what is wrong with it.
fn body_of<T: serde::de::DeserializeOwned>(body: &Bytes) -> Result<T, Refusal> {
  serde_json::from_slice(body).map_err(|fault| {
    Refusal(ErrorCode::BadRequest, format!("the body is not the shape this route takes: {fault}"))
  })
}

async fn create_placement(State(app): State<App>, body: Bytes) -> Response {
  let ask: PlacementAsk = match body_of(&body) {
    Ok(ask) => ask,
    Err(refusal) => return refusal.into_response(),
  };
  let placement = {
    let mut placements = app.placements.lock().unwrap();
    let placement = Placement {
      id: placements.next_id(),
      kind: ask.kind,
      service_name: ask.service_name,
      node_name: ask.node_name,
      state: PlacementState::Queued,
      lines: Vec::new(),
      failure: None,
      created_at: now_ms(),
      ended_at: None,
    };
    placements.kept.push_back(placement.clone());
    while placements.kept.len() > PLACEMENTS_KEPT {
      placements.kept.pop_front();
    }
    placement
  };
  app.publish_placement(EventKind::PlacementCreated, &placement);
  let _ = app.requests.send(placement.id);
  answered(StatusCode::CREATED, &placement)
}

async fn placement(State(app): State<App>, Named(id): Named<String>) -> Response {
  let Ok(id) = id.parse::<i64>() else {
    return refused(ErrorCode::BadRequest, format!("{id} is not a placement's id"));
  };
  match app.placements.lock().unwrap().get(id) {
    Some(placement) => answered(StatusCode::OK, placement),
    None => refused(ErrorCode::NotFound, format!("the master keeps no placement {id}")),
  }
}

/// A demand asked for (docs/api.md, 「Demands」): `201` where every card it asks is granted by the time the
/// answer goes, `202` where it waits; the master's next round is woken for it.
async fn create_demand(State(app): State<App>, body: Bytes) -> Response {
  let ask: DemandAsk = match body_of(&body) {
    Ok(ask) => ask,
    Err(refusal) => return refusal.into_response(),
  };
  let changed = app.demands.changed.subscribe();
  let asked = app.demands.book.lock().unwrap().ask(&ask, now_ms()).map(|held| (held.id, held.news));
  let (id, news) = match asked {
    Ok(asked) => asked,
    Err((code, why)) => return refused(code, why),
  };
  app.demands.wake.notify_one();
  demand_answer(&app, changed, id, news, ask.wait_ms, false, true).await
}

/// A demand's answer, held open up to `wait_ms` until its news moves — a card granted, granted again, lent
/// or taken — only while its caller still waits on it or `watch` asks it in any state: any other demand is
/// answered at once. Its id is no longer held where it was given back meanwhile.
async fn demand_answer(
  app: &App,
  mut changed: tokio::sync::watch::Receiver<u64>,
  id: i64,
  news: u64,
  wait_ms: Option<u64>,
  watch: bool,
  created: bool,
) -> Response {
  let deadline = Instant::now() + Duration::from_millis(wait_ms.unwrap_or(0));
  loop {
    let read = app
      .demands
      .book
      .lock()
      .unwrap()
      .held
      .get(&id)
      .map(|held| (held.news, held.answer(now_ms())));
    let Some((now, demand)) = read else {
      return refused(ErrorCode::NotFound, format!("the master holds no demand {id}"));
    };
    let left = deadline.saturating_duration_since(Instant::now());
    let waits = watch || demand.state == DemandState::Waiting;
    if now != news || left.is_zero() || !waits {
      if !created {
        return answered(StatusCode::OK, &demand);
      }
      if demand.state == DemandState::Granted {
        return answered(StatusCode::CREATED, &demand);
      }
      let mut waiting = answered(StatusCode::ACCEPTED, &demand);
      waiting.headers_mut().insert(header::RETRY_AFTER, header::HeaderValue::from_static("1"));
      return waiting;
    }
    // A round that moved nothing of it wakes it to look again; the time running out answers it as it stands.
    let _ = tokio::time::timeout(left, changed.changed()).await;
  }
}

async fn demand(
  State(app): State<App>,
  Named(id): Named<String>,
  query: Result<Query<DemandQuery>, QueryRejection>,
) -> Response {
  let Ok(id) = id.parse::<i64>() else {
    return refused(ErrorCode::BadRequest, format!("{id} is not a demand's id"));
  };
  let query = match query {
    Ok(Query(query)) => query,
    Err(why) => return refused(ErrorCode::BadRequest, format!("the query does not read: {why}")),
  };
  let changed = app.demands.changed.subscribe();
  let news = app.demands.book.lock().unwrap().held.get(&id).map(|held| held.news);
  let Some(news) = news else {
    return refused(ErrorCode::NotFound, format!("the master holds no demand {id}"));
  };
  demand_answer(&app, changed, id, news, query.wait_ms, query.watch.unwrap_or(false), false).await
}

async fn list_demands(State(app): State<App>) -> Response {
  let answers = app.demands.book.lock().unwrap().answers(now_ms());
  answered(StatusCode::OK, &DemandPage::whole(answers))
}

/// A demand given back by its caller, every card of it; the master's next round is woken for it.
async fn give_back_demand(State(app): State<App>, Named(id): Named<String>) -> Response {
  let Ok(id) = id.parse::<i64>() else {
    return refused(ErrorCode::BadRequest, format!("{id} is not a demand's id"));
  };
  if !app.demands.book.lock().unwrap().give_back(id) {
    return refused(ErrorCode::NotFound, format!("the master holds no demand {id}"));
  }
  app.demands.wake.notify_one();
  app.demands.tell();
  StatusCode::NO_CONTENT.into_response()
}

/// Whether a word may name a service: lower-case letters, digits and hyphens.
fn service_word(name: &str) -> bool {
  let mut chars = name.chars();
  chars.next().is_some_and(|first| first.is_ascii_lowercase() || first.is_ascii_digit())
    && chars.all(|one| one.is_ascii_lowercase() || one.is_ascii_digit() || one == '-')
}

/// What a service hands in of its units, held to its shape before it is written: a `units` object whose
/// every name is the service's own and whose every detail reads.
fn description(service: &str, body: &[u8]) -> Result<Description, Refusal> {
  let read: Value = serde_json::from_slice(body)
    .map_err(|fault| Refusal(ErrorCode::BadRequest, format!("the body does not read as JSON: {fault}")))?;
  let units = read
    .get("units")
    .and_then(Value::as_object)
    .ok_or_else(|| Refusal(ErrorCode::BadRequest, "the body holds no units object".to_owned()))?;
  let mut described = Description::default();
  for (unit, detail) in units {
    if !unit.starts_with(&format!("{service}/")) {
      return Err(Refusal(ErrorCode::BadField, format!("{unit} is not a unit of {service}")));
    }
    let detail =
      detail_from(detail).map_err(|fault| Refusal(ErrorCode::BadField, format!("{unit}: {fault}")))?;
    described.units.insert(unit.clone(), detail);
  }
  Ok(described)
}

/// A service's description of its units written whole into the master's store, replacing what it said
/// before (docs/api.md, 「Unit descriptions」); the master reads it again within its next round.
async fn describe(State(app): State<App>, Named(service): Named<String>, body: Bytes) -> Response {
  if !service_word(&service) {
    return refused(
      ErrorCode::NameRule,
      format!("{service} is not a service's name: lower-case letters, digits and hyphens"),
    );
  }
  let described = match description(&service, &body) {
    Ok(described) => described,
    Err(refusal) => return refusal.into_response(),
  };
  let path = app.descriptions.join(format!("{service}.json"));
  let replaced = path.exists();
  let text = serde_json::to_string_pretty(&described).expect("a description serializes");
  match write_whole(&path, &text) {
    Ok(()) => answered(if replaced { StatusCode::OK } else { StatusCode::CREATED }, &described),
    Err(fault) => refused(ErrorCode::Failed, format!("the description was not written: {fault}")),
  }
}

/// The kinds a reader asked for, or why they do not read; every kind where none is named.
fn kinds_of(query: &EventsQuery) -> Result<Vec<EventKind>, String> {
  let Some(named) = query.kinds.as_deref().filter(|named| !named.trim().is_empty()) else {
    return Ok(EventKind::ALL.to_vec());
  };
  named
    .split(',')
    .map(str::trim)
    .map(|name| EventKind::of_name(name).ok_or_else(|| format!("{name} is not a kind of event")))
    .collect()
}

/// The master's one stream, or its page to a reader asking for JSON (docs/api.md, 「Events」).
async fn events(
  State(app): State<App>,
  headers: HeaderMap,
  query: Result<Query<EventsQuery>, QueryRejection>,
) -> Response {
  let query = match query {
    Ok(Query(query)) => query,
    Err(why) => return refused(ErrorCode::BadRequest, format!("the query does not read: {why}")),
  };
  let kinds = match kinds_of(&query) {
    Ok(kinds) => kinds,
    Err(why) => return refused(ErrorCode::BadRequest, why),
  };
  let resumed = headers
    .get("last-event-id")
    .and_then(|value| value.to_str().ok())
    .and_then(|value| value.trim().parse::<i64>().ok());
  let after = query.after.or(resumed);
  let wants_page = headers
    .get(header::ACCEPT)
    .and_then(|value| value.to_str().ok())
    .is_some_and(|accept| accept.contains("application/json"));
  let events = app.events.lock().unwrap();
  if let Some(after) = after
    && !events.resumes(after)
  {
    return refused(
      ErrorCode::Gone,
      format!("event {after} is not within the window the master keeps; read the collections again"),
    );
  }
  let wanted = |event: &Event| kinds.contains(&event.kind) && after.is_none_or(|after| event.id > after);
  if wants_page {
    let limit = query.limit.unwrap_or(EVENTS_KEPT as u32).clamp(1, EVENTS_KEPT as u32) as usize;
    let matching: Vec<Event> = events.kept.iter().filter(|event| wanted(event)).cloned().collect();
    let more = matching.len() > limit;
    let page = EventPage {
      items: matching.into_iter().take(limit).collect(),
      more,
    };
    return answered(StatusCode::OK, &page);
  }
  // A stream opened with no number starts now; one resumed first hears what it missed.
  let backlog: VecDeque<Event> = match after {
    Some(_) => events.kept.iter().filter(|event| wanted(event)).cloned().collect(),
    None => VecDeque::new(),
  };
  let live = events.live.subscribe();
  drop(events);
  Sse::new(stream(backlog, live, kinds))
    .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
    .into_response()
}

/// What a stream sends: the events it missed, then each live one of the kinds it reads, none twice.
fn stream(
  backlog: VecDeque<Event>,
  live: broadcast::Receiver<Event>,
  kinds: Vec<EventKind>,
) -> impl Stream<Item = Result<SseEvent, Infallible>> {
  let sent = backlog.back().map(|event| event.id);
  futures_util::stream::unfold(
    (backlog, live, kinds, sent),
    |(mut backlog, mut live, kinds, mut sent)| async move {
      let event = loop {
        if let Some(event) = backlog.pop_front() {
          break event;
        }
        match live.recv().await {
          Ok(event) if kinds.contains(&event.kind) && sent.is_none_or(|sent| event.id > sent) => {
            break event;
          }
          Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => continue,
          // The master is going down.
          Err(broadcast::error::RecvError::Closed) => return None,
        }
      };
      sent = Some(event.id);
      let frame = SseEvent::default()
        .id(event.id.to_string())
        .event(event.kind.name())
        .data(serde_json::to_string(&event).expect("an event serializes"));
      Some((Ok(frame), (backlog, live, kinds, sent)))
    },
  )
}

async fn no_route() -> Response {
  refused(ErrorCode::NoRoute, "no route answers this method and path")
}

/// The master read once, its picture printed, for `--once`.
pub async fn once(mut master: Master) -> String {
  master.read_rules_if_changed();
  master.discover().await;
  master.poll().await;
  master.finish_runs().await;
  let mut out = Vec::new();
  let formatter = serde_json::ser::PrettyFormatter::with_indent(b" ");
  let mut serializer = serde_json::Serializer::with_formatter(&mut out, formatter);
  serde::Serialize::serialize(&master.state(), &mut serializer).expect("the cluster serializes");
  String::from_utf8(out).expect("serde_json writes UTF-8")
}

/// The master standing: the API up, then a round every `read_every` and the node list read again every
/// `discover_every`, each change of the picture published as `cluster.changed`.
pub async fn serve(mut master: Master, settings: Settings, placer: Placer) -> std::io::Result<()> {
  master.read_rules_if_changed();
  master.attach(placer.board());
  let demands = Arc::new(Demands::default());
  master.attach_demands(demands.clone());
  let descriptions = Arc::new(master.descriptions_folder());
  let (requests, waiting) = mpsc::unbounded_channel();
  master.discover().await;
  let picture = || master.state();
  let app = App {
    cluster: Arc::new(Mutex::new(picture())),
    events: Arc::new(Mutex::new(Events::new())),
    placements: Arc::new(Mutex::new(Placements {
      kept: VecDeque::new(),
      last_id: 0,
    })),
    requests,
    demands: demands.clone(),
    descriptions,
    token: settings.token.as_deref().map(Arc::from),
  };
  tokio::spawn(place_requests(placer, app.clone(), waiting));
  let router = Router::new()
    .route("/health", get(health))
    .route("/events", get(events))
    .route("/cluster", get(cluster))
    .route("/usage", get(usage))
    .route("/placements", post(create_placement))
    .route("/placements/{placementId}", get(placement))
    .route("/demands", post(create_demand).get(list_demands))
    .route("/demands/{demandId}", get(demand).delete(give_back_demand))
    .route("/descriptions/{serviceName}", put(describe))
    .fallback(no_route)
    .method_not_allowed_fallback(no_route)
    .layer(middleware::from_fn_with_state(app.clone(), authorized))
    .with_state(app.clone());
  let listener = tokio::net::TcpListener::bind(("127.0.0.1", settings.port)).await?;
  println!("bmo-arbiter: answering on 127.0.0.1:{}", listener.local_addr()?.port());
  tokio::spawn(async move {
    if let Err(fault) = axum::serve(listener, router).await {
      eprintln!("bmo-arbiter: the API stopped — {fault}");
    }
  });
  let mut published = master.revision();
  app.publish(
    EventKind::ClusterChanged,
    serde_json::to_value(app.cluster.lock().unwrap().clone()).expect("the cluster serializes"),
  );
  let mut discovered = Instant::now();
  loop {
    let began = Instant::now();
    if discovered.elapsed() > settings.discover_every {
      master.discover().await;
      discovered = began;
    }
    master.poll().await;
    if master.revision() != published {
      published = master.revision();
      let now = master.state();
      *app.cluster.lock().unwrap() = now.clone();
      app.publish(EventKind::ClusterChanged, serde_json::to_value(&now).expect("the cluster serializes"));
    }
    let rest = settings
      .read_every
      .saturating_sub(began.elapsed())
      .max(Duration::from_millis(200).min(settings.read_every));
    // A demand asked or given back has the next round come at once.
    tokio::select! {
      () = tokio::time::sleep(rest) => {}
      () = demands.wake.notified() => {}
    }
  }
}
