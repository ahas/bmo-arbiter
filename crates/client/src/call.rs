//! A call for every route of either program (docs/api.md, 「What the master serves」 and 「What the node
//! arbiter serves」), each sending the route's body and reading its answer as the route's type, and the
//! master's event stream read as events. One client reaches one program, at the base address it is given.

use std::fmt;

use futures_util::{Stream, StreamExt};
use reqwest::header::ACCEPT;
use reqwest::{Method, RequestBuilder, Url};
use serde::Serialize;
use serde::de::DeserializeOwned;
use sse_stream::SseStream;

use crate::card::{
  Card, CardUnit, CardUnitChanges, CardUnitPage, Ticket, TicketAsk, TicketChanges, TicketQuery,
};
use crate::cluster::Cluster;
use crate::demands::{Demand, DemandAsk, DemandPage, DemandQuery};
use crate::descriptions::Description;
use crate::error::WireError;
use crate::events::{Event, EventPage, EventsQuery, Health};
use crate::placements::{Placement, PlacementAsk};

/// Why a call did not come back with its route's answer.
#[derive(Debug)]
pub enum CallError {
  /// The program answered in the error shape.
  Answered { status: u16, error: WireError },
  /// The program answered something that is neither the route's answer nor the error shape.
  Unreadable { status: u16, body: String },
  /// The request did not reach the program, or its answer did not arrive whole.
  Transport(reqwest::Error),
}

impl CallError {
  /// A status other than success, with the body it came with: the error shape, or what was said instead.
  fn not_answered(status: u16, body: String) -> Self {
    match serde_json::from_str::<WireError>(&body) {
      Ok(error) => Self::Answered { status, error },
      Err(_) => Self::Unreadable { status, body },
    }
  }

  /// The error the program answered, where it answered one.
  pub fn wire(&self) -> Option<&WireError> {
    match self {
      Self::Answered { error, .. } => Some(error),
      _ => None,
    }
  }
}

impl fmt::Display for CallError {
  fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::Answered { status, error } => write!(out, "the arbiter answered {status}: {error}"),
      Self::Unreadable { status, body } => write!(out, "the arbiter answered {status} unreadably: {body}"),
      Self::Transport(why) => write!(out, "the arbiter could not be asked: {why}"),
    }
  }
}

impl std::error::Error for CallError {}

impl From<reqwest::Error> for CallError {
  fn from(why: reqwest::Error) -> Self {
    Self::Transport(why)
  }
}

pub type CallResult<T> = Result<T, CallError>;

/// One program of the arbiter at one base address, asked with a token where it is given one.
#[derive(Clone)]
pub struct ArbiterClient {
  base: Url,
  token: Option<String>,
  http: reqwest::Client,
}

impl ArbiterClient {
  /// A client of the program at `base`, an absolute HTTP address, with a client of its own.
  pub fn new(base: &str, token: Option<String>) -> Result<Self, String> {
    Self::with_http(base, token, reqwest::Client::new())
  }

  /// A client of the program at `base` over the HTTP client given, which sets how it connects.
  pub fn with_http(base: &str, token: Option<String>, http: reqwest::Client) -> Result<Self, String> {
    let base = Url::parse(base).map_err(|why| format!("{base} is not an address: {why}"))?;

    if base.cannot_be_a_base() {
      return Err(format!("{base} cannot carry a path"));
    }

    Ok(Self {
      base,
      token: token.filter(|token| !token.is_empty()),
      http,
    })
  }

  /// The address of a path, its segments given apart so each is escaped as one.
  fn url(&self, segments: &[&str]) -> Url {
    let mut url = self.base.clone();

    if let Ok(mut path) = url.path_segments_mut() {
      path.pop_if_empty().extend(segments);
    }

    url
  }

  fn request(&self, method: Method, segments: &[&str]) -> RequestBuilder {
    let request = self.http.request(method, self.url(segments));

    match &self.token {
      Some(token) => request.bearer_auth(token),
      None => request,
    }
  }

  async fn answer<T: DeserializeOwned>(request: RequestBuilder) -> CallResult<T> {
    let answer = request.send().await?;
    let status = answer.status();
    let body = answer.text().await?;

    if status.is_success() {
      return serde_json::from_str(&body).map_err(|_| CallError::Unreadable {
        status: status.as_u16(),
        body,
      });
    }

    Err(CallError::not_answered(status.as_u16(), body))
  }

  async fn get<T: DeserializeOwned>(&self, segments: &[&str]) -> CallResult<T> {
    Self::answer(self.request(Method::GET, segments)).await
  }

  async fn send<T: DeserializeOwned>(
    &self,
    method: Method,
    segments: &[&str],
    body: &impl Serialize,
  ) -> CallResult<T> {
    Self::answer(self.request(method, segments).json(body)).await
  }

  /// A request answered with no body, as a removal is.
  async fn remove(&self, segments: &[&str]) -> CallResult<()> {
    let answer = self.request(Method::DELETE, segments).send().await?;
    let status = answer.status();

    if status.is_success() {
      return Ok(());
    }

    Err(CallError::not_answered(status.as_u16(), answer.text().await?))
  }

  /// `GET /health`, of either program.
  pub async fn health(&self) -> CallResult<Health> {
    self.get(&["health"]).await
  }

  /// `GET /cluster`, of the master.
  pub async fn cluster(&self) -> CallResult<Cluster> {
    self.get(&["cluster"]).await
  }

  /// `POST /placements`, of the master.
  pub async fn create_placement(&self, ask: &PlacementAsk) -> CallResult<Placement> {
    self.send(Method::POST, &["placements"], ask).await
  }

  /// `GET /placements/{placementId}`, of the master.
  pub async fn placement(&self, placement_id: i64) -> CallResult<Placement> {
    self.get(&["placements", &placement_id.to_string()]).await
  }

  /// `POST /demands`, of the master: the demand with every card it asks granted, or waiting for any.
  pub async fn create_demand(&self, ask: &DemandAsk) -> CallResult<Demand> {
    self.send(Method::POST, &["demands"], ask).await
  }

  /// `GET /demands`, of the master.
  pub async fn demands(&self) -> CallResult<DemandPage> {
    self.get(&["demands"]).await
  }

  /// `GET /demands/{demandId}`, of the master.
  pub async fn demand(&self, demand_id: i64, query: &DemandQuery) -> CallResult<Demand> {
    Self::answer(self.request(Method::GET, &["demands", &demand_id.to_string()]).query(query)).await
  }

  /// `DELETE /demands/{demandId}`, of the master: the demand given back.
  pub async fn delete_demand(&self, demand_id: i64) -> CallResult<()> {
    self.remove(&["demands", &demand_id.to_string()]).await
  }

  /// `PUT /descriptions/{serviceName}`, of the master.
  pub async fn put_description(
    &self,
    service_name: &str,
    description: &Description,
  ) -> CallResult<Description> {
    self.send(Method::PUT, &["descriptions", service_name], description).await
  }

  /// `GET /events` of the master, asked as a page, for a caller that polls.
  pub async fn event_page(&self, query: &EventsQuery) -> CallResult<EventPage> {
    let request = self.request(Method::GET, &["events"]).query(query).header(ACCEPT, "application/json");

    Self::answer(request).await
  }

  /// `GET /events` of the master as the stream of server-sent events, from the event `query.after` names or
  /// from now.
  pub async fn events(
    &self,
    query: &EventsQuery,
  ) -> CallResult<impl Stream<Item = CallResult<Event>> + use<>> {
    let answer = self.request(Method::GET, &["events"]).query(query).send().await?;
    let status = answer.status();

    if !status.is_success() {
      return Err(CallError::not_answered(status.as_u16(), answer.text().await?));
    }

    let events = SseStream::from_bytes_stream(answer.bytes_stream()).filter_map(|event| async move {
      let event = match event {
        Ok(event) => event,
        Err(why) => {
          return Some(Err(CallError::Unreadable {
            status: 200,
            body: why.to_string(),
          }));
        }
      };
      // A comment line carries no data.
      let data = event.data?;

      Some(serde_json::from_str::<Event>(&data).map_err(|_| CallError::Unreadable {
        status: 200,
        body: data,
      }))
    });

    Ok(events)
  }

  /// `GET /card`, of the node arbiter.
  pub async fn card(&self) -> CallResult<Card> {
    self.get(&["card"]).await
  }

  /// `POST /tickets`, of the node arbiter: the ticket with its turn given, or with its place in line.
  pub async fn create_ticket(&self, ask: &TicketAsk) -> CallResult<Ticket> {
    self.send(Method::POST, &["tickets"], ask).await
  }

  /// `GET /tickets/{ticketId}`, of the node arbiter.
  pub async fn ticket(&self, ticket_id: i64, query: &TicketQuery) -> CallResult<Ticket> {
    Self::answer(self.request(Method::GET, &["tickets", &ticket_id.to_string()]).query(query)).await
  }

  /// `PATCH /tickets/{ticketId}`, of the node arbiter.
  pub async fn change_ticket(&self, ticket_id: i64, changes: &TicketChanges) -> CallResult<Ticket> {
    self.send(Method::PATCH, &["tickets", &ticket_id.to_string()], changes).await
  }

  /// `DELETE /tickets/{ticketId}`, of the node arbiter.
  pub async fn delete_ticket(&self, ticket_id: i64) -> CallResult<()> {
    self.remove(&["tickets", &ticket_id.to_string()]).await
  }

  /// `GET /card-units`, of the node arbiter.
  pub async fn card_units(&self) -> CallResult<CardUnitPage> {
    self.get(&["card-units"]).await
  }

  /// `PATCH /card-units/{unitName}`, of the node arbiter.
  pub async fn change_card_unit(&self, unit_name: &str, changes: &CardUnitChanges) -> CallResult<CardUnit> {
    self.send(Method::PATCH, &["card-units", unit_name], changes).await
  }
}
