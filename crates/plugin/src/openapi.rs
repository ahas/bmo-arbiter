//! The OpenAPI document of the arbiter (docs/api.md, 「The clients and the harness plugin」): every route of
//! the master and of the node arbiter docs/api.md lists, tagged by the program that serves it, with its path
//! and query parameters, its request body, its answer and the error answers it can give; the master's events
//! under `x-events`, the harness plugin under `x-harness` and its tools under `x-tools`. The body schemas come
//! from the client's types, each described as its sender writes it.

use std::collections::BTreeMap;

use bmo_arbiter_client::card::{
  Card, CardPlaces, CardUnit, CardUnitChanges, CardUnitPage, Ticket, TicketAsk, TicketChanges, TicketQuery,
};
use bmo_arbiter_client::cluster::Cluster;
use bmo_arbiter_client::demands::{Demand, DemandAsk, DemandPage, DemandQuery};
use bmo_arbiter_client::descriptions::Description;
use bmo_arbiter_client::events::{EventKind, EventPage, EventsQuery, Health};
use bmo_arbiter_client::placements::{Placement, PlacementAsk};
use bmo_arbiter_client::usage::Usage;
use bmo_arbiter_client::{ErrorCode, WireError};
use schemars::generate::{SchemaGenerator, SchemaSettings};
use schemars::{JsonSchema, Schema};
use serde_json::{Map, Value, json};

use crate::server::ENTRY;
use crate::tools::tools;

/// Where the schemas the routes refer to stand in the document.
const SCHEMAS: &str = "/components/schemas";

const DESCRIPTION: &str = "The card arbiter: the master, one for the system, which pictures the cluster, \
  places services on nodes, grants cards of a service as demands ask them and keeps what each service says \
  of its units; and the node arbiter on every GPU \
  node, which hands out turns on its card as tickets and moves the card's units in and out of its memory. A \
  route's tag names the program that serves it. Bodies are JSON with camelCase keys, times are integers of \
  epoch milliseconds and spans integers of milliseconds. The master's routes take its bearer token where it \
  is configured with one; the node arbiter's answer on the node's loopback and take none. Every failure and \
  refusal is a non-2xx status with a body of `code` and `error`. A page is `items` and `more`. Any other \
  method and path is answered 404 `no-route`.";

/// The arbiter's OpenAPI document.
pub fn openapi() -> Value {
  let mut generator = SchemaSettings::draft2020_12()
    .for_serialize()
    .with(|settings| {
      settings.definitions_path = SCHEMAS.into();
      settings.meta_schema = None;
    })
    .into_generator();
  let error = generator.subschema_for::<WireError>();
  let mut paths: BTreeMap<&str, Map<String, Value>> = BTreeMap::new();

  for route in routes(&mut generator) {
    paths
      .entry(route.path)
      .or_default()
      .insert(route.method.to_owned(), route.operation(&error));
  }

  let events: Map<String, Value> = EventKind::ALL
    .into_iter()
    .map(|kind| {
      let schema = match kind {
        EventKind::ClusterChanged => generator.subschema_for::<Cluster>(),
        EventKind::PlacementCreated | EventKind::PlacementChanged => generator.subschema_for::<Placement>(),
      };

      (kind.name().to_owned(), json!({ "data": schema.to_value() }))
    })
    .collect();
  let tools: Vec<Value> = tools()
    .iter()
    .map(|tool| {
      json!({
        "name": tool.name,
        "description": tool.description,
        "input": tool.input(&mut generator).to_value(),
        "output": tool.output(&mut generator).to_value(),
        "route": tool.route,
      })
    })
    .collect();
  let responses: Map<String, Value> = ErrorCode::ALL
    .into_iter()
    .map(|code| (response_name(code), answered(&error, &[code])))
    .collect();

  json!({
    "openapi": "3.1.0",
    "info": {
      "title": "bmo-arbiter",
      "version": env!("CARGO_PKG_VERSION"),
      "description": DESCRIPTION,
    },
    "servers": [
      { "url": "/", "description": "the master, or the node arbiter on the caller's own machine, as the tag says" },
    ],
    "security": [{ "bearer": [] }],
    "tags": [
      { "name": "master", "description": "The master: the cluster, placements, demands and unit descriptions." },
      { "name": "node", "description": "The node arbiter: the card, its tickets and its units." },
    ],
    "x-layer": "upstream",
    "x-calls": ["node"],
    "x-harness": {
      "plugin": "bmo-arbiter",
      "mcp": "bmo-arbiter mcp",
      "entry": ENTRY,
    },
    "paths": paths,
    "x-events": events,
    "x-tools": tools,
    "components": {
      "schemas": generator.take_definitions(true),
      "responses": responses,
      "securitySchemes": {
        "bearer": {
          "type": "http",
          "scheme": "bearer",
          "description": "The master's token, from the caller's own configuration, where the master is configured \
            with one.",
        },
      },
    },
  })
}

/// The document as the repository keeps it: two-space indented JSON with a closing newline.
pub fn openapi_json() -> String {
  let mut text = serde_json::to_string_pretty(&openapi()).expect("a JSON value always renders");

  text.push('\n');
  text
}

/// An answer in the error shape with one of these codes, all answered with one status.
fn answered(error: &Schema, codes: &[ErrorCode]) -> Value {
  let meaning = |code: ErrorCode| format!("`{code}`: {}", code.meaning());
  let description = match codes {
    [code] => meaning(*code),
    _ => codes.iter().map(|&code| format!("- {}", meaning(code))).collect::<Vec<_>>().join("\n"),
  };

  json!({
    "description": description,
    "content": { "application/json": { "schema": error } },
  })
}

/// The name the answer of one code stands under in the document: the code's own name in Rust.
fn response_name(code: ErrorCode) -> String {
  format!("{code:?}")
}

/// Which program serves a route.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Served {
  Master,
  Node,
  /// Both programs, each at its own address.
  Both,
}

/// What a route answers on success.
enum Answer {
  /// 200 and a body.
  Read(Schema),
  /// 201 and the member made.
  Made(Schema),
  /// 201 where the member is made, 200 where it is replaced.
  Put(Schema),
  /// 201 with the ticket whose turn is given, 202 with the ticket that waits.
  Ticketed(Schema),
  /// 201 with the demand whose every card is granted, 202 with the demand that waits.
  Demanded(Schema),
  /// 204 and no body.
  Nothing,
  /// The event stream, or its page to a reader asking for JSON.
  Events(Schema),
}

/// One route as the document states it.
struct Route {
  method: &'static str,
  path: &'static str,
  served: Served,
  id: &'static str,
  summary: &'static str,
  /// The path and query parameters, as OpenAPI parameter objects.
  parameters: Vec<Value>,
  body: Option<Schema>,
  answer: Answer,
  /// Whether the route can answer 500 `failed`.
  can_fail: bool,
  /// The refusals the route can answer, each with its code's status.
  refuses: Vec<ErrorCode>,
}

impl Route {
  fn new(
    method: &'static str,
    path: &'static str,
    served: Served,
    id: &'static str,
    summary: &'static str,
    answer: Answer,
  ) -> Self {
    Self {
      method,
      path,
      served,
      id,
      summary,
      parameters: Vec::new(),
      body: None,
      answer,
      can_fail: true,
      refuses: Vec::new(),
    }
  }

  /// A path parameter of type `T`, written `{name}` in the path.
  fn at<T: JsonSchema>(mut self, generator: &mut SchemaGenerator, name: &str, description: &str) -> Self {
    self.parameters.push(json!({
      "name": name,
      "in": "path",
      "required": true,
      "description": description,
      "schema": generator.subschema_for::<T>(),
    }));
    self
  }

  /// The query parameters, one for each field of the query's type.
  fn query<Query: JsonSchema>(mut self, generator: &mut SchemaGenerator) -> Self {
    let schema = generator.root_schema_for::<Query>();
    let required: Vec<&str> = schema
      .get("required")
      .and_then(Value::as_array)
      .map(|names| names.iter().filter_map(Value::as_str).collect())
      .unwrap_or_default();
    let properties = schema.get("properties").and_then(Value::as_object).cloned().unwrap_or_default();

    for (name, mut property) in properties {
      let description = property.as_object_mut().and_then(|fields| fields.remove("description"));

      // A query parameter is sent or left out, never sent as null.
      if let Some(Value::Array(types)) = property.get_mut("type") {
        types.retain(|one| one != "null");

        if let [only] = types.as_slice() {
          property["type"] = only.clone();
        }
      }
      let mut parameter = json!({
        "name": name,
        "in": "query",
        "required": required.contains(&name.as_str()),
        "schema": property,
      });

      if let Some(description) = description {
        parameter["description"] = description;
      }

      self.parameters.push(parameter);
    }

    self
  }

  fn body<Body: JsonSchema>(mut self, generator: &mut SchemaGenerator) -> Self {
    self.body = Some(generator.subschema_for::<Body>());
    self
  }

  fn never_fails(mut self) -> Self {
    self.can_fail = false;
    self
  }

  fn refuses(mut self, codes: &[ErrorCode]) -> Self {
    self.refuses.extend_from_slice(codes);
    self
  }

  /// The route's operation. A status that one code answers refers to that code's answer; a status that
  /// several share describes them together.
  fn operation(self, error: &Schema) -> Value {
    let mut codes = Vec::new();

    if self.body.is_some() || self.parameters.iter().any(|parameter| parameter["in"] == "query") {
      codes.push(ErrorCode::BadRequest);
    }

    if self.served != Served::Node {
      codes.push(ErrorCode::Unauthorized);
    }

    if self.can_fail {
      codes.push(ErrorCode::Failed);
    }

    let mut statuses: BTreeMap<u16, Vec<ErrorCode>> = BTreeMap::new();

    for &code in codes.iter().chain(&self.refuses) {
      let shared = statuses.entry(code.status()).or_default();

      if !shared.contains(&code) {
        shared.push(code);
      }
    }

    let member = |description: &str, schema: &Schema| {
      json!({
        "description": description,
        "content": { "application/json": { "schema": schema } },
      })
    };
    let mut responses = Map::new();

    match &self.answer {
      Answer::Read(schema) => {
        responses.insert("200".into(), member(self.summary, schema));
      }
      Answer::Made(schema) => {
        responses.insert("201".into(), member(self.summary, schema));
      }
      Answer::Put(schema) => {
        responses.insert("200".into(), member("Replaced what was said before.", schema));
        responses.insert("201".into(), member("Said for the first time.", schema));
      }
      Answer::Ticketed(schema) => {
        responses.insert("201".into(), member("The ticket, its turn given.", schema));
        let mut waiting = member("The ticket and its place in line.", schema);

        waiting["headers"] = json!({
          "Retry-After": {
            "description": "The ticket's `waitMs` in whole seconds, rounded up, at least 1.",
            "schema": { "type": "integer", "minimum": 1 },
          },
        });
        responses.insert("202".into(), waiting);
      }
      Answer::Demanded(schema) => {
        responses.insert("201".into(), member("The demand, every card it asks granted.", schema));
        let mut waiting = member("The demand, waiting for a card.", schema);

        waiting["headers"] = json!({
          "Retry-After": {
            "description": "1.",
            "schema": { "type": "integer", "minimum": 1 },
          },
        });
        responses.insert("202".into(), waiting);
      }
      Answer::Nothing => {
        responses.insert("204".into(), json!({ "description": "Done." }));
      }
      Answer::Events(page) => {
        responses.insert(
          "200".into(),
          json!({
            "description": "Server-sent events, each named by its kind with its number as its id and its data an \
              Event; or, to a reader asking `Accept: application/json`, a page of the same events.",
            "content": {
              "text/event-stream": { "schema": { "$ref": format!("#{SCHEMAS}/Event") } },
              "application/json": { "schema": page },
            },
          }),
        );
      }
    }

    for (status, codes) in statuses {
      let answer = match codes.as_slice() {
        [code] => json!({ "$ref": format!("#/components/responses/{}", response_name(*code)) }),
        _ => answered(error, &codes),
      };

      responses.insert(status.to_string(), answer);
    }

    let tags = match self.served {
      Served::Master => json!(["master"]),
      Served::Node => json!(["node"]),
      Served::Both => json!(["master", "node"]),
    };
    let mut operation = json!({
      "tags": tags,
      "operationId": self.id,
      "summary": self.summary,
      "responses": responses,
    });

    match self.served {
      Served::Node => operation["security"] = json!([]),
      // Each program answers its health at its own address, the master's taking the token, the node's none.
      Served::Both => operation["security"] = json!([{}, { "bearer": [] }]),
      Served::Master => {}
    }

    if !self.parameters.is_empty() {
      operation["parameters"] = self.parameters.into();
    }

    if let Some(body) = self.body {
      operation["requestBody"] = json!({
        "required": true,
        "content": { "application/json": { "schema": body } },
      });
    }

    operation
  }
}

fn read<T: JsonSchema>(generator: &mut SchemaGenerator) -> Answer {
  Answer::Read(generator.subschema_for::<T>())
}

fn made<T: JsonSchema>(generator: &mut SchemaGenerator) -> Answer {
  Answer::Made(generator.subschema_for::<T>())
}

/// Every route of docs/api.md.
fn routes(generator: &mut SchemaGenerator) -> Vec<Route> {
  use ErrorCode::{BadField, Conflict, Gone, NameRule, NoPlace, NotFound, NotReady};
  use Served::{Both, Master, Node};

  const DEMAND: &str = "The demand's id.";
  const PLACEMENT: &str = "The placement's id.";
  const SERVICE: &str = "The service's name: lower-case letters, digits and hyphens.";
  const TICKET: &str = "The ticket's id.";
  const UNIT: &str = "The unit's name, `<service>/<part>`, its slash escaped.";

  vec![
    Route::new(
      "get",
      "/health",
      Both,
      "getHealth",
      "Readiness and version, of the master or of the node arbiter at its own address.",
      read::<Health>(generator),
    )
    .never_fails(),
    Route::new(
      "get",
      "/events",
      Master,
      "streamEvents",
      "The master's stream: the cluster as it changes, and placement progress, resumed with `Last-Event-ID` or \
       `after`.",
      Answer::Events(generator.subschema_for::<EventPage>()),
    )
    .query::<EventsQuery>(generator)
    .never_fails()
    .refuses(&[Gone]),
    Route::new(
      "get",
      "/cluster",
      Master,
      "getCluster",
      "The whole picture: groups, nodes, cards, units, queues, declarations.",
      read::<Cluster>(generator),
    )
    .never_fails(),
    Route::new(
      "get",
      "/usage",
      Master,
      "getUsage",
      "What every card was doing at the master's last reading: its use, the turn or claim holding it, the \
       engine resident on it with its memory, and its waiters.",
      read::<Usage>(generator),
    )
    .never_fails(),
    Route::new(
      "post",
      "/placements",
      Master,
      "createPlacements",
      "Ask a unit placed on or removed from a node; progress lines follow on the member and the stream.",
      made::<Placement>(generator),
    )
    .body::<PlacementAsk>(generator)
    .never_fails(),
    Route::new(
      "get",
      "/placements/{placementId}",
      Master,
      "getPlacement",
      "One placement request and its lines.",
      read::<Placement>(generator),
    )
    .at::<i64>(generator, "placementId", PLACEMENT)
    .never_fails()
    .refuses(&[NotFound]),
    Route::new(
      "post",
      "/demands",
      Master,
      "createDemands",
      "Ask for cards of a service; the master decides which node answers each. 201 with every card granted, \
       202 while any waits.",
      Answer::Demanded(generator.subschema_for::<Demand>()),
    )
    .body::<DemandAsk>(generator)
    .never_fails()
    .refuses(&[NoPlace, NotFound]),
    Route::new(
      "get",
      "/demands",
      Master,
      "listDemands",
      "Every demand the master holds, in the line's order.",
      read::<DemandPage>(generator),
    )
    .never_fails(),
    Route::new(
      "get",
      "/demands/{demandId}",
      Master,
      "getDemand",
      "A demand as it stands, holding the answer open with waitMs while it waits, or in any state with watch; never dropped for it.",
      read::<Demand>(generator),
    )
    .at::<i64>(generator, "demandId", DEMAND)
    .query::<DemandQuery>(generator)
    .never_fails()
    .refuses(&[NotFound]),
    Route::new(
      "delete",
      "/demands/{demandId}",
      Master,
      "deleteDemand",
      "Give a demand back, every card of it.",
      Answer::Nothing,
    )
    .at::<i64>(generator, "demandId", DEMAND)
    .never_fails()
    .refuses(&[NotFound]),
    Route::new(
      "put",
      "/descriptions/{serviceName}",
      Master,
      "putDescription",
      "What a service says of its units, replacing what it said before.",
      Answer::Put(generator.subschema_for::<Description>()),
    )
    .at::<String>(generator, "serviceName", SERVICE)
    .body::<Description>(generator)
    .refuses(&[NameRule, BadField]),
    Route::new(
      "get",
      "/card",
      Node,
      "getCard",
      "The node's card: occupants, ranks, claim, queue, places, units.",
      read::<Card>(generator),
    ),
    Route::new(
      "put",
      "/card/places",
      Node,
      "putCardPlaces",
      "Hand the card every place of the master's demands, replacing those handed before.",
      read::<CardPlaces>(generator),
    )
    .body::<CardPlaces>(generator),
    Route::new(
      "post",
      "/tickets",
      Node,
      "createTickets",
      "Ask for a turn on the card; 201 with the turn or 202 with a place in line.",
      Answer::Ticketed(generator.subschema_for::<Ticket>()),
    )
    .body::<TicketAsk>(generator)
    .refuses(&[NoPlace]),
    Route::new(
      "get",
      "/tickets/{ticketId}",
      Node,
      "getTicket",
      "A ticket as it stands, holding the answer open with waitMs.",
      read::<Ticket>(generator),
    )
    .at::<i64>(generator, "ticketId", TICKET)
    .query::<TicketQuery>(generator)
    .refuses(&[NotFound]),
    Route::new(
      "patch",
      "/tickets/{ticketId}",
      Node,
      "changeTicket",
      "Rest a turn.",
      read::<Ticket>(generator),
    )
    .at::<i64>(generator, "ticketId", TICKET)
    .body::<TicketChanges>(generator)
    .refuses(&[NotFound, Conflict]),
    Route::new("delete", "/tickets/{ticketId}", Node, "deleteTicket", "Give a ticket back.", Answer::Nothing)
      .at::<i64>(generator, "ticketId", TICKET)
      .refuses(&[NotFound]),
    Route::new(
      "get",
      "/card-units",
      Node,
      "listCardUnits",
      "The units on the card.",
      read::<CardUnitPage>(generator),
    ),
    Route::new(
      "patch",
      "/card-units/{unitName}",
      Node,
      "changeCardUnit",
      "Evict a unit to host memory or restore it, with a reason.",
      read::<CardUnit>(generator),
    )
    .at::<String>(generator, "unitName", UNIT)
    .body::<CardUnitChanges>(generator)
    .refuses(&[NotFound, Conflict, NotReady]),
  ]
}
