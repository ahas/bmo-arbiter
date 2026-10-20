//! The harness tools (docs/api.md, 「The clients and the harness plugin」): one for each route a harness takes
//! a turn on a card or reads the cluster through, each a thin call of one method of the arbiter client. The
//! MCP server lists them and the OpenAPI document states them under `x-tools`, both from this one table.

use std::future::Future;
use std::pin::Pin;

use bmo_arbiter_client::card::{TicketAsk, TicketQuery};
use bmo_arbiter_client::placements::PlacementAsk;
use bmo_arbiter_client::{ArbiterClient, CallError};
use schemars::generate::SchemaGenerator;
use schemars::{JsonSchema, Schema};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// What a tool's call came to short of the route's answer.
#[derive(Debug)]
pub enum ToolFailure {
  /// The arguments are not the shape the tool takes.
  Arguments(String),
  /// The call reached no answer of the route's shape.
  Call(CallError),
}

impl ToolFailure {
  /// The failure as the tool error's content: the error the program answered with and its status, or the
  /// sentence saying why nothing answered.
  pub fn content(&self) -> Value {
    match self {
      Self::Arguments(why) => json!({ "code": "bad-request", "error": why }),
      Self::Call(CallError::Answered { status, error }) => {
        json!({ "status": status, "code": error.code, "error": error.error })
      }
      Self::Call(failure @ CallError::Unreadable { status, .. }) => {
        json!({ "status": status, "error": failure.to_string() })
      }
      Self::Call(failure) => json!({ "error": failure.to_string() }),
    }
  }
}

/// Which program a tool's route is served by.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Program {
  /// The master, one for the system.
  Master,
  /// The node arbiter on the harness's own machine.
  Node,
}

/// The two programs a harness reaches, each through its own client.
#[derive(Clone)]
pub struct Clients {
  pub master: ArbiterClient,
  pub node: ArbiterClient,
}

impl Clients {
  fn of(&self, program: Program) -> ArbiterClient {
    match program {
      Program::Master => self.master.clone(),
      Program::Node => self.node.clone(),
    }
  }
}

/// A tool's answer: the route's answer as JSON, or why there is none.
pub type Answered = Pin<Box<dyn Future<Output = Result<Value, ToolFailure>> + Send>>;

type Call = Box<dyn Fn(ArbiterClient, Value) -> Answered + Send + Sync>;

/// One tool: its name, what it does, the program and route it calls, its input and output, and the call
/// itself.
pub struct ArbiterTool {
  pub name: &'static str,
  pub description: &'static str,
  pub program: Program,
  /// The route the tool calls, as its method and path.
  pub route: &'static str,
  input: fn(&mut SchemaGenerator) -> Schema,
  output: fn(&mut SchemaGenerator) -> Schema,
  call: Call,
}

impl ArbiterTool {
  /// The tool's input, its nested types referred to where `generator` keeps its definitions.
  pub fn input(&self, generator: &mut SchemaGenerator) -> Schema {
    (self.input)(generator)
  }

  /// The tool's output, referred to where `generator` keeps its definitions.
  pub fn output(&self, generator: &mut SchemaGenerator) -> Schema {
    (self.output)(generator)
  }

  /// Calls the tool's route on the program it is served by, with the tool's arguments.
  pub fn call(&self, clients: &Clients, arguments: Value) -> Answered {
    (self.call)(clients.of(self.program), arguments)
  }
}

/// A tool whose input is `I` and whose answer is `O`, calling `call` with the arguments read as `I`.
fn tool<I, O, F, Fut>(
  name: &'static str,
  description: &'static str,
  program: Program,
  route: &'static str,
  call: F,
) -> ArbiterTool
where
  I: DeserializeOwned + JsonSchema,
  O: Serialize + JsonSchema,
  F: Fn(ArbiterClient, I) -> Fut + Send + Sync + 'static,
  Fut: Future<Output = Result<O, CallError>> + Send + 'static,
{
  ArbiterTool {
    name,
    description,
    program,
    route,
    input: I::json_schema,
    output: SchemaGenerator::subschema_for::<O>,
    call: Box::new(move |client, arguments| {
      let input = match serde_json::from_value::<I>(arguments) {
        Ok(input) => input,
        Err(why) => {
          let why = format!("the arguments are not the shape this tool takes: {why}");

          return Box::pin(std::future::ready(Err(ToolFailure::Arguments(why))));
        }
      };
      let answer = call(client, input);

      Box::pin(async move {
        let answer = answer.await.map_err(ToolFailure::Call)?;

        Ok(serde_json::to_value(answer).expect("a route's answer always renders as JSON"))
      })
    }),
  }
}

/// No input.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Nothing {}

/// A body sent as it is.
#[derive(Deserialize, JsonSchema)]
#[schemars(inline)]
struct Body<T> {
  body: T,
}

/// One placement, by its id.
#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct OnePlacement {
  placement_id: i64,
}

/// One ticket, by its id.
#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct OneTicket {
  ticket_id: i64,
}

/// One ticket asked after: the ticket, how long to hold the answer open, and the next request's wait.
#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct TicketAskedAfter {
  ticket_id: i64,
  #[serde(flatten)]
  query: TicketQuery,
}

/// A ticket given back.
#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct Returned {
  /// The ticket given back.
  ticket_id: i64,
}

/// The seven tools, in the order docs/api.md lists them.
pub fn tools() -> Vec<ArbiterTool> {
  vec![
    tool(
      "arbiter_cluster",
      "The whole picture: groups, nodes, cards, units, queues, declarations.",
      Program::Master,
      "GET /cluster",
      |client, Nothing {}| async move { client.cluster().await },
    ),
    tool(
      "arbiter_placement_request",
      "Ask a unit placed on or removed from a node; progress lines follow on the member and the stream.",
      Program::Master,
      "POST /placements",
      |client, Body { body }: Body<PlacementAsk>| async move { client.create_placement(&body).await },
    ),
    tool(
      "arbiter_placement",
      "One placement request and its lines.",
      Program::Master,
      "GET /placements/{placementId}",
      |client, OnePlacement { placement_id }| async move { client.placement(placement_id).await },
    ),
    tool(
      "arbiter_card",
      "The node's card: occupants, ranks, claim, queue, units.",
      Program::Node,
      "GET /card",
      |client, Nothing {}| async move { client.card().await },
    ),
    tool(
      "arbiter_ticket_take",
      "Ask for a turn on the card; the ticket comes back with its turn given, or with its place in line.",
      Program::Node,
      "POST /tickets",
      |client, Body { body }: Body<TicketAsk>| async move { client.create_ticket(&body).await },
    ),
    tool(
      "arbiter_ticket",
      "A ticket as it stands, holding the answer open with waitMs.",
      Program::Node,
      "GET /tickets/{ticketId}",
      |client, TicketAskedAfter { ticket_id, query }| async move { client.ticket(ticket_id, &query).await },
    ),
    tool(
      "arbiter_ticket_return",
      "Give a ticket back.",
      Program::Node,
      "DELETE /tickets/{ticketId}",
      |client, OneTicket { ticket_id }| async move {
        client.delete_ticket(ticket_id).await.map(|()| Returned { ticket_id })
      },
    ),
  ]
}
