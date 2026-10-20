//! The MCP server: the harness tools over the master and the node arbiter, each call made through the
//! arbiter client.

use std::sync::Arc;

use rmcp::model::{
  CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, Implementation, InitializeResult,
  JsonObject, ListToolsResult, PaginatedRequestParams, ServerCapabilities, Tool,
};
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, ServerHandler};
use schemars::Schema;
use schemars::generate::SchemaSettings;
use serde_json::{Map, Value};

use crate::tools::{ArbiterTool, Clients, ToolFailure, tools};

/// What a harness does first with the server, as the OpenAPI document's `x-harness` states it.
pub const ENTRY: &str = "A harness about to use a GPU takes a turn with arbiter_ticket_take, waits on \
  arbiter_ticket, and gives it back with arbiter_ticket_return.";

/// What the server tells a harness of itself when it starts.
const INSTRUCTIONS: &str = "Shares the GPU card of this machine with every other process that uses it, and \
  reads the cluster the arbiter's master places services on. Before putting anything on the card, take a \
  turn with arbiter_ticket_take; while its turn is not given, ask arbiter_ticket with waitMs until it is; use \
  the card only while the turn stands, let go once giveWay is true, and give the ticket back with \
  arbiter_ticket_return as soon as the card is empty again. arbiter_card shows the card's queue, claim and \
  units. arbiter_cluster shows every node, and arbiter_placement_request with arbiter_placement put a service \
  on a node or take it off. A refusal comes back as a tool error carrying the status, code and sentence.";

/// The server over the programs the clients reach.
pub struct ArbiterServer {
  clients: Clients,
  tools: Arc<Vec<ArbiterTool>>,
}

impl ArbiterServer {
  pub fn new(clients: Clients) -> Self {
    Self {
      clients,
      tools: Arc::new(tools()),
    }
  }

  /// The tools as MCP lists them, each input and output a schema of its own with its definitions inside.
  pub fn listed(&self) -> Vec<Tool> {
    self.tools.iter().map(listed).collect()
  }
}

fn listed(tool: &ArbiterTool) -> Tool {
  let mut generator = SchemaSettings::draft2020_12()
    .for_serialize()
    .with(|settings| settings.meta_schema = None)
    .into_generator();
  let input = tool.input(&mut generator);
  let output = tool.output(&mut generator);
  let definitions = generator.take_definitions(true);
  let mut listed = Tool::new(tool.name, tool.description, standing_alone(input, &definitions));

  listed.output_schema = Some(Arc::new(standing_alone(output, &definitions)));
  listed
}

/// A schema made to stand on its own: an object at its root, with every definition it refers to beside it.
fn standing_alone(schema: Schema, definitions: &Map<String, Value>) -> JsonObject {
  let mut object = match schema.to_value() {
    Value::Object(object) if object.contains_key("$ref") => {
      let mut wrapped = Map::new();

      wrapped.insert("allOf".into(), Value::Array(vec![Value::Object(object)]));
      wrapped.insert("type".into(), "object".into());
      wrapped
    }
    Value::Object(object) => object,
    _ => unreachable!("every tool's input and output is an object"),
  };

  if !definitions.is_empty() {
    object.insert("$defs".into(), Value::Object(definitions.clone()));
  }

  object
}

impl ServerHandler for ArbiterServer {
  fn get_info(&self) -> InitializeResult {
    let mut info = InitializeResult::new(ServerCapabilities::builder().enable_tools().build());
    let mut server = Implementation::default();

    server.name = "bmo-arbiter".to_owned();
    server.version = env!("CARGO_PKG_VERSION").to_owned();
    info.server_info = server;
    info.instructions = Some(INSTRUCTIONS.to_owned());
    info
  }

  async fn list_tools(
    &self,
    _: Option<PaginatedRequestParams>,
    _: RequestContext<RoleServer>,
  ) -> Result<ListToolsResult, ErrorData> {
    Ok(ListToolsResult::with_all_items(self.listed()))
  }

  async fn call_tool(
    &self,
    request: CallToolRequestParams,
    _: RequestContext<RoleServer>,
  ) -> Result<CallToolResponse, ErrorData> {
    let Some(tool) = self.tools.iter().find(|tool| tool.name == request.name) else {
      return Err(ErrorData::invalid_params(format!("there is no tool {}", request.name), None));
    };
    let arguments = Value::Object(request.arguments.unwrap_or_default());
    let result = match tool.call(&self.clients, arguments).await {
      Ok(answer) => CallToolResult::structured(answer),
      Err(failure) => failed(&failure),
    };

    Ok(CallToolResponse::Complete(result))
  }
}

/// A failed call as a tool error whose text is the failure's content. It carries no structured content,
/// which would be read against the tool's output schema.
fn failed(failure: &ToolFailure) -> CallToolResult {
  CallToolResult::error(vec![ContentBlock::text(failure.content().to_string())])
}
