//! The contract: the committed `docs/openapi.json` is what the code generates, and it states every route,
//! event and tool docs/api.md lists, the harness plugin among them.

use std::path::Path;

use serde_json::Value;

fn committed() -> String {
  std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("docs/openapi.json")).unwrap()
}

#[test]
fn the_committed_document_is_what_the_code_generates() {
  assert!(
    committed() == bmo_arbiter_plugin::openapi_json(),
    "docs/openapi.json differs from the code; run cargo run -p bmo-arbiter-plugin --example openapi > docs/openapi.json"
  );
}

#[test]
fn the_document_states_the_routes_events_and_tools_of_the_api_document() {
  let document: Value = serde_json::from_str(&committed()).unwrap();
  let mut routes: Vec<String> = document["paths"]
    .as_object()
    .unwrap()
    .iter()
    .flat_map(|(path, methods)| {
      methods
        .as_object()
        .unwrap()
        .keys()
        .map(move |method| format!("{} {path}", method.to_uppercase()))
    })
    .collect();
  routes.sort();
  assert_eq!(
    routes,
    [
      "DELETE /demands/{demandId}",
      "DELETE /tickets/{ticketId}",
      "GET /card",
      "GET /card-units",
      "GET /cluster",
      "GET /demands",
      "GET /demands/{demandId}",
      "GET /events",
      "GET /health",
      "GET /placements/{placementId}",
      "GET /tickets/{ticketId}",
      "GET /usage",
      "PATCH /card-units/{unitName}",
      "PATCH /tickets/{ticketId}",
      "POST /demands",
      "POST /placements",
      "POST /tickets",
      "PUT /card/places",
      "PUT /descriptions/{serviceName}",
    ]
  );
  assert_eq!(
    document["x-events"].as_object().unwrap().keys().collect::<Vec<_>>(),
    ["cluster.changed", "placement.changed", "placement.created"]
  );
  assert_eq!(
    (document["x-harness"]["plugin"].as_str(), document["x-harness"]["mcp"].as_str()),
    (Some("bmo-arbiter"), Some("bmo-arbiter mcp"))
  );
  assert_eq!(document["x-calls"], serde_json::json!(["node"]));
  assert_eq!(document["info"]["version"], env!("CARGO_PKG_VERSION"));
  let tools: Vec<&str> = document["x-tools"]
    .as_array()
    .unwrap()
    .iter()
    .map(|tool| tool["name"].as_str().unwrap())
    .collect();
  assert_eq!(
    tools,
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
  // Every tool calls a route the document states.
  for tool in document["x-tools"].as_array().unwrap() {
    let route = tool["route"].as_str().unwrap();
    assert!(routes.iter().any(|one| one == route), "{route}");
  }
}

#[test]
fn the_typescript_package_carries_the_repositorys_version() {
  let package: Value = serde_json::from_str(
    &std::fs::read_to_string(
      Path::new(env!("CARGO_MANIFEST_DIR")).join("packages/arbiter-client/package.json"),
    )
    .unwrap(),
  )
  .unwrap();
  assert_eq!(package["version"], env!("CARGO_PKG_VERSION"));
}
