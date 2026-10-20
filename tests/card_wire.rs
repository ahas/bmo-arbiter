//! The node arbiter's routes held to docs/openapi.json: `tests/card/wire_samples.py` stands its routes
//! in-process over stand-ins, asks each one and each refusal it can be brought to, and every answer it
//! records is held here to the status and the schema the document states (docs/api.md, 「What the node
//! arbiter serves」).

#[path = "support/contract.rs"]
mod contract;
mod support;

use std::process::Command;

use serde_json::Value;
use support::ROOT;

use contract::holds_to_contract;

#[test]
fn every_answer_of_the_node_arbiters_routes_is_the_documents() {
  let scratch = tempfile::tempdir().unwrap();
  let out = scratch.path().join("wire.json");
  let ran = Command::new("python3")
    .args(["-B", "tests/card/wire_samples.py"])
    .arg(&out)
    .current_dir(ROOT)
    .output()
    .expect("python3 runs");
  assert!(
    ran.status.success(),
    "the samples were not taken:\n{}{}",
    String::from_utf8_lossy(&ran.stdout),
    String::from_utf8_lossy(&ran.stderr)
  );
  let samples: Vec<Value> = serde_json::from_str(&std::fs::read_to_string(&out).unwrap()).unwrap();
  let mut answered: Vec<(String, u64)> = Vec::new();
  for sample in &samples {
    let method = sample["method"].as_str().unwrap();
    let path = sample["path"].as_str().unwrap();
    let status = sample["status"].as_u64().unwrap();
    holds_to_contract(method, path, status as u16, &sample["body"]);
    let route = path.split('?').next().unwrap().split('/').nth(1).unwrap_or_default();
    answered.push((format!("{method} /{route}"), status));
  }
  // Every node route and each documented refusal was asked.
  for expected in [
    ("GET /health", 200),
    ("GET /card", 200),
    ("GET /card-units", 200),
    ("POST /tickets", 202),
    ("POST /tickets", 422),
    ("POST /tickets", 400),
    ("GET /tickets", 200),
    ("GET /tickets", 404),
    ("PATCH /tickets", 200),
    ("PATCH /tickets", 409),
    ("DELETE /tickets", 204),
    ("DELETE /tickets", 404),
    ("PATCH /card-units", 200),
    ("PATCH /card-units", 404),
    ("PATCH /card-units", 409),
    ("GET /nowhere", 404),
  ] {
    assert!(
      answered.iter().any(|(route, status)| route == expected.0 && *status == expected.1),
      "no {} answered {}: {answered:?}",
      expected.0,
      expected.1
    );
  }
}
