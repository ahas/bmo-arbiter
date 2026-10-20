//! An answer held to the committed OpenAPI document: a status its route states, and a body of the schema the
//! document states for that status.

#![allow(dead_code)]

use std::path::Path;
use std::sync::LazyLock;

use serde_json::{Value, json};

/// The committed OpenAPI document.
pub static CONTRACT: LazyLock<Value> = LazyLock::new(|| {
  let text =
    std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("docs/openapi.json")).unwrap();
  serde_json::from_str(&text).unwrap()
});

/// The document's path a request's path stands under: each `{name}` segment standing for any one.
fn template(path: &str) -> String {
  let asked: Vec<&str> = path.split('?').next().unwrap_or_default().split('/').collect();
  let paths = CONTRACT["paths"].as_object().unwrap();
  paths
    .keys()
    .find(|template| {
      let parts: Vec<&str> = template.split('/').collect();
      parts.len() == asked.len()
        && parts.iter().zip(&asked).all(|(part, one)| part.starts_with('{') || part == one)
    })
    .cloned()
    .unwrap_or_else(|| panic!("the document states no path for {path}"))
}

/// A response object, its reference followed where it names a shared one.
fn followed(response: &Value) -> &Value {
  match response["$ref"].as_str() {
    Some(reference) => &CONTRACT["components"]["responses"][reference.rsplit('/').next().unwrap()],
    None => response,
  }
}

/// An answer held to the document; one no route answers is the shared `no-route` refusal.
pub fn holds_to_contract(method: &str, path: &str, status: u16, body: &Value) {
  let method = method.to_lowercase();
  if status == 404 && body["code"] == "no-route" {
    assert_eq!(body.as_object().map(|said| said.len()), Some(2), "{method} {path}: {body}");
    return;
  }
  let template = template(path);
  let operation = &CONTRACT["paths"][&template][&method];
  assert!(operation.is_object(), "the document states no {method} {template}");
  let response = followed(&operation["responses"][status.to_string()]);
  assert!(response.is_object(), "{method} {template} states no {status} answer: {body}");
  let Some(schema) = response["content"]["application/json"].get("schema") else {
    assert!(body.is_null(), "{method} {template} answers {status} with no body, not {body}");
    return;
  };
  let rooted = json!({ "components": CONTRACT["components"], "allOf": [schema] });
  let validator = jsonschema::validator_for(&rooted).unwrap();
  let faults: Vec<String> = validator.iter_errors(body).map(|fault| fault.to_string()).collect();
  assert!(
    faults.is_empty(),
    "{method} {path} {status} is not its stated schema: {faults:#?}\n{body}"
  );
}
