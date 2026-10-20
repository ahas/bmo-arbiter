//! Prints the arbiter's OpenAPI document as the repository keeps it:
//! `cargo run -p bmo-arbiter-plugin --example openapi > docs/openapi.json`.

fn main() {
  print!("{}", bmo_arbiter_plugin::openapi_json());
}
