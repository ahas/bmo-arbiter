//! Unit descriptions (docs/api.md, 「Unit descriptions」): what a service says each of its units holds on its
//! card, by the unit's name.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A range of a layered model's blocks, counted from 0.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Blocks {
  pub from: u64,
  pub to: u64,
}

/// What a unit of a layered model holds on its card (docs/placement.md, 「What a service says of its
/// units」).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UnitDetail {
  #[serde(default)]
  pub blocks: Option<Blocks>,
  #[serde(default)]
  pub output: bool,
  #[serde(default)]
  pub mtp: bool,
  #[serde(default)]
  pub full_attention: Option<u64>,
}

/// What a service says of its units, by each unit's name, `<service>/<part>`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Description {
  pub units: BTreeMap<String, UnitDetail>,
}
