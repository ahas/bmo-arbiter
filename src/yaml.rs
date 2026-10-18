//! A YAML document read into a value that keeps its mappings in the order they were written, so a
//! check that walks a mapping names the first wrong key a person wrote, as the rules' own messages
//! say. The reading is the YAML crate's; this is only the shape it is read into.

use std::fmt;

use serde::Deserialize;
use serde::de::{Deserializer, MapAccess, SeqAccess, Visitor};

#[derive(Debug, Clone, PartialEq)]
pub enum Yaml {
  Null,
  Bool(bool),
  Number(f64),
  Text(String),
  List(Vec<Yaml>),
  Map(Vec<(String, Yaml)>),
}

impl Yaml {
  /// The value under a key of a mapping, or none where the key is absent or this is no mapping.
  pub fn get(&self, key: &str) -> Option<&Yaml> {
    match self {
      Yaml::Map(entries) => entries.iter().find(|(name, _)| name == key).map(|(_, value)| value),
      _ => None,
    }
  }

  /// Whether a value stands under the key, null counting as absent the way an unset field does.
  pub fn has(&self, key: &str) -> bool {
    self.get(key).is_some_and(|value| *value != Yaml::Null)
  }

  /// The value as a key or a free text reads it.
  pub fn as_words(&self) -> String {
    match self {
      Yaml::Null => "null".to_owned(),
      Yaml::Bool(one) => one.to_string(),
      Yaml::Number(one) => js_number(*one),
      Yaml::Text(one) => one.clone(),
      Yaml::List(_) | Yaml::Map(_) => String::new(),
    }
  }
}

/// A number written the way a JavaScript runtime writes it: a whole number without a point.
pub fn js_number(value: f64) -> String {
  if value.fract() == 0.0 && value.abs() < 1e21 {
    format!("{}", value as i128)
  } else {
    format!("{value}")
  }
}

/// The document read as YAML 1.2 reads it — `on`, `yes` and their kin stay words, only `true` and
/// `false` are booleans — an empty one standing as an empty mapping and a key written twice keeping its
/// last value.
pub fn parse(source: &str) -> Result<Yaml, String> {
  if source.trim().is_empty() {
    return Ok(Yaml::Map(Vec::new()));
  }
  let options = serde_saphyr::options! {
    strict_booleans: true,
    duplicate_keys: serde_saphyr::options::DuplicateKeyPolicy::LastWins,
  };
  serde_saphyr::from_str_with_options::<Yaml>(source, options).map_err(|fault| fault.to_string())
}

impl<'de> Deserialize<'de> for Yaml {
  fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
    deserializer.deserialize_any(YamlVisitor)
  }
}

struct YamlVisitor;

impl<'de> Visitor<'de> for YamlVisitor {
  type Value = Yaml;

  fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
    formatter.write_str("a YAML value")
  }

  fn visit_unit<E>(self) -> Result<Yaml, E> {
    Ok(Yaml::Null)
  }

  fn visit_none<E>(self) -> Result<Yaml, E> {
    Ok(Yaml::Null)
  }

  fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<Yaml, D::Error> {
    Yaml::deserialize(deserializer)
  }

  fn visit_bool<E>(self, value: bool) -> Result<Yaml, E> {
    Ok(Yaml::Bool(value))
  }

  fn visit_i64<E>(self, value: i64) -> Result<Yaml, E> {
    Ok(Yaml::Number(value as f64))
  }

  fn visit_u64<E>(self, value: u64) -> Result<Yaml, E> {
    Ok(Yaml::Number(value as f64))
  }

  fn visit_f64<E>(self, value: f64) -> Result<Yaml, E> {
    Ok(Yaml::Number(value))
  }

  fn visit_str<E>(self, value: &str) -> Result<Yaml, E> {
    Ok(Yaml::Text(value.to_owned()))
  }

  fn visit_string<E>(self, value: String) -> Result<Yaml, E> {
    Ok(Yaml::Text(value))
  }

  fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Yaml, A::Error> {
    let mut items = Vec::new();
    while let Some(item) = seq.next_element::<Yaml>()? {
      items.push(item);
    }
    Ok(Yaml::List(items))
  }

  fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Yaml, A::Error> {
    let mut entries: Vec<(String, Yaml)> = Vec::new();
    while let Some((key, value)) = map.next_entry::<Yaml, Yaml>()? {
      let name = key.as_words();
      // A key written twice keeps its last value, in the place it was first written.
      match entries.iter_mut().find(|(standing, _)| *standing == name) {
        Some(entry) => entry.1 = value,
        None => entries.push((name, value)),
      }
    }
    Ok(Yaml::Map(entries))
  }
}
