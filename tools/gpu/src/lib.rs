//! The supervisor's read of the card master (docs/api.md, 「Usage」): one entry, `bmo/gpu`, answered by
//! calling the master's usage route and saying a person's reading first — what each card is doing, the
//! service or turn holding it, the engine resident on it with its memory, and the waiters in its queue —
//! with the route's answer beside it for a program.

use std::collections::BTreeMap;
use std::io::{Read as _, Write as _};
use std::path::PathBuf;
use std::process::ExitCode;

use bmo_arbiter_client::ArbiterClient;
use bmo_arbiter_client::card::UnitState;
use bmo_arbiter_client::usage::{Usage, UsageCard};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// The entry this executable answers, as its manifest declares it.
pub const ENTRY: &str = "bmo/gpu";

/// The master's loopback port where nothing names another.
pub const DEFAULT_PORT: u16 = 7440;

/// One call handed by the tool host.
#[derive(Debug, Deserialize)]
pub struct Handed {
  #[serde(default)]
  pub entry: String,
  #[serde(default)]
  pub arguments: Value,
}

/// The host's tool result: a person's reading first, and the route's answer for a program beside it.
#[derive(Debug, Serialize)]
pub struct Answer {
  pub content: Vec<Value>,
  #[serde(rename = "isError")]
  pub is_error: bool,
  #[serde(rename = "structuredContent", skip_serializing_if = "Option::is_none")]
  pub structured: Option<Value>,
}

impl Answer {
  /// A reading: its lines for a person, and the usage itself for a program.
  fn reading(lines: Vec<String>, usage: &Usage) -> Self {
    let structured = serde_json::to_value(usage).ok();

    Self {
      content: vec![json!({ "type": "text", "text": lines.join("\n") })],
      is_error: false,
      structured,
    }
  }

  /// A refusal saying why in English.
  pub fn refusal(why: impl Into<String>) -> Self {
    Self {
      content: vec![json!({ "type": "text", "text": why.into() })],
      is_error: true,
      structured: None,
    }
  }

  /// The reading's text, as a person reads it.
  pub fn text(&self) -> String {
    self
      .content
      .iter()
      .filter_map(|one| one.get("text").and_then(Value::as_str))
      .collect::<Vec<_>>()
      .join("\n")
  }
}

/// The call this process was handed, answered on standard output.
pub async fn run() -> ExitCode {
  let mut raw = String::new();
  if let Err(why) = std::io::stdin().read_to_string(&mut raw) {
    eprintln!("the call could not be read: {why}");
    return ExitCode::FAILURE;
  }
  let handed: Handed = match serde_json::from_str(&raw) {
    Ok(handed) => handed,
    Err(why) => return write(&Answer::refusal(format!("the call does not read: {why}"))),
  };
  let (base, token) = master();
  write(&answer_with(&base, token, &handed).await)
}

fn write(answer: &Answer) -> ExitCode {
  let text = match serde_json::to_string(answer) {
    Ok(text) => text,
    Err(why) => {
      eprintln!("the answer could not be written: {why}");
      return ExitCode::FAILURE;
    }
  };
  match std::io::stdout().lock().write_all(text.as_bytes()) {
    Ok(()) => ExitCode::SUCCESS,
    Err(why) => {
      eprintln!("the answer could not be written: {why}");
      ExitCode::FAILURE
    }
  }
}

/// The entry asked for, read from the master at `base`.
pub async fn answer_with(base: &str, token: Option<String>, handed: &Handed) -> Answer {
  if handed.entry != ENTRY {
    return Answer::refusal(format!("this executable holds no entry {}", handed.entry));
  }
  match usage_at(base, token).await {
    Ok(usage) => Answer::reading(lines(&usage), &usage),
    Err(why) => Answer::refusal(why),
  }
}

/// `GET /usage` of the master at `base`, or why it gave none.
pub async fn usage_at(base: &str, token: Option<String>) -> Result<Usage, String> {
  let client = ArbiterClient::new(base, token)
    .map_err(|why| format!("the card master's address does not read: {why}"))?;
  client.usage().await.map_err(|failure| match failure {
    bmo_arbiter_client::CallError::Transport(why) => {
      format!("the card master does not answer at {base}: {why}")
    }
    bmo_arbiter_client::CallError::Answered { status, error } => {
      format!("the card master answered {status} {}: {}", error.code, error.error)
    }
    bmo_arbiter_client::CallError::Unreadable { status, .. } => {
      format!("the card master answered {status}, and it is not the usage this tool reads")
    }
  })
}

/// The master's address and token: `CARD_MASTER_URL` and `CARD_MASTER_TOKEN`, else `CARD_MASTER_PORT` on
/// the loopback, else the settings the deployed master reads beside its unit.
pub fn master() -> (String, Option<String>) {
  let file = settings_file();
  let setting = |name: &str| {
    std::env::var(name)
      .ok()
      .or_else(|| file.get(name).cloned())
      .map(|value| value.trim().to_owned())
      .filter(|value| !value.is_empty())
  };
  let base = setting("CARD_MASTER_URL").unwrap_or_else(|| {
    let port = setting("CARD_MASTER_PORT")
      .and_then(|port| port.parse::<u16>().ok())
      .unwrap_or(DEFAULT_PORT);
    format!("http://127.0.0.1:{port}")
  });
  (base, setting("CARD_MASTER_TOKEN"))
}

fn settings_file() -> BTreeMap<String, String> {
  let home = std::env::var_os("HOME").map(PathBuf::from).unwrap_or_else(|| PathBuf::from("/"));
  dotenvy::from_path_iter(home.join(".config/bmo-arbiter/env"))
    .map(|settings| settings.flatten().collect())
    .unwrap_or_default()
}

/// The reading as a person reads it: one line for each card first, then the engine resident on it and
/// the waiters in its queue.
pub fn lines(usage: &Usage) -> Vec<String> {
  let cards: Vec<&UsageCard> = usage.nodes.iter().flat_map(|node| &node.cards).collect();
  let used = cards.iter().filter(|card| in_use(card)).count();
  let waiting = cards.iter().filter(|card| !card.waiters.is_empty()).count();
  let lost = usage.nodes.iter().filter(|node| !node.connected).count();
  let idle = cards.len().saturating_sub(used);
  let mut said = vec![format!(
    "{} on {}: {used} in use, {idle} idle, {waiting} waiting{}.",
    plural(cards.len(), "card", "cards"),
    plural(usage.nodes.len(), "node", "nodes"),
    if lost == 0 { String::new() } else { format!(", {lost} not reached") }
  )];
  if cards.is_empty() {
    said.push("No card stands on a node the master reads.".to_owned());
  }
  for node in &usage.nodes {
    if !node.connected {
      said.push(format!(
        "{} — not reached: {}",
        node.node_name,
        node.error.as_deref().unwrap_or("the slave did not answer")
      ));
      continue;
    }
    for card in &node.cards {
      said.push(card_line(&node.node_name, card));
      let units: Vec<String> = card
        .units
        .iter()
        .map(|unit| format!("{} {} MiB {}", unit.name, mib(unit.vram_mib), state_word(unit.state)))
        .collect();
      if !units.is_empty() {
        said.push(format!("  engine: {}", units.join("; ")));
      }
      if !card.waiters.is_empty() {
        let waiters: Vec<String> = card
          .waiters
          .iter()
          .map(|waiter| {
            let mut one = format!("{} ticket {}", waiter.service_name, waiter.ticket_id);
            if let Some(position) = waiter.position {
              one.push_str(&format!(", position {position}"));
            }
            one.push_str(&format!(", waited {} ms", waiter.waited_ms));
            if let Some(unit) = &waiter.unit_name {
              one.push_str(&format!(", {unit}"));
            }
            if let Some(vram) = waiter.vram_mib {
              one.push_str(&format!(" {} MiB", mib(vram)));
            }
            one
          })
          .collect();
        said.push(format!("  waiting: {}", waiters.join("; ")));
      }
    }
  }
  said
}

/// Whether a card is doing work: its turn is in use, or the driver sees its use above nothing.
fn in_use(card: &UsageCard) -> bool {
  card.turn.as_ref().and_then(|turn| turn.in_use).unwrap_or(false)
    || card.utilization_percent.is_some_and(|percent| percent > 0)
}

fn card_line(node: &str, card: &UsageCard) -> String {
  let name = card.name.as_deref().unwrap_or("card");
  let used = match card.utilization_percent {
    Some(percent) => format!("{percent}% used"),
    None => "use unread".to_owned(),
  };
  let memory = match (card.used_mib, card.total_mib, card.free_mib) {
    (Some(used), Some(total), Some(free)) => {
      format!("{} of {} MiB, {} free", mib(used), mib(total), mib(free))
    }
    _ => "memory unread".to_owned(),
  };
  let holder = match (&card.turn, &card.claim) {
    (Some(turn), _) => format!(
      "turn {} (ticket {}){}",
      turn.service_name,
      turn.ticket_id,
      if turn.in_use == Some(true) { " in use" } else { " not in use" }
    ),
    (None, Some(claim)) => format!("claim {}", claim.claimant_name),
    (None, None) => "no turn".to_owned(),
  };
  format!("{node} card {} ({name}): {used}, {memory}; {holder}", card.index)
}

fn plural(count: usize, one: &'static str, many: &'static str) -> String {
  format!("{count} {}", if count == 1 { one } else { many })
}

fn state_word(state: UnitState) -> &'static str {
  match state {
    UnitState::Resident => "resident",
    UnitState::Ram => "in host memory",
    UnitState::Evicted => "in the store",
    UnitState::Mixed => "partly out",
  }
}

/// A size in MiB as a person reads it, without a trailing `.0`.
fn mib(value: f64) -> String {
  if value.fract() == 0.0 { format!("{value:.0}") } else { format!("{value:.1}") }
}
