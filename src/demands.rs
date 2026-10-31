//! Demands (docs/placement.md, 「Demands」; docs/api.md, 「Demands」): cards of a service standing on several
//! nodes, asked of the master by a caller off the nodes. The book is what the routes and the master's rounds
//! share: the routes ask, read and give demands back; each round the master grants their cards by the one
//! rule and writes what it decided here; a card lent within a demand is owed back to it here, once the
//! demand it was lent to is given back. A demand stands until its caller gives it back; no time and no
//! silence drops one, and the book is the master's memory alone.

use std::collections::BTreeMap;
use std::sync::Mutex;

use bmo_arbiter_client::ErrorCode;
use bmo_arbiter_client::demands::{Demand, DemandAsk, DemandCard, DemandReason, DemandState};
use tokio::sync::{Notify, watch};

/// Where one card of a demand stands.
#[derive(Clone, Debug, PartialEq)]
pub enum Seat {
  /// In the line, no card chosen.
  Waiting,
  /// The service is being brought up on this node for it.
  Starting(String),
  /// Granted.
  Granted(DemandCard),
  /// Granted, and the service stopped there by a claim or a ticket of a lower tier, named.
  Taken { node: String, by: String },
  /// Its card lent to a demand asked within it.
  Lent(String),
  /// Its card coming back from the demand it was lent to, its own service being brought up there again.
  Returning(String),
}

impl Seat {
  /// The node this seat holds, where it holds one.
  pub fn node(&self) -> Option<&str> {
    match self {
      Seat::Waiting => None,
      Seat::Starting(node) | Seat::Lent(node) | Seat::Returning(node) => Some(node),
      Seat::Granted(card) => Some(&card.node_name),
      Seat::Taken { node, .. } => Some(node),
    }
  }

  /// The node whose card this seat uses for its service (docs/placement.md, 「A service's ceiling」): every
  /// node it holds but a card lent out, which counts for the demand it is lent to.
  pub fn uses(&self) -> Option<&str> {
    match self {
      Seat::Lent(_) => None,
      other => other.node(),
    }
  }
}

/// One demand the master holds.
#[derive(Clone, Debug, PartialEq)]
pub struct Held {
  pub id: i64,
  pub service: String,
  pub within: Option<i64>,
  pub work_key: Option<String>,
  pub reference: Option<String>,
  /// When it was asked, in epoch milliseconds.
  pub asked_at: i64,
  /// One seat for every card it asks.
  pub seats: Vec<Seat>,
  /// While a card waits, what it waits on.
  pub reason: Option<DemandReason>,
  /// Grows whenever a card is granted, granted again, lent, or taken: a held answer goes out on it.
  pub news: u64,
}

impl Held {
  /// The demand as every route answers it, its wait counted to `now_ms`.
  pub fn answer(&self, now_ms: i64) -> Demand {
    let cards: Vec<DemandCard> = self
      .seats
      .iter()
      .filter_map(|seat| match seat {
        Seat::Granted(card) => Some(card.clone()),
        _ => None,
      })
      .collect();
    let lent = self.seats.iter().any(|seat| matches!(seat, Seat::Lent(_) | Seat::Returning(_)));
    let state = if lent {
      DemandState::Lent
    } else if cards.len() == self.seats.len() {
      DemandState::Granted
    } else if cards.is_empty() {
      DemandState::Waiting
    } else {
      DemandState::Partly
    };
    Demand {
      id: self.id,
      service_name: self.service.clone(),
      count: self.seats.len() as u32,
      taken: self.seats.iter().find_map(|seat| match seat {
        Seat::Taken { by, .. } => Some(by.clone()),
        _ => None,
      }),
      reason: if state == DemandState::Granted { None } else { self.reason.clone() },
      cards,
      state,
      waited_ms: (now_ms - self.asked_at).max(0),
      within: self.within,
      work_key: self.work_key.clone(),
      reference: self.reference.clone(),
    }
  }

  /// What a held answer waits to change: its state, its cards and who took one.
  fn standing(&self) -> (DemandState, Vec<DemandCard>, Option<String>) {
    let answer = self.answer(0);
    (answer.state, answer.cards, answer.taken)
  }
}

/// What the routes and the master share.
#[derive(Debug, Default)]
pub struct Book {
  /// Every demand held, by id, which is the line's order.
  pub held: BTreeMap<i64, Held>,
  last_id: i64,
  /// Each service's nodes as the master last read its placement.
  pub placed: BTreeMap<String, Vec<String>>,
  /// Grows with every change, so the master knows its picture moved.
  pub version: u64,
}

/// A refusal of a route: its code and its sentence.
pub type Refused = (ErrorCode, String);

impl Book {
  /// A new demand in the line, or why it is refused.
  pub fn ask(&mut self, ask: &DemandAsk, now_ms: i64) -> Result<&Held, Refused> {
    let count = ask.count.unwrap_or(1);
    if ask.service_name.trim().is_empty() {
      return Err((ErrorCode::BadRequest, "the body names no service".into()));
    }
    if count == 0 {
      return Err((ErrorCode::BadRequest, "count must be one or more".into()));
    }
    if self.placed.get(&ask.service_name).is_none_or(Vec::is_empty) {
      return Err((ErrorCode::NoPlace, format!("{} is placed on no node", ask.service_name)));
    }
    if let Some(within) = ask.within
      && !self.held.contains_key(&within)
    {
      return Err((ErrorCode::NotFound, format!("the master holds no demand {within}")));
    }
    self.last_id = (self.last_id + 1).max(now_ms);
    let held = Held {
      id: self.last_id,
      service: ask.service_name.clone(),
      within: ask.within,
      work_key: ask.work_key.clone(),
      reference: ask.reference.clone(),
      asked_at: now_ms,
      seats: vec![Seat::Waiting; count as usize],
      reason: None,
      news: 0,
    };
    self.version += 1;
    Ok(self.held.entry(held.id).or_insert(held))
  }

  /// A demand given back, every card of it; a card lent to it within another demand owed back to that
  /// demand, its own service to be brought up there again. Whether the book held it.
  pub fn give_back(&mut self, id: i64) -> bool {
    let Some(gone) = self.held.remove(&id) else { return false };
    if let Some(owner) = gone.within.and_then(|owner| self.held.get_mut(&owner)) {
      let mut moved = false;
      for seat in owner.seats.iter_mut() {
        if let Seat::Lent(node) = seat
          && gone.seats.iter().any(|mine| mine.node() == Some(node.as_str()))
        {
          *seat = Seat::Returning(node.clone());
          moved = true;
        }
      }
      if moved {
        owner.news += 1;
      }
    }
    self.version += 1;
    true
  }

  /// Every demand held, in the line's order.
  pub fn answers(&self, now_ms: i64) -> Vec<Demand> {
    self.held.values().map(|held| held.answer(now_ms)).collect()
  }

  /// How many demands are granted the service on the node, or being brought up there for it: what keeps
  /// its turn in use (docs/placement.md, 「Yielding by the process」).
  pub fn on(&self, service: &str, node: &str) -> usize {
    self
      .held
      .values()
      .filter(|held| held.service == service)
      .flat_map(|held| &held.seats)
      .filter(|seat| match seat {
        Seat::Granted(card) => card.node_name == node,
        Seat::Starting(at) | Seat::Returning(at) => at == node,
        _ => false,
      })
      .count()
  }

  /// How many demands are granted the service on the node.
  pub fn granted_on(&self, service: &str, node: &str) -> usize {
    self
      .held
      .values()
      .filter(|held| held.service == service)
      .flat_map(|held| &held.seats)
      .filter(|seat| matches!(seat, Seat::Granted(card) if card.node_name == node))
      .count()
  }

  /// The demand's seats replaced as a round decided them, its news grown where its answer moved.
  pub fn seat(&mut self, id: i64, seats: Vec<Seat>, reason: Option<DemandReason>) {
    let Some(held) = self.held.get_mut(&id) else { return };
    let before = held.standing();
    let moved = held.seats != seats || held.reason != reason;
    held.seats = seats;
    held.reason = reason;
    if held.standing() != before {
      held.news += 1;
    }
    if moved {
      self.version += 1;
    }
  }
}

/// The book with what wakes those who wait on it: a held answer, on any change of a demand, and the
/// master's next round, on a demand asked or given back.
#[derive(Debug)]
pub struct Demands {
  pub book: Mutex<Book>,
  pub changed: watch::Sender<u64>,
  pub wake: Notify,
}

impl Default for Demands {
  fn default() -> Self {
    Demands {
      book: Mutex::default(),
      changed: watch::Sender::new(0),
      wake: Notify::new(),
    }
  }
}

impl Demands {
  /// Every held answer woken to look again.
  pub fn tell(&self) {
    self.changed.send_modify(|seen| *seen += 1);
  }
}
