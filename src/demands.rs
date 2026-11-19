//! Demands (docs/placement.md, 「Demands」; docs/api.md, 「Demands」): cards of a service standing on several
//! nodes, asked of the master by a caller off the nodes. The book is what the routes and the master's rounds
//! share: the routes ask, read and give demands back; each round the master grants their cards by the one
//! rule and writes what it decided here; a card lent within a demand is owed back to it here, once the
//! demand it was lent to is given back. A demand stands until its caller gives it back, or, where it names a
//! lease, until the lease runs with no call of its caller's on it; nothing else drops one, and the book is
//! the master's memory alone.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use bmo_arbiter_client::ErrorCode;
use bmo_arbiter_client::demands::{Demand, DemandAsk, DemandCard, DemandPool, DemandReason, DemandState};
use tokio::sync::{Notify, watch};

/// The rank of a demand that names none and is asked within none.
pub const DEFAULT_RANK: u32 = 1;

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
  /// Its card taken by a demand of a better rank, named: in the line again, holding no card.
  Outranked { by: String },
  /// Its card lent to a demand asked within it.
  Lent(String),
  /// Its card coming back from the demand it was lent to, its own service being brought up there again.
  Returning(String),
}

impl Seat {
  /// The node this seat holds, where it holds one.
  pub fn node(&self) -> Option<&str> {
    match self {
      Seat::Waiting | Seat::Outranked { .. } => None,
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

/// A demand's place in one card's line (docs/placement.md, 「A demand's place in a card's line」): the
/// demand, its service, when it was asked, and whether it holds that card or waits for it.
#[derive(Clone, Debug, PartialEq)]
pub struct Place {
  pub demand: i64,
  pub service: String,
  /// When the demand was asked, in epoch milliseconds.
  pub asked_at: i64,
  pub granted: bool,
}

/// One demand the master holds.
#[derive(Clone, Debug, PartialEq)]
pub struct Held {
  pub id: i64,
  pub service: String,
  pub within: Option<i64>,
  pub work_key: Option<String>,
  pub reference: Option<String>,
  pub rank: u32,
  pub pool: Option<DemandPool>,
  /// When it was asked, in epoch milliseconds.
  pub asked_at: i64,
  /// How long it stands with no call of its caller's on it, where it names a lease.
  pub lease_ms: Option<u64>,
  /// When the last call of its caller's on it came or was answered, in epoch milliseconds.
  pub called_at: i64,
  /// How many calls of its caller's on it are in hand now.
  pub calls: u32,
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
      rank: self.rank,
      pool: self.pool.clone(),
      taken: self.seats.iter().find_map(|seat| match seat {
        Seat::Taken { by, .. } | Seat::Outranked { by } => Some(by.clone()),
        _ => None,
      }),
      outranked: self.seats.iter().any(|seat| matches!(seat, Seat::Outranked { .. })),
      reason: if state == DemandState::Granted { None } else { self.reason.clone() },
      cards,
      state,
      waited_ms: (now_ms - self.asked_at).max(0),
      lease_ms: self.lease_ms,
      within: self.within,
      work_key: self.work_key.clone(),
      reference: self.reference.clone(),
    }
  }

  /// Whether its lease has run out at `now_ms`: one named, no call in hand on it, and the lease run since
  /// the last call was answered.
  fn lapsed(&self, now_ms: i64) -> bool {
    self.lease_ms.is_some_and(|lease| {
      self.calls == 0 && now_ms.saturating_sub(self.called_at) >= i64::try_from(lease).unwrap_or(i64::MAX)
    })
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
    if ask.pool.as_ref().is_some_and(|pool| pool.key.trim().is_empty() || pool.cards == 0) {
      return Err((ErrorCode::BadRequest, "a pool names a key and a limit of one card or more".into()));
    }
    if ask.lease_ms == Some(0) {
      return Err((ErrorCode::BadRequest, "a lease is one millisecond or more".into()));
    }
    let outer = match ask.within {
      Some(within) => match self.held.get(&within) {
        Some(outer) => Some(outer),
        None => return Err((ErrorCode::NotFound, format!("the master holds no demand {within}"))),
      },
      None => None,
    };
    let rank = ask.rank.or(outer.map(|outer| outer.rank)).unwrap_or(DEFAULT_RANK);
    let pool = ask.pool.clone().or_else(|| outer.and_then(|outer| outer.pool.clone()));
    self.last_id = (self.last_id + 1).max(now_ms);
    let held = Held {
      id: self.last_id,
      service: ask.service_name.clone(),
      within: ask.within,
      work_key: ask.work_key.clone(),
      reference: ask.reference.clone(),
      rank,
      pool,
      asked_at: now_ms,
      lease_ms: ask.lease_ms,
      called_at: now_ms,
      calls: 0,
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

  /// A call of its caller's on the demand come at `now_ms`, which keeps its lease while it is in hand; its
  /// news, where the book holds it.
  pub fn call(&mut self, id: i64, now_ms: i64) -> Option<u64> {
    let held = self.held.get_mut(&id)?;
    held.calls += 1;
    held.called_at = now_ms;
    Some(held.news)
  }

  /// A call on the demand answered or cut at `now_ms`: its lease runs from then once no other is in hand.
  pub fn called(&mut self, id: i64, now_ms: i64) {
    if let Some(held) = self.held.get_mut(&id) {
      held.calls = held.calls.saturating_sub(1);
      held.called_at = now_ms;
    }
  }

  /// Every demand whose lease has run out at `now_ms` given back as its caller would give it back; what
  /// they were, in the line's order.
  pub fn lapse(&mut self, now_ms: i64) -> Vec<Held> {
    let lapsed: Vec<Held> = self.held.values().filter(|held| held.lapsed(now_ms)).cloned().collect();
    for held in &lapsed {
      self.give_back(held.id);
    }
    lapsed
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

  /// The granted places each card is handed, by node (docs/placement.md, 「A demand's place in a card's
  /// line」): one for each demand holding the card — granted there, its service brought up or coming back
  /// there. A waiting demand's places are the master's to choose, by what each card is to it.
  pub fn places(&self) -> BTreeMap<String, Vec<Place>> {
    let mut places: BTreeMap<String, Vec<Place>> = BTreeMap::new();
    for held in self.held.values() {
      for seat in &held.seats {
        if let Seat::Granted(DemandCard {
          node_name: node, ..
        })
        | Seat::Starting(node)
        | Seat::Returning(node) = seat
        {
          places.entry(node.clone()).or_default().push(Place {
            demand: held.id,
            service: held.service.clone(),
            asked_at: held.asked_at,
            granted: true,
          });
        }
      }
    }
    places
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

  /// A new demand in the line, with the call that asked it in hand, or why it is refused: its id, its news
  /// and the call.
  pub fn ask(self: &Arc<Self>, ask: &DemandAsk, clock: fn() -> i64) -> Result<(i64, u64, Call), Refused> {
    let now_ms = clock();
    let mut book = self.book.lock().unwrap();
    let id = book.ask(ask, now_ms)?.id;
    let news = book.call(id, now_ms).unwrap_or_default();
    Ok((id, news, self.in_hand(id, clock)))
  }

  /// A call of its caller's on the demand, kept in hand until what is returned is dropped, its time read
  /// from `clock` in epoch milliseconds; with the demand's news, where the book holds it.
  pub fn call(self: &Arc<Self>, id: i64, clock: fn() -> i64) -> Option<(u64, Call)> {
    let news = self.book.lock().unwrap().call(id, clock())?;
    Some((news, self.in_hand(id, clock)))
  }

  fn in_hand(self: &Arc<Self>, id: i64, clock: fn() -> i64) -> Call {
    Call {
      demands: self.clone(),
      id,
      clock,
    }
  }
}

/// A call on a demand in hand: dropped, whether answered or cut, it lets the demand's lease run again.
#[derive(Debug)]
pub struct Call {
  demands: Arc<Demands>,
  id: i64,
  clock: fn() -> i64,
}

impl Drop for Call {
  fn drop(&mut self) {
    self.demands.book.lock().unwrap().called(self.id, (self.clock)());
  }
}
