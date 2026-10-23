//! The planner (docs/placement.md, 「The rules」): given the picture the slaves gave and a
//! request, where the asked unit goes, what goes to make room, whether that comes back, or why the
//! request waits. It reads nothing and touches nothing.

use std::collections::BTreeSet;

use crate::rules::{DEFAULT_PRIORITY, Eviction, NodeFacts, Rules, Selector, matches, selects};

// What moving a unit costs where neither the slave nor the rules give a measured time: its size at the
// dump store's measured rate, each way.
const MIB_PER_S: f64 = 1800.0;

// The most units a card's search weighs at once; a card holds a few.
const SEARCHED: usize = 16;

/// Where a unit stands: on the card, out in its owner's host memory, out in the dump store, or some of each.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnitState {
  Resident,
  Ram,
  Evicted,
  Mixed,
}

impl UnitState {
  /// Whether the unit holds room on its card.
  pub fn on_card(self) -> bool {
    matches!(self, UnitState::Resident | UnitState::Mixed)
  }
}

/// A unit as the master gathers it from its slave.
#[derive(Debug, Clone, PartialEq)]
pub struct PictureUnit {
  /// With its node: `model/store@node-b`.
  pub name: String,
  pub service: String,
  pub vram_mib: f64,
  pub state: UnitState,
  pub evictable: bool,
  /// What its slave last measured moving it took, where it has.
  pub load_s: Option<f64>,
  pub unload_s: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PictureNode {
  pub facts: NodeFacts,
  pub free_mib: f64,
  pub total_mib: f64,
  /// The occupant whose claim on the card stands, or none.
  pub claimed_by: Option<String>,
  pub units: Vec<PictureUnit>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Request {
  /// The unit asked for, without its node.
  pub unit: String,
  pub vram_mib: Option<f64>,
  /// Where it may go; the rules' `on` and the unit's `where` narrow it further.
  pub on: Option<Selector>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Decision {
  Place {
    node: String,
    evict: Vec<String>,
    restore: bool,
    cost_s: f64,
    rule: Option<String>,
  },
  Hold {
    reason: String,
    rule: Option<String>,
  },
}

impl Rules {
  /// Unloading and loading a unit back: as its slave measured, else as the rules say, else by its size.
  fn move_of(&self, unit: &PictureUnit) -> f64 {
    let spec = self.spec_of(&unit.name);
    let by_size = unit.vram_mib / MIB_PER_S;
    let unload = unit.unload_s.or(spec.and_then(|one| one.unload_s.map(|n| n.0))).unwrap_or(by_size);
    let load = unit.load_s.or(spec.and_then(|one| one.load_s.map(|n| n.0))).unwrap_or(by_size);
    unload + load
  }

  /// What a set of units costs to move: each one's move, and for every group among them the asker's hold,
  /// since the rest of that group waits at its stage for as long as the asker keeps the room.
  fn set_cost(&self, units: &[&PictureUnit], wait_s: f64) -> f64 {
    let groups: BTreeSet<&str> = units
      .iter()
      .filter_map(|unit| self.spec_of(&unit.name)?.group.as_deref())
      .filter(|g| !g.is_empty())
      .collect();
    units.iter().map(|unit| self.move_of(unit)).sum::<f64>() + groups.len() as f64 * wait_s
  }

  /// Where an asked unit goes and what goes to make room, by the first rule its request matches, or by
  /// tier, priority and size where none does; a hold where nothing fits.
  pub fn plan(&self, picture: &[PictureNode], request: &Request) -> Decision {
    let spec = self.spec_of(&request.unit);
    let rule = self
      .rules
      .iter()
      .find(|one| one.when.request.as_deref().is_some_and(|asked| matches(asked, &request.unit)));
    let rule_name = rule.map(|one| one.name.clone());
    let Some(vram) = request.vram_mib.or(spec.and_then(|one| one.vram_mib.map(|n| n.0))) else {
      return Decision::Hold {
        reason: format!("neither the rules nor the request give the card memory {} needs", request.unit),
        rule: rule_name,
      };
    };
    let priority = spec.map_or(DEFAULT_PRIORITY, |one| one.priority.0);
    let asker = spec.map_or_else(
      || request.unit.split('/').next().unwrap_or_default().to_owned(),
      |one| one.service.clone(),
    );
    let asker_tier = self.tier_of(&asker);
    let wait_s = spec.map_or(0.0, |one| one.hold.max_wait_s.0);
    let places: Vec<&PictureNode> = picture
      .iter()
      .filter(|node| {
        node.claimed_by.is_none()
          && selects(request.on.as_ref(), &node.facts)
          && selects(spec.and_then(|one| one.place.as_ref()), &node.facts)
          && selects(rule.and_then(|one| one.when.on.as_ref()), &node.facts)
          && selects(rule.and_then(|one| one.place.as_ref()), &node.facts)
      })
      .collect();

    let mut best: Option<(String, Vec<String>, f64, f64)> = None;
    for node in &places {
      let reserve = self.group_of(&node.facts).map_or(0.0, |group| group.reserve_mib.0);
      let need = vram + reserve - node.free_mib;
      let resident: Vec<&PictureUnit> =
        node.units.iter().filter(|unit| unit.state.on_card() && unit.evictable).collect();
      let named = |patterns: &[String]| -> Vec<&PictureUnit> {
        resident
          .iter()
          .copied()
          .filter(|unit| patterns.iter().any(|pattern| matches(pattern, &unit.name)))
          .collect()
      };
      let candidates: Vec<&PictureUnit> = match rule.and_then(|one| one.evict.as_ref()) {
        Some(Eviction::Node) => resident.clone(),
        Some(Eviction::List(patterns)) | Some(Eviction::Cheapest(patterns)) => named(patterns),
        // A request rule never says `below` (the rules refuse it as they are read); the default stands: the
        // units of a later tier, of another occupant of the asker's own tier, which the turn gate unloads only
        // once that turn gives way, and of the asker's own service at a lower priority.
        Some(Eviction::Below(_)) | None => resident
          .iter()
          .copied()
          .filter(|unit| {
            let tier = self.tier_of(&unit.service);
            tier > asker_tier
              || (tier == asker_tier
                && (unit.service != asker
                  || self.spec_of(&unit.name).map_or(DEFAULT_PRIORITY, |one| one.priority.0) < priority))
          })
          .collect(),
      };
      let apart = spec.map(|one| one.apart.as_slice()).unwrap_or_default();
      let must_go: Vec<&PictureUnit> = node
        .units
        .iter()
        .filter(|unit| unit.state.on_card() && apart.iter().any(|pattern| matches(pattern, &unit.name)))
        .collect();
      if must_go.iter().any(|unit| !unit.evictable) {
        continue;
      }
      let freed_first: f64 = must_go.iter().map(|unit| unit.vram_mib).sum();
      let rest: Vec<&PictureUnit> = candidates
        .into_iter()
        .filter(|unit| !must_go.iter().any(|gone| std::ptr::eq(*gone, *unit)))
        .collect();
      let Some(found) = cheapest_set(&rest, need - freed_first, |chosen| {
        let together: Vec<&PictureUnit> = must_go.iter().copied().chain(chosen.iter().copied()).collect();
        self.set_cost(&together, wait_s)
      }) else {
        continue;
      };
      let evict: Vec<&PictureUnit> = must_go.iter().copied().chain(found).collect();
      let cost_s = (self.set_cost(&evict, wait_s) * 100.0).round() / 100.0;
      let freed: f64 = evict.iter().map(|unit| unit.vram_mib).sum();
      // Of two that cost the same, the one that unloads less.
      let better = match &best {
        None => true,
        Some((_, _, best_cost, best_freed)) => {
          cost_s < *best_cost || (cost_s == *best_cost && freed < *best_freed)
        }
      };
      if better {
        best = Some((
          node.facts.name.clone(),
          evict.iter().map(|unit| unit.name.clone()).collect(),
          cost_s,
          freed,
        ));
      }
    }

    if let Some((node, evict, cost_s, _)) = best {
      let restore = !evict.is_empty() && rule.is_none_or(|one| one.restore);
      return Decision::Place {
        node,
        evict,
        restore,
        cost_s,
        rule: rule_name,
      };
    }
    let reason = if places.is_empty() {
      format!("no node may hold {} now", request.unit)
    } else {
      format!(
        "no card can be emptied enough to hold {} ({} MiB)",
        request.unit,
        crate::yaml::js_number(vram)
      )
    };
    Decision::Hold {
      reason,
      rule: rule_name,
    }
  }
}

/// The cheapest set of the candidates that frees at least `need` MiB, by exhaustive search over the few a
/// card holds; none where all of them together are not enough. Of two that cost the same, the smaller.
fn cheapest_set<'a>(
  candidates: &[&'a PictureUnit],
  need: f64,
  cost: impl Fn(&[&'a PictureUnit]) -> f64,
) -> Option<Vec<&'a PictureUnit>> {
  if need <= 0.0 {
    return Some(Vec::new());
  }
  let count = candidates.len().min(SEARCHED);
  let mut best: Option<(Vec<&PictureUnit>, f64)> = None;
  for mask in 1u32..(1u32 << count) {
    let chosen: Vec<&PictureUnit> = candidates
      .iter()
      .take(count)
      .enumerate()
      .filter(|(at, _)| mask & (1 << at) != 0)
      .map(|(_, unit)| *unit)
      .collect();
    let freed: f64 = chosen.iter().map(|unit| unit.vram_mib).sum();
    if freed < need {
      continue;
    }
    let total = cost(&chosen);
    let better = match &best {
      None => true,
      Some((units, cost_s)) => total < *cost_s || (total == *cost_s && chosen.len() < units.len()),
    };
    if better {
      best = Some((chosen, total));
    }
  }
  best.map(|(units, _)| units)
}
