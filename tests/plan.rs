//! The planner (docs/placement.md, 「The rules」): where an asked unit goes and what goes to make room, by
//! the rules or by priority and size, and a hold where nothing fits.

use crate::fixtures;

use bmo_arbiter::plan::{Decision, PictureNode, PictureUnit, Request, UnitState};
use bmo_arbiter::rules::{NodeFacts, RULES_FILE, Rules, parse_rules};

fn rules() -> Rules {
  parse_rules(fixtures::RULES, RULES_FILE).unwrap()
}

/// A node of the text model's pool, its free memory and the text model's units on its card.
fn model(node: &str, free: f64, units: &[(&str, f64)]) -> PictureNode {
  PictureNode {
    facts: NodeFacts {
      name: node.into(),
      role: Some("model".into()),
      desktop: false,
    },
    free_mib: free,
    total_mib: 11875.0,
    claimed_by: None,
    units: units
      .iter()
      .map(|(part, vram)| PictureUnit {
        name: format!("model/{part}@{node}"),
        service: "model".into(),
        vram_mib: *vram,
        state: UnitState::Resident,
        evictable: true,
        load_s: None,
        unload_s: None,
      })
      .collect(),
  }
}

fn asked(unit: &str) -> Request {
  Request {
    unit: unit.into(),
    ..Request::default()
  }
}

#[test]
fn a_unit_that_fits_takes_the_free_room_and_moves_nothing() {
  let decision =
    rules().plan(&[model("a", 8000.0, &[("weights", 900.0), ("store", 1152.0)])], &asked("voice/large"));
  assert_eq!(
    decision,
    Decision::Place {
      node: "a".into(),
      evict: vec![],
      restore: false,
      cost_s: 0.0,
      rule: Some("voice-on-compute".into())
    }
  );
}

#[test]
fn the_cheapest_set_that_makes_room_goes_on_the_card_where_it_costs_least_and_comes_back_after() {
  let picture = [
    model("a", 1300.0, &[("weights", 4000.0), ("store", 4608.0), ("compute", 1576.0)]),
    model("b", 4000.0, &[("weights", 900.0), ("store", 1152.0), ("compute", 1576.0)]),
  ];
  let Decision::Place {
    node,
    mut evict,
    restore,
    cost_s,
    ..
  } = rules().plan(&picture, &asked("voice/large"))
  else {
    panic!("held");
  };
  // a needs 5,000 MiB moved and b 2,300: on b its weights and compute (2,476 MiB) are the least that suffice.
  assert_eq!(node, "b");
  evict.sort();
  assert_eq!(evict, ["model/compute@b", "model/weights@b"]);
  assert!(restore);
  // Each unit costs its unload and load as the rules give them, and the pipeline its asker's hold once.
  assert_eq!(cost_s, 2.0 * (0.5 + 2.0) + 60.0);
}

#[test]
fn with_no_rule_only_a_fill_unit_of_lower_priority_goes() {
  let rules = parse_rules(
    "nodes:\n  - name: desk\n    select: { desktop: true }\n    reserve_mib: 1000\nunits:\n  - unit: \"voice/small\"\n    vram_mib: 1500\n  - unit: \"voice/big\"\n    kind: fill\n    priority: 70\n",
    RULES_FILE,
  )
  .unwrap();
  let unit = |name: &str, vram: f64| PictureUnit {
    name: name.into(),
    service: name.split('/').next().unwrap().into(),
    vram_mib: vram,
    state: UnitState::Resident,
    evictable: true,
    load_s: None,
    unload_s: None,
  };
  let desk = PictureNode {
    facts: NodeFacts {
      name: "desk-1".into(),
      role: None,
      desktop: true,
    },
    free_mib: 1900.0,
    total_mib: 10240.0,
    claimed_by: None,
    units: vec![unit("router/room@desk-1", 700.0), unit("voice/big@desk-1", 5000.0)],
  };
  let Decision::Place { node, evict, .. } = rules.plan(&[desk], &asked("voice/small")) else {
    panic!("held")
  };
  assert_eq!((node.as_str(), evict), ("desk-1", vec!["router/room@desk-1".to_owned()]));
}

#[test]
fn a_waiting_ticket_of_its_own_class_makes_an_idle_resident_give_way() {
  let rules = parse_rules(
    "units:\n  - unit: \"model/*\"\n    kind: resident\n    priority: 50\n    load_s: 1\n    unload_s: 1\n",
    RULES_FILE,
  )
  .unwrap();
  let decision = rules.plan(
    &[model("a", 100.0, &[("weights", 5000.0)])],
    &Request {
      unit: "model/large".into(),
      vram_mib: Some(4000.0),
      ..Request::default()
    },
  );
  let Decision::Place { node, evict, .. } = decision else {
    panic!("held: {decision:?}")
  };
  assert_eq!((node.as_str(), evict), ("a", vec!["model/weights@a".to_owned()]));
}

#[test]
fn a_lone_unit_is_not_moved_while_nothing_asks_for_the_card() {
  let decision = rules().plan(
    &[model("a", 8000.0, &[("weights", 900.0)])],
    &Request {
      unit: "model/weights".into(),
      vram_mib: Some(900.0),
      ..Request::default()
    },
  );
  assert_eq!(
    decision,
    Decision::Place {
      node: "a".into(),
      evict: vec![],
      restore: false,
      cost_s: 0.0,
      rule: None
    }
  );
}

#[test]
fn a_unit_no_owner_can_unload_is_named_in_the_hold_rather_than_waited_on_silently() {
  let rules =
    parse_rules("units:\n  - unit: \"model/*\"\n    kind: resident\n    priority: 50\n", RULES_FILE)
      .unwrap();
  let mut node = model("a", 100.0, &[("weights", 5000.0)]);
  node.units[0].evictable = false;
  let Decision::Hold { reason, .. } = rules.plan(
    &[node],
    &Request {
      unit: "model/large".into(),
      vram_mib: Some(4000.0),
      ..Request::default()
    },
  ) else {
    panic!("placed");
  };
  assert!(reason.contains("model/weights@a"), "{reason}");
  assert!(reason.contains("no owner can unload"), "{reason}");
}

#[test]
fn a_request_that_cannot_be_placed_is_held_and_a_node_a_claim_stands_on_is_never_chosen() {
  let rules = rules();
  let mut busy = model("a", 9000.0, &[]);
  busy.claimed_by = Some("tester".into());
  assert!(matches!(rules.plan(&[busy], &asked("voice/large")), Decision::Hold { .. }));
  assert!(matches!(
    rules.plan(&[model("a", 100.0, &[("store", 1000.0)])], &asked("voice/large")),
    Decision::Hold { .. }
  ));
  assert!(matches!(
    rules.plan(&[model("a", 9000.0, &[])], &asked("voice/unknown")),
    Decision::Hold { .. }
  ));
}
