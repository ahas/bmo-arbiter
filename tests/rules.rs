//! The placement rules the master reads (docs/placement.md, 「The rules」): their parts read and checked,
//! a fault naming what is wrong and where, patterns, and the part of them each node is pushed.

mod fixtures;

use bmo_arbiter::rules::{
  Claimant, Dump, Eviction, NodeFacts, Num, RULES_FILE, RamFree, Rules, SeenBy, TurnValues, UnitKind,
  matches, parse_rules,
};
use fixtures::RULES;

fn read(source: &str) -> Rules {
  parse_rules(source, RULES_FILE).unwrap()
}

fn refusal(source: &str) -> String {
  parse_rules(source, RULES_FILE).unwrap_err().0
}

#[test]
fn its_three_parts_are_read_and_checked() {
  let rules = read(RULES);
  let groups: Vec<(&str, bool, f64)> = rules
    .groups
    .iter()
    .map(|group| (group.name.as_str(), group.pool, group.reserve_mib.0))
    .collect();
  assert_eq!(groups, [("compute", true, 300.0), ("desk", false, 1000.0)]);
  let router = &rules.units[1];
  assert_eq!(
    (router.unit.as_str(), router.service.as_str(), router.kind, router.load_s),
    ("router/*", "router", UnitKind::Fill, Some(Num(17.0)))
  );
  let voice = &rules.rules[1];
  assert_eq!(voice.name, "voice-on-compute");
  assert!(voice.restore);
  assert_eq!(voice.evict, Some(Eviction::Cheapest(vec!["model/*".into()])));
  assert_eq!(rules.rules[0].evict, Some(Eviction::Node));
}

#[test]
fn the_occupants_that_take_the_card_how_each_is_seen_and_the_host_memory_it_needs_are_the_files_own() {
  let rules = read(RULES);
  assert_eq!(
    rules.occupants,
    [
      Claimant {
        name: "tester".into(),
        claims: true,
        first: true,
        seen_by: SeenBy {
          tickets: true,
          cgroup: None,
          containers: Some("tester-".into())
        },
        ram_free: RamFree::All,
      },
      Claimant {
        name: "batch".into(),
        claims: true,
        first: false,
        seen_by: SeenBy {
          tickets: false,
          cgroup: Some("batch.service".into()),
          containers: None
        },
        ram_free: RamFree::GiB(Num(8.0)),
      },
    ]
  );
  assert_eq!((rules.groups[0].dump, rules.groups[0].ram_max_gib), (Some(Dump::Ram), Some(Num(24.0))));
  assert_eq!((rules.groups[1].dump, rules.groups[1].ram_max_gib), (None, None));
  assert_eq!(rules.units[0].dump, Some(Dump::Disk));
  assert!(refusal("occupants:\n  - name: x\n").contains("occupants[0].claims"));
  assert!(
    refusal("occupants:\n  - { name: x, claims: true, ram_free: some }\n")
      .contains("occupants[0].ram_free")
  );
  assert!(
    refusal("occupants:\n  - { name: x, claims: true }\n  - { name: x, claims: true }\n").contains("twice")
  );
  assert!(refusal("nodes:\n  - select: { role: a }\n    dump: tape\n").contains("nodes[0].dump"));
  assert!(
    refusal("rules:\n  - when: { jobs: true }\n").contains("rules[0].when holds request, desktop and on")
  );
}

#[test]
fn a_malformed_file_names_what_is_wrong_and_where() {
  let fault = parse_rules("nodes:\n  - select: { host: node-a }\n", "rules.yaml").unwrap_err().0;
  assert!(
    fault.starts_with("rules.yaml: nodes[0].select chooses nodes by role and desktop alone"),
    "{fault}"
  );
  assert!(refusal("units:\n  - unit: x\n    kind: always\n").contains("units[0].kind"));
  assert!(refusal("rules:\n  - when: {}\n").contains("rules[0].when"));
  assert!(refusal("placements: []\n").contains("the top level holds"));
}

#[test]
fn an_eviction_below_an_occupant_is_the_desktops_and_refused_on_a_request() {
  let desktop = read("rules:\n  - when: { desktop: busy }\n    evict: { below: desktop }\n");
  assert_eq!(desktop.rules[0].evict, Some(Eviction::Below("desktop".into())));
  let fault = refusal("rules:\n  - when: { request: \"voice/*\" }\n    evict: { below: desktop }\n");
  assert!(fault.contains("rules[0].evict may say below only when the desktop is busy"), "{fault}");
}

#[test]
fn a_pattern_names_units_with_or_without_their_node() {
  assert!(matches("model/*", "model/store@node-b"));
  assert!(matches("voice/large", "voice/large"));
  assert!(!matches("voice/*", "router/room@node-c"));
  assert!(matches("*/store", "model/store@node-b"));
  assert!(!matches("model/*e", "model/store-x"));
}

#[test]
fn a_node_is_pushed_only_the_part_that_can_touch_it() {
  let rules = read(RULES);
  let desk = rules.for_node(&NodeFacts {
    name: "desk-1".into(),
    role: None,
    desktop: true,
  });
  assert_eq!(
    desk.occupants.iter().map(|one| one.name.as_str()).collect::<Vec<_>>(),
    ["tester", "batch"]
  );
  assert_eq!(desk.groups.iter().map(|group| group.name.as_str()).collect::<Vec<_>>(), ["desk"]);
  assert!(!desk.units.iter().any(|spec| spec.unit == "voice/large"));
  assert_eq!(desk.rules.iter().map(|rule| rule.name.as_str()).collect::<Vec<_>>(), ["rule-1"]);
}

#[test]
fn what_a_node_is_pushed_is_the_json_its_slave_reads() {
  let rules = read(RULES);
  let text = rules
    .for_node(&NodeFacts {
      name: "node-b".into(),
      role: Some("model".into()),
      desktop: false,
    })
    .node_text();
  let read: serde_json::Value = serde_json::from_str(&text).unwrap();
  assert!(text.starts_with("{\n \"occupants\": [\n  {\n   \"name\": \"tester\""), "{text}");
  assert!(text.ends_with("}\n"));
  assert_eq!(read["occupants"][1]["ramFree"], 8);
  assert_eq!(read["occupants"][0]["seenBy"]["containers"], "tester-");
  assert_eq!(read["occupants"][0]["seenBy"]["cgroup"], serde_json::Value::Null);
  assert_eq!(read["occupants"][1]["seenBy"]["cgroup"], "batch.service");
  assert_eq!(read["groups"][0]["ramMaxGiB"], 24);
  assert_eq!(read["units"][0]["unloadS"], 0.5);
  assert_eq!(read["units"][2]["where"]["role"], "model");
  assert_eq!(read["rules"][1]["evict"]["cheapest"][0], "model/*");
  assert_eq!(read["rules"][0]["evict"]["node"], true);
  assert_eq!(read["rules"][0]["when"], serde_json::json!({ "desktop": "busy" }));
  assert!(read.get("turns").is_none(), "{text}");
}

#[test]
fn every_node_is_handed_the_turns_part_whole_in_the_form_its_slave_reads() {
  let mut rules = read(RULES);
  rules.turns.insert(
    "model".into(),
    TurnValues {
      yield_after_s: Num(300.0),
      can_pause: false,
    },
  );
  rules.turns.insert(
    "coder".into(),
    TurnValues {
      yield_after_s: Num(60.5),
      can_pause: true,
    },
  );
  for (name, desktop) in [("node-b", false), ("desk-1", true)] {
    let facts = NodeFacts {
      name: name.into(),
      role: None,
      desktop,
    };
    let read: serde_json::Value = serde_json::from_str(&rules.for_node(&facts).node_text()).unwrap();
    assert_eq!(
      read["turns"],
      serde_json::json!({
        "coder": { "yieldAfterS": 60.5, "canPause": true },
        "model": { "yieldAfterS": 300, "canPause": false },
      }),
      "{name}"
    );
  }
}
