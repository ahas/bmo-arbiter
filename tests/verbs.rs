//! The verbs apart from the standing master (docs/api.md, 「The command line」): `check`'s lines and
//! misfits, what a slave says of its card and its machine, the tree the binary reads, and how a push names
//! what it wrote.

use crate::fixtures;

use std::path::Path;

use bmo_arbiter::rules::{NodeFacts, RULES_FILE, parse_rules};
use bmo_arbiter::verbs::{check_lines, code_root, misfits, slave_card_mib, slave_desktop, text_hash};
use fixtures::RULES;

#[test]
fn a_check_says_each_claimant_group_unit_and_rule() {
  let rules = parse_rules(RULES, RULES_FILE).unwrap();
  let nodes = [
    NodeFacts {
      name: "node-b".into(),
      role: Some("model".into()),
      desktop: false,
    },
    NodeFacts {
      name: "desktop-pc".into(),
      role: None,
      desktop: true,
    },
  ];
  let lines = check_lines(&rules, &nodes);
  assert_eq!(
    lines[0],
    "occupant tester: takes the whole card, first on every node, seen by tickets and tester- containers, RAM every dump written out, waits behind earlier demands at most 600 s"
  );
  assert_eq!(
    lines[1],
    "occupant batch: takes the whole card, seen by batch.service processes on the card, RAM 8 GiB kept free, waits behind earlier demands at most 120 s"
  );
  assert_eq!(lines[2], "group compute (one pool), unloads to RAM (RAM at most 24 GiB): node-b");
  assert_eq!(lines[3], "group desk: desktop-pc");
  assert_eq!(lines[4], "unit model/*: resident, priority 50, group model/pipeline, unloads to NVMe");
  assert_eq!(lines[6], "unit voice/large: on-demand, priority 40, 6000 MiB");
  assert_eq!(lines[9], "rule rule-1: the desktop busy → {\"node\":true}");
  assert_eq!(
    lines[10],
    "rule voice-on-compute: voice/large → {\"cheapest\":[\"model/*\"]}, restored once done"
  );
}

#[test]
fn a_check_refuses_a_unit_no_card_it_may_stand_on_holds() {
  let rules = parse_rules(RULES, RULES_FILE).unwrap();
  let pooled = NodeFacts {
    name: "node-b".into(),
    role: Some("coder+model".into()),
    desktop: false,
  };
  let desk = NodeFacts {
    name: "desktop-pc".into(),
    role: None,
    desktop: true,
  };
  let cards = [(pooled, 12282.0), (desk, 8192.0)];
  assert!(misfits(&rules, &cards).is_empty());

  let near_full = format!(
    "{}  - unit: \"coder/stage\"\n    where: {{ role: coder }}\n    vram_mib: 11982\n",
    RULES.split("rules:\n").next().unwrap()
  );
  assert!(misfits(&parse_rules(&near_full, RULES_FILE).unwrap(), &cards).is_empty());

  let over = near_full.replace("vram_mib: 11982", "vram_mib: 11983");
  let faults = misfits(&parse_rules(&over, RULES_FILE).unwrap(), &cards);
  assert_eq!(
    faults,
    [
      "unit coder/stage's 11983 MiB fits no card it may stand on; the widest, its reserve kept back, is node-b's 11982 MiB"
    ]
  );
  // A node whose slave gave no card is not counted, so a unit only it may hold is not judged.
  assert!(misfits(&parse_rules(&over, RULES_FILE).unwrap(), &cards[1..]).is_empty());
}

#[test]
fn a_slave_gives_its_cards_memory() {
  assert_eq!(slave_card_mib(r#"{"device": {"totalMiB": 12282}}"#), Some(12282.0));
  assert_eq!(slave_card_mib(r#"{"device": null}"#), None);
  assert_eq!(slave_card_mib(""), None);
}

#[test]
fn a_slave_says_whether_its_machine_is_a_desktop() {
  assert_eq!(slave_desktop(r#"{"desktop": true, "cards": []}"#), Some(true));
  assert_eq!(slave_desktop(r#"{"cards": []}"#), Some(false));
  assert_eq!(slave_desktop(""), None);
}

#[test]
fn a_binary_in_a_release_reads_the_deploy_link_and_any_other_reads_its_own_tree() {
  let top = tempfile::tempdir().unwrap();
  let releases = top.path().join("deploy-releases");
  for tree in ["old", "live"] {
    std::fs::create_dir_all(releases.join(tree).join("bin")).unwrap();
  }
  let link = top.path().join("deploy");
  std::os::unix::fs::symlink(releases.join("live"), &link).unwrap();
  let dev = top.path().join("checkout");
  std::fs::create_dir_all(dev.join("bin")).unwrap();
  let exe_in = |tree: &Path| tree.join("bin").join("bmo-arbiter");

  assert_eq!(code_root(None, Some(&exe_in(&releases.join("old"))), &link), link);
  assert_eq!(code_root(None, Some(&exe_in(&releases.join("live"))), &link), link);
  assert_eq!(code_root(None, Some(&exe_in(&dev)), &link), dev);
  assert_eq!(code_root(Some(dev.clone()), Some(&exe_in(&releases.join("old"))), &link), dev);
  assert_eq!(
    code_root(None, Some(&exe_in(&releases.join("old"))), &top.path().join("no-link")),
    releases.join("old")
  );
}

#[test]
fn a_push_is_named_by_its_texts_hash() {
  assert_eq!(text_hash("{}\n"), "ca3d163bab05");
}
