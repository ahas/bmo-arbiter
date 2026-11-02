//! A placement asked of the standing master as a person asks it, `bmo-arbiter place`, over a world of
//! stand-ins (`common`): the node arbiter's install through the node's shell, the node service's roles, the
//! node's occupants file and a service's adapter, each run as the process it is (docs/placement.md, 「A
//! request」).

use crate::integration::common;

use common::{World, connected, events, slave};
use serde_json::json;

const DEMO: &str = "service = \"demo\"\ncard_mib = 6000\npriority = 40\nspread = \"one\"\nalone = true\nadapter = [\"bun\", \"adapter.ts\"]\n";

#[tokio::test]
async fn a_place_stands_the_arbiter_reserves_the_node_installs_there_and_takes_the_old_node_off() {
  let world =
    World::new(json!({ "a": "demo", "b": null }), &[("a", slave(json!({}))), ("b", slave(json!({})))])
      .with_service(DEMO, &["a"])
      .start();
  world
    .state_where("demo adopted on a", |state| {
      connected(state, "b")
        && state["services"].as_array().is_some_and(|all| {
          all.iter().any(|one| one["serviceName"] == "demo" && one["nodeNames"] == json!(["a"]))
        })
    })
    .await;

  let (code, said) = world.run(&["place", "demo", "--to", "b"]);

  assert_eq!(code, 0, "{said}");
  assert!(said.trim_end().ends_with("\ndone"), "{said}");
  assert!(said.contains("doing install on b"), "{said}");
  let adapter = world.read("adapter.log");
  assert!(
    adapter.contains("install b a,b\n")
      && adapter.contains("health b a,b\n")
      && adapter.contains("uninstall a b\n"),
    "{adapter}"
  );
  assert!(
    world.commands().iter().any(|line| line == "b: upload /srv/stand/bmo-card/card.py"),
    "{:#?}",
    world.commands()
  );
  assert_eq!(std::fs::read_to_string(world.kept("b", "card.py")).unwrap(), "card.py one\n");
  assert_eq!(world.nodes_file(), json!({ "a": null, "b": "demo" }));
  assert_eq!(std::fs::read_to_string(world.kept("b", "occupants")).unwrap(), "demo\n");
  assert_eq!(std::fs::read_to_string(world.kept("a", "occupants")).unwrap(), "*\n");
  world
    .state_where("demo on b, the request ended", |state| {
      state["services"][0]["nodeNames"] == json!(["b"]) && state["placement"].is_null()
    })
    .await;
  world
    .state_where("the place said", |state| events(state).iter().any(|text| text == "place demo: done"))
    .await;
}

#[tokio::test]
async fn a_place_after_the_deploy_link_moves_installs_out_of_the_release_it_names_then() {
  let world =
    World::new(json!({ "a": "demo", "b": null }), &[("a", slave(json!({}))), ("b", slave(json!({})))])
      .with_service(DEMO, &["a"])
      .behind_link()
      .start();
  let first = world
    .state_where("demo adopted on a", |state| {
      connected(state, "b") && state["services"][0]["nodeNames"] == json!(["a"])
    })
    .await;
  world.move_link("two", |tree| {
    let change = |file: &str, from: &str, to: &str| {
      let path = tree.join(file);
      let text = std::fs::read_to_string(&path).unwrap();
      assert!(text.contains(from), "{file} has no {from}");
      std::fs::write(&path, text.replace(from, to)).unwrap();
    };
    change("svc/adapter.ts", "adapter.log", "adapter-two.log");
    change("svc/demo.card.toml", "priority = 40", "priority = 45");
    change("card/card.py", "card.py one", "card.py two");
  });

  let (code, said) = world.run(&["place", "demo", "--to", "b"]);

  assert_eq!(code, 0, "{said}");
  let two = world.read("adapter-two.log");
  assert!(two.contains("install b a,b\n") && two.contains("uninstall a b\n"), "{two}");
  assert!(!world.read("adapter.log").contains("install"), "{}", world.read("adapter.log"));
  assert_eq!(std::fs::read_to_string(world.kept("b", "card.py")).unwrap(), "card.py two\n");
  world
    .state_where("the version and the declaration of the release the link names", |state| {
      state["master"]["version"] != first["master"]["version"]
        && state["services"][0]["priority"].as_f64() == Some(45.0)
    })
    .await;
}

#[tokio::test]
async fn a_place_that_fails_at_install_is_undone_and_said_so() {
  let world =
    World::new(json!({ "a": "demo", "b": null }), &[("a", slave(json!({}))), ("b", slave(json!({})))])
      .with_service(DEMO, &["a"])
      .start();
  world
    .state_where("demo adopted", |state| state["services"][0]["nodeNames"] == json!(["a"]))
    .await;
  std::fs::write(world.root().join("fail-install"), "").unwrap();

  let (code, said) = world.run(&["place", "demo", "--to", "b"]);

  assert_eq!(code, 1, "{said}");
  assert!(
    said.trim_end().lines().last().unwrap().starts_with("failed: stopped at adapter"),
    "{said}"
  );
  assert!(world.read("adapter.log").contains("uninstall b a\n"));
  assert_eq!(world.nodes_file(), json!({ "a": "demo", "b": null }));
  world
    .state_where("demo still on a", |state| state["services"][0]["nodeNames"] == json!(["a"]))
    .await;
}

#[tokio::test]
async fn a_place_with_no_master_standing_says_so() {
  let world = World::new(json!({ "a": null }), &[]);

  let (code, said) = world.run(&["place", "demo", "--to", "a"]);

  assert_eq!(code, 1);
  assert!(said.contains("does not answer"), "{said}");
}
