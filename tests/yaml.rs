//! The YAML the rules file is read as (docs/placement.md, 「The rules」): mappings keep their written order,
//! the old boolean words stay words, and numbers are written as a script writes them.

use bmo_arbiter::yaml::{Yaml, js_number, parse};

#[test]
fn mappings_keep_their_written_order() {
  let read = parse("b: 1\na: [x, 2.5]\nc: { z: true, y: null }\n").unwrap();
  let Yaml::Map(entries) = &read else { panic!("not a mapping") };
  assert_eq!(entries.iter().map(|(name, _)| name.as_str()).collect::<Vec<_>>(), ["b", "a", "c"]);
  assert_eq!(read.get("a"), Some(&Yaml::List(vec![Yaml::Text("x".into()), Yaml::Number(2.5)])));
  assert!(!read.get("c").unwrap().has("y"));
}

#[test]
fn words_an_older_yaml_took_for_booleans_stay_words() {
  let read = parse("on: yes\nrestore: true\n").unwrap();
  assert_eq!(read.get("on"), Some(&Yaml::Text("yes".into())));
  assert_eq!(read.get("restore"), Some(&Yaml::Bool(true)));
}

#[test]
fn numbers_are_written_as_a_script_writes_them() {
  assert_eq!(js_number(2.0), "2");
  assert_eq!(js_number(0.5), "0.5");
  assert_eq!(js_number(11875.0), "11875");
}
