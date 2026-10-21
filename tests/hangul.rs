//! No Hangul beyond the ledger: the arbiter answers in English and in codes, and a person's words in
//! another language are a console's language pack. The ledger, `korean-ledger.json`, is a JSON object from
//! each file's path to the lines with Korean it may hold; a file it does not name may hold none.

mod support;

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use support::{ROOT, files_under, shown};

const LEDGER: &str = "korean-ledger.json";

/// The Hangul syllables block, U+AC00 to U+D7A3, named by code point so this file holds none of it.
fn is_hangul(c: char) -> bool {
  (0xAC00..=0xD7A3).contains(&u32::from(c))
}

/// How many lines of `text` hold Hangul.
fn hangul_lines(text: &str) -> usize {
  text.lines().filter(|line| line.chars().any(is_hangul)).count()
}

/// Each file holding more lines with Hangul than `ledger` allows it, with both counts.
fn over_ledger(files: &[(String, String)], ledger: &BTreeMap<String, usize>) -> Vec<String> {
  files
    .iter()
    .filter_map(|(path, text)| {
      let held = hangul_lines(text);
      let allowed = ledger.get(path).copied().unwrap_or(0);
      (held > allowed).then(|| format!("{path}: {held} lines with Hangul, the ledger allows {allowed}"))
    })
    .collect()
}

fn ledger() -> BTreeMap<String, usize> {
  let text = fs::read_to_string(Path::new(ROOT).join(LEDGER)).expect("the ledger is readable");
  serde_json::from_str(&text).expect("the ledger is a JSON object from paths to counts")
}

#[test]
fn no_file_holds_more_hangul_than_the_ledger_allows() {
  let files: Vec<(String, String)> = files_under(Path::new(ROOT))
    .into_iter()
    .map(|path| {
      let bytes = fs::read(&path).expect("a file is readable");
      (shown(&path), String::from_utf8_lossy(&bytes).into_owned())
    })
    .collect();
  let found = over_ledger(&files, &ledger());
  assert!(found.is_empty(), "these files hold Hangul past the ledger: {found:?}");
}

#[test]
fn the_ledger_allows_nothing() {
  assert!(ledger().is_empty(), "the ledger allows Korean lines: {:?}", ledger());
}

#[test]
fn a_file_past_its_count_is_named_and_the_range_is_the_whole_block() {
  let first = char::from_u32(0xAC00).expect("a code point");
  let last = char::from_u32(0xD7A3).expect("a code point");
  let files = [
    ("allowed.rs".to_owned(), format!("ok\nsays {first}\n")),
    ("unnamed.rs".to_owned(), format!("ends {last}\n")),
    ("clean.rs".to_owned(), "fine\n".to_owned()),
  ];
  let ledger = BTreeMap::from([("allowed.rs".to_owned(), 1)]);
  assert_eq!(over_ledger(&files, &ledger), ["unnamed.rs: 1 lines with Hangul, the ledger allows 0"]);
  let outside = [char::from_u32(0xABFF), char::from_u32(0xD7A4)].map(|c| c.expect("a code point"));
  assert!(!outside.into_iter().any(is_hangul));
}
