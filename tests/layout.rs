//! The repository's layout: the master's Cargo package at the root with the crates it publishes as its
//! workspace's members, no other build tool's manifest at the root, no directory named after its parent, and
//! one version.

use crate::support;

use std::fs;
use std::path::Path;

use pulldown_cmark::{Event, Options, Parser, Tag, TagEnd};
use support::{ROOT, files_under, is_skipped, shown};

/// File names that declare a package or a project to some other build tool.
const OTHER_MANIFESTS: [&str; 12] = [
  "package.json",
  "bun.lock",
  "bun.lockb",
  "deno.json",
  "pnpm-workspace.yaml",
  "pyproject.toml",
  "setup.py",
  "go.mod",
  "Gemfile",
  "pom.xml",
  "build.gradle",
  "Makefile",
];

#[test]
fn only_cargo_manifests_at_the_root() {
  let found: Vec<String> = fs::read_dir(ROOT)
    .expect("the repository root is readable")
    .map(|entry| entry.expect("a root entry is readable").file_name().to_string_lossy().into_owned())
    .filter(|name| OTHER_MANIFESTS.contains(&name.as_str()))
    .collect();
  assert!(found.is_empty(), "the root holds {found:?}; only Cargo manifests belong there");
}

#[test]
fn the_cargo_manifests_are_the_roots_and_its_published_crates() {
  let manifests: Vec<String> = files_under(Path::new(ROOT))
    .into_iter()
    .filter(|path| path.file_name().is_some_and(|name| name == "Cargo.toml"))
    .map(|path| shown(&path))
    .collect();
  assert_eq!(manifests, ["Cargo.toml", "crates/client/Cargo.toml", "crates/plugin/Cargo.toml"]);
}

fn nested_twins(dir: &Path, twins: &mut Vec<String>) {
  let parent = dir.file_name().map(|name| name.to_string_lossy().into_owned());
  for entry in fs::read_dir(dir).expect("a directory is readable") {
    let entry = entry.expect("an entry is readable");
    let name = entry.file_name().to_string_lossy().into_owned();
    if !entry.file_type().expect("an entry's type is readable").is_dir() || is_skipped(&name) {
      continue;
    }
    if parent.as_deref() == Some(name.as_str()) {
      twins.push(shown(&entry.path()));
    }
    nested_twins(&entry.path(), twins);
  }
}

#[test]
fn no_directory_is_named_after_its_parent() {
  let mut twins = Vec::new();
  nested_twins(Path::new(ROOT), &mut twins);
  assert!(twins.is_empty(), "these directories repeat their parent's name: {twins:?}");
}

#[test]
fn a_directory_named_after_its_parent_is_found() {
  let root = tempfile::tempdir().expect("a scratch directory");
  fs::create_dir_all(root.path().join("card/card")).expect("the sample stands");
  fs::create_dir_all(root.path().join("docs/notes")).expect("the sample stands");
  let mut twins = Vec::new();
  nested_twins(root.path(), &mut twins);
  assert_eq!(twins.len(), 1, "{twins:?}");
  assert!(twins[0].ends_with("card/card"), "{twins:?}");
}

/// The first column of the README's table whose first header cell is `Folder`, each cell's text with
/// its code marks and a trailing `/` taken off.
fn folder_table(markdown: &str) -> Vec<String> {
  let mut folders = Vec::new();
  let (mut in_head, mut first_cell, mut in_folder_table) = (false, false, false);
  let mut cell = String::new();
  for event in Parser::new_ext(markdown, Options::ENABLE_TABLES) {
    match event {
      Event::Start(Tag::Table(_)) => in_folder_table = false,
      // The head's cells stand in no row of their own.
      Event::Start(Tag::TableHead) => (in_head, first_cell) = (true, true),
      Event::End(TagEnd::TableHead) => in_head = false,
      Event::Start(Tag::TableRow) => first_cell = true,
      Event::Start(Tag::TableCell) => cell.clear(),
      Event::End(TagEnd::TableCell) => {
        let text = cell.trim().trim_end_matches('/').to_owned();
        if first_cell && in_head {
          in_folder_table = text == "Folder";
        } else if first_cell && in_folder_table {
          folders.push(text);
        }
        first_cell = false;
      }
      Event::Text(text) | Event::Code(text) => cell.push_str(&text),
      _ => {}
    }
  }
  folders
}

#[test]
fn the_readme_names_every_top_level_folder_and_no_other() {
  let readme = fs::read_to_string(Path::new(ROOT).join("README.md")).expect("README.md is readable");
  let mut named = folder_table(&readme);
  named.sort();
  let mut standing: Vec<String> = fs::read_dir(ROOT)
    .expect("the repository root is readable")
    .map(|entry| entry.expect("a root entry is readable"))
    .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
    .map(|entry| entry.file_name().to_string_lossy().into_owned())
    .filter(|name| !is_skipped(name))
    .collect();
  standing.sort();
  assert_eq!(named, standing, "the README's folder table and the top-level folders differ");
}

#[test]
fn the_folder_table_is_read_from_its_first_column() {
  let markdown = "| Name | What |\n|---|---|\n| `other/` | no |\n\n| Folder | Holds |\n|---|---|\n| `src/` | code |\n| `tests` | checks |\n";
  assert_eq!(folder_table(markdown), ["src", "tests"]);
}

#[test]
fn the_version_file_is_the_package_version() {
  let version = fs::read_to_string(Path::new(ROOT).join("VERSION")).expect("VERSION is readable");
  assert_eq!(version.trim(), env!("CARGO_PKG_VERSION"));
}
