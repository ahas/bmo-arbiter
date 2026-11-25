//! The repository's layout: the master's Cargo package at the root with the crates it publishes as its
//! workspace's members, no other build tool's manifest at the root, no directory named after its parent, and
//! one version.

use crate::support;

use std::fs;
use std::path::Path;

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
  assert_eq!(
    manifests,
    [
      "Cargo.toml",
      "crates/client/Cargo.toml",
      "crates/plugin/Cargo.toml",
      "tools/gpu/Cargo.toml"
    ]
  );
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
