//! Dependencies: no other repository's crate but the base libraries every unit may build on and the
//! published clients of the units this one calls, and no path out of the repository but to one of those.

mod support;

use std::fs;
use std::path::{Component, Path, PathBuf};

use support::{ROOT, files_under, shown};
use toml::{Table, Value};

/// The crates of other repositories the arbiter may depend on: the base libraries, which know no unit, and
/// the published client of the node service it calls (docs/api.md, 「The node service」).
const OTHER_REPOSITORIES: [&str; 4] = ["bmo-chat", "bmo-chat-client", "bmo-ui", "bmo-node-client"];

/// The dependency tables a manifest or a target section may carry.
const DEPENDENCY_TABLES: [&str; 5] = [
  "dependencies",
  "dev-dependencies",
  "build-dependencies",
  "dev_dependencies",
  "build_dependencies",
];

/// `path` with `.` and `..` resolved by reading alone, so a path that does not exist still compares.
fn lexical(path: &Path) -> PathBuf {
  let mut out = PathBuf::new();
  for component in path.components() {
    match component {
      Component::CurDir => {}
      Component::ParentDir => {
        out.pop();
      }
      other => out.push(other),
    }
  }
  out
}

/// The dependency tables standing directly in one section of a manifest.
fn own(section: &Table) -> impl Iterator<Item = &Table> {
  DEPENDENCY_TABLES.iter().filter_map(|name| section.get(*name).and_then(Value::as_table))
}

/// Every dependency table in a manifest: the package's own, each target's, a workspace's shared one,
/// and each patch source's.
fn dependency_tables(manifest: &Table) -> Vec<&Table> {
  let mut tables: Vec<&Table> = own(manifest).collect();
  if let Some(targets) = manifest.get("target").and_then(Value::as_table) {
    for target in targets.values().filter_map(Value::as_table) {
      tables.extend(own(target));
    }
  }
  if let Some(shared) = manifest
    .get("workspace")
    .and_then(|workspace| workspace.get("dependencies"))
    .and_then(Value::as_table)
  {
    tables.push(shared);
  }
  if let Some(patches) = manifest.get("patch").and_then(Value::as_table) {
    tables.extend(patches.values().filter_map(Value::as_table));
  }
  tables
}

/// What breaks the rules among one manifest's dependencies, read as standing in `manifest_dir` of a
/// repository rooted at `root`.
fn violations(root: &Path, manifest_dir: &Path, manifest: &Table, shown_as: &str) -> Vec<String> {
  let mut found = Vec::new();
  for table in dependency_tables(manifest) {
    for (key, spec) in table {
      let detail = spec.as_table();
      let name = detail.and_then(|d| d.get("package")).and_then(Value::as_str).unwrap_or(key);
      let path = detail.and_then(|d| d.get("path")).and_then(Value::as_str);
      match path {
        Some(path)
          if !lexical(&manifest_dir.join(path)).starts_with(root)
            && !OTHER_REPOSITORIES.contains(&name) =>
        {
          found.push(format!("{shown_as}: {key} reaches outside the repository through {path}"));
        }
        Some(_) => {}
        None if name.starts_with("bmo-") && !OTHER_REPOSITORIES.contains(&name) => {
          found.push(format!("{shown_as}: {key} is another unit's crate ({name})"));
        }
        None => {}
      }
    }
  }
  found
}

#[test]
fn dependencies_stay_in_the_repository_and_name_no_other_unit() {
  let root = lexical(Path::new(ROOT));
  let found: Vec<String> = files_under(&root)
    .into_iter()
    .filter(|path| path.file_name().is_some_and(|name| name == "Cargo.toml"))
    .flat_map(|path| {
      let text = fs::read_to_string(&path).expect("a manifest is readable");
      let manifest: Table = text.parse().unwrap_or_else(|error| panic!("{} reads: {error}", shown(&path)));
      let dir = path.parent().expect("a manifest stands in a directory").to_path_buf();
      violations(&root, &dir, &manifest, &shown(&path))
    })
    .collect();
  assert!(found.is_empty(), "these dependencies break the rules: {found:?}");
}

#[test]
fn the_testbed_allows_the_same_base_libraries() {
  let text = fs::read_to_string(Path::new(ROOT).join("testbed.toml")).expect("testbed.toml is readable");
  let declaration: Table = text.parse().expect("testbed.toml is TOML");
  let allowed: Vec<&str> = declaration["check"]["base_libraries"]
    .as_array()
    .expect("[check] base_libraries is a list")
    .iter()
    .filter_map(Value::as_str)
    .collect();
  assert_eq!(allowed, OTHER_REPOSITORIES);
}

#[test]
fn a_manifest_breaking_the_rules_is_named() {
  let manifest: Table = r#"
    [dependencies]
    serde = "1"
    bmo-chat = { version = "0.1" }
    bmo-agency = { git = "https://example.invalid/agency" }
    renamed = { package = "bmo-receiver", version = "0.1" }
    inside = { path = "src/inside" }
    outside = { path = "../../elsewhere" }
    bmo-node-client = { path = "../node/crates/client", version = "=0.1.0-rev.2" }

    [target.'cfg(unix)'.dev-dependencies]
    bmo-ui = "0.1"

    [workspace.dependencies]
    bmo-testbed = "0.1"
  "#
  .parse()
  .expect("the sample reads");
  let root = Path::new("/work/repository");
  let mut found = violations(root, root, &manifest, "sample");
  found.sort();
  assert_eq!(
    found,
    [
      "sample: bmo-agency is another unit's crate (bmo-agency)",
      "sample: bmo-testbed is another unit's crate (bmo-testbed)",
      "sample: outside reaches outside the repository through ../../elsewhere",
      "sample: renamed is another unit's crate (bmo-receiver)",
    ]
  );
}
