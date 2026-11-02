//! What the tests share: the root and the walk over the files the repository checks read.

pub mod contract;

use std::fs;
use std::path::{Path, PathBuf};

pub const ROOT: &str = env!("CARGO_MANIFEST_DIR");

/// Directories no check reads: build output, git's own, Python's byte-code caches, and the TypeScript
/// package's installed dependencies.
const SKIPPED: [&str; 4] = ["target", ".git", "__pycache__", "node_modules"];

pub fn is_skipped(name: &str) -> bool {
  SKIPPED.contains(&name)
}

/// Every file under `dir`, at any depth, skipping the directories no check reads. A directory that does
/// not exist holds no files.
pub fn files_under(dir: &Path) -> Vec<PathBuf> {
  let mut files = Vec::new();
  if dir.is_dir() {
    collect(dir, &mut files);
  }
  files.sort();
  files
}

fn collect(dir: &Path, files: &mut Vec<PathBuf>) {
  for entry in fs::read_dir(dir).expect("a directory is readable") {
    let entry = entry.expect("an entry is readable");
    let kind = entry.file_type().expect("an entry's type is readable");
    if kind.is_dir() {
      if !is_skipped(&entry.file_name().to_string_lossy()) {
        collect(&entry.path(), files);
      }
    } else if kind.is_file() {
      files.push(entry.path());
    }
  }
}

/// A path as it reads relative to the root, for a failure's message.
pub fn shown(path: &Path) -> String {
  path.strip_prefix(ROOT).unwrap_or(path).display().to_string()
}
