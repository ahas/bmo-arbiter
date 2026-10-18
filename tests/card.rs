//! The node arbiter's own tests (docs/node-arbiter.md, 「The checks」), the Python unit tests under
//! `tests/card/`, run on the system's Python by this Cargo test, so one `cargo test` runs every test of the
//! repository. Their output is shown where any fails.

mod support;

use std::process::Command;

use support::ROOT;

#[test]
fn the_node_arbiters_unit_tests_pass() {
  let ran = Command::new("python3")
    .args(["-B", "-m", "unittest", "discover", "-s", "tests/card"])
    .current_dir(ROOT)
    .output()
    .expect("python3 runs");
  let said = format!("{}{}", String::from_utf8_lossy(&ran.stdout), String::from_utf8_lossy(&ran.stderr));
  assert!(ran.status.success(), "the node arbiter's tests failed:\n{said}");
  assert!(said.contains("\nOK"), "the node arbiter's tests did not end OK:\n{said}");
}
