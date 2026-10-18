//! `card-master`, the master's binary (docs/api.md, 「The master's command line」).

use std::process::ExitCode;

fn main() -> ExitCode {
  eprintln!("card-master: no verb stands in this build");
  ExitCode::from(2)
}
