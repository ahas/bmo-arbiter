//! `card-master`, the master's binary (docs/api.md, 「The master's command line」): `install`, `uninstall`,
//! `lend` and `occupants` reach the nodes the node registry names.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use bmo_arbiter::install::{
  InstallOptions, Timing, install, lend, read_occupants, rival_units_from_env, uninstall,
};
use bmo_arbiter::reach::{NodeRegistry, NodeShell};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "card-master", about = "The card arbiter's master")]
struct Cli {
  /// The arbiter's own tree, whose `card/` the install carries and whose version it writes.
  #[arg(long, env = "BMO_CODE_ROOT", global = true)]
  code_root: Option<PathBuf>,
  #[command(subcommand)]
  verb: Verb,
}

#[derive(Subcommand)]
enum Verb {
  /// Stand or update the node arbiter on each node named.
  Install {
    #[arg(long = "node", required = true)]
    nodes: Vec<String>,
    /// The dump store's size in GiB; the card's memory where not given.
    #[arg(long)]
    store_gib: Option<f64>,
  },
  /// Take the node arbiter off each node named, its dump store with it unless kept.
  Uninstall {
    #[arg(long = "node", required = true)]
    nodes: Vec<String>,
    #[arg(long)]
    keep_store: bool,
  },
  /// Write a node's lend settings, or take them off, and restart its arbiter.
  Lend {
    #[arg(long)]
    node: String,
    /// The text of the lend settings, `{home}` standing for the node account's home.
    #[arg(long, conflicts_with = "off", required_unless_present = "off")]
    settings: Option<PathBuf>,
    /// Take the lend settings off, so the card is lent no more.
    #[arg(long)]
    off: bool,
  },
  /// Print a node's occupants order and the last changes its arbiter took up.
  Occupants {
    #[arg(long)]
    node: String,
  },
}

// The tree the binary stands in, `<tree>/bin/card-master`, where none is named.
fn own_code_root() -> PathBuf {
  std::env::current_exe()
    .ok()
    .and_then(|exe| Some(exe.parent()?.parent()?.to_owned()))
    .unwrap_or_else(|| PathBuf::from("."))
}

fn say(line: String) {
  println!("{line}");
}

async fn shell(registry: &NodeRegistry, node: &str) -> Result<NodeShell, String> {
  Ok(NodeShell {
    connection: registry.read_connection(node).await?,
  })
}

async fn run(verb: Verb, code_root: &Path) -> Result<(), String> {
  let registry = NodeRegistry::from_env()?;
  match verb {
    Verb::Install { nodes, store_gib } => {
      for node in nodes {
        let connection = registry.read_connection(&node).await?;
        let options = InstallOptions {
          role: connection.role.clone(),
          code_root: code_root.to_owned(),
          store_gib: store_gib.filter(|gib| *gib > 0.0),
          rival_units: rival_units_from_env(),
          timing: Timing::default(),
        };
        let outcome = install(&node, &NodeShell { connection }, &options, &say).await;
        if !outcome.ok {
          let step = outcome.step.map_or("?", |step| step.word());
          return Err(format!("{node} stopped at {step}: {}", outcome.fault.unwrap_or_default()));
        }
        say(format!("{node}: the arbiter stands"));
      }
    }
    Verb::Uninstall { nodes, keep_store } => {
      for node in nodes {
        uninstall(&shell(&registry, &node).await?, keep_store)
          .await
          .map_err(|fault| format!("the arbiter was not taken off {node}: {fault}"))?;
        say(format!("{node}: the arbiter is taken off"));
      }
    }
    Verb::Lend {
      node,
      settings,
      off,
    } => {
      let text = match settings.filter(|_| !off) {
        Some(path) => {
          Some(std::fs::read_to_string(&path).map_err(|fault| format!("{}: {fault}", path.display()))?)
        }
        None => None,
      };
      let calls = shell(&registry, &node).await?;
      lend(&node, &calls, text.as_deref(), &rival_units_from_env(), &Timing::default(), &say).await?;
    }
    Verb::Occupants { node } => {
      let view = read_occupants(&shell(&registry, &node).await?, 5).await?;
      say(format!(
        "{node} occupants: {}",
        if view.order.is_empty() { "(no file: *)" } else { view.order.as_str() }
      ));
      if view.history.is_empty() {
        say("changes: none".to_owned());
      } else {
        say("changes, latest last:".to_owned());
        for line in view.history {
          say(format!("  {line}"));
        }
      }
    }
  }
  Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
  let cli = Cli::parse();
  let code_root = cli.code_root.unwrap_or_else(own_code_root);
  match run(cli.verb, &code_root).await {
    Ok(()) => ExitCode::SUCCESS,
    Err(fault) => {
      eprintln!("card-master: {fault}");
      ExitCode::from(1)
    }
  }
}
