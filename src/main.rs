//! `bmo-arbiter` (docs/api.md, 「The command line」): with no verb it stands as the master, answering on its
//! loopback port; `--once` reads every slave once and prints the cluster; `mcp` is the harness plugin's MCP
//! server; `check` says what the placement rules would place where; `push` writes each node the part of them
//! that reaches it; `rules --node <node> --json` prints one node's part; `place` and `remove` ask the running
//! master to move a service; `install`, `uninstall`, `lend` and `occupants` reach the node arbiter on a node.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};

use bmo_arbiter::client::ask_master;
use bmo_arbiter::hands::LiveHands;
use bmo_arbiter::install::{
  InstallOptions, Timing, install, lend, read_occupants, rival_units_from_env, uninstall,
};
use bmo_arbiter::master::Master;
use bmo_arbiter::placement::{Board, Placer, declaration_roots_from_env};
use bmo_arbiter::reach::{NodeRegistry, NodeShell, home};
use bmo_arbiter::rules::{NodeFacts, rules_path};
use bmo_arbiter::serve::{LiveWorld, Settings, master_port, master_token, once, serve};
use bmo_arbiter::verbs::{
  Roots, carry_rules, check_lines, code_root, log_push, misfits, reach_nodes, standing_rules,
};
use bmo_arbiter_client::ArbiterClient;
use bmo_arbiter_client::placements::PlacementKind;
use bmo_arbiter_plugin::{ArbiterServer, Clients};
use clap::{Parser, Subcommand};
use rmcp::ServiceExt;
use rmcp::transport::stdio;

#[derive(Parser)]
#[command(
  name = "bmo-arbiter",
  version,
  about = "The card arbiter's master and its harness plugin"
)]
struct Cli {
  /// The data root whose placements hold the rules.
  #[arg(long, env = "BMO_DATA_DIR", global = true)]
  data_root: Option<PathBuf>,
  /// The arbiter's own tree, whose `card/` the install carries and whose version it shows: unless named,
  /// `BMO_DEPLOY_DIR`, else the deploy link where the binary stands in a release, else the tree the binary
  /// stands in.
  #[arg(long, env = "BMO_CODE_ROOT", global = true)]
  code_root: Option<PathBuf>,
  /// Read every slave once, print the picture and end.
  #[arg(long)]
  once: bool,
  #[command(subcommand)]
  verb: Option<Verb>,
}

#[derive(Subcommand)]
enum Verb {
  /// The harness plugin's MCP server on standard input and output.
  Mcp {
    /// The master's base address; `http://127.0.0.1:<CARD_MASTER_PORT>` where not named.
    #[arg(long)]
    master: Option<String>,
    /// The node arbiter's base address; `http://127.0.0.1:<CARD_TURN_PORT>` where not named.
    #[arg(long)]
    card: Option<String>,
    /// The master's token; `CARD_MASTER_TOKEN` where not named.
    #[arg(long)]
    token: Option<String>,
  },
  /// Read the rules and say what they would place where, writing nothing.
  Check,
  /// Write each node whose arbiter answers, or each node named, the part of the rules that reaches it.
  Push {
    #[arg(long = "node")]
    nodes: Vec<String>,
  },
  /// Put a service on a node through the running master: its arbiter, its role, its occupants and its
  /// adapter's install, the rest of a single-node service taken off.
  Place {
    service: String,
    #[arg(long)]
    to: String,
  },
  /// Take a service off a node through the running master.
  Remove {
    service: String,
    #[arg(long)]
    from: String,
  },
  /// Print the part of the rules that reaches one node, as the JSON a push writes.
  Rules {
    #[arg(long)]
    node: String,
    /// Print it as JSON, the one form it is given in, named so a reader of the call sees it.
    #[arg(long, required = true)]
    json: bool,
    /// Whether the machine is a desktop, where its slave does not say.
    #[arg(long)]
    desktop: Option<bool>,
  },
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

// The master's own data root, under the home.
fn own_data_root() -> PathBuf {
  home().join(".local/share/bmo/data")
}

// The arbiter's own tree, looked up through the deploy link rather than the release the binary started
// from.
fn own_code_root() -> PathBuf {
  code_root(
    std::env::var_os("BMO_DEPLOY_DIR").map(PathBuf::from),
    std::env::current_exe().ok().as_deref(),
    &home().join(".local/share/bmo-arbiter/deploy"),
  )
}

fn say(line: String) {
  println!("{line}");
}

async fn shell(roots: &Roots, node: &str) -> Result<NodeShell, String> {
  Ok(NodeShell {
    connection: roots.registry()?.read_connection(node).await?,
  })
}

/// The master standing, or read once.
async fn stand(roots: Roots, once_only: bool) -> Result<(), String> {
  let settings = Settings::from_env();
  if let Err(why) = &roots.registry {
    eprintln!("bmo-arbiter: {why}; no node is read");
  }
  let hands = Arc::new(LiveHands {
    registry: roots.registry.clone(),
    declaration_roots: roots.declaration_roots.clone(),
    code_root: roots.code_root.clone(),
    data_root: roots.data_root.clone(),
  });
  let world =
    Arc::new(LiveWorld::new(roots.registry.clone(), roots.code_root.clone()).with_hands(hands.clone()));
  let master = Master::new(world, rules_path(&roots.data_root), settings.read_every.as_secs_f64());
  if once_only {
    println!("{}", once(master).await);
    return Ok(());
  }
  let placer =
    Placer::new(hands, &roots.data_root.join("placements"), Arc::new(Mutex::new(Board::default())));
  serve(master, settings, placer).await.map_err(|fault| fault.to_string())
}

/// The node arbiter's port on this machine: `CARD_TURN_PORT`, else 7439.
fn card_port() -> u16 {
  std::env::var("CARD_TURN_PORT")
    .ok()
    .and_then(|port| port.trim().parse().ok())
    .unwrap_or(7439)
}

/// The harness plugin's MCP server over the master and this machine's node arbiter, until its harness
/// closes it.
async fn mcp(master: Option<String>, card: Option<String>, token: Option<String>) -> Result<bool, String> {
  let master = master.unwrap_or_else(|| format!("http://127.0.0.1:{}", master_port()));
  let card = card.unwrap_or_else(|| format!("http://127.0.0.1:{}", card_port()));
  let clients = Clients {
    master: ArbiterClient::new(&master, token.or_else(master_token))?,
    node: ArbiterClient::new(&card, None)?,
  };
  let served = ArbiterServer::new(clients)
    .serve(stdio())
    .await
    .map_err(|why| format!("the MCP session did not start: {why}"))?;
  served
    .waiting()
    .await
    .map_err(|why| format!("the MCP session ended in a failure: {why}"))?;
  Ok(true)
}

/// A verb that reaches the nodes; whether it did all it says, or why it could not.
async fn run(verb: Verb, roots: &Roots) -> Result<bool, String> {
  match verb {
    Verb::Mcp {
      master,
      card,
      token,
    } => mcp(master, card, token).await,
    Verb::Place { service, to } => {
      ask_master(master_port(), master_token(), PlacementKind::Place, &service, &to).await
    }
    Verb::Remove { service, from } => {
      ask_master(master_port(), master_token(), PlacementKind::Remove, &service, &from).await
    }
    Verb::Check => {
      let rules = standing_rules(roots)?;
      let reached = reach_nodes(roots, &[]).await?;
      let cards: Vec<(NodeFacts, f64)> =
        reached.iter().filter_map(|node| Some((node.facts.clone(), node.card_mib?))).collect();
      let facts: Vec<NodeFacts> = reached.into_iter().map(|node| node.facts).collect();
      for line in check_lines(&rules, &facts) {
        println!("{line}");
      }
      let faults = misfits(&rules, &cards);
      for fault in &faults {
        eprintln!("bmo-arbiter: {fault}");
      }
      Ok(faults.is_empty())
    }
    Verb::Push { nodes } => {
      let rules = standing_rules(roots)?;
      let mut failed = false;
      for node in reach_nodes(roots, &nodes).await? {
        // A node with no arbiter answering is left as it stands unless named.
        if nodes.is_empty() && !node.answers {
          println!("{}: no arbiter answers, so it is passed by", node.facts.name);
          continue;
        }
        let part = rules.for_node(&node.facts);
        match carry_rules(&node.shell, &part).await {
          Ok(hash) => {
            log_push(&roots.data_root, &node.facts.name, &hash);
            println!(
              "{}: {} occupants, {} groups, {} units, {} rules, {} tiers ({hash})",
              node.facts.name,
              part.occupants.len(),
              part.groups.len(),
              part.units.len(),
              part.rules.len(),
              part.tiers.len()
            );
          }
          Err(fault) => {
            eprintln!("bmo-arbiter: the rules were not written to {}: {fault}", node.facts.name);
            failed = true;
          }
        }
      }
      Ok(!failed)
    }
    Verb::Rules {
      node,
      json: _,
      desktop,
    } => {
      let rules = standing_rules(roots)?;
      let Some(reached) = reach_nodes(roots, std::slice::from_ref(&node)).await?.into_iter().next() else {
        return Ok(false);
      };
      let facts = NodeFacts {
        desktop: if reached.answers { reached.facts.desktop } else { desktop.unwrap_or(false) },
        ..reached.facts
      };
      print!("{}", rules.for_node(&facts).node_text());
      Ok(true)
    }
    Verb::Install { nodes, store_gib } => {
      for node in nodes {
        let connection = roots.registry()?.read_connection(&node).await?;
        let options = InstallOptions {
          role: connection.role.clone(),
          code_root: roots.code_root.clone(),
          store_gib: store_gib.filter(|gib| *gib > 0.0),
          rival_units: rival_units_from_env(),
          timing: Timing::from_env(),
        };
        let outcome = install(&node, &NodeShell { connection }, &options, &say).await;
        if !outcome.ok {
          let step = outcome.step.map_or("?", |step| step.word());
          return Err(format!("{node} stopped at {step}: {}", outcome.fault.unwrap_or_default()));
        }
        say(format!("{node}: the arbiter stands"));
      }
      Ok(true)
    }
    Verb::Uninstall { nodes, keep_store } => {
      for node in nodes {
        uninstall(&shell(roots, &node).await?, keep_store)
          .await
          .map_err(|fault| format!("the arbiter was not taken off {node}: {fault}"))?;
        say(format!("{node}: the arbiter is taken off"));
      }
      Ok(true)
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
      let calls = shell(roots, &node).await?;
      lend(&node, &calls, text.as_deref(), &rival_units_from_env(), &Timing::from_env(), &say).await?;
      Ok(true)
    }
    Verb::Occupants { node } => {
      let view = read_occupants(&shell(roots, &node).await?, 5).await?;
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
      Ok(true)
    }
  }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
  let cli = Cli::parse();
  let roots = Roots {
    data_root: cli.data_root.unwrap_or_else(own_data_root),
    code_root: cli.code_root.unwrap_or_else(own_code_root),
    declaration_roots: declaration_roots_from_env(),
    registry: NodeRegistry::from_env(),
  };
  let ran = match cli.verb {
    None => stand(roots, cli.once).await.map(|()| true),
    Some(verb) => run(verb, &roots).await,
  };
  match ran {
    Ok(true) => ExitCode::SUCCESS,
    Ok(false) => ExitCode::FAILURE,
    Err(fault) => {
      eprintln!("bmo-arbiter: {fault}");
      ExitCode::FAILURE
    }
  }
}
