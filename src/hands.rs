//! The world a placement acts on (docs/placement.md, 「Placing the services」): the services declared in
//! the declaration roots, read as each request is taken, the node service, each node's shell, the node
//! arbiter's install, and each service's adapter run as the command it is.

use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::install::{InstallOptions, Timing, install, rival_units_from_env};
use crate::master::BoxFuture;
use crate::placement::{AdapterAnswer, Declaration, Hands, NodeRow, Say, declarations};
use crate::reach::{NodeRegistry, NodeShell, bun, run_streaming};
use crate::rules::{NodeFacts, read_rules, rules_path};
use crate::verbs::{SLAVE_UNITS, carry_rules, log_push, slave_desktop, with_turns};

// How long each kind of call may take: an install builds environments and carries checkpoints.
const ARBITER_LIMIT: Duration = Duration::from_secs(45 * 60);
const ADAPTER_LIMIT: Duration = Duration::from_secs(3 * 60 * 60);
const ASK_LIMIT: Duration = Duration::from_secs(10 * 60);

const OCCUPANTS: &str = ".config/bmo-card/occupants";
const NONE_MARK: &str = "__BMO_NO_OCCUPANTS__";

pub struct LiveHands {
  /// The node service, or why none is named.
  pub registry: Result<NodeRegistry, String>,
  /// The roots the services' declarations are read from.
  pub declaration_roots: Vec<PathBuf>,
  /// The arbiter's own tree, whose `card/` its install carries.
  pub code_root: PathBuf,
  pub data_root: PathBuf,
}

impl LiveHands {
  fn registry(&self) -> Result<&NodeRegistry, String> {
    self.registry.as_ref().map_err(Clone::clone)
  }

  async fn shell(&self, node: &str) -> Result<NodeShell, String> {
    Ok(NodeShell {
      connection: self.registry()?.read_connection(node).await?,
    })
  }
}

fn fault_of(ran: &crate::reach::RunResult) -> String {
  ran.fault()
}

/// What an adapter's last line says, or a refusal naming what it wrote instead.
pub fn answer_of(stdout: &str, code: i32, stderr: &str) -> AdapterAnswer {
  let last = stdout.lines().map(str::trim).rfind(|line| !line.is_empty()).unwrap_or_default();
  match serde_json::from_str::<AdapterAnswer>(last) {
    Ok(answer) => answer,
    Err(_) => {
      let said = if stderr.trim().is_empty() { last } else { stderr.trim() };
      AdapterAnswer::refused(format!(
        "the adapter gave no answer (exit code {code}): {}",
        said.chars().rev().take(300).collect::<String>().chars().rev().collect::<String>()
      ))
    }
  }
}

impl Hands for LiveHands {
  fn declarations(&self) -> Result<Vec<Declaration>, String> {
    declarations(&self.declaration_roots)
  }

  fn nodes(&self) -> BoxFuture<'_, Result<Vec<NodeRow>, String>> {
    Box::pin(async move {
      let registry = self.registry()?;
      let mut rows = Vec::new();
      for name in registry.listed_nodes().await {
        let connection = registry.listed_connection(&name).await?;
        rows.push(NodeRow {
          name,
          role: connection.role,
        });
      }
      Ok(rows)
    })
  }

  fn desktop<'a>(&'a self, node: &'a str) -> BoxFuture<'a, bool> {
    Box::pin(async move {
      match self.shell(node).await {
        Ok(shell) => slave_desktop(&shell.exec(SLAVE_UNITS).await.stdout).unwrap_or(false),
        Err(_) => false,
      }
    })
  }

  fn set_role<'a>(&'a self, node: &'a str, role: Option<&'a str>) -> BoxFuture<'a, Result<(), String>> {
    Box::pin(async move { self.registry()?.set_role(node, role).await })
  }

  fn occupants<'a>(&'a self, node: &'a str) -> BoxFuture<'a, Result<Option<String>, String>> {
    Box::pin(async move {
      let shell = self.shell(node).await?;
      let ran = shell.exec(&format!("cat ~/{OCCUPANTS} 2>/dev/null || echo {NONE_MARK}")).await;
      if !ran.ok() {
        return Err(fault_of(&ran));
      }
      let text = ran.stdout.trim();
      Ok((text != NONE_MARK).then(|| text.to_owned()))
    })
  }

  fn write_occupants<'a>(
    &'a self,
    node: &'a str,
    order: Option<&'a str>,
  ) -> BoxFuture<'a, Result<(), String>> {
    Box::pin(async move {
      let shell = self.shell(node).await?;
      let Some(order) = order else {
        let ran = shell.exec(&format!("rm -f ~/{OCCUPANTS}")).await;
        return if ran.ok() { Ok(()) } else { Err(fault_of(&ran)) };
      };
      // A transfer opens the path it is handed with no shell in the way, so it is named from the home.
      let asked = shell.exec("echo HOME=$HOME").await;
      let home = asked
        .stdout
        .lines()
        .find_map(|line| line.strip_prefix("HOME="))
        .map(str::trim)
        .filter(|home| home.starts_with('/'));
      let Some(home) = home else {
        return Err(format!("{node} did not answer: {}", fault_of(&asked)));
      };
      let scratch = tempfile::Builder::new()
        .prefix("bmo-card-occupants-")
        .tempdir()
        .map_err(|fault| fault.to_string())?;
      let local = scratch.path().join("occupants");
      std::fs::write(&local, format!("{order}\n")).map_err(|fault| fault.to_string())?;
      let made = shell.exec("mkdir -p ~/.config/bmo-card").await;
      if !made.ok() {
        return Err(fault_of(&made));
      }
      let carried = shell.upload(&local, &format!("{home}/{OCCUPANTS}.partial")).await;
      if !carried.ok() {
        return Err(fault_of(&carried));
      }
      let moved = shell.exec(&format!("mv ~/{OCCUPANTS}.partial ~/{OCCUPANTS}")).await;
      if moved.ok() { Ok(()) } else { Err(fault_of(&moved)) }
    })
  }

  fn arbiter<'a>(&'a self, node: &'a str, say: &'a Say) -> BoxFuture<'a, Result<(), String>> {
    Box::pin(async move {
      let rules = read_rules(&rules_path(&self.data_root)).map_err(|fault| fault.0)?.unwrap_or_default();
      let rules = with_turns(rules, &self.declaration_roots)?;
      let connection = self.registry()?.listed_connection(node).await?;
      let shell = NodeShell { connection };
      let desktop = slave_desktop(&shell.exec(SLAVE_UNITS).await.stdout).unwrap_or(false);
      let facts = NodeFacts {
        name: node.to_owned(),
        role: shell.connection.role.clone(),
        desktop,
      };
      let options = InstallOptions {
        role: facts.role.clone(),
        code_root: self.code_root.clone(),
        store_gib: rules.group_of(&facts).and_then(|group| group.store_gib).map(|size| size.0),
        rival_units: rival_units_from_env(),
        timing: Timing::from_env(),
      };
      let told = |line: String| say(line);
      let outcome = tokio::time::timeout(ARBITER_LIMIT, install(node, &shell, &options, &told))
        .await
        .map_err(|_| format!("the arbiter's install did not end within {} s", ARBITER_LIMIT.as_secs()))?;
      if !outcome.ok {
        return Err(format!(
          "the arbiter was not stood: stopped at {}: {}",
          outcome.step.map_or("?", |step| step.word()),
          outcome.fault.unwrap_or_default()
        ));
      }
      let part = rules.for_node(&facts);
      let hash = carry_rules(&shell, &part)
        .await
        .map_err(|fault| format!("the rules were not written: {fault}"))?;
      log_push(&self.data_root, node, &hash);
      say(format!("{node}: rules {hash}"));
      Ok(())
    })
  }

  fn adapter<'a>(
    &'a self,
    declaration: &'a Declaration,
    verb: &'a str,
    node: Option<&'a str>,
    placed: &'a [String],
    say: &'a Say,
  ) -> BoxFuture<'a, AdapterAnswer> {
    Box::pin(async move {
      let Some(command) = &declaration.adapter else {
        return AdapterAnswer {
          ok: true,
          node: node.map(str::to_owned),
          state: Some("running".into()),
          ..AdapterAnswer::default()
        };
      };
      let mut argv: Vec<String> =
        command.iter().map(|word| if word == "bun" { bun() } else { word.clone() }).collect();
      argv.push(verb.to_owned());
      if let Some(node) = node {
        argv.extend(["--node".to_owned(), node.to_owned()]);
      }
      argv.extend(["--placed".to_owned(), placed.join(",")]);
      let limit = if verb == "install" || verb == "uninstall" { ADAPTER_LIMIT } else { ASK_LIMIT };
      let ran = run_streaming(&argv, Some(Path::new(&declaration.dir)), limit, &|line| {
        if !line.trim_start().starts_with('{') {
          say(line.to_owned());
        }
      })
      .await;
      if let Some(seconds) = ran.timed_out {
        return AdapterAnswer::refused(format!("{verb} did not end within {seconds} s"));
      }
      answer_of(&ran.stdout, ran.code, &ran.stderr)
    })
  }
}
