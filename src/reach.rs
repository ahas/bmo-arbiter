//! How the master reaches a node by its registered name (docs/api.md, 「The node registry」, 「Node
//! shells」). What a node is reached by belongs to the node registry, so it is asked of the registry's
//! command at every run; a node of the console kind is driven through the console script by its agent's
//! name, an ssh node by a batch login, and either runs a command in bash.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::Stdio;
use std::time::Duration;

use regex::Regex;
use serde::Deserialize;
use tokio::process::Command;

// A command is given one minute more than the hub streams one for, since the hub stops a command itself at
// five; a transfer twenty times the slowest the tunnel has carried and a minute to open.
const COMMAND_LIMIT: Duration = Duration::from_secs(6 * 60);
const TRANSFER_OPEN: Duration = Duration::from_secs(60);
const TRANSFER_BYTES_PER_SECOND: u64 = 1_000_000;

/// A future boxed to be handed across a trait object.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// What a call on a node came back with.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunResult {
  pub code: i32,
  pub stdout: String,
  pub stderr: String,
  /// Where the call outlived its limit and was killed, the limit in seconds.
  pub timed_out: Option<u64>,
  /// How many times the call was made, where more than once.
  pub attempts: u32,
}

impl RunResult {
  pub fn ok(&self) -> bool {
    self.code == 0 && self.timed_out.is_none()
  }

  /// Why the call came back as it did, never empty: the limit it outlived or its exit code, with the tail
  /// of what it wrote.
  pub fn fault(&self) -> String {
    let said =
      tail(if self.stderr.trim().is_empty() { self.stdout.trim() } else { self.stderr.trim() }, 300);
    let why = match self.timed_out {
      Some(seconds) => format!("timed out after {seconds} s"),
      None => format!("exit code {}", self.code),
    };
    let whole = if said.is_empty() { format!("{why}, no output") } else { format!("{why}: {said}") };
    if self.attempts > 1 {
      format!("tried {} times, the last {whole}", self.attempts)
    } else {
      whole
    }
  }

  /// A call that never ran, and why.
  pub fn refused(why: impl Into<String>) -> Self {
    Self {
      code: -1,
      stderr: why.into(),
      attempts: 1,
      ..Self::default()
    }
  }
}

fn tail(text: &str, chars: usize) -> &str {
  let count = text.chars().count();
  if count <= chars {
    return text;
  }
  let start = text.char_indices().nth(count - chars).map_or(0, |(at, _)| at);
  &text[start..]
}

/// A program run to its end or to its limit, each line it writes to its standard output handed on as it
/// comes and kept whole besides; killed where it outlives its limit.
pub async fn run_streaming(
  argv: &[String],
  cwd: Option<&Path>,
  limit: Duration,
  on_line: &(dyn Fn(&str) + Send + Sync),
) -> RunResult {
  use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};

  let Some((program, args)) = argv.split_first() else {
    return RunResult {
      code: -1,
      ..RunResult::default()
    };
  };
  let mut command = Command::new(program);
  command
    .args(args)
    .stdin(Stdio::null())
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .kill_on_drop(true);
  if let Some(dir) = cwd {
    command.current_dir(dir);
  }
  let mut child = match command.spawn() {
    Ok(child) => child,
    Err(fault) => {
      return RunResult {
        code: -1,
        stderr: format!("{program}: {fault}"),
        attempts: 1,
        ..RunResult::default()
      };
    }
  };
  let stdout = child.stdout.take().expect("piped");
  let mut stderr = child.stderr.take().expect("piped");
  let work = async {
    let mut kept = String::new();
    let mut lines = BufReader::new(stdout).lines();
    let errors = tokio::spawn(async move {
      let mut said = String::new();
      let _ = stderr.read_to_string(&mut said).await;
      said
    });
    while let Ok(Some(line)) = lines.next_line().await {
      on_line(&line);
      kept.push_str(&line);
      kept.push('\n');
    }
    let status = child.wait().await;
    let said = errors.await.unwrap_or_default();
    (status, kept, said)
  };
  match tokio::time::timeout(limit, work).await {
    Ok((Ok(status), stdout, stderr)) => RunResult {
      code: status.code().unwrap_or(-1),
      stdout,
      stderr,
      timed_out: None,
      attempts: 1,
    },
    Ok((Err(fault), stdout, _)) => RunResult {
      code: -1,
      stdout,
      stderr: fault.to_string(),
      attempts: 1,
      ..RunResult::default()
    },
    Err(_) => RunResult {
      code: -1,
      timed_out: Some(limit.as_secs()),
      attempts: 1,
      ..RunResult::default()
    },
  }
}

/// A program run to its end or to its limit, killed where it outlives it.
pub async fn run(argv: &[String], cwd: Option<&Path>, limit: Duration) -> RunResult {
  let Some((program, args)) = argv.split_first() else {
    return RunResult {
      code: -1,
      ..RunResult::default()
    };
  };
  let mut command = Command::new(program);
  command
    .args(args)
    .stdin(Stdio::null())
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .kill_on_drop(true);
  if let Some(dir) = cwd {
    command.current_dir(dir);
  }
  let child = match command.spawn() {
    Ok(child) => child,
    Err(fault) => {
      return RunResult {
        code: -1,
        stderr: format!("{program}: {fault}"),
        attempts: 1,
        ..RunResult::default()
      };
    }
  };
  match tokio::time::timeout(limit, child.wait_with_output()).await {
    Ok(Ok(done)) => RunResult {
      code: done.status.code().unwrap_or(-1),
      stdout: String::from_utf8_lossy(&done.stdout).into_owned(),
      stderr: String::from_utf8_lossy(&done.stderr).into_owned(),
      timed_out: None,
      attempts: 1,
    },
    Ok(Err(fault)) => RunResult {
      code: -1,
      stderr: fault.to_string(),
      attempts: 1,
      ..RunResult::default()
    },
    // The child is dropped with the future and killed with it.
    Err(_) => RunResult {
      code: -1,
      timed_out: Some(limit.as_secs()),
      attempts: 1,
      ..RunResult::default()
    },
  }
}

/// Where the account's own folder is, from the environment.
pub fn home() -> PathBuf {
  std::env::var_os("HOME").map(PathBuf::from).unwrap_or_else(|| PathBuf::from("/"))
}

/// The `bun` the console script and an adapter whose first word is `bun` run on: the one `BMO_BUN`
/// names, the account's own install, else the one on the path.
pub fn bun() -> String {
  if let Some(named) = std::env::var_os("BMO_BUN").filter(|one| !one.is_empty()) {
    return named.to_string_lossy().into_owned();
  }
  let installed = home().join(".bun/bin/bun");
  if installed.exists() {
    installed.to_string_lossy().into_owned()
  } else {
    "bun".to_owned()
  }
}

/// The console script a node of the console kind is driven with, as `BMO_HUB_CONSOLE` names it; it has
/// no default, so a deployment that reaches such nodes names it.
pub fn hub_console() -> Result<String, String> {
  match std::env::var_os("BMO_HUB_CONSOLE").filter(|one| !one.is_empty()) {
    Some(named) => Ok(named.to_string_lossy().into_owned()),
    None => {
      Err("BMO_HUB_CONSOLE names no console script, so a node of the console kind is not reached".into())
    }
  }
}

/// A node's connection as the node registry gives it: never a credential, only where one is kept, with
/// the node's role — the services its row reserves it for, or none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Connection {
  pub way: Way,
  pub role: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Way {
  Ssh {
    host: String,
    port: u16,
    user: String,
    key_path: Option<String>,
    powershell: bool,
  },
  Hub {
    agent: String,
  },
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListedConnection {
  kind: String,
  host: Option<String>,
  port: Option<u16>,
  user: Option<String>,
  key_path: Option<String>,
  shell: Option<String>,
  agent: Option<String>,
  hub_url: Option<String>,
  token_file: Option<String>,
  role: Option<String>,
}

fn word(value: Option<String>) -> Option<String> {
  value.filter(|one| !one.is_empty())
}

/// The connection out of the registry command's answer — its last line — or none where it is not one.
pub fn parse_connection(text: &str) -> Option<Connection> {
  let said: ListedConnection = serde_json::from_str(text.trim().lines().last()?).ok()?;
  let role = word(said.role);
  let way = match said.kind.as_str() {
    "hub" => {
      word(said.hub_url)?;
      word(said.token_file)?;
      Way::Hub {
        agent: word(said.agent)?,
      }
    }
    "ssh" => Way::Ssh {
      host: word(said.host)?,
      port: said.port?,
      user: word(said.user)?,
      key_path: word(said.key_path),
      powershell: said.shell.as_deref() == Some("powershell"),
    },
    _ => return None,
  };
  Some(Connection { way, role })
}

/// The names the registry's listing holds, in its order: every line beginning `<name> [ssh]` or
/// `<name> [hub]`, the rest of each line and every other line being the registry's own.
pub fn parse_node_list(text: &str) -> Vec<String> {
  let line = Regex::new(r"^(\S+) \[(?:ssh|hub)\]").expect("pattern");
  text
    .lines()
    .filter_map(|one| Some(line.captures(one)?.get(1)?.as_str().to_owned()))
    .collect()
}

/// The setting naming the node registry's command.
pub const NODES_COMMAND: &str = "CARD_NODES_COMMAND";

/// The node registry, reached through its command (docs/api.md, 「The node registry」): the words
/// `CARD_NODES_COMMAND` names, run with the words of each question.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeRegistry {
  pub command: Vec<String>,
}

impl NodeRegistry {
  /// The registry `CARD_NODES_COMMAND` names, split by shell word rules, or why there is none.
  pub fn from_env() -> Result<Self, String> {
    let named = std::env::var(NODES_COMMAND).unwrap_or_default();
    Self::from_words(&named)
  }

  /// The registry out of a command line, split by shell word rules, or why it names none.
  pub fn from_words(line: &str) -> Result<Self, String> {
    match shlex::split(line) {
      Some(command) if !command.is_empty() => Ok(Self { command }),
      Some(_) => Err(format!("{NODES_COMMAND} names no node registry command")),
      None => Err(format!("{NODES_COMMAND} does not read as a command line")),
    }
  }

  /// The registry's command with the words of one question.
  pub fn argv(&self, rest: &[&str]) -> Vec<String> {
    let mut argv = self.command.clone();
    argv.extend(rest.iter().map(|one| (*one).to_owned()));
    argv
  }

  /// Every node the registry holds, by name.
  pub async fn listed_nodes(&self) -> Vec<String> {
    let listed = run(&self.argv(&[]), None, COMMAND_LIMIT).await;
    parse_node_list(&listed.stdout)
  }

  /// The node's connection as the registry has it now, whatever shell it logs into, or why it could not
  /// be had.
  pub async fn listed_connection(&self, name: &str) -> Result<Connection, String> {
    let asked = run(&self.argv(&["--target", name]), None, COMMAND_LIMIT).await;
    let connection = if asked.code == 0 { parse_connection(&asked.stdout) } else { None };
    let Some(connection) = connection else {
      let said = tail(
        if asked.stderr.trim().is_empty() { asked.stdout.trim() } else { asked.stderr.trim() },
        300,
      );
      return Err(if said.is_empty() {
        format!("the node registry gave no connection for {name}")
      } else {
        format!("the node registry gave no connection for {name}: {said}")
      });
    };
    Ok(connection)
  }

  /// The node's connection as the registry has it now, or why it could not be had or takes no bash.
  pub async fn read_connection(&self, name: &str) -> Result<Connection, String> {
    let connection = self.listed_connection(name).await?;
    if matches!(
      connection.way,
      Way::Ssh {
        powershell: true,
        ..
      }
    ) {
      return Err(format!("{name} logs into Windows PowerShell, so it takes no bash command"));
    }
    Ok(connection)
  }

  /// The node's role written in the registry, within `limit`: the services joined by `+`, or none.
  pub async fn set_role(&self, node: &str, role: Option<&str>, limit: Duration) -> RunResult {
    run(&self.argv(&["--set", node, "--role", role.unwrap_or("none")]), None, limit).await
  }
}

/// A node's shell: a command run there, a file carried there.
#[derive(Debug, Clone)]
pub struct NodeShell {
  pub connection: Connection,
}

impl NodeShell {
  fn ssh_options(&self, port_flag: &str) -> Vec<String> {
    let Way::Ssh { port, key_path, .. } = &self.connection.way else { return Vec::new() };
    let mut options: Vec<String> = [
      "-o",
      "BatchMode=yes",
      "-o",
      "ConnectTimeout=10",
      "-o",
      "StrictHostKeyChecking=accept-new",
    ]
    .iter()
    .map(|one| (*one).to_owned())
    .collect();
    options.extend([port_flag.to_owned(), port.to_string()]);
    if let Some(key) = key_path {
      let expanded = match key.strip_prefix("~/") {
        Some(rest) => home().join(rest).to_string_lossy().into_owned(),
        None if key == "~" => home().to_string_lossy().into_owned(),
        None => key.clone(),
      };
      options.extend(["-i".to_owned(), expanded, "-o".to_owned(), "IdentitiesOnly=yes".to_owned()]);
    }
    options
  }

  /// The argv a command runs by on the node, handed to bash whatever shell the account logs into, or why
  /// the node cannot be reached.
  pub fn exec_argv(&self, command: &str) -> Result<Vec<String>, String> {
    Ok(match &self.connection.way {
      Way::Hub { agent } => {
        vec![bun(), hub_console()?, "exec".into(), "-a".into(), agent.clone(), command.to_owned()]
      }
      Way::Ssh { host, user, .. } => {
        let mut argv = vec!["ssh".to_owned()];
        argv.extend(self.ssh_options("-p"));
        argv.push(format!("{user}@{host}"));
        argv.push(format!("bash -c '{}'", command.replace('\'', "'\\''")));
        argv
      }
    })
  }

  /// The argv a file is carried to the node by, or why the node cannot be reached.
  pub fn upload_argv(&self, local: &Path, remote: &str) -> Result<Vec<String>, String> {
    let local = local.to_string_lossy().into_owned();
    Ok(match &self.connection.way {
      Way::Hub { agent } => vec![
        bun(),
        hub_console()?,
        "upload".into(),
        local,
        remote.to_owned(),
        "-a".into(),
        agent.clone(),
      ],
      Way::Ssh { host, user, .. } => {
        let mut argv = vec!["scp".to_owned(), "-q".to_owned()];
        argv.extend(self.ssh_options("-P"));
        argv.extend([local, format!("{user}@{host}:{remote}")]);
        argv
      }
    })
  }

  pub async fn exec(&self, command: &str) -> RunResult {
    match self.exec_argv(command) {
      Ok(argv) => run(&argv, None, COMMAND_LIMIT).await,
      Err(why) => RunResult::refused(why),
    }
  }

  /// A file carried to the node, made once more where the first try failed, since what breaks one is most
  /// often the tunnel of the moment.
  pub async fn upload(&self, local: &Path, remote: &str) -> RunResult {
    let bytes = std::fs::metadata(local).map_or(0, |meta| meta.len());
    let limit = TRANSFER_OPEN + Duration::from_millis(bytes * 1000 / TRANSFER_BYTES_PER_SECOND);
    let argv = match self.upload_argv(local, remote) {
      Ok(argv) => argv,
      Err(why) => return RunResult::refused(why),
    };
    let first = run(&argv, None, limit).await;
    if first.ok() {
      return first;
    }
    RunResult {
      attempts: 2,
      ..run(&argv, None, limit).await
    }
  }
}

/// The two calls an install makes on one node: a command run there in bash, and a file carried there.
/// A node's shell is one; a test stands in another.
pub trait NodeCalls: Send + Sync {
  fn exec<'a>(&'a self, command: &'a str) -> BoxFuture<'a, RunResult>;
  fn upload<'a>(&'a self, local: &'a Path, remote: &'a str) -> BoxFuture<'a, RunResult>;
}

impl NodeCalls for NodeShell {
  fn exec<'a>(&'a self, command: &'a str) -> BoxFuture<'a, RunResult> {
    Box::pin(NodeShell::exec(self, command))
  }

  fn upload<'a>(&'a self, local: &'a Path, remote: &'a str) -> BoxFuture<'a, RunResult> {
    Box::pin(NodeShell::upload(self, local, remote))
  }
}
