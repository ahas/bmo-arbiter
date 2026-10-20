//! How the master reaches a node by its registered name (docs/api.md, 「The node service」, 「Node
//! shells」). What a node is reached by belongs to the node service, so its target is asked through the node
//! service's client at every run; a hub node is driven through the console script by its agent's name, an ssh
//! node by the batch login its target lays out, and either runs a command in bash.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::Stdio;
use std::time::Duration;

use bmo_node_client::NodeClient;
use bmo_node_client::nodes::{LoginShell, NodeChanges, NodeKind, NodesQuery, Target};
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

/// A path written `~/…` under the account's own folder; any other as it is written.
pub fn expand_home(path: &str) -> String {
  match path.strip_prefix("~/") {
    Some(rest) => home().join(rest).to_string_lossy().into_owned(),
    None if path == "~" => home().to_string_lossy().into_owned(),
    None => path.to_owned(),
  }
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

/// The console script a hub node is driven with, as `BMO_HUB_CONSOLE` names it; it has no default, so a
/// deployment that reaches such nodes names it.
pub fn hub_console() -> Result<String, String> {
  match std::env::var_os("BMO_HUB_CONSOLE").filter(|one| !one.is_empty()) {
    Some(named) => Ok(named.to_string_lossy().into_owned()),
    None => Err("BMO_HUB_CONSOLE names no console script, so a node of the hub kind is not reached".into()),
  }
}

/// A node's connection as the node service gives it: never a credential, only where one is kept, with the
/// node's role — the services its row reserves it for, or none.
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
    /// The batch login laid out for `ssh` ahead of the command, as the node service gives it.
    ssh_args: Vec<String>,
    /// The batch login's OpenSSH options, each given after `-o`.
    ssh_options: Vec<String>,
    powershell: bool,
  },
  Hub {
    agent: String,
  },
}

fn word(value: Option<&String>) -> Option<String> {
  value.filter(|one| !one.is_empty()).cloned()
}

/// The connection out of a node's target, or why the target does not make one.
pub fn connection_of(target: &Target) -> Result<Connection, String> {
  let name = &target.node_name;
  let lacks = |field: &str| format!("the node service's target for {name} gives no {field}");
  let way = match target.kind {
    NodeKind::Hub => Way::Hub {
      agent: word(target.agent_name.as_ref()).ok_or_else(|| lacks("agentName"))?,
    },
    NodeKind::Ssh => Way::Ssh {
      host: word(target.host.as_ref()).ok_or_else(|| lacks("host"))?,
      port: target.port.ok_or_else(|| lacks("port"))?,
      user: word(target.user.as_ref()).ok_or_else(|| lacks("user"))?,
      key_path: word(target.key_path.as_ref()),
      ssh_args: target.ssh_args.clone(),
      ssh_options: target.ssh_options.clone(),
      powershell: target.shell == Some(LoginShell::Powershell),
    },
  };
  Ok(Connection {
    way,
    role: word(target.role.as_ref()),
  })
}

/// The settings naming the node service's base address and its token.
pub const NODE_URL: &str = "CARD_NODE_URL";
pub const NODE_TOKEN: &str = "CARD_NODE_TOKEN";

// How long one call of the node service may take.
const NODE_CALL_LIMIT: Duration = Duration::from_secs(30);

/// The node service, reached through its published client (docs/api.md, 「The node service」).
#[derive(Clone)]
pub struct NodeRegistry {
  client: NodeClient,
}

impl std::fmt::Debug for NodeRegistry {
  fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    formatter.write_str("NodeRegistry")
  }
}

impl NodeRegistry {
  /// The node service `CARD_NODE_URL` names, asked with `CARD_NODE_TOKEN`, or why there is none.
  pub fn from_env() -> Result<Self, String> {
    let url = std::env::var(NODE_URL).unwrap_or_default();
    if url.trim().is_empty() {
      return Err(format!("{NODE_URL} names no node service"));
    }
    Self::at(url.trim(), &std::env::var(NODE_TOKEN).unwrap_or_default())
  }

  /// The node service at `url`, asked with `token`.
  pub fn at(url: &str, token: &str) -> Result<Self, String> {
    let http = reqwest::Client::builder()
      .timeout(NODE_CALL_LIMIT)
      .build()
      .map_err(|fault| format!("no HTTP client could be made: {fault}"))?;
    let client = NodeClient::with_http(url, token, http).map_err(|why| format!("{NODE_URL}: {why}"))?;
    Ok(Self { client })
  }

  /// Every node the node service holds, by name, in its list's order; none where it cannot be read.
  pub async fn listed_nodes(&self) -> Vec<String> {
    let mut names = Vec::new();
    let mut query = NodesQuery::default();
    loop {
      let Ok(page) = self.client.nodes(&query).await else { return names };
      let last = page.items.last().map(|node| node.name.clone());
      names.extend(page.items.into_iter().map(|node| node.name));
      match last {
        Some(last) if page.more => query.after = Some(last),
        _ => return names,
      }
    }
  }

  /// The node's connection as the node service has it now, whatever shell it logs into, or why it could not
  /// be had.
  pub async fn listed_connection(&self, name: &str) -> Result<Connection, String> {
    let target = self
      .client
      .target(name)
      .await
      .map_err(|fault| format!("the node service gave no connection for {name}: {fault}"))?;
    connection_of(&target)
  }

  /// The node's connection as the node service has it now, or why it could not be had or takes no bash.
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

  /// The node's role written in the node service: the services joined by `+`, or none.
  pub async fn set_role(&self, node: &str, role: Option<&str>) -> Result<(), String> {
    let changes = NodeChanges {
      role: Some(role.map(str::to_owned)),
      ..NodeChanges::default()
    };
    self
      .client
      .change_node(node, &changes)
      .await
      .map(|_| ())
      .map_err(|fault| format!("the node service did not take {node}'s role: {fault}"))
  }
}

/// A node's shell: a command run there, a file carried there.
#[derive(Debug, Clone)]
pub struct NodeShell {
  pub connection: Connection,
}

impl NodeShell {
  /// The argv a command runs by on the node, handed to bash whatever shell the account logs into, or why
  /// the node cannot be reached.
  pub fn exec_argv(&self, command: &str) -> Result<Vec<String>, String> {
    Ok(match &self.connection.way {
      Way::Hub { agent } => {
        vec![bun(), hub_console()?, "exec".into(), "-a".into(), agent.clone(), command.to_owned()]
      }
      Way::Ssh { ssh_args, .. } => {
        let mut argv = vec!["ssh".to_owned()];
        argv.extend(ssh_args.iter().cloned());
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
      Way::Ssh {
        host,
        port,
        user,
        key_path,
        ssh_options,
        ..
      } => {
        let mut argv = vec!["scp".to_owned(), "-q".to_owned()];
        for option in ssh_options {
          argv.extend(["-o".to_owned(), option.clone()]);
        }
        if let Some(key) = key_path {
          argv.extend(["-i".to_owned(), expand_home(key)]);
        }
        argv.extend(["-P".to_owned(), port.to_string(), local, format!("{user}@{host}:{remote}")]);
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
