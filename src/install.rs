//! Putting the node arbiter on a node (docs/node-arbiter.md, 「Installing」): its folder, its settings read
//! from the node's machine, its files from this repository's `card/`, its venv and its unit. The master runs
//! it as the first step of every placement and writes the node's occupants and its part of the rules
//! itself; no service's install runs it, and the arbiter knows none of them. A service that borrows the
//! card over the network writes its lend settings with the lend step here (「Lending the card」), and
//! nothing else does; taking the arbiter off a node and reading a node's occupants stand here too.

use std::path::{Path, PathBuf};
use std::time::Duration;

use regex::Regex;
use serde_json::Value;

use crate::machine::{Machine, probe_machine};
use crate::reach::{NodeCalls, RunResult};

/// The arbiter's folder in the node account's home.
pub const CARD_DIR: &str = "bmo-card";
/// The arbiter's settings folder in the node account's home.
pub const CARD_CONFIG: &str = ".config/bmo-card";
/// The arbiter's user unit.
pub const CARD_UNIT: &str = "bmo-card.service";
/// The client an occupant in a process of its own waits its turn for the card through, installed beside
/// the arbiter (docs/api.md, 「The turn client」).
pub const TURN_CLIENT: &str = "turn.py";
/// What goes across from `card/`; the tests stay in this repository.
pub const CARD_FILES: [&str; 7] = [
  "card.py",
  "desktop.py",
  "units.py",
  TURN_CLIENT,
  "pyproject.toml",
  "node-setup.sh",
  CARD_UNIT,
];

/// The node's dump store, the one file the arbiter unloads units to, in its own data folder on the node's
/// own disk, and its table of slots beside it (docs/placement.md, 「The dump store」).
pub const STORE_DIR: &str = ".local/share/bmo-card/store";
pub const STORE_FILE: &str = ".local/share/bmo-card/store/units.store";

/// The node's occupants file, which the arbiter reads again whenever it changes and the master writes, and
/// the history of its changes beside it (docs/node-arbiter.md, 「The occupants」).
pub const OCCUPANTS_FILE: &str = ".config/bmo-card/occupants";
pub const OCCUPANTS_HISTORY: &str = ".config/bmo-card/occupants.log";

/// The lend settings on the node, which the arbiter's unit reads as it starts.
pub const LEND_FILE: &str = ".config/bmo-card/lend.env";

/// The setting naming the rival units (docs/api.md, 「Settings」).
pub const RIVAL_UNITS: &str = "CARD_RIVAL_UNITS";

/// The user units that keep a card lock by rules of their own, as `CARD_RIVAL_UNITS` names them,
/// separated by spaces: an install refuses a node where one is active, and the lend step replaces them.
pub fn rival_units_from_env() -> Vec<String> {
  std::env::var(RIVAL_UNITS)
    .unwrap_or_default()
    .split_whitespace()
    .map(str::to_owned)
    .collect()
}

const RUNTIME: &str = "export XDG_RUNTIME_DIR=${XDG_RUNTIME_DIR:-/run/user/$(id -u)}";

/// How long the install waits for the steps it watches: the setup, polled until it says it ended, and the
/// unit, read until its main process stands and then once more after a while.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timing {
  pub setup_polls: u32,
  pub setup_every: Duration,
  pub running_polls: u32,
  pub running_every: Duration,
  pub running_settle: Duration,
}

/// The setting that sets every wait of the install alike (docs/api.md, 「Settings」).
pub const INSTALL_POLL: &str = "CARD_INSTALL_POLL_MS";

impl Timing {
  /// As the environment sets it: `CARD_INSTALL_POLL_MS`, where it names a number of milliseconds, sets the
  /// setup's polls, the unit's polls and the wait before its main process is read again alike, which a
  /// check does to run an install quickly; else the defaults.
  pub fn from_env() -> Self {
    let named = std::env::var(INSTALL_POLL).ok().and_then(|value| value.trim().parse::<u64>().ok());
    match named {
      Some(ms) => {
        let every = Duration::from_millis(ms);
        Self {
          setup_every: every,
          running_every: every,
          running_settle: every,
          ..Self::default()
        }
      }
      None => Self::default(),
    }
  }
}

impl Default for Timing {
  fn default() -> Self {
    Self {
      setup_polls: 80,
      setup_every: Duration::from_secs(5),
      running_polls: 10,
      running_every: Duration::from_secs(1),
      running_settle: Duration::from_secs(5),
    }
  }
}

/// The store's size: as asked — the master asks the size the node's group fixes — else the card's memory
/// once over, room for everything one card can unload at once.
pub fn store_bytes(machine: &Machine, gib: Option<f64>) -> u64 {
  match gib {
    Some(gib) => (gib * f64::from(1u32 << 30)).round() as u64,
    None => machine.card.as_ref().map_or(0, |card| card.total_mib << 20),
  }
}

/// The arbiter's own account of its units, which says what lies out in the store.
pub fn units_ask_command() -> String {
  "curl -s -m 3 http://127.0.0.1:7439/units || true".to_owned()
}

/// The units out in the store, wholly or in part, out of the units' answer; none where no arbiter
/// answered.
pub fn held_units(text: &str) -> Vec<String> {
  let Ok(answer) = serde_json::from_str::<Value>(text) else { return Vec::new() };
  let Some(units) = answer.get("units").and_then(Value::as_array) else {
    return Vec::new();
  };
  units
    .iter()
    .filter(|unit| matches!(unit.get("state").and_then(Value::as_str), Some("evicted" | "mixed")))
    .map(|unit| unit.get("name").and_then(Value::as_str).unwrap_or("?").to_owned())
    .collect()
}

/// The store made at its size in one allocation where it is missing or another size, its table dropped
/// with it, unless units are held in it, which answers `STORE-HOLDS-UNITS` and leaves it; refused on a disk
/// that spins. A WSL machine's virtual disk says it spins whatever it stands on, so there the check is the
/// Windows side's to make and is not made. It answers the extent count filefrag reads, `STORE <extents>`.
pub fn store_command(bytes: u64, wsl: bool, held: bool) -> String {
  let mut command =
    format!("d=~/{STORE_DIR}; f=~/{STORE_FILE}; mkdir -p \"$d\" && chmod 700 \"$d\" || exit 2; ");
  if !wsl {
    command.push_str(
      "src=$(df --output=source \"$d\" | tail -n 1); rota=$(lsblk -no ROTA \"$src\" 2>/dev/null | head -n 1 | tr -d \" \"); \
       if [ \"$rota\" = 1 ]; then echo \"STORE-ON-SPINNING-DISK $src\"; exit 4; fi; ",
    );
  }
  command.push_str(&format!("if [ ! -f \"$f\" ] || [ \"$(stat -c %s \"$f\")\" != {bytes} ]; then "));
  if held {
    command
      .push_str("if [ -f \"$f\" ]; then echo \"STORE-HOLDS-UNITS $(stat -c %s \"$f\")\"; exit 5; fi; ");
  }
  command.push_str(&format!("rm -f \"$f\" \"$d/slots.json\"; fallocate -l {bytes} \"$f\" || exit 3; fi; "));
  command
    .push_str("echo \"STORE $(filefrag \"$f\" | sed -n \"s/.*: \\([0-9]*\\) extents\\? found.*/\\1/p\")\"");
  command
}

/// The extents the store lies in, out of the store command's answer, or none.
pub fn parse_store(text: &str) -> Option<u64> {
  let line = Regex::new(r"(?m)^STORE (\d+)$").expect("pattern");
  line.captures(text)?.get(1)?.as_str().parse().ok()
}

/// The version the node arbiter shows (docs/api.md, 「Versioning」): this repository's `VERSION`, then the
/// commit the tree stands at, marked where the arbiter's files differ from it.
pub fn version(code_root: &Path) -> String {
  let git = |args: &[&str]| {
    std::process::Command::new("git")
      .arg("-C")
      .arg(code_root)
      .args(args)
      .stdin(std::process::Stdio::null())
      .output()
      .ok()
      .filter(|done| done.status.success())
      .map(|done| String::from_utf8_lossy(&done.stdout).trim().to_owned())
  };
  let release = std::fs::read_to_string(code_root.join("VERSION"))
    .map(|text| text.trim().to_owned())
    .ok()
    .filter(|text| !text.is_empty())
    .unwrap_or_else(|| "unknown".to_owned());
  let commit = git(&["rev-parse", "--short=12", "HEAD"]).filter(|commit| !commit.is_empty());
  let Some(commit) = commit else { return format!("{release}+unknown") };
  match git(&["status", "--porcelain", "--", "card"]) {
    Some(changes) if changes.is_empty() => format!("{release}+{commit}"),
    _ => format!("{release}+{commit}.changes"),
  }
}

/// The lock every service on the node takes the card by, in the node account's own home.
pub fn lock_path(home: &str) -> String {
  format!("{home}/.gpu.lock")
}

/// The arbiter's own settings: the lock, and the desktop rule on a WSL machine.
pub fn card_settings(home: &str, machine: &Machine) -> String {
  let mut settings = format!("CARD_LOCK={}\n", lock_path(home));
  if machine.wsl {
    settings.push_str("CARD_DESKTOP=1\n");
  }
  settings
}

/// What the node answers its home with.
pub fn home_command() -> String {
  "echo HOME=$HOME".to_owned()
}

/// The home out of the node's answer, or none.
pub fn parse_home(text: &str) -> Option<String> {
  let said = Regex::new(r"(?:^|\n)HOME=(/\S+)").expect("pattern");
  Some(said.captures(text)?.get(1)?.as_str().to_owned())
}

pub fn card_dirs_command() -> String {
  format!("mkdir -p ~/{CARD_DIR} ~/{CARD_CONFIG} && chmod 700 ~/{CARD_CONFIG}")
}

/// Whether a unit runs here: `active` and nothing else on its first line where it does.
pub fn active_command(unit: &str) -> String {
  format!("{RUNTIME}; systemctl --user is-active {unit} 2>/dev/null || true")
}

/// The unit's main process, read twice a few seconds apart: the same number both times is a unit that
/// stays up rather than one restarting over and over.
pub fn main_pid_command() -> String {
  format!("{RUNTIME}; systemctl --user show {CARD_UNIT} -p MainPID --value")
}

/// The setup, started and let go of, so a hub that stops a long command it streams never stops it; it is
/// read back out of its log.
pub fn setup_launch_command() -> String {
  format!(
    "cd ~/{CARD_DIR} && chmod +x node-setup.sh && rm -f setup.log && \
     setsid nohup bash node-setup.sh > setup.log 2>&1 < /dev/null & echo launched"
  )
}

pub fn setup_tail_command() -> String {
  format!("tail -n 40 ~/{CARD_DIR}/setup.log 2>/dev/null || true")
}

/// How the setup ended, read off its log's last `CARD-SETUP-DONE` line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetupEnd {
  Running,
  Ok,
  Failed(String),
}

pub fn parse_setup_end(text: &str) -> SetupEnd {
  const MARKER: &str = "CARD-SETUP-DONE";
  let Some(line) = text.lines().map(str::trim).rfind(|line| line.starts_with(MARKER)) else {
    return SetupEnd::Running;
  };
  let said = line[MARKER.len()..].trim();
  if said == "ok" {
    return SetupEnd::Ok;
  }
  SetupEnd::Failed(said.strip_prefix("failed:").unwrap_or(said).to_owned())
}

/// The unit put where the user manager reads it, read again, and started — restarted where it already
/// ran, so an update takes effect.
pub fn units_command() -> String {
  format!(
    "{RUNTIME} && mkdir -p ~/.config/systemd/user && cp ~/{CARD_DIR}/{CARD_UNIT} ~/.config/systemd/user/{CARD_UNIT} \
     && systemctl --user daemon-reload && systemctl --user enable {CARD_UNIT} && systemctl --user restart {CARD_UNIT}"
  )
}

/// A text carried to the node into a private file, readable by the node account alone.
async fn carry_private(calls: &dyn NodeCalls, text: &str, remote: &str) -> Result<(), String> {
  use std::os::unix::fs::PermissionsExt;

  let scratch = tempfile::Builder::new()
    .prefix(&format!("{CARD_DIR}-env-"))
    .tempdir()
    .map_err(|fault| fault.to_string())?;
  let local = scratch.path().join("env");
  std::fs::write(&local, text).map_err(|fault| fault.to_string())?;
  std::fs::set_permissions(&local, std::fs::Permissions::from_mode(0o600))
    .map_err(|fault| fault.to_string())?;
  let carried = calls.upload(&local, remote).await;
  if carried.code != 0 {
    return Err(carried.fault());
  }
  let moded = calls.exec(&format!("chmod 600 {remote}")).await;
  if moded.code == 0 { Ok(()) } else { Err(moded.fault()) }
}

/// The steps of an install, in order, each with the word an outcome names it by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
  Home,
  Rival,
  Machine,
  Env,
  Upload,
  Store,
  Setup,
  Units,
  Running,
}

impl Step {
  pub fn word(self) -> &'static str {
    match self {
      Step::Home => "home",
      Step::Rival => "rival",
      Step::Machine => "machine",
      Step::Env => "env",
      Step::Upload => "upload",
      Step::Store => "store",
      Step::Setup => "setup",
      Step::Units => "units",
      Step::Running => "running",
    }
  }
}

/// How an install ended: the step it stopped at and why, the steps done, and the machine as it read it
/// where it got that far.
#[derive(Debug, Clone, PartialEq)]
pub struct Outcome {
  pub ok: bool,
  pub step: Option<Step>,
  pub fault: Option<String>,
  pub done: Vec<Step>,
  pub machine: Option<Machine>,
}

/// What an install is handed beside the node.
#[derive(Debug, Clone)]
pub struct InstallOptions {
  /// The services the node's row reserves it for, or none.
  pub role: Option<String>,
  /// The arbiter's own tree, whose `card/` is carried and whose version is written.
  pub code_root: PathBuf,
  /// The dump store's size in GiB, as the node's group fixes it; the card's memory where not given.
  pub store_gib: Option<f64>,
  /// The rival units a node is refused for while one is active.
  pub rival_units: Vec<String>,
  pub timing: Timing,
}

/// The whole install, one step after another, stopping at the first that fails and saying which.
pub async fn install(
  node: &str,
  calls: &dyn NodeCalls,
  options: &InstallOptions,
  say: &(dyn Fn(String) + Send + Sync),
) -> Outcome {
  let mut done: Vec<Step> = Vec::new();
  let mut machine: Option<Machine> = None;
  macro_rules! stop {
    ($step:expr, $fault:expr) => {
      return Outcome {
        ok: false,
        step: Some($step),
        fault: Some($fault),
        done,
        machine,
      }
    };
  }

  let asked = calls.exec(&home_command()).await;
  let Some(home) = parse_home(&asked.stdout) else {
    stop!(Step::Home, format!("{node} did not answer: {}", asked.fault()));
  };
  let made = calls.exec(&card_dirs_command()).await;
  if made.code != 0 {
    stop!(Step::Home, format!("the folders were not made: {}", made.fault()));
  }
  done.push(Step::Home);

  for unit in &options.rival_units {
    let active = calls.exec(&active_command(unit)).await;
    if active.stdout.trim().lines().next() == Some("active") {
      stop!(
        Step::Rival,
        format!(
          "{unit} still keeps the card lock on {node}; the lend step of the service that borrows the card replaces it"
        )
      );
    }
  }
  done.push(Step::Rival);

  let probed = match probe_machine(calls).await {
    Ok(probed) => probed,
    Err(fault) => stop!(Step::Machine, fault),
  };
  let Some(card) = probed.card.clone() else {
    stop!(Step::Machine, format!("{node} shows no card"));
  };
  machine = Some(probed.clone());
  say(format!(
    "{node}: {} {} MiB{}{}",
    card.name,
    card.total_mib,
    if probed.wsl { ", a WSL desktop" } else { "" },
    options.role.as_deref().map(|role| format!(", reserved for {role}")).unwrap_or_default()
  ));
  done.push(Step::Machine);

  if let Err(fault) =
    carry_private(calls, &card_settings(&home, &probed), &format!("{home}/{CARD_CONFIG}/env")).await
  {
    stop!(Step::Env, format!("the settings file was not carried: {fault}"));
  }
  done.push(Step::Env);

  for name in CARD_FILES {
    let local = options.code_root.join("card").join(name);
    let carried = calls.upload(&local, &format!("{home}/{CARD_DIR}/{name}")).await;
    if carried.code != 0 {
      stop!(Step::Upload, format!("{name} was not carried: {}", carried.fault()));
    }
  }
  let release = format!("{}\n", version(&options.code_root));
  if let Err(fault) = carry_private(calls, &release, &format!("{home}/{CARD_DIR}/VERSION")).await {
    stop!(Step::Upload, format!("VERSION was not carried: {fault}"));
  }
  done.push(Step::Upload);

  let size = store_bytes(&probed, options.store_gib);
  let held = held_units(&calls.exec(&units_ask_command()).await.stdout);
  let stored = calls.exec(&store_command(size, probed.wsl, !held.is_empty())).await;
  let extents = parse_store(&stored.stdout);
  let Some(extents) = extents.filter(|_| stored.code == 0) else {
    let fault = if stored.stdout.contains("STORE-ON-SPINNING-DISK") {
      format!("the dump store would stand on a spinning disk: {}", stored.stdout.trim())
    } else if stored.stdout.contains("STORE-HOLDS-UNITS") {
      format!(
        "the dump store is another size and has to be made again, but units are out in it: {}; restore them and run again",
        held.join(", ")
      )
    } else {
      format!("the dump store was not made: {}", stored.fault())
    };
    stop!(Step::Store, fault);
  };
  say(format!(
    "{node}: dump store {} GiB in {extents} extents",
    (size as f64 / f64::from(1u32 << 30)).round()
  ));
  done.push(Step::Store);

  say(format!("{node}: standing up the arbiter's environment"));
  let launched = calls.exec(&setup_launch_command()).await;
  if launched.code != 0 {
    stop!(Step::Setup, format!("the setup did not start: {}", launched.fault()));
  }
  let mut end = SetupEnd::Running;
  for _ in 0..options.timing.setup_polls {
    tokio::time::sleep(options.timing.setup_every).await;
    end = parse_setup_end(&calls.exec(&setup_tail_command()).await.stdout);
    if end != SetupEnd::Running {
      break;
    }
  }
  match end {
    SetupEnd::Ok => {}
    SetupEnd::Failed(step) => {
      stop!(Step::Setup, format!("the setup stopped at {step}; see ~/{CARD_DIR}/setup.log"));
    }
    SetupEnd::Running => stop!(Step::Setup, format!("the setup did not end; see ~/{CARD_DIR}/setup.log")),
  }
  done.push(Step::Setup);

  let started = calls.exec(&units_command()).await;
  if started.code != 0 {
    stop!(Step::Units, format!("the unit was not started: {}", started.fault()));
  }
  done.push(Step::Units);

  if !stands_running(calls, &options.timing).await {
    stop!(
      Step::Running,
      format!("{CARD_UNIT} does not stand; see journalctl --user -u {CARD_UNIT} on {node}")
    );
  }
  done.push(Step::Running);

  Outcome {
    ok: true,
    step: None,
    fault: None,
    done,
    machine,
  }
}

/// Whether the unit's main process stays the same across a few seconds, which a unit restarting over and
/// over does not.
async fn stands_running(calls: &dyn NodeCalls, timing: &Timing) -> bool {
  let pid = || async { calls.exec(&main_pid_command()).await.stdout.trim().to_owned() };
  let standing = |pid: &str| !pid.is_empty() && pid != "0";
  let mut first = String::new();
  for _ in 0..timing.running_polls {
    tokio::time::sleep(timing.running_every).await;
    first = pid().await;
    if standing(&first) {
      break;
    }
  }
  tokio::time::sleep(timing.running_settle).await;
  standing(&first) && pid().await == first
}

/// Whether the arbiter's unit stands installed on the node.
pub fn installed_command() -> String {
  format!("test -f ~/.config/systemd/user/{CARD_UNIT} && echo CARD-INSTALLED || true")
}

/// The arbiter restarted with the lend settings it reads as it starts, every rival unit gone where it
/// stood.
pub fn relend_command(rival_units: &[String]) -> String {
  let mut command = format!("{RUNTIME}; ");
  if !rival_units.is_empty() {
    let units = rival_units.join(" ");
    let files: Vec<String> =
      rival_units.iter().map(|unit| format!("~/.config/systemd/user/{unit}")).collect();
    command.push_str(&format!(
      "{{ systemctl --user disable --now {units} 2>/dev/null; rm -f {}; systemctl --user daemon-reload; }} && ",
      files.join(" ")
    ));
  }
  command.push_str(&format!("systemctl --user restart {CARD_UNIT}"));
  command
}

/// The lend settings written on a node whose arbiter the master stood, or taken off it where none are
/// given, every rival unit removed and the arbiter restarted to take that up (docs/node-arbiter.md,
/// 「Lending the card」); `{home}` in the settings stands for the node account's home. Nothing once it
/// stands, else what went wrong.
pub async fn lend(
  node: &str,
  calls: &dyn NodeCalls,
  settings: Option<&str>,
  rival_units: &[String],
  timing: &Timing,
  say: &(dyn Fn(String) + Send + Sync),
) -> Result<(), String> {
  let installed = calls.exec(&installed_command()).await;
  if !installed.stdout.contains("CARD-INSTALLED") {
    return Err(format!(
      "{node} has no arbiter; card-master place puts a service on the node, which stands the arbiter first"
    ));
  }
  let Some(home) = parse_home(&calls.exec(&home_command()).await.stdout) else {
    return Err(format!("{node} did not answer"));
  };
  match settings {
    None => {
      let taken = calls.exec(&format!("rm -f ~/{LEND_FILE}")).await;
      if taken.code != 0 {
        return Err(format!("the lend settings were not taken off: {}", taken.fault()));
      }
    }
    Some(settings) => {
      carry_private(calls, &settings.replace("{home}", &home), &format!("{home}/{LEND_FILE}"))
        .await
        .map_err(|fault| format!("the lend settings were not carried: {fault}"))?;
    }
  }
  let restarted = calls.exec(&relend_command(rival_units)).await;
  if restarted.code != 0 {
    return Err(format!("the arbiter was not restarted: {}", restarted.fault()));
  }
  say(format!(
    "{node}: the arbiter {}",
    if settings.is_none() {
      "lends the card no more"
    } else {
      "stands again with the new lend settings"
    }
  ));
  if stands_running(calls, timing).await {
    Ok(())
  } else {
    Err(format!("{CARD_UNIT} does not stand; see journalctl --user -u {CARD_UNIT} on {node}"))
  }
}

/// The arbiter taken off a node: its unit stopped, disabled and removed, and its dump store and table with
/// it unless they are kept. Its folder, settings and occupants stay with the node. `REMOVED` once done.
pub fn remove_command(keep_store: bool) -> String {
  format!(
    "{RUNTIME}; systemctl --user disable --now {CARD_UNIT} 2>/dev/null; rm -f ~/.config/systemd/user/{CARD_UNIT}; \
     systemctl --user daemon-reload; {}echo REMOVED",
    if keep_store { String::new() } else { format!("rm -rf ~/{STORE_DIR}; ") }
  )
}

/// The arbiter removed from a node; nothing once it is, else what went wrong.
pub async fn uninstall(calls: &dyn NodeCalls, keep_store: bool) -> Result<(), String> {
  let ran: RunResult = calls.exec(&remove_command(keep_store)).await;
  if ran.code == 0 && ran.stdout.contains("REMOVED") { Ok(()) } else { Err(ran.fault()) }
}

/// A node's occupants order as its file stands, and the last changes its arbiter took up.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OccupantsView {
  pub order: String,
  pub history: Vec<String>,
}

/// The node's order and the last `lines` changes its arbiter took up (docs/node-arbiter.md, 「The
/// occupants」); nothing here writes the order.
pub async fn read_occupants(calls: &dyn NodeCalls, lines: usize) -> Result<OccupantsView, String> {
  let read = calls
    .exec(&format!(
      "cat ~/{OCCUPANTS_FILE} 2>/dev/null; echo \"--history--\"; tail -n {lines} ~/{OCCUPANTS_HISTORY} 2>/dev/null || true"
    ))
    .await;
  if read.code != 0 {
    return Err(format!("the occupants file was not read: {}", read.fault()));
  }
  let (order, history) = read.stdout.split_once("--history--\n").unwrap_or((read.stdout.as_str(), ""));
  Ok(OccupantsView {
    order: order.trim().to_owned(),
    history: history.lines().filter(|line| !line.trim().is_empty()).map(str::to_owned).collect(),
  })
}
