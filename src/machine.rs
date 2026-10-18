//! What a node's machine is, read from the machine itself at install (docs/node-arbiter.md, 「What the
//! install reads and writes」): whether it is a WSL machine, which is a person's Windows desktop whose card
//! and memory come first, and its card's name and size. Nothing of it is written into this repository; an
//! install asks it afresh every time.

use crate::reach::NodeCalls;

/// The first card of a machine, as its driver names and sizes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Card {
  pub name: String,
  pub total_mib: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Machine {
  /// A WSL machine: a Windows desktop, its card the desktop's first.
  pub wsl: bool,
  /// The first card, or none where the driver answers for none.
  pub card: Option<Card>,
}

/// One line a value, each asked of the driver alone so no answer has to be split. WSL keeps the driver's
/// tools in a folder of its own.
pub fn probe_command() -> String {
  let smi = "nvidia-smi -i 0 --format=csv,noheader";
  format!(
    "export PATH=$PATH:/usr/lib/wsl/lib; \
     if grep -qi microsoft /proc/version; then echo MACHINE_WSL=1; else echo MACHINE_WSL=0; fi; \
     echo \"MACHINE_CARD_MIB=$({smi},nounits --query-gpu=memory.total 2>/dev/null | head -n 1)\"; \
     echo \"MACHINE_CARD_NAME=$({smi} --query-gpu=name 2>/dev/null | head -n 1)\""
  )
}

// The value the probe gave `name`, out of its `NAME=value` line.
fn said<'a>(text: &'a str, name: &str) -> Option<&'a str> {
  text
    .lines()
    .map(str::trim)
    .find_map(|line| line.strip_prefix(name)?.strip_prefix('='))
    .map(str::trim)
}

/// The machine out of the probe's answer, or none where the answer is not the probe's.
pub fn parse_machine(text: &str) -> Option<Machine> {
  let wsl = match said(text, "MACHINE_WSL")? {
    "1" => true,
    "0" => false,
    _ => return None,
  };
  let mib = said(text, "MACHINE_CARD_MIB")
    .and_then(|mib| mib.parse::<u64>().ok())
    .filter(|mib| *mib > 0);
  let name = said(text, "MACHINE_CARD_NAME").unwrap_or_default();
  let card = match mib {
    Some(total_mib) if !name.is_empty() => Some(Card {
      name: name.to_owned(),
      total_mib,
    }),
    _ => None,
  };
  Some(Machine { wsl, card })
}

/// The node's machine, or why it could not be read.
pub async fn probe_machine(calls: &dyn NodeCalls) -> Result<Machine, String> {
  let asked = calls.exec(&probe_command()).await;
  let machine = if asked.code == 0 { parse_machine(&asked.stdout) } else { None };
  machine.ok_or_else(|| {
    let said = if asked.stderr.trim().is_empty() { asked.stdout.trim() } else { asked.stderr.trim() };
    let tail: String = said.chars().rev().take(300).collect::<Vec<_>>().into_iter().rev().collect();
    if tail.is_empty() {
      format!("the machine could not be read: exit code {}", asked.code)
    } else {
      format!("the machine could not be read: {tail}")
    }
  })
}
