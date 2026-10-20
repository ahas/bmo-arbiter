//! `bmo-arbiter place` and `bmo-arbiter remove` as a person runs them: the placement asked of the running
//! master through the arbiter's client, and its lines said here as the master says them (docs/placement.md,
//! 「A request」).

use std::time::Duration;

use bmo_arbiter_client::placements::{PlacementAsk, PlacementKind, PlacementState};
use bmo_arbiter_client::{ArbiterClient, CallError};

// How often the placement is read again while it runs.
const FOLLOW_EVERY: Duration = Duration::from_millis(500);

/// The placement asked and its lines printed as they come; whether it ended done, or why it could not ask.
pub async fn ask_master(
  port: u16,
  token: Option<String>,
  kind: PlacementKind,
  service: &str,
  node: &str,
) -> Result<bool, String> {
  let address = format!("http://127.0.0.1:{port}");
  let master = ArbiterClient::new(&address, token)?;
  let ask = PlacementAsk {
    kind,
    service_name: service.to_owned(),
    node_name: node.to_owned(),
  };
  let mut placement = master.create_placement(&ask).await.map_err(|fault| match fault {
    CallError::Transport(why) => format!("the master does not answer on 127.0.0.1:{port}: {why}"),
    fault => fault.to_string(),
  })?;
  let mut said = 0;
  loop {
    for line in &placement.lines[said..] {
      println!("{line}");
    }
    said = placement.lines.len();
    match placement.state {
      PlacementState::Done => {
        println!("done");
        return Ok(true);
      }
      PlacementState::Failed => {
        println!("failed: {}", placement.failure.as_deref().unwrap_or("the master gave no reason"));
        return Ok(false);
      }
      PlacementState::Queued | PlacementState::Running => {}
    }
    tokio::time::sleep(FOLLOW_EVERY).await;
    placement = master.placement(placement.id).await.map_err(|fault| fault.to_string())?;
  }
}
