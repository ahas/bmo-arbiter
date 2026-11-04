//! The card master's use of every card (docs/api.md, 「Usage」): a projection of the cluster the master
//! last pictured, for a reader that asks what each node's card is doing, who holds it, what engine stands
//! on it and who waits. It names services and units alone; the picture's processes, pids and names stay
//! in the cluster.

use bmo_arbiter_client::cluster::{Cluster, LinkState};
use bmo_arbiter_client::usage::{Usage, UsageCard, UsageNode, UsageUnit, UsageWaiter};

/// The cluster read as the usage route answers it, at the moment the master answered.
pub fn usage_of(cluster: &Cluster, at: i64) -> Usage {
  Usage {
    at,
    nodes: cluster
      .nodes
      .iter()
      .map(|node| UsageNode {
        node_name: node.name.clone(),
        connected: node.link.state == LinkState::Connected,
        read_at: node.link.read_at,
        error: node.link.error.clone(),
        cards: node
          .cards
          .iter()
          .map(|card| UsageCard {
            index: card.index,
            name: card.name.clone(),
            total_mib: card.total_mib,
            used_mib: card.used_mib,
            free_mib: card.free_mib,
            utilization_percent: card.utilization_percent,
            turn: card.turn.clone(),
            claim: card.claim.clone(),
            units: card
              .units
              .iter()
              .map(|unit| UsageUnit {
                name: unit.name.clone(),
                service_name: unit.service_name.clone(),
                part: unit.part.clone(),
                vram_mib: unit.vram_mib,
                ram_mib: unit.ram_mib,
                state: unit.state,
              })
              .collect(),
            waiters: card
              .queue
              .iter()
              .map(|ticket| UsageWaiter {
                ticket_id: ticket.ticket_id,
                service_name: ticket.service_name.clone(),
                position: ticket.position,
                waited_ms: ticket.waited_ms,
                unit_name: ticket.unit_name.clone(),
                vram_mib: ticket.vram_mib,
              })
              .collect(),
          })
          .collect(),
      })
      .collect(),
  }
}
