//! A placement rules file the planner's and the rules' tests share: two claimants, a pool of nodes whose
//! role names the text model and a desktop group, the text model's, the router's and three voice units, and a
//! desktop rule and a request rule.

// Each test file is its own crate and uses only part of this module.
#![allow(dead_code)]

pub const RULES: &str = r#"
occupants:
  - name: tester
    claims: true
    seen_by: { tickets: true, containers: tester- }
    ram_free: all
  - name: batch
    claims: true
    first: false
    seen_by: { tickets: false, cgroup: batch.service }
    ram_free: 8
nodes:
  - name: compute
    select: { role: model }
    pool: true
    reserve_mib: 300
    dump: ram
    ram_max_gib: 24
  - name: desk
    select: { desktop: true }
    reserve_mib: 1000
units:
  - unit: "model/*"
    kind: resident
    priority: 50
    group: model/pipeline
    load_s: 2
    unload_s: 0.5
    dump: disk
  - unit: "router/*"
    kind: fill
    priority: 30
    load_s: 17
    unload_s: 1
  - unit: "voice/large"
    kind: on-demand
    priority: 40
    where: { role: model }
    vram_mib: 6000
    hold: { allowed: true, max_wait_s: 60 }
  - unit: "voice/small"
    priority: 60
    vram_mib: 1500
  - unit: "voice/big"
    priority: 70
rules:
  - when: { desktop: busy }
    evict: node
  - name: voice-on-compute
    when: { request: "voice/large", on: { role: model } }
    evict: { cheapest: ["model/*"] }
    restore: true
"#;
