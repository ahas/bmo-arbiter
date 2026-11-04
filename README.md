# bmo-arbiter

The card arbiter for a cluster of GPU nodes whose cards several services share. It is two programs:

- **The node arbiter,** one process on every GPU node, keeps that node's card lock: who may be on the card
  and in what order, a queue every occupant waits its turn in as a ticket, claimants that take the card
  outright, a person's desktop that takes it while in use, and the units of an engine's memory it unloads
  into host memory or a dump store and loads back. It can lend its card to an engine on another node.
- **The master,** `bmo-arbiter`, one process for the cluster, decides which service stands on which node,
  installs the node arbiter, plans every card's memory by placement rules, and serves the whole cluster
  and its placements.

It knows no service by name: each service declares itself, names itself to the node arbiter and answers
the master's adapter verbs. It is an upstream unit: a harness takes a turn on a card through its MCP server,
`bmo-arbiter mcp`, with no other unit running. A supervisor reads what every card is doing through the
hosted tool `bmo/gpu`, which calls the master's usage route from the tool host.

## Layout

| Folder | Holds |
|---|---|
| `.graph/` | The code dependency graph: one node per feature with its files and the features it needs, and the APIs this repository exposes and calls. Its area files are generated in the checkout and untracked, so a fresh checkout builds them again, while `settings.toml` is the repository's configuration and stays tracked |
| `src/` | The master's package: the library and the `bmo-arbiter` binary |
| `crates/` | The crates the arbiter publishes: `client/`, every body and a call for every route, and `plugin/`, the MCP server, the `bmo-arbiter` skill and the OpenAPI document's generator |
| `packages/` | The TypeScript client, `arbiter-client/`, generated from `docs/openapi.json` |
| `card/` | The node arbiter: its Python package, pinned `pyproject.toml`, node setup and user unit, uploaded to every node as it stands |
| `tests/` | The repository checks and every test: the master's as Cargo tests, the node arbiter's Python tests under `tests/card/` |
| `tools/` | The arbiter's own hosted tools: `gpu/`, the supervisor's read of every card, placed and registered by a deployment |
| `docs/` | The canonical statements of what the arbiter does, and the OpenAPI document |
| `deploy/` | The master's systemd user unit, which a deployment links from the deploy tree |

At the root stand the Cargo manifest, the version, the MIT license, `testbed.toml` with its Korean ledger,
`rustfmt.toml` and `CLAUDE.md`.

## Documents

| Document | States |
|---|---|
| `docs/api.md` | The boundary: every route of both programs, the clients and the harness plugin, every protocol, file, verb and setting the arbiter exposes or calls, the codes, the version rule, and where admission is decided on a shared pool |
| `docs/openapi.json` | The same routes as the OpenAPI 3.1 contract, generated from the client's types |
| `docs/node-arbiter.md` | The node arbiter: the lock, the occupants, claimants, the desktop, the keeper, the hold, lending, and its install |
| `docs/placement.md` | Units, host memory, the dump store, the master and its slaves, placing the services, and the rules |
| `docs/PERSONAL-DATA.md` | The rule against personal data in the repository, and the check that holds every tracked file to it |

## Version

- **One version for the repository,** `0.1.0` with a revision until the first release. `VERSION` holds it,
  every crate and the TypeScript package carry it, and the node arbiter shows the same version as its own.
- **How a change moves it** is stated in `docs/api.md`, 「Versions」.

## Commands

```sh
cargo check --workspace --all-targets
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test
cargo run -p bmo-arbiter-plugin --example openapi > docs/openapi.json
bun run --cwd packages/arbiter-client generate
bun run --cwd packages/arbiter-client check
```

## License

MIT. See [LICENSE](LICENSE).
