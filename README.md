# bmo-arbiter

The card arbiter for a cluster of GPU nodes whose cards several services share. It is two programs:

- **The node arbiter,** one process on every GPU node, keeps that node's card lock: who may be on the card
  and in what order, a queue every occupant waits its turn in, claimants that take the card outright, a
  person's desktop that takes it while in use, and the units of an engine's memory it unloads into host
  memory or a dump store and loads back. It can lend its card to an engine on another node.
- **The master,** `card-master`, one process for the cluster, decides which service stands on which node,
  installs the node arbiter, plans every card's memory by placement rules, and serves the whole picture
  to a console.

It knows no service by name: each service declares itself, names itself to the node arbiter and answers
the master's adapter verbs.

## Layout

| Folder | Holds |
|---|---|
| `src/` | The master's package: the library and the `card-master` binary |
| `card/` | The node arbiter: its Python package, pinned `pyproject.toml`, node setup and user unit, uploaded to every node as it stands |
| `tests/` | The repository checks and every test: the master's as Cargo tests, the node arbiter's Python tests under `tests/card/` |
| `docs/` | The canonical statements of what the arbiter does |

At the root stand the Cargo manifest, the version, `testbed.toml` with its Korean ledger, `rustfmt.toml`
and `CLAUDE.md`.

## Documents

| Document | States |
|---|---|
| `docs/api.md` | The boundary: every route, protocol, file, verb and setting the arbiter exposes or calls, the codes, the version rule, and where admission is decided on a shared pool |
| `docs/node-arbiter.md` | The node arbiter: the lock, the occupants, claimants, the desktop, the keeper, the hold, lending, and its install |
| `docs/placement.md` | Units, host memory, the dump store, the master and its slaves, placing the services, and the rules |

## Version

- **One version for the repository,** the package's, in semver. `VERSION` holds it and the package's
  version equals it; the node arbiter shows the same version as its own.
- **How a change moves it** is stated in `docs/api.md`, 「Versioning」.

## Commands

```sh
cargo check --all-targets
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```
