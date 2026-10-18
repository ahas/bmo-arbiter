# Working in bmo-arbiter

These rules hold every change to this repository.

## The documents come first

- `docs/` is the canonical statement of what the arbiter does; the code is one realisation of it. Before
  changing code, read the document covering that area: `docs/api.md` for every route, protocol, file,
  verb, setting and code; `docs/node-arbiter.md` for the node arbiter; `docs/placement.md` for units, the
  dump store, the master and the rules.
- A change that would make a document wrong or incomplete repairs the document first, stating what is
  true now, and the code is written to match. A change to the boundary moves the version as
  `docs/api.md` says, in the same commit.
- A document states what is true now: no change history, no rationale told through an earlier design,
  no pasted source, signature or config fragment, and no file-and-line pins.

## Boundaries

- **The arbiter knows no service.** Services declare themselves, name themselves in
  `BMO_CARD_SERVICE` and answer the adapter verbs; anything that differs per service stands in that
  service's declaration, its rules entry or its own settings, never in a table or a branch here.
- **Other units are reached only as `docs/api.md` states:** the node registry through its configured
  command, nodes through their shells, services through their adapters and declarations. Nothing here
  imports another unit's code or reads its files or database.
- **Dependencies:** crates from the registry pinned in the manifest, and of other repositories only the
  base libraries `bmo-chat`, `bmo-chat-client` and `bmo-ui`. The dependency check in `tests/` holds this.
- **No lookup table standing in for polymorphism,** and no hand-written parser for a general format:
  TOML, YAML, JSON, markdown, command lines and their kin are read by a maintained library.

## Language

- Everything in this repository is English: code, comments, strings, tests, documents, commit messages
  and branch names. No Korean in code or tests; the Hangul check in `tests/` and the testbed's `korean`
  check hold this against `korean-ledger.json`, which allows none.
- What the arbiter says is an English sentence for a person, or a status, a protocol word and a
  snake_case code for a machine. Words a person reads in another language are a console's language pack.
- No personal data: no user names, home directories, machine names, addresses or names of any agent.
  Names the arbiter needs at runtime come from its settings, the node registry and the services'
  declarations, never from the code. Tests use neutral names.

## Layout

- One Cargo package at the root, the master, and no other build tool's manifest at the root. The node
  arbiter is the Python package in `card/` with its own `pyproject.toml`, uploaded to every node as it
  stands.
- No directory named after its parent, and the README's folder table names every top-level folder.
- Tests live in `tests/`, never beside the code they test: the master's as Cargo tests, the node
  arbiter's under `tests/card/`, run by a Cargo test of their own so one `cargo test` runs them all.

## The stages of a change

1. **Writing:** the only check is `cargo check --all-targets`, and `python3 -m py_compile` on a changed
   Python file.
2. **Review:** read the whole diff against the document governing it, then `cargo fmt --check` and
   `cargo clippy --all-targets -- -D warnings`.
3. **Verification:** the test targets touched, then `cargo test` once, output written to a file and the
   file read.

A change is not done before stage 3 has passed.
