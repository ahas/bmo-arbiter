# Boundary API

Every interface by which the arbiter meets anything outside its own processes: what the master and the
node arbiter serve, the files and words a service on a node shares with the arbiter, and the four
contracts the arbiter itself calls. Other repositories match against this document; the code is one
realisation of it. What the contracts mean is `docs/node-arbiter.md` and `docs/placement.md`.

The arbiter is two programs: the **master**, `card-master`, one per system, which places services on
nodes and plans every card's memory; and the **node arbiter**, the `card/` package standing as
`bmo-card.service` on every GPU node, which keeps that node's card lock, queue, claims and units. It knows
no service by name: services declare themselves, name themselves to the node arbiter and answer its
adapter verbs.

## Versioning

The version is the package's in `Cargo.toml` (semver), starting at 0.1.0; `VERSION` at the root holds the
same, and the node arbiter shows it (with the commit it was carried from) as its own version. This
document changes in the same commit as the boundary it states. A change to a contract an existing
counterpart reads — a route, a field, its type or meaning, a status, a line of a protocol, a file's shape,
a verb or flag, a setting, a code — moves the major version; an addition no existing counterpart reads
moves the minor; anything else moves the patch.

## Counterparts

Each contract names its counterpart by role, not by its code.

| Role | Uses |
| --- | --- |
| Console | the master's state and events (「The master's state」) |
| Operator, job | the master's command line (「The master's command line」), the rules file (「The rules file」) |
| Service with a declaration | its declaration (「Declarations」), its adapter (「Adapters」), its unit descriptions (「Unit descriptions」) |
| Occupant on a node | the hold (「The node arbiter's hold」), the turn client (「The turn client」), `BMO_CARD_SERVICE`, the card lock |
| Borrowing engine | the lending protocol (「The lending protocol」), the `lend` verb, a control socket for its units |
| Pipeline owner | a control socket under the owners' folder (`docs/placement.md`, 「Unloading and loading a unit」) |
| Claimant's agent | the hold, under the claimant's name |
| Node registry | answers the master's node command (「The node registry」) |
| Supervisor | systemd, the testbed: the master's unit and its settings |

## Settings

### The master

Each setting is read from the environment; `--data-root` and `--code-root` name the first two on the
command line instead. The deployed unit, `bmo-card-master.service`, sets `BMO_DATA_DIR` to the deployment's
data root and reads the deployment's own settings from `~/.config/bmo-arbiter/env` where it stands.

| Environment | Meaning | Where unset |
| --- | --- | --- |
| `BMO_DATA_DIR` | the master's data root, whose `placements/` holds the rules, the placements, the request journal and the unit descriptions | `~/.local/share/bmo/data` |
| `BMO_CODE_ROOT` | the arbiter's own tree: the `card/` package the install uploads and the `VERSION` it carries | `BMO_DEPLOY_DIR`, else the deploy link beside the binary where it stands in a release, else the tree the binary stands in |
| `BMO_DEPLOY_DIR` | the deploy link of the arbiter's own release, read afresh each time | none |
| `CARD_DECLARATIONS` | the declaration roots, separated by `:` (「Declarations」) | no root, so no service is declared |
| `CARD_NODES_COMMAND` | the node registry's command, split by shell word rules (「The node registry」) | none; every verb that reaches a node ends with an error naming the setting |
| `CARD_RIVAL_UNITS` | user units, separated by spaces, that keep a card lock by rules of their own: `install` refuses a node where one is active, and `lend` stops, disables and removes them | none |
| `BMO_BUN` | the Bun binary a console command, and an adapter command whose first word is `bun`, run with | `~/.bun/bin/bun` where it stands, else `bun` |
| `BMO_HUB_CONSOLE` | the console script that reaches a node of the console kind (「Node shells」) | none; such a node is not reached, and the error names the setting |
| `CARD_MASTER_PORT` | the loopback port of the master's API; `0` has the system choose, named in the line the master prints as it starts listening | `7440` |
| `CARD_MASTER_READ_EVERY_MS` | how often every slave is read | `2000` |
| `CARD_MASTER_DISCOVER_EVERY_MS` | how often the node registry is read | `60000` |
| `CARD_INSTALL_POLL_MS` | how often the install polls the node setup and the unit, and how long it waits before reading the unit's main process again, all alike; a check sets it to run an install quickly | the setup every 5 s, the unit every second, then 5 s |
| `HOME` | the home a key path written `~/…` and the defaults above stand under | `/` |

### The node arbiter

Read from its environment, which `bmo-card.service` fills from `~/.config/bmo-card/env` and
`~/.config/bmo-card/lend.env` (「The node files」). None is set by the install but those named there.

| Environment | Meaning | Where unset |
| --- | --- | --- |
| `CARD_LOCK` | the card lock's path | `~/.gpu.lock` |
| `CARD_DESKTOP` | `1` on a machine whose card is a person's desktop, switching the desktop rule on | off |
| `CARD_OCCUPANTS` | the occupants file | `~/.config/bmo-card/occupants` |
| `CARD_RULES` | the node's part of the rules | `~/.config/bmo-card/rules.json` |
| `CARD_STORE` | the dump store, on a local NVMe | `~/.local/share/bmo-card/store/units.store` |
| `CARD_OWNERS` | the folder owners besides the borrower open their control sockets in | `~/.local/share/bmo-card/owners` |
| `CARD_TURN_PORT` | the hold's loopback port | `7439` |
| `CARD_TURN_KEEP_S` | how long a ticket not asked after is kept | 30 |
| `CARD_TURN_TAKE_S` | how long a granted turn may go untaken before it passes once | 5 |
| `CARD_TURN_HOLD_S` | how long a turn is kept in use while another ticket waits | 10 |
| `CARD_TURN_HOLD_OPEN_S` | the longest an answer is held open | 20 |
| `CARD_CLAIM_POLL_S` | how often a claimant's cgroup is read | 0.1 |
| `CARD_CLAIM_SETTLE_S` | how long a node holds nothing of a claimant before its claim ends | 10 |
| `CARD_CLAIM_IDLE_S` | how long a claim on tasks alone stands with nothing on the card | 600 |
| `CARD_LOCK_WAIT_S` | how long a hold waits for the lock | 10 |
| `CARD_YIELD_WAIT_S` | how long a hold told to `YIELD` has to let go | 5 |
| `CARD_LOCK_GRACE_S` | how long a holder below a claim may keep the lock | 20 |
| `CARD_LEND_QUIET_S` | how long after its owner's last graph a lent hold's turn rests | 10 |
| `CARD_LEND_GIVES_WAY_S` | a lent hold's `givesWayS` | 3 |
| `DESKTOP_READ_EVERY_S` | how often the desktop's card is read | 1 |
| `DESKTOP_BUSY_READINGS` | readings in a row that make the card the desktop's | 5 |
| `DESKTOP_BUSY_PERCENT` | the use that counts as busy | 20 |
| `DESKTOP_ROOM_MIB` | the free memory below which the card counts as busy | 4096 |
| `DESKTOP_QUIET_S` | how long the desktop is quiet before the card is given back | 60 |
| `LLM_KEY` | lend setting: the key the head asks with | lending off |
| `CARD_ADDRESS` | lend setting: the address on the inner network the node waits on | lending off |
| `CARD_LEND_SERVICE` | lend setting: the service the card is lent to | required with lending |
| `CARD_PACKED` | lend setting: `1` where the hold is packed beside the lock and takes no turn | not packed |
| `CARD_RPC_SERVER` | lend setting: the rpc-server binary the arbiter starts for a hold | required with lending |
| `CARD_RPC_CACHE` | lend setting: the rpc-server's tensor cache folder | required with lending |
| `CARD_SLOT_DIR` | lend setting: the folder of saved slot pieces | required with lending |
| `GGML_RPC_STATE_DIR`, `LLAMA_CACHE` | passed to the rpc-server the arbiter starts | the rpc-server's own |
| `BMO_CARD_SERVICE` | read from each lock holder's environment, never from the arbiter's own (「The service a process declares」) | — |

Lend settings missing some but not all of `LLM_KEY`, `CARD_ADDRESS`, `CARD_LEND_SERVICE`,
`CARD_RPC_SERVER`, `CARD_RPC_CACHE` and `CARD_SLOT_DIR` stop the node arbiter at its start, naming what
is missing. To each owner it starts it hands `GGML_CUDA_EVICTABLE` and `GGML_CUDA_CONTROL`
(`docs/node-arbiter.md`, 「Lending」).

## What the master serves

### The master's state

HTTP on `127.0.0.1:<CARD_MASTER_PORT>`, no key, loopback alone. Bodies are JSON in UTF-8.

- **`GET /state`** — `200` with the whole picture:

  | Field | Shape |
  | --- | --- |
  | `master` | `version`, `startedAt`, `rulesFile`, `rulesLoadedAt`, `rulesError`, `readEveryS` |
  | `groups` | each `name`, `pool`, `nodes`, `totalMiB`, `freeMiB` |
  | `nodes` | each `name`, `role`, `desktop`, `link` (`state` `connected`, `lost` or `unknown`, `since`, `readAt`, `lastReadMs`, `error`), `version` (its slave's), `rules` (`loadedAt`, `error`), `occupants`, `cards` |
  | `nodes[].cards` | each `index`, `name`, `totalMiB`, `usedMiB`, `freeMiB`, `units`, `turn` (`service`, `ticket`, `waitedS`, `inUse`) or none, `queue`, `claim` (`by`, `since`), `ram` (`dumpsMiB`, `availableMiB`, `maxMiB`, `floorMiB`, `dump`), `stage` (`waiting`, `waitingS`), `store` (`path`, `bytes`, `extents`, `slotsUsed`, `error`) or none, `processes` or none |
  | `…units` | each `name`, `service`, `part`, `vramMiB`, `state` (`resident`, `ram`, `evicted`, `mixed`), `ramMiB`, `since`, `evictable`, `reason`, `loadMs`, `unloadMs`, `kind` (`resident`, `on-demand`, `fill`) or none, `detail` (「Unit descriptions」) or none |
  | `…queue` | each `ticket`, `service`, `turn`, `position`, `waitedS`, `waitS`, `unit`, `vramMiB` |
  | `…processes` | each `pid`, `name`, `service` or none, `usedMiB` |
  | `declared` | the units the rules declare: each `unit` (a name or pattern), `service`, `kind`, `vramMiB` |
  | `services` | each service the master places: its declaration, its `nodes`, whether it is unsettled and why |
  | `request` | the placement request under way with its step, or none |
  | `events` | the last events, newest first: each `at`, `node` or none, `code`, `text` |

  Times are seconds since the epoch; sizes are MiB. An event's `code` is a snake_case word a console
  translates by; its `text` is an English sentence for a person (「Messages and codes」).
- **`GET /events`** — `200`, `text/event-stream`: one `state` event carrying the same JSON as `GET /state`
  whenever it changes and at least every five seconds; a listener that leaves ends its stream.
- **Any other path** — `404` with `error`.

### Placement requests

- **`POST /place`** and **`POST /remove`** with `service` and `node` — `200`, `text/plain`: what the
  master does, one English line at a time as it does it, the last line `== done` or `== failed: <why>`.
  Requests run one at a time in arrival order (`docs/placement.md`, 「A request」); a body that does not
  read is `400`/`422`.
- These are what `card-master place` and `remove` send; a stream that ends without its last line is a
  master that stopped mid-request, which undoes the request at its next start.

### Unit descriptions

- **`PUT /descriptions/<service>`** with `units`, an object from a unit's name to its detail:
  `blocks` (`from`, `to`, counted from 0) or none, `output`, `mtp`, `fullAttention` or none.
  - `204`: written whole to the master's store of descriptions, `placements/card-units/<service>.json`,
    replacing what the service said before; the next state shows it.
  - `400` with `code` and `error`: `service_name_invalid`, a service that is not lower-case letters,
    digits and hyphens; `description_invalid`, a body that does not read, holds no `units` object, names a
    unit whose name does not start with `<service>/`, or gives a detail that does not read.
  - `500` with `code` `description_not_written` and `error`: the store could not be written.
- What each detail means is `docs/placement.md`, 「What a service says of its units」. The folder itself is
  the master's own; a service hands its description over by this route alone.

### The master's command line

`card-master` is one binary. With no verb it is the master, serving the routes above and printing
`card-master: answering on 127.0.0.1:<port>` once it listens. Every verb exits `0` once it has done what it
says, `1` where it ran and failed, `2` where its words do not read; what it prints is English.

| Verb | Words | What it does |
| --- | --- | --- |
| *(none)* | | stands as the master |
| `--once` | | reads every slave once, prints the picture as JSON and ends |
| `check` | | reads the rules and says what they would place where — the claimants, each group with the nodes it chooses, the units and the rules — writing nothing; fails where a unit's `vram_mib` is more than any answering card it may stand on holds once its group's `reserve_mib` is kept back |
| `push` | `[--node <node> …]` | writes each node whose arbiter answers, or each node named, its part of the rules (「The node files」), and a line of `placements/card-rules.log` |
| `rules` | `--node <node> --json [--desktop true\|false]` | prints the part of the rules that reaches one node, the JSON a push writes; whether the machine is a desktop is its slave's to say where it answers, else the flag's, else not |
| `place` | `<service> --to <node>` | sends `POST /place` to the running master and prints its lines; exits `1` on `== failed` |
| `remove` | `<service> --from <node>` | sends `POST /remove` likewise |
| `install` | `--node <node> [--node …] [--store-gib <n>]` | stands or updates the node arbiter on each node (`docs/node-arbiter.md`, 「Installing」) |
| `uninstall` | `--node <node> [--node …] [--keep-store]` | takes the node arbiter off each node: its unit stopped, disabled and removed, and the dump store with its table unless kept |
| `lend` | `--node <node> (--settings <file> \| --off)` | writes the lend settings on a node whose arbiter stands, or takes them off, removes every rival unit there, restarts the arbiter and waits until it stands (「The node files」) |
| `occupants` | `--node <node>` | prints the node's occupants order and the last changes its arbiter took up, writing nothing |

`--data-root <dir>` and `--code-root <dir>` stand before or after the verb. A `--settings` file is the
text of `lend.env`, in which `{home}` stands for the node account's home and is written so before it is
carried.

## What the node arbiter serves

### The node arbiter's hold

HTTP on the node's loopback, `127.0.0.1:<CARD_TURN_PORT>`, no key. Bodies are JSON.

- **`POST /turns`** with `service`, and optionally `need` (`gpus`, `vramMiB`; shown, weighing nothing),
  `ref` (a reference such as a task id; shown), `givesWayS` (the seconds it takes to let go once told),
  `waitedS` (how long the request it stands for has waited), `waitS` (how long to hold the answer open)
  and `rest` (`true`: no request in hand, only to rest on the card once it is free).
  - `200` with `ticket`, `service`, `turn: true`, `inUse`, and where they apply `resting: true`,
    `giveWay: true` and `for` (the occupant or claimant to let go for).
  - `202` with `ticket`, `service`, `turn: false`, `position`, `waitS`, `claim` (the claimant the card
    waits for, or none), and a `Retry-After` header of `waitS` rounded, at least 1.
  - `403` with `error`: the service has no place among the node's occupants.
  - `400` with `error`: a body that does not name a service, or a field of the wrong kind.
- **`GET /turns/<ticket>`** — answered as the `POST` is, keeping the ticket in line. `?waitS=<s>` holds the
  answer open; `?next=<s>` asks whether a turn carries on to a next request that has waited `s`, answered
  `200` with `keep` true or false; `?rest=1` rests the turn. `404` with `error` is a ticket no longer held;
  `400` with `error` a query that does not read.
- **`DELETE /turns/<ticket>`** — gives the ticket back: `204`, or `404`.
- **`GET /turns`** — `200` with the state: `occupants` (the claimants standing first first), `ranks` (the
  same names grouped by rank), `claim`, `lockHeldBy` (pids), `busy`, `queue` (each ticket as answered,
  with `waitedS`, and `need`, `ref`, `rest` where asked), `typicalHoldS` (by service), and `units` as
  `GET /units` gives them.
- **An answer that cannot be delivered** gives its ticket back.

### The node arbiter's units

On the same port.

- **`GET /units`** — `200` with everything `GET /turns` gives, and: `version`, `desktop`, `card` (the
  driver's `name`, memory in all, used and free, and `processes`: each `pid`, `name`, `service`,
  `usedMiB`; none where the driver gives no count per process) or none, `units` (each with its name,
  owner, state, size, host memory, slot, last timings and, for an owner besides the lent one, its `door`),
  `stage` (the lent owner's door: `waiting`, `waitingS`, `graphs`, `waits`, `waitedS`), `store` (path,
  size, extents, slots in use) or none, `claim` (`by`, `since`), `ram` (`dumpsMiB`, `availableMiB`,
  `maxMiB`, `floorMiB`, `dump`) and `rules` (`loadedAt`, `error`).
- **`POST /units/evict`** and **`POST /units/restore`** with `units` (names; absent for all) and `reason`.
  - `200` with `results`, each unit's outcome (`ok`, the unit, where it went or why not).
  - `409` with `error`, and `results` for an unload: an unload that did not take out every unit asked for,
    or a restore that found no room, which leaves it out.
  - `400` with `error`: `units` not a list of names. `503` with `error`: the arbiter is starting.
- **Any other path** — `404` with `error`.

### The lending protocol

TCP on `CARD_ADDRESS`, open only where lend settings stand. Each connection opens with one line,
`<VERB> <key> [<words>]`, the key being `LLM_KEY`; a wrong key or verb is answered `REFUSED`.

| Asked | Answered | Meaning |
| --- | --- | --- |
| `HOLD <key> [local]` | `READY [<socket>]`, `BUSY …`, `CLAIMED <claimant> …` | holds the card for as long as the connection stays open; `local` for an engine on the node itself, told its control socket where the node keeps a dump store |
| `STATE <key>` | `FREE`, `HELD`, `LOCKED by <pid>`, `CLAIMED <claimant>` | whether it could be held |
| `QUIET <key>` | `QUIETED` | rests the standing hold's turn until its owner's next graph |
| `WANT <key> <seconds>` | `WANTED` | a request of the head's has waited that long for this card while its units are out |
| `UNWANT <key>` | `UNWANTED` | no request waits any more |
| `DROP <key> <file> …` | `DROPPED <n>` | removes the pieces of the named slot files |
| `FETCH <key> <file>` | `PIECE <name> <size>` and its bytes, for each piece, then `END` | sends one slot file's pieces |
| `SPACE <key> [<file>]` | `SPACE <free> <total> <held>` | the pieces' folder's bytes free and in all, and the bytes the named file's pieces take |

On a standing hold the head may say `USES <group>`, answered `USING <group>` or `REFUSED <why>`, and the
arbiter says, whenever each applies: `QUEUED <position> <seconds>` while the hold waits for its turn,
`EVICTED <reason>` or `EVICTED claim <claimant>` when its units went out, `RESTORED` when they are back,
`PASS <occupant>` when its turn is to pass at its request boundary, and `YIELD <claimant>` when a claim
needs the card and the units cannot be unloaded.

### The turn client

- **`~/bmo-card/turn.py`** on every node the arbiter stands on, installed with it and updated by every
  install. An occupant in a process of its own imports its `TurnClient` (a ticket asked for and after,
  `keep`, `rest`, `give_back`, `state`), `hold` (the turn waited for and the lock taken), `GivingTurn` (a
  turn that gives way) and `quiet` (wait until the card stands unasked), or runs it as a command:
  `python turn.py --service <name> [--lock <path>] -- <command> [<argument> …]`, which holds a turn and the
  lock around the command and exits with its code, or `3` where the service has no place on the node.
- **An occupant may carry a copy of its own** beside its code instead; the contract it meets is the hold
  above, and the installed file is what follows this repository's version.

### The service a process declares

- **`BMO_CARD_SERVICE=<service>`** in a process's environment is the one way a process on a node names
  the occupant it is. The arbiter reads it from every lock holder's environment; a holder that declares
  nothing stands below every occupant named and is never ended for being unnamed; one declaring a service
  with no place among the occupants is ended with its process group. A service's name is the one its
  declaration gives (「Declarations」).

### The card lock

- **`~/.gpu.lock`** in the node account's home (`CARD_LOCK`): an exclusive `flock` every occupant takes
  before it puts anything on the card and lets go only once everything is off it
  (`docs/node-arbiter.md`, 「The shared card lock」). An occupant takes it itself once its turn is granted;
  the arbiter takes it only for a claimant, the desktop and a lent or local hold.

### The node files

All in the node account's home, written by the master's verbs unless said otherwise:

| File | Written by | Shape |
| --- | --- | --- |
| `~/.config/bmo-card/occupants` | every placement that changes the order, whole, renamed over the old | words highest first, `=` joining one rank, `*` for every declared service not named, `#` a remark |
| `~/.config/bmo-card/occupants.log` | the node arbiter, one line per change it takes up | the time, the order before, the order after |
| `~/.config/bmo-card/rules.json` | `push`, and every placement, renamed over the old | the node's part of the rules: `occupants`, `nodes`, `units`, `rules` and `turns` (each service's `yieldAfterS` and `canPause`) |
| `~/.config/bmo-card/rules.log` | the node arbiter, one line per rules file it takes up | |
| `~/.config/bmo-card/env` | `install`, `0600` | `CARD_LOCK`, and `CARD_DESKTOP=1` on a WSL machine |
| `~/.config/bmo-card/lend.env` | `lend` alone, `0600` | the lend settings (「Settings」) |
| `~/bmo-card/VERSION` | `install` | this repository's version and the commit the files were carried from, marked where they differ from it: `0.1.0+<commit>` or `0.1.0+<commit>.changes` |

The node arbiter reads the occupants and rules files again within a second of a change.

### Declarations

- **The roots** are the directories `CARD_DECLARATIONS` names. In each root the master reads, again for
  every request and every round:
  - every `<service>.card.toml` its git index holds, checked-out submodules included, its adapter run from
    the file's directory;
  - the `[card]` table of each `tools/<folder>/.placed/tool.toml`, for each folder of `tools/` whose name
    starts with neither `_` nor `.` and each folder of `tools/_shared/`, its adapter run from
    `tools/<folder>`.
- **A declaration's keys:** `service`, `card_mib`, `priority`, `spread` (`one` or `many`), `alone`,
  `adapter` (a list of words, not empty, or absent), and an optional `[turn]` table with `yield_after_s`
  (seconds, zero or more) and `can_pause` (`docs/placement.md`, 「A service declares itself」).
- **Refused,** said among the events and failing the request that read it: a file named otherwise than
  its `service`, a `service` that is not lower-case letters, digits and hyphens, an empty `adapter`, a
  negative `yield_after_s`, and two declarations of one service across all roots.

### Adapters

- **The master runs** `<adapter words> <verb> --node <node> --placed <node>,…` from the declaration's
  directory, the verb one of `install`, `uninstall`, `start`, `stop`, `health`, `placed`. A first word
  `bun` is run as `BMO_BUN`.
- **The adapter's last line** is one JSON object for every verb: `ok`, `node`, `state` (`running`,
  `stopped`, `absent` or `failing`), `detail`, and for `placed` the `nodes` in order. Lines before it are
  shown as what the adapter says on its way.
- **An adapter touches no role and no occupants;** it keeps its own service's placement alone.

### The rules file

- **`placements/card-rules.yaml`** in the master's data root, YAML, written by a person or a job and read
  by the master alone, again whenever it changes. Its four parts — `occupants`, `nodes`, `units`, `rules`
  — are `docs/placement.md`, 「The rules」. A file that does not read leaves the last one read standing and
  is said in `master.rulesError`.
- **`placements/card-rules.log`** — one line for every change of the file the master takes up and every
  push: the time, the file's hash, and what changed or where it went.
- **Beside them,** the master alone writes `placements/services.json` (each service's nodes in order),
  `placements/services-request.json` (the request under way, journaled step by step) and the
  descriptions folder (「Unit descriptions」).

## What the arbiter calls

### The node registry

The master asks a node registry which nodes there are, how each is reached, and writes each node's role
there. The registry is another unit's; the master runs `CARD_NODES_COMMAND` with these words:

| Words | Exit and output the master reads |
| --- | --- |
| *(none)* | `0`; a listing in which every node is a line beginning `<name> [ssh]` or `<name> [hub]`, other lines ignored, in the registry's order |
| `--target <name>` | `0`; its last line one JSON object: `kind` (`ssh` or `hub`), `role` (the services the node is reserved for, joined by `+`, or none), and for `ssh` `host`, `port`, `user`, `keyPath` or none, `shell` (`posix` or `powershell`); for `hub` `agent`, `hubUrl`, `tokenFile` |
| `--set <node> --role <service>[+…]\|none` | `0` once the node's row holds that role |

A non-zero exit, or a last line that does not read, is the registry's fault, said with its last 300
characters. A node whose shell is PowerShell is not reached for a command.

### Node shells

- **`ssh` nodes:** a command runs as `ssh -o BatchMode=yes -o ConnectTimeout=10 -o
  StrictHostKeyChecking=accept-new -p <port> [-i <key> -o IdentitiesOnly=yes] <user>@<host> bash -c
  '<command>'`, and a file is carried by `scp -q` with the same options; a failed carry is tried once
  more.
- **`hub` nodes,** reached through a console hub: a command runs as `<BMO_BUN> <BMO_HUB_CONSOLE> exec -a
  <agent> <command>`, and a file is carried by `<BMO_BUN> <BMO_HUB_CONSOLE> upload <local> <remote> -a
  <agent>`.
- **On every node** the master runs only bash commands: reading a slave
  (`curl -s -m 3 http://127.0.0.1:7439/units`), writing the node files, the install's steps and
  `systemctl --user` on `bmo-card.service` and the rival units.

### Adapters, declarations and descriptions

The master calls each service's adapter (「Adapters」) and reads its declaration (「Declarations」); a
service hands its unit descriptions in (「Unit descriptions」). Nothing else of a service is read or run.

## Messages and codes

- **Everything the arbiter says is English:** the lines a verb prints, the lines of a placement request,
  an event's `text`, a unit's `reason`, an `error` field.
- **What a machine branches on is a status or a code:** an HTTP status, an exit code, a protocol word
  (`READY`, `QUEUED`, `== done`, `== failed: `), an event's `code`. A console shows a person words of its
  own language by the code, never by parsing the English.
- **Every event's code** is a snake_case word naming what happened, never the node or the service it
  happened to; the node stands in `node`, the rest in `text`.
- **The codes of what the master reads off a node between two rounds:**

  | Code | What happened |
  | --- | --- |
  | `node_reconnected` | the master reaches the node again |
  | `node_lost` | the master lost the node; the text carries the error where there is one |
  | `card_claimed` | a claimant claimed the card |
  | `card_given_back` | a claimant gave the card back |
  | `stage_waiting` | the engine waits at the card's stage, its units out |
  | `stage_resumed` | the engine goes on after waiting at the stage |
  | `unit_unloaded_to_ram` | a unit went into its owner's host memory; the text carries the reason |
  | `unit_unloaded_to_disk` | a unit went into the dump store; the text carries the reason |
  | `unit_lowered` | a unit kept in host memory moved into the dump store |
  | `unit_loaded` | a unit came back onto the card |
  | `unit_partly_unloaded` | some of a unit's buffers are out |
- **The codes of what the master itself does and reads:**

  | Code | What happened |
  | --- | --- |
  | `rules_read` | the rules file was read again |
  | `rules_absent` | no rules file stands, so the default places the units |
  | `rules_unreadable` | the rules file does not read, so the rules read before stand |
  | `description_unreadable` | a unit description in the store does not read |
  | `ticket_held` | a ticket is held; the text carries its reason |
  | `units_unloaded` | units were unloaded to make room for a ticket |
  | `unload_failed` | the unloads for a ticket failed and are tried again |
  | `restore_waiting` | units wait to come back until their asker's units leave the card |
  | `units_restored` | units unloaded for a ticket came back |
  | `restore_failed` | units unloaded for a ticket did not come back yet and are tried again |
- **The codes of the placement:**

  | Code | What happened |
  | --- | --- |
  | `request_done` | a placement request ended done |
  | `request_failed` | a placement request stopped; the text carries the step and why |
  | `request_undone` | a request a stopped master left unfinished was undone at its start |
  | `placement_unsettled` | a service's adapter and the node registry disagree on its nodes at the first start |
  | `placement_drift` | what stands differs from the master's state; named and not followed |
  | `service_not_uninstalled` | narrowing a single-node service, its old node's uninstall failed |

## Admission on a shared pool

Where two services stand at one rank on a pool of nodes — a borrowing engine that lends the cards (here
the text model) and a pipeline whose stages pause (here the coder) — whether a request of the text model
is let onto the cards while the coder holds them is decided at these points, in order. The current
deployment declares the coder with `yield_after_s = 60` and `can_pause = true`, and the text model with
`yield_after_s = 300` and `can_pause = false`.

1. **The text model's head finds its units out and says `WANT`** on each card's hold, with the seconds the
   request has waited (「The lending protocol」). Beside it the head bounds its own wait for a stage: a
   request still waiting at the stage after 120 seconds is given up by the head and refused to its caller.
   That bound is the borrowing engine's own, outside this repository.
   - `docs/node-arbiter.md`, 「Lending」: "A lent hold whose units are out takes a place in the queue on
     `WANT` through its own restore … The occupant holding the turn then gives way to it by its own rules:
     at once if it rests, by its turn values if it stands at the lend service's rank, never if it stands
     above."
2. **The node arbiter queues the text model's restore at the coder's rank:** `Card.want` records the
   request's arrival, `Card._start_wanting` starts `Card._want_back`, which calls `Card.restore` with
   `waits`, and `Card._take_lock_back` asks `Turns.wait` for the lend service's ticket, backdated by the
   wait at the owner's door or the `WANT` seconds, saying `QUEUED <position> <seconds>` as its place moves
   and waiting without bound for its turn. A claim standing, or the units already back, ends the attempt
   (`Card._start_wanting`, `Card._want_back`).
   - `docs/placement.md`, 「The engine's turn」: "the borrowing engine queues for them through its own
     restore: a request that finds its units out has the head say `WANT` on each card, so its hold takes a
     place in each node's queue at the pipeline's rank."
3. **The node arbiter decides when the coder's turn gives way.** The queue is ordered by rank and then by
   arrival (`Turns._order`). The coder's turn is told to give way (`giveWay`, `for`) by
   `Turns._give_way_for` only when a claim stands, when its turn rests, when an occupant above it waits, or
   — being of the same rank — once `Turns._turn_spent` finds its turn in use for its declared
   `yield_after_s` with `can_pause` true. Its use is counted from the grant or from the request that ended
   its rest. A turn that does not rest and has not used its `yield_after_s` does not give way to the text
   model's ticket.
   - `docs/node-arbiter.md`, 「One rank taking turns」: "Once its turn has been in use for its
     `yieldAfterS` while a ticket of its rank waits, it gives way to that ticket: in the middle of a
     request where `canPause` is true … at its next request boundary where it is false."
4. **The coder lets go and gives its ticket back;** `Turns._grant` then grants the text model's ticket,
   `Card._take_lock_back` takes the lock within `CARD_LOCK_WAIT_S`, and `Card.restore` loads the units
   back, saying `RESTORED`. A lock another occupant keeps past that wait makes the restore fail with no
   room, and `Card._want_back` tries again every two seconds while the `WANT` stands.
5. **The master never unloads an in-use turn of one rank for the other.** In each round,
   `Master` plans a ticket's placement and passes it through `turn_gate`: where the card's turn is in use
   and `Turn::passes_to` says it does not pass to the ticket's service — it passes only to its own
   service, a turn not in use, or the service it is told to give way to — the decision becomes a hold, its
   reason "<service>'s turn is in use". So the coder's unit is planned onto the cards only once the text
   model's turn rests on every node of the group (`group_gate`), and the text model's units come back only
   through step 2, never through a placement the master plans while the coder's turn is in use.
   - `docs/placement.md`, 「The engine's turn」: "a target whose turn is in use is never unloaded for one of
     its rank, whatever a rule names: its turn passes to that rank by its own turn values …, by resting, by
     its holder giving its ticket back once it has emptied the card itself, or by its head saying `QUIET`
     on a `PASS`, and the master unloads only once it has."

So a text-model request waits, at most, for the coder's turn to use its declared `yield_after_s` (or to
rest), plus the coder's dump and the text model's units loading back; where that passes the head's own
120-second bound, the request is refused by the head while the arbiter keeps its place in the queue.
