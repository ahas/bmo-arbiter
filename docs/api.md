# Boundary API

Every interface by which the arbiter meets anything outside its own processes: what the master and the
node arbiter serve over HTTP, the clients and the harness plugin the arbiter publishes, the files and words
a service on a node shares with the arbiter, and the contracts the arbiter itself calls. Other repositories
match against this document; the code is one realisation of it. What the contracts mean is
`docs/node-arbiter.md` and `docs/placement.md`.

The arbiter is two programs: the **master**, the `bmo-arbiter` binary, one per system, which places
services on nodes and plans every card's memory; and the **node arbiter**, the `card/` package standing as
`bmo-card.service` on every GPU node, which keeps that node's card lock, queue, claims and units. It knows
no service by name: services declare themselves, name themselves to the node arbiter and answer its
adapter verbs.

The arbiter is an upstream unit (bmo-chat's `docs/conventions.md`, 「Upstream and downstream」): a harness
takes it as its first entry point through its MCP server, with no other unit running, to take a turn on a
GPU card before it uses one. Its routes are grouped by the resource they act on, name no caller and keep
no list of callers.

## Versions

- **One version for the repository,** `0.1.0` with a revision until the first release:
  `0.1.0-rev.<n>`, the first `0.1.0` being the base and each later bump one revision. It stands in
  `VERSION`, in the workspace's manifest for every crate, in the TypeScript package, in `GET /health`, in
  the OpenAPI document and in the MCP server, and the node arbiter shows it, with the commit it was carried
  from, as its own.
- **A change to this document's routes, fields, events, codes, protocols, files, verbs or settings** raises
  the revision alone, in the same commit as this document. Major, minor and patch do not move before the
  first release.

This document states version **0.1.0-rev.10**.

## The wire format

Both programs answer every HTTP route in the common wire format of bmo (bmo-chat's
`docs/conventions.md`, 「The common wire format」 and 「API design」):

- **Keys are camelCase.** A point in time is one integer of epoch milliseconds, named `…At` or `at`; a
  span of time is one integer of milliseconds, named `…Ms`. A size is a number in the unit its name says
  (`vramMiB`, `bytes`).
- **A key in a path** is a placement's id or a ticket's id, each a signed 64-bit integer, or a name: a
  service's name, a card unit's name.
- **A page** is `{"items": [...], "more": boolean}`.
- **A field written with a question mark** may be null in an answer and may be left out of a request.
- **Every failure and every refusal** is a non-2xx status whose JSON body holds two keys: `code`, a
  kebab-case word a caller branches on, and `error`, an English sentence for a person.

| Status | Code | When |
| --- | --- | --- |
| 400 | `bad-request` | the body, the query or a key in the path is not the shape the route takes; the error names what is wrong |
| 401 | `unauthorized` | the master's token is missing or wrong |
| 404 | `no-route` | no route answers this method and path |
| 404 | `not-found` | nothing stands at the key the path names: no placement, ticket or card unit of that id or name |
| 409 | `conflict` | the member's state does not allow the change: an unload that left the unit on the card, a restore that found no room, a rest ended by a change |
| 410 | `gone` | an event stream resumed from before the window the master keeps |
| 422 | `name-rule` | a service's name is not lower-case letters, digits and hyphens |
| 422 | `bad-field` | a field breaks a rule of its own: a description's unit not of its service, or a detail that does not read; the error names it |
| 422 | `no-place` | a ticket for a service with no place among the node's occupants |
| 500 | `failed` | the program failed; the error is the failure as a sentence |
| 503 | `not-ready` | the node arbiter is starting and moves no unit yet |

## The clients and the harness plugin

The arbiter publishes its own clients and its harness plugin, and a caller uses them rather than writing
its own.

- **`docs/openapi.json`** is the contract in OpenAPI 3.1: every route of both programs with its
  parameters, its body, its answer and the codes it can answer, every body a JSON Schema, the events under
  `x-events`, the harness plugin under `x-harness` and its tools under `x-tools`. A route's tag says which
  program serves it: `master` or `node`. A body is described as its sender writes it, so a field the sender
  may leave out is not required, and a nullable field is required and may be null. It is generated from the
  arbiter's own types, and a test fails where the committed document differs from what the code generates;
  `cargo run -p bmo-arbiter-plugin --example openapi > docs/openapi.json` writes it again.
- **`bmo-arbiter-client`** (`crates/client`), the Rust crate, holds every body of this document as a type,
  calls every route of either program at the base address it is given, and reads the event stream. The
  master answers in these types, and its tests hold the node arbiter's answers to the same document.
- **`bmo-arbiter-client`** (`packages/arbiter-client`), the TypeScript package, is generated from the same
  document: its types by `openapi-typescript`, its calls by `openapi-fetch`. `bun run generate` writes the
  types again, and `bun run check` fails where they differ from the document.
- **The harness plugin, `bmo-arbiter`** (`crates/plugin`): the MCP server `bmo-arbiter mcp` starts on
  standard input and output (「The command line」), and the `bmo-arbiter` skill beside it that tells a
  harness when and how to use it. Each tool is one call of one route through the Rust client:

  | Tool | Route |
  | --- | --- |
  | `arbiter_cluster` | `GET /cluster` |
  | `arbiter_placement_request` | `POST /placements` |
  | `arbiter_placement` | `GET /placements/{placementId}` |
  | `arbiter_card` | `GET /card` |
  | `arbiter_ticket_take` | `POST /tickets` |
  | `arbiter_ticket` | `GET /tickets/{ticketId}` |
  | `arbiter_ticket_return` | `DELETE /tickets/{ticketId}` |

  A harness about to use a GPU takes a turn with `arbiter_ticket_take`, waits on `arbiter_ticket`, and
  gives it back with `arbiter_ticket_return`. A tool's input is the route's path and query parameters by
  name, and `body` for a route that takes one; a refusal comes back as a tool error whose text is the
  status, `code` and `error`. The master's tools call the master's address and the node's tools the node
  arbiter's on the harness's own machine.

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
| `CARD_NODE_URL` | the base address of the node service (「The node service」) | none; every verb that reaches a node ends with an error naming the setting |
| `CARD_NODE_TOKEN` | the node service's bearer token | empty |
| `CARD_MASTER_TOKEN` | the bearer token every route of the master takes; the MCP server and the command's `place` and `remove` send it | none: the master answers on its loopback without one |
| `CARD_RIVAL_UNITS` | user units, separated by spaces, that keep a card lock by rules of their own: `install` refuses a node where one is active, and `lend` stops, disables and removes them | none |
| `BMO_BUN` | the Bun binary a hub console command, and an adapter command whose first word is `bun`, run with | `~/.bun/bin/bun` where it stands, else `bun` |
| `BMO_HUB_CONSOLE` | the console script that reaches a node of the hub kind (「Node shells」) | none; such a node is not reached, and the error names the setting |
| `CARD_MASTER_PORT` | the loopback port of the master's API; `0` has the system choose, named in the line the master prints as it starts listening | `7440` |
| `CARD_MASTER_READ_EVERY_MS` | how often every slave is read | `2000` |
| `CARD_MASTER_DISCOVER_EVERY_MS` | how often the node service is read | `60000` |
| `CARD_INSTALL_POLL_MS` | how often the install looks at the node setup and at the arbiter's health, both alike; a check sets it to run an install quickly | the setup every 5 s, the health every second |
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
| `CARD_TURN_PORT` | the node arbiter's loopback port | `7439` |
| `CARD_TURN_HOLD_OPEN_S` | the longest an answer is held open | 20 |
| `CARD_CLAIM_POLL_S` | how often a claimant's tickets are looked at | 0.1 |
| `CARD_LEND_GIVES_WAY_S` | how long a lent hold takes to let go, its ticket's `givesWayMs` in seconds | 3 |
| `DESKTOP_READ_EVERY_S` | how often the desktop's card is read | 1 |
| `DESKTOP_BUSY_READINGS` | readings in a row that make the card the desktop's | 5 |
| `DESKTOP_BUSY_PERCENT` | the use that counts as busy | 20 |
| `DESKTOP_ROOM_MIB` | the free memory below which the card counts as busy | 4096 |
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

HTTP on `127.0.0.1:<CARD_MASTER_PORT>`, loopback alone. Where `CARD_MASTER_TOKEN` is set, every route
takes `Authorization: Bearer <token>`, `GET /health` among them, and answers `401` `unauthorized` without
it. Any other method and path is answered `404` `no-route`.

### Health

- **`GET /health`** — `200` with `ready`, `reason?` (null when ready, else why not) and `version`. The
  master is ready once it serves, which is after its first reading of the node service.

### Events

- **`GET /events`** is the master's one stream, as server-sent events. Each event's SSE `event` is its
  kind, its `id` the master's event number, and its data `{"id", "kind", "at", "data"}`, the inner `data`
  being the member as a `GET` answers it.

  | Kind | Data | Published when |
  | --- | --- | --- |
  | `cluster.changed` | the cluster, as `GET /cluster` answers it | the picture moves: a round that read anything new, a rule or a description read again, a placement's step |
  | `placement.created` | the placement | a placement is asked |
  | `placement.changed` | the placement | a placement starts, says a line, or ends |

- **The event number** is a signed 64-bit integer that only grows: the first of a run is the milliseconds
  since the epoch at its start, and each next one more, so a number an earlier run gave is never taken for
  one of this run's.
- **A reader resumes** with `Last-Event-ID` or `after`, the number of the last event it read, and narrows
  with `kinds`, a comma-separated list. A stream opened with neither starts at the moment it is opened.
- **The window** is the last 256 events, held in the master's memory. A resume from before it, or from a
  number this run never gave, is answered `410` `gone`, and the reader reads the collections again.
- **A reader asking `Accept: application/json`** is answered the same events as a page, by `after` and
  `limit` (at most 256), for a caller that polls.
- **A comment line every fifteen seconds** keeps the stream open.

### The cluster

- **`GET /cluster`** — `200` with the whole picture:

  | Field | Shape |
  | --- | --- |
  | `master` | `version`, `startedAt`, `rulesFile`, `rulesLoadedAt?`, `rulesError?`, `readEveryMs` |
  | `groups` | each `name`, `pool`, `nodeNames`, `totalMiB`, `freeMiB` |
  | `nodes` | each `name`, `role?` (the node service's as last read), `desktop`, `link` (`state` `connected` or `lost`, `sinceAt?`, `readAt?`, `lastReadMs?`, `error?`: why the last read gave no card, the shell's error or the slave's answer), `version?` (its slave's), `rules?` (`loadedAt?`, `error?`), `occupants`, `cards` |
  | `nodes[].cards` | each `index`, `name?`, `totalMiB?`, `usedMiB?`, `freeMiB?`, `units`, `turn?` (`serviceName`, `ticketId`, `waitedMs`, `inUse?`), `queue`, `claim?` (`claimantName`, `claimedAt?`), `ram` (`dumpsMiB`, `availableMiB?`, `maxMiB?`, `floorMiB?`, `dump?`), `stage` (`waiting`, `waitingMs`), `store?` (`path`, `bytes`, `extents?`, `slotsUsed`, `bytesUsed`, `error?`), `processes?` |
  | `…units` | each `name` (`<service>/<part>@<node>`), `serviceName`, `part`, `vramMiB`, `state` (`resident`, `ram`, `evicted`, `mixed`), `ramMiB`, `sinceAt?`, `evictable`, `reason?`, `loadMs?`, `unloadMs?`, `kind?` (`resident`, `on-demand`, `fill`), `detail?` (「Unit descriptions」) |
  | `…queue` | each `ticketId`, `serviceName`, `position?`, `waitedMs`, `waitMs?`, `unitName?`, `vramMiB?` |
  | `…processes` | each `pid`, `name`, `serviceName?`, `usedMiB` |
  | `declared` | the units the rules declare: each `unit` (a name or pattern), `serviceName`, `kind`, `vramMiB?` |
  | `tiers` | every occupant's tier as the nodes are handed it (`docs/placement.md`, 「Tiers」), lower first: each `name` and `tier` |
  | `services` | each service the master places: `serviceName`, `cardMiB`, `priority`, `tier?` (its declaration's own), `yieldBy` (`turn` or `process`), `spread`, `alone`, `cards?` (its ceiling, the most cards it uses at once, where it declares one), `cardsInUse` (the cards it uses now, by node name: `docs/placement.md`, 「A service's ceiling」), `nodeNames`, `instances` (each node where it stands now: `nodeName`, `address?`, `keyFile?`, `demands`, the number of demands on it), and `unsettled?`, why its placement is unsettled |
  | `placement` | the placement under way, `kind`, `serviceName`, `nodeName` and `step?`; or null |
  | `demands` | every demand the master holds, as `GET /demands/{demandId}` answers it |
  | `events` | the picture's last 200 notes, newest first: each `at`, `nodeName?`, `code`, `text` |

  A note's `code` is a kebab-case word a reader translates by; its `text` is an English sentence for a
  person (「Messages and codes」).

### Placements

A placement puts a service on a node or takes it off one (`docs/placement.md`, 「A request」). Placements
run one at a time in the order they were asked.

- **`POST /placements`** with `kind` (`place` or `remove`), `serviceName` and `nodeName` — `201` with the
  placement, `queued`; `400` `bad-request` where the body is not that shape.
- **`GET /placements/{placementId}`** — `200` with the placement, or `404` `not-found`.
- **A placement** is `id`, `kind`, `serviceName`, `nodeName`, `state` (`queued`, `running`, `done` or
  `failed`), `lines` (what the master did, one English sentence each, in order), `failure?` (why it
  failed, with the step it stopped at), `createdAt` and `endedAt?`. Its id is a signed 64-bit integer,
  each larger than the last and never below the milliseconds since the epoch at which it was asked.
- **The master keeps** the last 256 placements in its memory; one older, or one of an earlier run, is
  `not-found`. A placement a stopped master left running is undone at its next start (`request-undone`
  among the notes).

### Demands

A demand asks for cards of a service that stands on several nodes; which node answers each card is the
master's to decide (`docs/placement.md`, 「Demands」).

- **`POST /demands`** with `serviceName`, and optionally `count?` (how many cards, one where absent),
  `within?` (the id of a granted demand of the caller's it is asked within), `workKey?` (the key of the work it continues), `ref?` (shown) and `waitMs?` (how long to hold the answer
  open).
  - `201` with the demand where every card it asks is granted, `202` with the demand where it waits for
    any, with a `Retry-After` header of 1. `waitMs` holds the answer open as `GET` does.
  - `400` `bad-request`: a body that does not name a service, a `count` of nought, or a field of the wrong
    kind. `422` `no-place`: a service placed on no node. `404` `not-found`: a `within` the master does not
    hold.
- **`GET /demands`** — `200` with a page of every demand the master holds, in the order they were asked.
- **`GET /demands/{demandId}`** — `200` with the demand. `waitMs` holds the answer open only while the
  caller still waits on it: a demand `waiting`, its service `starting` on a card among them, with no card
  `taken`. Such an answer is held until a card is granted, granted again, lent, or taken by a lower tier,
  or the time runs out, when it is answered as it stands and the caller asks again; the demand itself is
  never dropped for it. A demand `granted`, `partly` or `lent`, or one with a card `taken`, is answered
  at once, whatever `waitMs` asks. `404` `not-found`, at once: a demand the master does not hold, one
  given back among them, so a new one is asked for.
- **`DELETE /demands/{demandId}`** — gives it back, every card of it: `204`, or `404` `not-found`. The
  caller alone gives a demand back; nothing else drops one.
- **A demand** is `id`, `serviceName`, `count`, `cards` (each card granted: `nodeName`, `address?` and
  `keyFile?` as the service's adapter reports them), `state` (`waiting`, `granted`, `partly` or `lent`),
  `taken?` (the claimant or lower-tier occupant that stopped the service on one of its cards), `reason?`
  (while a card waits: `code` — `starting`, `ceiling` (the service uses as many cards as its ceiling
  lets it), `in-use`, `held`, `start-failed` — and `text`, an English sentence), `waitedMs`, `within?`,
  `workKey?` and `ref?`.
- **Its id** is a positive signed 64-bit integer, each larger than the last the master gave and never
  below the milliseconds since the epoch at which it was given.

### Unit descriptions

- **`PUT /descriptions/{serviceName}`** with `units`, an object from a unit's name to its detail:
  `blocks?` (`from`, `to`, counted from 0), `output`, `mtp`, `fullAttention?`.
  - `201` with the description where the service had said none, `200` where it replaced what it said
    before: written whole to the master's store, `placements/card-units/<service>.json`; the next picture
    shows it.
  - `422` `name-rule`: a service that is not lower-case letters, digits and hyphens. `400` `bad-request`:
    a body that does not read or holds no `units` object. `422` `bad-field`: a unit whose name does not
    start with `<service>/`, or a detail that does not read. `500` `failed`: the store could not be
    written.
- What each detail means is `docs/placement.md`, 「What a service says of its units」. The folder itself is
  the master's own, and no other unit writes in it.

## What the node arbiter serves

HTTP on the node's loopback, `127.0.0.1:<CARD_TURN_PORT>`. Its routes take no token: they answer on the
node's loopback alone, where every process of the node may take a turn. Any other method and path is
answered `404` `no-route`.

### Health

- **`GET /health`** — `200` with `ready`, `reason?` and `version`, as the master's: ready once the
  arbiter's own loop runs and it can move units.

### The card

- **`GET /card`** — `200` with the card as the node arbiter keeps it:

  | Field | Shape |
  | --- | --- |
  | `version`, `desktop` | the node arbiter's version, and whether the card is a person's desktop |
  | `occupants` | the names that may be on the card, highest first, the claimants standing first first |
  | `ranks` | the same names grouped by rank, which are the tiers where the rules carry them |
  | `tiers` | each occupant's tier as the node's part of the rules carries it, by name; empty where it carries none |
  | `claim?` | the claim standing: `claimantName`, `claimedAt?` |
  | `lockHeldBy` | the pids holding the card lock |
  | `busy` | a claim stands, a request waits, a turn is in use, or the lock is held by anyone but a resting turn |
  | `queue` | every ticket, in line order, as `GET /tickets/{ticketId}` answers it |
  | `typicalHoldMs` | by service, how long its turns have held the card in use |
  | `units` | every card unit, as `GET /card-units` gives them |
  | `device?` | the card as the driver says it: `name`, `totalMiB`, `usedMiB`, `freeMiB`, and `processes?`, each `pid`, `name`, `serviceName?`, `usedMiB`, null where the driver gives no count per process |
  | `stage` | the lent owner's door: `waiting`, `waitingMs`, `graphs`, `waits`, `waitedMs` |
  | `store?` | the dump store: `path`, `bytes`, `extents?`, `slotsUsed`, `bytesUsed`, `error?` |
  | `ram` | `dumpsMiB`, `availableMiB?`, `maxMiB?`, `floorMiB?`, `dump?` (where a unit goes when no rule names its part: `ram` or `disk`) |
  | `rules` | the node's part of the rules as last read: `loadedAt?`, `error?` |

### Tickets

A ticket is a place in the node's queue for one turn on the card (`docs/node-arbiter.md`, 「The hold」).

- **`POST /tickets`** with `serviceName`, and optionally `need?` (`gpus?`, `vramMiB?`, `unitName?`; the
  unit the master places by the rules, the rest shown and weighing nothing), `ref?` (a reference such as a
  task id; shown), `givesWayMs?` (how long it takes to let go once told, more than zero), `waitedMs?` (how
  long the request it stands for has waited), `waitMs?` (how long to hold the answer open) and `rest?`
  (`true`: no request in hand, only to rest on the card once it is free).
  - `201` with the ticket where its turn is given, `202` with the ticket and its place in line where it
    waits, with a `Retry-After` header of `waitMs` in whole seconds rounded up, at least 1.
  - `400` `bad-request`: a body that does not name a service, or a field of the wrong kind. `422`
    `no-place`: the service has no place among the node's occupants.
- **`GET /tickets/{ticketId}`** — `200` with the ticket, keeping it in line. `waitMs` holds the answer
  open until its turn is given, it is told to give way, or the time runs out; `nextWaitedMs` asks whether
  a turn carries on to a next request that has waited that long, answered in `keep`, which ends its rest
  where it does. `404` `not-found`: a ticket no longer held, so a new one is asked for.
- **`PATCH /tickets/{ticketId}`** with `resting: true` — `200` with the ticket, its turn resting from now.
  `409` `conflict`: `resting: false`, since a rest ends only by `nextWaitedMs`. `404` `not-found`.
- **`DELETE /tickets/{ticketId}`** — gives the ticket back: `204`, or `404` `not-found`.
- **A ticket** is `id`, `serviceName`, `turn` (its turn is given), `inUse` (given and not resting),
  `resting`, `giveWay` (told to let go), `giveWayFor?` (the occupant or claimant it lets go for),
  `position?` and `waitMs?` (its place in line and how long it is counted to wait, while it waits),
  `claimantName?` (the claimant whose claim stands), `waitedMs` (how long it has stood, backdated by the
  `waitedMs` it was asked with), `need?`, `ref?`, `rest` and `keep?` (the answer to `nextWaitedMs`).
- **Its id** is a positive signed 64-bit integer, each larger than the last the node arbiter gave and never
  below the milliseconds since the epoch at which it was given, so a ticket from before a restart is never
  another's after it.
- **An asker that leaves gives its ticket back:** an answer that cannot be delivered does, and so does a
  held-open answer whose asker closes its connection while it is held.

### Card units

- **`GET /card-units`** — `200` with a page of every card unit, the lent owner's and every other owner's.
- **`PATCH /card-units/{unitName}`** with `state` (`evicted` or `resident`), `reason?` and `keepsTurn?` —
  moves one unit: `evicted` takes it off the card, into its owner's host memory or the dump store as the
  node's rules place it; `resident` loads it back. A lent hold lets its lock and turn go once a unit it was
  asked to unload is out, unless `keepsTurn` is true, which says more of its units go out after this one:
  a caller unloading several units of one holder sends it with every one but the last. `200` with the unit
  as it stands after.
  - A restore of a lent hold's unit whose turn is not the hold's takes its place in the card's line
    instead, standing as the head's `WANT` does (「The lending protocol」), and loads the hold's units back
    once its turn comes; it is answered `409` `conflict`, the error saying it waits its turn.
  - `409` `conflict`: an unload that left it on the card, or a restore that found no room or a claim
    standing; the error says why. `404` `not-found`: no unit of that name. `400` `bad-request`: a body
    that is not that shape. `503` `not-ready`: the arbiter is starting. `500` `failed`: the move itself
    failed, said in `error`.
- **A card unit** is `name` (`<service>/<part>`), `holderName` (the service whose engine holds it),
  `part`, `state` (`resident`, `ram`, `evicted`, `mixed`), `evictable`, `sinceAt?`, `vramMiB`, `ramMiB`,
  `reason?` (why it is out), `buffers` (each `id`, `bytes`, `state`, `clean`, `ramBytes`, and `slot?`, its
  `offset` and `length` in the dump store),
  `lastUnload?`, `lastLoad?` and `lastLower?` (each `tookMs`, `bytes`, `at`, `to?`), and `door?` (its own
  owner's door, as `stage` is the lent owner's, for an owner besides the lent one).

### The lending protocol

TCP on `CARD_ADDRESS`, open only where lend settings stand, every connection watched by TCP keep-alive
probes so a peer gone without closing ends it as a close does. Each connection opens with one line,
`<VERB> <key> [<words>]`, the key being `LLM_KEY`; a wrong key or verb is answered `REFUSED`. It is a
line protocol of its own words, outside the HTTP wire format: the `<seconds>` of `WANT` and `QUEUED` are
seconds, written as a decimal number.

| Asked | Answered | Meaning |
| --- | --- | --- |
| `HOLD <key> [local]` | `READY [<socket>]`, `REFUSED …`, `FAILED …` | holds the card for as long as the connection stays open, waiting first for a hold or a claim standing, its turn and the lock, however long that takes; `local` for an engine on the node itself, told its control socket where the node keeps a dump store; `REFUSED` where the lend service has no place among the occupants, `FAILED` where the rpc-server ends before it answers |
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
needs the card and the units cannot be unloaded, said right after `READY` where the claim began before
it.

### The turn client

- **`~/bmo-card/turn.py`** on every node the arbiter stands on, installed with it and updated by every
  install. An occupant in a process of its own imports its `TurnClient` (a ticket asked for and after,
  `keep`, `rest`, `give_back`, `state`), `hold` (the turn waited for and the lock taken), `GivingTurn` (a
  turn that gives way) and `quiet` (wait until the card stands unasked), or runs it as a command:
  `python turn.py --service <name> [--lock <path>] [--state <file>] -- <command> [<argument> …]`, which
  holds a turn and the lock around the command and exits with its code, or `3` where the service has no
  place on the node.
  While the command runs, a `SIGTERM` sent to `turn.py` is passed on to the command and `turn.py` waits
  for it to end, so a stop reaches the command once and its own way of ending runs to the end; `SIGINT`
  and `SIGHUP`, which a terminal sends to the command as well, are left to the command. A unit that runs
  it sends its stop to it alone (systemd's `KillMode=mixed`), the rest of the unit killed only once
  `turn.py` has ended.
- **Lending the card while the command runs:** with `--state <file>`, `SIGUSR1` lets the lock go and gives
  the ticket back, and `SIGUSR2` asks for a ticket again, waits for the turn as at the start and takes the
  lock back on the same descriptor the command shares. They are done one after another, and the file reads
  `held` or `lent` once each is done, `held` from the start, and is removed once the command has ended. What the command keeps on the card while the
  card is lent is the command's to take off first; the arbiter sees only that the lock is not held.
- **An occupant may carry a copy of its own** beside its code instead, or call the ticket routes through a
  published client; the contract it meets is 「Tickets」, and the installed file is what follows this
  repository's version.

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
| `~/.config/bmo-card/rules.json` | `push`, and every placement, renamed over the old | the node's part of the rules: `occupants`, `nodes`, `units`, `rules` and `tiers` (each occupant's tier, by name) |
| `~/.config/bmo-card/rules.log` | the node arbiter, one line per rules file it takes up | |
| `~/.config/bmo-card/env` | `install`, `0600` | `CARD_LOCK`, and `CARD_DESKTOP=1` on a WSL machine |
| `~/.config/bmo-card/lend.env` | `lend` alone, `0600` | the lend settings (「Settings」) |
| `~/bmo-card/VERSION` | `install` | this repository's version and the commit the files were carried from, marked where they differ from it: `0.1.0-rev.10+<commit>` or `0.1.0-rev.10+<commit>.changes` |

The node arbiter reads the occupants and rules files again within a second of a change.

### Declarations

- **The roots** are the directories `CARD_DECLARATIONS` names. In each root the master reads, again for
  every request and every round:
  - every `<service>.card.toml` its git index holds, checked-out submodules included, its adapter run from
    the file's directory; in a root that is no git work tree, every `<service>.card.toml` found under it
    instead, hidden folders left out and nothing its `.gitignore` or `.ignore` files exclude, in order of
    path;
  - the `[card]` table of each `tools/<folder>/.placed/tool.toml`, for each folder of `tools/` whose name
    starts with neither `_` nor `.` and each folder of `tools/_shared/`, its adapter run from
    `tools/<folder>`.
- **A declaration's keys:** `service`, `card_mib`, `priority`, `tier` (a number, zero or more, or absent),
  `yield_by` (`turn` or `process`, absent for `turn`), `spread` (`one` or `many`), `alone`, `adapter` (a
  list of words, not empty, or absent), `cards` (a whole number, one or more, or absent)
  (`docs/placement.md`, 「A service declares itself」). A
  declaration declares no number of seconds for how it takes turns: a `[turn]` table, whatever it holds,
  is read past and means nothing; any other key is refused.
- **Refused,** said among the notes and failing the placement that read it: a file named otherwise than
  its `service`, a `service` that is not lower-case letters, digits and hyphens, an empty `adapter`, a
  negative `tier`, `cards` of nought, `yield_by = "process"` with no `adapter`, and two declarations of
  one service across all roots.

### Adapters

- **The master runs** `<adapter words> <verb> --node <node> --placed <node>,…` from the declaration's
  directory, the verb one of `install`, `uninstall`, `start`, `stop`, `health`, `placed`. A first word
  `bun` is run as `BMO_BUN`.
- **The adapter's last line** is one JSON object for every verb: `ok`, `node`, `state` (`running`,
  `stopped`, `absent` or `failing`), `detail`, and for `placed` the `nodes` in order; `health` answering
  `running` may add `address`, where the instance answers its callers, and `keyFile`, the file its key is
  read from on the master's host. Lines before it are
  shown as what the adapter says on its way.
- **An adapter runs to its own end:** the master takes its last line whenever it comes and sets no limit
  on it. The verb runs beside the master's rounds, which go on meanwhile (`docs/placement.md`, 「Master
  and slaves」).
- **An adapter touches no role and no occupants;** it keeps its own service's placement alone.

### The rules file

- **`placements/card-rules.yaml`** in the master's data root, YAML, written by a person or by whoever
  keeps the deployment and read by the master alone, again whenever it changes. Its four parts —
  `occupants`, `nodes`, `units`, `rules` — are `docs/placement.md`, 「The rules」. An `occupants` entry is
  `name` and `tier` (a number, zero or more), and with `claims: true` also `first`, `seen_by` and
  `ram_free`; one without `claims: true` holds no other key. A file that does not read leaves the last one
  read standing and is said in `master.rulesError`.
- **`placements/card-rules.log`** — one line for every change of the file the master takes up and every
  push: the time, the file's hash, and what changed or where it went.
- **Beside them,** the master alone writes `placements/services.json` (each service's nodes in order),
  `placements/services-request.json` (the placement under way, journaled step by step),
  `placements/process-yields.json` (`stopped`, each service and node it stopped to yield the card and
  that stands there no more, with the reason and its `kind`, `yield` to be brought back or `handover` not
  to be; `docs/placement.md`, 「Yielding by the process」) and the descriptions folder (「Unit
  descriptions」).

## What the arbiter calls

### The node service

The master learns from the node service (bmo-node, `0.1.0-rev.2`) which nodes there are, how each is
reached and which services each is reserved for, and writes a node's role there. It calls the node service
only through that service's published Rust client, `bmo-node-client`, at `CARD_NODE_URL` with
`CARD_NODE_TOKEN`:

| What | Route |
| --- | --- |
| every node, by name, in the list's order | `GET /nodes`, page after page |
| how one node is reached, and its role | `GET /nodes/{nodeName}/target` |
| a node's role written: the services joined by `+`, or none | `PATCH /nodes/{nodeName}` with `role` |

A refusal or a failure of the node service is said with its status, code and sentence. Each call waits
for its answer, a refused or broken connection, or TCP keep-alive probes finding the node service gone;
no limit of the master's cuts it short. A node whose target's shell is PowerShell is not reached for a
command.

### Node shells

- **`ssh` nodes:** a command runs as `ssh <sshArgs…> bash -c '<command>'`, the target's `sshArgs` laid
  out as the node service gives them, and a file is carried by `scp -q` with each of the target's
  `sshOptions` after `-o`, `-i <keyPath>` where the target names a key (its leading `~/` the master's own
  home), `-P <port>` and `<user>@<host>:<path>`; a failed carry is tried once more.
- **`hub` nodes,** reached through their hub's console: a command runs as `<BMO_BUN> <BMO_HUB_CONSOLE>
  exec -a <agentName> <command>`, and a file is carried by `<BMO_BUN> <BMO_HUB_CONSOLE> upload <local>
  <remote> -a <agentName>`.
- **On every node** the master runs only bash commands: reading a slave
  (`curl -s http://127.0.0.1:7439/card`), moving a unit (`PATCH /card-units/{unitName}` there),
  writing the node files, the install's steps and `systemctl --user` on `bmo-card.service` and the rival
  units.
- **A command and a carry run to their own end:** the master sets no limit on either and waits for what
  the shell or the hub's console answers.

### Adapters, declarations and descriptions

The master calls each service's adapter (「Adapters」) and reads its declaration (「Declarations」); a
service hands its unit descriptions in (「Unit descriptions」). Nothing else of a service is read or run.

## The command line

`bmo-arbiter` is one binary. With no verb it is the master, serving the routes above and printing
`bmo-arbiter: answering on 127.0.0.1:<port>` once it listens. Every verb exits `0` once it has done what it
says, `1` where it ran and failed, `2` where its words do not read; what it prints is English.

| Verb | Words | What it does |
| --- | --- | --- |
| *(none)* | | stands as the master |
| `--once` | | reads every slave once, prints the cluster as JSON and ends |
| `mcp` | `[--master <url>] [--card <url>] [--token <token>]` | the harness plugin's MCP server on standard input and output (「The clients and the harness plugin」): the master's tools call `--master`, else `http://127.0.0.1:<CARD_MASTER_PORT>`, with `--token`, else `CARD_MASTER_TOKEN`; the node's tools call `--card`, else `http://127.0.0.1:<CARD_TURN_PORT>` |
| `check` | | reads the rules and says what they would place where — the claimants, each group with the nodes it chooses, the units and the rules — writing nothing; fails where a unit's `vram_mib` is more than any answering card it may stand on holds once its group's `reserve_mib` is kept back |
| `push` | `[--node <node> …]` | writes each node whose arbiter answers, or each node named, its part of the rules (「The node files」), and a line of `placements/card-rules.log` |
| `rules` | `--node <node> --json [--desktop true\|false]` | prints the part of the rules that reaches one node, the JSON a push writes; whether the machine is a desktop is its slave's to say where it answers, else the flag's, else not |
| `place` | `<service> --to <node>` | asks the running master for a placement through the client, prints its lines as they come and its failure where it fails; exits `1` where it failed |
| `remove` | `<service> --from <node>` | likewise, taking the service off the node |
| `install` | `--node <node> [--node …] [--store-gib <n>]` | stands or updates the node arbiter on each node (`docs/node-arbiter.md`, 「Installing」) |
| `uninstall` | `--node <node> [--node …] [--keep-store]` | takes the node arbiter off each node: its unit stopped, disabled and removed, and the dump store with its table unless kept |
| `lend` | `--node <node> (--settings <file> \| --off)` | writes the lend settings on a node whose arbiter stands, or takes them off, removes every rival unit there, restarts the arbiter and waits until it stands (「The node files」) |
| `occupants` | `--node <node>` | prints the node's occupants order and the last changes its arbiter took up, writing nothing |

`--data-root <dir>` and `--code-root <dir>` stand before or after the verb. A `--settings` file is the
text of `lend.env`, in which `{home}` stands for the node account's home and is written so before it is
carried.

## Messages and codes

- **Everything the arbiter says is English:** the lines a verb prints, the lines of a placement, a note's
  `text`, a unit's `reason`, an `error` field.
- **What a machine branches on is a status or a code:** an HTTP status, an error's `code`, an exit code,
  a protocol word (`READY`, `QUEUED`), a placement's `state`, a note's `code`. A reader shows a person
  words of its own language by the code, never by parsing the English.
- **Every code is a kebab-case word** naming what happened, never the node or the service it happened
  to; a note's node stands in `nodeName`, the rest in `text`. A note is no failure answered to a caller,
  so it carries `text` where an error carries `error`.
- **The codes of what the master reads off a node between two rounds:**

  | Code | What happened |
  | --- | --- |
  | `node-reconnected` | the master reaches the node again |
  | `node-lost` | the master lost the node, or has not reached it since it started; the text carries the error where there is one |
  | `card-claimed` | a claimant claimed the card |
  | `card-given-back` | a claimant gave the card back |
  | `stage-waiting` | the engine waits at the card's stage, its units out |
  | `stage-resumed` | the engine goes on after waiting at the stage |
  | `unit-unloaded-to-ram` | a unit went into its owner's host memory; the text carries the reason |
  | `unit-unloaded-to-disk` | a unit went into the dump store; the text carries the reason |
  | `unit-lowered` | a unit kept in host memory moved into the dump store |
  | `unit-loaded` | a unit came back onto the card |
  | `unit-partly-unloaded` | some of a unit's buffers are out |
- **The codes of what the master itself does and reads:**

  | Code | What happened |
  | --- | --- |
  | `rules-read` | the rules file was read again |
  | `rules-absent` | no rules file stands, so the default places the units |
  | `rules-unreadable` | the rules file does not read, so the rules read before stand |
  | `description-unreadable` | a unit description in the store does not read |
  | `ticket-held` | a ticket is held; the text carries its reason |
  | `units-unloaded` | units were unloaded to make room for a ticket |
  | `unload-failed` | the unloads for a ticket failed and are tried again |
  | `restore-waiting` | units wait to come back until their asker's units leave the card |
  | `units-restored` | units unloaded for a ticket came back |
  | `restore-failed` | units unloaded for a ticket did not come back yet and are tried again |
  | `occupant-stopped` | an occupant that yields by its process was stopped through its adapter, to yield the card or to hand it over; the text carries why |
  | `occupant-started` | an occupant that yields by its process was started through its adapter: again once what it yielded to is gone, or for a demand |
  | `occupant-stop-failed` | the adapter's `stop` failed and is tried again |
  | `occupant-start-failed` | the adapter's `start` failed and is tried again |
  | `occupant-standing` | an occupant the master had stopped stands on the node again, its `health` answering `running` or a placement of it there done, so the master keeps no stop of it there |
- **The codes of the placement:**

  | Code | What happened |
  | --- | --- |
  | `request-done` | a placement ended done |
  | `request-failed` | a placement stopped; the text carries the step and why |
  | `request-undone` | a placement a stopped master left unfinished was undone at its start |
  | `placement-unsettled` | a service's adapter and the node service disagree on its nodes at the first start |
  | `placement-drift` | what stands differs from the master's state; named and not followed |
  | `service-not-uninstalled` | narrowing a single-node service, its old node's uninstall failed |

## Admission on a shared pool

Where two services stand at one rank on a pool of nodes — a borrowing engine whose units the cards are
lent to, and a pipeline whose stages pause — whether a request of the engine is let onto the cards while
the pipeline holds them is decided at these points, in order. No measure of time decides any of them.

1. **The engine's head finds its units out and says `WANT`** on each card's hold, with the seconds the
   request has waited (「The lending protocol」). How the head's request waits for a stage is the borrowing
   engine's own, outside this repository.
   - `docs/node-arbiter.md`, 「Lending」: "A lent hold whose units are out takes a place in the queue on
     `WANT` through its own restore … The occupant holding the turn then gives way to it by its own rules:
     at once if it rests, at its request boundary if it stands at the lend service's rank, never if it
     stands above."
2. **The node arbiter queues the engine's restore at the pipeline's rank:** `Card.want` records the
   request's arrival, `Card._start_wanting` starts `Card._want_back`, which calls `Card.restore` with
   `waits`, and `Card._take_lock_back` asks `Turns.wait` for the lend service's ticket, backdated by the
   wait at the owner's door or the `WANT` seconds, saying `QUEUED <position> <seconds>` as its place moves
   and waiting without bound for its turn. The ticket names the first of the engine's units still out and
   its size (`need.unitName`, `need.vramMiB`), so the master plans room for it by the rules (step 5); the
   restore loads every unit the hold has out. A claim standing, or the units already back, ends the attempt
   (`Card._start_wanting`, `Card._want_back`).
   - `docs/placement.md`, 「The engine's turn」: "the borrowing engine queues for them through its own
     restore: a request that finds its units out has the head say `WANT` on each card, so its hold takes a
     place in each node's queue at the pipeline's rank."
3. **The node arbiter decides when the pipeline's turn passes.** The queue is ordered by rank and then by
   the arrival of the request each ticket stands for. The pipeline's turn is told to give way (`giveWay`,
   `giveWayFor`) only when a claim stands, when its turn rests, or when an occupant above it waits. To the
   engine's ticket, of its own rank, it passes at its next request boundary, where `nextWaitedMs` answers
   `"keep": false` because the engine's request stands ahead in line. How long the pipeline's turn has been
   in use does not count.
   - `docs/node-arbiter.md`, 「One rank taking turns」: "No measure of time passes a turn within a rank."
4. **The pipeline lets go and gives its ticket back;** `Turns._grant` then grants the engine's ticket,
   `Card._take_lock_back` takes the lock once whoever holds it lets go, and `Card.restore` loads the
   units back, saying `RESTORED`. A card without the memory for them makes the restore fail with no room,
   and `Card._want_back` tries again every two seconds while the `WANT` stands.
5. **The master never unloads an in-use turn of one rank for the other.** In each round, `Master` plans a
   ticket's placement and passes it through `turn_gate`: where the card's turn is in use and
   `Turn::passes_to` says it does not pass to the ticket's service — it passes only to its own service, a
   turn not in use, or the service it is told to give way to — the decision becomes a hold, its reason
   "<service>'s turn is in use". So the pipeline's unit is planned onto the cards only once the engine's
   turn rests on every node of the group (`group_gate`), and the engine's units come back only through
   step 2, never through a restore the master plans.
   - The engine's restore ticket is planned by that gate like any other: it names the unit it brings
     back, so a rule naming the pipeline's units unloads them for it — held, before the turn may pass,
     with that turn as its reason. What the master unloads there is the pipeline's; the engine's units
     come back through step 2 alone.
   - `docs/placement.md`, 「The engine's turn」: "a target whose turn is in use is never unloaded for one of
     its rank before its turn passes, whatever a rule names: its turn passes to that rank at its holder's
     request boundary …, by resting, by its holder giving its ticket back once it has emptied the card
     itself, or by its head saying `QUIET` on a `PASS` …, never on time, and the master unloads only once it
     has."

So a request of the engine waits for the pipeline's request in hand to end (or for its turn to rest),
plus the pipeline's dump and the engine's units loading back, however long that takes, the arbiter keeping
its place in the queue throughout.
