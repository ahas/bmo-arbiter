# Units, host memory, the dump store, and placing the services

The node arbiter (`docs/node-arbiter.md`) keeps more than the lock: it knows what lies in each card's
memory, one **unit** at a time, unloads each unit whole into its owner's **host memory** or the node's
**dump store** and loads it back, and places units by the **placement rules**. One arbiter on every node
does this for its own card, the **slave**; one process reads the rules for every node at once, the
**master**, which also decides which service stands on which node. This document is what the slaves,
the master, the owners of units and the services that declare themselves are written against; the wire
contracts are `docs/api.md`.

## Units

- **A unit is one thing in a card's memory that is unloaded and loaded whole,** and the least the arbiter
  and the rules move. Nothing larger is moved: unloading a unit leaves every other unit where it is, and a
  service spread over several cards is never laid out again because one of its units went.
- **A unit is named `<service>/<part>@<node>`,** the service being the one the card is lent to (its lend
  settings' `CARD_LEND_SERVICE`) or the one an owner declares, the part what its buffers are:

  | Part | What it is |
  |---|---|
  | `weights` | the blocks' weights on that card, the output and the MTP head with them on the last card |
  | `store` | the KV store and the recurrent state of those blocks, the MTP head's with them |
  | `compute` | the engine's compute buffer on that card, and its compute pools |
  | `stage` | everything one stage of a pipeline keeps on that card, unloaded whole by stopping it |

  A checkpoint of a whole-card engine (`classifier/<checkpoint>@<node>`) and a voice model
  (`voice/<model>@<node>`) are units of the rules too. A part with several buffers (the main store and the
  MTP head's, say) is one unit; each buffer has its own slot.
- **An owner is the process whose card memory the unit is,** and the owner does the copying, at its
  slave's word; the slave decides what, when and where to. Two kinds of owner unload single units: a
  card's rpc-server lending it to an engine, and an engine using its own node's card directly (a borrowing
  engine's head). Both are an engine's CUDA backend started evictable, and answer the same control socket.
  An owner that cannot unload a single unit gives its card back whole, by letting go of the lock when
  another occupant asks for the card or a claim makes it yield (`docs/node-arbiter.md`, 「Resting」), and
  its units are shown but unloaded only all together.
- **What the slave knows of a unit:** its name, its owner, its size on the card, where it is (`resident`
  on the card, `ram` in its owner's host memory, `evicted` in the dump store, or `mixed`), the host memory
  it takes while there, whether its slot holds its current contents, its slot, and how long its last
  unload, load and lowering took.

## Unloading and loading a unit

```mermaid
sequenceDiagram
  participant M as Master
  participant S as Slave
  participant O as Owner
  participant H as Head's engine
  M->>S: evict text-model/store@node-b
  S->>O: EVICT <buffer> ram, or EVICT <buffer> <store> <offset>
  O->>O: card memory → host memory, or → staging → slot; card memory given back
  O-->>S: EVICTED <buffer> <bytes> <ms> <where>
  H->>O: next use of the card
  Note over O: waits at the door
  M->>S: restore text-model/store@node-b
  S->>O: RESTORE <buffer> <store> <offset>
  O->>O: memory mapped at the same addresses, host memory or slot → card
  O-->>S: RESTORED <buffer> <bytes> <ms>
  O-->>H: the waiting use runs
```

- **The owner keeps its process, its CUDA context and every address.** Started with
  `GGML_CUDA_EVICTABLE`, its CUDA backend reserves an address range for every buffer it allocates and maps
  card memory behind it; unloading a buffer copies it out and unmaps the memory, loading maps fresh memory
  at the same addresses and copies it back. Whoever holds those addresses — the head through every tensor
  it has sent an rpc-server, an engine through its own graphs — notices nothing.
- **The stage waits, never the engine laid out again:** every use of the card — a graph computed, a
  tensor written or read, a buffer cleared or allocated — that comes while any of the owner's buffers is
  out waits at its CUDA backend's door, and runs once every one is back, so the engine goes on from the
  same token. Through an rpc-server that is the head's next command to that card; in an engine using its
  own card it is the engine's own next step.
- **The compute pools** — the memory the CUDA backend takes for an operation's temporaries — are given
  back when the first buffer goes and mapped again at the same addresses when the last comes back, so a
  captured CUDA graph's pointers stay right.
- **What a buffer is,** as the owner reads it from what was done to it: a buffer written before the first
  graph and never after is `weights`, one cleared is a `store`, the rest is `compute`.
- **To the dump store:** a buffer's contents go in chunks of 4 MiB through two pinned staging buffers of
  64 MiB taken for the transfer and given back with it (`GGML_EVICT_STAGE_MIB`), the copy from the card of
  one overlapping the disk write of the other, and the reverse on loading. A chunk of nothing but zeros is
  noted in the slot's head and neither written nor read, so a store holding a short context costs what
  that context takes. A buffer whose slot already holds what it holds — weights, which nothing writes
  after the start — is not written again; its unload is the unmapping alone.
- **To host memory:** each chunk that holds anything but zeros is copied into host memory of the owner's
  own, through the same staging, and the card's memory unmapped; loading copies them back and gives the
  host memory up. Nothing of it is pinned once the transfer ends.
- **Lowered from host memory:** a buffer kept in host memory is written to its slot, as an unload to the
  store writes it, and its host memory given up; where its slot already holds its contents it is only
  given up. The card is not touched, so a buffer is lowered whoever has the card meanwhile; it is loaded
  back from its slot afterwards.
- **The control socket:** the slave passes the owner a Unix socket in `GGML_CUDA_CONTROL`, open to the
  node account alone, which the owner's CUDA backend opens with its first evictable buffer; on it, one
  line each:
  - `LIST` — every buffer: its number, its device, its size, its slot's size, what it is (`weights`,
    `store` or `compute`), where it is (`resident`, `ram` or `evicted`), `clean` or `dirty` (whether its
    slot holds its contents) and the host memory it takes; then a `STATE` line: buffers out, whether a use
    of the card waits at the door and for how long, graphs computed, pool bytes given back, waits so far
    and their seconds; then `END`. `NOCLIENT` where the owner holds no buffer yet.
  - `EVICT <buffer> ram` — answered `EVICTED <buffer> <bytes kept> <ms> ram`.
  - `EVICT <buffer> <store> <offset>` — answered `EVICTED <buffer> <bytes written> <ms> disk`.
  - `LOWER <buffer> <store> <offset>` — answered `LOWERED <buffer> <bytes written> <ms>`.
  - `RESTORE <buffer> <store> <offset>` — from host memory where it is kept there, else from the slot;
    answered `RESTORED <buffer> <bytes> <ms>`, or `NOROOM <buffer>` where the card has not the memory,
    which leaves it out.
  - Anything wrong is `ERROR <why>`.
- **The evictable backend** is a patch of the borrowing engine's own build; the arbiter only speaks to it.
- **An owner without that backend dumps its whole engine.** A pipeline stage whose engine cannot keep its
  addresses through unloading one buffer speaks the same lines for one buffer, the whole stage, on a
  control socket of its own, lists that buffer as a `stage`, and moves it through the CUDA driver's
  process checkpoint, which takes a whole process's card memory off the card and puts it back. What the
  socket answers:
  - an unload to host memory dumps the stage's engine: the driver locks the process, copies all of its
    card memory into host memory it keeps for that process and gives the card memory back, and the
    process is stopped. Its CUDA context, its sockets, its sessions and the request it has in hand stand
    as they were, and the host memory the dump takes is the unit's;
  - a restore continues a dumped engine, the driver copying its memory back and unlocking it, and the
    request in hand goes on from the same token; a stage stopped instead starts its pipeline again; either
    answers `NOROOM` where the card has not the memory;
  - an unload the rules send to the dump store stops the whole pipeline instead, keeping nothing;
  - a lowering ends a dumped engine and the pipeline with it, the one way its host memory is given back;
  - its `STATE` line says what is true of its own door: its one buffer out or not, whether a request waits
    at the stage while it is paused and for how long, the graphs it has computed, and its waits so far and
    their seconds, with the pool bytes it keeps none of given as nought. The dumped engine's next use of
    the card waits inside the driver's lock, and a request in flight is paused there, never ended.
- **Owners besides the card's borrower** open their control socket in the arbiter's own folder,
  `~/.local/share/bmo-card/owners/<service>.sock`, open to the node account alone. The arbiter reads every
  socket there whose service has a place among the node's occupants, beside the one the card is lent
  through: their units are shown with the lent ones, keyed in the dump store under their own service, and
  the master's unload and restore of a unit go to the owner of that unit's service. A claim reaches such
  an owner through its turn, as every holder that gives way is told to.

## Host memory

- **Where a unit goes when it is unloaded is the rules'** (「The rules」): a unit's own entry, else its node
  group's `dump`, `ram` or `disk`; with neither it goes to the dump store.
- **What host memory the dumps may take:**
  - the node group's `ram_max_gib` is the most the dumps kept in host memory may take together on one
    node, every owner's on the node counted;
  - while a claim stands (`docs/node-arbiter.md`, 「Claimants」), its claimant's `ram_free` is the host
    memory the node keeps available, as the kernel counts it available to a new program; `all` keeps no
    dump in host memory at all.
  - A part that would pass either goes to the dump store instead; a part's whole size is counted, though
    only its written chunks are kept.
- **When a claim begins,** the slave unloads the hold's units within those bounds, then lowers the dumps
  kept in host memory — those whose slot already holds them first, since they cost nothing, then the
  largest — until what the claimant needs is available, and looks again every two seconds while the claim
  stands. The claimant's ticket is granted only once that first lowering is done.
- **Loading back** reads each buffer from wherever it is, host memory or its slot.
- **What an owner keeps in host memory beside its dumps** is no dump: an engine's context checkpoints in
  its rpc-servers, or conversations a pipeline keeps on a RAM disk, are that service's own. Nothing lowers
  either for a claim, and each takes from what a claim keeps available as any program's memory does.
- **A dumped pipeline stage is a dump:** a stage dumped to host memory is counted among the node's dumps,
  and is lowered for a claim by ending its engine.

## The dump store

- **One file on each node,** made by the arbiter's install and kept by the arbiter alone: no other service
  or person writes it. It lies on the node's NVMe, never on a disk that spins, at
  `~/.local/share/bmo-card/store/units.store` unless `CARD_STORE` names another path on a local NVMe.
- **Not on a disk that spins:** the install refuses a store whose filesystem stands on a rotating disk. A
  WSL machine's virtual disk always says it rotates, so there the Windows drive under it is the owner's to
  know.
- **Its size is fixed in the rules:** `--store-gib` given to the arbiter's install is the size of that
  run, which the master gives as the node's group's `store_gib` (「The rules」); else it is the node's card
  memory once over. What one card can unload at once is at most what it holds, its weights, stores and
  compute buffers together, so one card's memory is room for all of them.
- **A node whose card no service borrows keeps a small store:** only a lent card's rpc-server and a local
  hold's engine have units that go to the store, so a group of nodes with neither, such as a person's
  desktop, names a small `store_gib` and a unit that finds no slot left goes out by `YIELD`.
- **On a WSL machine the store counts against the virtual disk:** the space inside it is the Windows
  drive's, so its group's `store_gib` is kept to what its units need.
- **Made in one piece:** the install allocates it whole with `fallocate` at its size and checks with
  `filefrag` that it lies in as few extents as the filesystem allows; the slave reads the extent count
  again at every start and shows it. Where it stands at another size than the one asked for, it is made
  again, its table with it, unless the arbiter there answers a unit `evicted` or `mixed`, either of which
  may have buffers in it: then the install stops at the store and says which units, and is run again once
  they are restored. Where no arbiter answers, a first install or one stopped, nothing is known to be held
  and the store is made again.
- **Sized only by an install:** nothing else changes its size, so a store keeps its size until the next
  install on that node; the slave shows the size it stands at beside the card memory.
- **Slots:** a fixed range of the file for each unit buffer, each aligned to 2 MiB and long enough for its
  buffer and a 4 KiB head, so every slot is one contiguous range read with direct I/O. The slave keeps the
  table — unit, buffer, offset, length — in `slots.json` beside the store; a buffer whose size changed (a
  new layout, say) is given a slot again, and a slot no buffer uses is free.
- **Every slot an unload writes is found before anything of it moves:** where the store has no room for
  one, nothing of that unload goes out and it fails, `StoreFull`, saying how large a range it wanted and
  what the store holds.
- **Room is made before the store is called full,** each step only where the one before left too little,
  and never out of a slot whose buffer lies in it:
  - every slot cut to what its buffer needs now, its start kept, and every slot of the owner's that no
    buffer of it has given up;
  - then the slots of buffers on the card or in host memory whose slot does not hold their contents;
  - then those whose slot does, which costs that buffer's next unload a write.
  - A slot is judged by the owner whose service its key names, as that owner last listed its buffers; a
    slot of an owner not answering is kept as it stands.
- **What it is not:** KV paged out of the card. Nothing reads a slot while its unit runs; a slot is only
  where an unloaded unit waits.
- **Removed with the arbiter:** `bmo-arbiter uninstall --node <node>` stops and disables the unit and
  removes the store and its table, unless `--keep-store` keeps them.

## Master and slaves

- **The slave** is the arbiter on each node. On its own, and with no master reachable, it keeps the lock,
  the queue, the desktop, the occupants, and the claims: a claim evicts every evictable unit on the node at
  once and lets go of the lock for the claimant, the rest yields as it always has, and the master is told
  afterwards by what it next reads. It keeps the units as they are placed when the master is gone.
- **The master** is `bmo-card-master.service`, which runs the `bmo-arbiter` binary on one host of the
  system. It reads the rules (「The rules」) and the node service (`docs/api.md`, 「The node service」),
  reads every slave's state every two seconds through the node's shell, decides which units go and come
  back where, and tells each slave. It holds no lock and no card: losing it loses the plan, never the rules
  a slave keeps.
  - **It reads its declarations again for each request,** from the declaration roots it is given
    (「A service declares itself」), so a declaration or an adapter a deployment changed is what the next
    request runs, whichever release the master started in. A root given as a link is followed afresh each
    time.
  - **A ticket on hold is planned again at every read,** so room a card frees later reaches it; the hold is
    said once, and again only where its reason changes.
  - **A ticket stays planned at every read until its unit is on the card.**
    - A placement counts as done only while a unit it unloaded is still out waiting on that ticket;
      meanwhile the ticket is not planned again, so nothing is unloaded twice for it.
    - A placement that unloaded nothing, because the card had room at that read, marks nothing done; nor
      does one whose unloaded units came back while the ticket still stands, nor one whose units were
      unloaded not to come back, which unloads nothing again while the room it made stands.
    - Each is planned again at the next read, so a card that fills again before the ticket's turn comes —
      the other engine reloading its units — is unloaded for it once that engine's turn rests or passes,
      the ticket held with that turn as its reason meanwhile (「The engine's turn」).
  - **The unloads one read decides go out to every node at once,** so a group asking for a unit on several
    nodes in the same moment, such as a pipeline's stages, has its last card free as soon as its first,
    never one node's unload after another's.
  - **What it unloads to come back carries that in its reason:** the reason it hands the slave names the
    ticket the room is made for and that the unit comes back once it is done. Every read loads back a unit
    so unloaded whose ticket is gone from its card and on which no claim stands, every node's loads at
    once, so a group spread over several nodes has its last card back as soon as its first. What comes back
    is read off the slaves, never remembered, so a master restarted between the unload and the restore
    restores all the same, and a unit a claim or the desktop unloaded is never touched.
  - **A restore is tried only once the ticket's own units are out of the card,** not merely once the
    ticket is gone: a holder that lets its ticket go before its unit has left the card is waited for, so
    the restore never meets a card its memory still fills and is never answered `409` for it.
  - **A unit the slave has brought back itself is back:** a lent hold's units, restored by its hold's own
    restore on `WANT` (`docs/node-arbiter.md`, 「Lending」), stand on the card while the ticket they were
    unloaded for may still stand. The master reads them as back, stops waiting on that ticket for them and
    never restores them again; they go out again only by a rule planned anew, under the same conditions as
    any unload (「The engine's turn」). While a unit's own service stands in the card's line, its own turn is
    what brings it back, and the master tries no restore of it meanwhile.
  - **It makes an occupant that yields by its process go down and come back** (「Yielding by the
    process」).
  - **How often it reads:** every two seconds, and the node service once a minute;
    `CARD_MASTER_READ_EVERY_MS` and `CARD_MASTER_DISCOVER_EVERY_MS` set them otherwise.
  - **A node whose slave gives no card is lost,** and read again at every round all the same, so it is
    connected at the first read that gives one. It is said once in the notes when it is lost, a node never
    reached since the master started among them, with why: the shell's error, or what the slave answered
    where that answer is no card, as an arbiter of another release answers a route it does not serve.
  - **A node's role is the node service's as last read,** lost or connected: a role changed in the node
    service shows, and chooses the node's group, from the next discovery on.
- **The slave hands the card on only once every unit asked for is out.** The master asks for each unit
  apart, saying with every one of a holder's but the last that more of that holder's units follow
  (`keepsTurn`); the standing hold's lock and turn are let go once the last is out. Where a unit stays on
  the card, the slave keeps both, so no occupant is given a card some of the hold's memory still fills, and
  what did go out stays out. Such an unload is answered `409` with why, and the master asks for none of
  that holder's units after it.
- **An unload that fails leaves its ticket on hold,** its reason the slave's answer, said once and again
  only where it changes, and planned again at every read: the unload is tried again while the ticket
  stands, and what went out for it comes back once the ticket is gone.
- **A node alone** — a machine that is no part of a group of nodes the rules treat as one, such as a
  person's desktop — plans its own card with the same default as the master, and the master only shows
  it.
- **What a slave answers on its loopback** is `docs/api.md`, 「The card」 and 「Card units」: the node's card
  and every process using it, every unit with its state, the host memory the dumps take, the store, the
  door of the owner the card is lent through, every other owner's door on each of its own units, the queue
  and the arbiter's version; and the unload and restore of one unit the master asks for.

### Yielding by the process

- **An occupant that keeps the card with no units the arbiter can unload and asks for no turns** — a
  server that holds its model on the card from its start — yields by its process: its declaration says
  `yield_by = "process"` and names an adapter (「A service declares itself」).
- **What makes it yield,** read at every round on each node it is placed on: a claim standing there whose
  claimant's tier is lower than the occupant's, or a ticket there, waiting or holding the turn, for a
  request of an occupant of a lower tier or of the same tier, the server having stood there before the
  request. A ticket that names the card memory it needs (`need.vramMiB`) makes it yield only where the
  card's free memory, less its group's `reserve_mib`, does not hold that much.
- **Down:** the master runs the adapter's `stop` for that node alone, says `occupant-stopped`, and keeps
  the stop in `placements/process-yields.json` beside the rules, so a master restarted meanwhile still
  brings it back.
- **Back:** once nothing that made it yield stands on the node — the ticket gone, the claim ended — the
  master runs the adapter's `start` there and says `occupant-started`. A `stop` or `start` that fails is
  said once and tried again at the next round.
- **On that node alone:** a server placed on several nodes goes down only where a request asks for the
  card.
- **The slave knows nothing of it:** such a server asks no ticket, and the node's arbiter knows it only by
  the card lock its process holds and the name that process declares (`docs/api.md`, 「The service a
  process declares」). A ticket asked while it holds the lock waits behind it
  (`docs/node-arbiter.md`, 「The hold」), so the master's next read stops the server and the card is free
  before that ticket's turn is granted. A server that holds no lock leaves the card occupied until the
  master has stopped it, and a turn granted meanwhile finds it there.

### The engine's turn

- **An engine is one service's units across the nodes it spans,** such as a borrowing engine's units on
  its pool of nodes or a pipeline's stages, and on nodes two engines share the cards pass from one engine
  to the other whole: the master unloads one engine on every node and the other takes every card, never
  some of each.
- **The master makes room for a ticket only once the card's turn may pass to it.** Where the turn on the
  ticket's node is in use (`inUse`, `docs/node-arbiter.md`, 「The hold」) and does not give way to the
  ticket's service, nothing of another occupant is unloaded for it: the ticket is held, its reason that
  turn. The turn's own rules decide when it passes:
  - **a target whose turn is in use is never unloaded for one of its rank before its turn gives way,**
    whatever a rule names: its turn passes to that rank for a later request or by its own turn values
    (`docs/node-arbiter.md`, 「One rank taking turns」), by resting, by its holder giving its ticket back
    once it has emptied the card itself, or by its head saying `QUIET` on a `PASS`, and the master unloads
    only once it has;
  - one above it is made room for where the turn gives way to it;
  - a slave that says nothing of `inUse` is planned for as one whose turn is never in use.
- **A group's tickets are planned together.** A ticket naming a unit of a group (the rules' `group`, units
  useless apart) is planned only once every node where a unit of that group stands has such a ticket the
  master may plan now, or has that unit on its card already; then every one of them in the same read,
  their unloads at once. Until then each is held, its reason the nodes it waits for.
- **Two engines at one rank on a shared pool** — a borrowing engine that lends its cards and a pipeline
  whose stages pause — take the cards in equal turns:
  - **the pipeline takes the cards** only once the borrowing engine's turn rests on every node, its stages
    at once: `CARD_LEND_QUIET_S` after the borrowing engine's last computation there; at once where its
    head said `QUIET`; or once the borrowing engine's turn has been in use for its `yield_after_s` while
    the pipeline waits, the arbiter saying `PASS` and the head `QUIET` at its next request boundary
    (`docs/node-arbiter.md`, 「Lending」);
  - **the borrowing engine queues for them through its own restore:** a request that finds its units out
    has the head say `WANT` on each card, so its hold takes a place in each node's queue at the pipeline's
    rank. The pipeline's stages give way to it by their own turn values: at a request boundary, or in the
    middle of a request once their turn has been in use for the pipeline's `yield_after_s`, dumping
    themselves and giving their tickets back. Each slave then loads the borrowing engine's units back
    itself, and the master reads them as back (「Master and slaves」);
  - **a use waiting at the borrowing engine's door,** one that reached a card past the head while its
    units were out, still has the pipeline pause at once where the pipeline watches that door, as a
    backstop to the queue.
  - Where in this exchange a request is admitted or held is set out point by point in `docs/api.md`,
    「Admission on a shared pool」.

## Placing the services

- **The master is the one source of truth for which service stands on which node, and in what order.** A
  move is one request to it, and no person or other verb edits a node's role, its occupants or a service's
  own placement apart from it. It keeps:
  - each service's nodes, in asking order, in `placements/services.json` in its data root, which it alone
    writes: `services`, an object from each service to its nodes in order;
  - each node's role in the node service, written through its published client (`docs/api.md`, 「The node
    service」);
  - each node's occupants (`docs/node-arbiter.md`, 「The occupants」), derived and written whole;
  - each service's own placement, kept in step through that service's adapter.
- **The arbiter on a node takes no placement decision.** It keeps the lock, the queue and the claims by the
  occupants and the rules it is given, and goes on doing so while the master is down.
- **The arbiter and its rules are the master's own.** Every placement stands or updates the node's arbiter
  first, by its own install (`docs/node-arbiter.md`, 「Installing」), handing it the dump store size its
  group fixes, and then writes the node its part of the rules (「The rules」). No service's install stands
  the arbiter or writes its rules.

### A service declares itself

- **A declaration stands in one of two places within a declaration root, and the master keeps no list of
  services,** so a service added is one more declaration and one more adapter and nothing in the master
  changes. The roots are the directories `CARD_DECLARATIONS` names (`docs/api.md`, 「Declarations」); in
  each:
  - a committed file `<service>.card.toml` beside the service's own code, found through the root's git
    index, checked-out submodules included, and its adapter run from the file's own directory. A root that
    is no git tree, such as a deployed release copied out of its repository, holds every such file found
    under it instead, leaving out hidden folders and what the root's own ignore files exclude;
  - a `[card]` table in the manifest of a tool placed under the root's `tools/` as a tool host places it,
    each tool folder's `.placed/tool.toml`, and its adapter run from the tool's own folder.
- **Two declarations of one service, wherever each stands, are a declaration the master refuses.**
- **A declaration says:**
  - `service` — its name, the word it declares to the arbiter as `BMO_CARD_SERVICE`: lower-case letters,
    digits and hyphens; a `<service>.card.toml` file is named after it;
  - `card_mib` — the card memory it needs on a node;
  - `priority` — its standing in a node's reservation, higher first: which services may share a reserved
    node (`alone` below, 「A request」). The order on the card is the tier's, never the priority's;
  - `tier` — its tier on the card where the rules name none for it, a number of zero or more, the lower
    first (「Tiers」); absent, `2`;
  - `yield_by` — how it yields its card: `turn`, the default, by its turn and its units
    (`docs/node-arbiter.md`, 「Priority and yielding」), or `process`, by its adapter's `stop` and `start`
    (「Yielding by the process」), which needs an `adapter`;
  - `spread` — `one`, standing on a single node and moved whole, or `many`, standing on several;
  - `alone` — whether a node it stands on is reserved for it in the node service. Services that stand
    alone at one priority may share a node, which is then reserved for all of them. Beside them a reserved
    node takes only a service that does not stand alone and stands below every one of them in priority,
    and the reservation names it too, last; on the card each stands by its tier;
  - `adapter` — the command that carries its verbs, as a list of words, not empty; absent for a service
    the master orders on cards but never installs;
  - `[turn]` — how it takes turns with a service of its own rank, absent for a service that never gives
    way to one (`docs/node-arbiter.md`, 「One rank taking turns」):
    - `yield_after_s` — the seconds its turn is in use before it gives way to a waiter of its rank, zero
      or more;
    - `can_pause` — whether it gives way in the middle of a request, `true`, or at its next request
      boundary, `false`.
    - The master carries every declared `[turn]` into each node's part of the rules as its `turns` part
      (「The rules」), keyed by `service`.
- **The services declared, and the values each declares, are their own repositories' to state.**

### An adapter's verbs

- **Each verb is the adapter's command with the verb, `--node <node>` and `--placed <node>,…`,** the
  service's nodes in order once the verb is done. It may say what it does on its way, and its last line is
  one JSON object of the same shape for every verb (`docs/api.md`, 「Adapters」).
  - `install` — installs or updates the service on the node and brings the service's own placement in step
    with the placed nodes, carrying what the service keeps from a node where it already stands;
  - `uninstall` — stops and disables it there and brings its own placement in step; the installation and
    its data stay on the node;
  - `start`, `stop`, `health` — `stop` and `start` are also how a service declaring `yield_by =
    "process"` yields a node's card and takes it back (「Yielding by the process」);
  - `placed` — the nodes the service's own placement names, in order.
- **An adapter touches no role and no occupants.** It writes its own placement alone.

### A request

- **A placement is the one way a service moves:** `POST /placements` to the running master
  (`docs/api.md`, 「Placements」), which `bmo-arbiter place <service> --to <node>` and `bmo-arbiter remove
  <service> --from <node>` ask through the arbiter's client, printing what the master does as it does it
  and how it ended. The master runs placements one at a time, in the order they were asked.
- **A `place`, in order:**
  1. Refuses a node the node service does not hold; a node reserved for other services unless this one
     stands alone at their priority, or does not stand alone and stands below every service standing alone
     there; and, for an `alone` service, a node where a service stands that neither stands alone at its
     priority nor stands below it without standing alone. A service placed beside others standing alone at
     its priority joins their reservation, and one standing below them without standing alone is named in
     it after them.
  2. Stands or updates the node's arbiter and writes its part of the rules.
  3. Reserves the node where the service is `alone`, and writes the node's occupants.
  4. Adds the node last to the service's placement and runs the adapter's `install`.
  5. Asks the adapter's `health`, which has to answer `running`.
  6. For a `one` service, narrows the placement to the node, runs `uninstall` on every other node, and
     releases their reservations and rewrites their occupants. What fails here is said and not undone; the
     service already stands on the node.
- **A `remove`** runs the adapter's `uninstall` with the node left out, then takes the node out of the
  placement and rewrites its role and occupants.
- **Each step is journaled before it runs,** in `placements/services-request.json`. A failure before the
  narrowing undoes the steps already taken, in reverse — the node's `uninstall`, then the placement, the
  roles and the occupants as they stood — and the command says which step failed. A master restarted
  mid-request undoes the unfinished request at its start, from the journal.
- **A placement the master already holds is placed again freely:** placing a service on a node it stands
  on runs every step again, which is how a drift is repaired.

### What the master compares

- **On its first start it adopts what stands:** each service's nodes from its adapter's `placed`, else
  from the roles that name it. A service whose adapter and roles disagree is marked unsettled, said among
  the events, and refused a `remove` until a `place` settles it.
- **On every start it names the drift and follows none of it:** a service whose adapter places it
  elsewhere than the master's state, a node whose role is not the one the placement gives it, and a node
  whose occupants are not the derived order, each said once among the events.
- **The cluster carries the placement:** `services`, each with its declaration, its nodes and whether it
  is unsettled, and `placement`, the one under way with its step, or none.

### Occupants are derived

- **A node's order is computed, never written by hand:**
  - the desktop, where the node's machine is a person's desktop;
  - then the services standing on the node, by tier (「Tiers」), lower first, those of one tier written as
    one word joined by `=`, by name (`song=voice`);
  - then `*`, on a node reserved for no service.
  - The claimants the rules stand first come before it on every node anyway (`docs/node-arbiter.md`,
    「Claimants」), so it names none of them.
- **Written whole** wherever a placement changes it, beside the file and renamed over it, which the arbiter
  reads again within a second.

## The rules

- **The file** is `placements/card-rules.yaml` in the master's data root, written by a person or a job;
  YAML for people, read by the master alone, through a maintained YAML crate. The master reads it again
  whenever it changes and keeps every change it takes up in `placements/card-rules.log`.
- **Four parts:**
  - **`occupants`** — the occupants the rules say anything of, each by its `name` and its `tier`
    (「Tiers」). One with `claims: true` takes the card outright, and says whether it stands `first`, how
    it is `seen_by` (its `tickets`, a `containers` prefix, and only where neither sees it a unit's
    `cgroup`, whose processes claim only on the card) and the host memory it needs (`ram_free`, GiB or
    `all`); `docs/node-arbiter.md`, 「Claimants」, is what each means. One without says its tier alone.
    Every node is pushed all of the claimants.
  - **`nodes`** — groups of nodes chosen by their row in the node service, never by name: a `select` on
    the row's `role` or its machine (`desktop`), and whether they are one `pool` (cards counted as one);
    each with the memory every card keeps back for the driver and the CUDA context (`reserve_mib`), where
    its units go when unloaded (`dump`), the most host memory the dumps may take on one node
    (`ram_max_gib`), and the size in GiB of each node's dump store (`store_gib`, 「The dump store」).
  - **`units`** — each unit, or a pattern of units: its `service`, where it may stand (a `where`
    selector), its `kind` (`resident`, placed and kept on the card until the card is wanted by another;
    `on-demand`, placed when asked for and kept so after; `fill`, placed in what is left), its `priority`
    among the units of its own service,
    whether it may be put on hold (`hold`, with `max_wait_s`), its `group` (units that are useless apart,
    such as one engine's), what it must never share a card with (`apart`), where it goes when unloaded over
    its group's (`dump`), and its measured `vram_mib`, `load_s` and `unload_s` with the date they were
    measured.
  - **`rules`** — each a trigger (`when`: a unit asked for, the desktop busy), what goes (`evict`: a list of
    units, `node` for every unit on the node, `cheapest` among a pattern, or `below` an occupant), where the
    asked unit goes (`place`), and whether what went comes back once it is done (`restore`). A claim is no
    trigger here: a claimant takes the whole card by being one.
    - `below` is the desktop's eviction, which the slave carries: what stands below an occupant on the node
      gives way while the desktop is busy. A rule that names a unit asked for cannot say it, and a file
      whose request rule does is refused when it is read, rather than planned as the default.
- **Two more parts a node is written** come from the file and the declarations together, and `push` and
  `rules --node` write and print them with the rest:
  - `turns`, every declared `[turn]` keyed by its service (「A service declares itself」);
  - `tiers`, every occupant's tier keyed by its name (「Tiers」).

### Tiers

- **An occupant's tier** (`docs/node-arbiter.md`, 「Priority and yielding」) is the one its entry in the
  rules' `occupants` gives; else, for a service, the one its declaration gives; else `2`. A claimant's entry
  that gives none stands at `0`. The rules are the owner's word and stand over a declaration.
- **The tiers reach every node whole** as its `tiers` part: each claimant, each occupant the rules name and
  each declared service. The desktop, named by neither, stands at `0` on its node; the rules may give it
  another by an entry of its name.
- **Where tiers act:** on the node, the ranks of its queue (`docs/node-arbiter.md`, 「The occupants」); in
  the master, what a ticket may unload by default (「The rules」) and what yields by its process
  (「Yielding by the process」); in a placement, the order of the occupants written (「Occupants are
  derived」). A service's `priority` acts only on the reservations.
- **Shown** in the cluster, by occupant and beside each declared service (`docs/api.md`, 「The cluster」).
- **The node's part** is written as JSON to `~/.config/bmo-card/rules.json`, which its slave reads again
  within a second and logs in `rules.log`; each push is a line of `placements/card-rules.log`. A rules file
  that cannot be read leaves the one read before standing on the node, its claimants with it; a node with
  no rules file keeps no claimant.
- **With no rule for a request:** a unit asked for is placed where it fits in the free memory; where it
  does not, the cheapest set of units on that card that makes room goes, chosen among the units of
  occupants of a later tier than the asker's, of other occupants of its own tier — unloaded only once the
  turn gives way to the asker (「The engine's turn」) — and of its own service at a lower unit `priority`;
  where that is not enough either, the request is put on hold (`202`, `docs/node-arbiter.md`, 「The
  hold」). A node a claim stands on is never chosen. Only that card's units are weighed, so an occupant
  spread over several cards keeps the others.
- **Two engines at one rank on a shared pool** need rules that name what goes, since the default frees
  nothing between units of one priority: a rule for the pipeline's unit unloading the cheapest of the
  borrowing engine's units that make room, restored once the pipeline's ticket is done and planned only
  once the borrowing engine's turn rests (「The engine's turn」); and a rule for the borrowing engine's
  own request, unloading the cheapest of the pipeline's units that make room. That request is the ticket
  the engine's `WANT` raises, which names the first of its units still out; the master plans it once the
  pipeline's turn may pass to the engine and makes that room through the pipeline's own owner, so a
  pipeline whose own pause is late is still emptied for the engine. The engine's units themselves come
  back through its hold's own restore. A third service asking on such a pool
  unloads the cheapest of either engine's units that make room, and restores them once it is done.
- **Cheapest** is the least sum of each unit's unload and load time and of the time its group waits
  meanwhile; a group's resident units waiting at a stage cost that wait for every request in hand.
- **Measured values** are updated by the slaves, which see each unit's real size and times; `push` shows
  where the file's values differ from those measured, and a person writes them in.

## What a service says of its units

- **A store beside the rules,** `placements/card-units/` in the master's data root, holds one JSON file per
  service: `units`, an object from a unit's name, `<service>/<part>@<node>`, to its detail. A service
  hands its file to the master through the descriptions route (`docs/api.md`, 「Unit descriptions」), and
  the master writes it there; nothing else writes the folder.
- **A detail** is what a unit of a layered model holds on its card: its main blocks (`blocks`, first and
  last counted from 0, or none), whether the output layer (`output`) and the MTP head (`mtp`) are among
  them, and for a store how many full attention blocks it keeps the KV of (`fullAttention`, or none).
- **The master reads the folder again whenever a file in it changes,** and shows each detail beside the
  unit of that name; a file it cannot take is said once among the events and the rest stand as read. The
  master knows no service: what a description says, and when it is handed over, is the service's alone.

## The console

- **A console screen** reads the master's cluster and its event stream and nothing else, and changes
  nothing. The API is `docs/api.md`, 「The cluster」 and 「Events」.
