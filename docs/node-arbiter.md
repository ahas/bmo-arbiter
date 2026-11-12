# The node arbiter

One process on every GPU node the system puts work on, keeping the rules of that node's card lock. It
is the package in `card/`, installed by the master's `install` verb (`docs/api.md`, 「The master's
command line」), and it knows none of the services that stand on it: each occupant names itself, the
node's occupants file says which may be on the card and in what order, the placement rules declare the
occupants that take the card outright and how each is seen, and the one service that borrows the card
over the network brings its lend settings.

## What it keeps

- **The node's card lock,** an exclusive `flock` on `~/.gpu.lock` in the node account's own home: the
  one lock every occupant on the node takes the card by (「The shared card lock」). The arbiter holds it
  itself only for an occupant that is not a process of the system's: a claimant, or the desktop.
- **The occupants:** who may be on the card, from the node's own file, and their tiers, from the node's
  part of the rules (「Priority and yielding」, 「The occupants」).
- **The hold:** a queue every occupant waits in for its turn, ordered by tier, and the API it
  is asked through, which also serves as the arbiter side of an external job runner's resource
  negotiation protocol (「The hold」).
- **The units and the dump store:** what lies in the card's memory, one unloadable unit at a time, where
  each goes when it is unloaded — its owner's host memory, or the one file on the node's NVMe — and the
  rules that place them, as the slave of the master (`docs/placement.md`).
- **Its rules, each switched on by what the node has:**

  | Rule | On where | What it does |
  |---|---|---|
  | Claims | the rules declare a claimant | gives the card to a claimant before anyone, once the others have given way |
  | The desktop | the machine is a person's desktop | takes its turn for the desktop while it uses the card |
  | The keeper | always | ends a holder that declares a service with no place among the occupants |
  | The hold | always | keeps the queue and answers its API on the node's loopback |
  | Lending | lend settings stand on the node | lends the card to a borrowing engine's head over the inner network |

## Priority and yielding

The owner's covenant for the card. Every rule below realises it, on each node and in the master.

- **Every occupant has a tier,** a number of zero or more, and the lower number takes the card first. The
  covenant's tiers: a claimant that takes the card outright, such as an external job runner, `0`; the
  bench, a measurement that takes the lab for its length, `1`; every service `2`, the voice, the
  translator's engine, the training and the model engines alike. Where each occupant's tier is written is
  `docs/placement.md`, 「Tiers」; the desktop stands at `0` unless the rules give it another.
- **A higher number waits while a lower one has work.** A ticket waits behind every ticket and turn of a
  lower tier, and a turn in use gives way to a ticket of a lower tier (「Giving way」): at once where it can
  let go in the middle of its work, else at its next request boundary (「One queue of requests」). Tier `1`
  waits while tier `0` runs.
- **Within one tier the line is the order requests arrived in, and nothing passes on time.** A turn
  passes to another occupant of its own tier only at a point its holder reaches itself: its request
  boundary, where the next request in line is that occupant's; its rest; its giving the ticket back. How
  long a turn has been in use, and how long a ticket of its tier has waited, never make a turn give way;
  a request of its tier waits behind the requests ahead of it, however long they take (「One rank taking
  turns」).
- **The line is by tier, then by when the request each ticket stands for arrived** (「The hold」).
- **Yielding is the fastest path back, never a pause:** what the occupant keeps on that card comes down
  whole and comes back when its turn comes again.
  - **An owner of units** — a lent card's rpc-server, a local hold's engine, an owner on its own control
    socket such as a pipeline stage — has its units unloaded into its host memory or the node's dump
    store, as the rules place them, and loaded back once the ticket it yielded for is done
    (`docs/placement.md`, 「Unloading and loading a unit」).
  - **An occupant that asks for turns in a process of its own** takes its weights off the card and gives
    its ticket back when told to give way or at its request boundary, and asks again for its next
    request, loading them afresh.
  - **An occupant with neither,** whose declaration says it yields by its process, goes down through its
    adapter's `stop` and comes back through its `start` once nothing that made it yield stands on its node
    (`docs/placement.md`, 「Yielding by the process」). Two such occupants of one tier hand a card to each
    other only where the standing one has no demand on it, its caller lends it within a demand of its own,
    or a demand of a better rank takes the card, and the one that handed it over is not brought back by the
    other going (`docs/placement.md`, 「Demands」).
- **Only what the request needs comes down.** A ticket is asked on one node's arbiter and stands for that
  node's card; a request that needs several cards asks a ticket on each, its units one group of the rules.
  On each card the master unloads only what the ticket's unit needs there, so an occupant spread over
  several cards yields only the cards asked for and keeps the rest; where it may choose among the units of
  several occupants it may make yield, it takes those whose unload and load cost least
  (`docs/placement.md`, 「The rules」).

## The shared card lock

A node's card is shared by every service that runs a model there and by the arbiter, which holds the
card for the desktop and for claimants. They take turns by one lock, and a service that has the card
keeps its model on it until another asks for the card.

- **The lock:** an exclusive `flock` on `~/.gpu.lock` in the node's account, the same path for every
  service on that node, opened with create so whoever comes first makes it. The file's content means
  nothing. `CARD_LOCK` names it in the arbiter's own settings.
- **Taken before anything goes on the card:** a model's weights, a decision's or a generation's working
  memory. A service that has not got the lock puts nothing on the card.
- **Let go only once everything is off the card:** the weights dropped and the process's CUDA allocator
  cache emptied, so what the holder used is free for the next one. What a process may keep on the card
  without the lock is its CUDA context alone, a few hundred MiB that cannot be let go short of ending
  the process.
- **Held while its model rests there, and given up when asked:** a holder holds it through one use
  however long that use is, keeps it with its model on the card once the use ends, and lets go as soon
  as the arbiter says another occupant asks for the card (「Resting」).
- **A process that dies lets go of it,** since the kernel drops a flock with its last open file, so a
  crash never leaves the card locked.
- **Waiting is each service's own choice.** The lock says only who is on the card: every occupant but a
  claimant's task asks the arbiter for its turn first and waits in its queue, in the order the node's
  occupants give (「The hold」); how long it waits, and what it answers its own callers meanwhile, is the
  service's own to say in its own documents.
- **A process waiting on the lock is an occupant asking for the card.** A service that asks for no turn
  takes the lock itself as it comes and waits in the kernel's table; that wait is its request, and a rest
  gives way to it as to any ticket (「Resting」). The arbiter reads the waiting pid where the kernel lists
  it and the service that pid declares, and never grants it a turn: the process takes the lock itself once
  the rest is let go.
- **What a holder may assume:** no other service that follows the lock runs on the card while it holds
  the lock, so it may use the whole card less the other services' contexts and what the desktop keeps.
- **A claimant and the desktop take the lock first.** The arbiter takes it for as long as a claimant's
  claim stands (「Claimants」), or the desktop uses the card, the moment whoever holds it lets go. A holder
  is told to give way where its turn gives way or rests, and is never ended for keeping the lock: the claim
  waits until it lets go (「The keeper」).
- **What the arbiter says:** one journal line when it takes the card and why, and one when it gives it
  back, in the journal of `bmo-card.service`.

## The occupants

- **An occupant** is a claimant, the desktop, or a service, named by its own name. The arbiter sees the
  first two itself; a service names itself in its processes' environment as `BMO_CARD_SERVICE`, and
  nothing in the code keeps a list of the services. A process of a service that lends the card is named
  by the arbiter itself, as the lend service.
- **Who has a place is the node's own file,** `~/.config/bmo-card/occupants`: its words, highest first,
  one line or several, `#` starting a remark. A word is an occupant's name, several names joined by `=`
  standing at one rank (`song=voice`), or `*`, which stands for every declared service the file does not
  name, never for a claimant.
- **The ranks are the tiers** where the node's part of the rules carries them (`tiers`, 「Priority and
  yielding」): the claimants the rules stand first, then every occupant with a place by its tier, lower
  first, those of one tier at one rank, and `*` last for a service the tiers do not name. An occupant the
  tiers leave out stands at `2`, the desktop at `0`. Where the rules carry no tiers, the file's own order
  is the ranks.
- **Occupants of one rank are never ended for each other.** Their tickets stand in line by the arrival of
  the request each stands for, the other waiting with a `202` (「The hold」). The turn passes between them
  at a request boundary (「One queue of requests」), when its holder rests, or when its holder gives it back
  (「One rank taking turns」).
- **A claimant the rules stand first comes first on every node, whatever the file says.** It waits only
  while the others give way, never behind any of them (「Claimants」); a file that names it elsewhere or
  not at all is read with it first, and one added to the rules moves first within a second, with no
  restart. A claimant the rules do not stand first has the place the file gives it, and none where the
  file names it nowhere.
- **An occupant the file leaves out has no place on the node:** the desktop rule holds nothing there,
  and a holder of the lock declaring such a service is ended at once. A holder declaring nothing — a
  person's work, a training run that declares no service — stands below every occupant named and is
  never ended for being unnamed.
- **The order is the master's to write,** derived from the services placed on the node and written
  whole by every placement that changes it (`docs/placement.md`, 「Occupants are derived」): the desktop
  on a person's desktop, the node's services by their tier, those of one tier at one rank, and `*` on a
  node reserved for no service. It names no claimant, since those standing first come
  before it anyway. A node with no file at all is read as `*`.
- **Changed with no restart:** the arbiter looks at the file once a second and reads it again when it
  has changed. A file it cannot read leaves the order as it stood and says why in the journal.
- **Every change it takes up is kept:** one line in `~/.config/bmo-card/occupants.log` beside the file,
  the time, the order before and the order after, and the same line in the journal.
- **Shown by** `bmo-arbiter occupants --node <node>`: the order and the last changes the arbiter took
  up. It writes nothing; a different order is a placement's to make.

### One rank taking turns

- **Occupants of one rank take the card in the order their requests arrived.** Each ticket stands in the
  line by the arrival of the request it stands for (「One queue of requests」), and a turn passes to another
  occupant of its rank only at a point its holder reaches itself:
  - its request boundary, where the next request in line is the other occupant's: for an occupant that
    asks `nextWaitedMs`, its answer's `"keep": false`; for the lent hold, which asks none, its head saying
    `QUIET` on the `PASS` it is told (「Lending」);
  - its rest (「Resting」);
  - its giving the ticket back once it has emptied the card.
- **No measure of time passes a turn within a rank.** How long a turn has been in use and how long a
  ticket of its rank has waited make no turn give way, and nothing gives way in the middle of a request to
  an occupant of its own rank. A ticket of the holder's rank waits until the holder reaches one of the
  points above, however long that takes.
- **Only a lower tier takes the card from a turn in use:** at once where its holder lets go in the
  middle of its work, else at its next request boundary (「Giving way」); and a claim, as 「Claimants」 says.
- **A service declares no turn values,** and the rules carry none: what decides between two occupants of
  one rank is the order their requests arrived in and the points their holders reach, never a number of
  seconds.

## Claimants

- **A claimant is an occupant that takes the card outright** when it comes: nothing of the system's
  holds it back, and everything below it gives way. Which occupants are claimants, and everything about
  each, is the placement rules' `occupants` part (`docs/placement.md`, 「The rules」), pushed to every
  node; the arbiter's code names none of them.
  - **A typical claimant** is an external job runner whose agent starts jobs on the node in containers
    it names by a prefix: it stands first, is seen by its tickets, by its containers and by its processes
    on the card, and has every dump kept in host memory written out while it claims a node.
  - **A claimant added to the rules alone** is kept the same way on every node within a second of the
    rules reaching it: no code, no install, no restart.
- **A claim stands on the claimant's own word or its own work, never on what runs beside it.** It has
  three grounds: a ticket the claimant asks, a container it names, and a process of it on the card. What
  else runs on the node — under the claimant's unit or anywhere — claims nothing, since a unit runs
  whatever its account starts, a person's shell, a build or a browser among them, and none of that is
  the claimant's work on the card.
- **What the rules say of each claimant:**
  - `first` — it stands first on every node whatever the occupants file says (「The occupants」);
  - `seen_by.tickets` — a ticket it asks of the hold under its name is its claim (「The hold」); this is
    the ground a claimant is expected to use, and the default;
  - `seen_by.containers` — a running container whose name starts with this prefix, as the node's
    container runtimes list it, is its claim, and a process inside such a container is the claimant's on
    the card;
  - `seen_by.cgroup` — not set unless neither of the others can see the claimant: a process on the card
    whose cgroup lies in this unit's is the claimant's on the card, and nothing else in the unit claims.
    The unit is looked for as a system unit and as the node account's own user unit, or named outright
    by a cgroup's path;
  - `ram_free` — the host memory, in GiB, the node keeps available while it claims, or `all`, every dump
    kept in host memory written out (`docs/placement.md`, 「Host memory」).
- **The claim** begins when a ticket of the claimant is asked for, a container of it is listed, or a
  process of it is found on the card, and ends at the first look that finds none of them: its last ticket
  given back, its last container gone from the runtimes' list, its last process off the card. The tickets
  are looked at ten times a second (`CARD_CLAIM_POLL_S`), the containers and the card every two seconds,
  each reading of the runtimes and the card on a thread of its own, so a runtime slow to answer holds up
  that reading alone and the claim stands as last read meanwhile. A container running under a granted
  ticket is the same claim, never a second one, and a ticket asked while the claim stands joins it. A
  claimant with no place among the node's occupants claims nothing there.
- **A process of the claimant on the card** is a compute process the driver lists that lies in one of
  the claimant's containers, matched by the container's id against the names its runtime lists, or, where
  the rules name a cgroup, whose cgroup lies in that unit's. Where the driver cannot list the card's
  processes, the card is not read for claims.
- **Each ground claims for as long as it stands, whatever is on the card.** A container claims while it
  runs, and its end is its leaving the runtimes' list; a ticket is the claimant's own word that its work
  is held or running, and claims until its agent gives it back.
- **What the arbiter does when a claim begins:** it gives no turn to anyone else while it stands; it has
  the owner of a hold's units unload them, each where the rules put it within the host memory the
  claimant leaves, and lets go of that hold's lock, the hold itself standing (`docs/placement.md`); it
  writes out dumps kept in host memory until what the claimant needs is available, and keeps it so while
  the claim stands; where the units cannot be unloaded it tells the hold to `YIELD` and waits for it to let
  go; it tells every turn that gives way, and every resting one, to give way; and it takes the lock for the
  claim as soon as the holder lets go, however long that takes. A holder is never ended for keeping the
  lock: a turn held to its end keeps the card to that end, and a process that asks for no turns keeps it
  until it lets go. Once the claim ends it takes the hold's turn and lock again and loads the units back.
- **A claimant's ticket is granted only once the card is the claim's,** that is once the arbiter holds
  the lock for it, so every other holder has let go, and the host memory the claimant needs is
  available. Until then it is answered `202` with `Retry-After`, the wait counted as any ticket's is
  (「The hold」). Every such ticket waiting is granted together, and one asked while the card is already
  the claim's is granted at once.
- **A claimant never takes the lock itself:** the arbiter holds it for the whole claim, so a granted
  ticket never passes on for a lock not taken.
- **A local hold names the process that uses the card:** a process on the node that holds its card
  through the arbiter (`HOLD <key> local`) — a borrowing engine's head, a stage of its training — may
  send `USES <group>` on the hold, answered `USING <group>`. The arbiter keeps a local hold's lock itself,
  and a process of the group named is known on the card as the lend service's. The arbiter's own group,
  or what is no group, is refused, and a lent hold names none.
- **A claimant knows nothing of the system:** its agent knows only the general protocol and the address
  its node's setting names, and the lock, the occupants and the keeper are the arbiter's alone.

## The desktop

On a machine whose card is a person's desktop first, the arbiter takes the card for the desktop while it
uses it. Such a machine is read as one running under WSL: its card is the Windows desktop's, used by
games and the desktop whenever they like. `CARD_DESKTOP=1` switches the rule on.

- **The card is read through NVML,** opened once for the process's life and read once a second
  (`DESKTOP_READ_EVERY_S`), since a GPU tool started afresh on each reading makes the Windows graphics
  kernel stall a running game. A reading taken while a service holds the lock says what that service
  does and is passed over.
- **The card is the desktop's** after five readings in a row (`DESKTOP_BUSY_READINGS`) that each find its
  use at 20% or more (`DESKTOP_BUSY_PERCENT`), or less than 4 GiB of it free (`DESKTOP_ROOM_MIB`), and at
  once where it cannot be read at all, since its use cannot then be told. NVML that will not open is
  tried again a minute later.
- **The desktop waits its turn like any occupant in the queue,** standing where the occupants put it, and
  takes the lock when its turn comes. A holder keeps the card until it lets go, told to give way where its
  turn gives way or rests; a game does not wait for the lock, so meanwhile the holder only shares the card
  with it.
- **Given back** at the first reading that finds neither, read the same way, the lock and the turn with
  it: what the desktop runs lets its memory and its use go as it ends, and the reading that finds them
  gone is the desktop's end.
- **A claim and the desktop ask each other directly,** since they run in one process and the kernel's
  table of locks cannot tell their two holds apart: while a claim stands, a hold the desktop had is
  handed to it at once, and the card's use is the claimant's, never counted as the desktop's.

## The keeper

Once a second the arbiter reads who holds the lock and what each holder declares.

- **A holder declaring a service with no place among the occupants** is ended with its process group.
- **No holder is ended for keeping the lock.** One below a claim or the desktop is told to give way
  through its ticket where its turn gives way or rests, and keeps the lock until it lets go.
- **Each process is ended once,** known by its pid and the clock tick it started at, so a pid the kernel
  hands to a later process is that process's again. The arbiter never ends its own process group, and a
  holder whose declaration cannot be read, gone or another account's, declares nothing.

## The hold

Every occupant asks for its turn before it takes the lock, a claimant too where it is seen by its
tickets. Where the card is not its turn it is put on hold, never refused: it is told its place and how
long the wait is counted at, and it carries on once its turn comes. The wire contract is
`docs/api.md`, 「Tickets」 and 「The card」; this section is what it means.

```mermaid
sequenceDiagram
  participant O as Occupant
  participant A as Arbiter
  O->>A: POST /tickets {"serviceName": "voice", "waitMs": 20000}
  Note over A: held open until the turn is handed on
  A-->>O: 201 {"id", "turn": true}
  loop each request of its own
    O->>O: take the lock, serve one request
    O->>A: GET /tickets/<id>?nextWaitedMs=<its next request's wait>
    A-->>O: 200 {"keep": true} — carry on, weights on the card
  end
  O->>A: PATCH /tickets/<id> {"resting": true} — nothing left to serve
  A-->>O: 200 {"resting": true} — weights stay on the card
  O->>A: GET /tickets/<id>?waitMs=20000
  Note over A: held open until another asks for the card
  A-->>O: 200 {"resting": true, "giveWay": true, "giveWayFor": "classifier-train"}
  O->>O: weights off the card, the lock let go
  O->>A: DELETE /tickets/<id>
  Note over A: the next ticket's held answer goes out at once
  O->>A: POST /tickets {"serviceName": "classifier", "rest": true}
  Note over A: granted once the card stands free, to rest on it again
```

- **The resume signal is `turn: true`:** the occupant then takes the lock itself, without waiting, and holds
  it for as long as its use; a take that fails, since a process that never asks for turns took the lock
  in the moment between, is asked after again. A claimant alone takes no lock: the arbiter holds it for
  the claim (「Claimants」).
- **A turn is given back only once the card holds nothing of the occupant but its context.** The next in
  line has the card that instant (「Handing on」), while the process that gave it back may still be
  running, so an occupant empties its weights and its allocator's cache off the card first, at the end of
  its work as much as between two stretches of it; process exit is never what frees the card for the
  next.
- **An asker that leaves gives its ticket back:** a held-open answer watches the asker's connection the
  whole time it is held, and where that connection closes — the asker stopped asking, or its process
  ended — or the answer to a `POST` or `GET` about a ticket cannot be delivered, the arbiter gives that
  ticket back as a `DELETE` would, the moment it sees it. No turn is then granted, and no other owner's
  units unloaded for it, on behalf of an asker that has left.
- **The order of the line:** the ticket whose turn it is first, then by the occupants' ranks, which are
  their tiers (「The occupants」), those of one rank together, then by who asked first, and the tickets
  asking only to rest after all of them. A turn is given to the head only while no claim stands and no process holds the lock, and to
  one ticket at a time; a claimant's tickets are granted as 「Claimants」 says.
- **A ticket stands until its holder gives it back.** No silence drops it: a ticket ends by its `DELETE`;
  by an answer about it that cannot be delivered, its asker having gone; or, once its turn is given, by
  its holder letting the lock go, a holder that dies letting the lock go with it (「Handing on」). A
  claimant's ticket ends the same way, its agent giving it back once no task of its node is held or
  running.
- **A turn given stays the ticket's** until its holder takes the lock and lets it go, gives the ticket
  back, or is found gone, however long it goes untaken: the card waits for it meanwhile. An occupant that
  asks only as it needs the card finds its turn at its next call, and one that waits after its turn for
  the card's memory to take its model keeps it while it waits.
- **`403`, no place:** an occupant answers it as the refusal it is, never as a wait; the turn client run
  as a command ends with code 3 for it.
- **How a wait is counted:** each ticket ahead holds the card for as long as its service has typically
  held it in use here, a turn already given less what it has used, ten seconds for a service not yet
  seen, a resting turn its `givesWayMs` or one second, and ten more where a process that asks for no turns
  holds the lock.
- **A process that asks for no turns** still takes the lock as it comes; the queue waits behind it. **An
  occupant on a node where nothing answers at the arbiter's address** takes the lock as it comes. An
  arbiter that answers, even with a refusal or a fault, is one whose node keeps a queue: the occupant asks
  again and never takes the lock outside that queue.
- **Inside the arbiter** the desktop and a lent hold wait in the same queue as tickets of the arbiter's
  own, which it gives back itself and never drops for silence.

### Handing on

- **The card goes to the next in line the moment it is given back,** never at the next time somebody
  happens to ask. A `DELETE` from the holder grants the head of the queue there and then.
- **A waiting occupant does not poll.** It asks with `waitMs` — the body's on a `POST`, the query's on a
  `GET` — and the answer is held open until its turn is given or that time passes, at most twenty seconds
  (`CARD_TURN_HOLD_OPEN_S`).
  The held answer goes out the instant the turn is granted; past its time it says the ticket still waits,
  and the occupant asks again the same way.
- **A holder whose turn gives way, or rests, does not poll either.** Its `GET` with `waitMs` is held open
  until it is to give way — an occupant above it asking, any occupant asking while it rests, or a claim
  beginning — and goes out that instant with `"giveWay": true`; past its time it is the plain answer, and
  the holder asks again. A turn that neither gives way nor rests is answered at once.
- **A lock that frees with no `DELETE`** — a holder that died, one that asks for no turns — is found
  within a quarter of a second, and the held answers are woken then.

### One queue of requests

- **The queue is one line of requests, not of processes.** Each occupant stands in it by the oldest
  request it has waiting, placed by the occupants' order and then by that request's arrival, which its
  ticket's `waitedMs` backdates.
- **A holder asks after every request it serves whether it carries on:** `GET
  /tickets/<id>?nextWaitedMs=<ms>`, `ms` being how long its next request has waited, is answered with
  `"keep": true` while that
  request would stand first in the queue — no waiting ticket of a higher occupant and none of its own rank
  asked earlier (「One rank taking turns」) — and `"keep": false` otherwise.
  - Kept, it serves the next request with its weights still on the card: the same occupant twice in a
    row pays no move.
  - Not kept, it lets go after the request in hand and asks for a new ticket with that next request's
    `waitedMs`, taking its place in the line by it.
  - A holder with no request left rests (「Resting」).
- **Nobody waiting ahead of its next request, a turn is kept for as long as its occupant has requests.**
  No time limits it: an occupant below a busy one waits until the busy one's requests ahead of it are
  served. A ticket asking only to rest is nobody waiting.
- **A turn that gives way** keeps its own rules (「Giving way」); `nextWaitedMs` is for an occupant that serves
  requests one at a time, which finishes the request in hand and never stops mid-way.

### Giving way

- **An occupant that can let go of the card in the middle of its work** asks its ticket with
  `givesWayMs`, by its own measure of how long letting go takes it. Its turn then lasts only while nobody
  above it wants the card, so a holder that keeps the card for hours, low in the order, holds the
  occupants above it up for those seconds alone.
- **Told to give way:** while such a turn stands and a ticket of an occupant above it waits, or a claim
  stands, every
  answer to its ticket carries `"giveWay": true` and `"giveWayFor"`, the highest occupant waiting or the
  claimant. A holder that gives way keeps one ask after its ticket held open the whole time it holds the
  card (「Handing on」): on a turn of its own, the held answer goes out the instant it is to give way, so it
  hears of it without asking again and again.
- **What it does then:** at its next safe point it takes everything off the card, lets the lock go and
  gives the ticket back. When it asks again is its own choice, and its new ticket joins the line in its
  place.
- **The wait counted** for a ticket above such a holder is the holder's `givesWayMs`, not its typical
  hold, so the `Retry-After` an occupant above passes on says how long the holder takes to let go.
- **Nothing is ended for giving way slowly.** The keeper ends only a holder with no place among the
  occupants, and a claim waits for a holder to let go however long that takes.
- **A ticket asked without `givesWayMs`** holds its turn to its end, unless it rests.

### Resting

- **A holder keeps its weights on the card after its last request.** With nothing left to serve it asks
  its ticket rested (`PATCH` with `"resting": true`) and keeps its turn and the lock, its weights where they are, so its next request
  pays no move onto the card. It takes them off only when the card is wanted by another: another occupant
  asking for it, a claim beginning, or the desktop taking it.
- **A resting turn gives way to any occupant that asks,** above or below it in the order. The ranks order
  those using the card and those waiting for it; a holder with nothing to serve keeps nobody waiting
  whatever its rank. Told so with `"giveWay": true` and `"giveWayFor"`, on the held-open ask it keeps while it
  rests (「Handing on」), it takes everything off the card, lets the lock go and gives the ticket back, and
  the occupant asking has the card as soon as that is done.
  - **A process waiting on the lock asks as a ticket does,** under the service it declares: a wrapper that
    asks for no turn and waits for the lock is the occupant it names, and the rest gives way to it
    (「The shared card lock」). A process that declares no service is not an occupant and is not named.
  - **The card is one lock,** so an occupant asking for it never goes on beside a resting holder: what it
    asks for is the card's memory the resting weights stand in. An owner whose units the master unloads
    one at a time stays beside instead, by the rules' plan of the card's memory (`docs/placement.md`,
    「The rules」).
- **A request that comes while it rests** asks `nextWaitedMs` as any request does (「One queue of requests」).
  Kept, the turn is in use again and is served with its weights still on the card; not kept, the holder
  gives way and asks for a new ticket.
- **The answers say it:** every answer to a resting turn carries `"resting": true`. An occupant whose
  rest is answered without it is asking an arbiter that keeps no rests, and lets go.
- **Coming back once the card is free:** a holder that gave way while resting asks a new ticket with
  `"rest": true`, standing for no request, to rest on the card again.
  - It is granted only while no other ticket waits, no claim stands and nobody holds the lock; once
    granted, its turn rests from the start.
  - It never makes a holder give way, never stops a turn carrying on, and is not someone waiting for an
    untaken turn to pass to.
  - It stands as any ticket does, until it is given back or its asker is found gone.
  - A request that comes meanwhile gives it back and asks for a ticket of its own, standing as that
    request does.
- **What a resting turn counts for:** none of its rest is counted in how long its service typically holds
  the card; a ticket behind it waits its `givesWayMs`, or one second; and the card reads as not `busy`
  while it rests, so an occupant waiting for a quiet card is not held off by it.

### Who waits how

Each occupant chooses how it waits, inside the contract above:

- **An engine serving calls** asks for a turn as it needs the card for a call and waits for it inside the
  call for as long as the call's caller waits. Each call it serves is a request of the queue, and it
  carries on with the next while `nextWaitedMs` keeps it. Between calls it rests with its weights on the card,
  and comes back to them once the card is free after giving way.
- **A server answering calls** waits its turn for as long as its caller waits, and a turn it is granted
  may wait further, the turn its own meanwhile, until the card's free memory takes its model, so a holder
  that left its memory behind costs a wait and never an out-of-memory failure.
- **A batch of GPU work** asks for a turn before each piece and waits for it as long as it takes, holds
  the turn to the piece's end without giving way, and gives the ticket back after it, so another
  occupant's request asked meanwhile is served between two of its pieces. No place for it ends the run,
  naming the refusal.
- **A training run** waits for its turn for as long as it takes and gives way while it holds the card.
- **A lent hold** waits in the queue inside the arbiter as its lend service, saying
  `QUEUED <position> <seconds>` on the hold's connection whenever its place moves, then `READY`; the head
  waits for it for as long as it keeps the connection (「Lending」). Once its units are out, it takes its place in
  the queue again through its own restore, on the head's `WANT`, and its turn passes to one of its rank at
  its request boundary, on `PASS` (「Lending」).
- **A pipeline whose stages can pause** asks from each stage with `givesWayMs`, waits for the turn with
  the answer held open, and pauses in the middle of a request when told to give way, dumping its engine.
- **Two occupants of one rank** take the card in the order their requests arrived, each holding its turn
  to its request boundary, its rest or its giving it back (「One rank taking turns」).
- **A claimant's agent** in its negotiated mode asks under the claimant's name before a task starts and
  holds the task meanwhile; it asks after its ticket at least every ten seconds and gives it back once no
  task of its node is held or running.
- **A caller off the node** — a gateway, a daemon or a job that needs an instance of a service standing on
  several nodes — asks no ticket here: it asks the master for a demand naming the service, waits however
  long it takes for a card to be granted, and gives the demand back when its work ends; the service's own
  process asks this node's queue (`docs/placement.md`, 「Demands」).
- **The client** for an occupant in a process of its own is `turn.py`, installed beside the arbiter
  (`docs/api.md`, 「The turn client」): a ticket asked for and after, the turn waited for with the answer
  held open and the lock taken, whether a turn carries on to the next request, a turn resting and asked
  after until it is to give way, a ticket asking only to rest, and the ticket given back; run as a
  command it holds a turn and the lock around another command.

## Lending

- **A borrowing engine borrows its nodes' cards** over the inner network, and the arbiter on each of its
  nodes is what lends one: the `HOLD`, `STATE`, `QUIET`, `WANT`, `UNWANT`, `DROP`, `FETCH` and `SPACE` it
  answers (`docs/api.md`, 「The lending protocol」), the rpc-server it starts for a hold, and the pieces of
  saved slots it keeps.
- **The owner of a hold's units is started evictable,** with `GGML_CUDA_EVICTABLE` and its control socket
  `GGML_CUDA_CONTROL` at `~/.local/share/bmo-card/run/control.sock`, so its units can be unloaded and
  loaded back while the head waits at its stage (`docs/placement.md`): the rpc-server the arbiter starts
  for a hold, or, for a local hold, the engine of the process that holds it, which is told the socket on
  the hold as `READY <socket>` where the node keeps a dump store, plain `READY` where it does not. A build
  without the evictable backend ignores both.
- **It lends only with lend settings,** which the borrowing engine's install writes through the master's
  `lend` verb: the head's key (`LLM_KEY`), the address the node waits on (`CARD_ADDRESS`), the service the
  card is lent to (`CARD_LEND_SERVICE`, the name its units and its turns stand under), whether the hold is
  packed beside the lock (`CARD_PACKED`), the rpc-server that install builds (`CARD_RPC_SERVER`), its
  tensor cache (`CARD_RPC_CACHE`) and the slot pieces' folder (`CARD_SLOT_DIR`). Without them the arbiter
  opens no port on the inner network, and settings that lack any of them stop the arbiter at its start,
  saying which.
- **A hold waits, and is never refused for a card that is taken.** One asked while another hold stands,
  or while a claim stands, waits for that to end; one not packed beside the lock then waits its turn in
  the queue as the lend service, saying where it stands (「Who waits how」), and once its turn is given waits
  for whoever holds the lock to let go; and the rpc-server it starts is waited for until it answers on its
  port, or ends, which fails the hold. Each of these waits lasts for as long as the head keeps its
  connection, and ends the moment it closes. A packed hold takes no turn and no lock.
- **A head that is gone is seen to be gone:** every connection on the lend address is watched by TCP
  keep-alive probes, so a head whose machine or network went away without closing ends its connection,
  and its hold, as a close does.
- **A hold ends with its connection:** its rpc-server is asked to stop with `SIGTERM` and waited for until
  it has ended, which empties the card, and the lock is let go then. A stop of the arbiter ends it the same
  way (`docs/api.md`, 「A stop」).
- **The hold's turn follows its engine's use,** read off its units' owner with every read of the units:
  - **in use** from its grant, and again from the owner's next graph or a use of the card waiting at its
    door, until the head says `QUIET`;
  - **resting** from the head's `QUIET`, as any resting turn does (「Resting」), until the owner's next graph
    or a use at its door puts it back in use; no span without a graph rests it;
  - **`QUIET`** is a keyed line of its own, carrying nothing but the key: the head says it when its engine
    is to compute nothing for a while, with no tool, time or reason named. The arbiter reads the units
    first, so a graph computed before it counts as before it, and answers `QUIETED`; on a node with no
    turn standing for the hold it changes nothing;
  - **the count of graphs** the owner has computed stands in the units' own state beside its door
    (`docs/placement.md`, 「Master and slaves」), so a reader of the node's state sees the graphs a use is
    told by;
  - **a turn that gives way** (「Giving way」), its `givesWayMs` 3 seconds (`CARD_LEND_GIVES_WAY_S`), since
    its units unload with the engine waiting at its stage: to an occupant above it whenever one asks, and
    to any occupant while it rests. It gives way by having its units unloaded, which the master does for
    the ticket it gives way to (`docs/placement.md`, 「The engine's turn」);
  - **passed at its request boundary** to one of its rank whose request waits in line: once a ticket of
    its rank waits, the arbiter says `PASS <occupant>` on the hold, once for that turn. The head lets the
    request holding its slot finish, holds the requests after it, and says `QUIET` on every hold it keeps
    once that request ends, so the turn rests and gives way as any resting turn does. Nothing passes it on
    time.
- **`WANT` and `UNWANT`** are keyed lines of their own, as `QUIET` is: `WANT <key> <seconds>` while a
  request of the head's waits for a card whose units are out, the seconds how long the longest of them
  has waited, and `UNWANT <key>` once none does; answered `WANTED` and `UNWANTED`.
  - **A lent hold whose units are out takes a place in the queue on `WANT`** through its own restore: the
    arbiter asks the lend service's turn back as a restore does, its ticket standing by those seconds,
    says `QUEUED <position> <seconds>` on the hold while it waits, and loads the units back once the turn
    is the hold's, saying `RESTORED`. The ticket names the first of the hold's units still out and its
    size, so the master plans room for it by the rules (`docs/api.md`, 「Admission on a shared pool」).
    The occupant holding the turn then gives way to it by its own rules: at once if it rests, at its
    request boundary if it stands at the lend service's rank, never if it stands above.
  - **The seconds** count from the later of the request's arrival and its card's units going out, so a
    request held across a `PASS` stands behind the ticket the turn passed to.
  - **`UNWANT`** takes the ticket back where the units have not begun to come back; units already coming
    back come back.
  - **Units out for a claim** stay out while the claim stands: a `WANT` then waits in the queue behind it
    and changes nothing of the claim or of the return once it ends.
  - **Where no hold of the lend service stands,** or its units are all on the card, `WANT` changes
    nothing and is answered `WANTED` all the same.
- **Taken back before its units come back,** the hold's ticket stands by when the use waiting at its
  owner's door began, or by a `WANT`'s seconds where those are longer, so the request that waits is
  served before any ticket asked after it, whoever asks at its own rank meanwhile.
- **A claim makes a lent hold yield** where its claimant stands above the lend service.

## What the install reads and writes

The master's `install` verb stands the arbiter on a node (`docs/api.md`, 「The command line」);
no service's own install runs it.

- **Nothing about a node is written into this repository.** The install reads, at every run:
  - from the node's machine: whether it runs under WSL, and its card's name and size, asked of the
    driver; a machine whose driver answers for no card is refused;
  - from the node service: how the node is reached, and the service its row reserves it for;
  - from the node itself: the account's home, which the lock stands in.
  - It reads no placement rules: the size of the dump store the node's group fixes is handed to it as
    `--store-gib` by the master, which runs this install as its own step of every placement and writes
    the node its part of the rules itself.
- **What it writes from them:**
  - `~/.config/bmo-card/env` (`0600`): `CARD_LOCK`, the lock in the node account's home, and
    `CARD_DESKTOP=1` on a WSL machine;
  - `~/bmo-card/VERSION`, this repository's version and the commit the files were carried from, marked
    where the arbiter's files differ from it (`0.1.0+<commit>`, or `0.1.0+<commit>.changes`), which the
    slave shows as its version;
  - the dump store, made in one allocation where it is missing or another size (`docs/placement.md`,
    「The dump store」).
- **The lend settings** stand in `~/.config/bmo-card/lend.env` (`0600`), written by the `lend` verb alone,
    which restarts the arbiter to take them up, and left as they stand by `install`.
- **Timings** have settings of their own read from the same environment (`docs/api.md`, 「Settings」).
  None is set by the install.

## The installation

- **Where it stands:** `~/bmo-card` in the node's account, holding `card.py`, `desktop.py`, `units.py`,
  `turn.py`, its pinned `pyproject.toml`, its `VERSION` and its own venv on uv's Python 3.12; NVML's
  binding is its one dependency. Its dump store and the control sockets of its units' owners lie in
  `~/.local/share/bmo-card/`.
- **The unit,** `bmo-card.service`, a systemd user unit that restarts on any exit, starts with
  `default.target` and reads both settings files. Linger is on for the account, so it stands after a
  reboot with nobody logged in. Under WSL the driver's libraries are found where WSL keeps them. A stop
  drains it before it ends (`docs/api.md`, 「A stop」): its calls in hand are answered, and its hold ends,
  which stops the rpc-server and empties the card.
- **What it carries nothing of:** no key but the lend settings', no checkpoint, no data beyond the units
  it unloads. `bmo-arbiter uninstall --node <node>` stops, disables and removes the unit, and the dump
  store with its table unless `--keep-store`; the occupants file and its history stay with the node's
  settings.

## Installing

`bmo-arbiter install --node <node> [--node …] [--store-gib <n>]`, run again to update. The master runs
the same steps as the first of every placement (`docs/placement.md`, 「Placing the services」). For each
node:

1. Asks the node its home and makes `~/bmo-card` and `~/.config/bmo-card`.
2. Refuses a node where a rival unit runs: a user unit the master's `CARD_RIVAL_UNITS` names, which keeps
   the card lock by rules of its own. Two arbiters on one node would end each other's hold of the lock
   for a claim; the `lend` verb is what replaces such a unit (「Lending the card」).
3. Reads the machine, and refuses one without a card.
4. Carries the settings in a private `0600` file.
5. Uploads the package's files from this repository's `card/` folder, and the `VERSION`.
6. Makes the dump store at its size (`--store-gib`, which the master gives as the group's `store_gib`,
   else the card's memory once over), refusing a disk that spins, and says how many extents it lies in;
   a store at another size is made again only where the arbiter answers no unit `evicted` or `mixed`.
7. Runs the node setup (`node-setup.sh`) detached and waits for `CARD-SETUP-DONE ok` or
   `CARD-SETUP-DONE failed:<step>`, or for the setup's process to end without saying either, which stops
   the install at the setup; the setup installs uv into the account where it is missing, builds or repairs
   the venv and installs the pins, and changes nothing that already stands.
8. Installs the unit, reloads the user manager, enables and restarts it.
9. Waits until the arbiter's health answers ready (`docs/api.md`, 「Health」). A main process that ends
   before it does — the unit failed, or waiting to start again — stops the install there, naming the
   status it ended with.

### Lending the card

- **The borrowing engine's install writes its lend settings through the `lend` verb,** on a node whose
  arbiter the master has stood: the settings in their private `0600` file, every rival unit
  (`CARD_RIVAL_UNITS`) stopped, disabled and removed where it still stands, and the arbiter restarted so it
  takes them up and waited on as an install waits. A node with no arbiter is refused, with the placement
  named as the way to stand one. `--off` takes the settings off, so the card is lent no more.

## The checks

- `python3 -m unittest discover -s tests/card` on the system Python, which `cargo test` runs as one of its
  tests: the desktop rule, where a claimant's
  agent is found, the claimants and placing the rules declare, the occupants file and its history, the
  keeper, the queue and its API, resting turns and the tickets asking only to rest, a turn kept however
  long it goes untaken, an asker that leaves giving its ticket back, a claimant's tickets and the one claim
  they make with its containers and its processes on the card, a process of its unit that claims only on
  the card, a claim ending at the first look that finds nothing of it, one added to the rules alone, a
  hold waiting for the hold, the claim and the lock before it, the lend settings and which
  rules the settings switch on, the lent hold's turn following its engine's use and taken back by the
  request at its door or on `WANT`, its turn passed at its request boundary, two occupants of one rank
  taking the card in the order their requests arrived with no measure of time passing a turn, and
  units unloaded where the rules put them, every owner's dumps counted and lowered together; the ranks
  made by tier from the rules' `tiers`, a lower tier waited for and given way to, and a ticket kept until
  its holder gives it back; the arbiter started as a process, a call in hand answered as its ticket stands
  once `SIGTERM` comes, the arbiter ending with `0`, and the next arbiter answering on the same port.
- On a node: `systemctl --user status bmo-card` stands active, the journal's first line names the lock
  and the rules kept there, and `curl -s http://127.0.0.1:7439/card` there answers the card.
