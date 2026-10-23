"""The card arbiter on a GPU node (docs/node-arbiter.md): one process that keeps the rules of the node's card lock,
each switched on by what the node has, all of them under the node's occupants: who may be on the card,
highest first, as the node's own occupants file lists them, read again whenever it changes.

- Claims (docs/node-arbiter.md, 「Claimants」): an occupant the node's rules declare a claimant takes the card
  outright when a ticket of its own is asked of the hold, a container named with its prefix runs, or a
  process of its own is on the card, a container that never reaches the card counting only for so long,
  and nothing else that runs on the node claims; the node's card lock is taken for the claim for as long
  as it stands, the units of a hold below it are unloaded (or the hold told to YIELD and ended if it is
  not let go in time), dumps kept in host memory are lowered as far as the claimant needs, and its ticket
  is granted once the lock is held for it.
- The desktop (`CARD_DESKTOP=1`, desktop.py): the lock held for a person's desktop while it uses the card.
- The keeper: a holder of the lock that declares, in `BMO_CARD_SERVICE`, a service with no place among the
  occupants is ended, and one below a claimant or the desktop that has claimed the card is ended past the
  grace.
- Lending (the lend settings give `LLM_KEY` and `CARD_ADDRESS`): the card lent to the service the settings
  name, whose engine's head asks for it over the inner network. The head opens one connection and asks for
  the card with the service key; this takes the node's card lock where the card is not packed beside it,
  starts llama.cpp's rpc-server on the card (or, for the head's own card, nothing but the control socket
  its engine is to open) and says READY, and keeps both for as long as that connection stays open. When it
  closes, however it closes, the rpc-server is stopped, which empties the card, and the lock is let go
  (docs/node-arbiter.md, 「Lending」).

Run: the unit's settings files give every setting; python card.py
"""

from __future__ import annotations

import asyncio
import dataclasses
import fcntl
import hmac
import json
import math
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote

import units as units_mod

# The ports the head reaches on the inner network: this process's, and the rpc-server's while a hold
# stands.
HOLD_PORT = 50051
RPC_PORT = 50052
# A hold waits this long for the node's lock: a short call of another occupant lets go within it, and
# a card whose lock stays held past it is left out of the engine's layout rather than waited on.
LOCK_WAIT_S = float(os.environ.get("CARD_LOCK_WAIT_S", "10"))
# Where a claimant's unit is looked for, where the rules name one: a system unit on some nodes, the account's
# own user unit on others (docs/node-arbiter.md, 「Claimants」).
CGROUP_ROOT = Path("/sys/fs/cgroup")
# The occupant the arbiter itself sees on a person's desktop, as the occupants file names it, and the word
# standing for every declared service the file does not name.
DESKTOP = "desktop"
ANY = "*"
# What joins the names of one rank in the occupants file: occupants that never push each other out.
SAME_RANK = "="
OCCUPANT_NAME = re.compile(r"[a-z0-9][a-z0-9-]*")
# The order where a node keeps no occupants file: any service, the claimants that stand first before it.
DEFAULT_OCCUPANTS = (ANY,)
# Dumps kept in host memory are looked at this often while a claim stands, and lowered where the host
# memory the claimant needs is not available.
LOWER_POLL_S = 2.0
# How often the occupants file is looked at, and the lock's holders read against it.
OCCUPANTS_POLL_S = 1.0
KEEP_POLL_S = 1.0
# The hold (docs/node-arbiter.md, 「The hold」): the loopback port its API answers on, how long a ticket not asked
# after is kept, how long a turn given waits for its lock to be taken, how long an answer is held open for
# a waiter, how long a turn carries on from request to request while another waits, how often a waiter in
# this process asks again, and what a hold is counted at before a service has held the card here.
TURN_PORT = int(os.environ.get("CARD_TURN_PORT", "7439"))
TURN_KEEP_S = float(os.environ.get("CARD_TURN_KEEP_S", "30"))
TURN_TAKE_S = float(os.environ.get("CARD_TURN_TAKE_S", "5"))
TURN_HOLD_OPEN_S = float(os.environ.get("CARD_TURN_HOLD_OPEN_S", "20"))
TURN_HOLD_S = float(os.environ.get("CARD_TURN_HOLD_S", "10"))
# How often a held answer looks again at a lock that may have freed with nobody giving a ticket back.
TURN_LOOK_S = 0.25
TURN_POLL_S = 0.5
DEFAULT_HOLD_S = 10.0
# What a ticket waits behind a resting turn that said nothing of how long letting go takes it.
REST_GIVE_WAY_S = 1.0
# The lent hold's turn follows its engine's use (docs/node-arbiter.md, 「Lending」): in use while its units' owner has
# computed within this many seconds or a use waits at its door, resting after; and a turn that gives way, its
# units unloading in about this long with the engine waiting at its stage.
LEND_QUIET_S = float(os.environ.get("CARD_LEND_QUIET_S", "10"))
LEND_GIVES_WAY_S = float(os.environ.get("CARD_LEND_GIVES_WAY_S", "3"))
# What a process of a service that puts work on a card declares itself by, in its own environment.
SERVICE_VARIABLE = b"BMO_CARD_SERVICE"
# How often a claimant's tickets are looked at, and how long it stays without a ticket, a container or a
# process on the card before the node is given back.
CLAIM_POLL_S = float(os.environ.get("CARD_CLAIM_POLL_S", "0.1"))
CLAIM_SETTLE_S = float(os.environ.get("CARD_CLAIM_SETTLE_S", "10"))
# How long a claim kept by containers alone stands with no process of its claimant on the card before those
# containers stop counting, and how often the card and the containers are read for a claimant
# (docs/node-arbiter.md, 「Claimants」): a container readying its work does so well within the first, and one
# left running idle never ends.
CLAIM_IDLE_S = float(os.environ.get("CARD_CLAIM_IDLE_S", "600"))
CLAIM_CARD_S = 2.0
# How long a hold told to YIELD has to let go before its rpc-server is stopped under it: the head's
# save of the slot takes a fraction of a second.
YIELD_WAIT_S = float(os.environ.get("CARD_YIELD_WAIT_S", "5"))
# How long another occupant may keep the card lock once a claim began before it is ended: a short call
# lets go within it; a long run does not, and is resumed by its own contract once the node is given back.
LOCK_GRACE_S = float(os.environ.get("CARD_LOCK_GRACE_S", "20"))
# The container runtimes asked for the running containers, a claimant's named by its prefix.
RUNTIMES = frozenset({"docker", "podman"})
# A container's full id as it stands in the cgroup path of a process inside it.
CONTAINER_ID = re.compile(r"[0-9a-f]{64}")
START_WAIT_S = 30.0
STOP_WAIT_S = 10.0
ASK_WAIT_S = 10.0
POLL_S = 0.05
CHUNK = 1 << 20


def say(line: str) -> None:
    print(line, file=sys.stderr, flush=True)


@dataclass
class Refused(Exception):
    answer: str


@dataclass(frozen=True)
class Stat:
    """What the kernel says of a process: its process group, whether it has a controlling terminal, and the
    clock tick it started at."""
    group: int
    tty: int
    tick: int


def _stat(proc: Path, pid: int) -> Stat | None:
    """What the kernel says of a process, or None where it has gone."""
    try:
        text = (proc / str(pid) / "stat").read_text()
        fields = text[text.rfind(")") + 2:].split()
        return Stat(int(fields[2]), int(fields[4]), int(fields[19]))
    except (OSError, IndexError, ValueError):
        return None


def _argv(proc: Path, pid: int) -> list[str]:
    try:
        return [part.decode(errors="replace") for part in (proc / str(pid) / "cmdline").read_bytes().split(b"\0") if part]
    except OSError:
        return []


def container_names() -> dict[str, str]:
    """Every running container's full id and name, as each container runtime on the node lists them; a
    runtime that cannot be asked lists none."""
    names: dict[str, str] = {}
    for runtime in sorted(RUNTIMES):
        if shutil.which(runtime) is None:
            continue
        try:
            listed = subprocess.run([runtime, "ps", "--no-trunc", "--format", "{{.ID}} {{.Names}}"], capture_output=True,
                                    text=True, timeout=5, stdin=subprocess.DEVNULL)
        except (OSError, subprocess.SubprocessError):
            continue
        for line in listed.stdout.splitlines():
            id, _, name = line.strip().partition(" ")
            if id and name:
                names[id] = name
    return names


def claim_containers(containers: str, names: Callable[[], dict[str, str]] = container_names) -> list[str]:
    """The names of the running containers named with the claimant's prefix, as the runtimes list them."""
    return sorted(name for name in names().values() if name.startswith(containers))


def claim_on_card(cgroup: Path | None, containers: str | None, running: list[int] | None, root: Path = CGROUP_ROOT,
                  proc: Path = Path("/proc"), names: Callable[[], dict[str, str]] = container_names) -> list[int] | None:
    """The claimant's processes among those on the card: each in a container named with the claimant's
    prefix, its id matched against the names the runtimes list, asked only where a process on the card lies
    in a container, or, where the rules name the claimant's unit, whose cgroup lies in that unit's. Nothing
    else in the unit is the claimant's. None where the card cannot be read."""
    if running is None:
        return None
    unit = None if cgroup is None else str(cgroup)
    listed: dict[str, str] | None = None
    found = []
    for pid in running:
        try:
            lines = (proc / str(pid) / "cgroup").read_text().splitlines()
        except OSError:
            continue
        paths = [str(root) + line.split(":", 2)[2] for line in lines if line.count(":") >= 2]
        if unit is not None and any(path == unit or path.startswith(unit + "/") for path in paths):
            found.append(pid)
            continue
        ids = [id for path in paths for id in CONTAINER_ID.findall(path)]
        if not containers or not ids:
            continue
        if listed is None:
            listed = names()
        if any(listed.get(id, "").startswith(containers) for id in ids):
            found.append(pid)
    return sorted(found)


def lock_holders(lock: Path, mine: int) -> list[int]:
    """The processes other than this one holding a flock on the file, read from the kernel's table."""
    try:
        stat = lock.stat()
        table = Path("/proc/locks").read_text().splitlines()
    except OSError:
        return []
    where = f"{os.major(stat.st_dev):02x}:{os.minor(stat.st_dev):02x}:{stat.st_ino}"
    holders = []
    for fields in (line.split() for line in table):
        # A held lock reads `1: FLOCK  ADVISORY  WRITE <pid> <dev>:<inode> 0 EOF`; a waiter reads `1: -> FLOCK …`.
        if len(fields) >= 6 and fields[1] == "FLOCK" and fields[5] == where and fields[4].isdigit() and int(fields[4]) != mine:
            holders.append(int(fields[4]))
    return holders


def claim_cgroup(unit: str, root: Path = CGROUP_ROOT) -> Path | None:
    """A claimant's cgroup wherever its unit runs: a system unit, or a user unit in an account's own manager;
    a path is the cgroup named outright."""
    if unit.startswith("/"):
        return Path(unit)
    system = root / "system.slice" / unit
    if system.is_dir():
        return system
    users = sorted(root.glob(f"user.slice/user-*.slice/user@*.service/app.slice/{unit}"))
    return users[0] if users else None


def declared_service(pid: int, proc: Path = Path("/proc")) -> str | None:
    """The service a process declares itself part of in its environment, or None where it declares none
    or its environment cannot be read (gone, another account's): a process that says nothing is never
    taken for another service."""
    try:
        environ = (proc / str(pid) / "environ").read_bytes()
    except OSError:
        return None
    for entry in environ.split(b"\0"):
        name, _, value = entry.partition(b"=")
        if name == SERVICE_VARIABLE and value:
            return value.decode(errors="replace")
    return None


def start_tick(pid: int) -> int | None:
    """The clock tick a process started at, which tells it from a later process given the same pid."""
    stat = _stat(Path("/proc"), pid)
    return None if stat is None else stat.tick


def end_group(pid: int, said: str, rule: str) -> bool:
    """A process ended with its process group, never this process's own; `said` names it at its `{}`."""
    try:
        group = os.getpgid(pid)
        if group == os.getpgid(0):
            return False
        name = " ".join(_argv(Path("/proc"), pid)[:2])
        os.killpg(group, signal.SIGTERM)
        say(said.format(f"process group {group} ({name})"))
        return True
    except (ProcessLookupError, PermissionError) as fault:
        say(f"{rule}: could not end {pid}: {type(fault).__name__}")
        return False


def rank_names(word: str) -> list[str]:
    """The occupants one word of the order stands for: one name, or several of one rank joined by `=`."""
    return word.split(SAME_RANK)


def without(order: list[str], names: list[str]) -> list[str]:
    """An order with these names taken out of it, a rank they leave empty with them."""
    kept = [SAME_RANK.join(name for name in rank_names(word) if name not in names) for word in order]
    return [word for word in kept if word]


def parse_occupants(text: str) -> list[str]:
    """The occupants file read: the words of its lines, highest first, a `#` starting a remark. Each is
    an occupant's name, several names joined by `=` standing at one rank, or `*` standing for every
    declared service not named. A name twice, a `*` joined to another, or a word that is not a name is
    refused."""
    order: list[str] = []
    named: set[str] = set()
    for line in text.splitlines():
        for word in line.split("#", 1)[0].split():
            names = rank_names(word)
            for name in names:
                if not OCCUPANT_NAME.fullmatch(name) and (name != ANY or len(names) > 1):
                    raise ValueError(f"{word!r} is not an occupant's name")
                if name in named:
                    raise ValueError(f"{name} is named twice")
                named.add(name)
            order.append(word)
    return order


class Occupants:
    """Who may be on the node's card, highest first, as the node's occupants file lists them, the
    claimants the rules stand first before every other whatever the file says. The file is read again
    whenever it changes, and the order made again whenever those claimants change, with no restart; each
    change is said in the journal and kept in the history beside it, and a file that cannot be read leaves
    the order as it stood."""

    def __init__(self, path: Path, history: Path, now: Callable[[], str] = lambda: time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                 claimants: Callable[[], list[units_mod.Claimant]] = lambda: []) -> None:
        self.path = path
        self.history = history
        self.now = now
        self.claimants = claimants
        self.order: list[str] = list(DEFAULT_OCCUPANTS)
        self.seen: tuple[int, int] | None = None
        self.written: list[str] = list(DEFAULT_OCCUPANTS)
        self.firsts: list[str] = []

    def read(self) -> bool:
        """The file read again where it changed since the last reading, and the claimants standing first
        looked at; whether the order changed."""
        try:
            stat = self.path.stat()
            seen = (stat.st_mtime_ns, stat.st_size)
        except OSError:
            seen = None
        firsts = [claimant.name for claimant in self.claimants() if claimant.first]
        if seen == self.seen and firsts == self.firsts:
            return False
        if seen != self.seen:
            self.seen = seen
            try:
                self.written = parse_occupants(self.path.read_text()) if seen is not None else list(DEFAULT_OCCUPANTS)
            except (OSError, ValueError) as fault:
                say(f"occupants: {self.path} cannot be read ({fault}); the order stays {' '.join(self.order)}")
                return False
        self.firsts = firsts
        # A claimant the rules stand first comes before everyone whatever the file says: it takes the card
        # without asking, and nothing of ours can hold it back.
        if firsts and self.written[:len(firsts)] != firsts:
            say(f"occupants: {' '.join(firsts)} stand first whatever {self.path} says")
        order = [*firsts, *without(self.written, firsts)]
        if order == self.order:
            return False
        line = f"{self.now()} {' '.join(self.order) or '(none)'} -> {' '.join(order) or '(none)'}"
        say(f"occupants: {line}")
        try:
            with self.history.open("a") as kept:
                kept.write(f"{line}\n")
        except OSError as fault:
            say(f"occupants: the change could not be kept in {self.history} ({type(fault).__name__})")
        self.order = order
        return True

    def rank(self, name: str | None) -> int | None:
        """Where an occupant stands, 0 the highest, or None where the node has no place for it. A process
        declaring nothing stands below every occupant named."""
        if name is None:
            return len(self.order)
        for rank, word in enumerate(self.order):
            if name in rank_names(word):
                return rank
        if name != DESKTOP and not self.is_claimant(name) and ANY in self.order:
            return self.order.index(ANY)
        return None

    def names(self) -> list[str]:
        """Every occupant named, highest first, those of one rank side by side."""
        return [name for word in self.order for name in rank_names(word)]

    def is_claimant(self, name: str | None) -> bool:
        return any(claimant.name == name for claimant in self.claimants())

    def claims_by_ticket(self, name: str | None) -> bool:
        """Whether a ticket asked under this name claims the card, as a claimant seen by its tickets."""
        return any(claimant.name == name and claimant.tickets for claimant in self.claimants())

    def present(self, name: str) -> bool:
        return name in self.names()

    def outranks(self, claimant: str, holder: str | None) -> bool:
        """Whether a claimant of the card comes before a holder of the lock."""
        mine, theirs = self.rank(claimant), self.rank(holder)
        return mine is not None and (theirs is None or mine < theirs)

    async def run(self, poll_s: float = OCCUPANTS_POLL_S) -> None:
        while True:
            self.read()
            await asyncio.sleep(poll_s)


class Keeper:
    """The order kept on the lock's holders once a second. A holder declaring a service the node has no
    place for is ended at once; a holder below an occupant that has claimed the card — a claimant, the
    desktop — is ended once the claim has stood the grace. A holder declaring nothing stands below every
    occupant named. Each process is ended once, known by its pid and the tick it started at, so a pid the
    kernel hands to a later process is that process's again."""

    def __init__(self, occupants: Occupants, lock: Path, claims: Callable[[], list[tuple[str, float]]],
                 grace_s: float = LOCK_GRACE_S, poll_s: float = KEEP_POLL_S,
                 holders: Callable[[Path, int], list[int]] | None = None,
                 declared: Callable[[int], str | None] = declared_service,
                 end: Callable[[int, str, str], bool] = end_group,
                 started: Callable[[int], int | None] | None = None,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self.occupants = occupants
        self.lock = lock
        self.claims = claims
        self.grace_s = grace_s
        self.poll_s = poll_s
        self.holders = holders or lock_holders
        self.declared = declared
        self.end = end
        self.started = started or start_tick
        self.clock = clock
        self.ended: set[tuple[int, int | None]] = set()

    def step(self) -> list[int]:
        """One reading of the lock's holders; the processes ended by it."""
        claims = [(name, since) for name, since in self.claims() if self.occupants.present(name)]
        ended = []
        for pid in self.holders(self.lock, os.getpid()):
            service = self.declared(pid)
            which = (pid, self.started(pid))
            if which in self.ended:
                continue
            said = None
            if service is not None and self.occupants.rank(service) is None:
                said = f"occupants: ended {{}} of {service}, which has no place on this node"
            else:
                for name, since in claims:
                    if self.clock() - since >= self.grace_s and self.occupants.outranks(name, service):
                        said = f"{name}: ended {{}} of {service or 'no declared service'}, which kept the card lock past {self.grace_s:g} s"
                        break
            if said is None:
                continue
            self.ended.add(which)
            if self.end(pid, said, "occupants"):
                ended.append(pid)
        return ended

    async def run(self) -> None:
        while True:
            self.step()
            await asyncio.sleep(self.poll_s)


@dataclass
class Ticket:
    """One occupant waiting for the card, or given its turn: who, where it stands, when it asked, when it
    was last heard from, and when its turn came."""
    id: int
    service: str
    asked: float
    heard: float
    granted: float | None = None
    # When a turn given to it last passed on untaken, which lets the next in line before it once.
    passed: float | None = None
    # A ticket of this process's own — the desktop's, a lent hold's — which takes the lock at once when
    # its turn comes and is given back by this process, never dropped for silence.
    local: bool = False
    # What the occupant said it will use and its own reference, shown in the state and weighing nothing.
    need: dict | None = None
    ref: str | None = None
    # The seconds the occupant takes to let go once told, where its turn gives way (docs/node-arbiter.md,
    # 「Giving way」); None for a turn held to its end.
    gives_way_s: float | None = None
    # A ticket asked with no request in hand, only to rest on a free card (docs/node-arbiter.md, 「Resting」).
    rest: bool = False
    # A turn with nothing left to serve, its weights kept on the card, since when; and since when the turn
    # has been in use, from its grant or from the request that ended its rest.
    resting: bool = False
    rested_at: float | None = None
    busy_since: float | None = None


class Turns:
    """The queue every occupant waits in for the card (docs/node-arbiter.md, 「The hold」). The head of the
    queue, by the occupants' order and then by who asked first, is given its turn once no claim stands and
    nothing holds the lock; it then takes the lock itself and gives the ticket back once it has let go. A
    ticket not asked after for its keep is dropped, so a caller that died holds nobody up; a turn not
    taken up within its take is passed on where another ticket waits, and kept where none does. How long
    a wait will be is counted from how long each service has held the card before.

    The queue is one of requests (docs/node-arbiter.md, 「One queue of requests」): a ticket stands by the arrival
    of the request it is for, and a holder asks after each request whether its next one still stands
    first. A waiter may have its answer held open until its turn is given, which happens the moment the
    holder gives its ticket back (「Handing on」).

    A claimant's tickets stand apart (docs/node-arbiter.md, 「Claimants」): while one is in the queue nobody else is
    given a turn, and every one is granted once `claim_holds` says the lock is held for the claim. The lock
    is the arbiter's, so such a ticket never passes on for a lock not taken, and it is dropped for silence
    granted or not.

    A turn with nothing left to serve rests (docs/node-arbiter.md, 「Resting」): it keeps the lock with its weights on
    the card and gives way to any ticket that waits for a request, whatever its rank. A ticket asked only to
    rest stands after every other, is granted only to a card nobody else wants, and makes nobody give way."""

    def __init__(self, occupants: Occupants, lock: Path, claimed: Callable[[], str | None],
                 holders: Callable[[Path, int], list[int]] | None = None, clock: Callable[[], float] = time.monotonic,
                 keep_s: float = TURN_KEEP_S, take_s: float = TURN_TAKE_S,
                 claim_holds: Callable[[], bool] = lambda: False, claim_wait_s: float = LOCK_GRACE_S,
                 hold_s: float = TURN_HOLD_S, hold_open_s: float = TURN_HOLD_OPEN_S,
                 turn_values: Callable[[str], units_mod.Turn | None] = lambda service: None) -> None:
        self.occupants = occupants
        self.lock = lock
        # The claimant whose claim stands, or None.
        self.claimed = claimed
        self.claim_holds = claim_holds
        # The longest wait a claimant's ticket is told: the keeper ends a holder below it within it.
        self.claim_wait_s = claim_wait_s
        self.holders = holders or lock_holders
        self.clock = clock
        self.keep_s = keep_s
        self.take_s = take_s
        self.hold_s = hold_s
        self.hold_open_s = hold_open_s
        # Each service's own turn values, None for one that declares none (docs/node-arbiter.md, 「One rank taking turns」).
        self.turn_values = turn_values
        self.tickets: dict[int, Ticket] = {}
        self.typical: dict[str, float] = {}
        # The last ticket id given: each next one larger, and never below the epoch milliseconds it is given at.
        self.last_id = 0
        self.guard = threading.Lock()
        # Woken whenever a turn may have changed hands, so an answer held open goes out at once.
        self.changed = threading.Condition(self.guard)

    def _order(self) -> list[Ticket]:
        """The queue in its order: the ticket whose turn it is first, then by the occupants' order, then by
        who asked first, the tickets asked only to rest after every other."""
        def where(ticket: Ticket) -> tuple[int, int, int, float]:
            rank = self.occupants.rank(ticket.service)
            return (0 if ticket.granted is not None else 1, 1 if ticket.rest and ticket.granted is None else 0,
                    len(self.occupants.order) + 1 if rank is None else rank, ticket.asked)
        return sorted(self.tickets.values(), key=where)

    def _tidy(self) -> None:
        now = self.clock()
        held = bool(self.holders(self.lock, os.getpid()))
        for ticket in list(self.tickets.values()):
            if ticket.local:
                continue
            if self.occupants.claims_by_ticket(ticket.service):
                # A claimant asks after its ticket while it uses the grant too, and the lock it stands on is ours.
                if now - ticket.heard > self.keep_s:
                    del self.tickets[ticket.id]
                continue
            # A turn being used asks after nothing until it gives the ticket back: it stays while the lock is held.
            if ticket.granted is not None and held:
                continue
            if now - ticket.heard > self.keep_s:
                del self.tickets[ticket.id]
            elif ticket.granted is not None and now - ticket.granted > self.take_s and self._others_wait(ticket):
                # Given its turn and never took the lock: its turn passes on, to the next in line before it.
                ticket.granted = None
                ticket.passed = now

    def _asking(self) -> list[Ticket]:
        """The tickets waiting for a turn for a request, in the queue's order: those asked only to rest wait
        for nobody."""
        return [ticket for ticket in self._order() if ticket.granted is None and not ticket.rest]

    def _others_wait(self, ticket: Ticket) -> bool:
        """Whether a ticket other than this one waits for a turn, which an untaken turn could pass to."""
        return any(other is not ticket and other.granted is None and not other.rest for other in self.tickets.values())

    def _grant(self) -> None:
        claiming = [ticket for ticket in self.tickets.values() if self.occupants.claims_by_ticket(ticket.service)]
        if claiming:
            if self.claim_holds():
                now = self.clock()
                for ticket in claiming:
                    if ticket.granted is None:
                        ticket.granted = now
            return
        if self.claimed() is not None or any(ticket.granted is not None for ticket in self.tickets.values()):
            return
        if self.holders(self.lock, os.getpid()):
            return
        now = self.clock()
        # A ticket asked only to rest has the card only where nobody asks for it for a request.
        order = self._asking() or self._order()
        # A ticket whose turn just passed lets one ready to take the card before it, and nobody else.
        ready = [ticket for ticket in order if ticket.passed is None or now - ticket.passed > self.take_s] or order
        if ready:
            granted = ready[0]
            granted.granted = now
            if granted.rest:
                granted.resting, granted.rested_at, granted.busy_since = True, now, None
            else:
                granted.busy_since = now
            self.changed.notify_all()

    def _rank(self, ticket: Ticket) -> int:
        rank = self.occupants.rank(ticket.service)
        return len(self.occupants.order) + 1 if rank is None else rank

    def _turn_spent(self, ticket: Ticket) -> units_mod.Turn | None:
        """A turn's own turn values where it has been in use for their `yield_after_s`, counted as the bound
        counts it (docs/node-arbiter.md, 「One rank taking turns」); None where its service declares none, it rests,
        or its time has not come."""
        turn = self.turn_values(ticket.service)
        if turn is None or ticket.granted is None or ticket.resting:
            return None
        used = self.clock() - (ticket.busy_since if ticket.busy_since is not None else ticket.granted)
        return turn if used >= turn.yield_after_s else None

    def _keeps(self, ticket: Ticket, next_waited_s: float) -> bool:
        """Whether a holder's next request still stands first in the queue: no claim stands, no waiting
        ticket of a higher occupant or of its own rank asked before that request, the turn not held past
        its bound while anybody waits, nor past its own turn values while one of its rank does (docs/node-arbiter.md,
        「One queue of requests」, 「One rank taking turns」). Both count the turn's use, from its grant or from
        the request that ended its rest."""
        if ticket.granted is None or self.claimed() is not None:
            return False
        waiting = [other for other in self._asking() if other is not ticket]
        if not waiting:
            return True
        now = self.clock()
        if not ticket.resting and now - (ticket.busy_since if ticket.busy_since is not None else ticket.granted) >= self.hold_s:
            return False
        if self._turn_spent(ticket) is not None and any(self._rank(other) == self._rank(ticket) for other in waiting):
            return False
        mine = (self._rank(ticket), now - max(0.0, next_waited_s))
        return all((self._rank(other), other.asked) >= mine for other in waiting)

    def _held_open(self, ticket: Ticket, wait_s: float, holding: bool = False) -> None:
        """The answer held until there is news for it or the wait passes: for a waiting ticket its turn given,
        looking again at a lock that may have freed with nobody saying so; for a turn that gives way and was
        already `holding` when asked after, the moment it is to give way (docs/node-arbiter.md, 「Handing on」).
        Called under the guard, which the wait gives up meanwhile."""
        deadline = time.monotonic() + min(max(0.0, wait_s), self.hold_open_s)

        def nothing_new() -> bool:
            if self.tickets.get(ticket.id) is not ticket:
                return False
            if not holding:
                return ticket.granted is None
            return (ticket.gives_way_s is not None or ticket.resting) and self._give_way_for(ticket) is None

        while nothing_new():
            left = deadline - time.monotonic()
            if left <= 0:
                return
            self.changed.wait(min(TURN_LOOK_S, left))
            ticket.heard = self.clock()
            self._tidy()
            self._grant()

    def _above(self, ticket: Ticket, other: Ticket) -> bool:
        """Whether a ticket's occupant stands above another's."""
        mine, theirs = self.occupants.rank(ticket.service), self.occupants.rank(other.service)
        return mine is not None and (theirs is None or mine < theirs)

    def _give_way_for(self, ticket: Ticket) -> str | None:
        """Who a turn that gives way, or one resting, is to let go for: the claimant whose claim stands, else
        for a resting turn the first occupant waiting for a request, whatever its rank, and for a turn that
        gives way the highest occupant above it waiting, or of its own rank once its turn values say it lets go
        in the middle of a request and their time has come; None where it keeps the card."""
        if ticket.granted is None or (ticket.gives_way_s is None and not ticket.resting):
            return None
        claimant = self.claimed()
        if claimant is not None:
            return claimant
        spent = self._turn_spent(ticket)
        pauses = spent is not None and spent.can_pause
        waiting = [other for other in self._asking()
                   if ticket.resting or self._above(other, ticket) or (pauses and self._rank(other) == self._rank(ticket))]
        return waiting[0].service if waiting else None

    def passes_to(self, ticket_id: int) -> str | None:
        """Who a turn that lets go only at its request boundary is to pass to there, by its own turn values
        (docs/node-arbiter.md, 「One rank taking turns」): the first of its rank waiting once its turn has been in use
        for their `yield_after_s`; None where no claim, value or waiter makes it pass."""
        with self.guard:
            ticket = self.tickets.get(ticket_id)
            if ticket is None or self.claimed() is not None:
                return None
            spent = self._turn_spent(ticket)
            if spent is None or spent.can_pause:
                return None
            alike = [other for other in self._asking() if self._rank(other) == self._rank(ticket)]
            return alike[0].service if alike else None

    def _wait_s(self, ticket: Ticket, order: list[Ticket]) -> float:
        now = self.clock()
        ahead = order[:order.index(ticket)]

        def held_for(other: Ticket) -> float:
            # A turn that gives way to this ticket, or one resting, holds it up for as long as it takes to let go.
            if other.granted is not None and other.resting:
                return other.gives_way_s if other.gives_way_s is not None else REST_GIVE_WAY_S
            if other.granted is not None and other.gives_way_s is not None and self._above(ticket, other):
                return other.gives_way_s
            return max(0.0, self.typical.get(other.service, DEFAULT_HOLD_S) - (now - other.granted if other.granted else 0.0))
        wait = sum(held_for(other) for other in ahead)
        # A holder of the lock that asked for no turn holds it for as long as a hold is counted at.
        if not any(other.granted is not None for other in self.tickets.values()) and self.holders(self.lock, os.getpid()):
            wait += DEFAULT_HOLD_S
        if self.occupants.claims_by_ticket(ticket.service):
            wait = min(wait, self.claim_wait_s)
        return max(1.0, round(wait, 1))

    def answer(self, ticket: Ticket) -> dict:
        """A ticket as every route answers it (docs/api.md, 「Tickets」)."""
        order = self._order()
        granted = ticket.granted is not None
        giving_way_for = self._give_way_for(ticket) if granted else None
        return {"id": ticket.id, "serviceName": ticket.service, "turn": granted, "inUse": granted and not ticket.resting,
                "resting": granted and ticket.resting, "giveWay": giving_way_for is not None, "giveWayFor": giving_way_for,
                "position": None if granted else order.index(ticket) + 1,
                "waitMs": None if granted else to_ms(self._wait_s(ticket, order)), "claimantName": self.claimed(),
                "waitedMs": to_ms(self.clock() - ticket.asked), "need": ticket.need, "ref": ticket.ref, "rest": ticket.rest,
                "keep": None}

    def _new_id(self) -> int:
        self.last_id = max(self.last_id + 1, int(time.time() * 1000))
        return self.last_id

    def ask(self, service: str, local: bool = False, need: dict | None = None, ref: str | None = None,
            gives_way_s: float | None = None, waited_s: float = 0.0, wait_s: float = 0.0, rest: bool = False) -> dict | None:
        """A new ticket for a service, standing by when its request arrived, `waited_s` ago, or asked only to
        rest on a free card (`rest`); answered at once, or once its turn is given within `wait_s`. None where
        the service has no place on the node."""
        if self.occupants.rank(service) is None:
            return None
        with self.guard:
            now = self.clock()
            ticket = Ticket(self._new_id(), service, now - max(0.0, waited_s), now, local=local, need=need, ref=ref,
                            gives_way_s=gives_way_s, rest=rest)
            self.tickets[ticket.id] = ticket
            # A turn that gives way, its answer held open, hears at once that somebody now waits.
            self.changed.notify_all()
            self._tidy()
            self._grant()
            self._held_open(ticket, wait_s)
            return self.answer(ticket)

    def again(self, ticket_id: int, wait_s: float = 0.0, next_waited_s: float | None = None, rest: bool = False) -> dict | None:
        """A ticket asked after, which keeps it; None where it was dropped. A waiting one may be answered once
        its turn is given within `wait_s`; a turn asked with its next request's wait is told whether it keeps
        the card for that request, which ends its rest where it does; a turn asked with `rest` rests from
        then on."""
        with self.guard:
            self._tidy()
            ticket = self.tickets.get(ticket_id)
            if ticket is None:
                return None
            now = self.clock()
            ticket.heard = now
            holding = ticket.granted is not None
            if rest and holding and not ticket.resting:
                ticket.resting, ticket.rested_at = True, now
            self._grant()
            self._held_open(ticket, wait_s, holding)
            # Given back or dropped while its answer was held open: no ticket to answer for any more.
            if self.tickets.get(ticket_id) is not ticket:
                return None
            kept = None
            if next_waited_s is not None and ticket.granted is not None:
                kept = self._keeps(ticket, next_waited_s)
                if kept and ticket.resting:
                    ticket.resting, ticket.busy_since = False, self.clock()
            answered = self.answer(ticket)
            if kept is not None:
                answered["keep"] = kept
            return answered

    def done(self, ticket_id: int) -> bool:
        """A ticket given back, its hold over; how long it held the card is counted for the next waits, and
        the next in line is given its turn there and then."""
        with self.guard:
            ticket = self.tickets.pop(ticket_id, None)
            if ticket is None:
                return False
            # How long the card was in use, its rest left out; a turn that only rested used it for nothing.
            if ticket.busy_since is not None:
                ended = ticket.rested_at if ticket.resting and ticket.rested_at is not None else self.clock()
                held = max(0.0, ended - ticket.busy_since)
                before = self.typical.get(ticket.service)
                self.typical[ticket.service] = held if before is None else 0.7 * before + 0.3 * held
            self._grant()
            self.changed.notify_all()
            return True

    def follow_use(self, ticket_id: int, in_use: bool) -> None:
        """A turn this process holds for another's engine, in use or resting as that engine's use says: a turn
        coming into use is busy from now, one going quiet rests from now, and every answer held open hears it."""
        with self.guard:
            ticket = self.tickets.get(ticket_id)
            if ticket is None or ticket.granted is None or ticket.resting != in_use:
                return
            now = self.clock()
            if in_use:
                ticket.resting, ticket.busy_since = False, now
            else:
                ticket.resting, ticket.rested_at = True, now
            self._grant()
            self.changed.notify_all()

    def claim_asked(self, name: str) -> bool:
        """Whether a ticket of this claimant stands, a silent one dropped first: the claim it makes lasts
        while one does."""
        with self.guard:
            self._tidy()
            return self.occupants.claims_by_ticket(name) and any(ticket.service == name for ticket in self.tickets.values())

    def state(self) -> dict:
        """The queue as the card shows it (docs/api.md, 「The card」), the claim standing by its claimant's name."""
        with self.guard:
            self._tidy()
            self._grant()
            order = self._order()
            claim = self.claimed()
            held_by = self.holders(self.lock, os.getpid())
            granted = [ticket for ticket in order if ticket.granted is not None]
            # In use: a claim, a request waiting, a turn being used, or the lock held by anyone but a resting turn.
            busy = (claim is not None or bool(self._asking()) or any(not ticket.resting for ticket in granted)
                    or (bool(held_by) and not granted))
            return {
                "occupants": self.occupants.names(),
                "ranks": [rank_names(word) for word in self.occupants.order],
                "claim": claim,
                "lockHeldBy": held_by,
                "busy": busy,
                "queue": [self.answer(ticket) for ticket in order],
                "typicalHoldMs": {service: to_ms(held) for service, held in self.typical.items()},
            }

    async def wait(self, service: str, on_queued: Callable[[dict], Awaitable[None]] | None = None,
                   gives_way_s: float | None = None, waited_s: float = 0.0,
                   need: dict | None = None) -> int | None:
        """A turn waited for in this process, asked after every poll, giving way in `gives_way_s` where it
        does, and standing by a request that has waited `waited_s`; the ticket, or None where the service has
        no place on the node. `on_queued` hears each answer that is not yet a turn. `need` names the unit the
        ticket asks for, which the master places by the rules. A ticket dropped for silence is asked for
        afresh with the same need."""
        answered = self.ask(service, local=True, need=need, gives_way_s=gives_way_s, waited_s=waited_s)
        try:
            while answered is not None and not answered["turn"]:
                if on_queued is not None:
                    await on_queued(answered)
                await asyncio.sleep(TURN_POLL_S)
                answered = (self.again(answered["id"])
                            or self.ask(service, local=True, need=need, gives_way_s=gives_way_s, waited_s=waited_s))
        except BaseException:
            if answered is not None:
                self.done(answered["id"])
            raise
        return None if answered is None else answered["id"]


def to_ms(seconds: float) -> int:
    """Seconds as the wire's whole milliseconds."""
    return round(seconds * 1000)


def epoch_ms(seconds: float | None) -> int | None:
    """Seconds since the epoch as the wire's epoch milliseconds, None kept."""
    return None if seconds is None else round(seconds * 1000)


def asked_need(need: object) -> dict | None:
    """A ticket's `need` as the card shows it: `gpus` and `vramMiB`, each a whole number, and `unitName`, the
    unit asked for, which the master places by the rules (docs/placement.md); a null or missing one left
    out, anything else a body not understood."""
    if need is None:
        return None
    if not isinstance(need, dict):
        raise ValueError("need is not an object")
    kept: dict = {}
    if need.get("unitName") is not None:
        if not isinstance(need["unitName"], str) or not need["unitName"].strip():
            raise ValueError("need.unitName is not a unit name")
        kept["unitName"] = need["unitName"].strip()
    for name in ("gpus", "vramMiB"):
        value = need.get(name)
        if value is None:
            continue
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"need.{name} is not a whole number")
        kept[name] = value
    return kept or None


def asked_gives_way(millis: object) -> float | None:
    """A ticket's `givesWayMs` in seconds: the positive time the occupant takes to let go, or None for a turn
    held to its end; anything else a body not understood."""
    if millis is None:
        return None
    if isinstance(millis, bool) or not isinstance(millis, int) or not millis > 0:
        raise ValueError("givesWayMs is not a positive whole number of milliseconds")
    return millis / 1000


def asked_ms(millis: object, name: str) -> float | None:
    """A span asked in milliseconds in a body or a query, in seconds: none where it was not given, else a
    whole number not below nought; anything else is not understood."""
    if millis is None:
        return None
    if isinstance(millis, bool) or isinstance(millis, float):
        raise ValueError(f"{name} is not a whole number of milliseconds")
    try:
        value = int(millis)  # type: ignore[call-overload]
    except (TypeError, ValueError):
        raise ValueError(f"{name} is not a whole number of milliseconds") from None
    if value < 0:
        raise ValueError(f"{name} is not a whole number of milliseconds")
    return value / 1000


def outcome_line(outcomes: list[dict]) -> str:
    """Each unit's outcome of a move, for the journal."""
    return "; ".join(f"{one['name']} in {one.get('ms')} ms" if one["ok"] else f"{one['name']}: {one['error']}" for one in outcomes) or "nothing"


class UnitsApi:
    """What the slave answers of its units (docs/placement.md, 「Master and slaves」): the card, the units,
    the store, the host memory their dumps take, the claim standing, the queue and the rules as pushed,
    read in the hold API's threads; and a move asked of it, carried out on the arbiter's own loop. The rules
    it reads are handed to the card, whose claimants and placing they are."""

    def __init__(self, card: Card, turns: Turns, store: units_mod.Store | None, rules_path: Path, version: str,
                 memory: units_mod.CardMemory | None = None, desktop: bool = False,
                 owners: units_mod.Owners | None = None) -> None:
        self.card = card
        # The owners besides the card's borrower, whose units are shown and moved beside the lent ones.
        self.owners = owners
        self.desktop = desktop
        self.turns = turns
        self.store = store
        self.rules_path = rules_path
        self.version = version
        self.memory = memory
        self.loop: asyncio.AbstractEventLoop | None = None
        self.rules: dict | None = None
        self.rules_read: float | None = None
        self.rules_error: str | None = None
        # Nothing read yet, which a missing file is not: it too is read once, and declares no claimant.
        self.rules_mtime: float | None = -1.0

    def state(self) -> dict:
        """The card as `GET /card` answers it (docs/api.md, 「The card」)."""
        card = self.card
        claim = card.claimed()
        since = card.claims.get(claim) if claim is not None else None
        available = units_mod.memory_available()
        placing = card.placing()
        state = self.turns.state()
        stage = (card.units.stage_shown() if card.units is not None
                 else {"waiting": False, "waitingMs": 0, "graphs": 0, "waits": 0, "waitedMs": 0})
        return {
            **state,
            "claim": None if claim is None else {
                "claimantName": claim,
                "claimedAt": None if since is None else epoch_ms(time.time() - (time.monotonic() - since))},
            "version": self.version,
            "desktop": self.desktop,
            "units": self.units(),
            "device": self.card_shown(),
            # the lent owner's door alone; every other owner's stands on its own units as `door`
            "stage": stage,
            "store": self.store.state() if self.store is not None else None,
            "ram": {"dumpsMiB": round(card.dumps_bytes() / 2**20),
                    "availableMiB": None if available is None else round(available / 2**20),
                    "maxMiB": None if placing.ram_max_bytes is None else round(placing.ram_max_bytes / 2**20),
                    "floorMiB": None if placing.floor_bytes >= units_mod.NO_RAM else round(placing.floor_bytes / 2**20),
                    "dump": placing.default},
            "rules": {"loadedAt": epoch_ms(self.rules_read), "error": self.rules_error},
        }

    def units(self) -> list[dict]:
        """Every unit on the card: the lent owner's, then every other owner's."""
        units = self.card.units
        return (units.shown() if units is not None else []) + (self.owners.shown() if self.owners is not None else [])

    def card_shown(self) -> dict | None:
        """The card as the driver says it, with every process on it and the service each is known as."""
        read = self.memory.read() if self.memory is not None else None
        if read is None:
            return None
        return {**read, "processes": units_mod.processes_shown(self.memory.processes(), self.card.service_of)}

    def move_one(self, name: str, to: str, reason: str, keeps_turn: bool = False) -> tuple[int, dict]:
        """One unit moved, `evicted` off the card or `resident` back onto it, answered as the unit stands after
        (docs/api.md, 「Card units」), or refused in the wire's error shape. A lent hold keeps its lock and turn
        after an unload where the asker says more of its units follow (`keeps_turn`)."""
        if not any(unit["name"] == name for unit in self.units()):
            return HTTPStatus.NOT_FOUND, refusal("not-found", f"no unit {name} stands on this card")
        if self.loop is None:
            return HTTPStatus.SERVICE_UNAVAILABLE, refusal("not-ready", "the arbiter is starting")
        verb = "evict" if to == units_mod.EVICTED else "restore"
        try:
            outcomes = asyncio.run_coroutine_threadsafe(self.move(verb, [name], reason, not keeps_turn),
                                                        self.loop).result(timeout=300)
        except units_mod.NoRoom as why:
            return HTTPStatus.CONFLICT, refusal("conflict", f"no room: {why}")
        except Exception as fault:  # noqa: BLE001 - said to the asker rather than ending the thread
            return HTTPStatus.INTERNAL_SERVER_ERROR, refusal("failed", f"{type(fault).__name__}: {fault}")
        failed = [one for one in outcomes if not one["ok"]]
        if verb == "evict" and failed:
            return HTTPStatus.CONFLICT, refusal("conflict", f"not unloaded: {outcome_line(failed)}")
        after = next((unit for unit in self.units() if unit["name"] == name), None)
        if after is None:
            return HTTPStatus.NOT_FOUND, refusal("not-found", f"{name} left the card's units while it moved")
        return HTTPStatus.OK, after

    async def move(self, verb: str, names: list[str] | None, reason: str, hand_on: bool = True) -> list[dict]:
        """The units named moved: the lent ones through the card's hold, each other owner's through its own socket.
        Nothing is loaded back onto a card a claim stands on."""
        lent, others = self.owners.split(names) if self.owners is not None else (names, {})
        outcomes: list[dict] = []
        if lent is None or lent:
            outcomes += await (self.card.evict(lent, reason, hand_on=hand_on) if verb == "evict" else self.card.restore(lent))
        for service, named in others.items():
            owner = self.owners.units[service] if self.owners is not None else None
            if owner is None:
                continue
            if verb == "evict":
                outcomes += await owner.evict(named, reason, self.card.placing(service=service))
            elif self.card.claims:
                raise units_mod.NoRoom(f"{self.card.claimed()} claims this node")
            else:
                outcomes += await owner.restore(named)
        return outcomes

    def read_rules(self) -> None:
        """The node's part of the rules read again where it changed, and the change kept in rules.log."""
        try:
            mtime = self.rules_path.stat().st_mtime
        except OSError:
            mtime = None
        if mtime == self.rules_mtime:
            return
        self.rules_mtime = mtime
        rules, self.rules_read, self.rules_error = units_mod.read_rules(self.rules_path)
        # Rules that cannot be read leave those read before standing, the claimants with them.
        if self.rules_error is None:
            self.rules = rules
            self.card.take_rules(rules)
        line = f"{time.strftime('%Y-%m-%dT%H:%M:%S%z')} {'read' if self.rules_error is None else 'not read: ' + self.rules_error}"
        try:
            with self.rules_path.with_name("rules.log").open("a") as log:
                log.write(line + "\n")
        except OSError:
            pass
        say(f"rules: {self.rules_path} {line.split(' ', 1)[1]}")

    async def run(self) -> None:
        """The units read from their owner every two seconds while a hold stands, the store checked once a
        minute, and the rules looked at every second."""
        self.loop = asyncio.get_running_loop()
        checked = 0.0
        while True:
            if self.store is not None and time.monotonic() - checked > 60:
                self.store.check()
                checked = time.monotonic()
            self.read_rules()
            if self.card.units is not None and (self.card.held or self.card.units.connected):
                await self.card.units.refresh()
                self.card.follow_use()
            if self.owners is not None:
                await self.owners.refresh()
            await asyncio.sleep(2.0 if self.card.held else 1.0)


def refusal(code: str, error: str) -> dict:
    """The wire's one error shape: a kebab-case code and an English sentence (docs/api.md, 「The wire format」)."""
    return {"code": code, "error": error}


def card_answer(turns: Turns, units_api: UnitsApi | None, version: str) -> dict:
    """The card as `GET /card` answers it; with the hold alone, its queue and every other part empty."""
    if units_api is not None:
        return units_api.state()
    state = turns.state()
    claim = state["claim"]
    return {**state, "claim": None if claim is None else {"claimantName": claim, "claimedAt": None},
            "version": version, "desktop": False, "units": [], "device": None,
            "stage": {"waiting": False, "waitingMs": 0, "graphs": 0, "waits": 0, "waitedMs": 0}, "store": None,
            "ram": {"dumpsMiB": 0, "availableMiB": None, "maxMiB": None, "floorMiB": None, "dump": units_mod.TO_DISK},
            "rules": {"loadedAt": None, "error": None}}


def turns_handler(turns: Turns, units_api: UnitsApi | None = None, version: str = "") -> type[BaseHTTPRequestHandler]:
    """The node arbiter's routes on the node's loopback (docs/api.md, 「What the node arbiter serves」): its
    health, the card, a ticket asked for, asked after, rested and given back (docs/node-arbiter.md, 「The
    hold」), and the card's units read and moved one at a time (docs/placement.md)."""

    class Handler(BaseHTTPRequestHandler):
        def _send(self, status: int, body: dict | None, retry_after: int | None = None) -> None:
            data = b"" if body is None else json.dumps(body).encode()
            self.send_response(status)
            if body is not None:
                self.send_header("content-type", "application/json")
            if retry_after is not None:
                self.send_header("retry-after", str(retry_after))
            self.send_header("content-length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _refuse(self, status: int, code: str, error: str) -> None:
            self._send(status, refusal(code, error))

        def _body(self) -> object:
            raw = self.rfile.read(int(self.headers.get("content-length") or 0))
            return json.loads(raw) if raw else None

        def _route(self) -> tuple[list[str], dict[str, list[str]]]:
            path, _, query = self.path.partition("?")
            return [unquote(part) for part in path.strip("/").split("/")], parse_qs(query)

        def _ticket_id(self, word: str) -> int | None:
            try:
                return int(word)
            except ValueError:
                self._refuse(HTTPStatus.BAD_REQUEST, "bad-request", f"{word} is not a ticket's id")
                return None

        def _no_route(self) -> None:
            self._refuse(HTTPStatus.NOT_FOUND, "no-route", "no route answers this method and path")

        def _ticket_answer(self, answered: dict, status: int, retry_after: int | None = None) -> None:
            try:
                self._send(status, answered, retry_after)
            except (BrokenPipeError, ConnectionResetError):
                # the asker left while its answer was held open: nobody takes this turn, so it is given back
                turns.done(answered["id"])
                say(f"turns: {answered['serviceName']} left before its ticket {answered['id']} was answered; given back")

        def do_GET(self) -> None:  # noqa: N802
            parts, query = self._route()
            if parts == ["health"]:
                ready = units_api is None or units_api.loop is not None
                return self._send(HTTPStatus.OK, {"ready": ready, "reason": None if ready else "the arbiter is starting",
                                                  "version": units_api.version if units_api is not None else version})
            if parts == ["card"]:
                return self._send(HTTPStatus.OK, card_answer(turns, units_api, version))
            if parts == ["card-units"]:
                return self._send(HTTPStatus.OK, {"items": units_api.units() if units_api is not None else [], "more": False})
            if len(parts) == 2 and parts[0] == "tickets":
                ticket_id = self._ticket_id(parts[1])
                if ticket_id is None:
                    return None
                try:
                    wait_s = asked_ms(query.get("waitMs", [None])[0], "waitMs")
                    next_waited_s = asked_ms(query.get("nextWaitedMs", [None])[0], "nextWaitedMs")
                except ValueError as why:
                    return self._refuse(HTTPStatus.BAD_REQUEST, "bad-request", str(why))
                answered = turns.again(ticket_id, wait_s=wait_s or 0.0, next_waited_s=next_waited_s)
                if answered is None:
                    return self._refuse(HTTPStatus.NOT_FOUND, "not-found", f"no ticket {ticket_id} is held; ask for a new one")
                return self._ticket_answer(answered, HTTPStatus.OK)
            self._no_route()

        def do_POST(self) -> None:  # noqa: N802
            parts, _ = self._route()
            if parts != ["tickets"]:
                return self._no_route()
            try:
                body = self._body()
                if not isinstance(body, dict):
                    raise ValueError("the body is not an object")
                service = body["serviceName"]
                if not isinstance(service, str) or not service:
                    raise ValueError("serviceName is not a service's name")
                need = asked_need(body.get("need"))
                gives_way_s = asked_gives_way(body.get("givesWayMs"))
                waited_s = asked_ms(body.get("waitedMs"), "waitedMs")
                wait_s = asked_ms(body.get("waitMs"), "waitMs")
                rest = body.get("rest", False)
                if rest is None:
                    rest = False
                if not isinstance(rest, bool):
                    raise ValueError("rest is true or false")
            except (ValueError, KeyError, TypeError) as why:
                return self._refuse(HTTPStatus.BAD_REQUEST, "bad-request",
                                    f"a body of serviceName and need, ref, givesWayMs, waitedMs, waitMs and rest is wanted: {why}")
            ref = None if body.get("ref") is None else str(body["ref"])
            answered = turns.ask(service, need=need, ref=ref, gives_way_s=gives_way_s, waited_s=waited_s or 0.0,
                                 wait_s=wait_s or 0.0, rest=rest)
            if answered is None:
                return self._refuse(HTTPStatus.UNPROCESSABLE_ENTITY, "no-place", f"{service} has no place among this node's occupants")
            if answered["turn"]:
                return self._ticket_answer(answered, HTTPStatus.CREATED)
            self._ticket_answer(answered, HTTPStatus.ACCEPTED, max(1, math.ceil(answered["waitMs"] / 1000)))

        def do_PATCH(self) -> None:  # noqa: N802
            parts, _ = self._route()
            if len(parts) == 2 and parts[0] == "tickets":
                ticket_id = self._ticket_id(parts[1])
                if ticket_id is None:
                    return None
                try:
                    body = self._body()
                except ValueError:
                    body = None
                if not isinstance(body, dict) or not isinstance(body.get("resting"), bool):
                    return self._refuse(HTTPStatus.BAD_REQUEST, "bad-request", "a body {\"resting\": true} is wanted")
                if ticket_id not in turns.tickets:
                    return self._refuse(HTTPStatus.NOT_FOUND, "not-found", f"no ticket {ticket_id} is held; ask for a new one")
                if not body["resting"]:
                    return self._refuse(HTTPStatus.CONFLICT, "conflict", "a rest ends only by asking with nextWaitedMs")
                answered = turns.again(ticket_id, rest=True)
                if answered is None:
                    return self._refuse(HTTPStatus.NOT_FOUND, "not-found", f"no ticket {ticket_id} is held; ask for a new one")
                return self._send(HTTPStatus.OK, answered)
            if len(parts) == 2 and parts[0] == "card-units" and units_api is not None:
                try:
                    body = self._body()
                except ValueError:
                    body = None
                to = body.get("state") if isinstance(body, dict) else None
                reason = body.get("reason") if isinstance(body, dict) else None
                keeps_turn = body.get("keepsTurn") if isinstance(body, dict) else None
                if (to not in (units_mod.EVICTED, units_mod.RESIDENT) or not (reason is None or isinstance(reason, str))
                        or not (keeps_turn is None or isinstance(keeps_turn, bool))):
                    return self._refuse(HTTPStatus.BAD_REQUEST, "bad-request",
                                        "a body {\"state\": \"evicted\" or \"resident\", \"reason\"?, \"keepsTurn\"?} is wanted")
                return self._send(*units_api.move_one(parts[1], to, reason or "asked", bool(keeps_turn)))
            if len(parts) == 2 and parts[0] == "card-units":
                return self._refuse(HTTPStatus.NOT_FOUND, "not-found", f"no unit {parts[1]} stands on this card")
            self._no_route()

        def do_DELETE(self) -> None:  # noqa: N802
            parts, _ = self._route()
            if len(parts) != 2 or parts[0] != "tickets":
                return self._no_route()
            ticket_id = self._ticket_id(parts[1])
            if ticket_id is None:
                return None
            if turns.done(ticket_id):
                return self._send(HTTPStatus.NO_CONTENT, None)
            self._refuse(HTTPStatus.NOT_FOUND, "not-found", f"no ticket {ticket_id} is held")

        def do_PUT(self) -> None:  # noqa: N802
            self._no_route()

        def log_message(self, format: str, *args: object) -> None:  # noqa: A002
            pass

    return Handler


class ClaimWatch:
    """Every claimant's claim on the card, read every poll from its tickets standing (`asked`), and every two
    seconds from its containers running and its processes on the card, those in its containers or, where
    the rules name its unit, in that unit's cgroup: `started` is called, with the claimant and what was
    seen, when any appears, and `ended` once the node has held none for the settle time. A container
    running under a granted ticket is the same claim. A claim kept by containers alone with nothing of its
    claimant on the card for the idle time has those containers stop counting, each by its name, until a
    process of it comes onto the card. Nothing else that runs on the node claims. The claimants are the
    rules' as they stand at each poll, so one added to the rules is watched within the poll and needs no
    restart."""

    def __init__(self, claimants: Callable[[], list[units_mod.Claimant]], started: Callable[[str, str], None],
                 ended: Callable[[str], None], poll_s: float = CLAIM_POLL_S, settle_s: float = CLAIM_SETTLE_S,
                 containers: Callable[[str], list[str]] = claim_containers,
                 asked: Callable[[str], bool] = lambda name: False,
                 cgroup: Callable[[str], Path | None] = claim_cgroup,
                 on_card: Callable[[Path | None, str | None], list[int] | None] = lambda cgroup, containers: None,
                 idle_s: float = CLAIM_IDLE_S, card_every_s: float = CLAIM_CARD_S) -> None:
        self.claimants = claimants
        self.started = started
        self.ended = ended
        self.poll_s = poll_s
        self.settle_s = settle_s
        self.containers = containers
        self.asked = asked
        self.cgroup = cgroup
        self.on_card = on_card
        self.idle_s = idle_s
        self.card_every_s = card_every_s
        self.present: set[str] = set()
        self.quiet_since: dict[str, float] = {}
        self.where: dict[str, Path] = {}
        self.missing: set[str] = set()
        # The card and the containers as last read for each claimant, and when; since when a claim has had
        # nothing on the card; and the containers that no longer count.
        self.card_read: dict[str, tuple[float, list[int] | None, list[str]]] = {}
        self.card_since: dict[str, float] = {}
        self.not_counted: dict[str, set[str]] = {}

    def _unit(self, claimant: units_mod.Claimant) -> Path | None:
        """The cgroup of the claimant's unit where the rules name one and it is there, said once where not."""
        name = claimant.name
        if claimant.cgroup is None:
            return None
        cgroup = self.where.get(name)
        if cgroup is None or not cgroup.exists():
            cgroup = self.cgroup(claimant.cgroup)
            if cgroup is None or not cgroup.exists():
                if name not in self.missing:
                    self.missing.add(name)
                    say(f"{name}: {claimant.cgroup} is not there; none of its processes on the card is known by it")
                return None
            self.where[name] = cgroup
            self.missing.discard(name)
        return cgroup

    def _read(self, claimant: units_mod.Claimant, now: float) -> tuple[list[int] | None, list[str]]:
        """The claimant's processes on the card, None where the card cannot be read, and its containers
        running, as read at most every two seconds."""
        read = self.card_read.get(claimant.name)
        if read is None or now - read[0] >= self.card_every_s:
            unit = self._unit(claimant)
            if unit is None and not claimant.containers:
                read = (now, [], [])
            else:
                read = (now, self.on_card(unit, claimant.containers),
                        self.containers(claimant.containers) if claimant.containers else [])
            self.card_read[claimant.name] = read
        return read[1], read[2]

    def seen(self, claimant: units_mod.Claimant, now: float) -> str | None:
        """What of the claimant claims the card now, said for the journal, or None."""
        name = claimant.name
        if claimant.tickets and self.asked(name):
            self.card_since[name] = now
            return "a ticket was asked for"
        on_card, running = self._read(claimant, now)
        if on_card:
            self.card_since[name] = now
            return f"a process of it is on the card (process {on_card[0]})"
        # A container no longer counted is forgotten once it has gone.
        stopped = self.not_counted.setdefault(name, set()) & set(running)
        self.not_counted[name] = stopped
        counted = [one for one in running if one not in stopped]
        if not counted:
            return None
        if name in self.present and on_card is not None and now - self.card_since.setdefault(name, now) >= self.idle_s:
            stopped.update(counted)
            say(f"{name}: nothing of it on the card for {now - self.card_since[name]:.0f} s; its containers "
                f"({', '.join(counted)}) no longer count until one comes onto the card")
            return None
        return f"a container of it runs ({counted[0]})"

    def step(self, now: float) -> None:
        claimants = self.claimants()
        for claimant in claimants:
            found = self.seen(claimant, now)
            if found:
                self.quiet_since.pop(claimant.name, None)
                if claimant.name not in self.present:
                    self.present.add(claimant.name)
                    self.card_since[claimant.name] = now
                    self.started(claimant.name, found)
            elif claimant.name in self.present:
                quiet = self.quiet_since.setdefault(claimant.name, now)
                if now - quiet >= self.settle_s:
                    self._end(claimant.name)
        # A claimant the rules no longer declare claims nothing.
        for name in self.present - {claimant.name for claimant in claimants}:
            self._end(name)

    def _end(self, name: str) -> None:
        self.present.discard(name)
        self.quiet_since.pop(name, None)
        self.card_since.pop(name, None)
        self.ended(name)

    async def run(self) -> None:
        while True:
            self.step(time.monotonic())
            await asyncio.sleep(self.poll_s)


class Card:
    """The card held for one connection at a time, the slot pieces its rpc-server keeps, the node given way
    on while a claim stands, and the node's rules as pushed: its claimants and where its units go out to."""

    def __init__(self, key: str, address: str, lock_path: Path | None, rpc_argv: list[str], pieces: Path,
                 lock_wait_s: float = LOCK_WAIT_S, start_wait_s: float = START_WAIT_S, rpc_port: int = RPC_PORT,
                 packed: bool = False, yield_wait_s: float = YIELD_WAIT_S, stop_wait_s: float = STOP_WAIT_S,
                 claim_counts: Callable[[str], bool] = lambda name: True,
                 claim_yields: Callable[[str], bool] = lambda name: True) -> None:
        self.key = key.encode()
        self.address = address
        self.lock_path = lock_path
        self.rpc_argv = rpc_argv
        self.pieces = pieces
        self.lock_wait_s = lock_wait_s
        self.start_wait_s = start_wait_s
        self.rpc_port = rpc_port
        # A packed card's hold stands beside the node's lock rather than taking it.
        self.packed = packed
        self.yield_wait_s = yield_wait_s
        self.stop_wait_s = stop_wait_s
        # Whether a claimant has a place among the node's occupants at all, and whether it comes before the
        # service the card is lent to, read at the moment its claim begins.
        self.claim_counts = claim_counts
        self.claim_yields = claim_yields
        # The node's part of the rules as last read cleanly, and the claimants they declare.
        self.rules: dict | None = None
        self.claimants: list[units_mod.Claimant] = []
        # The node's hold queue, where it keeps one, the service a hold waits in it as, and the ticket of
        # the hold standing now.
        self.turns: Turns | None = None
        self.lent_to = ANY
        self.ticket: int | None = None
        self.held = False
        self.holder: asyncio.StreamWriter | None = None
        # Each claimant whose claim stands, with when it began; the lock held for them while any does.
        self.claims: dict[str, float] = {}
        self.claim_lock: int | None = None
        self.shutting_out: asyncio.Task[None] | None = None
        self.lowering: asyncio.Task[None] | None = None
        # Whether the dumps kept in host memory have been lowered as far as the claim standing needs.
        self.ram_ready = True
        self.told_to_yield = asyncio.Event()
        # The process group a local hold named as the one putting work on the card, ended with the hold
        # when it does not let go of a YIELD in time.
        self.uses: int | None = None
        # The rpc-server the standing lent hold started, so its memory on the card is known as the lend
        # service's.
        self.server_pid: int | None = None
        # The node's lock as the standing hold took it, let go while its units are out and taken again
        # before they come back (docs/placement.md, 「Unloading and loading a unit」).
        self.hold_lock: int | None = None
        # The text model's units on this card, where its rpc-server can unload them; one move at a time.
        self.units: units_mod.Units | None = None
        self.moving = asyncio.Lock()
        self.coming_back: asyncio.Task[None] | None = None
        # The graphs the standing hold's owner had computed at the last read, and when it last computed, which
        # its turn's use follows.
        self.graphs_seen = 0
        self.used_at = 0.0
        # The owners besides the borrower, whose dumps count with the lent ones and are lowered for a claim.
        self.owners: units_mod.Owners | None = None
        # Where the owner of a hold's units opens its control socket: the rpc-server this starts, or the
        # engine of a local hold, which is told it on READY.
        self.control: Path | None = None
        # The turn values the rules carry, by service, which the hold queue reads (docs/node-arbiter.md, 「One rank
        # taking turns」).
        self.turn_values: dict[str, units_mod.Turn] = {}
        # Since when the head's longest request has waited for this card's units while it says WANT, the restore
        # its WANT started, whether a restore is loading the units already, and the turn the hold was told to
        # PASS in (docs/node-arbiter.md, 「Lending」).
        self.wanted_at: float | None = None
        self.wanting: asyncio.Task[None] | None = None
        self.loading = False
        self.passed: str | None = None

    def take_rules(self, rules: dict | None) -> None:
        self.rules = rules
        self.claimants = units_mod.claimants_of(rules)
        self.turn_values = units_mod.turns_of(rules)

    def claimed(self) -> str | None:
        """The claimant whose claim stands, the first to have begun; None where none does. Read from the
        hold API's threads too, so it reads a copy."""
        claims = self.claims.copy()
        return min(claims, key=lambda name: claims[name]) if claims else None

    def floor_bytes(self, extra: str | None = None) -> int:
        """The host memory that must stay available while the claims standing (and `extra`) stand: the most
        any of their claimants needs, every dump out of host memory for one that needs all of it."""
        names = set(self.claims) | ({extra} if extra else set())
        floors = [claimant.ram_free_bytes for claimant in self.claimants if claimant.name in names]
        if any(floor is None for floor in floors):
            return units_mod.NO_RAM
        return max([floor for floor in floors if floor is not None], default=0)

    def service_of(self, pid: int) -> str | None:
        """The service a process on the card is known as: the one it declares in its own environment, else
        the service the card is lent to for the rpc-server the standing hold started and for the process
        group a local hold named; None for any other."""
        declared = declared_service(pid)
        if declared is not None:
            return declared
        if self.lent_to == ANY:
            return None
        if pid == self.server_pid:
            return self.lent_to
        try:
            return self.lent_to if self.uses is not None and os.getpgid(pid) == self.uses else None
        except (ProcessLookupError, PermissionError):
            return None

    def placing(self, extra: str | None = None, service: str | None = None) -> units_mod.Placing:
        """Where the rules put `service`'s units (the lent one's by default) when they go out, within the host
        memory every dump on the node takes together."""
        if service is None:
            service = self.units.service if self.units is not None else ""
        return dataclasses.replace(units_mod.placing_of(self.rules, service, self.floor_bytes(extra)),
                                   kept_bytes=self.dumps_bytes())

    def dumps_bytes(self) -> int:
        """The host memory every dump on the node takes: the lent units' and every other owner's."""
        lent = self.units.ram_bytes() if self.units is not None else 0
        return lent + (sum(one.ram_bytes() for one in self.owners.units.values()) if self.owners is not None else 0)

    async def serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """`HOLD <key> [local]` holds the card for as long as the connection stays open, a local hold
        naming the process group that uses the card with a `USES <group>` line on it; `STATE <key>`
        says whether it could be held; `QUIET <key>` rests the standing hold's turn until its owner's next
        graph; `WANT <key> <seconds>` asks the standing hold's units back for a request that has waited those
        seconds, and `UNWANT <key>` takes the ask back; `DROP <key> <file>…` removes the pieces of those slot
        files; `FETCH <key> <file>` sends them; `SPACE <key> [<file>]` says how many bytes the pieces' folder
        has free and in all, and how many the named file's pieces take."""
        try:
            asked = await asyncio.wait_for(reader.readline(), ASK_WAIT_S)
        except (asyncio.TimeoutError, ConnectionError):
            writer.close()
            return
        verb, _, rest = asked.strip().partition(b" ")
        key, _, names = rest.partition(b" ")
        if not hmac.compare_digest(key, self.key) or verb not in (b"HOLD", b"STATE", b"QUIET", b"WANT", b"UNWANT", b"DROP", b"FETCH", b"SPACE"):
            await self._answer(writer, "REFUSED")
            return
        if verb == b"DROP":
            await self._answer(writer, f"DROPPED {self.drop(names.decode(errors='replace').split())}")
            return
        if verb == b"STATE":
            await self._answer(writer, self.state())
            return
        if verb == b"QUIET":
            await self.quiet()
            await self._answer(writer, "QUIETED")
            return
        if verb == b"WANT":
            try:
                seconds = max(0.0, float(names.strip() or b"0"))
            except ValueError:
                seconds = 0.0
            self.want(seconds)
            await self._answer(writer, "WANTED")
            return
        if verb == b"UNWANT":
            self.unwant()
            await self._answer(writer, "UNWANTED")
            return
        if verb == b"FETCH":
            await self._fetch(writer, names.decode(errors="replace").strip())
            return
        if verb == b"SPACE":
            await self._answer(writer, self.space(names.decode(errors="replace").strip()))
            return
        await self._hold(reader, writer, local=names.strip() == b"local")

    def state(self) -> str:
        if self.claims:
            return f"CLAIMED {self.claimed()}"
        if self.held:
            return "HELD"
        if self.lock_path is not None and not self.packed and (holders := lock_holders(self.lock_path, os.getpid())):
            return f"LOCKED by {holders[0]}"
        return "FREE"

    async def _hold(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, local: bool) -> None:
        if self.claims:
            await self._answer(writer, f"CLAIMED {self.claimed()} claims this node")
            return
        if self.held:
            await self._answer(writer, "BUSY the card is already held")
            return
        self.held = True
        self.told_to_yield = asyncio.Event()
        self.uses = None
        lock_fd: int | None = None
        proc: asyncio.subprocess.Process | None = None
        began = time.monotonic()
        try:
            lock_fd = await self._take_lock(reader, writer)
            self.hold_lock = lock_fd
            proc = None if local else await self._start()
            self.server_pid = None if proc is None else proc.pid
            if self.claims:
                raise Refused(f"CLAIMED {self.claimed()} began a claim on this node")
            # The hold is the one a claim's YIELD goes to from the moment READY is written, so a claim that
            # begins while READY drains is still told, after it. A local hold is told where its engine is to
            # open the control socket its units are unloaded through, where this node keeps a dump store.
            self.holder = writer
            if local and self.units is not None and self.control is not None:
                self.control.unlink(missing_ok=True)
                writer.write(f"READY {self.control}\n".encode())
            else:
                writer.write(b"READY\n")
            await writer.drain()
            say(f"card held{' for the head' if local else ''}: ready in {(time.monotonic() - began) * 1000:.0f} ms")
            await self._until_let_go(reader, writer, proc)
        except Refused as refused:
            await self._answer(writer, refused.answer)
        except (ConnectionError, OSError) as fault:
            say(f"card: the hold ended on {type(fault).__name__}: {fault}")
        finally:
            self.holder = None
            self.server_pid = None
            if self.coming_back is not None:
                self.coming_back.cancel()
                self.coming_back = None
            if self.wanting is not None:
                self.wanting.cancel()
                self.wanting = None
            self.wanted_at = None
            await self._stop(proc)
            if self.units is not None:
                await self.units.refresh()
            if self.hold_lock is not None:
                os.close(self.hold_lock)
                self.hold_lock = None
            if self.turns is not None and self.ticket is not None:
                self.turns.done(self.ticket)
            self.ticket = None
            self.held = False
            writer.close()
            say(f"card let go after {time.monotonic() - began:.1f} s")

    def drop(self, files: list[str]) -> int:
        """The pieces of the named slot files removed: each piece is its file's name, a dot and its
        number. A name that is not a plain file name is passed over."""
        prefixes = tuple(f"{name}." for name in files if not name.startswith(".") and "/" not in name)
        removed = 0
        for piece in list(self.pieces.iterdir()) if prefixes else []:
            if piece.name.startswith(prefixes):
                piece.unlink(missing_ok=True)
                removed += 1
        return removed

    def space(self, name: str) -> str:
        """`SPACE <free> <total> <held>`: the bytes free and in all on the pieces' filesystem, and the
        bytes the named slot file's pieces take here, none for a name that is not a plain file name."""
        usage = shutil.disk_usage(self.pieces)
        held = 0
        if name and not name.startswith(".") and "/" not in name:
            held = sum(piece.stat().st_size for piece in self.pieces.glob(f"{name}.*") if piece.is_file())
        return f"SPACE {usage.free} {usage.total} {held}"

    async def _fetch(self, writer: asyncio.StreamWriter, name: str) -> None:
        """Every piece of one slot file, each as a `PIECE <name> <size>` line and its bytes, then END."""
        try:
            if name and not name.startswith(".") and "/" not in name:
                for piece in sorted(self.pieces.glob(f"{name}.*")):
                    size = piece.stat().st_size
                    writer.write(f"PIECE {piece.name} {size}\n".encode())
                    with piece.open("rb") as source:
                        while chunk := source.read(CHUNK):
                            writer.write(chunk)
                            await writer.drain()
            writer.write(b"END\n")
            await writer.drain()
        except (ConnectionError, OSError) as fault:
            say(f"card: sending pieces of {name} ended on {type(fault).__name__}")
        writer.close()

    async def _answer(self, writer: asyncio.StreamWriter, answer: str) -> None:
        try:
            writer.write(f"{answer}\n".encode())
            await writer.drain()
        except ConnectionError:
            pass
        writer.close()

    async def _take_lock(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> int | None:
        """The node's lock for a hold that is not packed beside it. Where the node keeps a hold queue, the
        hold waits its turn there for as long as the head keeps the connection, saying `QUEUED <position>
        <seconds>` whenever its place moves, before the lock is taken."""
        if self.lock_path is None or self.packed:
            return None
        if self.turns is not None:
            shown: int | None = None

            async def queued(answer: dict) -> None:
                nonlocal shown
                if answer["position"] != shown:
                    shown = answer["position"]
                    writer.write(f"QUEUED {answer['position']} {answer['waitMs'] / 1000:g}\n".encode())
                    await writer.drain()

            waiting = asyncio.ensure_future(self.turns.wait(self.lent_to, queued, gives_way_s=LEND_GIVES_WAY_S))
            gone = asyncio.ensure_future(reader.read(1))
            done, _ = await asyncio.wait({waiting, gone}, return_when=asyncio.FIRST_COMPLETED)
            if waiting not in done:
                waiting.cancel()
                await asyncio.gather(waiting, return_exceptions=True)
                raise ConnectionError("the head left while the hold waited its turn")
            gone.cancel()
            self.ticket = waiting.result()
            if self.ticket is None:
                raise Refused(f"REFUSED {self.lent_to} has no place among this node's occupants")
            self.used_at = time.monotonic()
        fd = os.open(self.lock_path, os.O_RDWR | os.O_CREAT, 0o666)
        deadline = time.monotonic() + self.lock_wait_s
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return fd
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    os.close(fd)
                    raise Refused("BUSY the card lock stayed held by another service")
                await asyncio.sleep(POLL_S)

    async def _start(self) -> asyncio.subprocess.Process:
        # Started evictable, with the control socket its units are unloaded through, where the arbiter keeps
        # a dump store; a build without the patch ignores both and its card is given back by YIELD alone.
        env = dict(os.environ)
        if self.units is not None and self.control is not None:
            self.control.unlink(missing_ok=True)
            env.update(GGML_CUDA_EVICTABLE="1", GGML_CUDA_CONTROL=str(self.control))
        proc = await asyncio.create_subprocess_exec(
            *self.rpc_argv, stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL, start_new_session=True, env=env)
        deadline = time.monotonic() + self.start_wait_s
        while True:
            if proc.returncode is not None:
                raise Refused(f"FAILED the rpc-server exited with {proc.returncode}")
            try:
                _, probe = await asyncio.open_connection(self.address, self.rpc_port)
                probe.close()
                return proc
            except OSError:
                pass
            if time.monotonic() >= deadline:
                await self._stop(proc)
                raise Refused("FAILED the rpc-server did not come up in time")
            await asyncio.sleep(POLL_S)

    async def _until_let_go(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                            proc: asyncio.subprocess.Process | None) -> None:
        """Until the holder closes its connection, the rpc-server ends under it, or a hold told to YIELD
        has not let go in time, when the process group a local hold named is ended with it: the arbiter
        holds a local hold's lock itself, so the keeper never reaches the process using the card. A
        local hold names that group with a `USES <group>` line, answered `USING <group>` or
        `REFUSED <why>`."""

        async def closed() -> None:
            while line := await reader.readline():
                verb, _, group = line.strip().partition(b" ")
                if verb != b"USES":
                    continue
                writer.write(f"{self._name_user(group.decode(errors='replace'), local=proc is None)}\n".encode())
                await writer.drain()

        async def overdue() -> None:
            await self.told_to_yield.wait()
            await asyncio.sleep(self.yield_wait_s)

        watching = [asyncio.ensure_future(closed()), asyncio.ensure_future(overdue())]
        if proc is not None:
            watching.append(asyncio.ensure_future(proc.wait()))
        done, pending = await asyncio.wait(watching, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        if watching[1] in done:
            if proc is not None:
                say(f"claim: the hold did not let go within {self.yield_wait_s:g} s of YIELD; its rpc-server is stopped")
            elif self.uses is not None:
                say(f"claim: the hold did not let go within {self.yield_wait_s:g} s of YIELD; process group {self.uses} is ended")
                await self._end_group(self.uses)
            else:
                say(f"claim: the hold did not let go within {self.yield_wait_s:g} s of YIELD and named no process group")
        elif proc is not None and proc.returncode is not None:
            say(f"card: the rpc-server exited with {proc.returncode} while held")

    def _name_user(self, group: str, local: bool) -> str:
        """The process group a local hold says puts work on the card, or why it is refused: a lent hold's
        user is its rpc-server, and the arbiter's own group, or one that is no group at all, is never
        ended."""
        if not local:
            return "REFUSED a hold with an rpc-server names no process group"
        if not group.isdigit() or int(group) <= 1:
            return "REFUSED not a process group"
        if int(group) == os.getpgid(0):
            return "REFUSED the arbiter's own process group"
        self.uses = int(group)
        return f"USING {group}"

    async def _end_group(self, group: int) -> None:
        """The process group sent SIGTERM, and SIGKILL where any of it is still there after the stop wait."""
        try:
            os.killpg(group, signal.SIGTERM)
        except (ProcessLookupError, PermissionError) as fault:
            say(f"card: could not end process group {group}: {type(fault).__name__}")
            return
        deadline = time.monotonic() + self.stop_wait_s
        while time.monotonic() < deadline:
            try:
                os.killpg(group, 0)
            except (ProcessLookupError, PermissionError):
                return
            await asyncio.sleep(POLL_S)
        try:
            os.killpg(group, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass

    async def _stop(self, proc: asyncio.subprocess.Process | None) -> None:
        if proc is None or proc.returncode is not None:
            return
        proc.send_signal(signal.SIGTERM)
        try:
            await asyncio.wait_for(proc.wait(), STOP_WAIT_S)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()

    def _tell(self, line: str) -> None:
        """A line on the standing hold, which the head reads as it reads YIELD."""
        if self.holder is not None:
            try:
                self.holder.write(f"{line}\n".encode())
            except (ConnectionError, RuntimeError):
                pass

    def _let_lock_go(self) -> None:
        """The standing hold's lock and turn given back while its units are out: the card's rest is inert
        memory, since its rpc-server computes nothing until every unit is back."""
        if self.hold_lock is not None:
            os.close(self.hold_lock)
            self.hold_lock = None
        if self.turns is not None and self.ticket is not None:
            self.turns.done(self.ticket)
            self.ticket = None

    def unit_out(self) -> dict | None:
        """The first of the hold's units still out, as the card shows it, and the size it needs back; None where
        every unit stands on the card. A restore the head's `WANT` starts names it in its ticket, so the master
        places room for it by the rules (docs/api.md, 「Admission on a shared pool」)."""
        if self.units is None:
            return None
        return next((one for one in self.units.shown() if one["state"] != units_mod.RESIDENT), None)

    async def _take_lock_back(self, waits: bool = False) -> None:
        """The standing hold's turn and lock taken again before its units come back, saying `QUEUED <position>
        <seconds>` on the hold whenever its place moves; NoRoom where another occupant keeps the lock past the
        wait. A restore the head's WANT started (`waits`) waits for its turn however long it takes."""
        if self.lock_path is None or self.packed or self.hold_lock is not None:
            return
        if self.turns is not None:
            # The request waiting at the owner's door, or the one the head's WANT waits for where it has waited
            # longer, arrived before anyone asking at this rank meanwhile (docs/node-arbiter.md, 「Lending」).
            door = self.units.stage if self.units is not None else units_mod.Stage()
            waited_s = max(door.waiting_s if door.waiting else 0.0,
                           time.monotonic() - self.wanted_at if self.wanted_at is not None else 0.0)
            shown: int | None = None

            async def queued(answer: dict) -> None:
                nonlocal shown
                if answer["position"] != shown:
                    shown = answer["position"]
                    self._tell(f"QUEUED {answer['position']} {answer['waitMs'] / 1000:g}")

            # A restore the head's WANT starts names the unit it brings back, so the master may make room for
            # it by the rules; the hold's first turn takes none, the unit asked for being the hold's own card.
            out = self.unit_out() if waits else None
            need = None if out is None else {"unitName": out["name"], "vramMiB": out["vramMiB"]}
            waiting = self.turns.wait(self.lent_to, queued, gives_way_s=LEND_GIVES_WAY_S, waited_s=waited_s,
                                      need=need)
            try:
                self.ticket = await (waiting if waits else asyncio.wait_for(waiting, self.lock_wait_s * 3))
            except asyncio.TimeoutError:
                raise units_mod.NoRoom("the turn did not come") from None
            if self.ticket is None:
                raise units_mod.NoRoom(f"{self.lent_to} has no place among this node's occupants")
            self.used_at = time.monotonic()
        fd = os.open(self.lock_path, os.O_RDWR | os.O_CREAT, 0o666)
        deadline = time.monotonic() + self.lock_wait_s
        try:
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    self.hold_lock = fd
                    return
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        raise units_mod.NoRoom("the card lock stayed held by another occupant") from None
                    await asyncio.sleep(POLL_S)
        except BaseException:
            # Not taken, or the WANT taken back meanwhile: neither the lock nor the turn is kept.
            os.close(fd)
            if self.turns is not None and self.ticket is not None:
                self.turns.done(self.ticket)
                self.ticket = None
            raise

    async def evict(self, names: list[str] | None, reason: str, claimant: str | None = None,
                    hand_on: bool = True) -> list[dict]:
        """Units of the standing hold unloaded, each where the node's rules put it, within the host memory
        the claims standing (and `claimant`'s) leave, and the head told on the hold once any is out, with
        `EVICTED claim <claimant>` where they went out for a claim and `EVICTED <reason>` otherwise. Its
        lock and turn are given back only once every unit asked for went out: one still on the card keeps
        both, so nobody is handed a card the hold's memory still fills; and where the asker says more of its
        units follow (`hand_on` false), they are kept until the last of them is out."""
        if self.units is None:
            return [{"name": name, "ok": False, "error": "this node lends no evictable card"} for name in names or ["*"]]
        aside = await self._want_aside()
        try:
            async with self.moving:
                outcomes = await self.units.evict(names, reason, self.placing(claimant))
                whole = bool(outcomes) and all(one["ok"] for one in outcomes)
                if self.units.out():
                    if whole and hand_on:
                        self._let_lock_go()
                    self._tell(f"EVICTED claim {claimant}" if claimant is not None else f"EVICTED {reason}")
                say(f"units: out for {reason}: {outcome_line(outcomes)}"
                    + ("" if whole else "; the lock and the turn are kept until every unit asked for is out"))
                return outcomes
        finally:
            if aside:
                self._start_wanting()

    async def restore(self, names: list[str] | None, waits: bool = False) -> list[dict]:
        """Units of the standing hold loaded back, its lock taken again first; NoRoom where it cannot be. One
        the head's WANT started (`waits`) waits for the turn however long it takes, and finds nothing to do
        where another move brought the units back first."""
        if self.units is None:
            return []
        aside = False if waits else await self._want_aside()
        try:
            async with self.moving:
                if self.claims:
                    raise units_mod.NoRoom(f"{self.claimed()} claims this node")
                if waits and not self.units.out():
                    return []
                await self._take_lock_back(waits)
                self.loading = True
                try:
                    outcomes = await self.units.restore(names)
                except units_mod.NoRoom:
                    if self.units.out():
                        self._let_lock_go()
                    raise
                finally:
                    self.loading = False
                if not self.units.out():
                    self.wanted_at = None
                    self._tell("RESTORED")
                say(f"units: back: {outcome_line(outcomes)}")
                return outcomes
        finally:
            if aside:
                self._start_wanting()

    def want(self, seconds: float) -> None:
        """The head's request waits for this card, `seconds` so far (docs/node-arbiter.md, 「Lending」): a standing hold
        whose units are out takes its place in the queue through its own restore, standing by those seconds.
        While a claim stands, or the units come back after one, the ask is kept for that return and nothing
        else moves; with no hold or every unit on the card it changes nothing."""
        if not self.held or self.units is None or not self.units.out():
            return
        since = time.monotonic() - seconds
        self.wanted_at = since if self.wanted_at is None else min(self.wanted_at, since)
        self._start_wanting()

    def _start_wanting(self) -> None:
        """The restore a WANT asks for started, where the ask stands, the units are out and neither a claim nor
        another return of theirs is under way."""
        if self.wanted_at is None or not self.held or self.units is None or not self.units.out() or self.claims:
            return
        if any(task is not None and not task.done() for task in (self.coming_back, self.wanting)):
            return
        self.wanting = asyncio.get_running_loop().create_task(self._want_back())

    async def _want_aside(self) -> bool:
        """A WANT's restore still waiting for its turn set aside while another move of the units runs, so
        the wait holds no move up; True where one was, to be started again after."""
        task = self.wanting
        if task is None or task.done() or self.loading or task is asyncio.current_task():
            return False
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        self.wanting = None
        return True

    def unwant(self) -> None:
        """No request of the head's waits for this card any more: the ask is taken back, and its restore with
        it where the units have not begun to come back."""
        self.wanted_at = None
        if self.wanting is not None and not self.wanting.done() and not self.loading:
            self.wanting.cancel()
            self.wanting = None

    async def _want_back(self) -> None:
        """The units the head's WANT asked back loaded once the hold's turn comes, tried again while the card
        has not the room; given up once the ask is taken back, a claim begins or the units are back."""
        while self.wanted_at is not None and self.held and not self.claims and self.units is not None and self.units.out():
            try:
                await self.restore(None, waits=True)
                return
            except units_mod.NoRoom as why:
                say(f"units: not back yet for the head's request: {why}")
                await asyncio.sleep(2.0)

    async def lower(self) -> list[dict]:
        """Dumps kept in host memory written to the store, or dropped where their slots hold them, until the
        host memory the claims standing need is available: the lent units' first, then every other owner's,
        whose lowering is its own (a dumped engine's, ended); nothing where it already is."""
        floor = self.floor_bytes()

        def enough() -> bool:
            available = units_mod.memory_available()
            return floor < units_mod.NO_RAM and available is not None and available >= floor

        if enough() or self.dumps_bytes() == 0:
            return []
        owning = ([self.units] if self.units is not None else []) + (list(self.owners.units.values()) if self.owners is not None else [])
        outcomes: list[dict] = []
        async with self.moving:
            for units in owning:
                if enough():
                    break
                outcomes += await units.lower(enough)
        if outcomes:
            freed = sum(one["freed"] for one in outcomes if one["ok"]) / 2**30
            say(f"units: {freed:.1f} GiB of host memory given back for {' '.join(self.claims) or 'nothing'}: "
                + "; ".join(f"{one['name']} #{one['buffer']} {'in ' + str(one['ms']) + ' ms' if one['ok'] else one['error']}" for one in outcomes))
        return outcomes

    def follow_use(self, now: float | None = None) -> None:
        """The standing hold's turn in use or resting as its engine's use says (docs/node-arbiter.md, 「Lending」): in use
        while its units' owner has computed within the quiet or a use waits at its door, resting after."""
        if self.turns is None or self.ticket is None or self.units is None or not self.units.connected:
            return
        now = time.monotonic() if now is None else now
        door = self.units.stage
        if door.graphs != self.graphs_seen or door.waiting:
            self.graphs_seen, self.used_at = door.graphs, now
        self.turns.follow_use(self.ticket, now - self.used_at < LEND_QUIET_S)
        # Passed at its request boundary to one of its rank, by the lend service's own turn values, once a turn.
        passing = self.turns.passes_to(self.ticket)
        if passing is not None and self.passed != self.ticket:
            self.passed = self.ticket
            self._tell(f"PASS {passing}")

    async def quiet(self, now: float | None = None) -> None:
        """The standing hold's owner said to compute nothing for a while (docs/node-arbiter.md, 「Lending」): the units are
        read first, so a graph computed before it counts as before it, and its turn rests at once, until the
        owner's next graph or a use waiting at its door."""
        if self.units is None:
            return
        await self.units.refresh()
        self.follow_use(now)
        self.used_at -= LEND_QUIET_S
        self.follow_use(now)

    def claim_holds(self) -> bool:
        """Whether the card is the claim's: the lock held for it and the host memory it needs given back."""
        return self.claim_lock is not None and (self.lowering is None or self.ram_ready)

    async def _keep_ram_low(self) -> None:
        """While a claim stands, the host memory its claimants need kept available: dumps lowered at once,
        and again whenever the available memory falls below it."""
        try:
            while self.claims:
                try:
                    await self.lower()
                except Exception as fault:  # noqa: BLE001 - a fault is said and the claim keeps the card
                    say(f"units: lowering failed: {type(fault).__name__}: {fault}")
                self.ram_ready = True
                await asyncio.sleep(LOWER_POLL_S)
        finally:
            self.ram_ready = True

    async def _back_after_claim(self) -> None:
        """Every unit a claim put out loaded back once the node is given back, tried again while the card has
        not the room."""
        while self.held and not self.claims and self.units is not None and self.units.out():
            try:
                await self.restore(None)
                return
            except units_mod.NoRoom as why:
                say(f"units: not back yet after the claim: {why}")
                await asyncio.sleep(2.0)

    async def _evict_for_claim(self, name: str) -> None:
        """Every unit out for a claim; where that fails the hold is told to YIELD after all."""
        outcomes = await self.evict(None, name, claimant=name)
        if not outcomes or not all(one["ok"] for one in outcomes):
            say(f"{name}: the units could not all be unloaded; the hold is told to YIELD")
            self._tell(f"YIELD {name}")
            self.told_to_yield.set()

    def claim_started(self, name: str, seen: str) -> None:
        """A claimant's claim on the node, `seen` saying what made it: the lent hold's units unloaded where its
        owner can (its lock let go for the claim) and the hold told to YIELD where it cannot, the claimant
        coming before it; dumps kept in host memory lowered as far as the claimant needs; and the node's lock
        taken for the claim. A holder below it is ended past the grace by the keeper."""
        if not self.claim_counts(name):
            say(f"{name}: {seen}, and it has no place among this node's occupants")
            return
        first = not self.claims
        self.claims[name] = time.monotonic()
        say(f"{name}: {seen}; the node gives way")
        if self.coming_back is not None:
            self.coming_back.cancel()
            self.coming_back = None
        # A WANT waiting for its turn waits for the claim's end instead, kept for the return after it.
        if self.wanting is not None and not self.loading:
            self.wanting.cancel()
            self.wanting = None
        loop = asyncio.get_running_loop()
        if self.claim_yields(name) and self.held and self.units is not None and self.units.evictable():
            # Units already out stay where they are; what is still on the card follows them.
            loop.create_task(self._evict_for_claim(name))
        elif self.claim_yields(name):
            if self.holder is not None:
                try:
                    self.holder.write(f"YIELD {name}\n".encode())
                except (ConnectionError, RuntimeError):
                    pass
            if self.held:
                self.told_to_yield.set()
        # Every claimant's ticket waits for the host memory it needs, a later one's for the next look.
        self.ram_ready = False
        if first or self.lowering is None or self.lowering.done():
            self.lowering = loop.create_task(self._keep_ram_low())
        if self.lock_path is not None and self.shutting_out is None:
            self.shutting_out = loop.create_task(self._shut_out())

    def claim_ended(self, name: str) -> None:
        since = self.claims.pop(name, None)
        if since is None:
            return
        say(f"{name}: no ticket, task or process on the card for {CLAIM_SETTLE_S:g} s after {time.monotonic() - since:.0f} s")
        if self.claims:
            return
        if self.units is not None and self.units.out() and self.held:
            self.coming_back = asyncio.get_running_loop().create_task(self._back_after_claim())
        if self.shutting_out is not None:
            self.shutting_out.cancel()
            self.shutting_out = None
        if self.lowering is not None:
            self.lowering.cancel()
            self.lowering = None
        if self.claim_lock is not None:
            os.close(self.claim_lock)
            self.claim_lock = None
        say("claim: the node is given back")

    async def _shut_out(self) -> None:
        """The node's lock taken for as long as a claim stands, as soon as whoever holds it lets go; the
        keeper ends a holder below the claimant once the grace has passed. This process's own lent hold lets
        go of it by itself."""
        assert self.lock_path is not None
        began = time.monotonic()
        fd = os.open(self.lock_path, os.O_RDWR | os.O_CREAT, 0o666)
        try:
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    self.claim_lock, fd = fd, -1
                    say(f"claim: the card lock is held for {self.claimed()}, {time.monotonic() - began:.1f} s after the claim began")
                    return
                except BlockingIOError:
                    pass
                await asyncio.sleep(POLL_S * 4)
        finally:
            if fd >= 0:
                os.close(fd)


def rpc_argv(binary: str, address: str) -> list[str]:
    return [binary, "-H", address, "-p", str(RPC_PORT), "-c"]


@dataclass
class Lending:
    """The card lent to an engine's head: the key it asks with, the address this node waits on, the service
    the card is lent to, the rpc-server and its folders, and whether the hold is packed beside the lock."""
    key: str
    address: str
    service: str
    rpc_server: str
    rpc_cache: Path
    pieces: Path
    packed: bool


LEND_SETTINGS = ("LLM_KEY", "CARD_ADDRESS", "CARD_LEND_SERVICE", "CARD_RPC_SERVER", "CARD_RPC_CACHE", "CARD_SLOT_DIR")


def lending(env: dict[str, str]) -> Lending | str | None:
    """The lend settings, None where the node lends nothing, or what is missing from them."""
    if env.get("LLM_KEY", "") == "" and env.get("CARD_ADDRESS", "") == "":
        return None
    missing = [name for name in LEND_SETTINGS if env.get(name, "") == ""]
    if missing:
        return "lending needs " + " ".join(missing)
    return Lending(env["LLM_KEY"], env["CARD_ADDRESS"], env["CARD_LEND_SERVICE"], env["CARD_RPC_SERVER"],
                   Path(env["CARD_RPC_CACHE"]), Path(env["CARD_SLOT_DIR"]), env.get("CARD_PACKED", "") == "1")


class LocalTurn:
    """A turn this process waits for one poll at a time, the desktop's: asked for when first wanted, asked
    after on every poll, and given back with the lock."""

    def __init__(self, turns: Turns, service: str) -> None:
        self.turns = turns
        self.service = service
        self.ticket: int | None = None

    def want(self) -> bool:
        answered = self.turns.again(self.ticket) if self.ticket is not None else None
        if answered is None:
            answered = self.turns.ask(self.service, local=True)
        self.ticket = None if answered is None else answered["id"]
        return answered is not None and answered["turn"]

    def drop(self) -> None:
        if self.ticket is not None:
            self.turns.done(self.ticket)
            self.ticket = None


async def serve_turns(turns: Turns, port: int, units_api: UnitsApi | None = None) -> None:
    """The hold API on the node's loopback, in a thread of its own, for the process's life. A port that
    cannot be had leaves the queue to this process's own waiters and says so."""
    try:
        server = ThreadingHTTPServer(("127.0.0.1", port), turns_handler(turns, units_api))
    except OSError as fault:
        say(f"turns: the hold API could not listen on 127.0.0.1:{port} ({fault}); occupants asking it take the lock as it comes")
        await asyncio.Event().wait()
        return
    threading.Thread(target=server.serve_forever, daemon=True).start()
    await asyncio.Event().wait()


def store_of(env: dict[str, str]) -> units_mod.Store:
    """The node's dump store, `CARD_STORE` or the one in the arbiter's data folder, and its table beside it
    (docs/placement.md, 「The dump store」)."""
    path = Path(env.get("CARD_STORE", "") or str(Path.home() / ".local/share/bmo-card/store/units.store"))
    store = units_mod.Store(path)
    store.check()
    return store


def version_of() -> str:
    """The version the install wrote beside this file: the commit it was carried from."""
    try:
        return (Path(__file__).resolve().with_name("VERSION")).read_text().strip() or "unknown"
    except OSError:
        return "unknown"


def occupants_path(env: dict[str, str]) -> Path:
    """The node's occupants file, `CARD_OCCUPANTS` or the one beside the arbiter's settings."""
    return Path(env.get("CARD_OCCUPANTS", "") or str(Path.home() / ".config/bmo-card/occupants"))


def rules(card: Card, lend: Lending | None, lock: Path, env: dict[str, str]) -> list[tuple[str, Callable[[], Awaitable[None]]]]:
    """The rules this node keeps, each named, as the settings switch them on, every one under the order
    of the node's occupants."""
    import desktop

    path = occupants_path(env)
    occupants = Occupants(path, path.with_name(f"{path.name}.log"), claimants=lambda: card.claimants)
    # The service the card is lent to, which waits its turn in the queue under that name and which a claimant
    # makes yield only where it comes before it.
    lent_to = lend.service if lend is not None else None
    card.claim_counts = lambda name: occupants.present(name)
    card.claim_yields = lambda name: occupants.outranks(name, lent_to)
    turns = Turns(occupants, lock, card.claimed, claim_holds=card.claim_holds,
                  turn_values=lambda service: card.turn_values.get(service))
    card.turns = turns
    card.lent_to = lent_to or ANY
    seen = desktop.Desktop()
    store = card.units.store if card.units is not None else store_of(env)
    memory = units_mod.CardMemory()

    def on_card(cgroup: Path | None, containers: str | None) -> list[int] | None:
        running = memory.processes()
        return claim_on_card(cgroup, containers, None if running is None else [pid for pid, _ in running])
    # The other owners open their sockets in a folder of the arbiter's own, open to the node account alone.
    owners_folder = Path(env.get("CARD_OWNERS", "") or str(Path.home() / ".local/share/bmo-card/owners"))
    owners_folder.mkdir(parents=True, exist_ok=True)
    owners_folder.chmod(0o700)
    owners = units_mod.Owners(owners_folder, store, lambda service: occupants.rank(service) is not None, lent_to)
    card.owners = owners
    units_api = UnitsApi(card, turns, store, Path(env.get("CARD_RULES", "") or str(Path.home() / ".config/bmo-card/rules.json")),
                         version_of(), memory, desktop=env.get("CARD_DESKTOP", "") == "1", owners=owners)
    # The rules are read before anything else, so the claimants they declare are kept from the start.
    units_api.read_rules()
    occupants.read()

    def claims() -> list[tuple[str, float]]:
        held = list(card.claims.items())
        return held + ([(DESKTOP, seen.since)] if seen.taken and seen.since is not None else [])

    kept: list[tuple[str, Callable[[], Awaitable[None]]]] = [
        (f"occupants {' '.join(occupants.order)} from {path}", occupants.run),
        ("the order kept on the lock's holders", Keeper(occupants, lock, claims).run),
        (f"the hold queue on 127.0.0.1:{TURN_PORT}", lambda: serve_turns(turns, TURN_PORT, units_api)),
        (f"the units{' and the store at ' + str(store.path) if store is not None else ''}", units_api.run),
    ]
    kept.append((f"claims of {' '.join(claimant.name for claimant in card.claimants) or 'the claimants the rules will declare'}",
                 ClaimWatch(lambda: card.claimants, card.claim_started, card.claim_ended, asked=turns.claim_asked,
                            on_card=on_card).run))
    if env.get("CARD_DESKTOP", "") == "1":
        kept.append(("desktop", lambda: desktop.watch_desktop(
            desktop.Nvml(), seen, desktop.DeskLock(lock), lambda: bool(card.claims),
            lambda: bool(lock_holders(lock, os.getpid())), lambda: occupants.present(DESKTOP), LocalTurn(turns, DESKTOP))))
    if lend is not None:
        async def lend_card() -> None:
            server = await asyncio.start_server(card.serve, lend.address, HOLD_PORT)
            async with server:
                await server.serve_forever()
        kept.append((f"lent from {lend.address}:{HOLD_PORT}", lend_card))
    return kept


async def main_async(kept: list[tuple[str, Callable[[], Awaitable[None]]]], lock: Path) -> None:
    say(f"card: the lock is {lock}; " + ("; ".join(name for name, _ in kept) if kept else "no rule but the lock itself"))
    if not kept:
        await asyncio.Event().wait()
    await asyncio.gather(*(run() for _, run in kept))


def main() -> int:
    env = dict(os.environ)
    lend = lending(env)
    if isinstance(lend, str):
        say(f"card: {lend}")
        return 2
    lock = Path(env.get("CARD_LOCK", "") or str(Path.home() / ".gpu.lock"))
    lock.parent.mkdir(parents=True, exist_ok=True)
    if lend is None:
        # Nothing is lent: the card is held by nobody through this process, which keeps the claims alone.
        card = Card("", "", lock, [], Path.home())
    else:
        lend.rpc_cache.mkdir(parents=True, exist_ok=True)
        lend.pieces.mkdir(parents=True, exist_ok=True)
        # llama.cpp keeps the rpc-server's tensor cache under LLAMA_CACHE, and the patched rpc-server writes
        # and reads its slot pieces under GGML_RPC_STATE_DIR.
        os.environ["LLAMA_CACHE"] = str(lend.rpc_cache)
        os.environ["GGML_RPC_STATE_DIR"] = str(lend.pieces)
        card = Card(lend.key, lend.address, lock, rpc_argv(lend.rpc_server, lend.address), lend.pieces, packed=lend.packed)
        # The lent units, unloaded through the control socket their owner opens in the arbiter's own folder:
        # the rpc-server this starts, or the engine of a local hold.
        run = Path.home() / ".local/share/bmo-card/run"
        run.mkdir(parents=True, exist_ok=True)
        run.chmod(0o700)
        card.control = run / "control.sock"
        card.units = units_mod.Units(lend.service, units_mod.Owner(card.control), store_of(env))
    asyncio.run(main_async(rules(card, lend, lock, env), lock))
    return 0


if __name__ == "__main__":
    sys.exit(main())
