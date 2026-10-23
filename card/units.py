"""The units on this node's card, the dump store they are unloaded to, the owners that unload them, and
what the node's rules say of them (docs/placement.md).

A unit is one thing in the card's memory unloaded and loaded whole. Its owner, the process whose card
memory it is, does the copying at this arbiter's word: an rpc-server lent to an engine, or an engine using
the card itself, asked over the Unix socket its CUDA backend opens when started evictable. A unit goes out
to one of two places, as the node's rules say: a slot of the dump store, or the owner's host memory, from
which it is later written to its slot, or dropped where the slot already holds it. The store is one file
on the node's NVMe, allocated whole by the install; this module keeps its table of slots, one fixed range
per buffer, and never grows or moves it.
"""

from __future__ import annotations

import asyncio
import json
import re
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

# Every slot starts on a boundary of this many bytes, so a slot is read with direct I/O whole.
SLOT_ALIGN = 2 << 20
# What each kind of buffer an owner reports is, as a part of its service's units; `stage` is all one stage of a
# pipeline keeps on the card, which its owner unloads whole by stopping it.
PARTS = ("weights", "store", "compute", "stage")
# Where a buffer is: on the card, kept in its owner's host memory, or in its slot of the store.
RESIDENT, RAM, EVICTED = "resident", "ram", "evicted"
# The two places a unit is unloaded to, as the rules name them.
TO_RAM, TO_DISK = "ram", "disk"
ASK_WAIT_S = 120.0
GIB = 2**30
# The floor a claim that wants every dump out of host memory sets: no host memory is ever that available.
NO_RAM = 1 << 62


class StoreFull(Exception):
    """The store has no free range long enough for a buffer's slot."""


class NoRoom(Exception):
    """The card has not the memory to load a unit back."""


def extents(path: Path) -> int | None:
    """How many extents the file lies in, as filefrag counts them; None where it cannot say."""
    try:
        said = subprocess.run(["filefrag", str(path)], capture_output=True, text=True, timeout=30).stdout
    except (OSError, subprocess.TimeoutExpired):
        return None
    found = re.search(r":\s+(\d+) extents? found", said)
    return int(found.group(1)) if found else None


def memory_available(meminfo: Path = Path("/proc/meminfo")) -> int | None:
    """The host memory the kernel counts as available to a new program, in bytes; None where unread."""
    try:
        for line in meminfo.read_text().splitlines():
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        return None
    return None


def aligned(length: int) -> int:
    return -(-length // SLOT_ALIGN) * SLOT_ALIGN


@dataclass(frozen=True)
class SlotNeed:
    """What a slot's owner says of it: the bytes the buffer its key names needs now, none where no buffer
    has the key; whether that buffer lies in the slot, its only copy; and whether the slot holds its
    contents as they are."""
    length: int
    lies_there: bool = False
    clean: bool = False


class Store:
    """The node's dump store and its table of slots: each buffer's key and the offset and length of the
    range it is written to, kept in `slots.json` beside the store and written whole on every change.

    Where no free range is long enough for a slot, room is made out of the slots their owners can spare
    (docs/placement.md, 「The dump store」): each owner judges the slots its service's keys name, and a slot
    handed out for a move under way is judged by nobody until the move gives it back."""

    def __init__(self, path: Path, table: Path | None = None) -> None:
        self.path = path
        self.table = table or path.with_name("slots.json")
        self.slots: dict[str, dict[str, int]] = {}
        self.extents: int | None = None
        self.error: str | None = None
        # Each owner's judgement of its own slots, by its service; None for a key it cannot judge now.
        self.judges: dict[str, Callable[[str], SlotNeed | None]] = {}
        # The slots handed out to moves still under way, each with how many hold it.
        self.moving: dict[str, int] = {}
        try:
            kept = json.loads(self.table.read_text())
            self.slots = {key: {"offset": int(slot["offset"]), "length": int(slot["length"])} for key, slot in kept.items()}
        except (OSError, ValueError, KeyError, TypeError, AttributeError):
            self.slots = {}

    def size(self) -> int:
        try:
            return self.path.stat().st_size
        except OSError:
            return 0

    def check(self) -> None:
        """Whether the store is there, and how many extents it lies in."""
        if not self.path.is_file():
            self.error = f"{self.path} is not there; the arbiter's install makes it"
            self.extents = None
            return
        self.error = None
        self.extents = extents(self.path)

    def slot(self, key: str, length: int) -> int:
        """The offset of the key's slot, at least `length` long, held for the move asking until `release`:
        its own where it is long enough, else the first free range that is, the key's old slot given up and
        room made where none is free."""
        length = aligned(length)
        kept = self.slots.get(key)
        if kept is not None and kept["length"] >= length:
            self.moving[key] = self.moving.get(key, 0) + 1
            return kept["offset"]
        if self.slots.pop(key, None) is not None:
            self._save()
        at = self._free(length)
        for spare in (self._spare_unused, self._spare_dirty, self._spare_clean):
            if at is not None:
                break
            if spare():
                self._save()
                at = self._free(length)
        if at is None:
            raise StoreFull(f"no free {length} bytes in {self.path} ({self.size()} bytes, {len(self.slots)} slots, "
                            f"{sum(slot['length'] for slot in self.slots.values())} bytes in them)")
        self.slots[key] = {"offset": at, "length": length}
        self.moving[key] = self.moving.get(key, 0) + 1
        self._save()
        return at

    def release(self, keys: set[str] | list[str]) -> None:
        """The slots a move was handed, judged by their owners again once no other move holds them."""
        for key in keys:
            held = self.moving.get(key, 0) - 1
            if held > 0:
                self.moving[key] = held
            else:
                self.moving.pop(key, None)

    def _free(self, length: int) -> int | None:
        taken = sorted((slot["offset"], slot["offset"] + slot["length"]) for slot in self.slots.values())
        at = 0
        for start, end in taken:
            if start - at >= length:
                break
            at = max(at, end)
        return at if at + length <= self.size() else None

    def _needs(self) -> dict[str, SlotNeed]:
        """Every slot an owner can judge now, with its judgement; a slot of a move under way is none."""
        found = {}
        for key in self.slots:
            judge = self.judges.get(key.partition("/")[0])
            need = judge(key) if judge is not None and key not in self.moving else None
            if need is not None:
                found[key] = need
        return found

    def _spare_unused(self) -> bool:
        """Every slot cut to what its buffer needs, its start kept, and every slot no buffer has given up;
        whether anything was."""
        changed = False
        for key, need in self._needs().items():
            if need.length <= 0:
                del self.slots[key]
                changed = True
            elif aligned(need.length) < self.slots[key]["length"]:
                self.slots[key]["length"] = aligned(need.length)
                changed = True
        return changed

    def _spare_dirty(self) -> bool:
        """The slots of buffers not lying in them that do not hold their contents given up."""
        return self._give_up(lambda need: not need.lies_there and not need.clean)

    def _spare_clean(self) -> bool:
        """The slots of buffers not lying in them given up, the next unload of each writing it again."""
        return self._give_up(lambda need: not need.lies_there)

    def _give_up(self, spared: Callable[[SlotNeed], bool]) -> bool:
        gone = [key for key, need in self._needs().items() if spared(need)]
        for key in gone:
            del self.slots[key]
        return bool(gone)

    def forget(self, keep: Callable[[str], bool]) -> None:
        """Every slot whose key `keep` does not keep given up."""
        gone = [key for key in self.slots if not keep(key)]
        for key in gone:
            del self.slots[key]
        if gone:
            self._save()

    def _save(self) -> None:
        partial = self.table.with_name(f"{self.table.name}.partial")
        partial.write_text(json.dumps(self.slots, indent=1, sort_keys=True))
        partial.replace(self.table)

    def state(self) -> dict:
        return {"path": str(self.path), "bytes": self.size(), "extents": self.extents, "slotsUsed": len(self.slots),
                "bytesUsed": sum(slot["length"] for slot in self.slots.values()), "error": self.error}


@dataclass(frozen=True)
class Buffer:
    """One buffer as the owner lists it: where it is, whether its slot holds its contents, and the host
    memory it takes while kept there."""
    id: int
    device: int
    bytes: int
    slot_bytes: int
    part: str
    state: str
    clean: bool
    ram_bytes: int = 0


@dataclass(frozen=True)
class Stage:
    """The owner's door: buffers out, whether a use of the card waits there and how long, and the waits
    so far."""
    out: int = 0
    waiting: bool = False
    waiting_s: float = 0.0
    graphs: int = 0
    pools_released: int = 0
    waits: int = 0
    waited_s: float = 0.0


def parse_listing(lines: list[str]) -> tuple[list[Buffer], Stage] | None:
    """The owner's LIST answer, or None where it holds no buffer yet."""
    buffers: list[Buffer] = []
    stage = Stage()
    for line in lines:
        words = line.split()
        if not words:
            continue
        if words[0] == "NOCLIENT":
            return None
        if words[0] == "BUFFER" and len(words) in (8, 9):
            buffers.append(Buffer(int(words[1]), int(words[2]), int(words[3]), int(words[4]), words[5], words[6],
                                  words[7] == "clean", int(words[8]) if len(words) == 9 else 0))
        elif words[0] == "STATE" and len(words) == 8:
            stage = Stage(int(words[1]), words[2] == "1", float(words[3]), int(words[4]), int(words[5]), int(words[6]), float(words[7]))
    return buffers, stage


class Owner:
    """The owner's control socket, asked one line at a time."""

    def __init__(self, socket: Path, wait_s: float = ASK_WAIT_S) -> None:
        self.socket = socket
        self.wait_s = wait_s
        self.guard = asyncio.Lock()

    def present(self) -> bool:
        return self.socket.is_socket()

    async def ask(self, line: str, until_end: bool = False) -> list[str]:
        async with self.guard:
            reader, writer = await asyncio.wait_for(asyncio.open_unix_connection(str(self.socket)), 5.0)
            try:
                writer.write(f"{line}\n".encode())
                await writer.drain()
                answer: list[str] = []
                while True:
                    said = (await asyncio.wait_for(reader.readline(), self.wait_s)).decode(errors="replace").strip()
                    answer.append(said)
                    if not until_end or said in ("END", "") or said.startswith(("NOCLIENT", "ERROR")):
                        return answer
            finally:
                writer.close()


@dataclass
class Timing:
    ms: float
    bytes: int
    at: float
    to: str | None = None

    def shown(self) -> dict:
        """A move as the card shows it (docs/api.md, 「Card units」): how long it took, how much it moved, when it
        ended in epoch milliseconds, and where an unload went."""
        return {"tookMs": round(self.ms), "bytes": self.bytes, "at": round(self.at * 1000), "to": self.to}


@dataclass(frozen=True)
class Placing:
    """Where the node's rules put a unit when it goes out, and the host memory they let its dumps take:
    `dump` for each part (the rules' unit entries, else the node's own default), the most every dump kept
    in host memory may take together, and the host memory that must stay available besides."""
    dump: dict[str, str] = field(default_factory=dict)
    default: str = TO_DISK
    ram_max_bytes: int | None = None
    floor_bytes: int = 0
    # The host memory every dump on the node takes now, every owner's; None counts the moving owner's alone.
    kept_bytes: int | None = None

    def of(self, part: str) -> str:
        return self.dump.get(part, self.default)


@dataclass
class Units:
    """One service's units on this card, by part, as their owner lists its buffers; each part's buffers
    keyed in the store by the part and their order, so a buffer of the same layout finds its slot again
    and weights already written are not written again."""

    service: str
    owner: Owner
    store: Store | None
    clock: Callable[[], float] = time.time
    available: Callable[[], int | None] = memory_available
    buffers: list[Buffer] = field(default_factory=list)
    stage: Stage = field(default_factory=Stage)
    connected: bool = False
    since: dict[str, float] = field(default_factory=dict)
    last_unload: dict[str, Timing] = field(default_factory=dict)
    last_load: dict[str, Timing] = field(default_factory=dict)
    last_lower: dict[str, Timing] = field(default_factory=dict)
    # Why the units are out, where they are: a claim on the card, or what the master said.
    reason: str | None = None

    def __post_init__(self) -> None:
        if self.store is not None:
            self.store.judges[self.service] = self.slot_need

    def slot_need(self, key: str) -> SlotNeed | None:
        """What this owner's buffers, as last listed, say of the slot a key of its service names; None where
        the owner is not answering."""
        if not self.connected:
            return None
        part, _, index = key.partition("/")[2].partition("#")
        buffers = self.of(part)
        if not index.isdigit() or int(index) >= len(buffers):
            return SlotNeed(0)
        buffer = buffers[int(index)]
        return SlotNeed(buffer.slot_bytes, buffer.state == EVICTED, buffer.clean)

    async def refresh(self) -> None:
        if not self.owner.present():
            self.buffers, self.stage, self.connected = [], Stage(), False
            return
        try:
            listed = parse_listing(await self.owner.ask("LIST", until_end=True))
        except (OSError, asyncio.TimeoutError, ValueError):
            listed = None
        if listed is None:
            self.buffers, self.stage, self.connected = [], Stage(), False
            return
        before = {part: self.part_state(part) for part in PARTS}
        self.buffers, self.stage = listed
        self.connected = True
        for part in PARTS:
            if self.part_state(part) != before[part]:
                self.since[part] = self.clock()

    def evictable(self) -> bool:
        return self.connected and self.store is not None and self.store.error is None and bool(self.buffers)

    def of(self, part: str) -> list[Buffer]:
        return [buffer for buffer in self.buffers if buffer.part == part]

    def part_state(self, part: str) -> str | None:
        states = {buffer.state for buffer in self.of(part)}
        if not states:
            return None
        return states.pop() if len(states) == 1 else "mixed"

    def key(self, part: str, buffer: Buffer) -> str:
        return f"{self.service}/{part}#{[one.id for one in self.of(part)].index(buffer.id)}"

    def parts_named(self, names: list[str] | None) -> list[str]:
        """The parts the names ask for (`<service>/<part>`, or every part where none is named)."""
        if not names:
            return [part for part in PARTS if self.of(part)]
        wanted = []
        for name in names:
            service, _, part = name.partition("/")
            if service == self.service and part in PARTS and self.of(part) and part not in wanted:
                wanted.append(part)
        return wanted

    def ram_bytes(self) -> int:
        """The host memory every buffer kept there takes."""
        return sum(buffer.ram_bytes for buffer in self.buffers if buffer.state == RAM)

    def to_ram(self, placing: Placing, part: str, granted: int) -> bool:
        """Whether a part going out now is kept in host memory: the rules put it there, and with what is
        already kept and what this move has granted so far it stays within the most dumps may take and
        leaves the floor available. A part's whole size is counted, though only its written chunks are kept."""
        if placing.of(part) != TO_RAM:
            return False
        size = sum(buffer.bytes for buffer in self.of(part))
        kept = self.ram_bytes() if placing.kept_bytes is None else placing.kept_bytes
        if placing.ram_max_bytes is not None and kept + granted + size > placing.ram_max_bytes:
            return False
        available = self.available()
        return available is not None and available - granted - size >= placing.floor_bytes

    async def evict(self, names: list[str] | None, reason: str, placing: Placing | None = None) -> list[dict]:
        """The named parts unloaded, each to where the placing puts it: kept in the owner's host memory, or
        each buffer written to its slot; each part's outcome. Every slot is found before anything moves, so
        a store without the room fails the whole unload with nothing moved."""
        await self.refresh()
        if not self.evictable():
            return [{"name": name, "ok": False, "error": "nothing here can be unloaded"} for name in names or [self.service]]
        assert self.store is not None
        placing = placing or Placing()
        planned: list[tuple[str, str]] = []
        granted = 0
        for part in self.parts_named(names):
            to = TO_RAM if self.to_ram(placing, part, granted) else TO_DISK
            if to == TO_RAM:
                granted += sum(buffer.bytes for buffer in self.of(part))
            planned.append((part, to))
        offsets: dict[int, int] = {}
        held: list[str] = []
        try:
            for part, to in planned:
                for buffer in self.of(part):
                    if to == TO_DISK and buffer.state == RESIDENT:
                        held.append(self.key(part, buffer))
                        offsets[buffer.id] = self.store.slot(held[-1], buffer.slot_bytes)
        except StoreFull as fault:
            self.store.release(held)
            return [{"name": f"{self.service}/{part}", "ok": False, "ms": 0.0, "bytes": 0, "to": to,
                     "error": f"StoreFull: {fault}"} for part, to in planned]
        try:
            outcomes = [await self._unload(part, to, offsets, reason) for part, to in planned]
            await self.refresh()
        finally:
            # Released once the listing says where the buffers now are, so none just written is judged spare.
            self.store.release(held)
        return outcomes

    async def _unload(self, part: str, to: str, offsets: dict[int, int], reason: str) -> dict:
        """One part's buffers still on the card unloaded, to host memory or each to the slot found for it."""
        assert self.store is not None
        began, moved, error = self.clock(), 0, None
        for buffer in self.of(part):
            if buffer.state != RESIDENT:
                continue
            line = f"EVICT {buffer.id} ram" if to == TO_RAM else f"EVICT {buffer.id} {self.store.path} {offsets[buffer.id]}"
            try:
                said = (await self.owner.ask(line))[0].split()
            except (OSError, asyncio.TimeoutError) as fault:
                error = f"{type(fault).__name__}: {fault}"
                break
            if said[:1] != ["EVICTED"]:
                error = " ".join(said) or "the owner answered nothing"
                break
            moved += int(said[2])
        took = (self.clock() - began) * 1000
        if error is None:
            self.last_unload[part] = Timing(took, moved, self.clock(), to)
            self.reason = reason
        return {"name": f"{self.service}/{part}", "ok": error is None, "ms": round(took, 1), "bytes": moved, "to": to, "error": error}

    async def lower(self, enough: Callable[[], bool]) -> list[dict]:
        """Buffers kept in host memory written to their slots, or dropped where the slot already holds
        them, until `enough` says enough host memory is available: those whose slot is clean first, since
        they cost nothing, then the largest. The card is not touched. Each buffer's outcome."""
        await self.refresh()
        if self.store is None:
            return []
        kept = sorted((buffer for buffer in self.buffers if buffer.state == RAM), key=lambda one: (not one.clean, -one.ram_bytes))
        outcomes = []
        for buffer in kept:
            if enough():
                break
            began = self.clock()
            key = self.key(buffer.part, buffer)
            try:
                offset = self.store.slot(key, buffer.slot_bytes)
                said = (await self.owner.ask(f"LOWER {buffer.id} {self.store.path} {offset}"))[0].split()
                error = None if said[:1] == ["LOWERED"] else (" ".join(said) or "the owner answered nothing")
            except (OSError, asyncio.TimeoutError, StoreFull) as fault:
                said, error = [], f"{type(fault).__name__}: {fault}"
            took = (self.clock() - began) * 1000
            written = int(said[2]) if error is None else 0
            if error is None:
                self.last_lower[buffer.part] = Timing(took, written, self.clock())
            outcomes.append({"name": f"{self.service}/{buffer.part}", "buffer": buffer.id, "ok": error is None, "ms": round(took, 1),
                             "freed": buffer.ram_bytes, "written": written, "error": error})
            await self.refresh()
            # Released once the listing says the buffer lies in its slot; one left held is only never judged.
            self.store.release([key])
        return outcomes

    async def restore(self, names: list[str] | None) -> list[dict]:
        """The named parts loaded back, from host memory or their slots; NoRoom where the card has not
        the memory."""
        await self.refresh()
        if not self.connected or self.store is None:
            return []
        outcomes = []
        for part in self.parts_named(names):
            began, read, error = self.clock(), 0, None
            for buffer in self.of(part):
                if buffer.state == RESIDENT:
                    continue
                kept = self.store.slots.get(self.key(part, buffer))
                offset = kept["offset"] if kept is not None else 0
                said = (await self.owner.ask(f"RESTORE {buffer.id} {self.store.path} {offset}"))[0].split()
                if said[:1] == ["NOROOM"]:
                    await self.refresh()
                    raise NoRoom(f"{self.service}/{part}")
                if said[:1] != ["RESTORED"]:
                    error = " ".join(said) or "the owner answered nothing"
                    break
                read += int(said[2])
            took = (self.clock() - began) * 1000
            if error is None:
                self.last_load[part] = Timing(took, read, self.clock())
            outcomes.append({"name": f"{self.service}/{part}", "ok": error is None, "ms": round(took, 1), "bytes": read, "error": error})
        await self.refresh()
        if not self.out():
            self.reason = None
        return outcomes

    def out(self) -> bool:
        return any(buffer.state != RESIDENT for buffer in self.buffers)

    def shown(self) -> list[dict]:
        units = []
        for part in PARTS:
            buffers = self.of(part)
            if not buffers:
                continue
            slots = self.store.slots if self.store is not None else {}
            state = self.part_state(part)
            since = self.since.get(part)
            units.append({
                "name": f"{self.service}/{part}", "holderName": self.service, "part": part,
                "evictable": self.evictable(), "state": state, "sinceAt": None if since is None else round(since * 1000),
                "vramMiB": round(sum(buffer.bytes for buffer in buffers) / 2**20),
                "ramMiB": round(sum(buffer.ram_bytes for buffer in buffers if buffer.state == RAM) / 2**20),
                "buffers": [{"id": buffer.id, "bytes": buffer.bytes, "state": buffer.state, "clean": buffer.clean,
                             "ramBytes": buffer.ram_bytes, "slot": slots.get(self.key(part, buffer))} for buffer in buffers],
                "lastUnload": self.last_unload[part].shown() if part in self.last_unload else None,
                "lastLoad": self.last_load[part].shown() if part in self.last_load else None,
                "lastLower": self.last_lower[part].shown() if part in self.last_lower else None,
                "reason": self.reason if state != RESIDENT else None,
                "door": None,
            })
        return units

    def stage_shown(self) -> dict:
        """The owner's door as the card shows it, its spans in whole milliseconds."""
        return {"waiting": self.stage.waiting, "waitingMs": round(self.stage.waiting_s * 1000), "graphs": self.stage.graphs,
                "waits": self.stage.waits, "waitedMs": round(self.stage.waited_s * 1000)}


class Owners:
    """The owners of units besides the one the card is lent through (docs/placement.md, 「Unloading and loading a
    unit」): each opens its control socket in the arbiter's folder as `<service>.sock`, and those whose service has
    a place among the node's occupants are read, their units keyed in the shared store under their own service."""

    def __init__(self, folder: Path, store: Store | None, placed: Callable[[str], bool], lent: str | None,
                 owner_of: Callable[[Path], Owner] = Owner) -> None:
        self.folder = folder
        self.store = store
        self.placed = placed
        self.lent = lent
        self.owner_of = owner_of
        self.units: dict[str, Units] = {}

    def discover(self) -> None:
        """The sockets standing now: an owner kept while its socket stands, one gone forgotten."""
        try:
            sockets = sorted(path for path in self.folder.glob("*.sock") if path.is_socket())
        except OSError:
            sockets = []
        found: dict[str, Units] = {}
        for path in sockets:
            service = path.stem
            if service == self.lent or not self.placed(service):
                continue
            found[service] = self.units.get(service) or Units(service, self.owner_of(path), self.store)
        self.units = found

    async def refresh(self) -> None:
        self.discover()
        for one in self.units.values():
            await one.refresh()

    def shown(self) -> list[dict]:
        """Every unit of these owners, each with its own owner's door, which the top-level `stage` never is."""
        return [{**unit, "door": one.stage_shown()} for one in self.units.values() for unit in one.shown()]

    def split(self, names: list[str] | None) -> tuple[list[str] | None, dict[str, list[str]]]:
        """The names of the lent units, and the rest by the service of the owner they are this one's; every
        unit is the lent owner's where no name is given."""
        if names is None:
            return None, {}
        lent: list[str] = []
        others: dict[str, list[str]] = {}
        for name in names:
            service = name.partition("/")[0]
            if service in self.units:
                others.setdefault(service, []).append(name)
            else:
                lent.append(name)
        return lent, others


class CardMemory:
    """The card's memory as the driver says, through NVML opened once for the process's life; None where
    it cannot be read."""

    def __init__(self) -> None:
        self.handle = None
        self.name: str | None = None
        try:
            import pynvml
            pynvml.nvmlInit()
            self.handle = pynvml.nvmlDeviceGetHandleByIndex(0)
            name = pynvml.nvmlDeviceGetName(self.handle)
            self.name = name.decode() if isinstance(name, bytes) else str(name)
        except Exception:  # noqa: BLE001 - a card NVML cannot open is shown as unread
            self.handle = None

    def read(self) -> dict | None:
        if self.handle is None:
            return None
        try:
            import pynvml
            memory = pynvml.nvmlDeviceGetMemoryInfo(self.handle)
        except Exception:  # noqa: BLE001
            return None
        return {"name": self.name, "totalMiB": round(memory.total / 2**20), "usedMiB": round(memory.used / 2**20),
                "freeMiB": round(memory.free / 2**20)}

    def processes(self) -> list[tuple[int, int]] | None:
        """Every compute process on the card with the memory the driver counts for it, in bytes; None where
        the driver does not say (a WSL machine, a driver without per-process accounting)."""
        if self.handle is None:
            return None
        try:
            import pynvml
            running = pynvml.nvmlDeviceGetComputeRunningProcesses(self.handle)
        except Exception:  # noqa: BLE001 - a driver that cannot say is shown as unread
            return None
        found = [(int(one.pid), int(one.usedGpuMemory)) for one in running if isinstance(getattr(one, "usedGpuMemory", None), int)]
        return found if found or not running else None


def process_name(pid: int, proc: Path = Path("/proc")) -> str:
    """A process's short name as the kernel keeps it, or its pid where it is gone or not ours to read."""
    try:
        return (proc / str(pid) / "comm").read_text().strip() or str(pid)
    except OSError:
        return str(pid)


def processes_shown(running: list[tuple[int, int]] | None, service_of: Callable[[int], str | None],
                    name_of: Callable[[int], str] = process_name) -> list[dict] | None:
    """The card's processes as the slave answers them: each with its name, the service it is known as, or
    None, and its memory."""
    if running is None:
        return None
    return [{"pid": pid, "name": name_of(pid), "serviceName": service_of(pid), "usedMiB": round(used / 2**20)} for pid, used in running]


def read_rules(path: Path) -> tuple[dict | None, float | None, str | None]:
    """The node's part of the placement rules as pushed (docs/placement.md, 「The rules」): its content,
    when it was read, and why it could not be."""
    try:
        return json.loads(path.read_text()), time.time(), None
    except FileNotFoundError:
        return None, None, None
    except (OSError, ValueError) as fault:
        return None, time.time(), f"{type(fault).__name__}: {fault}"


@dataclass(frozen=True)
class Claimant:
    """An occupant that takes the card outright when it comes, as the rules declare it: whether it stands
    first on every node whatever the node's order says, how it is seen — its tickets in the hold, its
    running containers named by a prefix, and its processes on the card, those in its containers or in a
    unit's cgroup — and the host memory it needs available while its claim stands, None for every dump
    kept there lowered."""
    name: str
    first: bool = True
    tickets: bool = True
    cgroup: str | None = None
    containers: str | None = None
    ram_free_bytes: int | None = 0


def claimants_of(rules: dict | None) -> list[Claimant]:
    """The claimants the node's rules declare, in their order; an entry not understood is passed over."""
    found = []
    for one in (rules or {}).get("occupants") or []:
        if not isinstance(one, dict) or not isinstance(one.get("name"), str) or not one.get("claims"):
            continue
        seen = one.get("seenBy") if isinstance(one.get("seenBy"), dict) else {}
        free = one.get("ramFree", 0)
        found.append(Claimant(
            one["name"], first=one.get("first", True) is not False, tickets=seen.get("tickets", True) is not False,
            cgroup=seen.get("cgroup") if isinstance(seen.get("cgroup"), str) else None,
            containers=seen.get("containers") if isinstance(seen.get("containers"), str) else None,
            ram_free_bytes=None if free == "all" else int(float(free) * GIB) if isinstance(free, (int, float)) else 0))
    return found


@dataclass(frozen=True)
class Turn:
    """A service's turn values as the rules carry them (docs/node-arbiter.md, 「One rank taking turns」): how long its
    turn is in use before it gives way to a waiter of its rank, and whether it lets go in the middle of a
    request rather than at its boundary."""
    yield_after_s: float
    can_pause: bool


def turns_of(rules: dict | None) -> dict[str, Turn]:
    """The turn values the node's rules carry, by service; an entry not understood is passed over, and a
    service the rules do not name has none."""
    found: dict[str, Turn] = {}
    turns = (rules or {}).get("turns")
    for service, one in (turns.items() if isinstance(turns, dict) else []):
        if not isinstance(one, dict) or not isinstance(one.get("canPause"), bool):
            continue
        after = one.get("yieldAfterS")
        if isinstance(after, bool) or not isinstance(after, (int, float)) or after < 0:
            continue
        found[service] = Turn(float(after), one["canPause"])
    return found


def placing_of(rules: dict | None, service: str, floor_bytes: int = 0) -> Placing:
    """Where the node's rules put the service's units when they go out (docs/placement.md, 「The rules」):
    each unit entry naming one of its parts, else the node's group; and the most host memory its dumps
    may take together."""
    rules = rules or {}
    groups = [group for group in rules.get("groups") or [] if isinstance(group, dict)]
    group = groups[0] if groups else {}
    dump: dict[str, str] = {}
    for spec in rules.get("units") or []:
        if not isinstance(spec, dict) or spec.get("dump") not in (TO_RAM, TO_DISK) or not isinstance(spec.get("unit"), str):
            continue
        pattern = re.compile("^" + ".*".join(re.escape(piece) for piece in spec["unit"].split("*")) + "$")
        for part in PARTS:
            if part not in dump and pattern.match(f"{service}/{part}"):
                dump[part] = spec["dump"]
    ram_max = group.get("ramMaxGiB")
    return Placing(dump, group.get("dump") if group.get("dump") in (TO_RAM, TO_DISK) else TO_DISK,
                   None if not isinstance(ram_max, (int, float)) else int(ram_max * GIB), floor_bytes)
