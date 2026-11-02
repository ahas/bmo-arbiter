"""The arbiter's desktop rule (docs/node-arbiter.md, 「The desktop」): on a machine whose card is a person's desktop
first, the node's card lock is held for the desktop while a game or anything else on the Windows side
uses the card. The card is read through NVML, opened once for the process's life, since a GPU tool started
afresh on each reading stalls a running game.

A service that never waits for the lock is answered at once while the desktop holds it; one that waits
waits its turn.
"""

from __future__ import annotations

import asyncio
import fcntl
import os
import sys
import time
from collections.abc import Callable
from typing import Any
from dataclasses import dataclass
from pathlib import Path

# A reading a second; the card is the desktop's after five busy readings in a row, and given back at the first
# reading that finds it neither in use nor short of room: what the desktop runs lets both go as it ends.
READ_EVERY_S = float(os.environ.get("DESKTOP_READ_EVERY_S", "1"))
BUSY_READINGS = int(os.environ.get("DESKTOP_BUSY_READINGS", "5"))
# A use at or above this share is the desktop's, and so is less free memory than a service needs to put
# a model and its work beside it.
BUSY_PERCENT = int(os.environ.get("DESKTOP_BUSY_PERCENT", "20"))
ROOM_MIB = int(os.environ.get("DESKTOP_ROOM_MIB", "4096"))
# How long a card that could not be read waits before NVML is opened again.
REOPEN_S = 60.0


def say(line: str) -> None:
    print(line, file=sys.stderr, flush=True)


@dataclass
class Reading:
    percent: int
    used_mib: int
    total_mib: int


class Nvml:
    """The card read through NVML, opened once and kept; None where it cannot be read."""

    def __init__(self) -> None:
        self.handle = None
        self.failed_at: float | None = None

    def read(self) -> Reading | None:
        if self.handle is None:
            if self.failed_at is not None and time.monotonic() - self.failed_at < REOPEN_S:
                return None
            try:
                import pynvml

                pynvml.nvmlInit()
                self.handle = pynvml.nvmlDeviceGetHandleByIndex(0)
                self.failed_at = None
                say("desktop: the card is read through NVML")
            except Exception as fault:  # noqa: BLE001  any failure to open is a card that cannot be read
                self.failed_at = time.monotonic()
                say(f"desktop: NVML did not open ({type(fault).__name__}); the card is left to the desktop")
                return None
        try:
            import pynvml

            use = pynvml.nvmlDeviceGetUtilizationRates(self.handle)
            memory = pynvml.nvmlDeviceGetMemoryInfo(self.handle)
            return Reading(int(use.gpu), int(memory.used) >> 20, int(memory.total) >> 20)
        except Exception as fault:  # noqa: BLE001
            say(f"desktop: the card could not be read ({type(fault).__name__})")
            return None


class Desktop:
    """Whether the card is the desktop's, from one reading at a time. A reading taken while a service of
    ours holds the lock says what that service does and is passed over; a card that cannot be read is
    the desktop's, since its use cannot be told. The first reading that finds the desktop neither using the
    card nor short of room gives it back."""

    def __init__(self, busy_percent: int = BUSY_PERCENT, room_mib: int = ROOM_MIB,
                 busy_readings: int = BUSY_READINGS) -> None:
        self.busy_percent = busy_percent
        self.room_mib = room_mib
        self.busy_readings = busy_readings
        self.streak = 0
        self.taken = False
        self.why = ""

    def busy(self, reading: Reading) -> str | None:
        if reading.percent >= self.busy_percent:
            return f"its use at {reading.percent}%"
        free = reading.total_mib - reading.used_mib
        if free < self.room_mib:
            return f"{free} MiB of it free"
        return None

    def see(self, reading: Reading | None, ours_on_card: bool) -> bool:
        """The reading weighed, and whether the card is the desktop's now."""
        if ours_on_card:
            return self.taken
        why = "it cannot be read" if reading is None else self.busy(reading)
        if why is not None:
            self.streak += 1
            if not self.taken and (reading is None or self.streak >= self.busy_readings):
                self.taken = True
                self.why = why
            return self.taken
        self.streak = 0
        self.taken = False
        return self.taken


class DeskLock:
    """The node's card lock, held for the desktop on a file of its own, taken without waiting: a service's
    hold lets go within its own use, and the arbiter never ends one for the desktop."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.fd: int | None = None

    def take(self) -> bool:
        if self.fd is not None:
            return True
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o666)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(fd)
            return False
        self.fd = fd
        return True

    def let_go(self) -> None:
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None


class NoQueue:
    """The turn of a node that keeps no hold queue: the desktop takes the lock whenever it is free."""

    def want(self) -> bool:
        return True

    def drop(self) -> None:
        pass


def desk_step(reading: Reading | None, ours_on_card: bool, claimed: bool, desktop: Desktop, desk: DeskLock,
              counts: bool = True, turn: Any = None) -> None:
    """One reading weighed and the desktop's lock taken or let go after it. A claimant's claim holds the
    lock from inside this same process, where the kernel's table cannot tell its hold from the desktop's,
    so the two ask each other directly: while a claim stands the desktop's hold is handed over at once and
    the card's use is the claimant's, never the desktop's. Where the node keeps a hold queue, the desktop
    waits its turn in it like any occupant (`turn`) before it takes the lock, and gives the turn back with
    the lock."""
    turn = turn or NoQueue()
    if claimed:
        if desk.fd is not None:
            desk.let_go()
            say("desktop: a claim on the card stands; the desktop's hold is handed over to it")
        turn.drop()
        return
    # The desktop with no place among the node's occupants is read all the same and holds nothing.
    wanted = desktop.see(reading, ours_on_card) and counts
    if wanted and desk.fd is None:
        if turn.want() and desk.take():
            say(f"desktop: the desktop has the card ({desktop.why}); the lock is held for it")
    elif not wanted:
        if desk.fd is not None:
            desk.let_go()
            say("desktop: the desktop has left the card; the lock is let go")
        turn.drop()


async def watch_desktop(nvml: Nvml, desktop: Desktop, desk: DeskLock, claimed: Callable[[], bool],
                        ours_on_card: Callable[[], bool], counts: Callable[[], bool] = lambda: True,
                        turn: Any = None) -> None:
    """A reading a second, for the process's life. `ours_on_card` says whether another process holds the
    lock, which is a service of ours on the card; `counts` whether the desktop has a place among the
    node's occupants now."""
    while True:
        desk_step(nvml.read(), ours_on_card(), claimed(), desktop, desk, counts(), turn)
        await asyncio.sleep(READ_EVERY_S)
