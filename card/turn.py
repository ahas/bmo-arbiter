"""Waiting a turn for the card through the node's card arbiter (docs/node-arbiter.md, 「The hold」), for an occupant
that runs in a process of its own: an engine serving calls, a server answering calls, a training run.

An occupant asks the arbiter on the node's loopback for a ticket under its own service name (docs/api.md,
「Tickets」). The answer is 201 with `turn: true` where the card is its now, or 202 with its place in the queue
and how long the wait is counted at; the answer is held open until the turn comes or a while passes, so the
occupant hears of its turn the moment the card is handed on, then takes the node's card lock itself and gives
the ticket back once it has let go. A turn is waited for for as long as it takes, and every ask waits for the
arbiter's answer. An occupant serving requests one after another asks after each whether its next request
still stands first (`keep`), and carries on with the card where it does (docs/node-arbiter.md, 「One queue of
requests」). Only a node where nothing answers at the arbiter's address keeps no queue, and there the
occupant takes the lock as it comes. An arbiter that answers, even with a refusal or a fault, is one whose
node keeps a queue: the occupant asks again and never takes the lock outside it (docs/node-arbiter.md,
「The hold」).

Run as a command, it holds a turn and the lock around another command:

    python turn.py --service <name> [--lock ~/.gpu.lock] [--state <file>] -- <command> [<argument> …]

While the command runs, a SIGTERM is passed on to it and waited out, so a stop reaches the command once and
its own way of ending runs to the end; SIGINT and SIGHUP, which a terminal sends to the command as well,
are left to the command. With `--state <file>`, the card is lent while the command keeps running: SIGUSR1
lets the lock and the ticket go, SIGUSR2 asks for a turn again and takes the lock back, and the file says
`held` or `lent` once each is done, and is removed once the command has ended. It ends as the command ended:
with its code, or, where a signal ended the command, by the same signal once the lock and the ticket are let go.

An occupant that can let go of the card in the middle of its work holds a turn that gives way instead
(docs/node-arbiter.md, 「Giving way」): `GivingTurn` asks for it with the seconds letting go takes, watches its
ticket while the card is held, and says once an occupant above it wants the card; `quiet` waits until
the card has stood unasked for a while before it is asked for again.

An occupant with nothing left to serve rests on the card with its weights there (docs/node-arbiter.md, 「Resting」):
`rest` says so of its turn, `watch` holds an ask open until it is to give way, and a ticket asked with
`rest` stands only for coming back to the card once it is free.
"""

from __future__ import annotations

import argparse
import fcntl
import http.client
import json
import os
import queue
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

PORT = int(os.environ.get("CARD_TURN_PORT", "7439"))
# How long a waiter has its answer held open at most before it asks again.
HOLD_OPEN_S = 20.0
# How long a waiter pauses before asking again where an answer came back early with no turn: an arbiter
# that holds no answer open, or a lock taken in the moment between by a process that asks for no turns.
AGAIN_S = 0.5


class ArbiterFault(Exception):
    """The arbiter answered, but not with a ticket: a status other than 200, 201 and 202, an answer that
    cannot be read, or a connection that broke after it was taken. The node keeps a queue, so the occupant
    keeps its place and asks again; it takes the lock as it comes only where nothing answers at all
    (docs/node-arbiter.md, 「The hold」)."""


@dataclass
class Answer:
    """What the arbiter said of a ticket: the card is the occupant's now, or where it stands, how long its
    wait is counted at and the claimant the card waits for, if any; and for a turn that gives way, who it
    is to let go for."""
    turn: bool
    position: int | None = None
    wait_s: float | None = None
    give_way_for: str | None = None
    claim: str | None = None
    # Whether the turn rests, its weights on the card with nothing left to serve; an arbiter that keeps
    # no rests never says so.
    resting: bool = False


class TurnClient:
    """One occupant's ticket at the node's arbiter, kept across asks and given back after the lock. With
    `gives_way_s` its turns give way, the seconds being how long letting go takes it."""

    def __init__(self, service: str, port: int = PORT, gives_way_s: float | None = None) -> None:
        self.service = service
        self.root = f"http://127.0.0.1:{port}"
        self.base = f"{self.root}/tickets"
        self.ticket: int | None = None
        self.gives_way_s = gives_way_s

    def _call(self, method: str, url: str, body: dict | None = None) -> tuple[int, dict] | None:
        """One call of the arbiter, waiting for its answer; None where nothing answers at its address: the
        connection is refused, so the node keeps no queue. An answer that cannot be read, or a connection
        that breaks after it was taken, is ArbiterFault: the arbiter is there, and its queue is the only
        place to wait."""
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(url, data=data, method=method, headers={"content-type": "application/json"})
        try:
            with urllib.request.urlopen(request) as answer:
                text = answer.read()
                try:
                    return answer.status, json.loads(text) if text else {}
                except ValueError:
                    raise ArbiterFault(f"the arbiter answered {answer.status} with an answer that cannot be read") from None
        except urllib.error.HTTPError as refused:
            try:
                return refused.code, json.loads(refused.read() or b"{}")
            except (OSError, ValueError, http.client.HTTPException):
                raise ArbiterFault(f"the arbiter answered {refused.code} with an answer that cannot be read") from None
        except urllib.error.URLError as why:
            if isinstance(why.reason, ConnectionRefusedError):
                return None
            raise ArbiterFault(f"the arbiter's answer broke: {why.reason}") from None
        except (OSError, ValueError, http.client.HTTPException) as why:
            raise ArbiterFault(f"the arbiter's answer broke: {type(why).__name__}") from None

    def ask(self, wait_s: float = 0.0, waited_s: float | None = None, rest: bool = False) -> Answer | None:
        """The card asked for, or the ticket asked after; None where nothing answers at the arbiter's address,
        so the occupant takes the lock as it comes. With `wait_s` the answer is held open until the turn
        comes, that long at most; a new ticket stands by its request's arrival, `waited_s` ago, or with
        `rest` for no request, only to rest on the card once it is free. A service the node has no place for
        is told so as PermissionError, and an arbiter that answers without a ticket is ArbiterFault."""
        held_s = max(0.0, min(wait_s, HOLD_OPEN_S))
        query = f"?waitMs={round(held_s * 1000)}" if held_s > 0 else ""
        answered = self._call("GET", f"{self.base}/{self.ticket}{query}") if self.ticket is not None else None
        if answered is None or answered[0] == 404:
            asked: dict = {"serviceName": self.service}
            if self.gives_way_s is not None:
                asked["givesWayMs"] = max(1, round(self.gives_way_s * 1000))
            if waited_s is not None and waited_s > 0:
                asked["waitedMs"] = round(waited_s * 1000)
            if rest:
                asked["rest"] = True
            if held_s > 0:
                asked["waitMs"] = round(held_s * 1000)
            answered = self._call("POST", self.base, asked)
        if answered is None:
            self.ticket = None
            return None
        status, body = answered
        if status == 422 and body.get("code") == "no-place":
            raise PermissionError(body.get("error", f"{self.service} has no place among this node's occupants"))
        if status not in (200, 201, 202):
            code = body.get("code") or "no-ticket"
            error = body.get("error") or "the arbiter answered without a ticket"
            raise ArbiterFault(f"the arbiter answered {status} {code}: {error}")
        self.ticket = body.get("id")
        return answer_of(body)

    def watch(self, ticket: int, wait_s: float = HOLD_OPEN_S) -> Answer | None:
        """A turn that gives way asked after with its answer held open until it is to give way, `wait_s` at
        most (docs/node-arbiter.md, 「Handing on」). It never asks for a new ticket: None where the arbiter no longer
        holds this one, and an OSError where no arbiter answers or its answer cannot be read."""
        held_s = max(0.0, min(wait_s, HOLD_OPEN_S))
        try:
            answered = self._call("GET", f"{self.base}/{ticket}?waitMs={round(held_s * 1000)}")
        except ArbiterFault as fault:
            raise OSError(str(fault)) from None
        if answered is None:
            raise OSError("the card arbiter did not answer")
        status, body = answered
        return answer_of(body) if status == 200 else None

    def keep(self, next_waited_s: float) -> bool:
        """Whether a turn carries on to the occupant's next request, which has waited `next_waited_s`: true only
        where the arbiter says that request still stands first. Anything else — another before it, no ticket,
        no arbiter, one that does not answer the question — lets the card go."""
        if self.ticket is None:
            return False
        try:
            answered = self._call("GET", f"{self.base}/{self.ticket}?nextWaitedMs={round(max(0.0, next_waited_s) * 1000)}")
        except ArbiterFault:
            return False
        return answered is not None and answered[0] == 200 and answered[1].get("keep") is True

    def rest(self) -> Answer | None:
        """The turn told it has nothing left to serve and keeps its weights on the card (docs/node-arbiter.md,
        「Resting」). The answer says whether the arbiter rests it (`resting`) and whether it is to give way at
        once; None where there is no ticket, no arbiter, or one that no longer holds the ticket."""
        if self.ticket is None:
            return None
        try:
            answered = self._call("PATCH", f"{self.base}/{self.ticket}", {"resting": True})
        except ArbiterFault:
            return None
        return answer_of(answered[1]) if answered is not None and answered[0] == 200 else None

    def give_back(self) -> None:
        if self.ticket is not None:
            try:
                self._call("DELETE", f"{self.base}/{self.ticket}")
            except ArbiterFault:
                # A ticket is given back as best it can be: the arbiter ends it when this asker leaves too.
                pass
            self.ticket = None

    def state(self) -> dict | None:
        """The card as the arbiter keeps it (docs/api.md, 「The card」), or None where no arbiter answers."""
        try:
            answered = self._call("GET", f"{self.root}/card")
        except ArbiterFault:
            return None
        return answered[1] if answered is not None and answered[0] == 200 else None


def answer_of(body: dict) -> Answer:
    wait_ms = body.get("waitMs")
    return Answer(bool(body.get("turn")), body.get("position"), None if wait_ms is None else wait_ms / 1000,
                  body.get("giveWayFor") if body.get("giveWay") else None, body.get("claimantName"), bool(body.get("resting")))


def try_lock(path: Path) -> int | None:
    """The lock taken without waiting, as an open descriptor, or None where another holds it."""
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o666)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd
    except BlockingIOError:
        os.close(fd)
        return None


def relock(fd: int) -> int | None:
    """The lock taken again without waiting on a descriptor that held it before, or None where another holds it."""
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd
    except BlockingIOError:
        return None


def hold(client: TurnClient, lock: Path | int, said=None, waited_s: float | None = None,
         report: Callable[[str], None] | None = None) -> int:
    """The occupant's turn waited for, with the answer held open, for as long as it takes, then the lock taken
    — at its path, or on the descriptor given, which held it before; its descriptor. The ticket stands by a
    request that has waited `waited_s` already. Each answer that is not yet a turn is handed to `said`; a
    fault the arbiter answers with is handed to `report`, and the ask is made again, the lock never taken
    outside the queue (docs/node-arbiter.md, 「The hold」). The lock is taken as it comes only where nothing
    answers at the arbiter's address."""
    told: str | None = None
    while True:
        asked_at = time.monotonic()
        try:
            answered = client.ask(wait_s=HOLD_OPEN_S, waited_s=waited_s)
        except ArbiterFault as fault:
            # The arbiter answers, so the node keeps a queue: taking the lock now would be an occupant on the
            # card outside it. The ask is made again, and the wait ends on the arbiter's answer.
            if report is not None and str(fault) != told:
                told = str(fault)
                report(str(fault))
            time.sleep(AGAIN_S)
            continue
        waited_s = None
        told = None
        if answered is None:
            # Nothing answers at the arbiter's address: the node keeps no queue, and the lock is taken as it
            # comes (docs/node-arbiter.md, 「The hold」).
            fd = relock(lock) if isinstance(lock, int) else try_lock(lock)
            if fd is not None:
                return fd
            time.sleep(AGAIN_S)
            continue
        if answered.turn:
            fd = relock(lock) if isinstance(lock, int) else try_lock(lock)
            if fd is not None:
                return fd
        elif said is not None:
            said(answered)
        # An answer that came back early with no turn, or a lock somebody took in the moment between, is
        # not asked again at once.
        if answered.turn or time.monotonic() - asked_at < HOLD_OPEN_S - 0.05:
            time.sleep(AGAIN_S)


class GivingTurn:
    """A turn that gives way, held by a process that can let go of the card in the middle of its work
    (docs/node-arbiter.md, 「Giving way」). `take` waits for the turn and the lock as `hold` does, then keeps one ask
    after the ticket held open on a thread of its own, which the arbiter answers the instant the turn is to
    give way; `asked_for` names the occupant it is to let go for once there is one, and `let_go` lets the
    lock and the ticket go. Where no arbiter answers, the lock is taken as it comes and nobody ever asks
    for it."""

    def __init__(self, client: TurnClient, lock: Path) -> None:
        self.client = client
        self.lock = lock
        self.fd: int | None = None
        self._asked: str | None = None
        self._stop = threading.Event()

    def take(self, said=None) -> None:
        self.fd = hold(self.client, self.lock, said=said)
        self._asked = None
        # Each turn is watched by a thread of its own that ends with it: one still holding its ask open as
        # the turn is let go hears its ticket is gone and says nothing to the next turn.
        self._stop = threading.Event()
        if self.client.ticket is not None:
            threading.Thread(target=self._watching, args=(self.client.ticket, self._stop), daemon=True).start()

    def _watching(self, ticket: int, stop: threading.Event) -> None:
        while not stop.is_set() and self._asked is None:
            try:
                self.check(ticket, stop)
            except OSError:
                # An arbiter that does not answer is asked again after a pause rather than at once.
                stop.wait(AGAIN_S)

    def check(self, ticket: int | None = None, stop: threading.Event | None = None, wait_s: float = HOLD_OPEN_S) -> None:
        """The ticket asked after once, its answer held open until the turn is to give way, `wait_s` at most."""
        ticket = ticket or self.client.ticket
        if ticket is None:
            return
        answered = self.client.watch(ticket, wait_s)
        if stop is not None and stop.is_set():
            return
        if answered is None or not answered.turn:
            # The arbiter no longer knows the turn — it started again — so the card is let go and asked
            # for afresh, where a turn that gives way can be told to again.
            self._asked = "a turn the arbiter lost"
        elif answered.give_way_for is not None:
            self._asked = answered.give_way_for

    def asked_for(self) -> str | None:
        return self._asked

    def let_go(self) -> None:
        self._stop.set()
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None
        self.client.give_back()


def quiet(client: TurnClient, seconds: float, said=None, every_s: float = AGAIN_S,
          clock=time.monotonic, sleep=time.sleep) -> None:
    """Returns once the card has stood quiet for `seconds` on end — nobody using it and nobody waiting for a
    request, a resting holder being no use (docs/node-arbiter.md, 「Resting」) — and at once where no arbiter answers.
    `said` hears how long it has waited, every ten minutes."""
    began = clock()
    since = began
    told = began
    while True:
        state = client.state()
        if state is None:
            return
        now = clock()
        busy = state.get("busy")
        # An arbiter that keeps no rests says nothing of `busy`: then anybody holding or waiting is.
        if busy is None:
            busy = bool(state.get("lockHeldBy") or state.get("queue") or state.get("claim"))
        if busy:
            since = now
        elif now - since >= seconds:
            return
        if said is not None and now - told >= 600:
            told = now
            said(now - began)
        sleep(every_s)


def main() -> int:
    parser = argparse.ArgumentParser(description="hold a turn for the card and its lock around a command")
    parser.add_argument("--service", required=True)
    parser.add_argument("--lock", default=str(Path.home() / ".gpu.lock"))
    parser.add_argument("--state", help="lend the card on SIGUSR1 and take it back on SIGUSR2, writing held or lent here")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("a command goes after --")
    client = TurnClient(args.service)
    shown: list[int | None] = [None]

    def said(answered: Answer) -> None:
        if answered.position != shown[0]:
            shown[0] = answered.position
            print(f"turn: waiting for the card, {answered.position} in line, about {answered.wait_s:g} s", file=sys.stderr, flush=True)

    def report(fault: str) -> None:
        print(f"turn: {fault}; the turn is still waited for in the queue", file=sys.stderr, flush=True)

    try:
        fd = hold(client, Path(args.lock), said=said, report=report)
    except PermissionError as refused:
        print(f"turn: {refused}", file=sys.stderr, flush=True)
        return 3
    lending = Lending(client, fd, Path(args.state)) if args.state else None
    try:
        return run_passing_stop(command, fd, lending)
    finally:
        if lending is not None:
            lending.state.unlink(missing_ok=True)
        os.close(fd)
        client.give_back()


class Lending:
    """The card let go and taken back while the command keeps running: SIGUSR1 lets the lock and the ticket go,
    SIGUSR2 waits for a turn and takes the lock again on the same descriptor, which the command shares. Each is
    done on a thread of its own, one after another, and what stands once it is done is written to the state
    file: `held` or `lent`."""

    def __init__(self, client: TurnClient, fd: int, state: Path) -> None:
        self.client, self.fd, self.state = client, fd, state
        self.asked: queue.Queue[int] = queue.Queue()
        self.held = True
        self.write()
        threading.Thread(target=self._run, daemon=True, name="turn-lending").start()

    def write(self) -> None:
        self.state.parent.mkdir(parents=True, exist_ok=True)
        part = self.state.with_name(self.state.name + ".part")
        part.write_text("held\n" if self.held else "lent\n")
        part.replace(self.state)

    def ask(self, signum, frame) -> None:
        self.asked.put(signum)

    def _run(self) -> None:
        while True:
            signum = self.asked.get()
            if signum == signal.SIGUSR1 and self.held:
                fcntl.flock(self.fd, fcntl.LOCK_UN)
                self.client.give_back()
                self.held = False
            elif signum == signal.SIGUSR2 and not self.held:
                hold(self.client, self.fd)
                self.held = True
            self.write()


def run_passing_stop(command: list[str], fd: int, lending: Lending | None = None) -> int:
    """The command run to its end with the lock's descriptor, a SIGTERM to this process passed on to it once it
    runs, the card lent and taken back on SIGUSR1 and SIGUSR2 where `lending` stands, and its exit code."""
    running: list[subprocess.Popen] = []
    asked: list[int] = []

    def pass_on(signum, frame) -> None:
        if running:
            running[0].send_signal(signum)
        else:
            asked.append(signum)

    # A caught signal goes back to its default in the command as it starts; one ignored would stay ignored
    # there, so SIGINT and SIGHUP are ignored here only once it runs.
    kept = {signal.SIGTERM: signal.signal(signal.SIGTERM, pass_on)}
    try:
        # The lock rides the command's own descriptor too, so it goes with the command's last process.
        with subprocess.Popen(command, pass_fds=(fd,)) as process:
            running.append(process)
            kept.update((signum, signal.signal(signum, signal.SIG_IGN)) for signum in (signal.SIGINT, signal.SIGHUP))
            if lending is not None:
                kept.update((signum, signal.signal(signum, lending.ask)) for signum in (signal.SIGUSR1, signal.SIGUSR2))
            for signum in asked:
                process.send_signal(signum)
            return process.wait()
    finally:
        for signum, disposition in kept.items():
            signal.signal(signum, disposition)


def end_as(code: int) -> int:
    """This process ended the way its command ended: by the same signal where a signal ended it (a negative
    code), so whatever started it reads a stop as a stop, else with the command's code."""
    if code < 0:
        signal.signal(-code, signal.SIG_DFL)
        os.kill(os.getpid(), -code)
        return 128 - code
    return code


if __name__ == "__main__":
    sys.exit(end_as(main()))
