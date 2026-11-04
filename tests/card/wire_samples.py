"""The node arbiter's routes answered for real, for the Rust check that holds every answer to docs/openapi.json.

Stands the node arbiter's HTTP routes in-process on a free loopback port over stand-ins — a lent hold with
units on the card, a queue with a granted and a waiting ticket, a store, a claim at the end — asks every route
and each refusal it can be brought to, and writes what came back to the file named, as a JSON list of
`method`, `path`, `status` and `body` (null where no body came).

Run from the repository root: python3 -B tests/card/wire_samples.py <out.json>
"""

import asyncio
import json
import os
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_card as fixtures  # noqa: E402

card = fixtures.card
units = fixtures.units


class StandInMemory:
    """The card as its driver would say it, with one process on it."""

    def read(self) -> dict:
        return {"name": "Test Card", "totalMiB": 24576, "usedMiB": 8192, "freeMiB": 16384,
                "utilizationPercent": 64}

    def processes(self) -> list[tuple[int, int]]:
        return [(os.getpid(), 1 << 30)]


def call(base: str, method: str, path: str, body: dict | None = None) -> dict:
    data = None if body is None else json.dumps(body).encode()
    asked = urllib.request.Request(base + path, data=data, method=method, headers={"content-type": "application/json"})
    try:
        with urllib.request.urlopen(asked, timeout=10) as answer:
            status, text = answer.status, answer.read()
    except urllib.error.HTTPError as refused:
        status, text = refused.code, refused.read()
    return {"method": method, "path": path, "status": status, "body": json.loads(text) if text else None}


async def samples(root: Path) -> list[dict]:
    with (root / "units.store").open("wb") as made:
        made.truncate(1 << 36)
    occupants_file = root / "occupants"
    occupants_file.write_text("model=coder voice\n")
    occupants = card.Occupants(occupants_file, root / "log", claimants=lambda: [])
    occupants.read()
    lock = root / ".gpu.lock"
    broker = card.Card("k", "127.0.0.1", lock, [], root)
    turns = card.Turns(occupants, lock, broker.claimed, holders=lambda path, mine: [])
    owner = fixtures.StandInOwner([(1, "weights", 1 << 30), (2, "compute", 1 << 28)])
    broker.units = units.Units("model", owner, units.Store(root / "units.store"))
    broker.turns = turns
    broker.ticket = turns.ask("model", local=True)["id"]
    broker.hold_lock = os.open(lock, os.O_RDWR | os.O_CREAT, 0o666)
    api = card.UnitsApi(broker, turns, broker.units.store, root / "rules.json", "0.1.0-rev.2+sample",
                        memory=StandInMemory())
    api.loop = asyncio.get_running_loop()
    await broker.units.refresh()
    server = ThreadingHTTPServer(("127.0.0.1", 0), card.turns_handler(turns, api))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    said: list[dict] = []

    async def ask(method: str, path: str, body: dict | None = None) -> dict:
        answered = await asyncio.to_thread(call, base, method, path, body)
        said.append(answered)
        return answered

    try:
        model = broker.ticket
        await ask("GET", "/health")
        voice = (await ask("POST", "/tickets", {"serviceName": "voice", "need": {"gpus": 1, "vramMiB": 4096, "unitName": "voice/weights"},
                                                "ref": "task-1", "givesWayMs": 3000}))["body"]["id"]
        await ask("GET", f"/tickets/{voice}?waitMs=0")
        await ask("GET", f"/tickets/{model}?nextWaitedMs=0")
        await ask("GET", "/card")
        await ask("GET", "/card-units")
        await ask("PATCH", f"/tickets/{model}", {"resting": True})
        await ask("PATCH", f"/tickets/{model}", {"resting": False})
        await ask("PATCH", f"/tickets/{model}", {"rest": 1})
        await ask("POST", "/tickets", {"serviceName": "nobody"})
        await ask("POST", "/tickets", {"serviceName": "voice", "need": "much"})
        await ask("GET", "/tickets/123")
        await ask("GET", "/tickets/soon")
        await ask("GET", "/nowhere")
        await ask("PUT", "/card")
        await ask("PATCH", "/card-units/model%2Fcompute", {"state": "evicted", "reason": "tester asked", "keepsTurn": True})
        await ask("PATCH", "/card-units/model%2Fnothing", {"state": "evicted"})
        await ask("PATCH", "/card-units/model%2Fcompute", {"state": "sideways"})
        original = owner.ask

        async def weights_refused(line: str, until_end: bool = False) -> list[str]:
            return ["ERROR out of host memory"] if line.startswith("EVICT 1 ") else await original(line, until_end)

        owner.ask = weights_refused
        await ask("PATCH", "/card-units/model%2Fweights", {"state": "evicted", "reason": "tester asked"})
        owner.ask = original
        broker.claims["tester"] = time.monotonic()
        await ask("PATCH", "/card-units/model%2Fcompute", {"state": "resident"})
        await ask("GET", "/card")
        await ask("DELETE", f"/tickets/{voice}")
        await ask("DELETE", f"/tickets/{voice}")
    finally:
        server.shutdown()
        server.server_close()
        if broker.hold_lock is not None:
            os.close(broker.hold_lock)
    return said


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: wire_samples.py <out.json>", file=sys.stderr)
        return 2
    with tempfile.TemporaryDirectory() as root:
        said = asyncio.run(samples(Path(root)))
    Path(sys.argv[1]).write_text(json.dumps(said, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
