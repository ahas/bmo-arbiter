"""The card arbiter's rules: when the desktop is taken to have the card, where a claimant's agent cgroup is
found, the claimants the rules declare and how each claim is seen, the desktop's lock taken beside a
service's hold without ending it, which holders the reservation ends, which settings make the node lend its
card, the process group a local hold names ended when it does not let go of a YIELD, and units unloaded to
host memory or the store as the rules say and lowered for a claim's host memory. The lending itself is
tried beside the engine that borrows the card, in that engine's own repository.

Run from the repository root: python3 -m unittest discover -s tests/card
"""

import asyncio
import fcntl
import importlib.util
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from collections.abc import Callable
from pathlib import Path

# The node arbiter's package, `card/` at the repository's root, whose modules the tests import as it does.
HERE = Path(__file__).resolve().parents[2] / "card"
sys.path.insert(0, str(HERE))

# The test's own folder is named `card` too, so the arbiter's own file is loaded under a name of its own.
_spec = importlib.util.spec_from_file_location("card_arbiter", HERE / "card.py")
card = importlib.util.module_from_spec(_spec)
sys.modules["card_arbiter"] = card
_spec.loader.exec_module(card)

import desktop as watch  # noqa: E402
import units  # noqa: E402

# Claimants as the rules declare them; none is known to the arbiter by its name.
TESTER = units.Claimant("tester", containers="tester-", ram_free_bytes=None)
BATCH = units.Claimant("batch", first=False, tickets=False, ram_free_bytes=8 << 30)

IDLE = watch.Reading(percent=2, used_mib=600, total_mib=10240)
GAME = watch.Reading(percent=85, used_mib=5200, total_mib=10240)
PAUSED = watch.Reading(percent=0, used_mib=7000, total_mib=10240)


class Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


class DesktopTest(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.desktop = watch.Desktop(busy_percent=20, room_mib=4096, busy_readings=5, quiet_s=60, clock=self.clock)

    def test_a_game_takes_the_card_after_five_busy_readings(self) -> None:
        for _ in range(4):
            self.assertFalse(self.desktop.see(GAME, ours_on_card=False))
        self.assertTrue(self.desktop.see(GAME, ours_on_card=False))
        self.assertIn("85%", self.desktop.why)

    def test_a_short_burst_does_not(self) -> None:
        for reading in (GAME, GAME, IDLE, GAME, GAME, IDLE):
            self.assertFalse(self.desktop.see(reading, ours_on_card=False))

    def test_memory_the_desktop_keeps_takes_it_while_its_use_is_low(self) -> None:
        for _ in range(5):
            taken = self.desktop.see(PAUSED, ours_on_card=False)
        self.assertTrue(taken)
        self.assertIn("MiB of it free", self.desktop.why)

    def test_readings_while_router_decides_are_passed_over(self) -> None:
        for _ in range(10):
            self.assertFalse(self.desktop.see(GAME, ours_on_card=True))

    def test_given_back_after_a_quiet_minute_and_not_before(self) -> None:
        for _ in range(5):
            self.desktop.see(GAME, ours_on_card=False)
        self.clock.now = 100.0
        self.assertTrue(self.desktop.see(IDLE, ours_on_card=False))
        self.clock.now = 159.0
        self.assertTrue(self.desktop.see(IDLE, ours_on_card=False))
        self.clock.now = 160.0
        self.assertFalse(self.desktop.see(IDLE, ours_on_card=False))

    def test_a_busy_reading_in_the_quiet_minute_starts_it_again(self) -> None:
        for _ in range(5):
            self.desktop.see(GAME, ours_on_card=False)
        self.desktop.see(IDLE, ours_on_card=False)
        self.clock.now = 50.0
        self.desktop.see(GAME, ours_on_card=False)
        self.clock.now = 70.0
        self.assertTrue(self.desktop.see(IDLE, ours_on_card=False))
        self.clock.now = 129.0
        self.assertTrue(self.desktop.see(IDLE, ours_on_card=False))
        self.clock.now = 130.0
        self.assertFalse(self.desktop.see(IDLE, ours_on_card=False))

    def test_a_card_that_cannot_be_read_is_left_to_the_desktop_at_once(self) -> None:
        self.assertTrue(self.desktop.see(None, ours_on_card=False))
        self.assertEqual(self.desktop.why, "it cannot be read")


class ClaimCgroupTest(unittest.TestCase):
    def test_a_system_unit_a_user_unit_and_a_path_are_all_found(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            base = Path(root)
            self.assertIsNone(card.claim_cgroup("tester-agent.service", base))
            user = base / "user.slice/user-1000.slice/user@1000.service/app.slice/tester-agent.service"
            user.mkdir(parents=True)
            self.assertEqual(card.claim_cgroup("tester-agent.service", base), user)
            system = base / "system.slice/tester-agent.service"
            system.mkdir(parents=True)
            self.assertEqual(card.claim_cgroup("tester-agent.service", base), system)
            self.assertEqual(card.claim_cgroup(str(user), base), user)


class ClaimantsTest(unittest.TestCase):
    def test_the_rules_declare_the_claimants_and_where_each_part_goes_out_to(self) -> None:
        rules = {
            "occupants": [
                {"name": "tester", "claims": True, "first": True, "seenBy": {"tickets": True, "cgroup": "tester-agent.service", "containers": "tester-"}, "ramFree": "all"},
                {"name": "batch", "claims": True, "first": False, "seenBy": {"tickets": False, "cgroup": None, "containers": None}, "ramFree": 8},
                {"name": "plain"},
            ],
            "groups": [{"name": "compute", "dump": "ram", "ramMaxGiB": 24}],
            "units": [{"unit": "svc/weights", "dump": "disk"}, {"unit": "other/*", "dump": "disk"}],
        }
        self.assertEqual(units.claimants_of(rules), [
            units.Claimant("tester", True, True, "tester-agent.service", "tester-", None),
            units.Claimant("batch", False, False, None, None, 8 << 30),
        ])
        placing = units.placing_of(rules, "svc", floor_bytes=5)
        self.assertEqual((placing.of("weights"), placing.of("store"), placing.ram_max_bytes, placing.floor_bytes), ("disk", "ram", 24 << 30, 5))
        self.assertEqual(units.placing_of(None, "svc").of("store"), "disk")
        self.assertEqual(units.claimants_of(None), [])


class DeclaredServiceTest(unittest.TestCase):
    def test_read_from_the_process_environment_and_none_where_it_says_nothing_or_is_gone(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            proc = Path(root)
            (proc / "10").mkdir()
            (proc / "10" / "environ").write_bytes(b"PATH=/bin\0BMO_CARD_SERVICE=voice\0HOME=/h\0")
            (proc / "11").mkdir()
            (proc / "11" / "environ").write_bytes(b"PATH=/bin\0BMO_CARD_SERVICE=\0")
            self.assertEqual(card.declared_service(10, proc), "voice")
            self.assertIsNone(card.declared_service(11, proc))
            self.assertIsNone(card.declared_service(12, proc))


class CardProcessesTest(unittest.TestCase):
    """The processes on the card as the slave answers them, each with the service it is known as."""

    class Memory:
        def __init__(self, running: list[tuple[int, int]] | None) -> None:
            self.running = running

        def read(self) -> dict:
            return {"name": "GPU", "totalMiB": 12288, "usedMiB": 10000, "freeMiB": 2288}

        def processes(self) -> list[tuple[int, int]] | None:
            return self.running

    def test_each_process_named_with_its_memory_and_the_service_it_is_known_as(self) -> None:
        shown = units.processes_shown([(41, 9100 * 2**20), (7, 120 * 2**20)], {41: "model"}.get, lambda pid: {41: "rpc-server", 7: "Xorg"}[pid])
        self.assertEqual(shown, [{"pid": 41, "name": "rpc-server", "serviceName": "model", "usedMiB": 9100},
                                 {"pid": 7, "name": "Xorg", "serviceName": None, "usedMiB": 120}])
        self.assertIsNone(units.processes_shown(None, {}.get))

    def test_the_rpc_server_and_a_local_holds_group_are_the_lend_services_and_nothing_else_is(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            lent = card.Card("", "", None, [], Path(root))
            gone, other = 2**22 + 11, 2**22 + 13
            self.assertIsNone(lent.service_of(gone))
            lent.lent_to = "model"
            lent.server_pid = gone
            self.assertEqual(lent.service_of(gone), "model")
            self.assertIsNone(lent.service_of(other))
            if "BMO_CARD_SERVICE" not in os.environ:
                lent.uses = os.getpgid(0)
                self.assertEqual(lent.service_of(os.getpid()), "model")
            api = card.UnitsApi(lent, None, None, Path(root) / "rules.json", "v", memory=self.Memory([(gone, 2**30)]))
            self.assertEqual(api.card_shown()["processes"], [{"pid": gone, "name": str(gone), "serviceName": "model", "usedMiB": 1024}])
            self.assertIsNone(card.UnitsApi(lent, None, None, Path(root) / "rules.json", "v", memory=self.Memory(None)).card_shown()["processes"])


class OccupantsFileTest(unittest.TestCase):
    def test_the_words_highest_first_with_remarks_and_the_word_for_any_service(self) -> None:
        self.assertEqual(card.parse_occupants("# games first\ndesktop tester\n*  # every other service\n"), ["desktop", "tester", "*"])
        for wrong in ("voice voice", "Voice", "voice/2"):
            with self.assertRaises(ValueError):
                card.parse_occupants(wrong)

    def test_names_of_one_rank_joined_by_the_sign(self) -> None:
        self.assertEqual(card.parse_occupants("desktop song=voice\n*\n"), ["desktop", "song=voice", "*"])
        for wrong in ("voice=voice", "song=voice voice", "voice=*", "voice=", "=voice"):
            with self.assertRaises(ValueError):
                card.parse_occupants(wrong)
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "occupants"
            path.write_text("tester=voice song=router\n")
            kept = card.Occupants(path, Path(root) / "log", claimants=lambda: [TESTER])
            kept.read()
            # A claimant standing first is taken out of the rank the file gave it.
            self.assertEqual(kept.order, ["tester", "voice", "song=router"])
            self.assertEqual([kept.rank("song"), kept.rank("router"), kept.rank("voice")], [2, 2, 1])
            self.assertEqual(kept.names(), ["tester", "voice", "song", "router"])
            self.assertFalse(kept.outranks("song", "router"))
            self.assertFalse(kept.outranks("router", "song"))
            self.assertTrue(kept.present("router"))

    def test_read_again_when_it_changes_each_change_kept_and_a_wrong_file_leaving_the_order(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "occupants"
            history = Path(root) / "occupants.log"
            kept = card.Occupants(path, history, now=lambda: "T")
            self.assertFalse(kept.read())
            self.assertEqual(kept.order, ["*"])
            path.write_text("desktop router voice\n")
            self.assertTrue(kept.read())
            self.assertFalse(kept.read())
            path.write_text("router router\n")
            self.assertFalse(kept.read())
            self.assertEqual(kept.order, ["desktop", "router", "voice"])
            path.write_text("model\n")
            self.assertTrue(kept.read())
            self.assertEqual(history.read_text(), "T * -> desktop router voice\nT desktop router voice -> model\n")

    def test_a_claimant_the_rules_stand_first_comes_first_whatever_the_file_says_and_as_soon_as_the_rules_say_so(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "occupants"
            declared: list[units.Claimant] = []
            kept = card.Occupants(path, Path(root) / "log", claimants=lambda: declared)
            path.write_text("model tester\n")
            kept.read()
            self.assertEqual(kept.order, ["model", "tester"])
            # Added to the rules with the file unchanged: it moves first at the next look, with no restart.
            declared.extend([TESTER, BATCH])
            self.assertTrue(kept.read())
            self.assertEqual(kept.order, ["tester", "model"])
            self.assertTrue(kept.outranks("tester", "model"))
            # One the rules do not stand first stands where the file puts it, and has no place where it puts it nowhere.
            self.assertIsNone(kept.rank("batch"))
            path.write_text("desktop *\n")
            kept.read()
            self.assertEqual(kept.order, ["tester", "desktop", "*"])
            self.assertIsNone(kept.rank("batch"))

    def test_where_each_occupant_stands(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "occupants"
            path.write_text("desktop *\n")
            kept = card.Occupants(path, Path(root) / "log", claimants=lambda: [TESTER])
            kept.read()
            self.assertEqual([kept.rank("tester"), kept.rank("desktop"), kept.rank("voice"), kept.rank(None)], [0, 1, 2, 3])
            self.assertTrue(kept.outranks("desktop", "router"))
            self.assertTrue(kept.outranks("tester", None))
            path.write_text("model\n")
            kept.read()
            self.assertIsNone(kept.rank("desktop"))
            self.assertIsNone(kept.rank("voice"))
            self.assertFalse(kept.outranks("desktop", "model"))
            path.write_text("voice router\n")
            kept.read()
            self.assertTrue(kept.outranks("voice", "router"))


class KeeperTest(unittest.TestCase):
    def setUp(self) -> None:
        self.ticks: dict[int, int] = {}
        self.now = 100.0
        self.claims: list[tuple[str, float]] = []

    def keeper(self, order: str, holders: list[int], declared: dict[int, str | None]) -> tuple[card.Keeper, list[int]]:
        ended: list[int] = []

        def end(pid: int, said: str, rule: str) -> bool:
            ended.append(pid)
            return True

        root = tempfile.mkdtemp()
        self.addCleanup(lambda: __import__("shutil").rmtree(root))
        path = Path(root) / "occupants"
        path.write_text(order)
        occupants = card.Occupants(path, Path(root) / "log", claimants=lambda: [TESTER])
        occupants.read()
        kept = card.Keeper(occupants, Path("/nowhere/.gpu.lock"), lambda: self.claims, grace_s=20,
                           holders=lambda lock, mine: holders, declared=lambda pid: declared.get(pid), end=end,
                           started=lambda pid: self.ticks.get(pid, 1), clock=lambda: self.now)
        return kept, ended

    def test_a_service_with_no_place_on_the_node_is_ended_once(self) -> None:
        kept, ended = self.keeper("model", [21, 30, 31], {21: "voice", 30: "model", 31: None})
        self.assertEqual(kept.step(), [21])
        self.assertEqual(kept.step(), [])
        self.assertEqual(ended, [21])

    def test_a_later_process_given_an_ended_pid_is_ended_in_its_turn(self) -> None:
        kept, ended = self.keeper("model", [21], {21: "voice"})
        self.assertEqual(kept.step(), [21])
        self.ticks[21] = 2
        self.assertEqual(kept.step(), [21])
        self.assertEqual(ended, [21, 21])

    def test_a_holder_below_a_claim_is_ended_past_the_grace_and_one_above_it_never(self) -> None:
        kept, ended = self.keeper("desktop *", [40], {40: "voice"})
        self.claims = [("desktop", 90.0)]
        self.assertEqual(kept.step(), [])
        self.now = 110.0
        self.assertEqual(kept.step(), [40])
        above, never = self.keeper("voice desktop", [41], {41: "voice"})
        self.assertEqual(above.step(), [])
        self.assertEqual(never, [])

    def test_a_claim_with_no_place_on_the_node_ends_nothing(self) -> None:
        kept, ended = self.keeper("model", [50], {50: None})
        self.claims = [("desktop", 0.0)]
        self.assertEqual(kept.step(), [])
        self.claims = [("tester", 0.0)]
        self.assertEqual(kept.step(), [50])

    def test_the_arbiter_never_ends_its_own_process_group(self) -> None:
        self.assertFalse(card.end_group(os.getpid(), "ended {}", "occupants"))


class TurnsTest(unittest.TestCase):
    """The hold queue against a stand-in lock and clock: who is given the turn, in what order, and what a
    waiter is told."""

    def setUp(self) -> None:
        root = tempfile.mkdtemp()
        self.addCleanup(lambda: __import__("shutil").rmtree(root))
        path = Path(root) / "occupants"
        path.write_text("desktop voice router\n")
        self.occupants = card.Occupants(path, Path(root) / "log", claimants=lambda: [TESTER])
        self.occupants.read()
        self.now = 0.0
        self.claim: str | None = None
        self.held: list[int] = []
        self.waiting: list[int] = []
        self.declared: dict[int, str] = {}
        self.turns = card.Turns(self.occupants, Path(root) / ".gpu.lock", lambda: self.claim,
                                holders=lambda lock, mine: self.held, clock=lambda: self.now,
                                waiters=lambda lock, mine: self.waiting,
                                declared=lambda pid: self.declared.get(pid))

    def test_a_free_card_is_given_at_once_and_the_next_waits_with_its_place_and_wait(self) -> None:
        router = self.turns.ask("router")
        self.assertTrue(router["turn"])
        self.held = [99]
        voice = self.turns.ask("voice")
        self.assertEqual((voice["turn"], voice["position"]), (False, 2))
        self.assertGreaterEqual(voice["waitMs"], 1000)

    def test_the_head_by_the_occupants_order_then_by_who_asked_first(self) -> None:
        self.held = [99]
        router = self.turns.ask("router")
        voice = self.turns.ask("voice")
        self.assertEqual([one["serviceName"] for one in self.turns.state()["queue"]], ["voice", "router"])
        self.held = []
        self.assertTrue(self.turns.again(voice["id"])["turn"])
        self.assertFalse(self.turns.again(router["id"])["turn"])
        self.assertTrue(self.turns.done(voice["id"]))
        self.assertTrue(self.turns.again(router["id"])["turn"])

    def test_nothing_is_given_while_a_claim_stands(self) -> None:
        self.claim = "tester"
        answered = self.turns.ask("voice")
        self.assertEqual((answered["turn"], answered["claimantName"]), (False, "tester"))
        self.assertIsNone(self.turns.ask("model"))

    def test_a_claimants_ticket_waits_until_the_card_is_the_claims_and_nobody_else_is_given_it(self) -> None:
        self.holds = False
        self.turns.claim_holds = lambda: self.holds
        self.held = [99]
        claim = self.turns.ask("tester", need={"gpus": 1}, ref="task-1")
        self.assertEqual((claim["turn"], claim["position"]), (False, 1))
        self.assertLessEqual(claim["waitMs"], self.turns.claim_wait_s * 1000)
        self.held = []
        voice = self.turns.ask("voice")
        self.assertFalse(voice["turn"])
        self.assertFalse(self.turns.again(claim["id"])["turn"])
        self.holds = True
        other = self.turns.ask("tester")
        self.assertTrue(other["turn"])
        self.assertTrue(self.turns.again(claim["id"])["turn"])
        self.assertFalse(self.turns.again(voice["id"])["turn"])
        shown = next(one for one in self.turns.state()["queue"] if one["id"] == claim["id"])
        self.assertEqual((shown["need"], shown["ref"]), ({"gpus": 1}, "task-1"))

    def test_a_granted_claimants_ticket_never_passes_on_and_stands_however_silent_until_given_back(self) -> None:
        self.turns.claim_holds = lambda: True
        self.held = [99]
        claim = self.turns.ask("tester")
        self.assertTrue(claim["turn"])
        self.now = 3600.0
        self.held = []
        self.assertTrue(self.turns.claim_asked("tester"))
        self.assertFalse(self.turns.claim_asked("voice"))
        self.assertTrue(self.turns.again(claim["id"])["turn"])
        self.turns.done(claim["id"])
        self.assertFalse(self.turns.claim_asked("tester"))

    def test_a_silent_ticket_stands_and_a_turn_not_taken_passes_on(self) -> None:
        self.held = [99]
        voice = self.turns.ask("voice")
        router = self.turns.ask("router")
        self.held = []
        self.assertTrue(self.turns.again(voice["id"])["turn"])
        self.now = 6.0
        self.assertFalse(self.turns.again(voice["id"])["turn"])
        self.assertTrue(self.turns.again(router["id"])["turn"])
        # Never asked after for an hour, the voice's ticket still stands in line behind the router's turn.
        self.held = [99]
        self.now = 3600.0
        self.assertEqual([one["id"] for one in self.turns.state()["queue"]], [router["id"], voice["id"]])
        self.assertFalse(self.turns.again(voice["id"])["turn"])

    def test_a_holder_that_lets_the_lock_go_with_no_ticket_given_back_ends_its_turn(self) -> None:
        voice = self.turns.ask("voice")
        self.held = [99]
        router = self.turns.ask("router")
        self.assertEqual(self.turns.state()["queue"][0]["id"], voice["id"])
        # The voice's process died: the kernel let its lock go, and the next in line has the card.
        self.held = []
        self.assertTrue(self.turns.again(router["id"])["turn"])
        self.assertIsNone(self.turns.again(voice["id"]))

    def test_a_turn_nobody_else_waits_for_stays_untaken_until_its_ticket_asks(self) -> None:
        # The Router engine asks only as a call comes: held behind another holder, its ticket is granted
        # once the card is free and then sits untaken while the master reads the state every second.
        voice = self.turns.ask("voice")
        self.assertTrue(voice["turn"])
        self.held = [99]
        router = self.turns.ask("router")
        self.assertFalse(router["turn"])
        self.held = []
        self.turns.done(voice["id"])
        for second in range(1, 21):
            self.now = float(second)
            self.assertEqual(self.turns.state()["lockHeldBy"], [])
        self.assertTrue(self.turns.again(router["id"])["turn"])

    def test_a_turn_that_passed_is_given_again_once_nobody_ready_stands_before_it(self) -> None:
        self.held = [99]
        voice = self.turns.ask("voice")
        router = self.turns.ask("router")
        self.held = []
        self.assertTrue(self.turns.again(voice["id"])["turn"])
        self.now = 6.0
        self.assertTrue(self.turns.again(router["id"])["turn"])
        self.now = 8.0
        self.turns.done(router["id"])
        self.assertTrue(self.turns.again(voice["id"])["turn"])

    def test_a_turn_in_use_stays_however_long_it_is_silent(self) -> None:
        voice = self.turns.ask("voice")
        self.held = [99]
        self.now = 600.0
        self.assertEqual(self.turns.state()["queue"][0]["id"], voice["id"])
        self.held = []
        self.turns.done(voice["id"])
        self.assertEqual(self.turns.typical["voice"], 600.0)

    def test_a_wait_is_counted_from_how_long_each_service_held_the_card(self) -> None:
        voice = self.turns.ask("voice")
        self.now = 12.0
        self.turns.done(voice["id"])
        self.held = [99]
        self.turns.ask("voice")
        router = self.turns.ask("router")
        self.assertEqual(router["waitMs"], 22000)

    def test_a_turn_that_gives_way_is_told_to_while_an_occupant_above_it_waits_or_a_claim_stands(self) -> None:
        router = self.turns.ask("router", gives_way_s=6.0)
        self.assertFalse(router["giveWay"])
        self.held = [99]
        alike = self.turns.ask("router")
        self.assertFalse(self.turns.again(router["id"])["giveWay"])
        self.turns.done(alike["id"])
        voice = self.turns.ask("voice")
        self.assertEqual((voice["turn"], voice["waitMs"]), (False, 6000))
        told = self.turns.again(router["id"])
        self.assertEqual((told["turn"], told["giveWay"], told["giveWayFor"]), (True, True, "voice"))
        self.held = []
        self.turns.done(router["id"])
        self.assertTrue(self.turns.again(voice["id"])["turn"])
        self.turns.done(voice["id"])
        held_to_its_end = self.turns.ask("router")
        self.assertTrue(held_to_its_end["turn"])
        self.claim = "tester"
        self.assertFalse(self.turns.again(held_to_its_end["id"])["giveWay"])
        self.turns.done(held_to_its_end["id"])
        self.claim = None
        giving = self.turns.ask("router", gives_way_s=6.0)
        self.assertTrue(giving["turn"])
        self.claim = "tester"
        self.assertEqual(self.turns.again(giving["id"])["giveWayFor"], "tester")

    def test_a_resting_turn_gives_way_to_any_occupant_that_asks_whatever_its_rank(self) -> None:
        voice = self.turns.ask("voice")
        self.held = [99]
        rests = self.turns.again(voice["id"], rest=True)
        self.assertEqual((rests["turn"], rests["resting"], rests["giveWay"]), (True, True, False))
        self.assertFalse(self.turns.state()["busy"])
        # router stands below voice here, and a resting voice gives way to it all the same.
        router = self.turns.ask("router")
        self.assertEqual((router["turn"], router["waitMs"]), (False, 1000))
        told = self.turns.again(voice["id"])
        self.assertEqual((told["resting"], told["giveWay"], told["giveWayFor"]), (True, True, "router"))
        self.assertTrue(self.turns.state()["busy"])
        self.held = []
        self.turns.done(voice["id"])
        self.assertTrue(self.turns.again(router["id"])["turn"])

    def test_a_resting_turn_gives_way_to_a_process_waiting_for_the_lock(self) -> None:
        voice = self.turns.ask("voice")
        self.held = [99]
        self.turns.again(voice["id"], rest=True)
        self.assertFalse(self.turns.again(voice["id"])["giveWay"])
        # A wrapper that asks for no turn waits in the kernel's table under the service it declares; a rest
        # gives way to it as to a ticket.
        self.declared[77] = "router"
        self.waiting = [77]
        told = self.turns.again(voice["id"])
        self.assertEqual((told["resting"], told["giveWay"], told["giveWayFor"]), (True, True, "router"))
        # A process of the resting service itself is not another occupant, and one with no place is nobody.
        self.declared[77] = "voice"
        self.assertFalse(self.turns.again(voice["id"])["giveWay"])
        self.declared[77] = "stranger"
        self.assertFalse(self.turns.again(voice["id"])["giveWay"])
        # A turn in use is not idle and is not told to let go for a process that asks for no turn.
        self.held = []
        self.waiting = []
        self.turns.done(voice["id"])
        busy = self.turns.ask("router", gives_way_s=6.0)
        self.held = [99]
        self.declared[77] = "voice"
        self.waiting = [77]
        self.assertFalse(self.turns.again(busy["id"])["giveWay"])

    def test_a_resting_turn_gives_way_to_a_claim(self) -> None:
        voice = self.turns.ask("voice")
        self.held = [99]
        self.turns.again(voice["id"], rest=True)
        self.claim = "tester"
        self.assertEqual(self.turns.again(voice["id"])["giveWayFor"], "tester")

    def test_a_request_while_resting_carries_on_where_it_stands_first_however_long_after(self) -> None:
        voice = self.turns.ask("voice")
        self.held = [99]
        self.now = 2.0
        self.turns.again(voice["id"], rest=True)
        self.now = 50.0
        self.turns.ask("router")
        kept = self.turns.again(voice["id"], next_waited_s=0.0)
        self.assertEqual((kept["keep"], kept["resting"], kept["giveWay"]), (True, False, False))
        self.now = 3600.0
        self.assertTrue(self.turns.again(voice["id"], next_waited_s=0.0)["keep"])

    def test_a_rest_is_left_out_of_how_long_a_service_holds_the_card(self) -> None:
        voice = self.turns.ask("voice")
        self.held = [99]
        self.now = 3.0
        self.turns.again(voice["id"], rest=True)
        self.now = 500.0
        self.held = []
        self.turns.done(voice["id"])
        self.assertEqual(self.turns.typical["voice"], 3.0)

    def test_a_ticket_asked_only_to_rest_waits_behind_every_request_and_makes_nobody_give_way(self) -> None:
        router = self.turns.ask("router", gives_way_s=6.0)
        self.held = [99]
        voice = self.turns.ask("voice", rest=True)
        self.assertFalse(voice["turn"])
        self.assertFalse(self.turns.again(router["id"])["giveWay"])
        self.assertTrue(self.turns.again(router["id"], next_waited_s=0.0)["keep"])
        self.assertTrue(self.turns.state()["busy"])
        later = self.turns.ask("router")
        self.assertEqual([one["id"] for one in self.turns.state()["queue"]],
                         [router["id"], later["id"], voice["id"]])
        self.held = []
        self.turns.done(router["id"])
        self.assertFalse(self.turns.again(voice["id"])["turn"])
        self.assertTrue(self.turns.again(later["id"])["turn"])
        self.turns.done(later["id"])
        back = self.turns.again(voice["id"])
        self.assertEqual((back["turn"], back["resting"]), (True, True))
        self.held = [99]
        self.assertFalse(self.turns.state()["busy"])
        self.turns.ask("router")
        self.assertEqual(self.turns.again(voice["id"])["giveWayFor"], "router")

    def test_occupants_of_one_rank_wait_their_turn_by_arrival_and_are_never_ended_for_each_other(self) -> None:
        self.occupants.path.write_text("song=voice router\n")
        self.occupants.read()
        song = self.turns.ask("song")
        self.assertTrue(song["turn"])
        self.held = [99]
        self.now = 1.0
        voice = self.turns.ask("voice")
        self.assertEqual((voice["turn"], voice["position"]), (False, 2))
        # Neither stands above the other: a turn held to its end is never told to give way for the other.
        self.assertFalse(self.turns.again(song["id"])["giveWay"])
        self.now = 2.0
        later_song = self.turns.ask("song")
        self.assertEqual([one["serviceName"] for one in self.turns.state()["queue"]], ["song", "voice", "song"])
        self.assertEqual(self.turns.state()["ranks"], [["tester"], ["song", "voice"], ["router"]])
        self.held = []
        self.turns.done(song["id"])
        self.assertTrue(self.turns.again(voice["id"])["turn"])
        self.assertFalse(self.turns.again(later_song["id"])["turn"])
        self.held = [99]
        self.now = 3.0
        # The voice's next line arrived after the song's waiting piece: the card goes to the song.
        self.assertFalse(self.turns.again(voice["id"], next_waited_s=0.5)["keep"])
        self.assertTrue(self.turns.again(voice["id"], next_waited_s=1.5)["keep"])

    def one_rank(self) -> None:
        """song and voice at one rank."""
        self.occupants.path.write_text("song=voice router\n")
        self.occupants.read()

    def test_one_rank_takes_the_card_in_arrival_order_however_long_a_turn_is_used(self) -> None:
        self.one_rank()
        song = self.turns.ask("song", gives_way_s=6.0)
        self.held = [99]
        self.now = 1.0
        voice = self.turns.ask("voice")
        # An hour on: the song's next request arrived before the voice's, so the song carries on, and a turn
        # that may let go in the middle of its work is never told to for one of its rank.
        self.now = 3600.0
        told = self.turns.again(song["id"], next_waited_s=3600.0)
        self.assertEqual((told["keep"], told["giveWay"]), (True, False))
        # Its next request arrived after the voice's: the turn passes at this boundary, and only here.
        told = self.turns.again(song["id"], next_waited_s=0.0)
        self.assertEqual((told["keep"], told["giveWay"]), (False, False))
        self.assertEqual(self.turns.passes_to(song["id"]), "voice")
        self.held = []
        self.turns.done(song["id"])
        self.assertTrue(self.turns.again(voice["id"])["turn"])

    def test_no_time_passes_a_turn_to_its_rank_but_a_rest_or_the_ticket_given_back_does(self) -> None:
        self.one_rank()
        holder = self.turns.ask("song", gives_way_s=6.0)
        self.held = [99]
        waiter = self.turns.ask("voice", waited_s=50.0)
        for second in range(1, 1001):
            self.now = float(second)
            self.assertFalse(self.turns.again(waiter["id"])["turn"])
            # Each of the holder's next requests arrived before the waiter's.
            told = self.turns.again(holder["id"], next_waited_s=self.now + 100.0)
            self.assertEqual((told["keep"], told["giveWay"]), (True, False))
        # Nothing left to serve: the rest gives way to the waiter of its rank.
        told = self.turns.again(holder["id"], rest=True)
        self.assertEqual((told["giveWay"], told["giveWayFor"]), (True, "voice"))
        self.assertIsNone(self.turns.passes_to(holder["id"]))
        self.held = []
        self.turns.done(holder["id"])
        self.assertTrue(self.turns.again(waiter["id"])["turn"])

    def test_a_request_of_one_tier_that_arrived_first_comes_first_whoever_holds_the_card(self) -> None:
        self.one_rank()
        song = self.turns.ask("song")
        self.held = [99]
        self.now = 1.0
        voice = self.turns.ask("voice")
        self.now = 2.0
        self.assertFalse(self.turns.again(song["id"], next_waited_s=0.0)["keep"])
        self.held = []
        self.turns.done(song["id"])
        self.assertTrue(self.turns.again(voice["id"])["turn"])
        # The song comes back for the request it held before the voice's next one: it stands before it in line,
        # and the voice lets go at its boundary for it, never in the middle of its request.
        self.held = [99]
        self.now = 3.0
        self.turns.ask("song", waited_s=2.5)
        self.assertFalse(self.turns.again(voice["id"])["giveWay"])
        self.assertEqual(self.turns.passes_to(voice["id"]), "song")
        self.assertTrue(self.turns.again(voice["id"], next_waited_s=2.8)["keep"])
        self.assertFalse(self.turns.again(voice["id"], next_waited_s=0.0)["keep"])

    def test_a_lower_tier_still_takes_the_card_from_a_turn_in_use(self) -> None:
        tiers = {"tester": 0.0, "trainer": 0.5, "song": 2.0, "voice": 2.0}
        self.occupants.tiers = lambda: tiers
        self.occupants.path.write_text("song=voice trainer\n")
        self.assertTrue(self.occupants.read())
        song = self.turns.ask("song", gives_way_s=6.0)
        self.held = [99]
        self.now = 1.0
        self.turns.ask("voice")
        self.assertFalse(self.turns.again(song["id"])["giveWay"])
        self.turns.ask("trainer")
        told = self.turns.again(song["id"])
        self.assertEqual((told["giveWay"], told["giveWayFor"]), (True, "trainer"))
        self.assertFalse(self.turns.again(song["id"], next_waited_s=100.0)["keep"])

    def test_tiers_make_the_ranks_a_lower_tier_waited_for_and_given_way_to(self) -> None:
        tiers = {"tester": 0.0, "trainer": 0.5, "voice": 1.0, "song": 2.0, "router": 2.0}
        self.occupants.tiers = lambda: tiers
        self.occupants.path.write_text("song=voice router trainer\n")
        self.assertTrue(self.occupants.read())
        self.assertEqual(self.occupants.order, ["tester", "trainer", "voice", "router=song"])
        self.assertEqual(self.turns.state()["tiers"], tiers)
        song = self.turns.ask("song", gives_way_s=6.0)
        self.held = [99]
        self.now = 1.0
        router = self.turns.ask("router", waited_s=5.0)
        voice = self.turns.ask("voice")
        trainer = self.turns.ask("trainer")
        # The line by tier, then by when each request arrived.
        self.assertEqual([one["serviceName"] for one in self.turns.state()["queue"]], ["song", "trainer", "voice", "router"])
        self.assertEqual(self.turns.again(song["id"])["giveWayFor"], "trainer")
        self.held = []
        self.turns.done(song["id"])
        self.assertTrue(self.turns.again(trainer["id"])["turn"])
        # Tier 1 waits while tier 0.5 runs, and a turn of tier 0.5 never gives way to it.
        self.held = [99]
        self.assertFalse(self.turns.again(voice["id"])["turn"])
        self.assertFalse(self.turns.again(trainer["id"], next_waited_s=0.0)["giveWay"])
        self.assertTrue(self.turns.again(trainer["id"], next_waited_s=0.0)["keep"])
        self.assertFalse(self.turns.again(router["id"])["turn"])

    def test_a_star_takes_every_declared_service_by_its_tier_and_rules_without_tiers_keep_the_files_order(self) -> None:
        tiers: dict[str, float] = {}
        self.occupants.tiers = lambda: tiers
        self.occupants.path.write_text("desktop router *\n")
        self.occupants.read()
        self.assertEqual(self.occupants.order, ["tester", "desktop", "router", "*"])
        tiers.update({"tester": 0.0, "trainer": 0.5, "voice": 1.0, "router": 2.0})
        self.assertTrue(self.occupants.read())
        # The desktop stands at nought; the claimant first, and never taken in by the star.
        self.assertEqual(self.occupants.order, ["tester", "desktop", "trainer", "voice", "router", "*"])
        self.assertEqual(self.occupants.rank("unnamed"), 5)

    def test_tiers_are_read_off_the_rules_and_an_entry_not_understood_is_passed_over(self) -> None:
        rules = {"tiers": {"runner": 0, "trainer": 0.5, "odd": "low", "flag": True, "below": -1}}
        self.assertEqual(units.tiers_of(rules), {"runner": 0.0, "trainer": 0.5})
        self.assertEqual(units.tiers_of({"tiers": []}), {})
        self.assertEqual(units.tiers_of(None), {})

    def test_a_ticket_stands_by_its_requests_arrival(self) -> None:
        self.held = [99]
        router_late = self.turns.ask("router")
        self.now = 5.0
        router_early = self.turns.ask("router", waited_s=8.0)
        self.assertEqual([one["id"] for one in self.turns.state()["queue"]], [router_early["id"], router_late["id"]])

    def test_a_holder_with_nobody_waiting_keeps_the_card(self) -> None:
        router = self.turns.ask("router")
        self.held = [99]
        self.now = 60.0
        self.assertTrue(self.turns.again(router["id"], next_waited_s=0.0)["keep"])

    def test_a_holder_lets_go_for_an_occupant_above_it_waiting(self) -> None:
        # The order here is desktop, voice, router: voice stands above router.
        router = self.turns.ask("router")
        self.held = [99]
        self.turns.ask("voice")
        self.assertFalse(self.turns.again(router["id"], next_waited_s=5.0)["keep"])

    def test_a_holder_above_the_waiter_carries_on_for_as_long_as_it_has_requests(self) -> None:
        voice = self.turns.ask("voice")
        self.held = [99]
        self.turns.ask("router")
        self.now = 3600.0
        self.assertTrue(self.turns.again(voice["id"], next_waited_s=0.0)["keep"])

    def test_a_holder_lets_go_where_its_own_rank_asked_first_or_a_claim_stands(self) -> None:
        voice = self.turns.ask("voice")
        self.held = [99]
        self.now = 3.0
        self.turns.ask("voice", waited_s=2.0)
        # The other request arrived at 1.0; this holder's next has waited one second, arriving at 2.0.
        self.assertFalse(self.turns.again(voice["id"], next_waited_s=1.0)["keep"])
        self.assertTrue(self.turns.again(voice["id"], next_waited_s=2.5)["keep"])
        self.claim = "tester"
        self.assertFalse(self.turns.again(voice["id"], next_waited_s=2.5)["keep"])

    def test_a_held_answer_goes_out_the_moment_the_holder_gives_its_ticket_back(self) -> None:
        router = self.turns.ask("router")
        self.held = [99]
        answered: list[dict] = []

        def waiter() -> None:
            answered.append(self.turns.ask("voice", wait_s=10.0))

        waiting = threading.Thread(target=waiter)
        started = time.monotonic()
        waiting.start()
        time.sleep(0.2)
        self.assertEqual(answered, [])
        self.held = []
        self.turns.done(router["id"])
        waiting.join(5.0)
        self.assertTrue(answered and answered[0]["turn"])
        self.assertLess(time.monotonic() - started, 2.0)

    def test_a_held_answer_finds_a_lock_freed_with_no_ticket_given_back(self) -> None:
        self.held = [99]
        answered: list[dict] = []
        waiting = threading.Thread(target=lambda: answered.append(self.turns.ask("voice", wait_s=10.0)))
        waiting.start()
        time.sleep(0.2)
        self.held = []
        waiting.join(5.0)
        self.assertTrue(answered and answered[0]["turn"])

    def test_a_ticket_given_back_while_its_answer_is_held_open_is_answered_as_gone(self) -> None:
        self.held = [99]
        voice = self.turns.ask("voice")
        answered: list = []
        waiting = threading.Thread(target=lambda: answered.append(self.turns.again(voice["id"], wait_s=10.0)))
        waiting.start()
        time.sleep(0.2)
        self.turns.done(voice["id"])
        waiting.join(5.0)
        self.assertEqual(answered, [None])

    def test_a_held_answer_comes_back_on_hold_once_its_wait_passes(self) -> None:
        self.held = [99]
        started = time.monotonic()
        answered = self.turns.ask("voice", wait_s=0.3)
        self.assertFalse(answered["turn"])
        self.assertGreaterEqual(time.monotonic() - started, 0.25)

    def test_the_api_answers_the_turn_with_201_a_wait_with_202_and_every_refusal_with_its_code(self) -> None:
        import json
        import threading
        import urllib.request
        from http.server import ThreadingHTTPServer

        server = ThreadingHTTPServer(("127.0.0.1", 0), card.turns_handler(self.turns))
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        url = f"http://127.0.0.1:{server.server_address[1]}"

        def call(method: str, path: str, body: dict | None = None) -> tuple[int, dict, dict]:
            data = None if body is None else json.dumps(body).encode()
            asked = urllib.request.Request(url + path, data=data, method=method)
            try:
                with urllib.request.urlopen(asked) as answer:
                    text = answer.read()
                    return answer.status, dict(answer.headers), json.loads(text) if text else {}
            except urllib.error.HTTPError as refused:
                return refused.code, dict(refused.headers), json.loads(refused.read() or b"{}")

        def refused(answered: tuple[int, dict, dict]) -> tuple[int, str]:
            status, _, body = answered
            self.assertEqual(set(body), {"code", "error"})
            return status, body["code"]

        self.held = [99]
        status, headers, body = call("POST", "/tickets", {"serviceName": "voice"})
        self.assertEqual((status, body["turn"], body["position"]), (202, False, 1))
        self.assertEqual(headers["retry-after"], str(max(1, -(-body["waitMs"] // 1000))))
        self.held = []
        status, _, body = call("GET", f"/tickets/{body['id']}")
        self.assertEqual((status, body["turn"], body["position"], body["waitMs"]), (200, True, None, None))
        self.assertEqual(call("GET", "/card")[2]["queue"][0]["serviceName"], "voice")
        status, _, kept = call("GET", f"/tickets/{body['id']}?nextWaitedMs=0")
        self.assertEqual((status, kept["keep"]), (200, True))
        status, _, rested = call("PATCH", f"/tickets/{body['id']}", {"resting": True})
        self.assertEqual((status, rested["resting"], rested["inUse"]), (200, True, False))
        self.assertEqual(refused(call("PATCH", f"/tickets/{body['id']}", {"resting": False})), (409, "conflict"))
        self.assertEqual(refused(call("PATCH", f"/tickets/{body['id']}", {"rest": 1})), (400, "bad-request"))
        self.assertEqual(call("DELETE", f"/tickets/{body['id']}")[0], 204)
        self.assertEqual(refused(call("GET", f"/tickets/{body['id']}")), (404, "not-found"))
        self.assertEqual(refused(call("DELETE", f"/tickets/{body['id']}")), (404, "not-found"))
        self.assertEqual(refused(call("GET", "/tickets/soon")), (400, "bad-request"))
        self.assertEqual(refused(call("POST", "/tickets", {"serviceName": "model"})), (422, "no-place"))
        self.assertEqual(refused(call("POST", "/tickets", {"serviceName": "tester", "need": "much"})), (400, "bad-request"))
        status, headers, body = call("POST", "/tickets", {"serviceName": "tester", "need": {"gpus": None, "vramMiB": 8192}, "ref": "t"})
        self.assertEqual((status, body["turn"], "retry-after" in headers), (202, False, True))
        self.assertEqual(call("GET", "/card")[2]["queue"][0]["need"], {"vramMiB": 8192})
        self.assertEqual(refused(call("POST", "/tickets", {"serviceName": "router", "givesWayMs": "soon"})), (400, "bad-request"))
        self.assertEqual(refused(call("POST", "/tickets", {"serviceName": "router", "givesWayMs": 0})), (400, "bad-request"))
        status, _, body = call("POST", "/tickets", {"serviceName": "router", "givesWayMs": 6000})
        self.assertEqual((status, self.turns.tickets[body["id"]].gives_way_s), (202, 6.0))
        self.assertEqual(refused(call("POST", "/tickets", {"serviceName": "router", "rest": "yes"})), (400, "bad-request"))
        status, _, body = call("POST", "/tickets", {"serviceName": "voice", "rest": True})
        self.assertEqual((status, self.turns.tickets[body["id"]].rest), (202, True))
        self.assertTrue(next(one for one in call("GET", "/card")[2]["queue"] if one["id"] == body["id"])["rest"])
        self.assertEqual(refused(call("GET", f"/tickets/{body['id']}?waitMs=soon")), (400, "bad-request"))
        self.assertEqual(refused(call("GET", "/turns")), (404, "no-route"))

    def test_a_ticket_whose_asker_left_before_its_held_answer_is_given_back(self) -> None:
        import socket
        import struct
        from http.server import ThreadingHTTPServer

        server = ThreadingHTTPServer(("127.0.0.1", 0), card.turns_handler(self.turns))
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        self.held = [99]
        body = b'{"serviceName": "voice", "waitMs": 10000}'
        asker = socket.create_connection(server.server_address)
        asker.sendall(b"POST /tickets HTTP/1.1\r\nhost: card\r\ncontent-type: application/json\r\n"
                      + f"content-length: {len(body)}\r\n\r\n".encode() + body)
        deadline = time.monotonic() + 5
        while not self.turns.tickets and time.monotonic() < deadline:
            time.sleep(0.02)
        self.assertEqual(len(self.turns.tickets), 1)
        # the asker goes at once, its connection reset rather than closed
        asker.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        asker.close()
        self.held = []
        deadline = time.monotonic() + 5
        while self.turns.tickets and time.monotonic() < deadline:
            time.sleep(0.02)
        self.assertEqual(self.turns.tickets, {})

    def test_a_client_rests_its_turn_and_hears_it_is_to_give_way_on_one_held_ask(self) -> None:
        from http.server import ThreadingHTTPServer

        import turn

        server = ThreadingHTTPServer(("127.0.0.1", 0), card.turns_handler(self.turns))
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        holder = turn.TurnClient("voice", port=server.server_address[1])
        self.assertTrue(holder.ask().turn)
        self.held = [99]
        rests = holder.rest()
        self.assertEqual((rests.turn, rests.resting, rests.give_way_for), (True, True, None))
        heard: list = []
        watching = threading.Thread(target=lambda: heard.append(holder.watch(holder.ticket, 10)))
        asked_at = time.monotonic()
        watching.start()
        time.sleep(0.2)
        self.assertEqual(heard, [])
        self.turns.ask("router")
        watching.join(10)
        self.assertLess(time.monotonic() - asked_at, 5)
        self.assertEqual((heard[0].resting, heard[0].give_way_for), (True, "router"))
        back = turn.TurnClient("voice", port=server.server_address[1])
        self.assertFalse(back.ask(rest=True).turn)
        self.assertTrue(self.turns.tickets[back.ticket].rest)

    def test_a_giving_turn_hears_it_is_to_let_go_and_lets_the_lock_and_the_ticket_go(self) -> None:
        import threading
        from http.server import ThreadingHTTPServer

        import turn

        server = ThreadingHTTPServer(("127.0.0.1", 0), card.turns_handler(self.turns))
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        lock = Path(tempfile.mkdtemp()) / ".gpu.lock"
        client = turn.TurnClient("router", port=server.server_address[1], gives_way_s=7.0)
        giving = turn.GivingTurn(client, lock)
        giving.take()
        self.assertEqual(self.turns.tickets[client.ticket].gives_way_s, 7.0)
        self.held = [99]
        giving.check(wait_s=0)
        self.assertIsNone(giving.asked_for())
        # An ask held open for ten seconds comes back the moment an occupant above asks for the card.
        heard = threading.Thread(target=giving.check, kwargs={"wait_s": 10})
        asked_at = time.monotonic()
        heard.start()
        self.turns.ask("voice")
        heard.join(10)
        self.assertLess(time.monotonic() - asked_at, 5)
        self.assertEqual(giving.asked_for(), "voice")
        ticket = client.ticket
        giving.let_go()
        self.assertNotIn(ticket, self.turns.tickets)
        self.assertIsNotNone(turn.try_lock(lock))

    def test_a_client_is_handed_the_card_when_it_is_given_back_and_asks_whether_it_keeps_it(self) -> None:
        from http.server import ThreadingHTTPServer

        import turn

        server = ThreadingHTTPServer(("127.0.0.1", 0), card.turns_handler(self.turns))
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        port = server.server_address[1]
        holder = turn.TurnClient("voice", port=port)
        self.assertTrue(holder.ask().turn)
        self.held = [99]
        self.assertTrue(holder.keep(0.0))
        waiter = turn.TurnClient("router", port=port)
        lock = Path(tempfile.mkdtemp()) / ".gpu.lock"
        took: list[float] = []

        def waits() -> None:
            fd = turn.hold(waiter, lock, until=time.monotonic() + 10.0)
            took.append(time.monotonic())
            if fd is not None:
                os.close(fd)

        waiting = threading.Thread(target=waits)
        waiting.start()
        time.sleep(0.3)
        self.assertEqual(took, [])
        # voice stands above router here, so the holder keeps the card until it has nothing left.
        self.assertTrue(holder.keep(0.0))
        self.held = []
        let_go = time.monotonic()
        holder.give_back()
        waiting.join(5.0)
        self.assertTrue(took and took[0] - let_go < 0.5)

    def test_quiet_waits_for_the_card_to_stand_unasked_on_end(self) -> None:
        import turn

        clock = Clock()
        states = iter([{"lockHeldBy": [9], "queue": []}, {"lockHeldBy": [], "queue": []},
                       {"lockHeldBy": [], "queue": [{"serviceName": "voice"}]}] + [{"lockHeldBy": [], "queue": []}] * 40)

        class Asked:
            def state(self) -> dict | None:
                return next(states)

        def sleep(seconds: float) -> None:
            clock.now += seconds

        turn.quiet(Asked(), 10.0, every_s=1.0, clock=clock, sleep=sleep)
        self.assertEqual(clock.now, 12.0)

        class Silent:
            def state(self) -> None:
                return None

        turn.quiet(Silent(), 10.0, clock=clock, sleep=sleep)
        self.assertEqual(clock.now, 12.0)

        # A holder resting on the card keeps the lock, and the card stands quiet all the same.
        resting = iter([{"busy": True, "lockHeldBy": [9], "queue": [{"serviceName": "router"}]}] * 2
                       + [{"busy": False, "lockHeldBy": [9], "queue": [{"serviceName": "router", "resting": True}]}] * 40)

        class Resting:
            def state(self) -> dict | None:
                return next(resting)

        clock.now = 0.0
        turn.quiet(Resting(), 10.0, every_s=1.0, clock=clock, sleep=sleep)
        self.assertEqual(clock.now, 11.0)


class TurnCommandTest(unittest.TestCase):
    """`turn.py` run as a command, against a node whose arbiter does not answer: a stop it is sent reaches the
    command once and is waited out, and an interrupt meant for the command is left to it."""

    # The command: says it stands, counts the SIGTERMs it hears, and ends with 7 a while after the first.
    COMMAND = (
        "import signal, sys, time\n"
        "heard = []\n"
        "signal.signal(signal.SIGTERM, lambda *_: heard.append(1))\n"
        "open(sys.argv[1], 'w').write('up')\n"
        "while not heard:\n"
        "    time.sleep(0.02)\n"
        "time.sleep(0.5)\n"
        "open(sys.argv[2], 'w').write(str(len(heard)))\n"
        "sys.exit(7)\n"
    )

    def run_turn(self, folder: Path) -> subprocess.Popen:
        env = {**os.environ, "CARD_TURN_PORT": "9"}
        process = subprocess.Popen([sys.executable, str(HERE / "turn.py"), "--service", "tester", "--lock",
                                    str(folder / "lock"), "--", sys.executable, "-c", self.COMMAND,
                                    str(folder / "up"), str(folder / "heard")], env=env)
        self.addCleanup(lambda: process.poll() is None and process.kill())
        deadline = time.monotonic() + 20
        while not (folder / "up").exists():
            self.assertLess(time.monotonic(), deadline, "the command did not start")
            time.sleep(0.02)
        return process

    def test_a_stop_reaches_the_command_once_and_is_waited_out(self) -> None:
        import signal

        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            process = self.run_turn(folder)
            process.send_signal(signal.SIGTERM)
            self.assertEqual(process.wait(timeout=20), 7)
            self.assertEqual((folder / "heard").read_text(), "1")

    def test_an_interrupt_is_left_to_the_command(self) -> None:
        import signal

        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            process = self.run_turn(folder)
            process.send_signal(signal.SIGINT)
            time.sleep(0.3)
            self.assertIsNone(process.poll())
            process.send_signal(signal.SIGTERM)
            self.assertEqual(process.wait(timeout=20), 7)


class ClaimWatchTest(unittest.TestCase):
    """The claims watched one poll at a time against a stand-in clock, tickets, containers and card."""

    def setUp(self) -> None:
        self.declared: list[units.Claimant] = [TESTER]
        self.asked: set[str] = set()
        # The running containers each prefix names.
        self.running: dict[str, list[str]] = {}
        # What the card holds of each claimant, by its prefix or its unit; None where it cannot be read.
        self.card_holds: list[int] | None = []
        self.asked_card: list[tuple[str | None, str | None]] = []
        self.said: list[str] = []
        root = tempfile.mkdtemp()
        self.addCleanup(lambda: __import__("shutil").rmtree(root))
        self.root = Path(root)

        def cgroup(unit: str) -> Path:
            path = self.root / unit
            path.mkdir(exist_ok=True)
            return path

        def on_card(path: Path | None, prefix: str | None) -> list[int] | None:
            self.asked_card.append((None if path is None else path.name, prefix))
            return self.card_holds

        self.watch = card.ClaimWatch(lambda: self.declared, lambda name, seen: self.said.append(f"{name}: {seen}"),
                                     lambda name: self.said.append(f"{name}: ended"), settle_s=10,
                                     containers=lambda prefix: self.running.get(prefix, []),
                                     asked=lambda name: name in self.asked, cgroup=cgroup,
                                     on_card=on_card, idle_s=600, card_every_s=0)

    def test_a_ticket_and_a_container_make_one_claim_ended_once_neither_has_stood_the_settle(self) -> None:
        self.asked.add("tester")
        self.watch.step(0)
        self.asked.clear()
        self.running["tester-"] = ["tester-1"]
        self.watch.step(1)
        self.running.clear()
        self.watch.step(2)
        self.watch.step(11.9)
        self.assertEqual(self.said, ["tester: a ticket was asked for"])
        self.watch.step(12)
        self.assertEqual(self.said, ["tester: a ticket was asked for", "tester: ended"])

    def test_a_container_named_for_the_claimant_claims_and_one_named_otherwise_does_not(self) -> None:
        self.running["other-"] = ["other-1"]
        self.watch.step(0)
        self.assertEqual(self.said, [])
        self.running["tester-"] = ["tester-1"]
        self.watch.step(1)
        self.assertEqual(self.said, ["tester: a container of it runs (tester-1)"])

    def test_a_claimant_added_to_the_rules_is_watched_at_the_next_poll_and_one_taken_out_claims_nothing(self) -> None:
        added = units.Claimant("newcomer", containers="newcomer-", ram_free_bytes=1 << 30)
        self.running["newcomer-"] = ["newcomer-7"]
        self.watch.step(0)
        self.assertEqual(self.said, [])
        self.declared.append(added)
        self.watch.step(1)
        self.assertEqual(self.said, ["newcomer: a container of it runs (newcomer-7)"])
        self.declared.remove(added)
        self.watch.step(2)
        self.assertEqual(self.said[-1], "newcomer: ended")

    def test_a_claimant_seen_by_its_unit_alone_ignores_its_tickets(self) -> None:
        self.declared = [BATCH]
        self.asked.add("batch")
        self.watch.step(0)
        self.assertEqual(self.said, [])

    def test_a_claimant_seen_by_its_tickets_alone_never_reads_the_card(self) -> None:
        self.declared = [units.Claimant("plain")]
        self.card_holds = [5150]
        self.watch.step(0)
        self.assertEqual((self.said, self.asked_card), ([], []))
        self.asked.add("plain")
        self.watch.step(1)
        self.assertEqual(self.said, ["plain: a ticket was asked for"])

    def test_a_process_of_the_claimant_on_the_card_claims_it(self) -> None:
        self.card_holds = [5150]
        self.watch.step(0)
        self.card_holds = []
        self.watch.step(1)
        self.watch.step(11)
        self.assertEqual(self.said, ["tester: a process of it is on the card (process 5150)", "tester: ended"])
        self.assertEqual(self.asked_card[0], (None, "tester-"))

    def test_a_unit_claims_only_by_its_processes_on_the_card(self) -> None:
        self.declared = [units.Claimant("agent", cgroup="agent.service")]
        self.watch.step(0)
        self.assertEqual(self.said, [])
        self.card_holds = [6160]
        self.watch.step(1)
        self.assertEqual(self.said, ["agent: a process of it is on the card (process 6160)"])
        self.assertEqual(self.asked_card[-1], ("agent.service", None))

    def test_containers_that_never_reach_the_card_stop_counting_after_the_idle_time(self) -> None:
        self.running["tester-"] = ["tester-1"]
        self.watch.step(0)
        self.watch.step(599)
        self.assertEqual(self.said, ["tester: a container of it runs (tester-1)"])
        self.watch.step(600)
        self.watch.step(609.9)
        self.assertEqual(len(self.said), 1)
        self.watch.step(610)
        self.assertEqual(self.said[-1], "tester: ended")
        # The same container claims nothing more, until a process of it comes onto the card; a new one
        # claims as any container does.
        self.watch.step(700)
        self.assertEqual(len(self.said), 2)
        self.card_holds = [4242]
        self.watch.step(701)
        self.assertEqual(self.said[-1], "tester: a process of it is on the card (process 4242)")
        self.card_holds = []
        self.watch.step(702)
        self.watch.step(712)
        self.assertEqual(self.said[-1], "tester: ended")
        self.running["tester-"] = ["tester-1", "tester-2"]
        self.watch.step(713)
        self.assertEqual(self.said[-1], "tester: a container of it runs (tester-2)")

    def test_the_card_in_use_and_a_ticket_keep_a_claim_however_long(self) -> None:
        self.running["tester-"] = ["tester-1"]
        self.card_holds = [4242]
        for now in range(0, 3000, 100):
            self.watch.step(now)
        self.card_holds = []
        self.running.clear()
        self.asked.add("tester")
        for now in range(3000, 6000, 100):
            self.watch.step(now)
        self.assertEqual(self.said, ["tester: a process of it is on the card (process 4242)"])

    def test_a_card_that_cannot_be_read_gives_nothing_back(self) -> None:
        self.running["tester-"] = ["tester-1"]
        self.card_holds = None
        for now in range(0, 3000, 100):
            self.watch.step(now)
        self.assertEqual(self.said, ["tester: a container of it runs (tester-1)"])


class ClaimOnCardTest(unittest.TestCase):
    """Which processes on the card are a claimant's, read from a stand-in /proc and container listing."""

    def setUp(self) -> None:
        root = tempfile.mkdtemp()
        self.addCleanup(lambda: __import__("shutil").rmtree(root))
        self.proc = Path(root) / "proc"
        self.cgroups = Path(root) / "cgroup"
        self.unit = self.cgroups / "system.slice/tester-agent.service"
        self.mine = "a" * 64
        self.theirs = "b" * 64
        self.listed = {self.mine: "tester-1", self.theirs: "other-1"}
        self.place(101, "/system.slice/tester-agent.service/run")
        self.place(102, f"/system.slice/docker-{self.mine}.scope")
        self.place(103, f"/system.slice/docker-{self.theirs}.scope")
        self.place(104, "/user.slice/user-1000.slice/session-1.scope")

    def place(self, pid: int, cgroup: str) -> None:
        (self.proc / str(pid)).mkdir(parents=True)
        (self.proc / str(pid) / "cgroup").write_text(f"0::{cgroup}\n")

    def on_card(self, cgroup: Path | None, containers: str | None, running: list[int] | None) -> list[int] | None:
        return card.claim_on_card(cgroup, containers, running, root=self.cgroups, proc=self.proc, names=lambda: self.listed)

    def test_a_process_in_a_container_named_for_the_claimant_is_its_own(self) -> None:
        self.assertEqual(self.on_card(None, "tester-", [101, 102, 103, 104]), [102])

    def test_a_process_of_the_unit_is_the_claimants_only_where_the_rules_name_the_unit(self) -> None:
        self.assertEqual(self.on_card(self.unit, None, [101, 102, 103, 104]), [101])
        self.assertEqual(self.on_card(self.unit, "tester-", [101, 102, 103, 104]), [101, 102])
        self.assertEqual(self.on_card(None, None, [101, 102, 103, 104]), [])

    def test_a_card_that_cannot_be_read_is_none(self) -> None:
        self.assertIsNone(self.on_card(None, "tester-", None))

    def test_the_running_containers_named_for_the_claimant(self) -> None:
        self.assertEqual(card.claim_containers("tester-", lambda: self.listed), ["tester-1"])
        self.assertEqual(card.claim_containers("tester-", lambda: {}), [])


class StandInOwner:
    """An owner's control socket as the CUDA backend answers it, over buffers held in a dict: where each is,
    whether its slot holds it, and what it keeps in host memory."""

    def __init__(self, buffers: list[tuple[int, str, int]]) -> None:
        self.buffers = {id: {"part": part, "bytes": size, "state": "resident", "clean": False, "ram": 0} for id, part, size in buffers}
        self.asked: list[str] = []
        # The graphs computed so far and whether a use waits at the door, as the STATE line says them.
        self.graphs = 1
        self.waiting = False

    def present(self) -> bool:
        return True

    async def ask(self, line: str, until_end: bool = False) -> list[str]:
        self.asked.append(line)
        verb, id, *rest = line.split() + ["0"]
        if verb == "LIST":
            listed = [f"BUFFER {n} 0 {b['bytes']} {b['bytes'] + 4096} {b['part']} {b['state']} {'clean' if b['clean'] else 'dirty'} {b['ram']}"
                      for n, b in self.buffers.items()]
            out = sum(b["state"] != "resident" for b in self.buffers.values())
            return [*listed, f"STATE {out} {int(self.waiting)} 0 {self.graphs} 0 0 0", "END"]
        buffer = self.buffers[int(id)]
        if verb == "EVICT":
            to_ram = rest[0] == "ram"
            buffer["state"], buffer["ram"] = ("ram", buffer["bytes"]) if to_ram else ("evicted", 0)
            buffer["clean"] = buffer["clean"] or not to_ram
            return [f"EVICTED {id} {buffer['bytes']} 1.0 {'ram' if to_ram else 'disk'}"]
        if verb == "LOWER":
            written = 0 if buffer["clean"] else buffer["bytes"]
            buffer.update(state="evicted", ram=0, clean=True)
            return [f"LOWERED {id} {written} 1.0"]
        buffer.update(state="resident", ram=0)
        return [f"RESTORED {id} {buffer['bytes']} 1.0"]


class UnitsPlacingTest(unittest.IsolatedAsyncioTestCase):
    """Units unloaded where the rules put them, within the host memory they leave, and lowered for a claim."""

    GIB = 1 << 30

    async def asyncSetUp(self) -> None:
        root = tempfile.mkdtemp()
        self.addCleanup(lambda: __import__("shutil").rmtree(root))
        path = Path(root) / "units.store"
        with path.open("wb") as made:
            made.truncate(64 * self.GIB)
        self.available = 40 * self.GIB
        self.owner = StandInOwner([(1, "weights", 4 * self.GIB), (2, "store", 5 * self.GIB), (3, "compute", 1 * self.GIB)])
        self.units = units.Units("svc", self.owner, units.Store(path), available=lambda: self.available)

    async def test_each_part_goes_where_the_rules_say_within_the_most_dumps_may_take(self) -> None:
        placing = units.Placing({"compute": "disk"}, "ram", ram_max_bytes=6 * self.GIB)
        outcomes = await self.units.evict(None, "the master", placing)
        # weights fit the 6 GiB and go to host memory; the store would pass it and goes to the store; compute is the rules' disk.
        self.assertEqual([(one["name"], one["to"]) for one in outcomes], [("svc/weights", "ram"), ("svc/store", "disk"), ("svc/compute", "disk")])
        self.assertEqual(self.units.ram_bytes(), 4 * self.GIB)
        self.assertTrue(self.units.out())

    async def test_a_claim_that_needs_every_dump_out_of_host_memory_sends_them_to_the_store(self) -> None:
        outcomes = await self.units.evict(None, "tester", units.Placing({}, "ram", floor_bytes=units.NO_RAM))
        self.assertEqual({one["to"] for one in outcomes}, {"disk"})

    async def test_a_claims_floor_keeps_what_would_leave_too_little_available_in_the_store(self) -> None:
        outcomes = await self.units.evict(None, "batch", units.Placing({}, "ram", floor_bytes=32 * self.GIB))
        # 40 GiB available: the weights leave 36 and the compute buffer 35, the store would leave 31.
        self.assertEqual([one["to"] for one in outcomes], ["ram", "disk", "ram"])

    async def test_lowered_clean_first_then_largest_until_enough_is_available(self) -> None:
        await self.units.evict(None, "the master", units.Placing({}, "ram"))
        self.owner.buffers[3]["clean"] = True
        lowered: list[int] = []

        def enough() -> bool:
            return len(lowered) >= 2

        original = self.owner.ask

        async def counting(line: str, until_end: bool = False) -> list[str]:
            if line.startswith("LOWER"):
                lowered.append(int(line.split()[1]))
            return await original(line, until_end)

        self.owner.ask = counting  # type: ignore[method-assign]
        outcomes = await self.units.lower(enough)
        self.assertEqual(lowered, [3, 2])
        self.assertEqual([(one["buffer"], one["written"]) for one in outcomes], [(3, 0), (2, 5 * self.GIB)])
        self.assertEqual(self.units.ram_bytes(), 4 * self.GIB)
        await self.units.restore(None)
        self.assertFalse(self.units.out())


class StoreRoomTest(unittest.IsolatedAsyncioTestCase):
    """A store whose free ranges are cut up by slots longer than their buffers, or kept for none, finds room
    out of them; a slot a buffer lies in is never taken, and an unload the store has no room for moves nothing
    (docs/card-units.md, 「The dump store」)."""

    GIB = 1 << 30

    async def asyncSetUp(self) -> None:
        root = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: __import__("shutil").rmtree(root))
        self.path = root / "units.store"
        with self.path.open("wb") as made:
            made.truncate(12 * self.GIB)
        self.owner = StandInOwner([(1, "weights", 5 * self.GIB + (400 << 20)), (2, "store", 1 * self.GIB), (3, "compute", 256 << 20)])

    def store_with(self, slots: dict[str, tuple[float, float]]) -> units.Store:
        (self.path.parent / "slots.json").write_text(__import__("json").dumps(
            {key: {"offset": int(at * self.GIB), "length": int(length * self.GIB)} for key, (at, length) in slots.items()}))
        return units.Store(self.path)

    async def test_room_is_made_out_of_slots_longer_than_their_buffers_and_slots_no_buffer_has(self) -> None:
        # the widest free range is 2 GiB: a compute slot ten times its buffer and a store slot of a layout gone
        store = self.store_with({"model/compute#0": (2, 2.5), "model/store#2": (6, 1.25), "model/store#0": (9, 1.0)})
        model = units.Units("model", self.owner, store)
        outcomes = await model.evict(["model/weights"], "tester")
        self.assertEqual([(one["ok"], one["to"]) for one in outcomes], [(True, "disk")])
        self.assertEqual(store.slots["model/compute#0"], {"offset": 2 * self.GIB, "length": 258 << 20})
        self.assertNotIn("model/store#2", store.slots)
        self.assertEqual(store.moving, {})

    async def test_a_slot_a_buffer_lies_in_or_an_owner_not_answering_keeps_is_never_taken(self) -> None:
        store = self.store_with({"model/store#0": (4, 1.0), "stranger/stage#0": (8, 1.0)})
        self.owner.buffers[2].update(state="evicted", clean=True)
        model = units.Units("model", self.owner, store)
        outcomes = await model.evict(["model/weights", "model/compute"], "tester")
        self.assertEqual({(one["ok"], one["error"].split(":")[0]) for one in outcomes}, {(False, "StoreFull")})
        # every slot was looked for first, so nothing went out
        self.assertFalse(any(line.startswith("EVICT") for line in self.owner.asked))
        self.assertEqual(self.owner.buffers[3]["state"], "resident")
        self.assertEqual(set(store.slots) - {"model/compute#0"}, {"model/store#0", "stranger/stage#0"})
        self.assertEqual(store.moving, {})

    async def test_a_clean_slot_goes_only_once_every_other_kind_of_room_is_spent(self) -> None:
        store = self.store_with({"model/store#0": (6, 1.0), "model/compute#0": (2, 0.25)})
        self.owner.buffers[2]["clean"] = True
        model = units.Units("model", self.owner, store)
        await model.refresh()
        store.slot("other/weights#0", int(5.5 * self.GIB))
        self.assertEqual(set(store.slots), {"model/store#0", "other/weights#0"})
        store.slot("other/weights#1", int(5.5 * self.GIB))
        self.assertEqual(set(store.slots), {"other/weights#0", "other/weights#1"})


class OwnersTest(unittest.IsolatedAsyncioTestCase):
    """Owners besides the card's borrower (docs/card-units.md, 「Unloading and loading a unit」): read from their
    sockets in the arbiter's folder where their service is placed on the node, shown beside the lent units, and
    moved through their own socket when the master names their units."""

    async def asyncSetUp(self) -> None:
        self.root = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: __import__("shutil").rmtree(self.root))
        with (self.root / "units.store").open("wb") as made:
            made.truncate(1 << 36)
        self.store = units.Store(self.root / "units.store")
        folder = self.root / "owners"
        folder.mkdir()
        # A socket file for each service, as an owner leaves one standing while it listens.
        for service in ("coder", "stranger", "model"):
            made = __import__("socket").socket(__import__("socket").AF_UNIX)
            made.bind(str(folder / f"{service}.sock"))
            made.close()
        (folder / "notes.sock").write_text("not a socket")
        self.stage = StandInOwner([(0, "stage", 10 << 30)])
        self.lent = StandInOwner([(1, "weights", 7 << 30), (2, "store", 2 << 30)])
        self.owners = units.Owners(folder, self.store, lambda service: service in ("coder", "model"), "model",
                                   owner_of=lambda path: self.stage)
        self.broker = card.Card("k", "127.0.0.1", self.root / ".gpu.lock", [], self.root)
        # The host memory this test weighs is the rules' bound, not the machine's: a machine with little
        # available would send every dump to the store and test nothing of the accounting.
        self.broker.units = units.Units("model", self.lent, self.store, available=lambda: 40 << 30)
        self.api = card.UnitsApi(self.broker, None, self.store, self.root / "rules.json", "v", owners=self.owners)

    async def test_only_an_owner_placed_on_the_node_and_not_the_lent_one_is_read(self) -> None:
        await self.owners.refresh()
        self.assertEqual(list(self.owners.units), ["coder"])
        self.assertEqual([(one["name"], one["state"], one["vramMiB"]) for one in self.owners.shown()], [("coder/stage", "resident", 10240)])
        self.assertEqual(self.owners.split(["model/store", "coder/stage"]), (["model/store"], {"coder": ["coder/stage"]}))
        self.assertEqual(self.owners.split(None), (None, {}))

    async def test_the_top_level_stage_is_the_lent_owners_door_and_each_other_owner_shows_its_own_on_its_units(self) -> None:
        self.lent.graphs, self.lent.waiting = 5, False
        self.stage.graphs, self.stage.waiting = 9, True
        await self.broker.units.refresh()
        await self.owners.refresh()
        turns = __import__("types").SimpleNamespace(state=lambda: {})
        api = card.UnitsApi(self.broker, turns, self.store, self.root / "rules.json", "v", owners=self.owners)
        shown = api.state()
        # the stage's use waiting is never read as the lent owner's
        self.assertEqual(shown["stage"], {"waiting": False, "waitingMs": 0, "graphs": 5, "waits": 0, "waitedMs": 0})
        doors = {one["name"]: one.get("door") for one in shown["units"]}
        self.assertEqual(doors["coder/stage"], {"waiting": True, "waitingMs": 0, "graphs": 9, "waits": 0, "waitedMs": 0})
        self.assertEqual((doors["model/weights"], doors["model/store"]), (None, None))

    async def test_the_masters_unload_of_its_unit_goes_to_its_own_socket_and_the_lent_units_stay(self) -> None:
        await self.owners.refresh()
        outcomes = await self.api.move("evict", ["coder/stage"], "the voice asked")
        self.assertEqual([(one["name"], one["ok"]) for one in outcomes], [("coder/stage", True)])
        self.assertEqual([line.split()[:2] for line in self.stage.asked if line.startswith("EVICT")], [["EVICT", "0"]])
        self.assertFalse(any(line.startswith("EVICT") for line in self.lent.asked))
        self.assertEqual(self.owners.shown()[0]["reason"], "the voice asked")
        restored = await self.api.move("restore", ["coder/stage"], "the master")
        self.assertEqual([(one["name"], one["ok"]) for one in restored], [("coder/stage", True)])
        self.assertEqual(self.owners.shown()[0]["state"], "resident")

    async def test_the_dumps_of_every_owner_on_the_node_count_together_and_are_lowered_together(self) -> None:
        self.broker.owners = self.owners
        self.broker.take_rules({"groups": [{"name": "g", "dump": "ram", "ramMaxGiB": 12}]})
        await self.owners.refresh()
        self.owners.units["coder"].available = lambda: 40 << 30
        await self.broker.units.evict(["model/weights"], "the master", self.broker.placing())
        # 7 GiB of the lent units kept already: the stage's 10 would pass the 12 a node's dumps may take
        outcomes = await self.api.move("evict", ["coder/stage"], "model waits")
        self.assertEqual([one["to"] for one in outcomes], ["disk"])
        await self.api.move("restore", ["coder/stage"], "the master")
        self.broker.take_rules({"occupants": [{"name": "tester", "claims": True, "ramFree": "all"}],
                                "groups": [{"name": "g", "dump": "ram", "ramMaxGiB": 24}]})
        outcomes = await self.api.move("evict", ["coder/stage"], "model waits")
        self.assertEqual([one["to"] for one in outcomes], ["ram"])
        self.assertEqual(self.broker.dumps_bytes(), 17 << 30)
        # a claim that needs every dump out lowers the lent units' and the stage's alike
        self.broker.claims["tester"] = 0.0
        await self.broker.lower()
        self.assertTrue(any(line.startswith("LOWER 1 ") for line in self.lent.asked))
        self.assertTrue(any(line.startswith("LOWER 0 ") for line in self.stage.asked))

    async def test_nothing_is_loaded_back_onto_a_card_a_claim_stands_on(self) -> None:
        await self.owners.refresh()
        await self.api.move("evict", ["coder/stage"], "the master")
        self.broker.claims["tester"] = 0.0
        with self.assertRaises(units.NoRoom):
            await self.api.move("restore", ["coder/stage"], "the master")


class LentTurnTest(unittest.IsolatedAsyncioTestCase):
    """The lent hold's turn following its engine's use (docs/card.md, 「Lending」): in use while its owner
    computes or a use waits at its door, resting after the quiet, giving way to an occupant above it always and
    to one of its rank only while it rests, and taken back by the request waiting at its door."""

    async def asyncSetUp(self) -> None:
        root = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: __import__("shutil").rmtree(root))
        path = root / "occupants"
        path.write_text("voice model=coder\n")
        occupants = card.Occupants(path, root / "log", claimants=lambda: [])
        occupants.read()
        self.now = 100.0
        self.held: list[int] = []
        self.turns = card.Turns(occupants, root / ".gpu.lock", lambda: None, holders=lambda lock, mine: self.held,
                                clock=lambda: self.now)

    def lent_turn(self) -> str:
        answered = self.turns.ask("model", local=True, gives_way_s=card.LEND_GIVES_WAY_S)
        self.held = [99]
        return answered["id"]

    def answer_of(self, ticket: str) -> dict:
        return self.turns.again(ticket)

    async def test_a_turn_in_use_gives_way_to_its_rank_only_once_it_rests_and_to_one_above_always(self) -> None:
        model = self.lent_turn()
        self.turns.ask("coder")
        said = self.answer_of(model)
        self.assertEqual((said["inUse"], said["giveWay"]), (True, False))
        self.turns.follow_use(model, False)
        said = self.answer_of(model)
        self.assertEqual((said["inUse"], said["giveWay"], said["giveWayFor"]), (False, True, "coder"))
        self.turns.follow_use(model, True)
        self.assertFalse(self.answer_of(model)["giveWay"])
        self.turns.ask("voice")
        self.assertEqual(self.answer_of(model)["giveWayFor"], "voice")

    async def test_a_turn_taken_back_stands_by_the_request_waiting_at_its_door(self) -> None:
        self.held = [99]
        self.turns.ask("coder")
        self.now += 2
        self.turns.ask("model", local=True, gives_way_s=card.LEND_GIVES_WAY_S, waited_s=5)
        self.assertEqual([one["serviceName"] for one in self.turns.state()["queue"]], ["model", "coder"])

    async def lent_broker(self) -> tuple[card.Card, StandInOwner]:
        """A broker whose standing hold holds the lent turn, its owner read once, coder asking behind it."""
        root = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: __import__("shutil").rmtree(root))
        broker = card.Card("k", "127.0.0.1", root / ".gpu.lock", [], root)
        owner = StandInOwner([(1, "weights", 1 << 30)])
        broker.units = units.Units("model", owner, None)
        broker.turns = self.turns
        broker.ticket = self.lent_turn()
        self.turns.ask("coder")
        await broker.units.refresh()
        return broker, owner

    async def test_the_holds_turn_follows_its_owners_graphs_and_its_door(self) -> None:
        broker, owner = await self.lent_broker()
        broker.follow_use(now=0.0)
        self.assertTrue(self.answer_of(broker.ticket)["inUse"])
        broker.follow_use(now=card.LEND_QUIET_S + 1)
        self.assertFalse(self.answer_of(broker.ticket)["inUse"])
        owner.graphs = 2
        await broker.units.refresh()
        broker.follow_use(now=card.LEND_QUIET_S + 2)
        self.assertTrue(self.answer_of(broker.ticket)["inUse"])
        owner.waiting = True
        await broker.units.refresh()
        broker.follow_use(now=card.LEND_QUIET_S * 3)
        self.assertTrue(self.answer_of(broker.ticket)["inUse"])

    async def test_an_owner_that_never_says_quiet_keeps_its_turn_in_use_for_the_whole_quiet(self) -> None:
        broker, owner = await self.lent_broker()
        broker.follow_use(now=0.0)
        broker.follow_use(now=card.LEND_QUIET_S - 0.1)
        self.assertTrue(self.answer_of(broker.ticket)["inUse"])
        self.assertFalse(self.answer_of(broker.ticket)["giveWay"])
        broker.follow_use(now=card.LEND_QUIET_S)
        said = self.answer_of(broker.ticket)
        self.assertEqual((said["inUse"], said.get("resting", False)), (False, True))

    async def test_quiet_rests_the_turn_at_once_a_graph_computed_before_it_counting_as_before(self) -> None:
        broker, owner = await self.lent_broker()
        broker.follow_use(now=0.0)
        # The answer's last graph, computed before QUIET and not yet read, is read by QUIET itself.
        owner.graphs = 2
        await broker.quiet(now=1.0)
        said = self.answer_of(broker.ticket)
        self.assertEqual((said["inUse"], said.get("resting", False)), (False, True))
        broker.follow_use(now=2.0)
        self.assertFalse(self.answer_of(broker.ticket)["inUse"])

    async def test_a_graph_after_quiet_puts_the_turn_back_in_use(self) -> None:
        broker, owner = await self.lent_broker()
        await broker.quiet(now=1.0)
        owner.graphs = 2
        await broker.units.refresh()
        broker.follow_use(now=2.0)
        said = self.answer_of(broker.ticket)
        self.assertEqual((said["inUse"], said.get("resting", False)), (True, False))
        broker.follow_use(now=2.0 + card.LEND_QUIET_S - 0.1)
        self.assertTrue(self.answer_of(broker.ticket)["inUse"])

    async def test_a_use_waiting_at_the_door_keeps_the_turn_in_use_whatever_quiet_says(self) -> None:
        broker, owner = await self.lent_broker()
        owner.waiting = True
        await broker.quiet(now=1.0)
        self.assertTrue(self.answer_of(broker.ticket)["inUse"])
        owner.waiting = False
        await broker.quiet(now=2.0)
        self.assertFalse(self.answer_of(broker.ticket)["inUse"])
        owner.waiting = True
        await broker.units.refresh()
        broker.follow_use(now=3.0)
        self.assertTrue(self.answer_of(broker.ticket)["inUse"])

    async def test_a_quieted_turn_gives_way_to_a_ticket_asking_at_its_rank(self) -> None:
        broker, owner = await self.lent_broker()
        broker.follow_use(now=0.0)
        self.assertFalse(self.answer_of(broker.ticket)["giveWay"])
        await broker.quiet(now=1.0)
        said = self.answer_of(broker.ticket)
        self.assertEqual((said["inUse"], said["giveWay"], said["giveWayFor"]), (False, True, "coder"))

    async def test_a_lent_turn_in_use_is_told_pass_once_while_one_of_its_rank_waits_and_never_given_way_on_time(self) -> None:
        broker, owner = await self.lent_broker()
        told: list[bytes] = []

        class Hold:
            write = staticmethod(told.append)

        broker.holder = Hold()  # type: ignore[assignment]
        broker.follow_use(now=0.0)
        self.assertEqual(told, [b"PASS coder\n"])
        # An hour of use later it is told nothing more and kept in the middle of its use: the head lets the
        # request in hand end and says QUIET.
        for second in range(1, 3601, 60):
            self.now = 100.0 + second
            owner.graphs += 1
            await broker.units.refresh()
            broker.follow_use(now=float(second))
        self.assertEqual(told, [b"PASS coder\n"])
        said = self.answer_of(broker.ticket)
        self.assertEqual((said["inUse"], said["giveWay"]), (True, False))
        await broker.quiet(now=3600.0)
        self.assertEqual(self.answer_of(broker.ticket)["giveWayFor"], "coder")

    async def out_broker(self) -> tuple[card.Card, list[bytes], str]:
        """A broker whose standing hold's units went out for coder, which now holds the turn, and the
        lines said on the hold; coder's turn's ticket."""
        root = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: __import__("shutil").rmtree(root))
        with (root / "units.store").open("wb") as made:
            made.truncate(1 << 36)
        broker = card.Card("k", "127.0.0.1", root / ".gpu.lock", [], root)
        broker.units = units.Units("model", StandInOwner([(1, "weights", 1 << 30)]), units.Store(root / "units.store"))
        broker.turns = self.turns
        broker.lent_to = "model"
        broker.held = True
        told: list[bytes] = []

        class Hold:
            write = staticmethod(told.append)

        broker.holder = Hold()  # type: ignore[assignment]
        broker.ticket = self.lent_turn()
        await broker.units.refresh()
        coder = self.turns.ask("coder")["id"]
        await broker.evict(None, "coder asked")
        self.held = []
        self.assertTrue(self.turns.again(coder)["turn"])
        self.held = [99]
        told.clear()
        self.addCleanup(lambda: broker.hold_lock is not None and os.close(broker.hold_lock))
        return broker, told, coder

    async def until(self, met: Callable[[], bool]) -> None:
        for _ in range(100):
            if met():
                return
            await asyncio.sleep(0.05)
        self.fail("not met within five seconds")

    def model_tickets(self) -> list[dict]:
        return [one for one in self.turns.state()["queue"] if one["serviceName"] == "model"]

    async def test_a_wants_ticket_names_the_first_unit_out_so_the_master_places_room_for_it(self) -> None:
        broker, _told, _coder = await self.out_broker()
        broker.want(5.0)
        await self.until(lambda: bool(self.model_tickets()))
        ticket = self.model_tickets()[0]
        self.assertEqual(ticket["need"], {"unitName": "model/weights", "vramMiB": 1024})

    async def test_want_puts_a_hold_whose_units_are_out_in_the_queue_by_its_seconds_and_loads_them_on_its_turn(self) -> None:
        broker, told, coder = await self.out_broker()
        self.turns.ask("coder")
        broker.want(5.0)
        await self.until(lambda: bool(told))
        # It stands by its request's seconds, before the ticket coder asked just now.
        self.assertEqual(told[0].split()[:2], [b"QUEUED", b"2"])
        self.assertGreaterEqual(self.model_tickets()[0]["waitedMs"], 5000)
        self.held = []
        self.turns.done(coder)
        await self.until(lambda: told[-1] == b"RESTORED\n")
        self.assertFalse(broker.units.out())
        self.assertIsNotNone(broker.hold_lock)
        self.assertTrue(self.model_tickets()[0]["turn"])
        self.assertIsNone(broker.wanted_at)

    async def test_unwant_takes_the_ticket_back_before_the_units_come_back(self) -> None:
        broker, told, _ = await self.out_broker()
        broker.want(5.0)
        await self.until(lambda: bool(self.model_tickets()))
        broker.unwant()
        await self.until(lambda: not self.model_tickets())
        self.assertTrue(broker.units.out())
        self.assertIsNone(broker.hold_lock)
        self.assertNotIn(b"RESTORED\n", told)

    async def test_want_with_every_unit_on_the_card_changes_nothing(self) -> None:
        broker, _ = await self.lent_broker()
        broker.held = True
        broker.want(5.0)
        self.assertEqual((broker.wanting, broker.wanted_at), (None, None))

    async def test_a_want_during_a_claim_waits_for_its_end_and_the_return_after_it_stands_by_its_seconds(self) -> None:
        broker, told, coder = await self.out_broker()
        broker.claims["tester"] = time.monotonic()
        broker.want(5.0)
        # Nothing of the claim moves: no restore started, no ticket asked.
        self.assertIsNone(broker.wanting)
        self.assertEqual(self.model_tickets(), [])
        broker.claim_ended("tester")
        returning = broker.coming_back
        self.assertIsNotNone(returning)
        await self.until(lambda: bool(self.model_tickets()))
        self.assertGreaterEqual(self.model_tickets()[0]["waitedMs"], 5000)
        # A WANT while the units come back after the claim leaves that return to them.
        broker.want(8.0)
        self.assertIsNone(broker.wanting)
        self.assertIs(broker.coming_back, returning)
        self.held = []
        self.turns.done(coder)
        await self.until(lambda: returning.done())
        self.assertFalse(broker.units.out())
        self.assertEqual(told[-1], b"RESTORED\n")

    async def test_the_units_state_names_the_graphs_the_owner_has_computed(self) -> None:
        broker, owner = await self.lent_broker()
        api = card.UnitsApi(broker, self.turns, None, broker.pieces / "rules.json", "v")
        await broker.units.refresh()
        self.assertEqual(api.state()["stage"], {"waiting": False, "waitingMs": 0, "graphs": owner.graphs,
                                                "waits": 0, "waitedMs": 0})
        owner.graphs += 2
        await broker.units.refresh()
        self.assertEqual(api.state()["stage"]["graphs"], owner.graphs)


class HandOnTest(unittest.IsolatedAsyncioTestCase):
    """The standing hold's lock and turn handed on only once every unit asked for is out (docs/card-units.md,
    「Master and slaves」): one left on the card keeps both, and the master is answered 409 with why."""

    async def asyncSetUp(self) -> None:
        root = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: __import__("shutil").rmtree(root))
        with (root / "units.store").open("wb") as made:
            made.truncate(1 << 36)
        occupants_file = root / "occupants"
        occupants_file.write_text("model=coder\n")
        occupants = card.Occupants(occupants_file, root / "log", claimants=lambda: [])
        occupants.read()
        self.turns = card.Turns(occupants, root / ".gpu.lock", lambda: None, holders=lambda lock, mine: [])
        self.owner = StandInOwner([(1, "weights", 1 << 30), (2, "compute", 1 << 28)])
        self.broker = card.Card("k", "127.0.0.1", root / ".gpu.lock", [], root)
        self.broker.units = units.Units("model", self.owner, units.Store(root / "units.store"))
        self.broker.turns = self.turns
        self.broker.ticket = self.turns.ask("model", local=True)["id"]
        self.broker.hold_lock = os.open(root / ".gpu.lock", os.O_RDWR | os.O_CREAT, 0o666)
        self.addCleanup(lambda: self.broker.hold_lock is not None and os.close(self.broker.hold_lock))
        self.api = card.UnitsApi(self.broker, self.turns, self.broker.units.store, root / "rules.json", "v")
        self.api.loop = asyncio.get_running_loop()
        await self.broker.units.refresh()

    async def test_a_unit_left_on_the_card_keeps_the_lock_and_the_turn_and_the_master_hears_why(self) -> None:
        original = self.owner.ask

        async def weights_refused(line: str, until_end: bool = False) -> list[str]:
            return ["ERROR out of host memory"] if line.startswith("EVICT 1 ") else await original(line, until_end)

        self.owner.ask = weights_refused  # type: ignore[method-assign]
        # the first of two units asked out goes, the master saying more follow: the hold keeps the card
        status, answer = await asyncio.to_thread(self.api.move_one, "model/compute", units.EVICTED, "coder asked", True)
        self.assertEqual((status, answer["name"], answer["state"] != units.RESIDENT), (card.HTTPStatus.OK, "model/compute", True))
        self.assertIsNotNone(self.broker.hold_lock)
        status, answer = await asyncio.to_thread(self.api.move_one, "model/weights", units.EVICTED, "coder asked")
        self.assertEqual((status, answer["code"]), (card.HTTPStatus.CONFLICT, "conflict"))
        self.assertIn("model/weights: ERROR out of host memory", answer["error"])
        self.assertIsNotNone(self.broker.hold_lock)
        self.assertIn(self.broker.ticket, self.turns.tickets)
        # tried again with the owner answering: the last goes out and the card is handed on
        self.owner.ask = original  # type: ignore[method-assign]
        status, answer = await asyncio.to_thread(self.api.move_one, "model/weights", units.EVICTED, "coder asked")
        self.assertEqual((status, answer["state"] != units.RESIDENT), (card.HTTPStatus.OK, True))
        self.assertIsNone(self.broker.hold_lock)
        self.assertEqual(self.turns.tickets, {})
        self.assertEqual(await asyncio.to_thread(self.api.move_one, "model/nothing", units.EVICTED, "coder asked"),
                         (card.HTTPStatus.NOT_FOUND, {"code": "not-found", "error": "no unit model/nothing stands on this card"}))


class ClaimLowersTest(unittest.IsolatedAsyncioTestCase):
    """A claim on a held card: its units out, dumps lowered to what the claimant needs, and the card the
    claim's only once both are done."""

    async def test_a_claim_that_needs_all_host_memory_has_it_before_its_ticket_is_granted(self) -> None:
        root = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: __import__("shutil").rmtree(root))
        with (root / "units.store").open("wb") as made:
            made.truncate(1 << 36)
        broker = card.Card("k", "127.0.0.1", root / ".gpu.lock", [], root)
        owner = StandInOwner([(1, "weights", 1 << 30), (2, "store", 1 << 30)])
        broker.units = units.Units("svc", owner, units.Store(root / "units.store"), available=lambda: 1 << 40)
        broker.take_rules({"occupants": [{"name": "tester", "claims": True, "ramFree": "all"}],
                           "groups": [{"name": "g", "dump": "ram"}]})
        broker.held = True
        # Something put the weights in host memory before the claim, as the master's move would.
        await broker.units.evict(["svc/weights"], "the master", broker.placing())
        self.assertEqual(owner.buffers[1]["state"], "ram")
        broker.claim_started("tester", "a ticket was asked for")
        self.assertEqual(broker.claimed(), "tester")
        for _ in range(500):
            if broker.claim_holds():
                break
            await asyncio.sleep(0.01)
        self.assertTrue(broker.claim_holds())
        self.assertEqual({one["state"] for one in owner.buffers.values()}, {"evicted"})
        self.assertTrue(any(line.startswith("LOWER 1 ") for line in owner.asked))
        broker.claim_ended("tester")
        self.assertIsNone(broker.claimed())

    async def test_the_hold_is_told_whether_its_units_went_out_for_a_claim(self) -> None:
        root = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: __import__("shutil").rmtree(root))
        with (root / "units.store").open("wb") as made:
            made.truncate(1 << 36)
        broker = card.Card("k", "127.0.0.1", root / ".gpu.lock", [], root)
        broker.units = units.Units("svc", StandInOwner([(1, "weights", 1 << 30)]), units.Store(root / "units.store"),
                                   available=lambda: 1 << 40)
        broker.take_rules({"occupants": [{"name": "tester", "claims": True}], "groups": [{"name": "g", "dump": "ram"}]})
        broker.held = True
        told: list[bytes] = []

        class Hold:
            write = staticmethod(told.append)

        broker.holder = Hold()  # type: ignore[assignment]
        await broker.evict(None, "card-master: voice asked for voice/weights")
        await broker.evict(None, "tester", claimant="tester")
        self.assertEqual(told, [b"EVICTED card-master: voice asked for voice/weights\n", b"EVICTED claim tester\n"])


class LendingTest(unittest.TestCase):
    def test_a_node_without_lend_settings_lends_nothing(self) -> None:
        self.assertIsNone(card.lending({"CARD_LOCK": "/h/.gpu.lock"}))

    def test_lend_settings_must_be_whole(self) -> None:
        self.assertEqual(card.lending({"LLM_KEY": "k", "CARD_ADDRESS": "192.0.2.10"}),
                         "lending needs CARD_LEND_SERVICE CARD_RPC_SERVER CARD_RPC_CACHE CARD_SLOT_DIR")

    def test_whole_lend_settings_lend_the_card(self) -> None:
        lent = card.lending({"LLM_KEY": "k", "CARD_ADDRESS": "192.0.2.10", "CARD_LEND_SERVICE": "svc", "CARD_RPC_SERVER": "/b/rpc",
                             "CARD_RPC_CACHE": "/c", "CARD_SLOT_DIR": "/s", "CARD_PACKED": "1"})
        self.assertIsInstance(lent, card.Lending)
        assert isinstance(lent, card.Lending)
        self.assertEqual((lent.address, lent.service, lent.rpc_server, lent.pieces, lent.packed), ("192.0.2.10", "svc", "/b/rpc", Path("/s"), True))
        self.assertEqual(card.rpc_argv(lent.rpc_server, lent.address), ["/b/rpc", "-H", "192.0.2.10", "-p", "50052", "-c"])

    def test_the_rules_follow_the_settings(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            lock = Path(root) / ".gpu.lock"
            broker = card.Card("", "", lock, [], Path(root))
            rules = Path(root) / "rules.json"
            rules.write_text('{"occupants": [{"name": "tester", "claims": true, "seenBy": {"cgroup": "tester-agent.service"}}]}')
            (Path(root) / "occupants").write_text("desktop *\n")
            env = {"CARD_DESKTOP": "1", "CARD_OCCUPANTS": str(Path(root) / "occupants"), "CARD_STORE": f"{root}/units.store", "CARD_RULES": str(rules)}
            names = [name for name, _ in card.rules(broker, None, lock, env)]
            self.assertEqual(names, [f"occupants tester desktop * from {root}/occupants", "the order kept on the lock's holders",
                                     "the hold queue on 127.0.0.1:7439", f"the units and the store at {root}/units.store",
                                     "claims of tester", "desktop"])
            self.assertTrue(broker.claim_counts("tester"))
            plain = [name for name, _ in card.rules(broker, None, lock, {"CARD_OCCUPANTS": str(Path(root) / "none"),
                                                                        "CARD_STORE": f"{root}/units.store", "CARD_RULES": str(Path(root) / "no-rules.json")})]
            self.assertEqual(plain[0], f"occupants * from {root}/none")
            self.assertEqual(plain[4:], ["claims of the claimants the rules will declare"])


class PiecesTest(unittest.IsolatedAsyncioTestCase):
    """The saved slot pieces the head reads over the lending protocol (docs/api.md, 「The lending protocol」):
    one slot file's pieces sent with `FETCH`, and the room on their filesystem said with `SPACE`."""

    async def asyncSetUp(self) -> None:
        root = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: __import__("shutil").rmtree(root))
        self.pieces = root / "pieces"
        self.pieces.mkdir()
        (self.pieces / "slot-a.0").write_bytes(b"first piece")
        (self.pieces / "slot-a.1").write_bytes(b"second")
        (self.pieces / "slot-b.0").write_bytes(b"another slot")
        broker = card.Card("k", "127.0.0.1", None, [], self.pieces)
        server = await asyncio.start_server(broker.serve, "127.0.0.1", 0)
        self.addAsyncCleanup(server.wait_closed)
        self.addCleanup(server.close)
        self.port = server.sockets[0].getsockname()[1]

    async def ask(self, line: str) -> asyncio.StreamReader:
        reader, writer = await asyncio.open_connection("127.0.0.1", self.port)
        self.addCleanup(writer.close)
        writer.write(f"{line}\n".encode())
        await writer.drain()
        return reader

    async def test_fetch_sends_each_piece_of_one_slot_file_with_its_size_then_end(self) -> None:
        reader = await self.ask("FETCH k slot-a")
        sent: list[tuple[str, bytes]] = []
        while (line := (await asyncio.wait_for(reader.readline(), 5)).decode().strip()) != "END":
            word, name, size = line.split(" ")
            self.assertEqual(word, "PIECE")
            sent.append((name, await reader.readexactly(int(size))))
        self.assertEqual(sent, [("slot-a.0", b"first piece"), ("slot-a.1", b"second")])
        # a name that is not a plain file name sends nothing but its end
        self.assertEqual(await asyncio.wait_for((await self.ask("FETCH k ../slot-a")).readline(), 5), b"END\n")

    async def test_space_says_the_room_on_the_pieces_filesystem_and_what_one_slot_file_takes(self) -> None:
        said = (await asyncio.wait_for((await self.ask("SPACE k slot-a")).readline(), 5)).decode().split()
        usage = __import__("shutil").disk_usage(self.pieces)
        self.assertEqual((said[0], int(said[2]), int(said[3])), ("SPACE", usage.total, len(b"first piece") + len(b"second")))
        self.assertGreater(int(said[1]), 0)
        self.assertEqual((await asyncio.wait_for((await self.ask("SPACE k .hidden")).readline(), 5)).split()[3], b"0")
        self.assertEqual(await asyncio.wait_for((await self.ask("SPACE wrong slot-a")).readline(), 5), b"REFUSED\n")


class DeskLockTest(unittest.TestCase):
    def test_taken_only_once_a_decision_has_let_go(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / ".gpu.lock"
            deciding = os.open(path, os.O_RDWR | os.O_CREAT, 0o666)
            fcntl.flock(deciding, fcntl.LOCK_EX | fcntl.LOCK_NB)
            desk = watch.DeskLock(path)
            self.assertFalse(desk.take())
            os.close(deciding)
            self.assertTrue(desk.take())
            other = os.open(path, os.O_RDWR)
            with self.assertRaises(BlockingIOError):
                fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
            desk.let_go()
            fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
            os.close(other)


class ClaimAndDesktopTest(unittest.TestCase):
    def test_a_claim_takes_the_desktops_hold_and_its_use_is_never_the_desktops(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / ".gpu.lock"
            desktop = watch.Desktop(busy_readings=2, clock=Clock())
            desk = watch.DeskLock(path)
            for _ in range(2):
                watch.desk_step(GAME, False, False, desktop, desk)
            self.assertIsNotNone(desk.fd)

            watch.desk_step(GAME, False, True, desktop, desk)
            self.assertIsNone(desk.fd)
            claim = os.open(path, os.O_RDWR)
            fcntl.flock(claim, fcntl.LOCK_EX | fcntl.LOCK_NB)

            fresh = watch.Desktop(busy_readings=2, clock=Clock())
            for _ in range(5):
                watch.desk_step(GAME, False, True, fresh, desk)
            self.assertFalse(fresh.taken)
            self.assertEqual(fresh.streak, 0)
            os.close(claim)


class LocalHoldUserTest(unittest.IsolatedAsyncioTestCase):
    """A local hold naming the process group that uses the card, and what a YIELD it does not let go of in
    time ends: an arbiter on a loopback port of its own, with no lock and no rpc-server, held locally the
    way a process on its own node holds it."""

    KEY = "card-key-7a2f90"

    async def asyncSetUp(self) -> None:
        pieces = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: __import__("shutil").rmtree(pieces))
        self.card = card.Card(self.KEY, "127.0.0.1", None, [], pieces, yield_wait_s=0.2, stop_wait_s=0.5)
        self.server = await asyncio.start_server(self.card.serve, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        # A process in a group of its own, standing in for whatever puts work on the card.
        self.user = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"], start_new_session=True)
        self.addCleanup(self._end_user)

    async def asyncTearDown(self) -> None:
        self.server.close()
        await self.server.wait_closed()

    def _end_user(self) -> None:
        if self.user.poll() is None:
            self.user.kill()
        self.user.wait()

    async def hold(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        reader, writer = await asyncio.open_connection("127.0.0.1", self.port)
        writer.write(f"HOLD {self.KEY} local\n".encode())
        await writer.drain()
        self.assertEqual(await reader.readline(), b"READY\n")
        return reader, writer

    async def name(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, group: str) -> bytes:
        writer.write(f"USES {group}\n".encode())
        await writer.drain()
        return await asyncio.wait_for(reader.readline(), 5)

    async def yield_unanswered(self, reader: asyncio.StreamReader) -> None:
        """A claim begins and the holder keeps the connection without letting go: YIELD comes, and the
        arbiter ends the hold on its own once the wait for it has passed."""
        self.card.claim_started("tester", "a task started (process 4242)")
        self.assertEqual(await asyncio.wait_for(reader.readline(), 5), b"YIELD tester\n")
        self.assertEqual(await asyncio.wait_for(reader.read(), 5), b"")
        while self.card.held:
            await asyncio.sleep(0.01)

    async def test_an_overdue_hold_ends_the_group_it_named(self) -> None:
        reader, writer = await self.hold()
        self.assertEqual(await self.name(reader, writer, str(self.user.pid)), f"USING {self.user.pid}\n".encode())
        await self.yield_unanswered(reader)
        self.assertEqual(await asyncio.to_thread(self.user.wait, 5), -15)
        writer.close()

    async def test_an_overdue_hold_that_named_nothing_ends_nothing(self) -> None:
        reader, writer = await self.hold()
        await self.yield_unanswered(reader)
        self.assertIsNone(self.user.poll())
        writer.close()

    async def test_the_arbiters_own_group_and_what_is_no_group_are_refused(self) -> None:
        reader, writer = await self.hold()
        self.assertEqual(await self.name(reader, writer, str(os.getpgid(0))), b"REFUSED the arbiter's own process group\n")
        for group in ("abc", "1", "0", "-5"):
            self.assertEqual(await self.name(reader, writer, group), b"REFUSED not a process group\n")
        self.assertIsNone(self.card.uses)
        writer.close()

    async def test_quiet_is_answered_with_the_key_and_refused_without_it_like_every_verb(self) -> None:
        async def asked(line: str) -> bytes:
            reader, writer = await asyncio.open_connection("127.0.0.1", self.port)
            writer.write(f"{line}\n".encode())
            await writer.drain()
            said = await asyncio.wait_for(reader.read(), 5)
            writer.close()
            return said

        heard: list[float | None] = []

        async def quiet(now: float | None = None) -> None:
            heard.append(now)

        self.card.quiet = quiet
        self.assertEqual(await asked(f"QUIET {self.KEY}"), b"QUIETED\n")
        self.assertEqual(heard, [None])
        for line in ("QUIET", "QUIET wrong-key", f"QUIET {self.KEY}x", f"quiet {self.KEY}"):
            self.assertEqual(await asked(line), b"REFUSED\n")
        self.assertEqual(heard, [None])

    async def test_a_hold_let_go_in_time_ends_nothing(self) -> None:
        reader, writer = await self.hold()
        await self.name(reader, writer, str(self.user.pid))
        writer.close()
        while self.card.held:
            await asyncio.sleep(0.01)
        self.assertIsNone(self.user.poll())
if __name__ == "__main__":
    unittest.main()
