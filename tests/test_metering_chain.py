from __future__ import annotations

import json
import sqlite3
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from metering_chain.acceptance import run as acceptance_run
from metering_chain.api import JsonApplication
from metering_chain.clock import FrozenClock
from metering_chain.errors import Conflict, Forbidden, InvalidState, NotFound, ValidationFailed
from metering_chain.ledger import allocate_proportional, corrected_quantity, segment_variance
from metering_chain.service import MeteringService


ROOT = Path(__file__).resolve().parents[1]

POINTS = [
    ("wg-east-wellhead", "东井组井口", "wellhead", "haiji-2", "WG-EAST"),
    ("wg-west-wellhead", "西井组井口", "wellhead", "haiji-2", "WG-WEST"),
    ("haiji2-separation", "海基二号平台分离", "platform-separation", "haiji-2", None),
    ("pipe-inlet", "海底管道入口", "pipeline-inlet", "subsea-pipeline", None),
    ("pipe-outlet", "海底管道出口", "pipeline-outlet", "haikui-1", None),
    ("haikui1-processing", "海葵一号浮式加工", "floating-processing", "haikui-1", None),
    ("haikui1-tank", "海葵一号储罐", "storage-tank", "haikui-1", None),
    ("offtake-meter", "提油交接计量点", "offtake", "haikui-1", None),
]

LINKS = [
    ("link-east-sep", "wg-east-wellhead", "haiji2-separation", 10),
    ("link-west-sep", "wg-west-wellhead", "haiji2-separation", 10),
    ("link-sep-inlet", "haiji2-separation", "pipe-inlet", 5),
    ("link-pipe", "pipe-inlet", "pipe-outlet", 20),
    ("link-outlet-proc", "pipe-outlet", "haikui1-processing", 5),
    ("link-proc-tank", "haikui1-processing", "haikui1-tank", 10),
    ("link-tank-offtake", "haikui1-tank", "offtake-meter", 5),
]

CHAIN_E = [
    ("wg-east-wellhead", "10000"),
    ("haiji2-separation", "9990.5"),
    ("pipe-inlet", "9986"),
    ("pipe-outlet", "9967"),
    ("haikui1-processing", "9962.5"),
    ("haikui1-tank", "9953"),
]

CHAIN_W = [
    ("wg-west-wellhead", "5000"),
    ("haiji2-separation", "4995"),
    ("pipe-inlet", "4992.5"),
    ("pipe-outlet", "4982.5"),
    ("haikui1-processing", "4980"),
    ("haikui1-tank", "4976"),
]


class LedgerTests(unittest.TestCase):
    def test_corrected_quantity_pins_coefficient(self) -> None:
        self.assertEqual(corrected_quantity(Decimal("100"), Decimal("1.0002")), Decimal("100.020"))

    def test_segment_variance_flags_excess_loss(self) -> None:
        row = segment_variance(
            link_id="l1",
            from_point_id="a",
            to_point_id="b",
            loss_basis_points=10,
            input_quantity=Decimal("1000"),
            output_quantity=Decimal("980"),
        )
        self.assertEqual(row["variance_units"], "-20.000")
        self.assertEqual(row["allowable_loss_units"], "1.000")
        self.assertFalse(row["within_allowance"])

    def test_allocate_proportional_sums_exactly(self) -> None:
        shares = allocate_proportional(
            Decimal("6000"), {"batch-e": Decimal("9953"), "batch-w": Decimal("4976")}
        )
        self.assertEqual(shares["batch-e"], Decimal("4000.134"))
        self.assertEqual(shares["batch-w"], Decimal("1999.866"))
        self.assertEqual(sum(shares.values(), Decimal("0")), Decimal("6000.000"))

    def test_allocate_proportional_rejects_shortfall(self) -> None:
        with self.assertRaises(ValueError):
            allocate_proportional(Decimal("10"), {"batch-e": Decimal("5")})


class MeteringServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.clock = FrozenClock(datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc))
        self.service = MeteringService(self.connection, self.clock)
        for user_id, role in (
            ("op", "operator"),
            ("metro", "metrologist"),
            ("officer", "officer"),
            ("mgr", "manager"),
            ("audit", "auditor"),
        ):
            self.service.create_user(user_id, user_id, role)
        for point_id, name, stage, facility, well_group in POINTS:
            self.service.create_point("metro", {
                "point_id": point_id,
                "name": name,
                "stage": stage,
                "facility_id": facility,
                "well_group_id": well_group,
            })
        for link_id, from_point, to_point, loss_bp in LINKS:
            self.service.create_link("metro", {
                "link_id": link_id,
                "from_point_id": from_point,
                "to_point_id": to_point,
                "loss_basis_points": loss_bp,
            })
        self.calibration_ids = {}
        for point_id, *_ in POINTS:
            created = self.service.create_calibration("metro", {
                "point_id": point_id,
                "version_no": 1,
                "coefficient": "1",
            })
            self.service.activate_calibration("metro", created["version_id"])
            self.calibration_ids[point_id] = created["version_id"]

    def tearDown(self) -> None:
        self.connection.close()

    def register_batches(self) -> None:
        self.service.register_batch("op", {"batch_id": "batch-e-001", "wellhead_point_id": "wg-east-wellhead"})
        self.service.register_batch("op", {"batch_id": "batch-w-001", "wellhead_point_id": "wg-west-wellhead"})

    def upload_chain(self, batch_id: str, chain: list[tuple[str, str]], hour: int = 6) -> None:
        for index, (point_id, quantity) in enumerate(chain):
            self.service.upload_reading("op", {
                "point_id": point_id,
                "batch_id": batch_id,
                "observed_at": f"2026-10-01T{hour:02d}:{index * 10:02d}:00Z",
                "raw_quantity": quantity,
                "idempotency_key": f"{batch_id}-{point_id}-r1",
            })

    def sign_voyage(self, voyage_id: str, quantity: str) -> dict[str, object]:
        self.service.create_voyage("officer", {
            "voyage_id": voyage_id,
            "tank_point_id": "haikui1-tank",
            "vessel_name": "远海号提油轮",
        })
        draft = self.service.create_handover("officer", voyage_id, {"quantity_units": quantity})
        return self.service.sign_handover("officer", draft["handover_id"])

    # --------------------------------------------------------------
    # 端到端链路、反查与平衡
    # --------------------------------------------------------------

    def test_end_to_end_chain_trace_and_balance(self) -> None:
        self.register_batches()
        self.upload_chain("batch-e-001", CHAIN_E, hour=6)
        self.upload_chain("batch-w-001", CHAIN_W, hour=7)
        signed = self.sign_voyage("VOY-1", "6000")
        attributions = {row["batch_id"]: row["quantity_units"] for row in signed["attributions"]}
        self.assertEqual(attributions, {"batch-e-001": "4000.134", "batch-w-001": "1999.866"})
        trace = self.service.trace_voyage("mgr", "VOY-1")
        self.assertEqual(trace["handover"]["quantity_units"], "6000")
        self.assertEqual(trace["difference_units"], "0.000")
        self.assertEqual(
            {source["well_group_id"] for source in trace["sources"]},
            {"WG-EAST", "WG-WEST"},
        )
        east = next(source for source in trace["sources"] if source["batch_id"] == "batch-e-001")
        self.assertEqual(east["attributed_units"], "4000.134")
        self.assertEqual(len(east["segments"]), 5)
        pipe_segment = next(row for row in east["segments"] if row["link_id"] == "link-pipe")
        self.assertEqual(pipe_segment["input_units"], "9986.000")
        self.assertEqual(pipe_segment["output_units"], "9967.000")
        self.assertEqual(pipe_segment["variance_units"], "-19.000")
        self.assertTrue(pipe_segment["within_allowance"])
        balance = self.service.balance_report("mgr")
        self.assertTrue(balance["balanced"])
        totals = balance["totals"]
        self.assertEqual(totals["produced_units"], "15000.000")
        self.assertEqual(totals["exported_units"], "6000.000")
        self.assertEqual(totals["inventory_units"], "8929.000")
        self.assertEqual(totals["in_transit_units"], "0.000")
        identity = (
            Decimal(totals["exported_units"])
            + Decimal(totals["inventory_units"])
            + Decimal(totals["in_transit_units"])
            + Decimal(totals["allowable_loss_units"])
            + Decimal(totals["unaccounted_units"])
        )
        self.assertEqual(identity, Decimal(totals["produced_units"]))

    def test_in_transit_before_reaching_tank(self) -> None:
        self.register_batches()
        self.upload_chain("batch-e-001", CHAIN_E[:3])
        balance = self.service.balance_report("mgr")
        east = next(row for row in balance["batches"] if row["batch_id"] == "batch-e-001")
        self.assertEqual(east["in_transit_units"], "9986.000")
        self.assertEqual(east["inventory_units"], "0.000")
        self.assertEqual(east["produced_units"], "10000.000")

    def test_balance_flags_loss_beyond_allowance(self) -> None:
        self.register_batches()
        self.upload_chain("batch-e-001", [("wg-east-wellhead", "1000"), ("haiji2-separation", "980")])
        balance = self.service.balance_report("mgr")
        east = next(row for row in balance["batches"] if row["batch_id"] == "batch-e-001")
        self.assertFalse(east["balanced"])
        self.assertEqual(east["unaccounted_units"], "19.000")
        segment = east["segments"][0]
        self.assertFalse(segment["within_allowance"])
        self.assertFalse(balance["balanced"])

    # --------------------------------------------------------------
    # 幂等上传
    # --------------------------------------------------------------

    def test_reading_upload_is_idempotent(self) -> None:
        self.register_batches()
        payload = {
            "point_id": "wg-east-wellhead",
            "batch_id": "batch-e-001",
            "observed_at": "2026-10-01T06:00:00Z",
            "raw_quantity": "10000",
            "idempotency_key": "key-e-1",
        }
        first = self.service.upload_reading("op", payload)
        second = self.service.upload_reading("op", payload)
        self.assertEqual(first, second)
        rows = self.connection.execute("SELECT * FROM meter_readings").fetchall()
        self.assertEqual(len(rows), 1)
        changed = dict(payload, raw_quantity="10001")
        with self.assertRaises(Conflict):
            self.service.upload_reading("op", changed)
        rows = self.connection.execute("SELECT * FROM meter_readings").fetchall()
        self.assertEqual(len(rows), 1)

    # --------------------------------------------------------------
    # 迟到读数只能形成后继结算
    # --------------------------------------------------------------

    def test_late_reading_forms_successor_settlement(self) -> None:
        self.register_batches()
        self.service.upload_reading("op", {
            "point_id": "wg-east-wellhead",
            "batch_id": "batch-e-001",
            "observed_at": "2026-10-01T06:00:00Z",
            "raw_quantity": "10000",
            "idempotency_key": "key-e-1",
        })
        first = self.service.close_settlement("officer", "wg-east-wellhead", "batch-e-001")
        self.assertEqual(first["revision"], 1)
        self.assertEqual(first["quantity_units"], "10000.000")
        late = self.service.upload_reading("op", {
            "point_id": "wg-east-wellhead",
            "batch_id": "batch-e-001",
            "observed_at": "2026-10-01T05:30:00Z",
            "raw_quantity": "0.5",
            "idempotency_key": "key-e-late",
        })
        self.assertTrue(late["late"])
        self.assertEqual(late["settlement"], "pending-successor")
        successor = self.service.close_settlement("officer", "wg-east-wellhead", "batch-e-001")
        self.assertEqual(successor["revision"], 2)
        self.assertEqual(successor["quantity_units"], "10000.500")
        self.assertEqual(successor["delta_units"], "0.500")
        self.assertEqual(successor["supersedes_settlement_id"], first["settlement_id"])
        rows = self.connection.execute(
            "SELECT revision,state,quantity_units FROM settlements ORDER BY revision"
        ).fetchall()
        self.assertEqual([(row["revision"], row["state"]) for row in rows], [(1, "superseded"), (2, "closed")])
        self.assertEqual(rows[0]["quantity_units"], "10000.000")
        fresh = self.service.upload_reading("op", {
            "point_id": "wg-east-wellhead",
            "batch_id": "batch-e-001",
            "observed_at": "2026-10-01T06:30:00Z",
            "raw_quantity": "1",
            "idempotency_key": "key-e-fresh",
        })
        self.assertFalse(fresh["late"])
        with self.assertRaises(InvalidState):
            self.service.close_settlement("officer", "pipe-inlet", "batch-e-001")

    # --------------------------------------------------------------
    # 计量争议只冻结相关批次
    # --------------------------------------------------------------

    def test_dispute_freezes_only_related_batch(self) -> None:
        self.register_batches()
        self.upload_chain("batch-e-001", CHAIN_E, hour=6)
        self.upload_chain("batch-w-001", CHAIN_W, hour=7)
        self.service.open_dispute("officer", {
            "dispute_id": "dispute-1",
            "batch_id": "batch-w-001",
            "point_id": "pipe-outlet",
            "reason": "管输出口流量计漂移",
        })
        with self.assertRaises(InvalidState):
            self.service.upload_reading("op", {
                "point_id": "wg-west-wellhead",
                "batch_id": "batch-w-001",
                "observed_at": "2026-10-01T08:00:00Z",
                "raw_quantity": "1",
                "idempotency_key": "key-w-blocked",
            })
        with self.assertRaises(InvalidState):
            self.service.close_settlement("officer", "wg-west-wellhead", "batch-w-001")
        with self.assertRaises(Conflict):
            self.service.open_dispute("officer", {
                "dispute_id": "dispute-2",
                "batch_id": "batch-w-001",
                "reason": "重复争议",
            })
        follow_up = self.service.upload_reading("op", {
            "point_id": "wg-east-wellhead",
            "batch_id": "batch-e-001",
            "observed_at": "2026-10-01T08:00:00Z",
            "raw_quantity": "1",
            "idempotency_key": "key-e-follow",
        })
        self.assertFalse(follow_up["late"])
        signed = self.sign_voyage("VOY-1", "1000")
        self.assertEqual(
            {row["batch_id"] for row in signed["attributions"]},
            {"batch-e-001"},
        )
        self.service.resolve_dispute("officer", "dispute-1", "复检合格")
        recovered = self.service.upload_reading("op", {
            "point_id": "wg-west-wellhead",
            "batch_id": "batch-w-001",
            "observed_at": "2026-10-01T08:05:00Z",
            "raw_quantity": "1",
            "idempotency_key": "key-w-recovered",
        })
        self.assertEqual(recovered["batch_id"], "batch-w-001")

    # --------------------------------------------------------------
    # 已签署交接量不被新系数静默改写
    # --------------------------------------------------------------

    def test_signed_handover_survives_recalibration(self) -> None:
        self.register_batches()
        self.upload_chain("batch-e-001", CHAIN_E, hour=6)
        signed = self.sign_voyage("VOY-1", "1000")
        self.assertEqual(signed["calibration_version_id"], self.calibration_ids["offtake-meter"])
        recalibration = self.service.create_calibration("metro", {
            "point_id": "offtake-meter",
            "version_no": 2,
            "coefficient": "1.0002",
        })
        self.service.activate_calibration("metro", recalibration["version_id"])
        row = self.connection.execute("SELECT * FROM handovers WHERE handover_id=?",
                                      (signed["handover_id"],)).fetchone()
        self.assertEqual(row["quantity_units"], "1000")
        self.assertEqual(row["state"], "signed")
        self.assertEqual(row["calibration_version_id"], self.calibration_ids["offtake-meter"])
        draft = self.service.create_handover("officer", "VOY-1", {"quantity_units": "1000.5"})
        self.assertEqual(draft["revision"], 2)
        self.assertEqual(draft["supersedes_handover_id"], signed["handover_id"])
        replacement = self.service.sign_handover("officer", draft["handover_id"])
        self.assertEqual(replacement["calibration_version_id"], recalibration["version_id"])
        rows = self.connection.execute(
            "SELECT revision,state FROM handovers ORDER BY revision"
        ).fetchall()
        self.assertEqual([(row["revision"], row["state"]) for row in rows], [(1, "superseded"), (2, "signed")])
        attributions = self.connection.execute(
            "SELECT handover_id,state,SUM(quantity_units) AS total FROM lifting_attributions "
            "GROUP BY handover_id,state ORDER BY handover_id"
        ).fetchall()
        states = {(row["handover_id"], row["state"]): row["total"] for row in attributions}
        self.assertEqual(Decimal(states[(signed["handover_id"], "reversed")]), Decimal("1000.000"))
        self.assertEqual(Decimal(states[(draft["handover_id"], "active")]), Decimal("1000.500"))
        trace = self.service.trace_voyage("mgr", "VOY-1")
        self.assertEqual(trace["handover"]["quantity_units"], "1000.5")
        self.assertEqual(trace["difference_units"], "0.000")

    def test_new_calibration_only_applies_to_new_readings(self) -> None:
        self.register_batches()
        first = self.service.upload_reading("op", {
            "point_id": "wg-east-wellhead",
            "batch_id": "batch-e-001",
            "observed_at": "2026-10-01T06:00:00Z",
            "raw_quantity": "100",
            "idempotency_key": "key-before",
        })
        recalibration = self.service.create_calibration("metro", {
            "point_id": "wg-east-wellhead",
            "version_no": 2,
            "coefficient": "1.01",
        })
        self.service.activate_calibration("metro", recalibration["version_id"])
        second = self.service.upload_reading("op", {
            "point_id": "wg-east-wellhead",
            "batch_id": "batch-e-001",
            "observed_at": "2026-10-01T06:10:00Z",
            "raw_quantity": "100",
            "idempotency_key": "key-after",
        })
        self.assertEqual(first["corrected_quantity"], "100.000")
        self.assertEqual(second["corrected_quantity"], "101.000")
        rows = self.connection.execute(
            "SELECT corrected_quantity FROM meter_readings ORDER BY reading_id"
        ).fetchall()
        self.assertEqual([row["corrected_quantity"] for row in rows], ["100.000", "101.000"])

    # --------------------------------------------------------------
    # 校验、权限与审计
    # --------------------------------------------------------------

    def test_handover_requires_inventory_and_draft_state(self) -> None:
        self.register_batches()
        self.upload_chain("batch-e-001", CHAIN_E, hour=6)
        self.service.create_voyage("officer", {
            "voyage_id": "VOY-1",
            "tank_point_id": "haikui1-tank",
            "vessel_name": "远海号提油轮",
        })
        draft = self.service.create_handover("officer", "VOY-1", {"quantity_units": "99999"})
        with self.assertRaises(Conflict):
            self.service.sign_handover("officer", draft["handover_id"])
        with self.assertRaises(InvalidState):
            self.service.create_handover("officer", "VOY-1", {"quantity_units": "1"})

    def test_validation_and_state_boundaries(self) -> None:
        with self.assertRaises(ValidationFailed):
            self.service.create_link("metro", {
                "link_id": "bad-link",
                "from_point_id": "haikui1-tank",
                "to_point_id": "wg-east-wellhead",
                "loss_basis_points": 5,
            })
        with self.assertRaises(Conflict):
            self.service.create_link("metro", {
                "link_id": "dup-link",
                "from_point_id": "wg-east-wellhead",
                "to_point_id": "pipe-inlet",
                "loss_basis_points": 5,
            })
        with self.assertRaises(ValidationFailed):
            self.service.register_batch("op", {"batch_id": "batch-x", "wellhead_point_id": "haikui1-tank"})
        self.register_batches()
        with self.assertRaises(ValidationFailed):
            self.service.upload_reading("op", {
                "point_id": "wg-east-wellhead",
                "batch_id": "batch-e-001",
                "observed_at": "2026-10-02T06:00:00Z",
                "raw_quantity": "1",
                "idempotency_key": "key-future",
            })
        with self.assertRaises(NotFound):
            self.service.upload_reading("op", {
                "point_id": "wg-east-wellhead",
                "batch_id": "batch-ghost",
                "observed_at": "2026-10-01T06:00:00Z",
                "raw_quantity": "1",
                "idempotency_key": "key-ghost",
            })
        self.service.create_point("metro", {
            "point_id": "bare-wellhead",
            "name": "未校准井口",
            "stage": "wellhead",
            "facility_id": "haiji-2",
            "well_group_id": "WG-NORTH",
        })
        self.service.register_batch("op", {"batch_id": "batch-n-001", "wellhead_point_id": "bare-wellhead"})
        with self.assertRaises(InvalidState):
            self.service.upload_reading("op", {
                "point_id": "bare-wellhead",
                "batch_id": "batch-n-001",
                "observed_at": "2026-10-01T06:00:00Z",
                "raw_quantity": "1",
                "idempotency_key": "key-no-cal",
            })

    def test_permissions_are_enforced(self) -> None:
        with self.assertRaises(Forbidden):
            self.service.create_point("op", {"point_id": "p-x", "name": "x", "stage": "offtake", "facility_id": "f"})
        with self.assertRaises(Forbidden):
            self.service.activate_calibration("op", self.calibration_ids["offtake-meter"])
        with self.assertRaises(Forbidden):
            self.service.upload_reading("metro", {
                "point_id": "wg-east-wellhead",
                "batch_id": "b",
                "observed_at": "2026-10-01T06:00:00Z",
                "raw_quantity": "1",
                "idempotency_key": "key-denied",
            })
        with self.assertRaises(Forbidden):
            self.service.close_settlement("op", "wg-east-wellhead", "batch-e-001")
        with self.assertRaises(Forbidden):
            self.service.open_dispute("mgr", {"dispute_id": "d", "batch_id": "b", "reason": "r"})
        with self.assertRaises(Forbidden):
            self.service.trace_voyage("op", "VOY-1")
        with self.assertRaises(Forbidden):
            self.service.balance_report("officer")
        with self.assertRaises(Forbidden):
            self.service.audit_chain("mgr")

    def test_audit_chain_detects_tampering(self) -> None:
        self.register_batches()
        self.assertTrue(self.service.audit_chain("audit")["valid"])
        self.connection.execute("UPDATE meter_audit_events SET payload_json='{}' WHERE event_id=1")
        self.assertFalse(self.service.audit_chain("audit")["valid"])

    def test_api_routes(self) -> None:
        app = JsonApplication(self.service)
        self.assertEqual(app.handle("GET", "/health").status, 200)
        missing_actor = app.handle("GET", "/balance")
        self.assertEqual(missing_actor.status, 422)
        created = app.handle(
            "POST",
            "/users",
            {"X-Actor-Id": "mgr"},
            body=json.dumps({"user_id": "op2", "display_name": "操作员乙", "role": "operator"}).encode(),
        )
        self.assertEqual(created.status, 201)
        balance = app.handle("GET", "/balance", {"X-Actor-Id": "mgr"})
        self.assertEqual(balance.status, 200)
        self.assertTrue(balance.body["balanced"])
        not_found = app.handle("GET", "/trace/VOY-404", {"X-Actor-Id": "mgr"})
        self.assertEqual(not_found.status, 404)
        unknown = app.handle("GET", "/no-such-route", {"X-Actor-Id": "mgr"})
        self.assertEqual(unknown.status, 404)


class AcceptanceTests(unittest.TestCase):
    def test_offline_acceptance(self) -> None:
        result = acceptance_run(ROOT)
        self.assertEqual(result["status"], "ok")
        self.assertTrue(result["idempotent_replay"])
        self.assertEqual(result["settlement"]["successor_revision"], 2)
        self.assertEqual(result["settlement"]["successor_quantity_units"], "10000.500")
        self.assertTrue(result["settlement"]["late_reading"])
        self.assertTrue(result["dispute_blocked_reading"])
        self.assertTrue(result["handover_unchanged_after_recalibration"])
        self.assertEqual(result["trace"]["well_groups"], ["WG-EAST", "WG-WEST"])
        self.assertEqual(result["trace"]["difference_units"], "0.000")
        self.assertTrue(result["balance"]["balanced"])
        self.assertTrue(result["audit"]["valid"])


if __name__ == "__main__":
    unittest.main()
