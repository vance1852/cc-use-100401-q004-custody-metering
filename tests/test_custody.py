from __future__ import annotations

import json
import sqlite3
import unittest
from datetime import datetime, timezone
from decimal import Decimal

from production_flow.clock import FrozenClock
from production_flow.custody import (
    LIFTING_TRANSFER,
    PLATFORM_SEPARATION,
    PIPELINE_BATCH,
    TANK_BLEND,
    WELL_PRODUCTION,
    FLOATING_PROCESSING,
    allowed_loss,
    apply_factor,
    attribute_sources,
    lineage_batch_ids,
    mass_balance,
    ordered_lineage,
    segment_variance,
)
from production_flow.errors import Conflict, Forbidden, InvalidState
from production_flow.service import SupplyService


def reading(calibration_id: str, meter: str, gross: str, at: str = "2026-10-05T06:00:00Z") -> dict[str, str]:
    return {
        "meter_id": meter,
        "calibration_version_id": calibration_id,
        "gross_reading": gross,
        "observed_at": at,
    }


class CustodyCalculationTests(unittest.TestCase):
    def test_apply_factor_and_allowed_loss(self) -> None:
        self.assertEqual(apply_factor(Decimal("1000"), Decimal("1.002")), Decimal("1002.000"))
        self.assertEqual(allowed_loss(Decimal("1600"), 50), Decimal("8.000"))
        with self.assertRaises(ValueError):
            allowed_loss(Decimal("1"), 2000)

    def test_segment_variance_is_inputs_minus_loss_minus_output(self) -> None:
        result = segment_variance(Decimal("1592"), Decimal("1576"), 100)
        self.assertEqual(result.allowed_loss, Decimal("15.920"))
        self.assertEqual(result.variance, Decimal("0.080"))

    def test_lineage_scope_covers_ancestors_and_descendants(self) -> None:
        links = [
            {"parent_batch_id": "w1", "child_batch_id": "s1", "consumed_quota_units": "1"},
            {"parent_batch_id": "s1", "child_batch_id": "p1", "consumed_quota_units": "1"},
            {"parent_batch_id": "p1", "child_batch_id": "t1", "consumed_quota_units": "1"},
            {"parent_batch_id": "t1", "child_batch_id": "l1", "consumed_quota_units": "1"},
            {"parent_batch_id": "wX", "child_batch_id": "sX", "consumed_quota_units": "1"},
        ]
        scope = lineage_batch_ids("p1", links)
        self.assertEqual(scope, frozenset({"w1", "s1", "p1", "t1", "l1"}))

    def test_attribute_sources_conserves_lifted_quantity_through_blend(self) -> None:
        batches = [
            {"batch_id": "w1", "segment": WELL_PRODUCTION, "quantity_quota_units": "1000",
             "allowed_loss_basis_points": 0, "state": "recorded", "well_group": "WG-A"},
            {"batch_id": "w2", "segment": WELL_PRODUCTION, "quantity_quota_units": "600",
             "allowed_loss_basis_points": 0, "state": "recorded", "well_group": "WG-B"},
            {"batch_id": "sep", "segment": PLATFORM_SEPARATION, "quantity_quota_units": "1600",
             "allowed_loss_basis_points": 0, "state": "recorded", "well_group": None},
            {"batch_id": "lift", "segment": LIFTING_TRANSFER, "quantity_quota_units": "800",
             "allowed_loss_basis_points": 0, "state": "recorded", "well_group": None},
        ]
        links = [
            {"parent_batch_id": "w1", "child_batch_id": "sep", "consumed_quota_units": "1000"},
            {"parent_batch_id": "w2", "child_batch_id": "sep", "consumed_quota_units": "600"},
            {"parent_batch_id": "sep", "child_batch_id": "lift", "consumed_quota_units": "800"},
        ]
        sources = attribute_sources("lift", batches, links)
        total = sum((Decimal(row["attributed_quota_units"]) for row in sources), Decimal("0"))
        self.assertEqual(total, Decimal("800.000"))
        self.assertEqual(sources[0]["well_group"], "WG-A")
        self.assertEqual(sources[0]["attributed_quota_units"], "500.000")
        self.assertEqual(sources[1]["attributed_quota_units"], "300.000")

    def test_mass_balance_telescopes_to_zero_residual(self) -> None:
        batches = [
            {"batch_id": "w1", "segment": WELL_PRODUCTION, "quantity_quota_units": "1600",
             "allowed_loss_basis_points": 0, "state": "recorded", "well_group": "WG-A"},
            {"batch_id": "sep", "segment": PLATFORM_SEPARATION, "quantity_quota_units": "1592",
             "allowed_loss_basis_points": 50, "state": "recorded", "well_group": None},
            {"batch_id": "pipe", "segment": PIPELINE_BATCH, "quantity_quota_units": "1576",
             "allowed_loss_basis_points": 100, "state": "recorded", "well_group": None},
            {"batch_id": "fpso", "segment": FLOATING_PROCESSING, "quantity_quota_units": "1576",
             "allowed_loss_basis_points": 0, "state": "recorded", "well_group": None},
            {"batch_id": "tank", "segment": TANK_BLEND, "quantity_quota_units": "1576",
             "allowed_loss_basis_points": 0, "state": "recorded", "well_group": None},
            {"batch_id": "lift", "segment": LIFTING_TRANSFER, "quantity_quota_units": "1000",
             "allowed_loss_basis_points": 0, "state": "recorded", "well_group": None},
        ]
        links = [
            {"parent_batch_id": "w1", "child_batch_id": "sep", "consumed_quota_units": "1600"},
            {"parent_batch_id": "sep", "child_batch_id": "pipe", "consumed_quota_units": "1592"},
            {"parent_batch_id": "pipe", "child_batch_id": "fpso", "consumed_quota_units": "1576"},
            {"parent_batch_id": "fpso", "child_batch_id": "tank", "consumed_quota_units": "1576"},
            {"parent_batch_id": "tank", "child_batch_id": "lift", "consumed_quota_units": "1000"},
        ]
        balance = mass_balance(batches, links)
        self.assertTrue(balance["balanced"])
        self.assertEqual(balance["residual"], "0.000")
        self.assertEqual(balance["production"], "1600.000")
        self.assertEqual(balance["inventory"], "576.000")
        self.assertEqual(balance["offtake"], "1000.000")
        self.assertEqual(balance["allowed_loss"], "23.920")
        self.assertEqual(balance["unaccounted_variance"], "0.080")
        self.assertEqual(
            ordered_lineage("lift", batches, links),
            ["w1", "sep", "pipe", "fpso", "tank", "lift"],
        )

    def test_mass_balance_detects_over_consumption(self) -> None:
        batches = [
            {"batch_id": "w1", "segment": WELL_PRODUCTION, "quantity_quota_units": "10",
             "allowed_loss_basis_points": 0, "state": "recorded", "well_group": "WG-A"},
            {"batch_id": "lift", "segment": PLATFORM_SEPARATION, "quantity_quota_units": "10",
             "allowed_loss_basis_points": 0, "state": "recorded", "well_group": None},
        ]
        links = [{"parent_batch_id": "w1", "child_batch_id": "lift", "consumed_quota_units": "12"}]
        with self.assertRaises(ValueError):
            mass_balance(batches, links)


class CustodyServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.clock = FrozenClock(datetime(2026, 10, 5, 8, 0, tzinfo=timezone.utc))
        self.service = SupplyService(self.connection, self.clock)
        for uid, role in (
            ("plan", "planner"),
            ("dispatch", "dispatcher"),
            ("risk", "risk"),
            ("audit", "auditor"),
            ("meter", "metering"),
        ):
            self.service.create_user(uid, uid, role)
        self.custody = self.service.custody
        for cid, kind in (
            ("cal-w", "wellhead"),
            ("cal-s", "separator"),
            ("cal-p", "pipeline"),
            ("cal-f", "fpsoprocess"),
            ("cal-t", "tank"),
            ("cal-l", "lifting"),
        ):
            self.custody.register_calibration("meter", {
                "calibration_version_id": cid, "meter_kind": kind, "correction_factor": "1.000",
                "basis": "实流标定", "effective_from": "2026-10-01T00:00:00Z",
            })

    def tearDown(self) -> None:
        self.connection.close()

    def cal(self, cid: str, kind: str, factor: str) -> None:
        self.custody.register_calibration("meter", {
            "calibration_version_id": cid, "meter_kind": kind, "correction_factor": factor,
            "basis": "复测", "effective_from": "2026-10-02T00:00:00Z",
        })

    def batch(self, bid, segment, cal_id, meter, gross, key, *, loss=0, well=None, at="2026-10-05T06:00:00Z"):
        payload = {
            "batch_id": bid, "segment": segment, "product": "crude-oil", "well_group": well,
            "allowed_loss_basis_points": loss, "observed_at": at,
            "readings": [reading(cal_id, meter, gross, at)], "idempotency_key": key,
        }
        return self.custody.record_batch("meter", payload), payload

    def build_chain(self):
        self.batch("wp-A", WELL_PRODUCTION, "cal-w", "m-wa", "1000", "k-wa", well="WG-A")
        self.batch("wp-B", WELL_PRODUCTION, "cal-w", "m-wb", "600", "k-wb", well="WG-B")
        self.batch("sep-1", PLATFORM_SEPARATION, "cal-s", "m-sep", "1592", "k-sep", loss=50)
        self.batch("pipe-1", PIPELINE_BATCH, "cal-p", "m-pipe", "1576", "k-pipe", loss=100)
        self.batch("fpso-1", FLOATING_PROCESSING, "cal-f", "m-fpso", "1576", "k-fpso")
        self.batch("tank-1", TANK_BLEND, "cal-t", "m-tank", "1576", "k-tank")
        self.batch("lift-1", LIFTING_TRANSFER, "cal-l", "m-lift", "1000", "k-lift")
        for parent, child, qty, key in (
            ("wp-A", "sep-1", "1000", "lk-1"),
            ("wp-B", "sep-1", "600", "lk-2"),
            ("sep-1", "pipe-1", "1592", "lk-3"),
            ("pipe-1", "fpso-1", "1576", "lk-4"),
            ("fpso-1", "tank-1", "1576", "lk-5"),
            ("tank-1", "lift-1", "1000", "lk-6"),
        ):
            self.custody.link_batches("meter", {
                "parent_batch_id": parent, "child_batch_id": child,
                "consumed_quota_units": qty, "idempotency_key": key,
            })

    def link(self, parent, child, qty, key):
        return self.custody.link_batches("meter", {
            "parent_batch_id": parent, "child_batch_id": child,
            "consumed_quota_units": qty, "idempotency_key": key,
        })

    # -- 校准与读数 ----------------------------------------------------------

    def test_duplicate_calibration_id_conflicts_and_factor_is_snapshotted(self) -> None:
        with self.assertRaises(Conflict):
            self.cal("cal-w", "wellhead", "1.020")
        result, _ = self.batch("wp-x", WELL_PRODUCTION, "cal-w", "m-x", "1000", "k-x", well="WG-X")
        self.assertEqual(result["quantity_quota_units"], "1000.000")
        row = self.connection.execute(
            "SELECT factor_snapshot FROM meter_readings WHERE batch_id='wp-x'"
        ).fetchone()
        self.assertEqual(row["factor_snapshot"], "1.000")

    def test_calibration_registration_is_naturally_idempotent(self) -> None:
        payload = {
            "calibration_version_id": "cal-dup", "meter_kind": "tank", "correction_factor": "1.003",
            "basis": "实流标定", "effective_from": "2026-10-03T00:00:00Z",
        }
        first = self.custody.register_calibration("meter", payload)
        second = self.custody.register_calibration("meter", payload)
        self.assertEqual(first, second)
        self.assertEqual(
            self.connection.execute(
                "SELECT COUNT(*) AS n FROM calibration_versions WHERE calibration_id='cal-dup'"
            ).fetchone()["n"],
            1,
        )

    def test_batch_aggregates_multiple_meter_readings_with_factor(self) -> None:
        payload = {
            "batch_id": "wp-multi", "segment": WELL_PRODUCTION, "product": "crude-oil",
            "well_group": "WG-M", "allowed_loss_basis_points": 0,
            "observed_at": "2026-10-05T06:00:00Z",
            "readings": [
                reading("cal-w", "m1", "400"),
                reading("cal-w", "m2", "600"),
            ],
            "idempotency_key": "k-multi",
        }
        result = self.custody.record_batch("meter", payload)
        self.assertEqual(result["gross_quota_units"], "1000.000")
        self.assertEqual(result["quantity_quota_units"], "1000.000")
        self.assertEqual(
            self.connection.execute(
                "SELECT COUNT(*) AS n FROM meter_readings WHERE batch_id='wp-multi'"
            ).fetchone()["n"],
            2,
        )

    def test_new_calibration_version_does_not_rewrite_recorded_quantity(self) -> None:
        self.batch("wp-x", WELL_PRODUCTION, "cal-w", "m-x", "1000", "k-x", well="WG-X")
        self.cal("cal-w-2", "wellhead", "1.010")
        row = self.connection.execute(
            "SELECT quantity_quota_units FROM custody_batches WHERE batch_id='wp-x'"
        ).fetchone()
        self.assertEqual(row["quantity_quota_units"], "1000.000")

    def test_reading_requires_matching_meter_kind(self) -> None:
        # 井口段使用分离段校准必须被拒
        with self.assertRaises(Exception):
            self.custody.record_batch("meter", {
                "batch_id": "wp-bad", "segment": WELL_PRODUCTION, "product": "crude-oil",
                "well_group": "WG-Z", "allowed_loss_basis_points": 0,
                "observed_at": "2026-10-05T06:00:00Z",
                "readings": [reading("cal-s", "m-x", "100")], "idempotency_key": "k-bad",
            })

    # -- 幂等 ----------------------------------------------------------------

    def test_batch_upload_is_idempotent_and_conflicts_on_changed_payload(self) -> None:
        first, payload = self.batch("wp-a", WELL_PRODUCTION, "cal-w", "m-a", "1000", "k-a", well="WG-A")
        second = self.custody.record_batch("meter", payload)
        self.assertEqual(first, second)
        changed = dict(payload, readings=[reading("cal-w", "m-a", "1001")])
        with self.assertRaises(Conflict):
            self.custody.record_batch("meter", changed)

    def test_link_is_idempotent(self) -> None:
        self.batch("wp-a", WELL_PRODUCTION, "cal-w", "m-a", "100", "k-a", well="WG-A")
        self.batch("sep", PLATFORM_SEPARATION, "cal-s", "m-s", "100", "k-s")
        payload = {"parent_batch_id": "wp-a", "child_batch_id": "sep",
                   "consumed_quota_units": "100", "idempotency_key": "lk"}
        first = self.custody.link_batches("meter", payload)
        second = self.custody.link_batches("meter", payload)
        self.assertEqual(first["link_id"], second["link_id"])

    # -- 监管链约束 ----------------------------------------------------------

    def test_link_rejects_wrong_segment_order_and_over_consumption(self) -> None:
        self.batch("wp-a", WELL_PRODUCTION, "cal-w", "m-a", "100", "k-a", well="WG-A")
        self.batch("tank", TANK_BLEND, "cal-t", "m-t", "100", "k-tank")
        with self.assertRaises(InvalidState):
            self.link("wp-a", "tank", "100", "lk-wrong")
        self.batch("sep", PLATFORM_SEPARATION, "cal-s", "m-s", "100", "k-s")
        self.link("wp-a", "sep", "100", "lk-ok")
        with self.assertRaises(Conflict):
            self.link("wp-a", "sep", "1", "lk-extra")

    # -- 交接不可变 ----------------------------------------------------------

    def test_signed_handover_quantity_is_frozen_and_cannot_be_re_signed(self) -> None:
        self.build_chain()
        payload = {"lifting_batch_id": "lift-1", "vessel_voyage": "V-1", "receiver": "炼厂",
                   "terminal": "锚地", "bill_of_lading": "BL-1", "idempotency_key": "ho"}
        signed = self.custody.sign_handover("dispatch", payload)
        self.assertEqual(signed["signed_quantity_quota_units"], "1000.000")
        replay = self.custody.sign_handover("dispatch", payload)
        self.assertEqual(replay["handover_id"], signed["handover_id"])
        with self.assertRaises(Conflict):
            self.custody.sign_handover("dispatch", dict(payload, idempotency_key="ho-2"))
        # 已签署交接的提油批不能再追加来源链接
        self.batch("tank-2", TANK_BLEND, "cal-t", "m-tank2", "100", "k-tank2")
        with self.assertRaises(InvalidState):
            self.link("tank-2", "lift-1", "100", "lk-ho")

    def test_late_reading_cannot_rewrite_signed_handover(self) -> None:
        self.build_chain()
        self.custody.sign_handover("dispatch", {
            "lifting_batch_id": "lift-1", "vessel_voyage": "V-1", "receiver": "炼厂",
            "terminal": "锚地", "idempotency_key": "ho"})
        with self.assertRaises(InvalidState):
            self.custody.record_late_reading("meter", {
                "batch_id": "lift-1", "reason_code": "recheck", "note": "船方复测",
                "idempotency_key": "late", "readings": [reading("cal-l", "m-lift", "1005")]})

    # -- 迟到读数 -> 后继结算 -------------------------------------------------

    def test_late_reading_creates_successor_without_moving_original(self) -> None:
        self.build_chain()
        settlement = self.custody.record_late_reading("meter", {
            "batch_id": "pipe-1", "reason_code": "resync", "note": "流量计算机补传",
            "idempotency_key": "late-pipe", "readings": [reading("cal-p", "m-pipe", "1578")]})
        self.assertEqual(settlement["delta_net_quota_units"], "2.000")
        original = self.connection.execute(
            "SELECT quantity_quota_units,state FROM custody_batches WHERE batch_id='pipe-1'"
        ).fetchone()
        self.assertEqual(original["quantity_quota_units"], "1576.000")
        self.assertEqual(original["state"], "recorded")
        replay = self.custody.record_late_reading("meter", {
            "batch_id": "pipe-1", "reason_code": "resync", "note": "流量计算机补传",
            "idempotency_key": "late-pipe", "readings": [reading("cal-p", "m-pipe", "1578")]})
        self.assertEqual(replay["settlement_id"], settlement["settlement_id"])
        balance = self.custody.material_balance("audit")
        self.assertTrue(balance["balanced"])

    def test_late_reading_below_already_consumed_is_rejected(self) -> None:
        self.build_chain()
        with self.assertRaises(InvalidState):
            self.custody.record_late_reading("meter", {
                "batch_id": "tank-1", "reason_code": "resync", "note": "下修",
                "idempotency_key": "late-tank", "readings": [reading("cal-t", "m-tank", "900")]})

    # -- 争议冻结 ------------------------------------------------------------

    def test_single_dispute_freees_only_anchor_and_blocks_its_links(self) -> None:
        self.build_chain()
        dispute = self.custody.open_dispute("risk", {
            "scope": "single", "anchor_batch_id": "tank-1", "note": "液位争议", "idempotency_key": "d1"})
        self.assertEqual(dispute["frozen_batches"], ["tank-1"])
        self.batch("lift-2", LIFTING_TRANSFER, "cal-l", "m-lift2", "400", "k-lift2")
        with self.assertRaises(InvalidState):
            self.link("tank-1", "lift-2", "400", "lk-7")
        # 井口环节不受影响，仍可登记新读数
        self.batch("wp-C", WELL_PRODUCTION, "cal-w", "m-wc", "10", "k-wc", well="WG-C")
        resolved = self.custody.resolve_dispute("risk", dispute["dispute_id"], "复测无误")
        self.assertEqual(resolved["unfrozen_batches"], ["tank-1"])
        self.link("tank-1", "lift-2", "400", "lk-7")

    def test_lineage_dispute_freees_ancestors_and_descendants(self) -> None:
        self.build_chain()
        dispute = self.custody.open_dispute("risk", {
            "scope": "lineage", "anchor_batch_id": "pipe-1", "note": "管输系数", "idempotency_key": "d2"})
        self.assertEqual(set(dispute["frozen_batches"]),
                         {"wp-A", "wp-B", "sep-1", "pipe-1", "fpso-1", "tank-1", "lift-1"})

    def test_overlapping_disputes_keep_shared_batch_frozen(self) -> None:
        self.build_chain()
        d1 = self.custody.open_dispute("risk", {
            "scope": "single", "anchor_batch_id": "tank-1", "note": "争议1", "idempotency_key": "d1"})
        d2 = self.custody.open_dispute("risk", {
            "scope": "single", "anchor_batch_id": "lift-1", "note": "争议2", "idempotency_key": "d2"})
        self.custody.resolve_dispute("risk", d1["dispute_id"], "处理")
        state = self.connection.execute(
            "SELECT state FROM custody_batches WHERE batch_id='tank-1'"
        ).fetchone()["state"]
        self.assertEqual(state, "recorded")
        # lift-1 仍被 d2 冻结
        with self.assertRaises(InvalidState):
            self.custody.record_late_reading("meter", {
                "batch_id": "lift-1", "reason_code": "resync", "note": "n",
                "idempotency_key": "late-l", "readings": [reading("cal-l", "m-lift", "1001")]})
        self.custody.resolve_dispute("risk", d2["dispute_id"], "处理")

    # -- 反查与恒等式 --------------------------------------------------------

    def test_trace_voyage_attributes_sources_and_shows_segment_differences(self) -> None:
        self.build_chain()
        self.custody.sign_handover("dispatch", {
            "lifting_batch_id": "lift-1", "vessel_voyage": "MT-712", "receiver": "炼厂",
            "terminal": "舟山", "bill_of_lading": "BL-712", "idempotency_key": "ho"})
        trace = self.custody.trace_voyage("audit", "MT-712")
        self.assertTrue(trace["signed_quantity_immutable"])
        self.assertEqual(trace["signed_quantity_quota_units"], "1000.000")
        total = sum((Decimal(r["attributed_quota_units"]) for r in trace["source_well_groups"]), Decimal("0"))
        self.assertEqual(total, Decimal("1000.000"))
        by_id = {row["batch_id"]: row for row in trace["lineage"]}
        self.assertEqual(by_id["pipe-1"]["segment_difference"]["segment_variance"], "0.080")
        self.assertEqual(by_id["wp-A"]["well_group"], "WG-A")
        self.assertEqual(by_id["wp-B"]["well_group"], "WG-B")

    def test_balance_reports_open_disputes_and_balances(self) -> None:
        self.build_chain()
        balance = self.custody.material_balance("audit")
        self.assertTrue(balance["balanced"])
        self.custody.open_dispute("risk", {
            "scope": "single", "anchor_batch_id": "sep-1", "note": "n", "idempotency_key": "disp"})
        balance = self.custody.material_balance("audit")
        self.assertIn("sep-1", balance["frozen_batches"])
        self.assertEqual(balance["open_disputes"][0]["anchor_batch_id"], "sep-1")

    # -- 权限 ----------------------------------------------------------------

    def test_role_separation_for_custody_writes(self) -> None:
        with self.assertRaises(Forbidden):
            self.custody.register_calibration("plan", {
                "calibration_version_id": "c", "meter_kind": "tank", "correction_factor": "1",
                "basis": "b", "effective_from": "2026-10-01T00:00:00Z"})
        with self.assertRaises(Forbidden):
            self.custody.open_dispute("meter", {
                "scope": "single", "anchor_batch_id": "x", "note": "n", "idempotency_key": "k"})
        with self.assertRaises(Forbidden):
            self.custody.material_balance("dispatch")


if __name__ == "__main__":
    unittest.main()
