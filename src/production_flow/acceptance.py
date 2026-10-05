"""贯通处理额度单价、生产输送通道、处理额度库存、外输申请和情景分析的离线验收。"""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .clock import FrozenClock
from .service import SupplyService


def _run_custody(service) -> dict[str, object]:
    """端到端计量监管链：井口→平台分离→管输→浮式加工→储罐混合→提油交接。"""
    custody = service.custody
    service.create_user("meter", "meter", "metering")
    calibrations = (
        ("cal-well", "wellhead", "1.000"),
        ("cal-sep", "separator", "1.000"),
        ("cal-pipe", "pipeline", "1.000"),
        ("cal-fpso", "fpsoprocess", "1.000"),
        ("cal-tank", "tank", "1.000"),
        ("cal-lift", "lifting", "1.000"),
    )
    for calibration_id, meter_kind, factor in calibrations:
        custody.register_calibration("meter", {
            "calibration_version_id": calibration_id,
            "meter_kind": meter_kind,
            "correction_factor": factor,
            "basis": "离线实流标定",
            "effective_from": "2026-09-20T00:00:00Z",
        })

    def reading(calibration_id: str, meter_id: str, gross: str) -> dict[str, str]:
        return {
            "meter_id": meter_id,
            "calibration_version_id": calibration_id,
            "gross_reading": gross,
            "observed_at": "2026-09-25T06:00:00Z",
        }

    def batch(batch_id: str, segment: str, calibration_id: str, meter_id: str,
              gross: str, key: str, *, loss: int = 0, well_group: str | None = None) -> None:
        custody.record_batch("meter", {
            "batch_id": batch_id,
            "segment": segment,
            "product": "crude-oil",
            "well_group": well_group,
            "allowed_loss_basis_points": loss,
            "observed_at": "2026-09-25T06:00:00Z",
            "readings": [reading(calibration_id, meter_id, gross)],
            "idempotency_key": key,
        })

    # 两个来源井组汇入海基二号分离器。
    batch("well-a", "well_production", "cal-well", "mt-well-a", "1000", "ck-well-a", well_group="WG-NORTH")
    batch("well-b", "well_production", "cal-well", "mt-well-b", "600", "ck-well-b", well_group="WG-SOUTH")
    # 平台分离允许 50bp 损耗；海底管道允许 100bp，存在 0.080 未解释差异。
    batch("sep-0925", "platform_separation", "cal-sep", "mt-sep", "1592", "ck-sep", loss=50)
    batch("pipe-0925", "pipeline_batch", "cal-pipe", "mt-pipe", "1576", "ck-pipe", loss=100)
    batch("fpso-0925", "floating_processing", "cal-fpso", "mt-fpso", "1576", "ck-fpso")
    batch("tank-0925", "tank_blend", "cal-tank", "mt-tank", "1576", "ck-tank")
    batch("lift-0925", "lifting_transfer", "cal-lift", "mt-lift", "1000", "ck-lift")
    for parent, child, consumed, key in (
        ("well-a", "sep-0925", "1000", "clk-1"),
        ("well-b", "sep-0925", "600", "clk-2"),
        ("sep-0925", "pipe-0925", "1592", "clk-3"),
        ("pipe-0925", "fpso-0925", "1576", "clk-4"),
        ("fpso-0925", "tank-0925", "1576", "clk-5"),
        ("tank-0925", "lift-0925", "1000", "clk-6"),
    ):
        custody.link_batches("meter", {
            "parent_batch_id": parent,
            "child_batch_id": child,
            "consumed_quota_units": consumed,
            "idempotency_key": key,
        })

    custody.sign_handover("dispatch", {
        "lifting_batch_id": "lift-0925",
        "vessel_voyage": "MT-HAIJI-0925",
        "receiver": "远东炼化",
        "terminal": "舟山锚地",
        "bill_of_lading": "BL-0925",
        "idempotency_key": "ck-handover",
    })
    balance = custody.material_balance("audit")
    trace = custody.trace_voyage("audit", "MT-HAIJI-0925")
    return {
        "balanced": balance["balanced"],
        "residual": balance["residual"],
        "production": balance["production"],
        "offtake": balance["offtake"],
        "inventory": balance["inventory"],
        "allowed_loss": balance["allowed_loss"],
        "unaccounted_variance": balance["unaccounted_variance"],
        "vessel_voyage": trace["vessel_voyage"],
        "signed_quantity_quota_units": trace["signed_quantity_quota_units"],
        "source_well_groups": trace["source_well_groups"],
    }


def run(workspace: Path) -> dict[str, object]:
    connection = sqlite3.connect(":memory:", isolation_level=None)
    connection.row_factory = sqlite3.Row
    service = SupplyService(connection, FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc)))
    for user_id, role in (("plan", "planner"), ("dispatch", "dispatcher"), ("risk", "risk"), ("audit", "auditor")):
        service.create_user(user_id, user_id, role)
    for index, close in enumerate(("108", "105", "102", "100", "98", "96"), start=18):
        service.record_quote("plan", {"market_index": "BRENT", "trade_date": f"2026-09-{index}", "close_cny": close, "source_revision": f"rev-{index}", "observed_at": f"2026-09-{index}T21:00:00Z"})
    service.create_facility("plan", {"facility_id": "cluster-a", "name": "北部深水生产节点", "kind": "storage", "timezone": "Asia/Shanghai", "capacity_quota_units": "500000"})
    service.create_facility("plan", {"facility_id": "pool-b", "name": "东部浮式处理中心", "kind": "floating-processing", "timezone": "Asia/Shanghai", "capacity_quota_units": "800000"})
    service.create_route("plan", {"route_id": "subsea-pipeline-a-b", "origin_id": "cluster-a", "destination_id": "pool-b", "product": "crude-oil", "daily_capacity": "100000", "loss_basis_points": 25, "transit_hours": 36})
    service.add_inventory_lot("dispatch", {"lot_id": "lot-001", "facility_id": "cluster-a", "product": "crude-oil", "grade": "BRENT", "quantity_quota_units": "150000", "unit_cost_cny": "91.25", "received_at": "2026-09-24T06:00:00Z"})
    service.submit_nomination("dispatch", {"nomination_id": "nom-001", "route_id": "subsea-pipeline-a-b", "shipper_id": "tenant-east", "service_date": "2026-09-25", "requested_quota_units": "80000", "priority": 10, "idempotency_key": "nom-key-001"})
    allocation = service.allocate("dispatch", "subsea-pipeline-a-b", "2026-09-25")
    transfer = service.dispatch_transfer("dispatch", "transfer-001", "nom-001", "lot-001", 2)
    service.create_scenario("plan", {"scenario_id": "pipeline-recovery", "name": "关键服务节点检修恢复与需求回落", "market_index_drop_percent": "9", "route_capacity_changes": {"subsea-pipeline-a-b": "20"}, "demand_changes": {"cluster-a:crude-oil": "-5"}})
    service.approve_scenario("risk", "pipeline-recovery", 1)
    scenario = service.run_scenario("plan", "pipeline-recovery", "2026-09-23")
    custody_report = _run_custody(service)
    result = {"status": "ok", "price": service.price_summary("BRENT"), "allocation_id": allocation["allocation_id"], "transfer": transfer, "scenario_run_id": scenario["run_id"], "custody": custody_report, "audit": service.audit_chain("audit"), "workspace": workspace.name}
    connection.close()
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="运行深水生产节点调度服务离线验收")
    parser.add_argument("--workspace", type=Path, default=Path.cwd())
    args = parser.parse_args(argv)
    print(json.dumps(run(args.workspace), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
