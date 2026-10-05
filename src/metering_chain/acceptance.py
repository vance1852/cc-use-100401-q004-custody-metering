"""贯通井口、分离、管输、浮式加工、储罐、提油交接和校准版本的离线验收。

场景：海基二号两个井组（WG-EAST、WG-WEST）的原油批次经平台分离、海底管道
进入海葵一号浮式加工与储罐，最终由提油轮 VOY-001 外输。演示覆盖迟到读数
后继结算、重复上传幂等、争议冻结、签署交接不被新系数改写、船次反查与
全链平衡。
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .clock import FrozenClock
from .errors import InvalidState
from .service import MeteringService


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

CHAIN_READINGS = {
    "batch-e-001": [
        ("wg-east-wellhead", "10000"),
        ("haiji2-separation", "9990.5"),
        ("pipe-inlet", "9986"),
        ("pipe-outlet", "9967"),
        ("haikui1-processing", "9962.5"),
        ("haikui1-tank", "9953"),
    ],
    "batch-w-001": [
        ("wg-west-wellhead", "5000"),
        ("haiji2-separation", "4995"),
        ("pipe-inlet", "4992.5"),
        ("pipe-outlet", "4982.5"),
        ("haikui1-processing", "4980"),
        ("haikui1-tank", "4976"),
    ],
}


def run(workspace: Path) -> dict[str, object]:
    connection = sqlite3.connect(":memory:", isolation_level=None)
    connection.row_factory = sqlite3.Row
    clock = FrozenClock(datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc))
    service = MeteringService(connection, clock)
    for user_id, role in (
        ("op", "operator"),
        ("metro", "metrologist"),
        ("officer", "officer"),
        ("mgr", "manager"),
        ("audit", "auditor"),
    ):
        service.create_user(user_id, user_id, role)
    for point_id, name, stage, facility, well_group in POINTS:
        service.create_point("metro", {
            "point_id": point_id,
            "name": name,
            "stage": stage,
            "facility_id": facility,
            "well_group_id": well_group,
        })
    for link_id, from_point, to_point, loss_bp in LINKS:
        service.create_link("metro", {
            "link_id": link_id,
            "from_point_id": from_point,
            "to_point_id": to_point,
            "loss_basis_points": loss_bp,
        })
    for point_id, *_ in POINTS:
        created = service.create_calibration("metro", {
            "point_id": point_id,
            "version_no": 1,
            "coefficient": "1",
            "note": "投用基线校准",
        })
        service.activate_calibration("metro", created["version_id"])
    service.register_batch("op", {"batch_id": "batch-e-001", "wellhead_point_id": "wg-east-wellhead"})
    service.register_batch("op", {"batch_id": "batch-w-001", "wellhead_point_id": "wg-west-wellhead"})
    for batch_index, (batch_id, readings) in enumerate(CHAIN_READINGS.items()):
        for index, (point_id, quantity) in enumerate(readings):
            service.upload_reading("op", {
                "point_id": point_id,
                "batch_id": batch_id,
                "observed_at": f"2026-10-01T{6 + batch_index:02d}:{index * 10:02d}:00Z",
                "raw_quantity": quantity,
                "idempotency_key": f"{batch_id}-{point_id}-r1",
            })
    first = service.close_settlement("officer", "wg-east-wellhead", "batch-e-001")
    late = service.upload_reading("op", {
        "point_id": "wg-east-wellhead",
        "batch_id": "batch-e-001",
        "observed_at": "2026-10-01T05:30:00Z",
        "raw_quantity": "0.5",
        "idempotency_key": "batch-e-001-wellhead-late-1",
    })
    replay = service.upload_reading("op", {
        "point_id": "wg-east-wellhead",
        "batch_id": "batch-e-001",
        "observed_at": "2026-10-01T05:30:00Z",
        "raw_quantity": "0.5",
        "idempotency_key": "batch-e-001-wellhead-late-1",
    })
    successor = service.close_settlement("officer", "wg-east-wellhead", "batch-e-001")
    service.create_voyage("officer", {
        "voyage_id": "VOY-001",
        "tank_point_id": "haikui1-tank",
        "vessel_name": "远海号提油轮",
    })
    draft = service.create_handover("officer", "VOY-001", {"quantity_units": "6000"})
    signed = service.sign_handover("officer", draft["handover_id"])
    service.open_dispute("officer", {
        "dispute_id": "dispute-001",
        "batch_id": "batch-w-001",
        "point_id": "pipe-outlet",
        "reason": "管输出口流量计漂移待核查",
    })
    dispute_blocked = False
    try:
        service.upload_reading("op", {
            "point_id": "wg-west-wellhead",
            "batch_id": "batch-w-001",
            "observed_at": "2026-10-01T08:00:00Z",
            "raw_quantity": "1",
            "idempotency_key": "batch-w-001-blocked-1",
        })
    except InvalidState:
        dispute_blocked = True
    service.resolve_dispute("officer", "dispute-001", "流量计复检合格，解除冻结")
    recalibration = service.create_calibration("metro", {
        "point_id": "offtake-meter",
        "version_no": 2,
        "coefficient": "1.0002",
        "note": "标定后微调",
    })
    service.activate_calibration("metro", recalibration["version_id"])
    trace = service.trace_voyage("mgr", "VOY-001")
    balance = service.balance_report("mgr")
    audit = service.audit_chain("audit")
    result = {
        "status": "ok",
        "workspace": workspace.name,
        "settlement": {
            "first_revision": first["revision"],
            "successor_revision": successor["revision"],
            "successor_quantity_units": successor["quantity_units"],
            "late_reading": late["late"],
        },
        "idempotent_replay": replay["reading_id"] == late["reading_id"],
        "handover": {
            "voyage_id": "VOY-001",
            "quantity_units": signed["quantity_units"],
            "attributions": signed["attributions"],
        },
        "dispute_blocked_reading": dispute_blocked,
        "handover_unchanged_after_recalibration": trace["handover"]["quantity_units"] == "6000"
        and trace["handover"]["calibration_version_id"] != recalibration["version_id"],
        "trace": {
            "voyage_id": trace["voyage_id"],
            "well_groups": sorted({source["well_group_id"] for source in trace["sources"]}),
            "source_count": len(trace["sources"]),
            "difference_units": trace["difference_units"],
        },
        "balance": {
            "balanced": balance["balanced"],
            **balance["totals"],
        },
        "audit": {"valid": audit["valid"], "events": audit["events"]},
    }
    connection.close()
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="运行端到端计量监管链离线验收")
    parser.add_argument("--workspace", type=Path, default=Path.cwd())
    args = parser.parse_args(argv)
    print(json.dumps(run(args.workspace), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
