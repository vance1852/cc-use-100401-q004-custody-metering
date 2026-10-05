"""端到端计量监管链的事务用例。

把井口产量、平台分离、管输批次、浮式加工、储罐混合、提油交接和校准版本
串成一条可审计的监管链：读数在上传时钉住校准版本，迟到读数只能形成后继
结算，重复上传保持幂等，计量争议只冻结相关批次，已签署的交接量不会被新
系数静默改写。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from decimal import Decimal
from typing import Any, Mapping

from .clock import SystemClock, parse_utc, utc_text
from .errors import Conflict, Forbidden, InvalidState, NotFound, ValidationFailed
from .ledger import (
    ZERO,
    allocate_proportional,
    canonical_json,
    corrected_quantity,
    decimal_text,
    digest,
    quantize_volume,
    segment_variance,
    summarize_balance,
)
from .models import (
    STAGE_ORDER,
    BatchInput,
    CalibrationInput,
    ChainLinkInput,
    DisputeInput,
    HandoverInput,
    MeteringPointInput,
    ReadingInput,
    VoyageInput,
)
from .storage import initialize, transaction


ROLE_PERMISSIONS = {
    "operator": {"reading.write", "batch.write"},
    "metrologist": {"topology.write", "calibration.write"},
    "officer": {"settlement.close", "handover.write", "dispute.write"},
    "manager": {"trace.read", "balance.read"},
    "auditor": {"audit.read", "trace.read", "balance.read"},
}

DEFAULT_TOLERANCE_BASIS_POINTS = 50


class MeteringService:
    def __init__(self, connection: sqlite3.Connection, clock=None) -> None:
        self.connection = connection
        self.clock = clock or SystemClock()
        initialize(connection)

    def _now(self) -> str:
        return utc_text(self.clock.now())

    def _user(self, user_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM meter_users WHERE user_id=?", (user_id,)
        ).fetchone()
        if row is None:
            raise NotFound("用户不存在")
        if not row["active"]:
            raise Forbidden("用户已停用")
        return row

    def _require(self, user_id: str, permission: str) -> sqlite3.Row:
        user = self._user(user_id)
        if permission not in ROLE_PERMISSIONS[user["role"]]:
            raise Forbidden(f"角色 {user['role']} 无权执行 {permission}")
        return user

    def _audit(
        self,
        entity_type: str,
        entity_id: str,
        event_type: str,
        actor_id: str,
        payload: Mapping[str, Any],
    ) -> None:
        previous = self.connection.execute(
            "SELECT event_hash FROM meter_audit_events ORDER BY event_id DESC LIMIT 1"
        ).fetchone()
        previous_hash = "0" * 64 if previous is None else previous["event_hash"]
        body = {
            "entity_type": entity_type,
            "entity_id": entity_id,
            "event_type": event_type,
            "actor_id": actor_id,
            "payload": payload,
            "created_at": self._now(),
            "previous_hash": previous_hash,
        }
        event_hash = hashlib.sha256(canonical_json(body).encode("utf-8")).hexdigest()
        self.connection.execute(
            "INSERT INTO meter_audit_events(entity_type,entity_id,event_type,actor_id,payload_json,"
            "previous_hash,event_hash,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (
                entity_type,
                entity_id,
                event_type,
                actor_id,
                canonical_json(payload),
                previous_hash,
                event_hash,
                body["created_at"],
            ),
        )

    def create_user(self, user_id: str, display_name: str, role: str) -> dict[str, Any]:
        if role not in ROLE_PERMISSIONS:
            raise ValidationFailed("未知角色")
        if not user_id.strip() or not display_name.strip():
            raise ValidationFailed("用户编号和名称不能为空")
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO meter_users(user_id,display_name,role,created_at) VALUES(?,?,?,?)",
                    (user_id.strip(), display_name.strip(), role, self._now()),
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict("用户已经存在") from exc
        return {"user_id": user_id.strip(), "role": role}

    # ------------------------------------------------------------------
    # 拓扑与校准版本
    # ------------------------------------------------------------------

    def _point(self, point_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM metering_points WHERE point_id=?", (point_id,)
        ).fetchone()
        if row is None:
            raise NotFound("计量点不存在")
        return row

    def create_point(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "topology.write")
        point = MeteringPointInput.from_dict(raw)
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO metering_points(point_id,name,stage,facility_id,well_group_id,"
                    "created_by,created_at) VALUES(?,?,?,?,?,?,?)",
                    (
                        point.point_id,
                        point.name,
                        point.stage,
                        point.facility_id,
                        point.well_group_id,
                        actor_id,
                        self._now(),
                    ),
                )
                self._audit("metering_point", point.point_id, "point.created", actor_id, raw)
        except sqlite3.IntegrityError as exc:
            raise Conflict("计量点编号已经存在") from exc
        return dict(raw)

    def create_link(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "topology.write")
        link = ChainLinkInput.from_dict(raw)
        upstream = self._point(link.from_point_id)
        downstream = self._point(link.to_point_id)
        if STAGE_ORDER.index(upstream["stage"]) >= STAGE_ORDER.index(downstream["stage"]):
            raise ValidationFailed("链路方向与计量环节顺序不符")
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO chain_links(link_id,from_point_id,to_point_id,loss_basis_points,"
                    "created_by,created_at) VALUES(?,?,?,?,?,?)",
                    (
                        link.link_id,
                        link.from_point_id,
                        link.to_point_id,
                        link.loss_basis_points,
                        actor_id,
                        self._now(),
                    ),
                )
                self._audit("chain_link", link.link_id, "link.created", actor_id, raw)
        except sqlite3.IntegrityError as exc:
            raise Conflict("计量链链路冲突：起点已存在出站链路或链路编号重复") from exc
        return dict(raw)

    def _active_calibration(self, point_id: str) -> sqlite3.Row | None:
        return self.connection.execute(
            "SELECT * FROM calibration_versions WHERE point_id=? AND state='active'",
            (point_id,),
        ).fetchone()

    def create_calibration(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "calibration.write")
        calibration = CalibrationInput.from_dict(raw)
        self._point(calibration.point_id)
        try:
            with transaction(self.connection, immediate=True):
                cursor = self.connection.execute(
                    "INSERT INTO calibration_versions(point_id,version_no,coefficient,note,state,"
                    "created_by,created_at) VALUES(?,?,?,?,'draft',?,?)",
                    (
                        calibration.point_id,
                        calibration.version_no,
                        decimal_text(calibration.coefficient),
                        calibration.note,
                        actor_id,
                        self._now(),
                    ),
                )
                version_id = int(cursor.lastrowid)
                self._audit(
                    "calibration_version",
                    str(version_id),
                    "calibration.created",
                    actor_id,
                    {"point_id": calibration.point_id, "version_no": calibration.version_no},
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict("该校准版本号已存在") from exc
        return {
            "version_id": version_id,
            "point_id": calibration.point_id,
            "version_no": calibration.version_no,
            "state": "draft",
        }

    def activate_calibration(self, actor_id: str, version_id: int) -> dict[str, Any]:
        self._require(actor_id, "calibration.write")
        row = self.connection.execute(
            "SELECT * FROM calibration_versions WHERE version_id=?", (version_id,)
        ).fetchone()
        if row is None:
            raise NotFound("校准版本不存在")
        with transaction(self.connection, immediate=True):
            self.connection.execute(
                "UPDATE calibration_versions SET state='retired' WHERE point_id=? AND state='active'",
                (row["point_id"],),
            )
            cursor = self.connection.execute(
                "UPDATE calibration_versions SET state='active',activated_at=? "
                "WHERE version_id=? AND state='draft'",
                (self._now(), version_id),
            )
            if cursor.rowcount != 1:
                raise InvalidState("校准版本不是可激活草稿")
            self._audit(
                "calibration_version",
                str(version_id),
                "calibration.activated",
                actor_id,
                {"point_id": row["point_id"], "version_no": row["version_no"]},
            )
        return {"version_id": version_id, "point_id": row["point_id"], "state": "active"}

    # ------------------------------------------------------------------
    # 批次与读数
    # ------------------------------------------------------------------

    def _batch(self, batch_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM crude_batches WHERE batch_id=?", (batch_id,)
        ).fetchone()
        if row is None:
            raise NotFound("原油批次不存在")
        return row

    def register_batch(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "batch.write")
        batch = BatchInput.from_dict(raw)
        point = self._point(batch.wellhead_point_id)
        if point["stage"] != "wellhead":
            raise ValidationFailed("批次必须注册在井口计量点")
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO crude_batches(batch_id,wellhead_point_id,well_group_id,created_by,"
                    "created_at) VALUES(?,?,?,?,?)",
                    (
                        batch.batch_id,
                        batch.wellhead_point_id,
                        point["well_group_id"],
                        actor_id,
                        self._now(),
                    ),
                )
                self._audit(
                    "crude_batch",
                    batch.batch_id,
                    "batch.registered",
                    actor_id,
                    {"well_group_id": point["well_group_id"]},
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict("原油批次编号已经存在") from exc
        return {
            "batch_id": batch.batch_id,
            "well_group_id": point["well_group_id"],
            "state": "open",
        }

    def upload_reading(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "reading.write")
        reading = ReadingInput.from_dict(raw)
        request_digest = digest(raw)
        stored = self.connection.execute(
            "SELECT request_sha256,response_json FROM meter_idempotency "
            "WHERE scope='reading' AND idempotency_key=?",
            (reading.idempotency_key,),
        ).fetchone()
        if stored is not None:
            if stored["request_sha256"] != request_digest:
                raise Conflict("幂等键对应不同读数内容")
            return json.loads(stored["response_json"])
        self._point(reading.point_id)
        batch = self._batch(reading.batch_id)
        if batch["state"] == "frozen":
            raise InvalidState("批次处于争议冻结状态，不能上传读数")
        if parse_utc(reading.observed_at, "observed_at") > self.clock.now():
            raise ValidationFailed("observed_at 不能晚于当前时间")
        calibration = self._active_calibration(reading.point_id)
        if calibration is None:
            raise InvalidState("计量点缺少生效校准版本")
        coefficient = Decimal(calibration["coefficient"])
        corrected = corrected_quantity(reading.raw_quantity, coefficient)
        settled_max = self.connection.execute(
            "SELECT MAX(observed_at) AS max_observed FROM meter_readings "
            "WHERE point_id=? AND batch_id=? AND settlement_id IS NOT NULL",
            (reading.point_id, reading.batch_id),
        ).fetchone()["max_observed"]
        late = settled_max is not None and reading.observed_at <= settled_max
        try:
            with transaction(self.connection, immediate=True):
                cursor = self.connection.execute(
                    "INSERT INTO meter_readings(point_id,batch_id,observed_at,raw_quantity,"
                    "calibration_version_id,coefficient,corrected_quantity,idempotency_key,"
                    "uploaded_by,uploaded_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (
                        reading.point_id,
                        reading.batch_id,
                        reading.observed_at,
                        decimal_text(reading.raw_quantity),
                        calibration["version_id"],
                        calibration["coefficient"],
                        decimal_text(corrected),
                        reading.idempotency_key,
                        actor_id,
                        self._now(),
                    ),
                )
                reading_id = int(cursor.lastrowid)
                response = {
                    "reading_id": reading_id,
                    "point_id": reading.point_id,
                    "batch_id": reading.batch_id,
                    "corrected_quantity": decimal_text(corrected),
                    "calibration_version_id": calibration["version_id"],
                    "late": late,
                    "settlement": "pending-successor" if late else "unsettled",
                }
                self.connection.execute(
                    "INSERT INTO meter_idempotency(scope,idempotency_key,request_sha256,response_json,"
                    "created_at) VALUES('reading',?,?,?,?)",
                    (reading.idempotency_key, request_digest, canonical_json(response), self._now()),
                )
                self._audit(
                    "reading",
                    str(reading_id),
                    "reading.uploaded",
                    actor_id,
                    {
                        "point_id": reading.point_id,
                        "batch_id": reading.batch_id,
                        "corrected_quantity": decimal_text(corrected),
                        "late": late,
                    },
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict("读数幂等键冲突") from exc
        return response

    # ------------------------------------------------------------------
    # 结算：迟到读数只能形成后继结算
    # ------------------------------------------------------------------

    def _effective_quantity(self, point_id: str, batch_id: str) -> Decimal:
        settled = self.connection.execute(
            "SELECT quantity_units FROM settlements WHERE point_id=? AND batch_id=? AND state='closed'",
            (point_id, batch_id),
        ).fetchone()
        total = ZERO if settled is None else Decimal(settled["quantity_units"])
        rows = self.connection.execute(
            "SELECT corrected_quantity FROM meter_readings "
            "WHERE point_id=? AND batch_id=? AND settlement_id IS NULL",
            (point_id, batch_id),
        ).fetchall()
        return total + sum((Decimal(row["corrected_quantity"]) for row in rows), ZERO)

    def close_settlement(self, actor_id: str, point_id: str, batch_id: str) -> dict[str, Any]:
        self._require(actor_id, "settlement.close")
        self._point(point_id)
        batch = self._batch(batch_id)
        if batch["state"] == "frozen":
            raise InvalidState("批次处于争议冻结状态，不能结算")
        previous = self.connection.execute(
            "SELECT * FROM settlements WHERE point_id=? AND batch_id=? AND state='closed'",
            (point_id, batch_id),
        ).fetchone()
        readings = self.connection.execute(
            "SELECT * FROM meter_readings WHERE point_id=? AND batch_id=? AND settlement_id IS NULL "
            "ORDER BY reading_id",
            (point_id, batch_id),
        ).fetchall()
        if not readings:
            raise InvalidState("没有待结算读数")
        increment = sum((Decimal(row["corrected_quantity"]) for row in readings), ZERO)
        base = ZERO if previous is None else Decimal(previous["quantity_units"])
        quantity = quantize_volume(base + increment)
        revision = 1 if previous is None else int(previous["revision"]) + 1
        with transaction(self.connection, immediate=True):
            if previous is not None:
                cursor = self.connection.execute(
                    "UPDATE settlements SET state='superseded' WHERE settlement_id=? AND state='closed'",
                    (previous["settlement_id"],),
                )
                if cursor.rowcount != 1:
                    raise InvalidState("结算版本已被并发更新")
            cursor = self.connection.execute(
                "INSERT INTO settlements(point_id,batch_id,revision,quantity_units,delta_units,"
                "reading_count,state,supersedes_settlement_id,closed_by,closed_at) "
                "VALUES(?,?,?,?,?,?,'closed',?,?,?)",
                (
                    point_id,
                    batch_id,
                    revision,
                    decimal_text(quantity),
                    decimal_text(quantize_volume(increment)),
                    len(readings),
                    None if previous is None else previous["settlement_id"],
                    actor_id,
                    self._now(),
                ),
            )
            settlement_id = int(cursor.lastrowid)
            for row in readings:
                self.connection.execute(
                    "UPDATE meter_readings SET settlement_id=? WHERE reading_id=?",
                    (settlement_id, row["reading_id"]),
                )
            self._audit(
                "settlement",
                str(settlement_id),
                "settlement.closed",
                actor_id,
                {
                    "point_id": point_id,
                    "batch_id": batch_id,
                    "revision": revision,
                    "quantity_units": decimal_text(quantity),
                    "delta_units": decimal_text(quantize_volume(increment)),
                },
            )
        return {
            "settlement_id": settlement_id,
            "point_id": point_id,
            "batch_id": batch_id,
            "revision": revision,
            "state": "closed",
            "quantity_units": decimal_text(quantity),
            "delta_units": decimal_text(quantize_volume(increment)),
            "supersedes_settlement_id": None if previous is None else previous["settlement_id"],
        }

    # ------------------------------------------------------------------
    # 提油船次与交接
    # ------------------------------------------------------------------

    def _voyage(self, voyage_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM liftings WHERE voyage_id=?", (voyage_id,)
        ).fetchone()
        if row is None:
            raise NotFound("提油船次不存在")
        return row

    def create_voyage(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "handover.write")
        voyage = VoyageInput.from_dict(raw)
        point = self._point(voyage.tank_point_id)
        if point["stage"] != "storage-tank":
            raise ValidationFailed("提油船次必须挂在储罐计量点")
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO liftings(voyage_id,tank_point_id,vessel_name,created_by,created_at) "
                    "VALUES(?,?,?,?,?)",
                    (voyage.voyage_id, voyage.tank_point_id, voyage.vessel_name, actor_id, self._now()),
                )
                self._audit("lifting", voyage.voyage_id, "voyage.created", actor_id, raw)
        except sqlite3.IntegrityError as exc:
            raise Conflict("提油船次编号已经存在") from exc
        return dict(raw)

    def _handover(self, handover_id: int) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM handovers WHERE handover_id=?", (handover_id,)
        ).fetchone()
        if row is None:
            raise NotFound("交接单不存在")
        return row

    def create_handover(self, actor_id: str, voyage_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "handover.write")
        self._voyage(voyage_id)
        handover = HandoverInput.from_dict(raw)
        draft = self.connection.execute(
            "SELECT 1 FROM handovers WHERE voyage_id=? AND state='draft'", (voyage_id,)
        ).fetchone()
        if draft is not None:
            raise InvalidState("船次存在未签署交接草稿")
        latest = self.connection.execute(
            "SELECT * FROM handovers WHERE voyage_id=? ORDER BY revision DESC LIMIT 1",
            (voyage_id,),
        ).fetchone()
        revision = 1 if latest is None else int(latest["revision"]) + 1
        supersedes = None if latest is None or latest["state"] != "signed" else latest["handover_id"]
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "INSERT INTO handovers(voyage_id,revision,quantity_units,state,supersedes_handover_id,"
                "created_by,created_at) VALUES(?,?,?,'draft',?,?,?)",
                (
                    voyage_id,
                    revision,
                    decimal_text(handover.quantity),
                    supersedes,
                    actor_id,
                    self._now(),
                ),
            )
            handover_id = int(cursor.lastrowid)
            self._audit(
                "handover",
                str(handover_id),
                "handover.created",
                actor_id,
                {"voyage_id": voyage_id, "revision": revision, "supersedes": supersedes},
            )
        return {
            "handover_id": handover_id,
            "voyage_id": voyage_id,
            "revision": revision,
            "state": "draft",
            "quantity_units": decimal_text(handover.quantity),
            "supersedes_handover_id": supersedes,
        }

    def _attributed_quantity(self, tank_point_id: str, batch_id: str) -> Decimal:
        rows = self.connection.execute(
            "SELECT a.quantity_units FROM lifting_attributions a "
            "JOIN handovers h ON h.handover_id=a.handover_id "
            "JOIN liftings l ON l.voyage_id=h.voyage_id "
            "WHERE l.tank_point_id=? AND a.batch_id=? AND a.state='active'",
            (tank_point_id, batch_id),
        ).fetchall()
        return sum((Decimal(row["quantity_units"]) for row in rows), ZERO)

    def _tank_availability(self, tank_point_id: str) -> dict[str, Decimal]:
        rows = self.connection.execute(
            "SELECT DISTINCT batch_id FROM meter_readings WHERE point_id=?", (tank_point_id,)
        ).fetchall()
        availability: dict[str, Decimal] = {}
        for row in rows:
            batch = self._batch(row["batch_id"])
            if batch["state"] == "frozen":
                continue
            effective = self._effective_quantity(tank_point_id, row["batch_id"])
            available = quantize_volume(effective - self._attributed_quantity(tank_point_id, row["batch_id"]))
            if available > ZERO:
                availability[row["batch_id"]] = available
        return availability

    def sign_handover(self, actor_id: str, handover_id: int) -> dict[str, Any]:
        self._require(actor_id, "handover.write")
        handover = self._handover(handover_id)
        if handover["state"] != "draft":
            raise InvalidState("交接单不是可签署草稿")
        voyage = self._voyage(handover["voyage_id"])
        tank_point_id = voyage["tank_point_id"]
        link = self.connection.execute(
            "SELECT * FROM chain_links WHERE from_point_id=?", (tank_point_id,)
        ).fetchone()
        if link is None:
            raise InvalidState("储罐未连接提油计量点")
        offtake = self._point(link["to_point_id"])
        if offtake["stage"] != "offtake":
            raise InvalidState("储罐下游不是提油计量点")
        calibration = self._active_calibration(offtake["point_id"])
        if calibration is None:
            raise InvalidState("提油计量点缺少生效校准版本")
        quantity = Decimal(handover["quantity_units"])
        with transaction(self.connection, immediate=True):
            superseded = handover["supersedes_handover_id"]
            if superseded is not None:
                cursor = self.connection.execute(
                    "UPDATE handovers SET state='superseded' WHERE handover_id=? AND state='signed'",
                    (superseded,),
                )
                if cursor.rowcount != 1:
                    raise InvalidState("被替代的交接单不是已签署状态")
                self.connection.execute(
                    "UPDATE lifting_attributions SET state='reversed' WHERE handover_id=? AND state='active'",
                    (superseded,),
                )
            availability = self._tank_availability(tank_point_id)
            if quantity > sum(availability.values(), ZERO):
                raise Conflict("罐内可交接库存不足")
            shares = allocate_proportional(quantity, availability)
            cursor = self.connection.execute(
                "UPDATE handovers SET state='signed',signed_by=?,signed_at=?,calibration_version_id=? "
                "WHERE handover_id=? AND state='draft'",
                (actor_id, self._now(), calibration["version_id"], handover_id),
            )
            if cursor.rowcount != 1:
                raise InvalidState("交接单不是可签署草稿")
            attributions = []
            for batch_id, share in shares.items():
                if share <= ZERO:
                    continue
                batch = self._batch(batch_id)
                self.connection.execute(
                    "INSERT INTO lifting_attributions(handover_id,batch_id,well_group_id,quantity_units,"
                    "state) VALUES(?,?,?,?,'active')",
                    (handover_id, batch_id, batch["well_group_id"], decimal_text(share)),
                )
                attributions.append(
                    {
                        "batch_id": batch_id,
                        "well_group_id": batch["well_group_id"],
                        "quantity_units": decimal_text(share),
                    }
                )
            self._audit(
                "handover",
                str(handover_id),
                "handover.signed",
                actor_id,
                {
                    "voyage_id": handover["voyage_id"],
                    "quantity_units": decimal_text(quantity),
                    "calibration_version_id": calibration["version_id"],
                    "superseded_handover_id": superseded,
                },
            )
        return {
            "handover_id": handover_id,
            "voyage_id": handover["voyage_id"],
            "revision": handover["revision"],
            "state": "signed",
            "quantity_units": decimal_text(quantity),
            "calibration_version_id": calibration["version_id"],
            "superseded_handover_id": superseded,
            "attributions": attributions,
        }

    # ------------------------------------------------------------------
    # 计量争议：只冻结相关批次
    # ------------------------------------------------------------------

    def open_dispute(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "dispute.write")
        dispute = DisputeInput.from_dict(raw)
        self._batch(dispute.batch_id)
        if dispute.point_id is not None:
            self._point(dispute.point_id)
        existing = self.connection.execute(
            "SELECT 1 FROM metering_disputes WHERE batch_id=? AND state='open'",
            (dispute.batch_id,),
        ).fetchone()
        if existing is not None:
            raise Conflict("批次已存在未解决争议")
        try:
            with transaction(self.connection, immediate=True):
                cursor = self.connection.execute(
                    "UPDATE crude_batches SET state='frozen',revision=revision+1 "
                    "WHERE batch_id=? AND state='open'",
                    (dispute.batch_id,),
                )
                if cursor.rowcount != 1:
                    raise InvalidState("批次不在可冻结状态")
                self.connection.execute(
                    "INSERT INTO metering_disputes(dispute_id,batch_id,point_id,reason,state,opened_by,"
                    "opened_at) VALUES(?,?,?,?,'open',?,?)",
                    (
                        dispute.dispute_id,
                        dispute.batch_id,
                        dispute.point_id,
                        dispute.reason,
                        actor_id,
                        self._now(),
                    ),
                )
                self._audit(
                    "dispute",
                    dispute.dispute_id,
                    "dispute.opened",
                    actor_id,
                    {"batch_id": dispute.batch_id, "point_id": dispute.point_id},
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict("争议编号已经存在") from exc
        return {"dispute_id": dispute.dispute_id, "batch_id": dispute.batch_id, "state": "open"}

    def resolve_dispute(self, actor_id: str, dispute_id: str, note: str) -> dict[str, Any]:
        self._require(actor_id, "dispute.write")
        if not isinstance(note, str) or not note.strip():
            raise ValidationFailed("resolution note 不能为空")
        row = self.connection.execute(
            "SELECT * FROM metering_disputes WHERE dispute_id=?", (dispute_id,)
        ).fetchone()
        if row is None:
            raise NotFound("计量争议不存在")
        if row["state"] != "open":
            raise InvalidState("争议不在待解决状态")
        with transaction(self.connection, immediate=True):
            self.connection.execute(
                "UPDATE metering_disputes SET state='resolved',resolved_by=?,resolved_at=?,"
                "resolution_note=? WHERE dispute_id=? AND state='open'",
                (actor_id, self._now(), note.strip(), dispute_id),
            )
            self.connection.execute(
                "UPDATE crude_batches SET state='open',revision=revision+1 "
                "WHERE batch_id=? AND state='frozen'",
                (row["batch_id"],),
            )
            self._audit(
                "dispute",
                dispute_id,
                "dispute.resolved",
                actor_id,
                {"batch_id": row["batch_id"], "note": note.strip()},
            )
        return {"dispute_id": dispute_id, "batch_id": row["batch_id"], "state": "resolved"}

    # ------------------------------------------------------------------
    # 反查与平衡
    # ------------------------------------------------------------------

    def _points_map(self) -> dict[str, sqlite3.Row]:
        rows = self.connection.execute("SELECT * FROM metering_points").fetchall()
        return {row["point_id"]: row for row in rows}

    def _chain_path(self, start_point_id: str) -> list[str]:
        links = self.connection.execute("SELECT from_point_id,to_point_id FROM chain_links").fetchall()
        outgoing = {row["from_point_id"]: row["to_point_id"] for row in links}
        path = [start_point_id]
        visited = {start_point_id}
        current = start_point_id
        while current in outgoing:
            following = outgoing[current]
            if following in visited:
                break
            path.append(following)
            visited.add(following)
            current = following
        return path

    def _segment_rows(
        self, batch_id: str, path: list[str]
    ) -> tuple[list[dict[str, Any]], dict[str, Decimal]]:
        links = {
            (row["from_point_id"], row["to_point_id"]): row
            for row in self.connection.execute("SELECT * FROM chain_links").fetchall()
        }
        quantities = {point_id: self._effective_quantity(point_id, batch_id) for point_id in path}
        rows: list[dict[str, Any]] = []
        for upstream, downstream in zip(path, path[1:]):
            if quantities[upstream] > ZERO and quantities[downstream] > ZERO:
                link = links[(upstream, downstream)]
                rows.append(
                    segment_variance(
                        link_id=link["link_id"],
                        from_point_id=upstream,
                        to_point_id=downstream,
                        loss_basis_points=int(link["loss_basis_points"]),
                        input_quantity=quantities[upstream],
                        output_quantity=quantities[downstream],
                    )
                )
        return rows, quantities

    def _exported_quantity(self, batch_id: str) -> Decimal:
        rows = self.connection.execute(
            "SELECT quantity_units FROM lifting_attributions WHERE batch_id=? AND state='active'",
            (batch_id,),
        ).fetchall()
        return sum((Decimal(row["quantity_units"]) for row in rows), ZERO)

    def _batch_balance_row(self, batch: sqlite3.Row, tolerance_basis_points: int) -> dict[str, Any]:
        batch_id = batch["batch_id"]
        path = self._chain_path(batch["wellhead_point_id"])
        points = self._points_map()
        segments, quantities = self._segment_rows(batch_id, path)
        produced = quantities.get(batch["wellhead_point_id"], ZERO)
        furthest_quantity = ZERO
        reached_tank = False
        for point_id in path:
            if quantities.get(point_id, ZERO) > ZERO:
                furthest_quantity = quantities[point_id]
                reached_tank = points[point_id]["stage"] == "storage-tank"
        summary = summarize_balance(
            produced=produced,
            furthest_quantity=furthest_quantity,
            reached_tank=reached_tank,
            exported=self._exported_quantity(batch_id),
            segments=segments,
            tolerance_basis_points=tolerance_basis_points,
        )
        return {
            "batch_id": batch_id,
            "well_group_id": batch["well_group_id"],
            "state": batch["state"],
            "segments": segments,
            **summary,
        }

    def trace_voyage(self, actor_id: str, voyage_id: str) -> dict[str, Any]:
        self._require(actor_id, "trace.read")
        voyage = self._voyage(voyage_id)
        handover = self.connection.execute(
            "SELECT * FROM handovers WHERE voyage_id=? AND state='signed' "
            "ORDER BY revision DESC LIMIT 1",
            (voyage_id,),
        ).fetchone()
        result: dict[str, Any] = {
            "voyage_id": voyage_id,
            "vessel_name": voyage["vessel_name"],
            "tank_point_id": voyage["tank_point_id"],
            "handover": None,
            "sources": [],
        }
        if handover is None:
            return result
        attributions = self.connection.execute(
            "SELECT * FROM lifting_attributions WHERE handover_id=? AND state='active' "
            "ORDER BY batch_id",
            (handover["handover_id"],),
        ).fetchall()
        sources = []
        attributed_total = ZERO
        for attribution in attributions:
            batch = self._batch(attribution["batch_id"])
            balance = self._batch_balance_row(batch, DEFAULT_TOLERANCE_BASIS_POINTS)
            quantity = Decimal(attribution["quantity_units"])
            attributed_total += quantity
            sources.append(
                {
                    "batch_id": batch["batch_id"],
                    "well_group_id": batch["well_group_id"],
                    "attributed_units": decimal_text(quantity),
                    "produced_units": balance["produced_units"],
                    "allowable_loss_units": balance["allowable_loss_units"],
                    "unaccounted_units": balance["unaccounted_units"],
                    "segments": balance["segments"],
                }
            )
        result["handover"] = {
            "handover_id": handover["handover_id"],
            "revision": handover["revision"],
            "state": handover["state"],
            "quantity_units": handover["quantity_units"],
            "calibration_version_id": handover["calibration_version_id"],
            "signed_by": handover["signed_by"],
            "signed_at": handover["signed_at"],
        }
        result["sources"] = sources
        result["attributed_total_units"] = decimal_text(quantize_volume(attributed_total))
        result["difference_units"] = decimal_text(
            quantize_volume(Decimal(handover["quantity_units"]) - attributed_total)
        )
        return result

    def balance_report(
        self,
        actor_id: str,
        well_group_id: str | None = None,
        tolerance_basis_points: int = DEFAULT_TOLERANCE_BASIS_POINTS,
    ) -> dict[str, Any]:
        self._require(actor_id, "balance.read")
        if (
            isinstance(tolerance_basis_points, bool)
            or not isinstance(tolerance_basis_points, int)
            or not 0 <= tolerance_basis_points <= 1000
        ):
            raise ValidationFailed("tolerance_basis_points 必须是 0 到 1000 的整数")
        if well_group_id is None:
            batches = self.connection.execute(
                "SELECT * FROM crude_batches ORDER BY batch_id"
            ).fetchall()
        else:
            batches = self.connection.execute(
                "SELECT * FROM crude_batches WHERE well_group_id=? ORDER BY batch_id",
                (well_group_id,),
            ).fetchall()
        rows = [self._batch_balance_row(batch, tolerance_basis_points) for batch in batches]
        totals: dict[str, str] = {}
        for key in (
            "produced_units",
            "exported_units",
            "inventory_units",
            "in_transit_units",
            "allowable_loss_units",
            "actual_loss_units",
            "unaccounted_units",
        ):
            totals[key] = decimal_text(
                quantize_volume(sum((Decimal(row[key]) for row in rows), ZERO))
            )
        return {
            "tolerance_basis_points": tolerance_basis_points,
            "balanced": all(row["balanced"] for row in rows),
            "totals": totals,
            "batches": rows,
        }

    def audit_chain(self, actor_id: str) -> dict[str, Any]:
        self._require(actor_id, "audit.read")
        rows = self.connection.execute("SELECT * FROM meter_audit_events ORDER BY event_id").fetchall()
        previous_hash = "0" * 64
        valid = True
        for row in rows:
            body = {
                "entity_type": row["entity_type"],
                "entity_id": row["entity_id"],
                "event_type": row["event_type"],
                "actor_id": row["actor_id"],
                "payload": json.loads(row["payload_json"]),
                "created_at": row["created_at"],
                "previous_hash": row["previous_hash"],
            }
            calculated = hashlib.sha256(canonical_json(body).encode("utf-8")).hexdigest()
            if row["previous_hash"] != previous_hash or row["event_hash"] != calculated:
                valid = False
                break
            previous_hash = row["event_hash"]
        return {"valid": valid, "events": len(rows), "head_hash": previous_hash}
