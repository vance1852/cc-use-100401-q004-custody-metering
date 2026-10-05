"""端到端计量监管链事务用例。

设计要点：
- 校准版本只增不改，批次落库时快照所用修正系数，任何新系数都不会回写历史净量；
- 提油交接量在签署时快照，之后只能通过争议与补充处理，不能被静默改写；
- 迟到读数不修改原批次，而是生成同段后继结算批次，账面"当前认定值"沿后继链解析；
- 所有写接口都走幂等表，重复上传返回同一结果；
- 计量争议按 single/lineage 作用域只冻结相关批次，不波及无关环节。
"""

from __future__ import annotations

import json
import sqlite3
from decimal import Decimal
from typing import Any, Callable, Mapping

from .custody import (
    LIFTING_TRANSFER,
    NODE_BY_SEGMENT,
    UPSTREAM_SEGMENTS,
    WELL_PRODUCTION,
    attribute_sources,
    lineage_batch_ids,
    mass_balance,
    ordered_lineage,
    segment_variance,
)
from .custody_models import (
    BatchDraft,
    CalibrationDraft,
    DisputeDraft,
    HandoverDraft,
    LateReadingDraft,
    LinkDraft,
)
from .errors import Conflict, InvalidState, NotFound, ValidationFailed
from .planning import canonical_json, decimal_text, digest, quantize_volume
from .storage import transaction


class CustodyService:
    def __init__(self, supply_service: "SupplyService") -> None:  # noqa: F821
        self.supply = supply_service
        self.connection = supply_service.connection

    def _now(self) -> str:
        return self.supply._now()

    def _audit(
        self,
        entity_type: str,
        entity_id: str,
        event_type: str,
        actor_id: str,
        payload: Mapping[str, Any],
    ) -> None:
        self.supply._audit(entity_type, entity_id, event_type, actor_id, payload)

    def _require(self, actor_id: str, permission: str) -> None:
        self.supply._require(actor_id, permission)

    # -- 幂等 ----------------------------------------------------------------

    def _idempotent(
        self,
        scope: str,
        key: str,
        raw: Mapping[str, Any],
        builder: Callable[[], dict[str, Any]],
    ) -> dict[str, Any]:
        request_digest = digest(raw)
        stored = self.connection.execute(
            "SELECT request_sha256,response_json FROM custody_idempotency WHERE scope=? AND idempotency_key=?",
            (scope, key),
        ).fetchone()
        if stored is not None:
            if stored["request_sha256"] != request_digest:
                raise Conflict("幂等键对应不同的计量请求内容")
            return json.loads(stored["response_json"])
        with transaction(self.connection, immediate=True):
            response = builder()
            self.connection.execute(
                "INSERT INTO custody_idempotency(scope,idempotency_key,request_sha256,response_json,created_at) "
                "VALUES(?,?,?,?,?)",
                (scope, key, request_digest, canonical_json(response), self._now()),
            )
        return response

    # -- 校准版本 ------------------------------------------------------------

    def register_calibration(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "calibration.write")
        draft = CalibrationDraft.from_dict(raw)
        existing = self.connection.execute(
            "SELECT correction_factor,meter_kind,state FROM calibration_versions WHERE calibration_id=?",
            (draft.calibration_id,),
        ).fetchone()
        if existing is not None:
            # 校准版本只增不改：完全相同的重复上传幂等返回，任何差异都拒绝。
            if (
                existing["meter_kind"] == draft.meter_kind
                and Decimal(existing["correction_factor"]) == draft.factor
            ):
                return {
                    "calibration_version_id": draft.calibration_id,
                    "meter_kind": draft.meter_kind,
                    "correction_factor": decimal_text(draft.factor),
                    "state": existing["state"],
                    "revision": 1,
                }
            raise Conflict("校准版本已存在且系数或计量点类型不同，修正请改用新版本编号")
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO calibration_versions(calibration_id,meter_kind,correction_factor,basis,"
                    "effective_from,created_by,created_at) VALUES(?,?,?,?,?,?,?)",
                    (
                        draft.calibration_id,
                        draft.meter_kind,
                        decimal_text(draft.factor),
                        draft.basis,
                        draft.effective_from,
                        actor_id,
                        self._now(),
                    ),
                )
                self._audit(
                    "calibration",
                    draft.calibration_id,
                    "calibration.registered",
                    actor_id,
                    {"meter_kind": draft.meter_kind, "correction_factor": decimal_text(draft.factor)},
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict("校准版本编号已经存在") from exc
        return {
            "calibration_version_id": draft.calibration_id,
            "meter_kind": draft.meter_kind,
            "correction_factor": decimal_text(draft.factor),
            "state": "active",
            "revision": 1,
        }

    def _calibration(self, calibration_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM calibration_versions WHERE calibration_id=?", (calibration_id,)
        ).fetchone()
        if row is None:
            raise NotFound(f"校准版本 {calibration_id} 不存在")
        return row

    # -- 读数与净量 ----------------------------------------------------------

    def _evaluate_readings(self, segment: str, readings) -> tuple[Decimal, Decimal, list[dict[str, Any]], str]:
        expected_kind = NODE_BY_SEGMENT[segment]
        gross_total = Decimal("0")
        net_total = Decimal("0")
        evaluated: list[dict[str, Any]] = []
        seen_meters: set[str] = set()
        for reading in readings:
            if reading.meter_id in seen_meters:
                raise ValidationFailed(f"同一计量批次内仪表 {reading.meter_id} 读数重复")
            seen_meters.add(reading.meter_id)
            calibration = self._calibration(reading.calibration_version_id)
            if calibration["meter_kind"] != expected_kind:
                raise ValidationFailed(
                    f"{segment} 段只能使用 {expected_kind} 类校准，收到 {calibration['meter_kind']}"
                )
            factor = Decimal(calibration["correction_factor"])
            net = quantize_volume(reading.gross_reading * factor)
            gross_total += reading.gross_reading
            net_total += net
            evaluated.append(
                {
                    "meter_id": reading.meter_id,
                    "calibration_version_id": reading.calibration_version_id,
                    "meter_kind": expected_kind,
                    "factor_snapshot": decimal_text(factor),
                    "gross_reading": decimal_text(reading.gross_reading),
                    "net_reading": decimal_text(net),
                    "observed_at": reading.observed_at,
                }
            )
        factor_set_sha256 = digest(
            [
                {key: item[key] for key in ("meter_id", "calibration_version_id", "factor_snapshot", "gross_reading")}
                for item in sorted(evaluated, key=lambda item: item["meter_id"])
            ]
        )
        return quantize_volume(gross_total), quantize_volume(net_total), evaluated, factor_set_sha256

    def record_batch(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "measurement.write")
        draft = BatchDraft.from_dict(raw)

        def build() -> dict[str, Any]:
            gross_total, net_total, evaluated, factor_set_sha256 = self._evaluate_readings(
                draft.segment, draft.readings
            )
            try:
                cursor = self.connection.execute(
                    "INSERT INTO custody_batches(batch_id,segment,product,well_group,gross_quota_units,"
                    "quantity_quota_units,factor_set_sha256,allowed_loss_basis_points,observed_at,"
                    "idempotency_key,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        draft.batch_id,
                        draft.segment,
                        draft.product,
                        draft.well_group,
                        decimal_text(gross_total),
                        decimal_text(net_total),
                        factor_set_sha256,
                        draft.allowed_loss_basis_points,
                        draft.observed_at,
                        draft.idempotency_key,
                        actor_id,
                        self._now(),
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise Conflict("计量批次编号或幂等键冲突") from exc
            for item in evaluated:
                self.connection.execute(
                    "INSERT INTO meter_readings(batch_id,meter_id,calibration_id,meter_kind,factor_snapshot,"
                    "gross_reading,net_reading,reading_role,observed_at,recorded_by,recorded_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        draft.batch_id,
                        item["meter_id"],
                        item["calibration_version_id"],
                        item["meter_kind"],
                        item["factor_snapshot"],
                        item["gross_reading"],
                        item["net_reading"],
                        "primary",
                        item["observed_at"],
                        actor_id,
                        self._now(),
                    ),
                )
            self._audit(
                "custody_batch",
                draft.batch_id,
                "custody.batch.recorded",
                actor_id,
                {"segment": draft.segment, "net_quota_units": decimal_text(net_total)},
            )
            return {
                "batch_id": draft.batch_id,
                "segment": draft.segment,
                "gross_quota_units": decimal_text(gross_total),
                "quantity_quota_units": decimal_text(net_total),
                "factor_set_sha256": factor_set_sha256,
                "readings": len(evaluated),
                "state": "recorded",
                "revision": 1,
            }

        return self._idempotent("custody_batch", draft.idempotency_key, raw, build)

    def _batch(self, batch_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM custody_batches WHERE batch_id=?", (batch_id,)
        ).fetchone()
        if row is None:
            raise NotFound(f"计量批次 {batch_id} 不存在")
        return row

    def _latest_quantity(self, batch_id: str, original_net: Decimal) -> Decimal:
        """沿后继结算链走到最新后继，取其净量；无结算时即原净量。"""
        current = batch_id
        net = original_net
        seen = {batch_id}
        while True:
            row = self.connection.execute(
                "SELECT successor_batch_id, quantity_quota_units FROM late_settlements s "
                "JOIN custody_batches b ON b.batch_id=s.successor_batch_id "
                "WHERE s.original_batch_id=? ORDER BY s.settlement_id DESC LIMIT 1",
                (current,),
            ).fetchone()
            if row is None:
                return net
            if row["successor_batch_id"] in seen:
                raise InvalidState("后继结算链出现环路")
            seen.add(row["successor_batch_id"])
            current = row["successor_batch_id"]
            net = Decimal(row["quantity_quota_units"])

    def _require_unfrozen(self, batch: sqlite3.Row) -> None:
        if batch["state"] == "frozen":
            raise InvalidState(f"计量批次 {batch['batch_id']} 处于争议冻结状态")
        if batch["state"] == "void":
            raise InvalidState(f"计量批次 {batch['batch_id']} 已作废")

    # -- 监管链链接 ----------------------------------------------------------

    def link_batches(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "measurement.write")
        draft = LinkDraft.from_dict(raw)

        def build() -> dict[str, Any]:
            parent = self._batch(draft.parent_batch_id)
            child = self._batch(draft.child_batch_id)
            self._require_unfrozen(parent)
            self._require_unfrozen(child)
            if parent["state"] == "settlement" or child["state"] == "settlement":
                raise InvalidState("后继结算批次是只读书签，不能重新接入监管链")
            allowed_upstream = UPSTREAM_SEGMENTS[child["segment"]]
            if parent["segment"] not in allowed_upstream:
                raise InvalidState(
                    f"{child['segment']} 的来源必须是 {sorted(allowed_upstream)}，不能接 {parent['segment']}"
                )
            if child["segment"] == LIFTING_TRANSFER:
                signed = self.connection.execute(
                    "SELECT 1 FROM lifting_handovers WHERE lifting_batch_id=? AND state='signed' LIMIT 1",
                    (draft.child_batch_id,),
                ).fetchone()
                if signed is not None:
                    raise InvalidState("提油批次已签署交接，不能再改变其来源构成")
            used_rows = self.connection.execute(
                "SELECT consumed_quota_units FROM custody_links WHERE parent_batch_id=?",
                (draft.parent_batch_id,),
            ).fetchall()
            used = sum((Decimal(row["consumed_quota_units"]) for row in used_rows), Decimal("0"))
            effective_parent_net = self._latest_quantity(draft.parent_batch_id, Decimal(parent["quantity_quota_units"]))
            if used + draft.consumed_quota_units > effective_parent_net:
                raise Conflict("消耗量不能超过来源批次经后继结算后的最新认定净量")
            try:
                cursor = self.connection.execute(
                    "INSERT INTO custody_links(parent_batch_id,child_batch_id,consumed_quota_units,"
                    "idempotency_key,created_by,created_at) VALUES(?,?,?,?,?,?)",
                    (
                        draft.parent_batch_id,
                        draft.child_batch_id,
                        decimal_text(draft.consumed_quota_units),
                        draft.idempotency_key,
                        actor_id,
                        self._now(),
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise Conflict("监管链链接或幂等键冲突") from exc
            link_id = int(cursor.lastrowid)
            self._audit(
                "custody_link",
                str(link_id),
                "custody.link.created",
                actor_id,
                {
                    "parent_batch_id": draft.parent_batch_id,
                    "child_batch_id": draft.child_batch_id,
                    "consumed_quota_units": decimal_text(draft.consumed_quota_units),
                },
            )
            return {
                "link_id": link_id,
                "parent_batch_id": draft.parent_batch_id,
                "child_batch_id": draft.child_batch_id,
                "consumed_quota_units": decimal_text(draft.consumed_quota_units),
            }

        return self._idempotent("custody_link", draft.idempotency_key, raw, build)

    # -- 提油交接 ------------------------------------------------------------

    def sign_handover(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "handover.write")
        draft = HandoverDraft.from_dict(raw)

        def build() -> dict[str, Any]:
            batch = self._batch(draft.lifting_batch_id)
            self._require_unfrozen(batch)
            if batch["segment"] != LIFTING_TRANSFER:
                raise InvalidState("只能对提油交接段批次签署交接单")
            signed_quantity = Decimal(batch["quantity_quota_units"])  # 签署即快照，永不再算
            try:
                cursor = self.connection.execute(
                    "INSERT INTO lifting_handovers(lifting_batch_id,signed_quantity_quota_units,vessel_voyage,"
                    "receiver,terminal,bill_of_lading,signed_by,signed_at,idempotency_key,created_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (
                        draft.lifting_batch_id,
                        decimal_text(signed_quantity),
                        draft.vessel_voyage,
                        draft.receiver,
                        draft.terminal,
                        draft.bill_of_lading,
                        actor_id,
                        self._now(),
                        draft.idempotency_key,
                        self._now(),
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise Conflict("该提油批次已签署交接或幂等键冲突") from exc
            handover_id = int(cursor.lastrowid)
            self._audit(
                "lifting_handover",
                str(handover_id),
                "custody.handover.signed",
                actor_id,
                {
                    "lifting_batch_id": draft.lifting_batch_id,
                    "vessel_voyage": draft.vessel_voyage,
                    "signed_quantity_quota_units": decimal_text(signed_quantity),
                },
            )
            return {
                "handover_id": handover_id,
                "lifting_batch_id": draft.lifting_batch_id,
                "vessel_voyage": draft.vessel_voyage,
                "signed_quantity_quota_units": decimal_text(signed_quantity),
                "state": "signed",
            }

        return self._idempotent("custody_handover", draft.idempotency_key, raw, build)

    def _handover_for(self, *, lifting_batch_id: str | None = None, vessel_voyage: str | None = None) -> sqlite3.Row:
        if lifting_batch_id is not None:
            row = self.connection.execute(
                "SELECT * FROM lifting_handovers WHERE lifting_batch_id=? AND state='signed'",
                (lifting_batch_id,),
            ).fetchone()
        else:
            row = self.connection.execute(
                "SELECT * FROM lifting_handovers WHERE vessel_voyage=? AND state='signed' "
                "ORDER BY handover_id DESC LIMIT 1",
                (vessel_voyage,),
            ).fetchone()
        if row is None:
            raise NotFound("没有已签署的提油交接单")
        return row

    # -- 迟到读数 -> 后继结算 -------------------------------------------------

    def record_late_reading(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "settlement.write")
        draft = LateReadingDraft.from_dict(raw)

        def build() -> dict[str, Any]:
            original = self._batch(draft.batch_id)
            self._require_unfrozen(original)
            signed = self.connection.execute(
                "SELECT handover_id FROM lifting_handovers WHERE lifting_batch_id=? AND state='signed'",
                (draft.batch_id,),
            ).fetchone()
            if signed is not None:
                raise InvalidState("已签署的提油交接量不能被迟到读数改写，请发起计量争议")
            gross_total, net_total, evaluated, factor_set_sha256 = self._evaluate_readings(
                original["segment"], draft.readings
            )
            consumed_rows = self.connection.execute(
                "SELECT consumed_quota_units FROM custody_links WHERE parent_batch_id=?",
                (draft.batch_id,),
            ).fetchall()
            already_consumed = sum(
                (Decimal(row["consumed_quota_units"]) for row in consumed_rows), Decimal("0")
            )
            if net_total < already_consumed:
                raise InvalidState(
                    "迟到读数修正后的净量低于该批次已向下游交接的量，不能直接结算，请发起计量争议"
                )
            delta_gross = quantize_volume(gross_total - Decimal(original["gross_quota_units"]))
            delta_net = quantize_volume(net_total - Decimal(original["quantity_quota_units"]))
            digest_suffix = digest(draft.idempotency_key)[:10]
            base = original["batch_id"]
            successor_id = f"{base[:51]}-L{digest_suffix}" if len(base) > 51 else f"{base}-L{digest_suffix}"
            try:
                self.connection.execute(
                    "INSERT INTO custody_batches(batch_id,segment,product,well_group,gross_quota_units,"
                    "quantity_quota_units,factor_set_sha256,allowed_loss_basis_points,observed_at,state,"
                    "idempotency_key,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        successor_id,
                        original["segment"],
                        original["product"],
                        original["well_group"],
                        decimal_text(gross_total),
                        decimal_text(net_total),
                        factor_set_sha256,
                        int(original["allowed_loss_basis_points"]),
                        self._now(),
                        "settlement",
                        f"settle:{draft.idempotency_key}",
                        actor_id,
                        self._now(),
                    ),
                )
                reading_ids: list[int] = []
                for item in evaluated:
                    cursor = self.connection.execute(
                        "INSERT INTO meter_readings(batch_id,meter_id,calibration_id,meter_kind,factor_snapshot,"
                        "gross_reading,net_reading,reading_role,observed_at,recorded_by,recorded_at) "
                        "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            successor_id,
                            item["meter_id"],
                            item["calibration_version_id"],
                            item["meter_kind"],
                            item["factor_snapshot"],
                            item["gross_reading"],
                            item["net_reading"],
                            "late",
                            item["observed_at"],
                            actor_id,
                            self._now(),
                        ),
                    )
                    reading_ids.append(int(cursor.lastrowid))
                cursor = self.connection.execute(
                    "INSERT INTO late_settlements(original_batch_id,successor_batch_id,delta_gross_quota_units,"
                    "delta_net_quota_units,reason_code,note,idempotency_key,created_by,created_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        draft.batch_id,
                        successor_id,
                        decimal_text(delta_gross),
                        decimal_text(delta_net),
                        draft.reason_code,
                        draft.note,
                        draft.idempotency_key,
                        actor_id,
                        self._now(),
                    ),
                )
                settlement_id = int(cursor.lastrowid)
                self.connection.executemany(
                    "UPDATE meter_readings SET late_settlement_id=? WHERE reading_id=?",
                    [(settlement_id, reading_id) for reading_id in reading_ids],
                )
            except sqlite3.IntegrityError as exc:
                raise Conflict("后继结算批次或幂等键冲突") from exc
            self._audit(
                "custody_batch",
                successor_id,
                "custody.settlement.created",
                actor_id,
                {
                    "original_batch_id": draft.batch_id,
                    "delta_net_quota_units": decimal_text(delta_net),
                    "settlement_id": settlement_id,
                },
            )
            return {
                "settlement_id": settlement_id,
                "original_batch_id": draft.batch_id,
                "successor_batch_id": successor_id,
                "delta_gross_quota_units": decimal_text(delta_gross),
                "delta_net_quota_units": decimal_text(delta_net),
                "revised_quantity_quota_units": decimal_text(net_total),
                "state": "settlement",
            }

        return self._idempotent("custody_settlement", draft.idempotency_key, raw, build)

    def _successor_chain(self) -> dict[str, tuple[str, Decimal]]:
        """原批次 -> 最新后继批次及其净量（按结算编号取最新）。"""
        rows = self.connection.execute(
            "SELECT s.original_batch_id, s.successor_batch_id, s.settlement_id, b.quantity_quota_units "
            "FROM late_settlements s JOIN custody_batches b ON b.batch_id=s.successor_batch_id "
            "ORDER BY s.settlement_id"
        ).fetchall()
        latest: dict[str, tuple[str, Decimal, int]] = {}
        for row in rows:
            latest[row["original_batch_id"]] = (
                row["successor_batch_id"],
                Decimal(row["quantity_quota_units"]),
                int(row["settlement_id"]),
            )
        result: dict[str, tuple[str, Decimal]] = {}
        for start in latest:
            node = start
            while node in latest:
                node = latest[node][0]
            final_row = self.connection.execute(
                "SELECT quantity_quota_units FROM custody_batches WHERE batch_id=?", (node,)
            ).fetchone()
            result[start] = (node, Decimal(final_row["quantity_quota_units"]))
        return result

    # -- 争议与冻结 ----------------------------------------------------------

    def _open_dispute_freeze_sets(self) -> dict[str, set[str]]:
        """每个仍处于 open 状态的争议冻结的批次集合。"""
        disputes = self.connection.execute(
            "SELECT dispute_id,affected_batch_ids_json FROM measurement_disputes WHERE state='open'"
        ).fetchall()
        return {
            str(row["dispute_id"]): set(json.loads(row["affected_batch_ids_json"])) for row in disputes
        }

    def open_dispute(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "dispute.write")
        draft = DisputeDraft.from_dict(raw)

        def build() -> dict[str, Any]:
            anchor = self._batch(draft.anchor_batch_id)
            links = [
                dict(row)
                for row in self.connection.execute(
                    "SELECT parent_batch_id,child_batch_id,consumed_quota_units FROM custody_links"
                ).fetchall()
            ]
            if draft.scope == "lineage":
                affected = lineage_batch_ids(draft.anchor_batch_id, links)
            else:
                affected = frozenset({draft.anchor_batch_id})
            try:
                cursor = self.connection.execute(
                    "INSERT INTO measurement_disputes(scope,anchor_batch_id,affected_batch_ids_json,note,"
                    "created_by,created_at,idempotency_key) VALUES(?,?,?,?,?,?,?)",
                    (
                        draft.scope,
                        draft.anchor_batch_id,
                        canonical_json(sorted(affected)),
                        draft.note,
                        actor_id,
                        self._now(),
                        draft.idempotency_key,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise Conflict("争议幂等键冲突") from exc
            dispute_id = int(cursor.lastrowid)
            # 只冻结作用域内、当前可冻结的批次；已签署交接的提油批标记冻结但交接量不动。
            self.connection.execute(
                "UPDATE custody_batches SET state='frozen',revision=revision+1 "
                "WHERE state IN ('recorded','settlement') AND batch_id IN (%s)" % ",".join("?" for _ in affected),
                sorted(affected),
            )
            self._audit(
                "measurement_dispute",
                str(dispute_id),
                "custody.dispute.opened",
                actor_id,
                {
                    "scope": draft.scope,
                    "anchor_batch_id": draft.anchor_batch_id,
                    "frozen_batches": sorted(affected),
                },
            )
            return {
                "dispute_id": dispute_id,
                "scope": draft.scope,
                "anchor_batch_id": draft.anchor_batch_id,
                "frozen_batches": sorted(affected),
                "state": "open",
            }

        return self._idempotent("custody_dispute", draft.idempotency_key, raw, build)

    def resolve_dispute(self, actor_id: str, dispute_id: int, resolution_note: str) -> dict[str, Any]:
        self._require(actor_id, "dispute.resolve")
        if not resolution_note.strip():
            raise ValidationFailed("resolution_note 不能为空")
        with transaction(self.connection, immediate=True):
            dispute = self.connection.execute(
                "SELECT * FROM measurement_disputes WHERE dispute_id=?", (dispute_id,)
            ).fetchone()
            if dispute is None:
                raise NotFound("计量争议不存在")
            if dispute["state"] != "open":
                raise InvalidState("计量争议已经处理")
            affected = set(json.loads(dispute["affected_batch_ids_json"]))
            self.connection.execute(
                "UPDATE measurement_disputes SET state='resolved',resolution_note=?,resolved_by=?,resolved_at=? "
                "WHERE dispute_id=?",
                (resolution_note.strip(), actor_id, self._now(), dispute_id),
            )
            # 其它未决争议仍冻结的批次保持冻结。
            still_frozen: set[str] = set()
            for other_id, batches in self._open_dispute_freeze_sets().items():
                if int(other_id) != dispute_id:
                    still_frozen |= batches
            releasable = affected - still_frozen
            for batch_id in releasable:
                row = self.connection.execute(
                    "SELECT state FROM custody_batches WHERE batch_id=?", (batch_id,)
                ).fetchone()
                if row is None or row["state"] != "frozen":
                    continue
                is_successor = self.connection.execute(
                    "SELECT 1 FROM late_settlements WHERE successor_batch_id=? LIMIT 1", (batch_id,)
                ).fetchone()
                restored = "settlement" if is_successor else "recorded"
                self.connection.execute(
                    "UPDATE custody_batches SET state=?,revision=revision+1 WHERE batch_id=? AND state='frozen'",
                    (restored, batch_id),
                )
            self._audit(
                "measurement_dispute",
                str(dispute_id),
                "custody.dispute.resolved",
                actor_id,
                {"unfrozen_batches": sorted(releasable)},
            )
        return {"dispute_id": dispute_id, "state": "resolved", "unfrozen_batches": sorted(releasable)}

    # -- 反查与恒等式 --------------------------------------------------------

    def _batch_calibrations(self, batch_id: str) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT meter_id,calibration_id,meter_kind,factor_snapshot,gross_reading,net_reading,"
            "reading_role,observed_at FROM meter_readings WHERE batch_id=? ORDER BY reading_id",
            (batch_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def trace_voyage(self, actor_id: str, vessel_voyage: str) -> dict[str, Any]:
        self._require(actor_id, "report.read")
        handover = self._handover_for(vessel_voyage=vessel_voyage)
        return self._trace(handover)

    def trace_lifting(self, actor_id: str, lifting_batch_id: str) -> dict[str, Any]:
        self._require(actor_id, "report.read")
        handover = self._handover_for(lifting_batch_id=lifting_batch_id)
        return self._trace(handover)

    def _trace(self, handover: sqlite3.Row) -> dict[str, Any]:
        lifting_batch_id = handover["lifting_batch_id"]
        batch_rows = self.connection.execute("SELECT * FROM custody_batches").fetchall()
        link_rows = self.connection.execute(
            "SELECT parent_batch_id,child_batch_id,consumed_quota_units FROM custody_links"
        ).fetchall()
        batches = [dict(row) for row in batch_rows if row["state"] != "void"]
        links = [dict(row) for row in link_rows]
        lineage_ids = ordered_lineage(lifting_batch_id, batches, links)
        lineage: list[dict[str, Any]] = []
        link_index: dict[tuple[str, str], Decimal] = {
            (row["parent_batch_id"], row["child_batch_id"]): Decimal(row["consumed_quota_units"]) for row in link_rows
        }
        by_id = {row["batch_id"]: row for row in batches}
        for batch_id in lineage_ids:
            batch = by_id[batch_id]
            entry = {
                "batch_id": batch_id,
                "segment": batch["segment"],
                "well_group": batch["well_group"],
                "quantity_quota_units": batch["quantity_quota_units"],
                "allowed_loss_basis_points": batch["allowed_loss_basis_points"],
                "state": batch["state"],
                "calibrations": self._batch_calibrations(batch_id),
            }
            if batch["segment"] != WELL_PRODUCTION:
                inputs = [
                    {"parent_batch_id": parent, "consumed_quota_units": decimal_text(consumed)}
                    for (parent, child), consumed in sorted(link_index.items())
                    if child == batch_id and parent in lineage_ids
                ]
                inputs_total = quantize_volume(
                    sum((Decimal(item["consumed_quota_units"]) for item in inputs), Decimal("0"))
                )
                variance = segment_variance(
                    inputs_total, Decimal(batch["quantity_quota_units"]), int(batch["allowed_loss_basis_points"])
                )
                entry["inputs"] = inputs
                entry["segment_difference"] = variance.as_dict()
            lineage.append(entry)
        sources = attribute_sources(lifting_batch_id, batches, links)
        attributed_total = quantize_volume(
            sum((Decimal(row["attributed_quota_units"]) for row in sources), Decimal("0"))
        )
        return {
            "vessel_voyage": handover["vessel_voyage"],
            "receiver": handover["receiver"],
            "terminal": handover["terminal"],
            "bill_of_lading": handover["bill_of_lading"],
            "lifting_batch_id": lifting_batch_id,
            "signed_at": handover["signed_at"],
            "signed_quantity_quota_units": handover["signed_quantity_quota_units"],
            "signed_quantity_immutable": True,
            "source_well_groups": sources,
            "attributed_total_quota_units": decimal_text(attributed_total),
            "lineage": lineage,
        }

    def _effective_balance_rows(self) -> list[dict[str, Any]]:
        """把后继结算净量应用到原批次，得到用于恒等式核对的当前账面行。"""
        chain = self._successor_chain()
        rows = self.connection.execute(
            "SELECT * FROM custody_batches WHERE state!='settlement' AND state!='void'"
        ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            if row["batch_id"] in chain:
                _, revised_net = chain[row["batch_id"]]
                item["quantity_quota_units"] = decimal_text(revised_net)
                item["revised_by_settlement"] = True
            else:
                item["revised_by_settlement"] = False
            result.append(item)
        return result

    def material_balance(self, actor_id: str) -> dict[str, Any]:
        self._require(actor_id, "report.read")
        batches = self._effective_balance_rows()
        links = [
            dict(row)
            for row in self.connection.execute(
                "SELECT parent_batch_id,child_batch_id,consumed_quota_units FROM custody_links"
            ).fetchall()
        ]
        frozen: set[str] = set()
        for affected in self._open_dispute_freeze_sets().values():
            frozen |= affected
        balance = mass_balance(batches, links, frozen_batch_ids=frozenset(frozen))
        disputes = self.connection.execute(
            "SELECT dispute_id,scope,anchor_batch_id,state FROM measurement_disputes ORDER BY dispute_id"
        ).fetchall()
        balance["open_disputes"] = [dict(row) for row in disputes if row["state"] == "open"]
        return balance
