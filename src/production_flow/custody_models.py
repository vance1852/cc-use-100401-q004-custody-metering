"""端到端计量监管链的输入契约。"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping

from .clock import parse_utc
from .custody import NODE_BY_SEGMENT, SEGMENTS
from .errors import ValidationFailed
from .models import decimal_value, identifier, required_text


METER_KINDS = frozenset(NODE_BY_SEGMENT.values())
DISPUTE_SCOPES = frozenset({"single", "lineage"})


def iso_time(value: object, field: str) -> str:
    text = required_text(value, field, 40)
    try:
        return parse_utc(text, field).isoformat().replace("+00:00", "Z")
    except ValueError as exc:
        raise ValidationFailed(str(exc)) from exc


def loss_points(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 1000:
        raise ValidationFailed("allowed_loss_basis_points 必须是 0 到 1000 的整数")
    return value


@dataclass(frozen=True, slots=True)
class CalibrationDraft:
    calibration_id: str
    meter_kind: str
    factor: Decimal
    basis: str
    effective_from: str

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "CalibrationDraft":
        meter_kind = required_text(raw.get("meter_kind"), "meter_kind", 32)
        if meter_kind not in METER_KINDS:
            raise ValidationFailed("meter_kind 不是受支持的计量点类型")
        return cls(
            calibration_id=identifier(raw.get("calibration_version_id"), "calibration_version_id"),
            meter_kind=meter_kind,
            factor=decimal_value(raw.get("correction_factor"), "correction_factor", minimum=Decimal("0.000001")),
            basis=required_text(raw.get("basis"), "basis"),
            effective_from=iso_time(raw.get("effective_from"), "effective_from"),
        )


@dataclass(frozen=True, slots=True)
class ReadingDraft:
    meter_id: str
    calibration_version_id: str
    gross_reading: Decimal
    observed_at: str

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "ReadingDraft":
        return cls(
            meter_id=identifier(raw.get("meter_id"), "meter_id"),
            calibration_version_id=identifier(raw.get("calibration_version_id"), "calibration_version_id"),
            gross_reading=decimal_value(raw.get("gross_reading"), "gross_reading", minimum=Decimal("0.000001")),
            observed_at=iso_time(raw.get("observed_at"), "observed_at"),
        )


@dataclass(frozen=True, slots=True)
class BatchDraft:
    batch_id: str
    segment: str
    readings: tuple[ReadingDraft, ...]
    allowed_loss_basis_points: int
    well_group: str | None
    product: str
    observed_at: str
    idempotency_key: str

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "BatchDraft":
        segment = required_text(raw.get("segment"), "segment", 32)
        if segment not in SEGMENTS:
            raise ValidationFailed("segment 不是受支持的监管链段")
        readings_raw = raw.get("readings")
        if not isinstance(readings_raw, list) or not readings_raw:
            raise ValidationFailed("readings 至少包含一条仪表读数")
        readings = tuple(ReadingDraft.from_dict(item) for item in readings_raw)
        well_group = raw.get("well_group")
        well_group = None if well_group is None else identifier(well_group, "well_group")
        if segment == "well_production" and not well_group:
            raise ValidationFailed("井口产量批次必须提供来源井组 well_group")
        if segment != "well_production" and well_group:
            raise ValidationFailed("只有井口产量批次可以直接登记来源井组")
        product = required_text(raw.get("product", "crude-oil"), "product", 32)
        if product != "crude-oil":
            raise ValidationFailed("计量监管链目前只跟踪 crude-oil")
        return cls(
            batch_id=identifier(raw.get("batch_id"), "batch_id"),
            segment=segment,
            readings=readings,
            allowed_loss_basis_points=loss_points(raw.get("allowed_loss_basis_points", 0)),
            well_group=well_group,
            product=product,
            observed_at=iso_time(raw.get("observed_at"), "observed_at"),
            idempotency_key=identifier(raw.get("idempotency_key"), "idempotency_key"),
        )


@dataclass(frozen=True, slots=True)
class LinkDraft:
    child_batch_id: str
    parent_batch_id: str
    consumed_quota_units: Decimal
    idempotency_key: str

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "LinkDraft":
        return cls(
            child_batch_id=identifier(raw.get("child_batch_id"), "child_batch_id"),
            parent_batch_id=identifier(raw.get("parent_batch_id"), "parent_batch_id"),
            consumed_quota_units=decimal_value(
                raw.get("consumed_quota_units"), "consumed_quota_units", minimum=Decimal("0.001")
            ),
            idempotency_key=identifier(raw.get("idempotency_key"), "idempotency_key"),
        )


@dataclass(frozen=True, slots=True)
class HandoverDraft:
    lifting_batch_id: str
    vessel_voyage: str
    receiver: str
    terminal: str
    bill_of_lading: str | None
    idempotency_key: str

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "HandoverDraft":
        bill = raw.get("bill_of_lading")
        return cls(
            lifting_batch_id=identifier(raw.get("lifting_batch_id"), "lifting_batch_id"),
            vessel_voyage=required_text(raw.get("vessel_voyage"), "vessel_voyage", 64),
            receiver=required_text(raw.get("receiver"), "receiver", 128),
            terminal=required_text(raw.get("terminal"), "terminal", 128),
            bill_of_lading=None if bill is None else identifier(bill, "bill_of_lading"),
            idempotency_key=identifier(raw.get("idempotency_key"), "idempotency_key"),
        )


@dataclass(frozen=True, slots=True)
class LateReadingDraft:
    batch_id: str
    readings: tuple[ReadingDraft, ...]
    reason_code: str
    note: str
    idempotency_key: str

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "LateReadingDraft":
        readings_raw = raw.get("readings")
        if not isinstance(readings_raw, list) or not readings_raw:
            raise ValidationFailed("readings 至少包含一条迟到仪表读数")
        return cls(
            batch_id=identifier(raw.get("batch_id"), "batch_id"),
            readings=tuple(ReadingDraft.from_dict(item) for item in readings_raw),
            reason_code=identifier(raw.get("reason_code"), "reason_code"),
            note=required_text(raw.get("note"), "note", 512),
            idempotency_key=identifier(raw.get("idempotency_key"), "idempotency_key"),
        )


@dataclass(frozen=True, slots=True)
class DisputeDraft:
    scope: str
    anchor_batch_id: str
    note: str
    idempotency_key: str

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "DisputeDraft":
        scope = required_text(raw.get("scope"), "scope", 16)
        if scope not in DISPUTE_SCOPES:
            raise ValidationFailed("scope 必须是 single 或 lineage")
        return cls(
            scope=scope,
            anchor_batch_id=identifier(raw.get("anchor_batch_id"), "anchor_batch_id"),
            note=required_text(raw.get("note"), "note", 512),
            idempotency_key=identifier(raw.get("idempotency_key"), "idempotency_key"),
        )
