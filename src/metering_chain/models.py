"""端到端计量监管链的领域输入契约。"""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from .clock import parse_utc, utc_text
from .errors import ValidationFailed


IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{1,63}$")
STAGES = {
    "wellhead",
    "platform-separation",
    "pipeline-inlet",
    "pipeline-outlet",
    "floating-processing",
    "storage-tank",
    "offtake",
}
STAGE_ORDER = [
    "wellhead",
    "platform-separation",
    "pipeline-inlet",
    "pipeline-outlet",
    "floating-processing",
    "storage-tank",
    "offtake",
]
MAX_QUANTITY = Decimal("1000000000")


def required_text(value: object, field: str, maximum: int = 256) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationFailed(f"{field} 不能为空")
    result = value.strip()
    if len(result) > maximum:
        raise ValidationFailed(f"{field} 不能超过 {maximum} 个字符")
    return result


def optional_text(value: object, field: str, maximum: int = 256) -> str:
    if value is None:
        return ""
    return required_text(value, field, maximum)


def identifier(value: object, field: str) -> str:
    result = required_text(value, field, 64)
    if not IDENTIFIER.fullmatch(result):
        raise ValidationFailed(f"{field} 格式不正确")
    return result


def decimal_value(
    value: object,
    field: str,
    *,
    minimum: Decimal | None = None,
    maximum: Decimal | None = None,
) -> Decimal:
    if isinstance(value, bool):
        raise ValidationFailed(f"{field} 必须是数值")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValidationFailed(f"{field} 必须是十进制数值") from exc
    if not result.is_finite():
        raise ValidationFailed(f"{field} 必须是有限数值")
    if minimum is not None and result < minimum:
        raise ValidationFailed(f"{field} 不能小于 {minimum}")
    if maximum is not None and result > maximum:
        raise ValidationFailed(f"{field} 不能大于 {maximum}")
    return result


def positive_integer(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValidationFailed(f"{field} 必须是正整数")
    return value


def loss_basis_points(value: object, field: str = "loss_basis_points") -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 1000:
        raise ValidationFailed(f"{field} 必须是 0 到 1000 的整数")
    return value


@dataclass(frozen=True, slots=True)
class MeteringPointInput:
    point_id: str
    name: str
    stage: str
    facility_id: str
    well_group_id: str | None

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "MeteringPointInput":
        stage = required_text(raw.get("stage"), "stage", 32)
        if stage not in STAGES:
            raise ValidationFailed("stage 不是受支持的计量环节")
        well_group = raw.get("well_group_id")
        if stage == "wellhead" and well_group is None:
            raise ValidationFailed("井口计量点必须归属井组")
        return cls(
            point_id=identifier(raw.get("point_id"), "point_id"),
            name=required_text(raw.get("name"), "name"),
            stage=stage,
            facility_id=identifier(raw.get("facility_id"), "facility_id"),
            well_group_id=None if well_group is None else identifier(well_group, "well_group_id"),
        )


@dataclass(frozen=True, slots=True)
class ChainLinkInput:
    link_id: str
    from_point_id: str
    to_point_id: str
    loss_basis_points: int

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "ChainLinkInput":
        from_point = identifier(raw.get("from_point_id"), "from_point_id")
        to_point = identifier(raw.get("to_point_id"), "to_point_id")
        if from_point == to_point:
            raise ValidationFailed("计量链环节起点和终点不能相同")
        return cls(
            link_id=identifier(raw.get("link_id"), "link_id"),
            from_point_id=from_point,
            to_point_id=to_point,
            loss_basis_points=loss_basis_points(raw.get("loss_basis_points", 0)),
        )


@dataclass(frozen=True, slots=True)
class CalibrationInput:
    point_id: str
    version_no: int
    coefficient: Decimal
    note: str

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "CalibrationInput":
        return cls(
            point_id=identifier(raw.get("point_id"), "point_id"),
            version_no=positive_integer(raw.get("version_no"), "version_no"),
            coefficient=decimal_value(
                raw.get("coefficient"),
                "coefficient",
                minimum=Decimal("0.5"),
                maximum=Decimal("1.5"),
            ),
            note=optional_text(raw.get("note"), "note"),
        )


@dataclass(frozen=True, slots=True)
class BatchInput:
    batch_id: str
    wellhead_point_id: str

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "BatchInput":
        return cls(
            batch_id=identifier(raw.get("batch_id"), "batch_id"),
            wellhead_point_id=identifier(raw.get("wellhead_point_id"), "wellhead_point_id"),
        )


@dataclass(frozen=True, slots=True)
class ReadingInput:
    point_id: str
    batch_id: str
    observed_at: str
    raw_quantity: Decimal
    idempotency_key: str

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "ReadingInput":
        observed_at = required_text(raw.get("observed_at"), "observed_at", 40)
        try:
            observed_at = utc_text(parse_utc(observed_at, "observed_at"))
        except ValueError as exc:
            raise ValidationFailed(str(exc)) from exc
        return cls(
            point_id=identifier(raw.get("point_id"), "point_id"),
            batch_id=identifier(raw.get("batch_id"), "batch_id"),
            observed_at=observed_at,
            raw_quantity=decimal_value(
                raw.get("raw_quantity"),
                "raw_quantity",
                minimum=Decimal("0.001"),
                maximum=MAX_QUANTITY,
            ),
            idempotency_key=identifier(raw.get("idempotency_key"), "idempotency_key"),
        )


@dataclass(frozen=True, slots=True)
class VoyageInput:
    voyage_id: str
    tank_point_id: str
    vessel_name: str

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "VoyageInput":
        return cls(
            voyage_id=identifier(raw.get("voyage_id"), "voyage_id"),
            tank_point_id=identifier(raw.get("tank_point_id"), "tank_point_id"),
            vessel_name=required_text(raw.get("vessel_name"), "vessel_name", 128),
        )


@dataclass(frozen=True, slots=True)
class HandoverInput:
    quantity: Decimal

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "HandoverInput":
        return cls(
            quantity=decimal_value(
                raw.get("quantity_units"),
                "quantity_units",
                minimum=Decimal("0.001"),
                maximum=MAX_QUANTITY,
            ),
        )


@dataclass(frozen=True, slots=True)
class DisputeInput:
    dispute_id: str
    batch_id: str
    reason: str
    point_id: str | None

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "DisputeInput":
        point_id = raw.get("point_id")
        return cls(
            dispute_id=identifier(raw.get("dispute_id"), "dispute_id"),
            batch_id=identifier(raw.get("batch_id"), "batch_id"),
            reason=required_text(raw.get("reason"), "reason"),
            point_id=None if point_id is None else identifier(point_id, "point_id"),
        )
