"""计量监管链的确定性计算：读数修正、环节差异、提油分摊与全链平衡。"""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal, ROUND_FLOOR, ROUND_HALF_UP
from typing import Mapping, Sequence


ZERO = Decimal("0")
UNIT = Decimal("0.001")
BASIS_POINTS = Decimal("10000")


def quantize_volume(value: Decimal) -> Decimal:
    return value.quantize(UNIT, rounding=ROUND_HALF_UP)


def decimal_text(value: Decimal) -> str:
    return format(value, "f")


def canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def digest(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def corrected_quantity(raw_quantity: Decimal, coefficient: Decimal) -> Decimal:
    """按上传时生效的校准系数修正原始读数，结果一旦写入不再随新系数变化。"""
    if raw_quantity < ZERO or coefficient <= ZERO:
        raise ValueError("原始读数和校准系数必须为正数")
    return quantize_volume(raw_quantity * coefficient)


def segment_variance(
    *,
    link_id: str,
    from_point_id: str,
    to_point_id: str,
    loss_basis_points: int,
    input_quantity: Decimal,
    output_quantity: Decimal,
) -> dict[str, object]:
    """相邻计量点之间的差异：负差异为损耗，与链路允许损耗对比。"""
    if not 0 <= loss_basis_points <= 1000:
        raise ValueError("损耗基点超出范围")
    variance = quantize_volume(output_quantity - input_quantity)
    allowable = quantize_volume(input_quantity * Decimal(loss_basis_points) / BASIS_POINTS)
    return {
        "link_id": link_id,
        "from_point_id": from_point_id,
        "to_point_id": to_point_id,
        "input_units": decimal_text(quantize_volume(input_quantity)),
        "output_units": decimal_text(quantize_volume(output_quantity)),
        "variance_units": decimal_text(variance),
        "allowable_loss_units": decimal_text(allowable),
        "within_allowance": abs(variance) <= allowable,
    }


def allocate_proportional(total: Decimal, shares: Mapping[str, Decimal]) -> dict[str, Decimal]:
    """把交接量按各批次罐内份额比例分摊，最大余数法保证合计精确等于交接量。"""
    total = quantize_volume(total)
    if total < ZERO:
        raise ValueError("交接数量不能为负数")
    if any(share < ZERO for share in shares.values()):
        raise ValueError("批次份额不能为负数")
    available = sum(shares.values(), ZERO)
    if available < total:
        raise ValueError("可分摊库存不足以覆盖交接数量")
    if not shares:
        if total == ZERO:
            return {}
        raise ValueError("没有可分摊的批次")
    if total == ZERO:
        return {key: ZERO for key in sorted(shares)}
    exact = {key: total * share / available for key, share in shares.items()}
    floors = {key: value.quantize(UNIT, rounding=ROUND_FLOOR) for key, value in exact.items()}
    remaining = int((total - sum(floors.values(), ZERO)) / UNIT)
    remaining = max(0, min(remaining, len(shares)))
    order = sorted(shares, key=lambda key: (-(exact[key] - floors[key]), key))
    result = dict(floors)
    for key in order[:remaining]:
        result[key] += UNIT
    return {key: result[key] for key in sorted(result)}


def summarize_balance(
    *,
    produced: Decimal,
    furthest_quantity: Decimal,
    reached_tank: bool,
    exported: Decimal,
    segments: Sequence[Mapping[str, object]],
    tolerance_basis_points: int,
) -> dict[str, object]:
    """单批次平衡：产出 = 外输 + 库存 + 在途 + 允许损耗 + 未明差异。"""
    if produced < ZERO or furthest_quantity < ZERO or exported < ZERO:
        raise ValueError("平衡输入不能为负数")
    if not 0 <= tolerance_basis_points <= 1000:
        raise ValueError("容差基点超出范围")
    allowable = sum((Decimal(str(row["allowable_loss_units"])) for row in segments), ZERO)
    remaining = quantize_volume(furthest_quantity - exported)
    inventory = quantize_volume(remaining if reached_tank else ZERO)
    in_transit = quantize_volume(ZERO if reached_tank else remaining)
    unaccounted = quantize_volume(produced - exported - inventory - in_transit - allowable)
    actual_loss = quantize_volume(produced - furthest_quantity)
    tolerance = quantize_volume(produced * Decimal(tolerance_basis_points) / BASIS_POINTS)
    return {
        "produced_units": decimal_text(quantize_volume(produced)),
        "exported_units": decimal_text(quantize_volume(exported)),
        "inventory_units": decimal_text(inventory),
        "in_transit_units": decimal_text(in_transit),
        "allowable_loss_units": decimal_text(quantize_volume(allowable)),
        "actual_loss_units": decimal_text(actual_loss),
        "unaccounted_units": decimal_text(unaccounted),
        "tolerance_units": decimal_text(tolerance),
        "balanced": abs(unaccounted) <= tolerance,
    }
