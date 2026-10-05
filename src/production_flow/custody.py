"""端到端计量监管链的纯领域计算。

监管链把同一批原油从来源井组到提油船次拆成六段，每段用各自的校准版本
把仪表读数修正为净量，并逐段记录允许损耗与计量差异：

    井口产量 well_production
        -> 平台分离 platform_separation   （海基二号）
        -> 管输批次 pipeline_batch         （海底管道）
        -> 浮式加工 floating_processing    （海葵一号）
        -> 储罐混合 tank_blend             （海葵一号储罐）
        -> 提油交接 lifting_transfer       （提油轮船次）

本模块只做确定性计算，不访问 SQLite，便于离线验收与单元测试。
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from typing import Mapping, Sequence

from .planning import BASIS_POINTS, HUNDRED, ZERO, decimal_text, quantize_volume


QUANTUM = Decimal("0.001")
DEFAULT_TOLERANCE = Decimal("0.01")

# 段 -> 该段计量点使用的仪表/校准类别。
WELL_PRODUCTION = "well_production"
PLATFORM_SEPARATION = "platform_separation"
PIPELINE_BATCH = "pipeline_batch"
FLOATING_PROCESSING = "floating_processing"
TANK_BLEND = "tank_blend"
LIFTING_TRANSFER = "lifting_transfer"

SEGMENTS = (
    WELL_PRODUCTION,
    PLATFORM_SEPARATION,
    PIPELINE_BATCH,
    FLOATING_PROCESSING,
    TANK_BLEND,
    LIFTING_TRANSFER,
)

NODE_BY_SEGMENT = {
    WELL_PRODUCTION: "wellhead",
    PLATFORM_SEPARATION: "separator",
    PIPELINE_BATCH: "pipeline",
    FLOATING_PROCESSING: "fpsoprocess",
    TANK_BLEND: "tank",
    LIFTING_TRANSFER: "lifting",
}

# 每段只允许接收紧邻上游段的批次，防止监管链断裂或跳段。
UPSTREAM_SEGMENTS: Mapping[str, frozenset[str]] = {
    WELL_PRODUCTION: frozenset(),
    PLATFORM_SEPARATION: frozenset({WELL_PRODUCTION}),
    PIPELINE_BATCH: frozenset({PLATFORM_SEPARATION}),
    FLOATING_PROCESSING: frozenset({PIPELINE_BATCH}),
    TANK_BLEND: frozenset({FLOATING_PROCESSING}),
    LIFTING_TRANSFER: frozenset({TANK_BLEND}),
}

# 未进入储罐之前的持有量都算在途/在制，储罐持有算库存，提油批持有算外输。
IN_TRANSIT_SEGMENTS = (WELL_PRODUCTION, PLATFORM_SEPARATION, PIPELINE_BATCH, FLOATING_PROCESSING)

SEGMENT_ORDER = {segment: index for index, segment in enumerate(SEGMENTS)}


def apply_factor(gross: Decimal, factor: Decimal) -> Decimal:
    """把仪表原始读数按修正系数换算成净量。"""
    if gross < ZERO or factor <= ZERO:
        raise ValueError("读数和修正系数必须为正数")
    return quantize_volume(gross * factor)


def allowed_loss(inputs_total: Decimal, loss_basis_points: int) -> Decimal:
    if not 0 <= loss_basis_points <= 1000:
        raise ValueError("允许损耗基点必须在 0 到 1000 之间")
    return quantize_volume(inputs_total * Decimal(loss_basis_points) / BASIS_POINTS)


@dataclass(frozen=True, slots=True)
class SegmentVariance:
    inputs_total: Decimal
    measured_output: Decimal
    allowed_loss_basis_points: int
    allowed_loss: Decimal
    variance: Decimal

    def as_dict(self) -> dict[str, object]:
        return {
            "inputs_total": decimal_text(self.inputs_total),
            "measured_output": decimal_text(self.measured_output),
            "allowed_loss_basis_points": self.allowed_loss_basis_points,
            "allowed_loss": decimal_text(self.allowed_loss),
            "segment_variance": decimal_text(self.variance),
        }


def segment_variance(
    inputs_total: Decimal,
    measured_output: Decimal,
    loss_basis_points: int,
) -> SegmentVariance:
    """单段差异 = 实测投入 - 允许损耗 - 实测产出。"""
    loss = allowed_loss(inputs_total, loss_basis_points)
    variance = quantize_volume(inputs_total - loss - measured_output)
    return SegmentVariance(inputs_total, measured_output, loss_basis_points, loss, variance)


def lineage_batch_ids(anchor_batch_id: str, links: Sequence[Mapping[str, object]]) -> frozenset[str]:
    """争议 lineage 作用域：锚点批次沿血缘的全部祖先与后代，加锚点自身。"""
    parents: dict[str, set[str]] = defaultdict(set)
    children: dict[str, set[str]] = defaultdict(set)
    for link in links:
        parent = str(link["parent_batch_id"])
        child = str(link["child_batch_id"])
        children[parent].add(child)
        parents[child].add(parent)

    related: set[str] = {anchor_batch_id}
    stack = [anchor_batch_id]
    while stack:
        current = stack.pop()
        for nxt in parents.get(current, ()):  # 向来源方向
            if nxt not in related:
                related.add(nxt)
                stack.append(nxt)
    stack = [anchor_batch_id]
    while stack:
        current = stack.pop()
        for nxt in children.get(current, ()):  # 向下游方向
            if nxt not in related:
                related.add(nxt)
                stack.append(nxt)
    return frozenset(related)


def _balances(
    batches: Sequence[Mapping[str, object]],
    links: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    quantity: dict[str, Decimal] = {}
    segment: dict[str, str] = {}
    loss_bp: dict[str, int] = {}
    state: dict[str, str] = {}
    for row in batches:
        batch_id = str(row["batch_id"])
        quantity[batch_id] = Decimal(str(row["quantity_quota_units"]))
        segment[batch_id] = str(row["segment"])
        loss_bp[batch_id] = int(row["allowed_loss_basis_points"])
        state[batch_id] = str(row["state"])

    consumed_out: dict[str, Decimal] = defaultdict(lambda: ZERO)
    inputs_by_child: dict[str, list[tuple[str, Decimal]]] = defaultdict(list)
    for link in links:
        parent = str(link["parent_batch_id"])
        child = str(link["child_batch_id"])
        consumed = Decimal(str(link["consumed_quota_units"]))
        if parent in quantity and child in quantity:  # 跳过已作废批次（预留）
            consumed_out[parent] += consumed
            inputs_by_child[child].append((parent, consumed))
    return {
        "quantity": quantity,
        "segment": segment,
        "loss_bp": loss_bp,
        "state": state,
        "consumed_out": consumed_out,
        "inputs_by_child": inputs_by_child,
    }


def held_quantity(batch: Mapping[str, object], consumed_out: Decimal) -> Decimal:
    """批次当前仍持有的净量 = 入账净量 - 已被下游消耗的量。"""
    held = quantize_volume(Decimal(str(batch["quantity_quota_units"])) - consumed_out)
    if held < ZERO:
        raise ValueError("批次消耗量超过入账量，监管链不守恒")
    return held


def mass_balance(
    batches: Sequence[Mapping[str, object]],
    links: Sequence[Mapping[str, object]],
    *,
    tolerance: Decimal = DEFAULT_TOLERANCE,
    frozen_batch_ids: frozenset[str] = frozenset(),
) -> dict[str, object]:
    """验证 产出 = 在途 + 库存 + 外输 + 允许损耗 + 未解释计量差异。

    由于每段都满足 投入 = 产出 + 允许损耗 + 段差异，沿整条链望远镜求和后，
    源头注入必然等于各阶段持有量加允许损耗加未解释差异；残差只能来自十进制量化。
    """
    derived = _balances(batches, links)
    quantity = derived["quantity"]
    segment_of = derived["segment"]
    loss_bp = derived["loss_bp"]
    consumed_out = derived["consumed_out"]
    inputs_by_child = derived["inputs_by_child"]

    production = ZERO
    buckets = {
        "in_transit": ZERO,
        "inventory": ZERO,
        "offtake": ZERO,
    }
    held_by_segment: dict[str, Decimal] = {name: ZERO for name in SEGMENTS}
    allowed_loss_total = ZERO
    unaccounted_variance = ZERO
    segment_rows: list[dict[str, object]] = []

    for row in sorted(batches, key=lambda item: str(item["batch_id"])):
        batch_id = str(row["batch_id"])
        seg = segment_of[batch_id]
        held = held_quantity(row, consumed_out.get(batch_id, ZERO))
        held_by_segment[seg] += held
        if seg == WELL_PRODUCTION:
            # 井口产量是恒等式的源头注入侧。
            production += quantity[batch_id]
        if seg == LIFTING_TRANSFER:
            buckets["offtake"] += held
        elif seg == TANK_BLEND:
            buckets["inventory"] += held
        else:
            # 井口、平台分离、管输、浮式加工段的持有量都属于在途/在制。
            buckets["in_transit"] += held

        if seg != WELL_PRODUCTION:
            inputs_total = quantize_volume(sum((consumed for _, consumed in inputs_by_child.get(batch_id, ())), ZERO))
            variance = segment_variance(inputs_total, quantity[batch_id], loss_bp[batch_id])
            allowed_loss_total += variance.allowed_loss
            unaccounted_variance += variance.variance
            segment_rows.append({
                "batch_id": batch_id,
                "segment": seg,
                "frozen": batch_id in frozen_batch_ids,
                **variance.as_dict(),
            })

    for name in held_by_segment:
        held_by_segment[name] = quantize_volume(held_by_segment[name])
    production = quantize_volume(production)
    for name in buckets:
        buckets[name] = quantize_volume(buckets[name])
    allowed_loss_total = quantize_volume(allowed_loss_total)
    unaccounted_variance = quantize_volume(unaccounted_variance)
    accountable = quantize_volume(
        buckets["in_transit"] + buckets["inventory"] + buckets["offtake"]
        + allowed_loss_total + unaccounted_variance
    )
    residual = quantize_volume(production - accountable)
    return {
        "production": decimal_text(production),
        "in_transit": decimal_text(buckets["in_transit"]),
        "inventory": decimal_text(buckets["inventory"]),
        "offtake": decimal_text(buckets["offtake"]),
        "allowed_loss": decimal_text(allowed_loss_total),
        "unaccounted_variance": decimal_text(unaccounted_variance),
        "residual": decimal_text(residual),
        "balanced": abs(residual) <= tolerance,
        "held_by_segment": {name: decimal_text(held_by_segment[name]) for name in SEGMENTS},
        "frozen_batches": sorted(frozen_batch_ids),
        "segments": segment_rows,
    }


def attribute_sources(
    target_batch_id: str,
    batches: Sequence[Mapping[str, object]],
    links: Sequence[Mapping[str, object]],
) -> list[dict[str, str]]:
    """把目标批次（通常是提油批）的净量沿监管链级联归属到来源井组。

    在每个汇聚点按各来源实际投入量占该子批总投入的份额，把"子批自己的净量"
    分配给来源。这样各段的允许损耗与未解释差异会按比例摊薄，归属到井组的
    合计始终等于目标批次净量（只有末端十进制量化的极小残差）。
    """
    derived = _balances(batches, links)
    quantity = derived["quantity"]
    segment_of = derived["segment"]
    well_group: dict[str, str] = {}
    for row in batches:
        if row.get("well_group") is not None:
            well_group[str(row["batch_id"])] = str(row["well_group"])
    inputs_by_child = derived["inputs_by_child"]

    aggregated: dict[str, Decimal] = defaultdict(lambda: ZERO)

    def walk(batch_id: str, amount: Decimal) -> None:
        if segment_of[batch_id] == WELL_PRODUCTION:
            aggregated[well_group[batch_id]] += amount
            return
        inputs = inputs_by_child.get(batch_id, ())
        total_input = sum((consumed for _, consumed in inputs), ZERO)
        if total_input <= ZERO:
            return
        for parent_id, consumed in inputs:
            walk(parent_id, amount * consumed / total_input)

    walk(target_batch_id, quantity[target_batch_id])
    return [
        {"well_group": group, "attributed_quota_units": decimal_text(quantize_volume(aggregated[group]))}
        for group in sorted(aggregated)
    ]


def ordered_lineage(
    target_batch_id: str,
    batches: Sequence[Mapping[str, object]],
    links: Sequence[Mapping[str, object]],
) -> list[str]:
    """目标批次沿来源方向的全部批次，按监管链段次与批次编号稳定排序。"""
    derived = _balances(batches, links)
    inputs_by_child = derived["inputs_by_child"]
    segment_of = derived["segment"]

    related: set[str] = {target_batch_id}
    stack = [target_batch_id]
    while stack:
        current = stack.pop()
        for parent_id, _ in inputs_by_child.get(current, ()):
            if parent_id not in related:
                related.add(parent_id)
                stack.append(parent_id)
    return sorted(related, key=lambda bid: (SEGMENT_ORDER[segment_of[bid]], bid))
