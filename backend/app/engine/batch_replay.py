"""批量回放引擎：异常归并、补偿分区清单、回刷规划和写入冲突校验。

异常捕获原本只能逐条处置，多条异常指向同一个缺失分区时会产生重复回刷。
本引擎把多条异常按「目标表 + 分区」归并成一份补偿分区清单，再消费清单生成
执行批次，并对并发写入申请做目标分区级别的冲突校验。

核心约束：
- 同一目标表的同一分区只进入清单一次；
- 只有源表和加工口径都有效的日期才进入执行批次；
- 上游源分区缺失的日期先阻塞，补源成功后重新规划即可释放；
- 两个申请引用了相交的目标分区时，同一分区只允许确认一次写入。
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Iterable

DEFAULT_PARTITION_FORMAT = "%Y-%m-%d"
MAX_EXPAND_DAYS = 3660

# 既支持日期范围，也支持逐日枚举。范围分隔符放在日期之后，避免吃掉日期本身的 '-'.
_RANGE_SPLIT = re.compile(r"\s*(?:~|～|至|到|—|–|\.\.|\bto\b)\s*")
_DATE_TOKEN = re.compile(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}")


@dataclass
class ExceptionReplayInput:
    """一条异常记录回放时需要的最小信息。"""

    exception_id: int
    pipeline: str
    source_table: str = ""
    target_table: str = ""
    processing_spec: str = ""
    pipeline_status: str = ""
    partitions: list[str] = field(default_factory=list)
    missing_source_partitions: list[str] = field(default_factory=list)
    message: str = ""


@dataclass
class ReplayManifestItem:
    """补偿分区清单中的一项（按目标表归并）。"""

    target_table: str
    pipeline: str
    source_table: str
    processing_spec: str
    requested_partitions: list[str]
    actual_gap: list[str]
    exception_refs: list[int]
    overlap_partitions: list[str] = field(default_factory=list)
    missing_source_partitions: list[str] = field(default_factory=list)
    pipeline_status: str = ""
    valid: bool = True
    invalid_reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "target_table": self.target_table,
            "pipeline": self.pipeline,
            "source_table": self.source_table,
            "processing_spec": self.processing_spec,
            "requested_partitions": list(self.requested_partitions),
            "actual_gap": list(self.actual_gap),
            "exception_refs": list(self.exception_refs),
            "overlap_partitions": list(self.overlap_partitions),
            "missing_source_partitions": list(self.missing_source_partitions),
            "valid": self.valid,
            "invalid_reason": self.invalid_reason,
            # 中文别名字段：清单直接可读，也方便前端/接口按业务术语取数。
            "目标表": self.target_table,
            "源表": self.source_table,
            "加工口径": self.processing_spec,
            "实际缺口": list(self.actual_gap),
            "原异常引用": list(self.exception_refs),
        }


class BatchReplayEngine:
    """批量回放领域算法集合。

    引擎本身不碰 store，便于单独测试；store 读写由 services 层完成。
    """

    # ---- 分区表达式解析 ----

    def parse_partitions(
        self,
        raw: Any,
        partition_format: str = DEFAULT_PARTITION_FORMAT,
    ) -> list[str]:
        """把异常记录里的「影响分区」解析为去重、有序的分区列表。

        支持：
        - 单日：``2026-11-04``
        - 范围：``2026-11-02~2026-11-04`` / ``2026-11-02 至 2026-11-04``
        - 枚举：``2026-11-02,2026-11-03、2026-11-04``
        - JSON 数组：``["2026-11-02", "2026-11-03"]``
        """
        tokens = self._flatten_partition_input(raw)
        partitions: list[str] = []
        for token in tokens:
            token = str(token).strip().strip("'\"")
            if not token:
                continue
            bounds = self._split_range(token)
            if len(bounds) == 2:
                expanded = self._expand_range(
                    bounds[0], bounds[1], partition_format,
                )
                partitions.extend(expanded)
            else:
                # 一个 token 内可能还残留空格分隔的多个日期。
                parts = [p for p in re.split(r"[\s,，;；、]+", token) if p]
                if len(parts) > 1 and all(_DATE_TOKEN.fullmatch(p) for p in parts):
                    partitions.extend(parts)
                else:
                    partitions.append(token)
        return self._dedupe_sorted(partitions, partition_format)

    def _flatten_partition_input(self, raw: Any) -> list[str]:
        if raw is None:
            return []
        if isinstance(raw, (list, tuple, set)):
            result: list[str] = []
            for item in raw:
                result.extend(self._flatten_partition_input(item))
            return result
        text = str(raw).strip()
        if not text:
            return []
        if text.startswith("[") and text.endswith("]"):
            try:
                decoded = json.loads(text)
                if isinstance(decoded, list):
                    return self._flatten_partition_input(decoded)
            except (ValueError, TypeError):
                pass
        # 逗号/分号/顿号是强分隔符；范围里的空格不能被拆开。
        return [part for part in re.split(r"[,，;；、\n]+", text) if part.strip()]

    def _split_range(self, token: str) -> list[str]:
        stripped = token.strip()
        parts = [part.strip() for part in _RANGE_SPLIT.split(stripped) if part.strip()]
        if len(parts) == 2 and all(_DATE_TOKEN.fullmatch(part) for part in parts):
            return parts
        # "2026-11-02 - 2026-11-04" 这种带空格的连字符范围。
        spaced = re.fullmatch(
            r"\s*(.+?)\s+-\s+(.+?)\s*", stripped,
        )
        if spaced and _DATE_TOKEN.fullmatch(spaced.group(1)) and _DATE_TOKEN.fullmatch(spaced.group(2)):
            return [spaced.group(1), spaced.group(2)]
        return [stripped]

    def _expand_range(
        self, start: str, end: str, partition_format: str,
    ) -> list[str]:
        try:
            start_dt = datetime.strptime(start, partition_format)
            end_dt = datetime.strptime(end, partition_format)
        except ValueError:
            return [start, end]
        if end_dt < start_dt:
            start_dt, end_dt = end_dt, start_dt
        result: list[str] = []
        current = start_dt
        while current <= end_dt and len(result) < MAX_EXPAND_DAYS:
            result.append(current.strftime(partition_format))
            current += timedelta(days=1)
        return result

    def _dedupe_sorted(
        self, partitions: Iterable[str], partition_format: str,
    ) -> list[str]:
        unique = list(dict.fromkeys(str(part).strip() for part in partitions if str(part).strip()))

        def sort_key(value: str) -> tuple[int, Any]:
            try:
                return 0, datetime.strptime(value, partition_format)
            except ValueError:
                return 1, value

        return sorted(unique, key=sort_key)

    # ---- 清单归并 ----

    def build_manifest(
        self,
        exceptions: list[ExceptionReplayInput],
        *,
        written_by_target: dict[str, set[str]] | None = None,
        source_missing_by_table: dict[str, set[str]] | None = None,
        partition_format: str = DEFAULT_PARTITION_FORMAT,
    ) -> list[dict[str, Any]]:
        """把多条异常归并为按目标表聚合的补偿分区清单。

        - 同一目标表的多个日期范围取并集，重叠日期只保留一次；
        - ``actual_gap`` 扣掉已经写入的目标分区；
        - 原异常引用保留在清单项里，便于回溯。
        """
        written_by_target = written_by_target or {}
        source_missing_by_table = source_missing_by_table or {}

        grouped: dict[str, ReplayManifestItem] = {}
        partition_exception_refs: dict[tuple[str, str], set[int]] = {}

        for item in exceptions:
            target = item.target_table or f"未解析目标表:{item.pipeline or item.exception_id}"
            group = grouped.get(target)
            if group is None:
                group = ReplayManifestItem(
                    target_table=target,
                    pipeline=item.pipeline,
                    source_table=item.source_table,
                    processing_spec=item.processing_spec,
                    requested_partitions=[],
                    actual_gap=[],
                    exception_refs=[],
                    pipeline_status=item.pipeline_status,
                )
                grouped[target] = group
            else:
                # 同目标表的异常应共享管线和口径；缺失时用后续异常补齐。
                group.pipeline = group.pipeline or item.pipeline
                group.source_table = group.source_table or item.source_table
                group.processing_spec = group.processing_spec or item.processing_spec
                group.pipeline_status = group.pipeline_status or item.pipeline_status

            for partition in item.partitions:
                if partition not in group.requested_partitions:
                    group.requested_partitions.append(partition)
                partition_exception_refs.setdefault(
                    (target, partition), set(),
                ).add(item.exception_id)

            if item.exception_id not in group.exception_refs:
                group.exception_refs.append(item.exception_id)

        manifest: list[dict[str, Any]] = []
        for target, group in grouped.items():
            group.requested_partitions = self._dedupe_sorted(
                group.requested_partitions, partition_format,
            )
            written = written_by_target.get(target, set())
            group.actual_gap = [
                partition for partition in group.requested_partitions
                if partition not in written
            ]

            overlap: list[str] = []
            for partition in group.requested_partitions:
                refs = partition_exception_refs.get((target, partition), set())
                if len(refs) > 1:
                    overlap.append(partition)
            group.overlap_partitions = self._dedupe_sorted(overlap, partition_format)

            missing_source = set(source_missing_by_table.get(group.source_table, set()))
            group.missing_source_partitions = [
                partition for partition in group.requested_partitions
                if partition in missing_source
            ]

            reasons: list[str] = []
            if not group.target_table or group.target_table.startswith("未解析目标表"):
                reasons.append("未找到输出目标表")
            if not group.source_table:
                reasons.append("缺少有效源表")
            if not group.processing_spec:
                reasons.append("缺少加工口径")
            group.valid = not reasons
            group.invalid_reason = "；".join(reasons)

            payload = group.to_dict()
            payload["already_written"] = [
                partition for partition in group.requested_partitions
                if partition in written
            ]
            manifest.append(payload)

        return sorted(manifest, key=lambda item: item["target_table"])

    # ---- 回刷规划 ----

    def build_plan(
        self,
        manifest: list[dict[str, Any]],
        *,
        batch_code: str,
        written_by_target: dict[str, set[str]] | None = None,
        source_missing_by_table: dict[str, set[str]] | None = None,
        partition_format: str = DEFAULT_PARTITION_FORMAT,
        parallelism: int = 1,
        batch_suffix_by_target: dict[str, int] | None = None,
    ) -> dict[str, Any]:
        """消费补偿分区清单，生成可执行批次和阻塞分区说明。

        一个目标表的多个有依赖日期放进同一个执行批次（``depends_on`` 描述
        日期链），因此同一回放批次内可以包含多个有依赖的日期。
        ``batch_suffix_by_target`` 让同一目标表的批次号在多次规划间保持稳定，
        补源后不会把旧批次的写入确认错记到另一个目标表的批次上。
        """
        written_by_target = written_by_target or {}
        source_missing_by_table = source_missing_by_table or {}
        batch_suffix_by_target = batch_suffix_by_target or {}

        execution_batches: list[dict[str, Any]] = []
        executable_keys: list[dict[str, str]] = []
        blocked_details: list[dict[str, str]] = []
        not_planned: list[dict[str, Any]] = []
        skipped_entries: list[dict[str, Any]] = []

        for item in manifest:
            target = str(item.get("target_table", ""))
            if not item.get("valid", True):
                skipped_entries.append({
                    "target_table": target,
                    "exception_refs": list(item.get("exception_refs", [])),
                    "reason": item.get("invalid_reason") or "缺少有效源表或加工口径",
                    "partitions": list(item.get("requested_partitions", [])),
                })
                continue

            written = written_by_target.get(target, set())
            gap = [
                partition for partition in item.get("actual_gap", [])
                if partition not in written
            ]
            missing_source = source_missing_by_table.get(
                str(item.get("source_table", "")), set(),
            )
            blocked = [partition for partition in gap if partition in missing_source]
            executable = [partition for partition in gap if partition not in missing_source]

            if not gap:
                not_planned.append({
                    "target_table": target,
                    "reason": "实际缺口已全部写入",
                    "partitions": [],
                })
                continue

            for partition in blocked:
                blocked_details.append({
                    "target_table": target,
                    "source_table": str(item.get("source_table", "")),
                    "partition": partition,
                    "reason": "上游源分区缺失",
                })
            if not executable:
                not_planned.append({
                    "target_table": target,
                    "reason": "上游源分区缺失，等待补源",
                    "partitions": blocked,
                })
                continue

            suffix = batch_suffix_by_target.get(target, len(execution_batches) + 1)
            batch_id = f"{batch_code}-B{suffix}"
            depends_on: dict[str, list[str]] = {}
            for index, partition in enumerate(executable):
                depends_on[partition] = [executable[index - 1]] if index else []
            execution_batches.append({
                "execution_batch_id": batch_id,
                "target_table": target,
                "source_table": str(item.get("source_table", "")),
                "processing_spec": str(item.get("processing_spec", "")),
                "pipeline": str(item.get("pipeline", "")),
                "exception_refs": list(item.get("exception_refs", [])),
                "partitions": executable,
                "partition_count": len(executable),
                "blocked_partitions": blocked,
                "depends_on": depends_on,
                "dependency_chain": [
                    [executable[index - 1], executable[index]]
                    for index in range(1, len(executable))
                ],
                "status": "待执行",
            })
            executable_keys.extend(
                {"target_table": target, "partition": partition}
                for partition in executable
            )

        executable_partitions = self._dedupe_sorted(
            [item["partition"] for item in executable_keys], partition_format,
        )
        blocked_partitions = self._dedupe_sorted(
            [item["partition"] for item in blocked_details], partition_format,
        )

        return {
            "batch_id": batch_code,
            "execution_batches": execution_batches,
            "executable_targets": [
                {
                    "target_table": batch["target_table"],
                    "partitions": list(batch["partitions"]),
                }
                for batch in execution_batches
            ],
            "executable_partitions": executable_partitions,
            "executable_partition_keys": executable_keys,
            "blocked_partitions": blocked_partitions,
            "blocked_details": blocked_details,
            "skipped_entries": skipped_entries,
            "not_planned": not_planned,
            "execution_batch_count": len(execution_batches),
            "executable_count": len(executable_keys),
            "blocked_count": len(blocked_details),
            "parallelism": max(int(parallelism or 1), 1),
        }

    # ---- 写入申请校验 ----

    def normalize_claims(
        self,
        claims: Iterable[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """规范化一次写入申请里的 (目标表, 分区)，同一分区只保留一次。"""
        seen: set[tuple[str, str]] = set()
        result: list[dict[str, Any]] = []
        for claim in claims:
            target = str(claim.get("target_table") or claim.get("目标表") or "").strip()
            raw_partitions = claim.get("partitions")
            if raw_partitions is None:
                partition = claim.get("partition") or claim.get("分区")
                partitions = [str(partition)] if partition else []
            elif isinstance(raw_partitions, (list, tuple, set)):
                partitions = [str(partition) for partition in raw_partitions]
            else:
                partitions = [str(raw_partitions)]
            for partition in partitions:
                partition = partition.strip()
                key = (target, partition)
                if not target or not partition or key in seen:
                    continue
                seen.add(key)
                result.append({"target_table": target, "partition": partition})
        return result

    def find_conflicts(
        self,
        claims: Iterable[dict[str, Any]],
        confirmed_writes: dict[str, set[str]],
    ) -> list[dict[str, str]]:
        """返回申请中已被其他写入确认占用的目标分区。"""
        conflicts: list[dict[str, str]] = []
        for claim in claims:
            target = str(claim.get("target_table", ""))
            partition = str(claim.get("partition", ""))
            if partition in confirmed_writes.get(target, set()):
                conflicts.append({
                    "target_table": target,
                    "partition": partition,
                    "reason": "目标分区已由其他申请确认写入",
                })
        return conflicts


def write_summary(
    write_events: list[dict[str, Any]],
) -> dict[str, Any]:
    """按目标表汇总实际写过的分区，重叠日期只计一次。"""
    by_target: dict[str, list[str]] = {}
    seen: set[tuple[str, str]] = set()
    for event in write_events:
        target = str(event.get("target_table", ""))
        partition = str(event.get("partition", ""))
        if (target, partition) in seen:
            continue
        seen.add((target, partition))
        by_target.setdefault(target, []).append(partition)

    return {
        "written_targets": sorted(by_target),
        "targets": [
            {
                "target_table": target,
                "partitions": sorted(partitions),
                "write_count": len(partitions),
            }
            for target, partitions in sorted(by_target.items())
        ],
        "written_partitions": sorted(
            f"{target}:{partition}" for target, partition in seen
        ),
        "total_partitions": len(seen),
    }
