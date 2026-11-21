"""批量回放领域服务：把多条异常归并为补偿分区清单并驱动回刷批次。

与单条异常处置不同，批量回放会跨异常记录归并历史分区：
- 同一目标表的重复/重叠日期只保留一份缺口；
- 清单项携带目标表、实际缺口和原异常引用；
- 回刷规划只给具有有效源表和加工口径的日期生成执行批次；
- 上游源分区缺失的日期单独阻塞，补源成功后重算即可释放。
"""
from __future__ import annotations

import json
import threading
from datetime import datetime
from typing import Any, Iterable

from app.engine.batch_replay import (
    BatchReplayEngine,
    ExceptionReplayInput,
    write_summary as summarize_writes,
)
from app.services.base import BaseModuleService
from app.store import store

REPLAY_TABLE = "_batch_replay"
WRITE_TABLE = "_replay_write"
SOURCE_TABLE = "_source_partition_status"


class BatchReplayService(BaseModuleService):
    MODULE = "exception_capture"
    ENTITY = "异常记录"
    OPERATION_TERM = "回放"
    LIST_FIELDS = ["发生时间", "所属管线", "异常类型", "异常消息", "影响分区", "栈跟踪", "处理状态"]
    REQUIRED_FIELDS = ["发生时间", "所属管线", "异常类型"]
    STATUS_ORDER = ["未处理", "处理中", "已解决", "已忽略"]
    ACTION_RULES = {}

    def __init__(self) -> None:
        self.engine = BatchReplayEngine()
        # 同步接口在线程池里执行，写入确认需要串行化，避免相交批次同时通过校验。
        self._write_lock = threading.Lock()

    # ---- 异常解析 ----

    def _pipeline_for(self, pipeline_name: str) -> dict[str, Any] | None:
        """按管线名解析源表、目标表和加工口径。

        先精确匹配，再退化为包含匹配；找不到时返回 None，
        由清单标记为「缺少有效源表/加工口径」而不是凭空生成批次。
        """
        rows = store.rows("pipeline_def")
        exact = next(
            (row for row in rows if str(row.get("流水线名称", "")) == pipeline_name),
            None,
        )
        if exact is not None:
            return exact
        folded = pipeline_name.casefold()
        for row in rows:
            name = str(row.get("流水线名称", ""))
            if folded and (folded in name.casefold() or name.casefold() in folded):
                return row
        return None

    def _exception_input(self, entry: dict[str, Any]) -> ExceptionReplayInput:
        pipeline_name = str(entry.get("所属管线", ""))
        pipeline = self._pipeline_for(pipeline_name)
        source_table = str(pipeline.get("输入源表", "")) if pipeline else ""
        target_table = str(pipeline.get("输出目标表", "")) if pipeline else ""
        processing_spec = str(pipeline.get("处理阶段", "")) if pipeline else ""
        pipeline_status = str(pipeline.get("status", "")) if pipeline else ""

        return ExceptionReplayInput(
            exception_id=int(entry.get("id", 0)),
            pipeline=pipeline_name,
            source_table=source_table,
            target_table=target_table,
            processing_spec=processing_spec,
            pipeline_status=pipeline_status,
            partitions=self.engine.parse_partitions(entry.get("影响分区")),
            missing_source_partitions=self._source_missing_for(source_table),
            message=str(entry.get("异常消息", "")),
        )

    # ---- 私有状态表 ----

    def _source_missing_for(self, source_table: str) -> list[str]:
        if not source_table:
            return []
        return [
            str(row.get("partition", ""))
            for row in store.rows(SOURCE_TABLE)
            if str(row.get("source_table", "")) == source_table
            and not bool(row.get("available", False))
        ]

    def _source_missing_map(self) -> dict[str, set[str]]:
        result: dict[str, set[str]] = {}
        for row in store.rows(SOURCE_TABLE):
            if bool(row.get("available", False)):
                continue
            result.setdefault(str(row.get("source_table", "")), set()).add(
                str(row.get("partition", ""))
            )
        return result

    def _written_map(self) -> dict[str, set[str]]:
        result: dict[str, set[str]] = {}
        for row in store.rows(WRITE_TABLE):
            result.setdefault(str(row.get("target_table", "")), set()).add(
                str(row.get("partition", ""))
            )
        return result

    def _next_id(self, table: str) -> int:
        return max(
            (int(row.get("id", 0)) for row in store.rows(table)), default=0,
        ) + 1

    def _find_replay(self, replay_id: int) -> dict[str, Any] | None:
        for row in store.rows(REPLAY_TABLE):
            if int(row.get("id", 0)) == int(replay_id):
                return row
        return None

    def _load_manifest(self, replay: dict[str, Any]) -> list[dict[str, Any]]:
        raw = replay.get("manifest")
        if isinstance(raw, list):
            return raw
        try:
            decoded = json.loads(str(replay.get("manifest_json", "[]")))
        except (ValueError, TypeError):
            return []
        return decoded if isinstance(decoded, list) else []

    # ---- 清单 ----

    def _exception_entries(
        self, exception_ids: Iterable[int],
    ) -> tuple[list[dict[str, Any]], list[int]]:
        entries: list[dict[str, Any]] = []
        missing: list[int] = []
        for exception_id in exception_ids:
            entry = store.find("exception_capture", int(exception_id))
            if entry is None:
                missing.append(int(exception_id))
            else:
                entries.append(entry)
        return entries, missing

    def build_manifest(
        self, exception_ids: Iterable[int],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[int]]:
        """读取异常并归并为补偿分区清单（不落库）。"""
        entries, missing = self._exception_entries(exception_ids)
        manifest = self.engine.build_manifest(
            [self._exception_input(entry) for entry in entries],
            written_by_target=self._written_map(),
            source_missing_by_table=self._source_missing_map(),
        )
        return manifest, entries, missing

    def preview(
        self, exception_ids: list[int],
    ) -> dict[str, Any]:
        """仅计算清单，不落库；用于提交前的多选预览。"""
        manifest, entries, missing = self.build_manifest(exception_ids)
        return {
            "exception_ids": [int(entry.get("id", 0)) for entry in entries],
            "missing_exception_ids": missing,
            "manifest": manifest,
            "summary": self._manifest_summary(manifest),
        }

    def create_replay(
        self, exception_ids: list[int],
    ) -> dict[str, Any]:
        """一次提交多条异常，落库为一个批量回放请求。"""
        manifest, entries, missing = self.build_manifest(exception_ids)
        if not entries:
            return {
                "ok": False,
                "message": "没有可回放的异常记录",
                "missing_exception_ids": missing,
            }
        replay_id = self._next_id(REPLAY_TABLE)
        now = datetime.now().isoformat()
        replay = {
            "id": replay_id,
            "replay_code": f"RPL-{datetime.now().strftime('%Y%m%d')}-{replay_id:03d}",
            "status": "已建单",
            "pending": True,
            "abnormal": False,
            "exception_ids": [int(entry.get("id", 0)) for entry in entries],
            "missing_exception_ids": missing,
            "manifest": manifest,
            "manifest_json": json.dumps(manifest, ensure_ascii=False),
            "plan": None,
            "first_plan": None,
            # 建单时的补源状态快照，用于对比补齐上游前后的可执行分区。
            "initial_source_missing": {
                table: sorted(partitions)
                for table, partitions in self._source_missing_map().items()
            },
            "created_at": now,
            "updated_at": now,
        }
        store.rows(REPLAY_TABLE).append(replay)
        return {
            "ok": True,
            "message": f"已归并 {len(entries)} 条异常，生成补偿分区清单",
            "replay_id": replay_id,
            "replay_code": replay["replay_code"],
            "manifest": manifest,
            "summary": self._manifest_summary(manifest),
            "missing_exception_ids": missing,
        }

    def _manifest_summary(self, manifest: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "target_count": len(manifest),
            "exception_count": len({
                ref
                for item in manifest
                for ref in item.get("exception_refs", [])
            }),
            "requested_partitions": sum(
                len(item.get("requested_partitions", [])) for item in manifest
            ),
            "actual_gap_partitions": sum(
                len(item.get("actual_gap", [])) for item in manifest
            ),
            "overlap_partitions": sorted({
                partition
                for item in manifest
                for partition in item.get("overlap_partitions", [])
            }),
            "invalid_targets": [
                item.get("target_table") for item in manifest if not item.get("valid", True)
            ],
        }

    # ---- 规划 ----

    def _compute_plan(
        self, replay: dict[str, Any], parallelism: int = 1,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        manifest, _entries, _missing = self.build_manifest(
            replay.get("exception_ids", []),
        )
        plan = self.engine.build_plan(
            manifest,
            batch_code=str(replay.get("replay_code", f"RPL-{replay.get('id')}")),
            written_by_target=self._written_map(),
            source_missing_by_table=self._source_missing_map(),
            parallelism=parallelism,
            batch_suffix_by_target=self._batch_suffixes(replay),
        )
        return manifest, plan

    def _batch_suffixes(self, replay: dict[str, Any]) -> dict[str, int]:
        """按目标表分配稳定批次号。

        首次出现的目标表取下一个空号；已经规划过的目标表沿用原批次号，
        这样重新规划（例如补源后新增分区）不会把批次号挪给另一张表。
        """
        suffix_by_target = dict(replay.get("batch_suffix_by_target") or {})
        previous_plan = replay.get("plan") or {}
        for batch in previous_plan.get("execution_batches", []):
            target = str(batch.get("target_table", ""))
            batch_id = str(batch.get("execution_batch_id", ""))
            if target in suffix_by_target or "-B" not in batch_id:
                continue
            try:
                suffix_by_target[target] = int(batch_id.rsplit("-B", 1)[1])
            except ValueError:
                continue
        for item in self._load_manifest(replay):
            target = str(item.get("target_table", ""))
            if target in suffix_by_target:
                continue
            suffix_by_target[target] = max(suffix_by_target.values(), default=0) + 1
        replay["batch_suffix_by_target"] = suffix_by_target
        return suffix_by_target

    def plan(
        self, replay_id: int, parallelism: int = 1,
    ) -> dict[str, Any]:
        replay = self._find_replay(replay_id)
        if replay is None:
            return {"ok": False, "message": f"回放请求 {replay_id} 不存在"}

        manifest, plan = self._compute_plan(replay, parallelism)
        now = datetime.now().isoformat()
        replay["manifest"] = manifest
        replay["manifest_json"] = json.dumps(manifest, ensure_ascii=False)
        previous_plan = replay.get("plan")
        # 已确认写入的批次状态要保留，重新规划不把写入记录倒回「待执行」；
        # 只有整批分区都已落库时才算「已写入」，避免新增缺口被旧状态掩盖。
        written_map = self._written_map()
        written_batches = {
            str(item.get("execution_batch_id")): item
            for item in (previous_plan or {}).get("execution_batches", [])
            if item.get("status") == "已写入"
        }
        for batch in plan.get("execution_batches", []):
            done = written_batches.get(str(batch.get("execution_batch_id")))
            if not done:
                continue
            target_written = written_map.get(str(batch.get("target_table", "")), set())
            if set(batch.get("partitions", [])) <= target_written:
                batch["status"] = "已写入"
                batch["written_at"] = done.get("written_at")
        if replay.get("first_plan") is None:
            replay["first_plan"] = plan
        replay["plan"] = plan
        replay["status"] = "已规划" if plan.get("executable_count") else "等待补源"
        replay["pending"] = True
        replay["updated_at"] = now

        return {
            "ok": True,
            "message": (
                f"已生成 {plan.get('execution_batch_count', 0)} 个执行批次，"
                f"可执行 {plan.get('executable_count', 0)} 个分区，"
                f"阻塞 {plan.get('blocked_count', 0)} 个分区"
            ),
            "replay_id": replay_id,
            "replay_code": replay.get("replay_code"),
            "manifest": manifest,
            "summary": self._manifest_summary(manifest),
            "plan": plan,
            "planned_at": now,
        }

    def get(self, replay_id: int) -> dict[str, Any]:
        replay = self._find_replay(replay_id)
        if replay is None:
            return {"ok": False, "message": f"回放请求 {replay_id} 不存在"}
        manifest = self._load_manifest(replay)
        return {
            "ok": True,
            "replay_id": replay_id,
            "replay_code": replay.get("replay_code"),
            "status": replay.get("status"),
            "exception_ids": list(replay.get("exception_ids", [])),
            "manifest": manifest,
            "summary": self._manifest_summary(manifest),
            "first_plan": replay.get("first_plan"),
            "plan": replay.get("plan"),
            "writes": self.write_summary(),
            "source_missing": {
                table: sorted(partitions)
                for table, partitions in self._source_missing_map().items()
            },
            "created_at": replay.get("created_at"),
            "updated_at": replay.get("updated_at"),
        }

    # ---- 补源 ----

    def supplement_source(
        self,
        replay_id: int,
        source_table: str,
        partitions: Any,
        *,
        failed_partitions: Any = None,
        succeeded: bool = True,
    ) -> dict[str, Any]:
        """登记补源结果并重算规划。

        失败的分区继续保持缺失，下一次重试只从仍然缺失的源分区继续；
        成功补上的分区立即释放对应目标的回刷批次。
        """
        replay = self._find_replay(replay_id)
        if replay is None:
            return {"ok": False, "message": f"回放请求 {replay_id} 不存在"}
        if not source_table:
            return {"ok": False, "message": "缺少补源表名"}
        requested = self.engine.parse_partitions(partitions)
        if not requested:
            return {"ok": False, "message": "缺少补源分区"}

        failed = set(self.engine.parse_partitions(failed_partitions or []))
        if not succeeded:
            failed.update(requested)
        succeeded_partitions = [
            partition for partition in requested if partition not in failed
        ]
        now = datetime.now().isoformat()

        for partition in requested:
            rows = [
                row for row in store.rows(SOURCE_TABLE)
                if str(row.get("source_table", "")) == source_table
                and str(row.get("partition", "")) == partition
            ]
            row = rows[0] if rows else None
            if row is None:
                row = {
                    "id": self._next_id(SOURCE_TABLE),
                    "source_table": source_table,
                    "partition": partition,
                    "available": False,
                    "status": "缺失",
                    "retry_count": 0,
                    "last_error": "",
                }
                store.rows(SOURCE_TABLE).append(row)
            if partition in failed:
                row["retry_count"] = int(row.get("retry_count", 0)) + 1
                row["status"] = "补源失败"
                row["last_error"] = "最近一次补源未成功"
                row["available"] = False
            else:
                row["status"] = "已补源"
                row["last_error"] = ""
                row["available"] = True
            row["updated_at"] = now

        plan_result = self.plan(replay_id)
        released = self._released_partitions(
            source_table, succeeded_partitions, plan_result,
        )
        return {
            "ok": True,
            "message": (
                f"补源成功 {len(succeeded_partitions)} 个分区，失败 {len(failed)} 个；"
                "重试只保留仍缺失的源分区"
            ),
            "replay_id": replay_id,
            "source_table": source_table,
            "succeeded_partitions": succeeded_partitions,
            "failed_partitions": sorted(failed),
            "still_missing_partitions": sorted(self._source_missing_for(source_table)),
            "released_target_partitions": released,
            "plan": plan_result.get("plan"),
            "manifest": plan_result.get("manifest"),
            "summary": plan_result.get("summary"),
            "updated_at": now,
        }

    def _released_partitions(
        self,
        source_table: str,
        supplied: list[str],
        plan_result: dict[str, Any],
    ) -> list[str]:
        plan = plan_result.get("plan") or {}
        released: list[str] = []
        for batch in plan.get("execution_batches", []):
            if str(batch.get("source_table", "")) != source_table:
                continue
            released.extend(
                partition for partition in batch.get("partitions", [])
                if partition in supplied
            )
        return sorted(set(released))

    def retry_plan(self, replay_id: int) -> dict[str, Any]:
        """补源失败后的重试入口：按当前仍缺失的源分区重新规划。"""
        replay = self._find_replay(replay_id)
        if replay is None:
            return {"ok": False, "message": f"回放请求 {replay_id} 不存在"}
        missing = self._source_missing_map()
        result = self.plan(replay_id)
        plan = result.get("plan") or {}
        result["retry"] = {
            # 重试只从仍然缺失的源分区继续，已补上的分区不再出现在待补列表。
            "still_missing_source": {
                table: sorted(partitions) for table, partitions in missing.items()
            },
            "resumable_partitions": plan.get("executable_partitions", []),
            "blocked_partitions": plan.get("blocked_partitions", []),
        }
        result["message"] = (
            "重试规划从仍缺失的源分区继续："
            f"可执行 {len(plan.get('executable_partitions', []))} 个，"
            f"仍阻塞 {len(plan.get('blocked_partitions', []))} 个"
        )
        return result

    # ---- 写入确认 ----

    def validate_write(
        self,
        replay_id: int,
        *,
        execution_batch_id: str | None = None,
        claims: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """校验一次写入申请关联的目标分区是否已被其他申请占用。"""
        replay = self._find_replay(replay_id)
        if replay is None:
            return {"ok": False, "message": f"回放请求 {replay_id} 不存在"}
        normalized, batch, error = self._resolve_claims(
            replay, execution_batch_id, claims,
        )
        if error:
            return {"ok": False, "message": error}
        conflicts = self.engine.find_conflicts(normalized, self._written_map())
        return {
            "ok": True,
            "valid": not conflicts,
            "message": "目标分区可写入" if not conflicts else "存在已被确认写入的目标分区",
            "replay_id": replay_id,
            "execution_batch_id": execution_batch_id,
            "claims": normalized,
            "conflicts": conflicts,
            "batch": batch,
        }

    def confirm_write(
        self,
        replay_id: int,
        *,
        execution_batch_id: str | None = None,
        claims: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """确认写入；相交目标分区只允许确认一次。"""
        replay = self._find_replay(replay_id)
        if replay is None:
            return {"ok": False, "message": f"回放请求 {replay_id} 不存在"}
        normalized, batch, error = self._resolve_claims(
            replay, execution_batch_id, claims,
        )
        if error:
            return {"ok": False, "message": error}
        if not normalized:
            return {"ok": False, "message": "没有需要确认写入的目标分区"}

        with self._write_lock:
            conflicts = self.engine.find_conflicts(normalized, self._written_map())
            if conflicts:
                return {
                    "ok": False,
                    "message": "目标分区已被其他批次确认写入，相交区间只允许写入一次",
                    "replay_id": replay_id,
                    "execution_batch_id": execution_batch_id,
                    "claims": normalized,
                    "conflicts": conflicts,
                    "writes": self.write_summary(),
                }

            now = datetime.now().isoformat()
            write_key = f"W-{replay.get('replay_code', replay_id)}-{self._next_id(WRITE_TABLE)}"
            for claim in normalized:
                store.rows(WRITE_TABLE).append({
                    "id": self._next_id(WRITE_TABLE),
                    "write_key": write_key,
                    "replay_id": replay_id,
                    "replay_code": replay.get("replay_code"),
                    "execution_batch_id": execution_batch_id,
                    "target_table": claim["target_table"],
                    "partition": claim["partition"],
                    "status": "已确认",
                    "confirmed_at": now,
                })

        if execution_batch_id:
            for item in (replay.get("plan") or {}).get("execution_batches", []):
                if item.get("execution_batch_id") == execution_batch_id:
                    item["status"] = "已写入"
                    item["written_at"] = now

        replay["status"] = "写入中"
        replay["updated_at"] = now
        summary = self.write_summary()
        return {
            "ok": True,
            "message": f"已确认 {len(normalized)} 个目标分区写入",
            "replay_id": replay_id,
            "execution_batch_id": execution_batch_id,
            "write_key": write_key,
            "claims": normalized,
            "writes": summary,
            "confirmed_at": now,
        }

    def _resolve_claims(
        self,
        replay: dict[str, Any],
        execution_batch_id: str | None,
        claims: list[dict[str, Any]] | None,
    ) -> tuple[list[dict[str, Any]], dict[str, Any] | None, str]:
        batch = None
        if execution_batch_id:
            for item in (replay.get("plan") or {}).get("execution_batches", []):
                if item.get("execution_batch_id") == execution_batch_id:
                    batch = item
                    break
            if batch is None:
                return [], None, f"执行批次 {execution_batch_id} 不存在，请先规划"
        if claims:
            return self.engine.normalize_claims(claims), batch, ""
        if batch is not None:
            return self.engine.normalize_claims([
                {
                    "target_table": batch.get("target_table"),
                    "partitions": batch.get("partitions", []),
                },
            ]), batch, ""
        return [], None, "需要提供 execution_batch_id 或目标分区申请"

    def write_summary(self) -> dict[str, Any]:
        return summarize_writes(store.rows(WRITE_TABLE))

    # ---- 对比报告 ----

    def compare(self, replay_id: int) -> dict[str, Any]:
        """输出补齐上游前后的可执行分区与实际写过的目标表对比。"""
        replay = self._find_replay(replay_id)
        if replay is None:
            return {"ok": False, "message": f"回放请求 {replay_id} 不存在"}
        # 只读重算，不改写已落库的规划与批次状态。
        manifest, current_plan = self._compute_plan(replay)
        summary = self.write_summary()
        written_keys = set(summary.get("written_partitions", []))
        executable = current_plan.get("executable_partitions", [])
        awaiting = [
            claim["partition"]
            for claim in current_plan.get("executable_partition_keys", [])
            if f"{claim.get('target_table', '')}:{claim.get('partition', '')}"
            not in written_keys
        ]
        first_plan = replay.get("first_plan")
        # 首轮被阻塞、当前已可执行的分区，就是补齐上游后释放出来的部分。
        released = [
            partition for partition in current_plan.get("executable_partitions", [])
            if partition in (first_plan or {}).get("blocked_partitions", [])
        ]
        return {
            "ok": True,
            "replay_id": replay_id,
            "replay_code": replay.get("replay_code"),
            "before_source_supplement": first_plan,
            "before_executable_partitions": (
                first_plan or {}
            ).get("executable_partitions", []),
            "current_plan": current_plan,
            "manifest": manifest,
            "executable_partitions": executable,
            "released_partitions": released,
            "blocked_partitions": current_plan.get("blocked_partitions", []),
            "written_targets": summary.get("written_targets", []),
            "written_partitions": summary.get("written_partitions", []),
            "targets": summary.get("targets", []),
            "awaiting_write_partitions": awaiting,
            "still_missing_source": {
                table: sorted(partitions)
                for table, partitions in self._source_missing_map().items()
            },
            "summary": self._manifest_summary(manifest),
        }


service = BatchReplayService()
