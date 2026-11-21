"""批量回放API：多选异常归并清单、回刷规划、补源重试和写入确认。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException

from app.services.batch_replay import service as replay_service

router = APIRouter(prefix="/api/batch-replay", tags=["批量回放"])


def _replay_id(body: dict[str, Any], path_id: int | None = None) -> int:
    raw = path_id if path_id is not None else body.get("replay_id")
    try:
        return int(raw)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="缺少有效的 replay_id")


def _exception_ids(body: dict[str, Any]) -> list[int]:
    raw = body.get("exception_ids") or body.get("exceptionIds") or []
    if isinstance(raw, (int, str)):
        raw = [raw]
    ids: list[int] = []
    for item in raw:
        try:
            ids.append(int(item))
        except (TypeError, ValueError):
            continue
    if not ids:
        raise HTTPException(status_code=400, detail="请选择至少一条异常记录")
    return ids


@router.post("/preview")
def preview_replay(body: dict[str, Any]) -> dict[str, Any]:
    """预览补偿分区清单，不落库。"""
    return replay_service.preview(_exception_ids(body))


@router.post("/replays")
def create_replay(body: dict[str, Any]) -> dict[str, Any]:
    """一次提交多条异常，归并历史分区并生成补偿分区清单。"""
    result = replay_service.create_replay(_exception_ids(body))
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("message", "回放建单失败"))
    return result


@router.get("/replays")
def list_replays() -> dict[str, Any]:
    """列出已创建的批量回放请求。"""
    from app.store import store

    items = [
        {
            "replay_id": row.get("id"),
            "replay_code": row.get("replay_code"),
            "status": row.get("status"),
            "exception_ids": list(row.get("exception_ids", [])),
            "created_at": row.get("created_at"),
        }
        for row in store.rows("_batch_replay")
    ]
    return {"items": items, "total": len(items)}


@router.get("/replays/{replay_id}")
def get_replay(replay_id: int) -> dict[str, Any]:
    """查看回放请求的清单、规划和已写入分区。"""
    result = replay_service.get(replay_id)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("message"))
    return result


@router.post("/replays/{replay_id}/plan")
def plan_replay(replay_id: int, body: dict[str, Any] | None = None) -> dict[str, Any]:
    """消费补偿分区清单，为有效日期生成执行批次。"""
    body = body or {}
    result = replay_service.plan(replay_id, int(body.get("parallelism", 1)))
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("message"))
    return result


@router.post("/replays/{replay_id}/source-supplement")
def supplement_source(replay_id: int, body: dict[str, Any]) -> dict[str, Any]:
    """登记补源结果并重算规划。

    body:
      source_table, partitions: [...] | "2026-11-01~2026-11-05",
      failed_partitions（可选，失败的源分区继续保留缺失）, succeeded（默认 true）
    """
    result = replay_service.supplement_source(
        replay_id,
        str(body.get("source_table", "")),
        body.get("partitions") or [],
        failed_partitions=body.get("failed_partitions") or [],
        succeeded=bool(body.get("succeeded", True)),
    )
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("message"))
    return result


@router.post("/replays/{replay_id}/retry")
def retry_replay(replay_id: int) -> dict[str, Any]:
    """补源失败后重试：从仍然缺失的源分区继续规划。"""
    result = replay_service.retry_plan(replay_id)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("message"))
    return result


@router.post("/replays/{replay_id}/writes/validate")
def validate_write(replay_id: int, body: dict[str, Any]) -> dict[str, Any]:
    """写入前置校验：目标分区是否已被其他申请确认。

    body:
      execution_batch_id 或 claims: [{"target_table", "partitions": [...]}]
    """
    result = replay_service.validate_write(
        replay_id,
        execution_batch_id=body.get("execution_batch_id"),
        claims=body.get("claims"),
    )
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("message"))
    return result


@router.post("/replays/{replay_id}/writes")
def confirm_write(replay_id: int, body: dict[str, Any]) -> dict[str, Any]:
    """确认写入；相交目标分区只确认一次。"""
    result = replay_service.confirm_write(
        replay_id,
        execution_batch_id=body.get("execution_batch_id"),
        claims=body.get("claims"),
    )
    if not result.get("ok"):
        raise HTTPException(status_code=409, detail=result.get("message"))
    return result


@router.get("/replays/{replay_id}/compare")
def compare_replay(replay_id: int) -> dict[str, Any]:
    """对比补齐上游前后的可执行分区与实际写过的目标表。"""
    result = replay_service.compare(replay_id)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("message"))
    return result
