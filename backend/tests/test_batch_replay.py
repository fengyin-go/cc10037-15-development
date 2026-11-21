"""批量回放领域测试：归并清单、规划批次、补源重试和写入冲突。

运行：cd backend && .venv/bin/python -m pytest tests -q
（仓库未锁定 pytest 时也可直接 python tests/test_batch_replay.py 运行。）
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.engine.batch_replay import (  # noqa: E402
    BatchReplayEngine,
    ExceptionReplayInput,
)
from app.services import batch_replay as replay_module  # noqa: E402
from app.services.batch_replay import BatchReplayService  # noqa: E402
from app.store import store  # noqa: E402

SOUTH = "ads.ads_sales_dashboard"
PROFILE = "dw.dws_user_profile_daily"


def fresh_service() -> BatchReplayService:
    """每个用例使用独立的服务与私有状态表，互不污染。"""
    store._tables[replay_module.REPLAY_TABLE] = []
    store._tables[replay_module.WRITE_TABLE] = []
    status = store._tables[replay_module.SOURCE_TABLE]
    for row in status:
        row["available"] = row["partition"] != "2026-11-04"
        row["retry_count"] = 0
    return BatchReplayService()


def test_manifest_merges_overlap_once() -> None:
    service = fresh_service()
    preview = service.preview([5, 6, 7])
    manifest = preview["manifest"]
    sales = next(item for item in manifest if item["target_table"] == SOUTH)
    assert sales["requested_partitions"] == [
        "2026-11-02", "2026-11-03", "2026-11-04", "2026-11-05", "2026-11-06",
    ]
    assert sales["exception_refs"] == [5, 6]
    assert sales["overlap_partitions"] == ["2026-11-04"]
    assert sales["missing_source_partitions"] == ["2026-11-04"]
    assert sales["实际缺口"] == sales["actual_gap"]
    profile = next(item for item in manifest if item["target_table"] == PROFILE)
    assert profile["requested_partitions"] == ["2026-11-03"]
    assert preview["summary"]["requested_partitions"] == 6


def test_invalid_target_skipped() -> None:
    engine = BatchReplayEngine()
    manifest = engine.build_manifest([
        ExceptionReplayInput(exception_id=1, pipeline="不存在的管线", partitions=["2026-11-01"]),
    ])
    plan = engine.build_plan(manifest, batch_code="T")
    assert plan["execution_batches"] == []
    assert plan["skipped_entries"][0]["reason"]


def test_plan_blocks_missing_source_and_keeps_dates_in_one_batch() -> None:
    service = fresh_service()
    replay_id = service.create_replay([5, 6, 7])["replay_id"]
    plan = service.plan(replay_id)["plan"]
    assert plan["blocked_partitions"] == ["2026-11-04"]
    assert plan["executable_partitions"] == [
        "2026-11-02", "2026-11-03", "2026-11-05", "2026-11-06",
    ]
    sales_batch = next(
        batch for batch in plan["execution_batches"] if batch["target_table"] == SOUTH
    )
    # 同一批次内包含多个有依赖的日期。
    assert sales_batch["partitions"] == plan["executable_partitions"]
    assert ["2026-11-02", "2026-11-03"] in sales_batch["dependency_chain"]
    assert sales_batch["blocked_partitions"] == ["2026-11-04"]


def test_supplement_failure_retry_resumes_from_missing() -> None:
    service = fresh_service()
    replay_id = service.create_replay([5, 6, 7])["replay_id"]
    service.plan(replay_id)

    failed = service.supplement_source(
        replay_id, "dw.dws_sales_summary", ["2026-11-04"], succeeded=False,
    )
    assert failed["failed_partitions"] == ["2026-11-04"]
    assert failed["still_missing_partitions"] == ["2026-11-04"]
    assert failed["released_target_partitions"] == []

    retry = service.retry_plan(replay_id)
    assert retry["retry"]["still_missing_source"] == {
        "dw.dws_sales_summary": ["2026-11-04"],
    }
    assert "2026-11-04" not in retry["retry"]["resumable_partitions"]

    ok = service.supplement_source(
        replay_id, "dw.dws_sales_summary", "2026-11-04",
    )
    assert ok["still_missing_partitions"] == []
    assert ok["released_target_partitions"] == ["2026-11-04"]
    assert "2026-11-04" in (ok["plan"] or {})["executable_partitions"]


def test_compare_before_and_after_supplement() -> None:
    service = fresh_service()
    replay_id = service.create_replay([5, 6, 7])["replay_id"]
    service.plan(replay_id)
    service.supplement_source(replay_id, "dw.dws_sales_summary", "2026-11-04")
    result = service.compare(replay_id)
    assert result["before_executable_partitions"] == [
        "2026-11-02", "2026-11-03", "2026-11-05", "2026-11-06",
    ]
    assert "2026-11-04" in result["executable_partitions"]
    assert result["released_partitions"] == ["2026-11-04"]
    assert result["blocked_partitions"] == []
    assert result["written_targets"] == []


def test_overlapping_batches_write_once() -> None:
    service = fresh_service()
    replay_id = service.create_replay([5, 6, 7])["replay_id"]
    service.plan(replay_id)

    first = service.confirm_write(replay_id, claims=[{
        "target_table": SOUTH,
        "partitions": ["2026-11-02", "2026-11-03", "2026-11-05", "2026-11-06"],
    }])
    assert first["ok"] is True

    reported = service.validate_write(replay_id, claims=[{
        "target_table": SOUTH, "partitions": ["2026-11-03", "2026-11-04"],
    }])
    assert reported["valid"] is False
    assert [item["partition"] for item in reported["conflicts"]] == ["2026-11-03"]

    second = service.confirm_write(replay_id, claims=[{
        "target_table": SOUTH, "partitions": ["2026-11-03", "2026-11-04"],
    }])
    assert second["ok"] is False and second["conflicts"]

    # 另一个目标表同一天不算冲突。
    other = service.confirm_write(replay_id, claims=[{
        "target_table": PROFILE, "partitions": ["2026-11-03"],
    }])
    assert other["ok"] is True
    summary = service.write_summary()
    assert summary["written_targets"] == sorted([SOUTH, PROFILE])
    assert f"{SOUTH}:2026-11-03" in summary["written_partitions"]
    assert f"{PROFILE}:2026-11-03" in summary["written_partitions"]


def test_replan_keeps_written_batch_status() -> None:
    service = fresh_service()
    replay_id = service.create_replay([5, 6, 7])["replay_id"]
    plan = service.plan(replay_id)["plan"]
    batch_id = plan["execution_batches"][0]["execution_batch_id"]
    assert service.confirm_write(replay_id, execution_batch_id=batch_id)["ok"] is True

    service.supplement_source(replay_id, "dw.dws_sales_summary", "2026-11-04")
    replanned = service.plan(replay_id)["plan"]
    # 补源后同一目标表仍用原批次号，未写的新分区状态回到待执行。
    sales_batch = next(
        batch for batch in replanned["execution_batches"]
        if batch["target_table"] == SOUTH
    )
    assert sales_batch["execution_batch_id"] == batch_id
    assert sales_batch["partitions"] == ["2026-11-04"]
    assert sales_batch["status"] == "待执行"


def test_parse_partitions_variants() -> None:
    engine = BatchReplayEngine()
    assert engine.parse_partitions("2026-11-02~2026-11-04") == [
        "2026-11-02", "2026-11-03", "2026-11-04",
    ]
    assert engine.parse_partitions("2026-11-02 至 2026-11-04") == [
        "2026-11-02", "2026-11-03", "2026-11-04",
    ]
    assert engine.parse_partitions("2026-11-02,2026-11-03、2026-11-04") == [
        "2026-11-02", "2026-11-03", "2026-11-04",
    ]
    assert engine.parse_partitions('["2026-11-02", "2026-11-03"]') == [
        "2026-11-02", "2026-11-03",
    ]
    assert engine.parse_partitions("2026-11-04") == ["2026-11-04"]


if __name__ == "__main__":
    tests = [value for name, value in sorted(globals().items()) if name.startswith("test_")]
    for test in tests:
        test()
        print(f"ok  {test.__name__}")
    print(f"{len(tests)} passed")
