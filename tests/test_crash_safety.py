"""Tests for the two "don't ruin a live demo" fixes:

1. run_and_record must not leave a permanently stuck "running" row if
   the *engine itself* crashes (as opposed to a task failing normally,
   which the Executor already handles) -- a hung blue badge is a worse
   demo failure than an honest "failed".
2. The dashboard's "Run now" button must return immediately and run
   the pipeline in the background, so the live poll can actually show
   the running -> success/failed transition instead of the whole HTTP
   request blocking until the pipeline is already done.
"""
import tempfile
import time
from pathlib import Path

import pytest

from flowctl.app.application import (
    create_linear_job,
    get_session,
    list_runs,
    run_and_record,
    run_registered_in_background,
)
from flowctl.core.pipeline import CycleError, Pipeline
from flowctl.core.task import Task
from flowctl.storage.db import get_engine, get_session_factory, init_db


def _fresh_session():
    engine = get_engine(":memory:")
    init_db(engine)
    return get_session_factory(engine)()


def test_run_and_record_marks_run_failed_instead_of_stuck_running_on_engine_crash():
    # Build a genuinely cyclic pipeline (a real Task.depends_on mutual
    # reference) -- Pipeline.execution_plan() raises CycleError for
    # this, which happens *inside* Executor.execute(), before any task
    # outcome is ever produced. This is the "engine itself crashed"
    # case run_and_record's try/except is meant to catch.
    task_a = Task(name="a", func=lambda: None, depends_on=[])
    task_b = Task(name="b", func=lambda: None, depends_on=[task_a])
    task_a.depends_on.append(task_b)  # sneak in a cycle after construction

    pipeline = Pipeline("cyclic_pipeline", [task_a, task_b])
    session = _fresh_session()

    with pytest.raises(CycleError):
        run_and_record(pipeline, session)

    run = list_runs(session, pipeline_name="cyclic_pipeline", limit=1)[0]
    assert run.status == "failed"
    assert run.ended_at is not None

    crash_result = [tr for tr in run.task_results if tr.task_name == "__pipeline_crash__"]
    assert len(crash_result) == 1
    assert "CycleError" in crash_result[0].error


def test_run_registered_in_background_returns_immediately_and_completes_async(monkeypatch, tmp_path):
    db_path = tmp_path / "bg_run.db"
    monkeypatch.setenv("FLOWCTL_DB_PATH", str(db_path))

    with get_session() as session:
        create_linear_job("bg-demo-job", ["sleep 1", "echo done"], session, schedule=None)

    started = time.time()
    run_registered_in_background("bg-demo-job")
    elapsed = time.time() - started

    # The whole pipeline takes >= 1s (the sleep step); the call
    # returning almost instantly is the actual behavior under test.
    assert elapsed < 0.5, f"run_registered_in_background blocked for {elapsed:.2f}s, expected near-instant return"

    deadline = time.time() + 5
    final_status = None
    while time.time() < deadline:
        with get_session() as session:
            runs = list_runs(session, pipeline_name="bg-demo-job", limit=1)
            if runs and runs[0].status != "running":
                final_status = runs[0].status
                break
        time.sleep(0.05)

    assert final_status == "success"


def test_run_registered_in_background_raises_immediately_for_unknown_pipeline(monkeypatch, tmp_path):
    db_path = tmp_path / "bg_run_missing.db"
    monkeypatch.setenv("FLOWCTL_DB_PATH", str(db_path))

    with pytest.raises(ValueError, match="No registered pipeline"):
        run_registered_in_background("does-not-exist")
