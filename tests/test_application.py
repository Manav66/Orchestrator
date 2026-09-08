"""Tests for the application layer: run_and_record().

These tests prove two things at once:
  1. Running a pipeline through run_and_record() correctly persists a
     Run row and one TaskResult row per task, including retries and
     failures.
  2. The Phase 1 engine tests (test_pipeline.py, test_executor.py)
     remain completely free of any database import -- the boundary
     between "pure engine" and "persistence" wasn't quietly broken
     while wiring this layer up.
"""
import ast
from pathlib import Path

from flowctl.app.application import run_and_record
from flowctl.core.pipeline import Pipeline
from flowctl.core.task import task
from flowctl.storage.db import get_engine, get_session_factory, init_db
from flowctl.storage.models import Pipeline as PipelineModel
from flowctl.storage.models import Run, TaskResult


def _fresh_session():
    engine = get_engine(":memory:")
    init_db(engine)
    return get_session_factory(engine)()


def test_run_and_record_persists_a_successful_pipeline():
    @task()
    def fetch():
        return {"orders": 42}

    @task(depends_on=[fetch])
    def report(data):
        return f"Report: {data['orders']} orders"

    pipeline = Pipeline("nightly_report", [fetch, report])
    session = _fresh_session()

    run = run_and_record(pipeline, session)

    assert run.id is not None
    assert run.pipeline_name == "nightly_report"
    assert run.status == "success"

    persisted = session.query(Run).filter_by(pipeline_name="nightly_report").one()
    task_names = {tr.task_name for tr in persisted.task_results}
    assert task_names == {"fetch", "report"}


def test_run_and_record_persists_retry_and_failure_detail():
    attempts_made = {"count": 0}

    @task(retries=2, retry_delay=0)
    def flaky():
        attempts_made["count"] += 1
        if attempts_made["count"] < 3:
            raise RuntimeError("transient")
        return "ok"

    @task()
    def always_fails():
        raise RuntimeError("permanent")

    pipeline = Pipeline("mixed", [flaky, always_fails])
    session = _fresh_session()

    run = run_and_record(pipeline, session)

    results_by_name = {tr.task_name: tr for tr in run.task_results}
    assert results_by_name["flaky"].status == "success"
    assert results_by_name["flaky"].attempts == 3
    assert results_by_name["always_fails"].status == "failed"
    assert "permanent" in results_by_name["always_fails"].error


def test_two_runs_of_the_same_pipeline_are_both_recorded_separately():
    @task()
    def one():
        return 1

    pipeline = Pipeline("repeatable", [one])
    session = _fresh_session()

    run_and_record(pipeline, session)
    run_and_record(pipeline, session)

    assert session.query(Run).filter_by(pipeline_name="repeatable").count() == 2


def test_skipped_tasks_are_persisted_with_skipped_status():
    """Review-feedback gap: nothing previously asserted that a task
    the executor marks SKIPPED (because its dependency failed) is
    actually saved to the database with that status.
    """

    @task()
    def upstream_fails():
        raise RuntimeError("boom")

    @task(depends_on=[upstream_fails])
    def downstream(_result):
        return "should never run"

    pipeline = Pipeline("cascade_persisted", [upstream_fails, downstream])
    session = _fresh_session()

    run = run_and_record(pipeline, session)

    results_by_name = {tr.task_name: tr for tr in run.task_results}
    assert results_by_name["downstream"].status == "skipped"
    assert results_by_name["downstream"].error is not None

    # Re-fetch independently to prove it was actually written to the DB,
    # not just present on the in-memory object returned by run_and_record.
    reloaded = session.query(Run).filter_by(pipeline_name="cascade_persisted").one()
    reloaded_downstream = next(tr for tr in reloaded.task_results if tr.task_name == "downstream")
    assert reloaded_downstream.status == "skipped"


def test_ad_hoc_run_does_not_create_a_pipeline_row():
    """Review-feedback gap: run_and_record() must never silently
    auto-register a pipeline. Running an unregistered pipeline should
    leave the `pipelines` table empty, even though its Run/TaskResult
    rows are saved.
    """

    @task()
    def one():
        return 1

    pipeline = Pipeline("never_registered", [one])
    session = _fresh_session()

    run_and_record(pipeline, session)

    assert session.query(Run).filter_by(pipeline_name="never_registered").count() == 1
    assert session.query(PipelineModel).count() == 0


def test_phase1_engine_modules_have_no_database_imports():
    """Guard test: re-asserts the pure-engine boundary at the source
    level, not just by convention. If someone later imports sqlalchemy
    (or flowctl.storage/app) inside core/, this test fails loudly.
    """
    core_dir = Path(__file__).resolve().parent.parent / "src" / "flowctl" / "core"
    forbidden_prefixes = ("sqlalchemy", "flowctl.storage", "flowctl.app")

    for py_file in core_dir.glob("*.py"):
        tree = ast.parse(py_file.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [n.name for n in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [node.module] if node.module else []
            else:
                continue
            for name in names:
                assert not any(name.startswith(p) for p in forbidden_prefixes), (
                    f"{py_file.name} imports {name!r} -- the core engine must stay "
                    "pure and DB-free"
                )
