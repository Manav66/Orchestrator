"""Tests for the application-layer functions added specifically to
support the web dashboard (Phase 5): create_linear_job,
load_pipeline_for_row, run_registered, update_schedule, set_enabled,
get_pipeline, latest_task_statuses.
"""
from pathlib import Path

import pytest

from flowctl.app.application import (
    create_linear_job,
    get_pipeline,
    latest_task_details,
    latest_task_statuses,
    list_pipelines,
    list_runs,
    load_demo_content,
    load_pipeline_for_row,
    register,
    run_registered,
    set_enabled,
    update_schedule,
)
from flowctl.storage.db import get_engine, get_session_factory, init_db

FIXTURE = Path(__file__).parent / "fixtures" / "demo_pipeline.py"


def _fresh_session():
    engine = get_engine(":memory:")
    init_db(engine)
    return get_session_factory(engine)()


def test_create_linear_job_persists_commands_and_schedule():
    session = _fresh_session()
    row = create_linear_job(
        "backup_job", ["echo one", "echo two"], session, schedule="0 6 * * *"
    )
    assert row.commands is not None
    assert row.load_ref is None
    assert row.schedule == "0 6 * * *"
    assert row.enabled is True


def test_create_linear_job_rejects_empty_commands():
    session = _fresh_session()
    with pytest.raises(ValueError, match="at least one command"):
        create_linear_job("empty_job", [], session)


def test_create_linear_job_rejects_invalid_cron():
    session = _fresh_session()
    with pytest.raises(ValueError, match="not a valid cron expression"):
        create_linear_job("bad_cron_job", ["echo hi"], session, schedule="nonsense")


def test_load_pipeline_for_row_works_for_both_origins():
    session = _fresh_session()

    code_row = register(str(FIXTURE), session)
    linear_row = create_linear_job("linear_job", ["echo a", "echo b"], session)

    code_pipeline = load_pipeline_for_row(code_row)
    linear_pipeline = load_pipeline_for_row(linear_row)

    assert code_pipeline.name == "demo_pipeline"
    assert linear_pipeline.name == "linear_job"
    assert set(linear_pipeline.tasks.keys()) == {"step_1", "step_2"}


def test_run_registered_works_for_a_linear_job():
    session = _fresh_session()
    create_linear_job("linear_run_test", ["echo hello"], session)

    run = run_registered("linear_run_test", session)

    assert run.status == "success"
    assert run.task_results[0].result_repr == "'hello'"


def test_run_registered_raises_for_unknown_name():
    session = _fresh_session()
    with pytest.raises(ValueError, match="No registered pipeline"):
        run_registered("does_not_exist", session)


def test_update_schedule_sets_and_clears():
    session = _fresh_session()
    row = create_linear_job("sched_job", ["echo hi"], session)

    updated = update_schedule("sched_job", "0 6 * * *", session)
    assert updated.schedule == "0 6 * * *"

    cleared = update_schedule("sched_job", None, session)
    assert cleared.schedule is None


def test_update_schedule_rejects_invalid_cron():
    session = _fresh_session()
    create_linear_job("sched_job2", ["echo hi"], session)
    with pytest.raises(ValueError, match="not a valid cron expression"):
        update_schedule("sched_job2", "garbage", session)


def test_set_enabled_pauses_and_resumes():
    session = _fresh_session()
    create_linear_job("pausable_job", ["echo hi"], session)

    paused = set_enabled("pausable_job", False, session)
    assert paused.enabled is False

    resumed = set_enabled("pausable_job", True, session)
    assert resumed.enabled is True


def test_get_pipeline_returns_none_for_unknown_name():
    session = _fresh_session()
    assert get_pipeline(session, "nope") is None


def test_create_linear_job_refuses_to_clobber_a_code_defined_pipeline():
    """Regression test: a dashboard 'New Job' with the same name as an
    already code-registered pipeline used to silently overwrite it
    (clearing load_ref, replacing it with commands), converting a real
    branching pipeline into a linear job with no warning.
    """
    session = _fresh_session()
    register(str(FIXTURE), session)  # registers as "demo_pipeline" (the fixture's Pipeline name)

    with pytest.raises(ValueError, match="already a code-defined pipeline"):
        create_linear_job("demo_pipeline", ["echo hi"], session)

    # Confirm it's genuinely untouched.
    row = get_pipeline(session, "demo_pipeline")
    assert row.load_ref is not None
    assert row.commands is None


def test_register_refuses_to_clobber_a_linear_job():
    """Same protection in the other direction: `flowctl register`
    loading a code file whose Pipeline happens to share a name with an
    existing dashboard-created linear job must not silently convert
    that job into a code-defined pipeline.
    """
    session = _fresh_session()
    # The fixture file's Pipeline is named "demo_pipeline" -- create a
    # linear job with that exact name first, so register() collides with it.
    create_linear_job("demo_pipeline", ["echo hi"], session)

    with pytest.raises(ValueError, match="already registered as a dashboard-created linear job"):
        register(str(FIXTURE), session)

    # Confirm it's genuinely untouched.
    row = get_pipeline(session, "demo_pipeline")
    assert row.commands is not None
    assert row.load_ref is None


def test_latest_task_statuses_empty_until_first_run_then_reflects_it():
    session = _fresh_session()
    create_linear_job("status_job", ["echo one", "echo two"], session)

    assert latest_task_statuses(session, "status_job") == {}

    run_registered("status_job", session)

    statuses = latest_task_statuses(session, "status_job")
    assert statuses == {"step_1": "success", "step_2": "success"}


def test_load_demo_content_registers_and_runs_all_three_examples():
    # Assumes pytest is run from the repo root, same as every other
    # test here that references examples/ or tests/fixtures/ by a
    # relative-feeling path -- load_demo_content deliberately looks
    # relative to cwd (see its docstring) since it's meant to work the
    # same way `flowctl register examples/foo.py` already does.
    session = _fresh_session()

    loaded = load_demo_content(session)

    assert loaded == ["nightly_sales_report", "daily_data_pipeline", "log_analysis"]
    names = {p.name for p in list_pipelines(session)}
    assert names == set(loaded)

    for name in loaded:
        runs = list_runs(session, pipeline_name=name, limit=1)
        assert len(runs) == 1
        assert runs[0].status in {"success", "failed"}  # ran for real, not a no-op


def test_latest_task_details_empty_until_first_run_then_has_rich_fields():
    session = _fresh_session()
    create_linear_job("detail_job", ["echo one", "echo two"], session)

    assert latest_task_details(session, "detail_job") == {}

    run_registered("detail_job", session)

    details = latest_task_details(session, "detail_job")
    assert set(details.keys()) == {"step_1", "step_2"}
    for task_name, info in details.items():
        assert info["status"] == "success"
        assert info["attempts"] == 1
        assert isinstance(info["duration"], float) and info["duration"] >= 0
        assert info["error"] is None
        assert info["started_at"] is not None
        assert info["ended_at"] is not None
        assert info["run_id"] is not None
        # JSON-safety: every value must already be a plain type, not a
        # datetime -- this is what lets both Jinja's `tojson` filter and
        # the raw FastAPI JSON route hand these straight to json.dumps.
        for value in info.values():
            assert value is None or isinstance(value, (str, int, float))
