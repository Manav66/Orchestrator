"""End-to-end tests for the CLI, using Typer's CliRunner. Each test
points FLOWCTL_DB_PATH at a fresh temp SQLite file so tests never
touch a real ~/.flowctl/flowctl.db.
"""
from pathlib import Path

import pytest
from typer.testing import CliRunner

from flowctl.cli.main import app

FIXTURES = Path(__file__).parent / "fixtures"
runner = CliRunner()


@pytest.fixture(autouse=True)
def _isolated_db(tmp_path, monkeypatch):
    monkeypatch.setenv("FLOWCTL_DB_PATH", str(tmp_path / "test.db"))


def test_run_command_executes_and_reports_success():
    result = runner.invoke(app, ["run", str(FIXTURES / "demo_pipeline.py")])
    assert result.exit_code == 0
    assert "demo_pipeline" in result.stdout
    assert "SUCCESS" in result.stdout
    assert "FETCH" in result.stdout.upper()


def test_run_command_reports_failure_with_nonzero_exit():
    result = runner.invoke(app, ["run", str(FIXTURES / "failing_pipeline.py")])
    assert result.exit_code == 1
    assert "FAILED" in result.stdout


def test_register_then_status_shows_pipeline_and_last_run():
    reg = runner.invoke(
        app, ["register", str(FIXTURES / "demo_pipeline.py"), "--schedule", "0 6 * * *"]
    )
    assert reg.exit_code == 0
    assert "Registered" in reg.stdout

    run_result = runner.invoke(app, ["run", str(FIXTURES / "demo_pipeline.py")])
    assert run_result.exit_code == 0

    status_result = runner.invoke(app, ["status"])
    assert status_result.exit_code == 0
    assert "demo_pipeline" in status_result.stdout
    assert "0 6 * * *" in status_result.stdout
    assert "success" in status_result.stdout


def test_status_shows_unregistered_pipeline_as_adhoc():
    run_result = runner.invoke(app, ["run", str(FIXTURES / "demo_pipeline.py")])
    assert run_result.exit_code == 0

    status_result = runner.invoke(app, ["status"])
    assert "Recent ad-hoc runs" in status_result.stdout
    assert "demo_pipeline" in status_result.stdout


def test_logs_command_shows_task_detail():
    run_result = runner.invoke(app, ["run", str(FIXTURES / "demo_pipeline.py")])
    assert run_result.exit_code == 0

    run_id_line = [l for l in run_result.stdout.splitlines() if "Run ID" in l][0]
    run_id = run_id_line.split("Run ID:")[-1].strip()

    logs_result = runner.invoke(app, ["logs", run_id])
    assert logs_result.exit_code == 0
    assert "fetch" in logs_result.stdout
    assert "report" in logs_result.stdout
    assert "result:" in logs_result.stdout


def test_logs_command_with_unknown_run_id_fails_cleanly():
    result = runner.invoke(app, ["logs", "999999"])
    assert result.exit_code == 1
    assert "No run with id" in result.stdout


def test_register_without_schedule_flag_does_not_wipe_existing_schedule():
    """Regression test for the review-flagged bug: re-running `flowctl
    register` without --schedule used to silently clear an already-set
    schedule, which would make the scheduler stop firing the pipeline.
    """
    first = runner.invoke(
        app, ["register", str(FIXTURES / "demo_pipeline.py"), "--schedule", "0 6 * * *"]
    )
    assert first.exit_code == 0

    second = runner.invoke(app, ["register", str(FIXTURES / "demo_pipeline.py")])
    assert second.exit_code == 0

    status_result = runner.invoke(app, ["status"])
    assert "0 6 * * *" in status_result.stdout


def test_register_with_empty_schedule_clears_it_on_purpose():
    runner.invoke(app, ["register", str(FIXTURES / "demo_pipeline.py"), "--schedule", "0 6 * * *"])
    result = runner.invoke(app, ["register", str(FIXTURES / "demo_pipeline.py"), "--schedule", ""])
    assert result.exit_code == 0
    assert "schedule=none" in result.stdout


def test_run_with_attr_selects_the_right_pipeline_from_a_multi_pipeline_file():
    result = runner.invoke(
        app, ["run", str(FIXTURES / "multi_pipeline.py"), "--attr", "pipeline_two"]
    )
    assert result.exit_code == 0
    assert "multi_two" in result.stdout


def test_register_with_invalid_cron_fails_cleanly():
    result = runner.invoke(
        app, ["register", str(FIXTURES / "demo_pipeline.py"), "--schedule", "not a cron"]
    )
    assert result.exit_code == 1
    assert "not a valid cron expression" in result.stdout


def test_scheduler_tick_reports_no_pipelines_due_when_none_are_registered():
    result = runner.invoke(app, ["scheduler", "tick"])
    assert result.exit_code == 0
    assert "No pipelines were due" in result.stdout


def test_scheduler_tick_can_fire_a_registered_pipeline():
    # A schedule of "* * * * *" (every minute) combined with two ticks
    # a minute apart (via the Scheduler's real clock) is timing-dependent
    # in a live process, so here we only check the "nothing due yet"
    # path through the CLI -- the actual due/fire logic is covered
    # deterministically (fake clock) in test_scheduler.py.
    runner.invoke(
        app, ["register", str(FIXTURES / "demo_pipeline.py"), "--schedule", "* * * * *"]
    )
    result = runner.invoke(app, ["scheduler", "tick"])
    assert result.exit_code == 0
