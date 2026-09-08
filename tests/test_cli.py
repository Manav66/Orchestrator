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
