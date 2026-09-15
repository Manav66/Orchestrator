"""End-to-end tests for the web dashboard, using FastAPI's TestClient.
Each test points FLOWCTL_DB_PATH at a fresh temp SQLite file, same
pattern as test_cli.py, so tests never touch a real ~/.flowctl/flowctl.db.
"""
import time

import pytest
from fastapi.testclient import TestClient

from flowctl.web.app import app

client = TestClient(app)


@pytest.fixture(autouse=True)
def _isolated_db(tmp_path, monkeypatch):
    monkeypatch.setenv("FLOWCTL_DB_PATH", str(tmp_path / "web_test.db"))


def test_homepage_loads_with_no_pipelines():
    response = client.get("/")
    assert response.status_code == 200
    assert "No pipelines registered yet" in response.text


def test_new_job_form_loads():
    response = client.get("/jobs/new")
    assert response.status_code == 200
    assert "Create a simple job" in response.text


def test_creating_a_linear_job_then_seeing_it_on_the_homepage():
    response = client.post(
        "/jobs/new",
        data={"name": "site_check", "commands": "echo checking\necho done", "schedule": ""},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"] == "/pipelines/site_check"

    home = client.get("/")
    assert "site_check" in home.text
    assert "linear job" in home.text


def test_creating_a_job_with_invalid_cron_shows_error():
    response = client.post(
        "/jobs/new",
        data={"name": "bad_job", "commands": "echo hi", "schedule": "garbage"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert "error=" in response.headers["location"]


def test_pipeline_detail_shows_dag_and_allows_running():
    client.post("/jobs/new", data={"name": "job_a", "commands": "echo a\necho b", "schedule": ""})

    detail = client.get("/pipelines/job_a")
    assert detail.status_code == 200
    assert "step_1" in detail.text
    assert "step_2" in detail.text
    assert "No runs yet" in detail.text

    run_response = client.post("/pipelines/job_a/run", follow_redirects=False)
    assert run_response.status_code == 303

    # "Run now" executes in a background thread on purpose (so the
    # dashboard's live poll can show running -> success/failed instead
    # of the request blocking until the run is already done) -- so the
    # very next request may briefly still show "running" for a fast
    # pipeline. Poll briefly rather than asserting instant completion.
    deadline = time.time() + 3
    detail_after = client.get("/pipelines/job_a")
    while "No runs yet" in detail_after.text or "running" in detail_after.text.lower():
        if time.time() > deadline:
            break
        time.sleep(0.05)
        detail_after = client.get("/pipelines/job_a")

    assert "No runs yet" not in detail_after.text
    assert "success" in detail_after.text


def test_pipeline_detail_404_for_unknown_pipeline():
    response = client.get("/pipelines/does_not_exist")
    assert response.status_code == 404


def test_updating_schedule_through_the_form():
    client.post("/jobs/new", data={"name": "job_b", "commands": "echo hi", "schedule": ""})

    client.post("/pipelines/job_b/schedule", data={"schedule": "0 6 * * *"})
    detail = client.get("/pipelines/job_b")
    assert "0 6 * * *" in detail.text


def test_toggle_pause_and_resume():
    client.post("/jobs/new", data={"name": "job_c", "commands": "echo hi", "schedule": ""})

    first_toggle = client.post("/pipelines/job_c/toggle", follow_redirects=False)
    assert first_toggle.status_code == 303
    paused_detail = client.get("/pipelines/job_c")
    assert "Resume" in paused_detail.text  # button now offers to resume, since it's paused

    client.post("/pipelines/job_c/toggle")
    resumed_detail = client.get("/pipelines/job_c")
    assert "Pause" in resumed_detail.text


def test_run_detail_page_shows_task_results():
    client.post("/jobs/new", data={"name": "job_d", "commands": "echo hello_world", "schedule": ""})
    client.post("/pipelines/job_d/run")

    # Run now is asynchronous (see the comment in the earlier DAG test)
    # -- wait for the background thread to finish rather than assuming
    # the run is already recorded the instant the request returns.
    deadline = time.time() + 3
    detail = client.get("/pipelines/job_d")
    while "#1" not in detail.text:
        if time.time() > deadline:
            break
        time.sleep(0.05)
        detail = client.get("/pipelines/job_d")
    assert "#1" in detail.text  # first run id linked in the history table

    run_detail = client.get("/runs/1")
    assert run_detail.status_code == 200
    while "hello_world" not in run_detail.text:
        if time.time() > deadline:
            break
        time.sleep(0.05)
        run_detail = client.get("/runs/1")
    assert "hello_world" in run_detail.text
    assert "step_1" in run_detail.text


def test_run_detail_404_for_unknown_run_id():
    response = client.get("/runs/999999")
    assert response.status_code == 404


def test_new_job_cannot_clobber_a_code_registered_pipeline():
    """Regression test for the review-flagged bug: submitting the
    'New Job' form with a name matching an already code-registered
    pipeline used to silently overwrite it.
    """
    from pathlib import Path

    from flowctl.app import application as app_layer

    # Register a real code pipeline first (register() derives the name
    # from the file's Pipeline object, which is "demo_pipeline" here).
    with app_layer.get_session() as session:
        app_layer.register(str(Path(__file__).parent / "fixtures" / "demo_pipeline.py"), session)

    response = client.post(
        "/jobs/new",
        data={"name": "demo_pipeline", "commands": "echo hi", "schedule": ""},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"].startswith("/jobs/new?error=")

    # Confirm the original code-defined pipeline is untouched.
    detail = client.get("/pipelines/demo_pipeline")
    assert "code-defined" in detail.text


def test_error_messages_with_special_characters_are_url_safe():
    """Regression test: error messages were interpolated into redirect
    URLs unescaped, so a message containing '&', '#', or spaces (any
    real Python exception message is likely to have spaces) would
    corrupt the query string. quote() fixes this.
    """
    response = client.post(
        "/jobs/new",
        data={"name": "weird_job", "commands": "echo hi", "schedule": "not a valid cron"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    location = response.headers["location"]
    # A raw space or '&' in the location would either break the query
    # string or silently truncate the message -- neither should happen.
    assert " " not in location
    assert location.count("?") == 1

    # Following the redirect should render the full, readable error text.
    follow_up = client.get(location)
    assert "not a valid cron expression" in follow_up.text
