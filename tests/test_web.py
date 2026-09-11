"""End-to-end tests for the web dashboard, using FastAPI's TestClient.
Each test points FLOWCTL_DB_PATH at a fresh temp SQLite file, same
pattern as test_cli.py, so tests never touch a real ~/.flowctl/flowctl.db.
"""
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

    detail = client.get("/pipelines/job_d")
    assert "#1" in detail.text  # first run id linked in the history table

    run_detail = client.get("/runs/1")
    assert run_detail.status_code == 200
    assert "hello_world" in run_detail.text
    assert "step_1" in run_detail.text


def test_run_detail_404_for_unknown_run_id():
    response = client.get("/runs/999999")
    assert response.status_code == 404
