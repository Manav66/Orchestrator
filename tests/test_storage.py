"""Tests for the SQLAlchemy models and table setup, in isolation from
the application layer or the engine.
"""
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from flowctl.storage.db import get_engine, get_session_factory, init_db
from flowctl.storage.models import Run, TaskResult


def _fresh_session():
    engine = get_engine(":memory:")
    init_db(engine)
    return get_session_factory(engine)()


def test_tables_are_created_and_run_can_be_saved_with_task_results():
    session = _fresh_session()
    now = datetime.now(timezone.utc)

    run = Run(
        pipeline_name="demo",
        status="success",
        started_at=now,
        ended_at=now,
    )
    run.task_results.append(
        TaskResult(
            task_name="fetch",
            status="success",
            attempts=1,
            started_at=now,
            ended_at=now,
            result_repr="{'orders': 42}",
        )
    )
    session.add(run)
    session.commit()

    fetched = session.query(Run).filter_by(pipeline_name="demo").one()
    assert fetched.status == "success"
    assert len(fetched.task_results) == 1
    assert fetched.task_results[0].task_name == "fetch"


def test_multiple_runs_are_kept_separate():
    session = _fresh_session()
    now = datetime.now(timezone.utc)

    for i in range(2):
        session.add(
            Run(pipeline_name=f"pipeline_{i}", status="success", started_at=now, ended_at=now)
        )
    session.commit()

    assert session.query(Run).count() == 2


def test_datetime_stays_timezone_aware_across_separate_sessions_on_a_real_file():
    """Regression test for a real bug: SQLite silently drops tzinfo on
    a plain DateTime(timezone=True) column once a row is reloaded
    through a *different* session/connection than the one that wrote
    it (same-session reads are misleadingly fine because of the
    identity map). This only shows up with a real file-backed DB, not
    ":memory:", so this test deliberately uses a temp file.
    """
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "tz_check.db"
        write_engine = get_engine(db_path)
        init_db(write_engine)
        write_session = get_session_factory(write_engine)()

        aware_time = datetime(2026, 1, 1, 12, 0, 10, tzinfo=timezone.utc)
        write_session.add(
            Run(pipeline_name="tz_check", status="success", started_at=aware_time, ended_at=aware_time)
        )
        write_session.commit()
        write_session.close()

        # A brand new engine + session, simulating a completely separate
        # process reading the same file.
        read_engine = get_engine(db_path)
        read_session = get_session_factory(read_engine)()
        reloaded = read_session.query(Run).filter_by(pipeline_name="tz_check").one()

        assert reloaded.started_at.tzinfo is not None
        assert reloaded.started_at == aware_time
