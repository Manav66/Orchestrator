"""Tests for the SQLAlchemy models and table setup, in isolation from
the application layer or the engine.
"""
from datetime import datetime, timezone

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
