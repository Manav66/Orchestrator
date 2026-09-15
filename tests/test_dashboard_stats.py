"""Tests for the homepage stats/chart helpers: dashboard_stats() and
daily_run_counts(), plus list_runs()'s status filter.
"""
from datetime import datetime, timedelta, timezone

from flowctl.app.application import dashboard_stats, daily_run_counts, list_runs
from flowctl.core.pipeline import Pipeline
from flowctl.core.task import task
from flowctl.storage.db import get_engine, get_session_factory, init_db
from flowctl.storage.models import Run


def _fresh_session():
    engine = get_engine(":memory:")
    init_db(engine)
    return get_session_factory(engine)()


def _add_run(session, *, pipeline_name="p", status="success", started_at=None):
    now = started_at or datetime.now(timezone.utc)
    session.add(Run(pipeline_name=pipeline_name, status=status, started_at=now, ended_at=now))
    session.commit()


def test_dashboard_stats_on_empty_database():
    session = _fresh_session()
    stats = dashboard_stats(session)
    assert stats["total_pipelines"] == 0
    assert stats["total_runs"] == 0
    assert stats["runs_today"] == 0
    assert stats["success_rate"] is None
    assert stats["currently_running"] == 0


def test_dashboard_stats_computes_success_rate_and_running_count():
    session = _fresh_session()
    _add_run(session, status="success")
    _add_run(session, status="success")
    _add_run(session, status="failed")
    _add_run(session, status="running")

    stats = dashboard_stats(session)
    assert stats["total_runs"] == 4
    assert stats["currently_running"] == 1
    # 2 success out of 3 completed (running excluded) = 66.7%
    assert stats["success_rate"] == 66.7


def test_dashboard_stats_counts_runs_today_correctly():
    session = _fresh_session()
    now = datetime.now(timezone.utc)
    _add_run(session, started_at=now)  # today
    _add_run(session, started_at=now - timedelta(days=2))  # not today

    stats = dashboard_stats(session)
    assert stats["runs_today"] == 1
    assert stats["total_runs"] == 2


def test_daily_run_counts_includes_zero_days_and_buckets_correctly():
    session = _fresh_session()
    now = datetime.now(timezone.utc)
    _add_run(session, status="success", started_at=now)
    _add_run(session, status="failed", started_at=now)
    _add_run(session, status="success", started_at=now - timedelta(days=1))

    counts = daily_run_counts(session, days=3)
    assert len(counts) == 3  # continuous timeline, even with a gapless day in between
    today = counts[-1]
    yesterday = counts[-2]
    two_days_ago = counts[-3]

    assert today["success"] == 1
    assert today["failed"] == 1
    assert yesterday["success"] == 1
    assert yesterday["failed"] == 0
    assert two_days_ago == {"date": two_days_ago["date"], "success": 0, "failed": 0}


def test_list_runs_filters_by_status():
    @task()
    def ok():
        return 1

    pipeline = Pipeline("status_filter_test", [ok])
    session = _fresh_session()
    from flowctl.app.application import run_and_record

    run_and_record(pipeline, session)

    successes = list_runs(session, status="success")
    failures = list_runs(session, status="failed")
    assert len(successes) == 1
    assert len(failures) == 0
