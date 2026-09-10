"""Tests for the scheduler: cron-due detection, and firing pipelines
through the exact same run_and_record() the CLI uses.

These use an injectable fake clock instead of real wall-clock time,
so tests are deterministic and fast rather than depending on sleeping
past real cron boundaries.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from flowctl.app import application as app_layer
from flowctl.scheduler.scheduler import Scheduler
from flowctl.storage.models import Run

FIXTURE = Path(__file__).parent / "fixtures" / "demo_pipeline.py"


class FakeClock:
    """A callable clock whose value we control explicitly in tests."""

    def __init__(self, start: datetime):
        self.current = start

    def __call__(self) -> datetime:
        return self.current

    def advance(self, **kwargs) -> None:
        self.current += timedelta(**kwargs)


@pytest.fixture(autouse=True)
def _isolated_db(tmp_path, monkeypatch):
    monkeypatch.setenv("FLOWCTL_DB_PATH", str(tmp_path / "scheduler_test.db"))


def _register(schedule: str = "* * * * *"):
    with app_layer.get_session() as session:
        return app_layer.register(str(FIXTURE), session, schedule=schedule)


def test_tick_does_not_fire_a_freshly_registered_pipeline_immediately():
    _register()
    clock = FakeClock(datetime(2026, 1, 1, 12, 0, 30, tzinfo=timezone.utc))
    scheduler = Scheduler(clock=clock)

    fired = scheduler.tick()

    assert fired == []


def test_tick_fires_once_a_cron_boundary_is_crossed():
    _register()
    clock = FakeClock(datetime(2026, 1, 1, 12, 0, 30, tzinfo=timezone.utc))
    scheduler = Scheduler(clock=clock)

    scheduler.tick()  # first tick: seeds last_fired, nothing due yet
    clock.advance(minutes=1)  # now 12:01:30 -- crossed the 12:01:00 boundary
    fired = scheduler.tick()

    assert len(fired) == 1
    assert fired[0]["pipeline"] == "demo_pipeline"
    assert fired[0]["status"] == "success"

    with app_layer.get_session() as session:
        runs = app_layer.list_runs(session, pipeline_name="demo_pipeline")
        assert len(runs) == 1


def test_tick_does_not_refire_within_the_same_minute():
    _register()
    clock = FakeClock(datetime(2026, 1, 1, 12, 0, 30, tzinfo=timezone.utc))
    scheduler = Scheduler(clock=clock)

    scheduler.tick()
    clock.advance(minutes=1)
    scheduler.tick()
    fired_again = scheduler.tick()  # same clock value as the previous tick

    assert fired_again == []


def test_scheduler_seeds_from_most_recent_run_so_restart_does_not_refire_old_work():
    """Simulates a scheduler restart: a Run already exists in the DB
    from before this Scheduler instance existed, and it must not
    immediately re-fire work that's already accounted for -- but it
    must still fire a genuinely new boundary once time moves past it.
    """
    _register()
    with app_layer.get_session() as session:
        session.add(
            Run(
                pipeline_name="demo_pipeline",
                status="success",
                started_at=datetime(2026, 1, 1, 12, 0, 10, tzinfo=timezone.utc),
                ended_at=datetime(2026, 1, 1, 12, 0, 10, tzinfo=timezone.utc),
            )
        )
        session.commit()

    clock = FakeClock(datetime(2026, 1, 1, 12, 0, 20, tzinfo=timezone.utc))
    scheduler = Scheduler(clock=clock)

    fired = scheduler.tick()
    assert fired == []  # the 12:00:00 boundary already happened before the recorded run

    clock.advance(minutes=1)
    fired = scheduler.tick()
    assert len(fired) == 1  # the 12:01:00 boundary is genuinely new


def test_tick_skips_pipelines_with_no_schedule():
    with app_layer.get_session() as session:
        app_layer.register(str(FIXTURE), session)  # no schedule passed

    clock = FakeClock(datetime(2026, 1, 1, 12, 1, 30, tzinfo=timezone.utc))
    scheduler = Scheduler(clock=clock)

    assert scheduler.tick() == []


def test_run_forever_stops_after_max_ticks():
    _register()
    clock = FakeClock(datetime(2026, 1, 1, 12, 0, 30, tzinfo=timezone.utc))
    scheduler = Scheduler(clock=clock, tick_seconds=0)

    scheduler.run_forever(max_ticks=3)  # must return, not hang
