"""The scheduler: polls registered pipelines' cron schedules and fires
any that are due -- by calling the exact same run_and_record() the CLI
uses. The scheduler has no executor of its own; it is just another
caller of the one run path, same as the CLI and (later) the dashboard.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Set

from croniter import croniter

from flowctl.app import application as app_layer


def _default_clock() -> datetime:
    return datetime.now(timezone.utc)


class Scheduler:
    """Polls registered pipelines and fires any that are due.

    A pipeline is "due" on a given tick when its cron schedule's most
    recent scheduled fire time (as of `clock()`) is more recent than
    the last time this Scheduler last fired it.

    The first time a given pipeline is seen, "last fired" is seeded
    from that pipeline's most recent recorded Run (if any), so a
    scheduler restart does not immediately re-fire everything that
    already ran under a previous scheduler process -- but a pipeline
    whose schedule genuinely came due while no scheduler was running
    at all will still fire on the next tick.

    `clock` is injectable specifically so tests can control "now"
    deterministically instead of depending on real wall-clock time.
    """

    def __init__(
        self,
        tick_seconds: float = 5.0,
        clock: Callable[[], datetime] = _default_clock,
    ):
        self.tick_seconds = tick_seconds
        self.clock = clock
        self._last_fired: Dict[str, datetime] = {}
        self._seeded: Set[str] = set()

    def _seed_last_fired(self, name: str) -> None:
        if name in self._seeded:
            return
        with app_layer.get_session() as session:
            recent = app_layer.list_runs(session, pipeline_name=name, limit=1)
            self._last_fired[name] = recent[0].started_at if recent else self.clock()
        self._seeded.add(name)

    def tick(self) -> List[dict]:
        """Check all registered pipelines once; fire any that are due.

        Returns a list of {"pipeline": name, "run_id": int, "status": str}
        dicts, one per pipeline fired this tick (empty if none were due).
        Only plain values are returned, never ORM objects, since the
        session used to fire a pipeline is closed before tick() returns.
        """
        now = self.clock()
        fired: List[dict] = []

        with app_layer.get_session() as session:
            pipelines = app_layer.list_pipelines(session)

        for pipeline_row in pipelines:
            if not pipeline_row.schedule or not pipeline_row.enabled:
                continue

            self._seed_last_fired(pipeline_row.name)

            try:
                prev_fire = croniter(pipeline_row.schedule, now).get_prev(datetime)
            except Exception:
                # Malformed schedule shouldn't crash the whole loop --
                # register() already validates new schedules, but this
                # guards against, e.g., manually edited DB rows.
                continue

            if prev_fire <= self._last_fired[pipeline_row.name]:
                continue

            self._last_fired[pipeline_row.name] = prev_fire

            pipeline_obj = app_layer.load_pipeline_for_row(pipeline_row)
            with app_layer.get_session() as session:
                run = app_layer.run_and_record(pipeline_obj, session)
                fired.append(
                    {"pipeline": pipeline_row.name, "run_id": run.id, "status": run.status}
                )

        return fired

    def run_forever(self, *, max_ticks: Optional[int] = None) -> None:
        """Block, ticking every `tick_seconds`, until interrupted.

        `max_ticks` exists for tests, so they can run a bounded number
        of ticks and return instead of looping forever. Production
        usage (`flowctl scheduler start`) calls this with no limit and
        relies on Ctrl+C / process shutdown to stop it.
        """
        ticks = 0
        while max_ticks is None or ticks < max_ticks:
            self.tick()
            ticks += 1
            if max_ticks is None or ticks < max_ticks:
                time.sleep(self.tick_seconds)
