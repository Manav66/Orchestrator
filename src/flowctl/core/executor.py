"""The execution engine: runs a Pipeline's tasks in dependency order.

This is the "pure" heart of the whole project. It:
  - walks the execution plan produced by Pipeline.execution_plan()
  - runs each level's tasks concurrently (thread pool), since tasks in
    the same level have no dependency on each other
  - retries a failing task according to its own retry settings
  - skips any task whose dependencies did not all succeed, and
    cascades that skip to further dependents automatically
  - returns a plain, in-memory PipelineResult object

Deliberately, this module imports nothing related to a database, a
file, or any other form of I/O. Persistence is the job of the
application layer (flowctl.app), which calls this module and then
decides what to do with the result it gets back. Keeping that
boundary is what lets this module's tests run in total isolation,
fast and deterministic, with nothing to mock.
"""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

from flowctl.core.pipeline import Pipeline
from flowctl.core.task import Task


class TaskStatus(str, Enum):
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class PipelineStatus(str, Enum):
    SUCCESS = "success"
    FAILED = "failed"


@dataclass
class TaskOutcome:
    """Result of running (or skipping) a single task."""

    task_name: str
    status: TaskStatus
    attempts: int
    started_at: datetime
    ended_at: datetime
    result: Any = None
    error: Optional[str] = None

    @property
    def duration_seconds(self) -> float:
        return (self.ended_at - self.started_at).total_seconds()


@dataclass
class PipelineResult:
    """Result of running an entire pipeline once."""

    pipeline_name: str
    status: PipelineStatus
    started_at: datetime
    ended_at: datetime
    task_outcomes: Dict[str, TaskOutcome] = field(default_factory=dict)

    @property
    def duration_seconds(self) -> float:
        return (self.ended_at - self.started_at).total_seconds()


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _run_task_with_retries(task_obj: Task, dep_results: List[Any]) -> TaskOutcome:
    """Run one task, retrying on failure per its own retry settings."""
    max_attempts = task_obj.retries + 1
    started_at = _now()
    last_error: Optional[str] = None

    for attempt in range(1, max_attempts + 1):
        try:
            result = task_obj.func(*dep_results)
            return TaskOutcome(
                task_name=task_obj.name,
                status=TaskStatus.SUCCESS,
                attempts=attempt,
                started_at=started_at,
                ended_at=_now(),
                result=result,
            )
        except Exception as exc:  # noqa: BLE001 - deliberately broad: any task can fail
            last_error = f"{type(exc).__name__}: {exc}"
            if attempt < max_attempts and task_obj.retry_delay > 0:
                time.sleep(task_obj.retry_delay)

    return TaskOutcome(
        task_name=task_obj.name,
        status=TaskStatus.FAILED,
        attempts=max_attempts,
        started_at=started_at,
        ended_at=_now(),
        error=last_error,
    )


class Executor:
    """Runs a Pipeline's tasks according to its dependency graph."""

    def __init__(self, max_workers: int = 4):
        self.max_workers = max_workers

    def execute(self, pipeline: Pipeline) -> PipelineResult:
        started_at = _now()
        levels = pipeline.execution_plan()
        outcomes: Dict[str, TaskOutcome] = {}

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            for level in levels:
                to_run: List[Task] = []
                for t in level:
                    if self._dependencies_ok(t, outcomes):
                        to_run.append(t)
                    else:
                        # A dependency failed or was skipped upstream:
                        # cascade the skip instead of running this task.
                        now = _now()
                        outcomes[t.name] = TaskOutcome(
                            task_name=t.name,
                            status=TaskStatus.SKIPPED,
                            attempts=0,
                            started_at=now,
                            ended_at=now,
                            error="skipped: one or more dependencies did not succeed",
                        )

                if not to_run:
                    continue

                futures = {
                    pool.submit(
                        _run_task_with_retries,
                        t,
                        [outcomes[dep.name].result for dep in t.depends_on],
                    ): t
                    for t in to_run
                }
                for future, t in futures.items():
                    outcomes[t.name] = future.result()

        ended_at = _now()
        overall = (
            PipelineStatus.SUCCESS
            if all(o.status == TaskStatus.SUCCESS for o in outcomes.values())
            else PipelineStatus.FAILED
        )
        return PipelineResult(
            pipeline_name=pipeline.name,
            status=overall,
            started_at=started_at,
            ended_at=ended_at,
            task_outcomes=outcomes,
        )

    @staticmethod
    def _dependencies_ok(t: Task, outcomes: Dict[str, TaskOutcome]) -> bool:
        return all(
            dep.name in outcomes and outcomes[dep.name].status == TaskStatus.SUCCESS
            for dep in t.depends_on
        )
