"""The application layer: the ONLY code allowed to know about both the
pure execution engine and the database.

CLI commands, the scheduler, and the web dashboard must all call
functions in this module -- never `flowctl.core` or `flowctl.storage`
directly. That rule is what keeps the engine's tests fast and
DB-free, and keeps persistence logic in exactly one place.

Phase 2 introduces:
  - run_and_record(): execute a pipeline right now and persist the run,
    whether or not the pipeline has ever been registered.

Registration (`register()`), listing, and log-retrieval helpers are
built out properly in Phase 3, once the CLI needs them.
"""
from __future__ import annotations

from sqlalchemy.orm import Session

from flowctl.core.executor import Executor, PipelineResult
from flowctl.core.pipeline import Pipeline
from flowctl.storage.models import Run, TaskResult


def run_and_record(pipeline: Pipeline, session: Session, *, max_workers: int = 4) -> Run:
    """Execute a pipeline now and persist the run + per-task results.

    This function is the seam between the pure engine and storage:
    `Executor().execute()` is called exactly as it would be in a
    DB-free unit test, and only the *result* that comes back is ever
    touched by SQLAlchemy. The engine itself never imports storage.
    """
    result: PipelineResult = Executor(max_workers=max_workers).execute(pipeline)

    run = Run(
        pipeline_name=pipeline.name,
        status=result.status.value,
        started_at=result.started_at,
        ended_at=result.ended_at,
    )
    for task_name, outcome in result.task_outcomes.items():
        run.task_results.append(
            TaskResult(
                task_name=task_name,
                status=outcome.status.value,
                attempts=outcome.attempts,
                started_at=outcome.started_at,
                ended_at=outcome.ended_at,
                result_repr=repr(outcome.result) if outcome.result is not None else None,
                error=outcome.error,
            )
        )

    session.add(run)
    session.commit()
    session.refresh(run)
    return run
