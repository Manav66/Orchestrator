"""The application layer: the ONLY code allowed to know about both the
pure execution engine and the database.

CLI commands, the scheduler, and the web dashboard must all call
functions in this module -- never `flowctl.core` or `flowctl.storage`
directly. That rule is what keeps the engine's tests fast and
DB-free, and keeps persistence logic in exactly one place.

Session/commit convention: every public function in this module owns
and commits its own transaction. Callers should not call
session.commit() themselves after calling into here -- mixing "the
function commits" with "the caller commits" is how you get
double-commit bugs later, so this module always commits and callers
never do.
"""
from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator, Optional

from croniter import croniter
from sqlalchemy.orm import Session

from flowctl.app.loader import load_pipeline_from_file, make_load_ref
from flowctl.core.executor import Executor, PipelineResult
from flowctl.core.pipeline import Pipeline
from flowctl.storage.db import get_engine, get_session_factory, init_db
from flowctl.storage.models import Pipeline as PipelineModel
from flowctl.storage.models import Run, TaskResult

# Sentinel meaning "the caller didn't specify a schedule at all", distinct
# from an explicit `schedule=None`, which means "clear the schedule on
# purpose". Without this distinction, re-registering an already-scheduled
# pipeline without repeating --schedule would silently wipe it -- exactly
# the bug flagged in review.
UNSET = object()


@contextmanager
def get_session() -> Iterator[Session]:
    """The one place that knows how to open (and close) a DB session.

    CLI/scheduler/dashboard code should call this instead of importing
    flowctl.storage.db directly, so the "three doors never touch
    storage themselves" rule is actually true, not just true of the
    business logic they call.
    """
    engine = get_engine()
    init_db(engine)
    session = get_session_factory(engine)()
    try:
        yield session
    finally:
        session.close()


def run_and_record(pipeline: Pipeline, session: Session, *, max_workers: int = 4) -> Run:
    """Execute a pipeline now and persist the run + per-task results.

    This function is the seam between the pure engine and storage:
    `Executor().execute()` is called exactly as it would be in a
    DB-free unit test, and only the *result* that comes back is ever
    touched by SQLAlchemy. The engine itself never imports storage.

    This runs regardless of whether `pipeline` has ever been
    registered -- registration and running are independent (see
    `register` below). This function commits its own session.
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


def run_from_file(
    file_path: str,
    session: Session,
    *,
    attr: Optional[str] = None,
    max_workers: int = 4,
) -> Run:
    """Load a Pipeline from a .py file and run_and_record() it.

    This is what `flowctl run <file>` calls. It works whether or not
    the pipeline has ever been registered.
    """
    pipeline = load_pipeline_from_file(file_path, attr)
    return run_and_record(pipeline, session, max_workers=max_workers)


def register(
    file_path: str,
    session: Session,
    *,
    attr: Optional[str] = None,
    schedule: Optional[str] = UNSET,  # type: ignore[assignment]
) -> PipelineModel:
    """Register a pipeline as a named, schedulable entity.

    Loads the pipeline from `file_path` (to validate it actually
    parses and to read its name), computes a stable load reference
    (absolute path [+ "::attr"]), and upserts a row in the `pipelines`
    table keyed by the pipeline's name. Registering an already-known
    pipeline updates its load_ref, but the schedule is only touched if
    `schedule` was actually passed:
      - omit `schedule` entirely -> existing schedule is left as-is
        (fixes the bug where re-registering without --schedule wiped
        an already-configured schedule)
      - `schedule=None` -> explicitly clears the schedule
      - `schedule="0 6 * * *"` -> sets/replaces the schedule

    This does NOT run the pipeline. Running and registering are
    independent operations (see module docstring / run_from_file).
    Commits its own session.
    """
    if schedule is not UNSET and schedule is not None and not croniter.is_valid(schedule):
        raise ValueError(
            f"{schedule!r} is not a valid cron expression, e.g. '0 6 * * *' for daily at 6am"
        )

    pipeline = load_pipeline_from_file(file_path, attr)
    load_ref = make_load_ref(file_path, attr)

    existing = session.query(PipelineModel).filter_by(name=pipeline.name).one_or_none()
    if existing is None:
        existing = PipelineModel(
            name=pipeline.name,
            schedule=None if schedule is UNSET else schedule,
            load_ref=load_ref,
        )
        session.add(existing)
    else:
        existing.load_ref = load_ref
        if schedule is not UNSET:
            existing.schedule = schedule

    session.commit()
    session.refresh(existing)
    return existing


def list_pipelines(session: Session) -> list[PipelineModel]:
    """All registered pipelines, alphabetically. Read-only, no commit."""
    return session.query(PipelineModel).order_by(PipelineModel.name).all()


def list_runs(
    session: Session, *, pipeline_name: Optional[str] = None, limit: int = 20
) -> list[Run]:
    """Most recent runs, optionally filtered to one pipeline name.

    Includes ad-hoc runs (pipelines that were never registered), since
    Run rows are keyed by plain pipeline_name, not a foreign key into
    the pipelines table. Read-only, no commit.
    """
    query = session.query(Run).order_by(Run.id.desc())
    if pipeline_name is not None:
        query = query.filter_by(pipeline_name=pipeline_name)
    return query.limit(limit).all()


def get_run(session: Session, run_id: int) -> Optional[Run]:
    """Fetch a single run (with its task results) by id. Read-only."""
    return session.get(Run, run_id)
