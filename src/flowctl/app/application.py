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

import json
from contextlib import contextmanager
from typing import Iterator, List, Optional

from croniter import croniter
from sqlalchemy.orm import Session

from flowctl.app.loader import (
    build_linear_pipeline,
    load_pipeline_from_file,
    load_pipeline_from_ref,
    make_load_ref,
)
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


def create_linear_job(
    name: str,
    commands: List[str],
    session: Session,
    *,
    schedule: Optional[str] = None,
) -> PipelineModel:
    """Create (or replace) a simple, linear job from the dashboard.

    Deliberately limited on purpose (see PROJECT_CONTEXT.md's scoping
    section): a linear job is an ordered list of shell commands with an
    optional schedule -- never a branching DAG. Real branching
    pipelines are always code-defined via `register`. Under the hood
    this is stored as a Pipeline row with `commands` set instead of
    `load_ref`; at run time it becomes a real Pipeline of Tasks via
    build_linear_pipeline(), so it runs through the exact same executor
    as any code-defined pipeline. Commits its own session.
    """
    if not commands:
        raise ValueError("A linear job needs at least one command")
    if schedule is not None and not croniter.is_valid(schedule):
        raise ValueError(
            f"{schedule!r} is not a valid cron expression, e.g. '0 6 * * *' for daily at 6am"
        )

    commands_json = json.dumps(commands)
    existing = session.query(PipelineModel).filter_by(name=name).one_or_none()
    if existing is None:
        existing = PipelineModel(
            name=name, schedule=schedule, load_ref=None, commands=commands_json, enabled=True
        )
        session.add(existing)
    else:
        existing.load_ref = None
        existing.commands = commands_json
        existing.schedule = schedule

    session.commit()
    session.refresh(existing)
    return existing


def load_pipeline_for_row(pipeline_row: PipelineModel) -> Pipeline:
    """Turn a Pipeline DB row back into a real, runnable core Pipeline,
    regardless of whether it's code-defined or a UI-created linear job.

    This is the crux of the hybrid model: both origins converge on the
    same core.Pipeline representation and the same executor -- there
    is no second code path for "UI jobs" anywhere below this function.
    """
    if pipeline_row.load_ref:
        return load_pipeline_from_ref(pipeline_row.load_ref)
    if pipeline_row.commands:
        return build_linear_pipeline(pipeline_row.name, json.loads(pipeline_row.commands))
    raise ValueError(
        f"Pipeline {pipeline_row.name!r} has neither a load_ref nor commands configured"
    )


def run_registered(name: str, session: Session, *, max_workers: int = 4) -> Run:
    """Run an already-registered pipeline (code-defined or linear job)
    by name, without needing to know its file path or commands. This is
    what the dashboard's "Run now" button calls.
    """
    pipeline_row = session.query(PipelineModel).filter_by(name=name).one_or_none()
    if pipeline_row is None:
        raise ValueError(f"No registered pipeline named {name!r}")
    pipeline = load_pipeline_for_row(pipeline_row)
    return run_and_record(pipeline, session, max_workers=max_workers)


def update_schedule(name: str, schedule: Optional[str], session: Session) -> PipelineModel:
    """Set (or clear, with schedule=None) a registered pipeline's cron
    schedule directly, without needing to reload/revalidate its source.
    Used by the dashboard's schedule-edit control. Commits its own
    session.
    """
    if schedule is not None and not croniter.is_valid(schedule):
        raise ValueError(
            f"{schedule!r} is not a valid cron expression, e.g. '0 6 * * *' for daily at 6am"
        )
    pipeline_row = session.query(PipelineModel).filter_by(name=name).one_or_none()
    if pipeline_row is None:
        raise ValueError(f"No registered pipeline named {name!r}")
    pipeline_row.schedule = schedule
    session.commit()
    session.refresh(pipeline_row)
    return pipeline_row


def set_enabled(name: str, enabled: bool, session: Session) -> PipelineModel:
    """Pause (enabled=False) or resume (enabled=True) a pipeline without
    touching its schedule text. The scheduler skips disabled pipelines.
    Used by the dashboard's pause/resume control. Commits its own
    session.
    """
    pipeline_row = session.query(PipelineModel).filter_by(name=name).one_or_none()
    if pipeline_row is None:
        raise ValueError(f"No registered pipeline named {name!r}")
    pipeline_row.enabled = enabled
    session.commit()
    session.refresh(pipeline_row)
    return pipeline_row


def get_pipeline(session: Session, name: str) -> Optional[PipelineModel]:
    """Fetch a single registered pipeline by name. Read-only."""
    return session.query(PipelineModel).filter_by(name=name).one_or_none()


def latest_task_statuses(session: Session, pipeline_name: str) -> dict[str, str]:
    """task_name -> status, from the most recent run of a pipeline.

    Used to color a pipeline's DAG diagram by last-known status. Empty
    dict if the pipeline has never been run. Read-only.
    """
    recent = list_runs(session, pipeline_name=pipeline_name, limit=1)
    if not recent:
        return {}
    return {tr.task_name: tr.status for tr in recent[0].task_results}


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
