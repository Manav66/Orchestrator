"""SQLAlchemy models: the durable record of pipelines and their runs.

Three tables:
  - Pipeline:   a named, registered pipeline (schedule + how to load its
                code). A pipeline does NOT need a row here to be run
                ad-hoc via `run_and_record` -- registration is what
                promotes it to a named, schedulable entity (Phase 3+).
  - Run:        one execution of a pipeline, registered or not. Stores
                overall status and timing.
  - TaskResult: one task's outcome within a specific Run. Many rows
                per Run, one per task that was attempted or skipped.

This module only describes tables. Nothing here calls the engine or
decides when to write a row -- that's the application layer's job
(flowctl.app.application).
"""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import Boolean, DateTime as _SADateTime
from sqlalchemy import ForeignKey, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.types import TypeDecorator


class Base(DeclarativeBase):
    pass


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class TZDateTime(TypeDecorator):
    """A DateTime column that's always timezone-aware UTC in Python,
    even though SQLite has no native timezone-aware storage.

    Without this, SQLAlchemy's plain TZDateTime() silently
    *drops* tzinfo once a row is reloaded through a fresh session or
    connection on SQLite (it only appears to work in same-session
    tests because of the identity map, which masks the round-trip).
    That mismatch (naive vs. aware) breaks any comparison against a
    timezone-aware "now", which is exactly what the scheduler does on
    every tick. This type normalizes to naive UTC on the way into the
    database and reattaches timezone.utc on the way out, so every
    caller always sees a proper aware datetime, regardless of how many
    sessions or connections separate the write from the read.
    """

    impl = _SADateTime
    cache_ok = True

    def process_bind_param(self, value: datetime | None, dialect):
        if value is None:
            return None
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return value

    def process_result_value(self, value: datetime | None, dialect):
        if value is None:
            return None
        return value.replace(tzinfo=timezone.utc)


class Pipeline(Base):
    """A registered, named pipeline. See flowctl.app.application.register."""

    __tablename__ = "pipelines"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    schedule: Mapped[str | None] = mapped_column(String(255), nullable=True)
    # Stable load reference decided in Phase 3, e.g. "module:attribute" or
    # an absolute file path plus pipeline name. Set for code-defined
    # pipelines; left null for UI-created linear jobs, which use
    # `commands` instead. A given row has exactly one of the two set.
    load_ref: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    # JSON-encoded list of shell commands, e.g. '["echo one", "echo two"]'.
    # Set only for simple/linear jobs created through the dashboard's
    # "New Job" form (Phase 5) -- deliberately NOT a general DAG editor,
    # see flowctl.app.loader.build_linear_pipeline. Null for code-defined
    # pipelines registered via `flowctl register`.
    commands: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Pause/resume without losing the configured schedule text (used by
    # the dashboard's pause control; the scheduler skips disabled rows).
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(TZDateTime(), default=_utcnow)

    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        return f"Pipeline(name={self.name!r}, schedule={self.schedule!r})"


class Run(Base):
    """One execution of a pipeline (registered or ad-hoc)."""

    __tablename__ = "runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    # Plain string, not a required FK: a pipeline can be run ad-hoc
    # (via `flowctl run <file>`) without ever being registered.
    pipeline_name: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    started_at: Mapped[datetime] = mapped_column(TZDateTime(), nullable=False)
    ended_at: Mapped[datetime] = mapped_column(TZDateTime(), nullable=False)

    task_results: Mapped[list["TaskResult"]] = relationship(
        back_populates="run", cascade="all, delete-orphan", order_by="TaskResult.id"
    )

    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        return f"Run(pipeline_name={self.pipeline_name!r}, status={self.status!r})"


class TaskResult(Base):
    """One task's outcome within a specific Run."""

    __tablename__ = "task_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("runs.id"), nullable=False)
    task_name: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    started_at: Mapped[datetime] = mapped_column(TZDateTime(), nullable=False)
    ended_at: Mapped[datetime] = mapped_column(TZDateTime(), nullable=False)
    # Results can be arbitrary Python objects (dict, str, etc). We store a
    # repr() for display purposes only -- this is not meant to be
    # deserialized back into a live object.
    result_repr: Mapped[str | None] = mapped_column(Text, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    run: Mapped["Run"] = relationship(back_populates="task_results")

    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        return f"TaskResult(task_name={self.task_name!r}, status={self.status!r})"
