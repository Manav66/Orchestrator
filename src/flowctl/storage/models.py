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

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Pipeline(Base):
    """A registered, named pipeline. See flowctl.app.application.register."""

    __tablename__ = "pipelines"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    schedule: Mapped[str | None] = mapped_column(String(255), nullable=True)
    # Stable load reference decided in Phase 3, e.g. "module:attribute" or
    # an absolute file path plus pipeline name. Nullable for now.
    load_ref: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)

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
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ended_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

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
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ended_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    # Results can be arbitrary Python objects (dict, str, etc). We store a
    # repr() for display purposes only -- this is not meant to be
    # deserialized back into a live object.
    result_repr: Mapped[str | None] = mapped_column(Text, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    run: Mapped["Run"] = relationship(back_populates="task_results")

    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        return f"TaskResult(task_name={self.task_name!r}, status={self.status!r})"
