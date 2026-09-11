"""Database connection setup: turning a path into a usable SQLAlchemy
engine + session, and creating tables if they don't exist yet.

This module is intentionally tiny. It has no opinions about *when* to
read or write rows -- that's the application layer's job. It only
answers "how do I connect" and "how do I get a session."
"""
from __future__ import annotations

import os
from pathlib import Path

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from flowctl.storage.models import Base

DEFAULT_DB_PATH = Path.home() / ".flowctl" / "flowctl.db"


def get_engine(db_path: str | Path | None = None, *, echo: bool = False) -> Engine:
    """Create a SQLAlchemy engine for a SQLite database at db_path.

    Pass ":memory:" for an ephemeral, in-memory database (used heavily
    in tests). Otherwise, the parent directory is created if needed.

    If db_path is None, falls back to the FLOWCTL_DB_PATH environment
    variable (handy for pointing the CLI at a temp DB in tests) and
    finally to DEFAULT_DB_PATH.
    """
    if db_path is None:
        db_path = os.environ.get("FLOWCTL_DB_PATH", str(DEFAULT_DB_PATH))

    if str(db_path) == ":memory:":
        url = "sqlite:///:memory:"
        connect_args = {"check_same_thread": False}
    else:
        db_path = Path(db_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        url = f"sqlite:///{db_path}"
        connect_args = {}

    return create_engine(url, echo=echo, connect_args=connect_args)


def init_db(engine: Engine) -> None:
    """Create all tables described in flowctl.storage.models, if missing,
    and apply small forward-only migrations for columns added after a
    database file may already have been created by an earlier phase.

    This is deliberately not a real migration framework (Alembic would
    be overkill for a local run-history database) -- it just adds a
    couple of columns if they're missing, so a database created back
    in Phase 3/4 keeps working after Phase 5 adds new Pipeline columns,
    instead of erroring with "no such column".
    """
    Base.metadata.create_all(engine)
    with engine.connect() as conn:
        existing_columns = {
            row[1] for row in conn.exec_driver_sql("PRAGMA table_info(pipelines)").fetchall()
        }
        if "commands" not in existing_columns:
            conn.exec_driver_sql("ALTER TABLE pipelines ADD COLUMN commands TEXT")
        if "enabled" not in existing_columns:
            conn.exec_driver_sql(
                "ALTER TABLE pipelines ADD COLUMN enabled BOOLEAN DEFAULT 1 NOT NULL"
            )
        conn.commit()


def get_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, expire_on_commit=False)
