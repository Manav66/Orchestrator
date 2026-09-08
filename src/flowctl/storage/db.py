"""Database connection setup: turning a path into a usable SQLAlchemy
engine + session, and creating tables if they don't exist yet.

This module is intentionally tiny. It has no opinions about *when* to
read or write rows -- that's the application layer's job. It only
answers "how do I connect" and "how do I get a session."
"""
from __future__ import annotations

from pathlib import Path

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from flowctl.storage.models import Base

DEFAULT_DB_PATH = Path.home() / ".flowctl" / "flowctl.db"


def get_engine(db_path: str | Path | None = None, *, echo: bool = False) -> Engine:
    """Create a SQLAlchemy engine for a SQLite database at db_path.

    Pass ":memory:" for an ephemeral, in-memory database (used heavily
    in tests). Otherwise, the parent directory is created if needed.
    """
    if db_path is None:
        db_path = DEFAULT_DB_PATH

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
    """Create all tables described in flowctl.storage.models, if missing."""
    Base.metadata.create_all(engine)


def get_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, expire_on_commit=False)
