"""Task definitions and the @task decorator.

A Task wraps a plain Python callable and records which other tasks it
depends on. Tasks are the nodes of a pipeline's dependency graph.

This module is pure: it has no knowledge of databases, files, or any
other I/O. It is imported and tested in complete isolation.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, List, Optional


@dataclass
class Task:
    """A single unit of work inside a pipeline.

    Attributes:
        name: Unique identifier for this task within its pipeline.
        func: The underlying Python callable to execute. Receives the
            return values of its direct dependencies as positional
            arguments, in the order they're listed in depends_on.
        depends_on: Other Task objects that must succeed before this
            task is allowed to run.
        retries: Number of extra attempts allowed after the first
            failure (0 means no retries, so 1 total attempt).
        retry_delay: Seconds to wait between retry attempts.
    """

    name: str
    func: Callable
    depends_on: List["Task"] = field(default_factory=list)
    retries: int = 0
    retry_delay: float = 0.0

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Task):
            return NotImplemented
        return self.name == other.name

    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        deps = [d.name for d in self.depends_on]
        return f"Task(name={self.name!r}, depends_on={deps!r})"


def task(
    depends_on: Optional[List[Task]] = None,
    retries: int = 0,
    retry_delay: float = 0.0,
    name: Optional[str] = None,
):
    """Decorator that turns a plain function into a Task.

    Example:
        @task()
        def fetch_data():
            return {"orders": 42}

        @task(depends_on=[fetch_data], retries=2)
        def validate_data(data):
            ...

    After decoration, `fetch_data` and `validate_data` are Task
    instances, not plain functions. Use `.func` to reach the original
    callable directly (mainly useful in tests).
    """

    def decorator(func: Callable) -> Task:
        return Task(
            name=name or func.__name__,
            func=func,
            depends_on=list(depends_on or []),
            retries=retries,
            retry_delay=retry_delay,
        )

    return decorator
