"""Pipeline: a named collection of Tasks forming a dependency graph.

Responsible for validating the graph (no cycles, no unknown deps) and
producing an execution plan: an ordered list of "levels", where every
task in a level can safely run in parallel because none of them depend
on each other, but every level fully finishes before the next starts.

Pure module: no I/O, no database, no file access.
"""
from __future__ import annotations

from typing import Dict, List, Optional

from flowctl.core.task import Task


class CycleError(Exception):
    """Raised when a pipeline's tasks contain a dependency cycle."""


class Pipeline:
    def __init__(self, name: str, tasks: List[Task], schedule: Optional[str] = None):
        """
        Args:
            name: Unique pipeline name.
            tasks: All Task objects belonging to this pipeline. Every
                task referenced via depends_on must also appear here.
            schedule: Optional cron expression (e.g. "0 6 * * *"). Not
                acted upon in Phase 1 -- just carried along for later
                phases (registration/scheduler) to use.
        """
        self.name = name
        self.schedule = schedule
        self.tasks: Dict[str, Task] = {t.name: t for t in tasks}
        self._validate_dependencies()

    def _validate_dependencies(self) -> None:
        for t in self.tasks.values():
            for dep in t.depends_on:
                if dep.name not in self.tasks:
                    raise ValueError(
                        f"Task {t.name!r} depends on {dep.name!r}, "
                        f"which is not part of pipeline {self.name!r}"
                    )

    def execution_plan(self) -> List[List[Task]]:
        """Compute execution order as a list of parallelizable levels.

        Uses Kahn's algorithm for topological sorting:
        1. Compute each task's in-degree (number of unmet dependencies).
        2. Any task with in-degree 0 can run now -- these form a level.
        3. "Remove" that level and decrement in-degree for their
           dependents, then repeat.
        4. If tasks remain but none have in-degree 0, there's a cycle.
        """
        in_degree: Dict[str, int] = {name: 0 for name in self.tasks}
        dependents: Dict[str, List[Task]] = {name: [] for name in self.tasks}

        for t in self.tasks.values():
            in_degree[t.name] = len(t.depends_on)
            for dep in t.depends_on:
                dependents[dep.name].append(t)

        levels: List[List[Task]] = []
        remaining = dict(in_degree)

        while remaining:
            current_level = [
                self.tasks[name] for name, deg in remaining.items() if deg == 0
            ]
            if not current_level:
                cyclic = ", ".join(sorted(remaining.keys()))
                raise CycleError(
                    f"Cycle detected in pipeline {self.name!r} among tasks: {cyclic}"
                )

            levels.append(sorted(current_level, key=lambda t: t.name))

            for t in current_level:
                del remaining[t.name]
            for t in current_level:
                for dependent in dependents[t.name]:
                    if dependent.name in remaining:
                        remaining[dependent.name] -= 1

        return levels

    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        return f"Pipeline(name={self.name!r}, tasks={list(self.tasks.keys())!r})"
