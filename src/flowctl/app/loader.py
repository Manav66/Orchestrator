"""Loading a Pipeline object out of a plain Python file.

This is how code-defined pipelines get discovered: you write a file
with a module-level `Pipeline(...)` somewhere in it (conventionally
named `pipeline`), and this module imports that file dynamically and
hands back the Pipeline object.

Also defines the *stable load reference* format used when registering
a pipeline (Phase 3 fix from review feedback): an absolute file path,
optionally followed by "::attr_name" if the file defines more than one
Pipeline and you need to disambiguate. Using "::" (not a single ":")
as the separator deliberately avoids clashing with Windows drive
letters like "C:\\Users\\...".
"""
from __future__ import annotations

import importlib.util
import subprocess
from pathlib import Path
from typing import List, Optional

from flowctl.core.pipeline import Pipeline
from flowctl.core.task import Task

_REF_SEPARATOR = "::"


def load_pipeline_from_file(path: str | Path, attr: Optional[str] = None) -> Pipeline:
    """Import a .py file and return the Pipeline object defined in it.

    If `attr` is given, that specific module-level name is used and
    must be a Pipeline. Otherwise, the file must define exactly one
    Pipeline at module level, or a ValueError is raised asking the
    caller to disambiguate.
    """
    abs_path = Path(path).resolve()
    if not abs_path.exists():
        raise FileNotFoundError(f"No such file: {abs_path}")

    spec = importlib.util.spec_from_file_location(abs_path.stem, abs_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load a module from {abs_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    if attr:
        obj = getattr(module, attr, None)
        if not isinstance(obj, Pipeline):
            raise ValueError(f"{attr!r} in {abs_path} is not a Pipeline instance")
        return obj

    candidates = [v for v in vars(module).values() if isinstance(v, Pipeline)]
    if not candidates:
        raise ValueError(
            f"No Pipeline instance found in {abs_path}. "
            "Define one at module level, e.g. `pipeline = Pipeline(...)`."
        )
    if len(candidates) > 1:
        raise ValueError(
            f"Multiple Pipeline instances found in {abs_path}; "
            "pass --attr to specify which one."
        )
    return candidates[0]


def make_load_ref(path: str | Path, attr: Optional[str] = None) -> str:
    """Build the stable load reference stored in the Pipeline table."""
    abs_path = Path(path).resolve()
    return f"{abs_path}{_REF_SEPARATOR}{attr}" if attr else str(abs_path)


def load_pipeline_from_ref(load_ref: str) -> Pipeline:
    """Reverse of make_load_ref: re-import a pipeline from its stored ref.

    Used by the scheduler (Phase 4), which only has the DB row, not the
    original CLI arguments, to work with.
    """
    if _REF_SEPARATOR in load_ref:
        path_part, attr_part = load_ref.split(_REF_SEPARATOR, 1)
        return load_pipeline_from_file(path_part, attr_part)
    return load_pipeline_from_file(load_ref)


def _make_shell_step(command: str):
    """Build a Task function that runs one shell command.

    Accepts an optional single positional arg (the previous step's
    output) so it works both as the first task in a chain (called with
    zero args) and as a later one (called with one arg, per how
    Executor passes dependency results) -- it doesn't use that
    argument, a linear job's steps don't pass data between each other,
    they just need to run in order.
    """

    def _run(_previous_output: Optional[str] = None) -> str:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip() or "no output"
            raise RuntimeError(f"command exited {result.returncode}: {detail}")
        return result.stdout.strip()

    return _run


def build_linear_pipeline(name: str, commands: List[str]) -> Pipeline:
    """Build a plain, sequential Pipeline out of shell commands.

    This is what a dashboard "New Job" entry becomes at run time: it's
    not a special case handled differently from code-defined pipelines
    -- it's a real Pipeline of real Tasks (each one running a shell
    command), so it goes through the exact same Executor as anything
    written in Python. There is deliberately no branching here: each
    step depends only on the one directly before it, which is the
    scope limit that keeps the dashboard's job creation "simple jobs
    only" rather than a full DAG editor.
    """
    if not commands:
        raise ValueError("A linear pipeline needs at least one command")

    tasks: List[Task] = []
    previous: Optional[Task] = None
    for index, command in enumerate(commands, start=1):
        step = Task(
            name=f"step_{index}",
            func=_make_shell_step(command),
            depends_on=[previous] if previous else [],
        )
        tasks.append(step)
        previous = step

    return Pipeline(name, tasks)
