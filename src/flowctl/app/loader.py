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
from pathlib import Path
from typing import Optional

from flowctl.core.pipeline import Pipeline

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
