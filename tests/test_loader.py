"""Tests for flowctl.app.loader: turning a .py file into a Pipeline
object, and the stable load-reference format used by `register`.
"""
from pathlib import Path

import pytest

from flowctl.app.loader import load_pipeline_from_file, load_pipeline_from_ref, make_load_ref

FIXTURES = Path(__file__).parent / "fixtures"


def test_load_pipeline_auto_detects_single_pipeline_in_file():
    pipeline = load_pipeline_from_file(FIXTURES / "demo_pipeline.py")
    assert pipeline.name == "demo_pipeline"


def test_load_pipeline_with_explicit_attr():
    pipeline = load_pipeline_from_file(FIXTURES / "multi_pipeline.py", attr="pipeline_two")
    assert pipeline.name == "multi_two"


def test_load_pipeline_raises_when_multiple_candidates_and_no_attr():
    with pytest.raises(ValueError, match="Multiple Pipeline instances"):
        load_pipeline_from_file(FIXTURES / "multi_pipeline.py")


def test_load_pipeline_raises_on_missing_file():
    with pytest.raises(FileNotFoundError):
        load_pipeline_from_file(FIXTURES / "does_not_exist.py")


def test_make_load_ref_and_round_trip_without_attr():
    ref = make_load_ref(FIXTURES / "demo_pipeline.py")
    assert "::" not in ref  # no attr needed -> plain absolute path
    pipeline = load_pipeline_from_ref(ref)
    assert pipeline.name == "demo_pipeline"


def test_make_load_ref_and_round_trip_with_attr():
    ref = make_load_ref(FIXTURES / "multi_pipeline.py", attr="pipeline_one")
    assert ref.endswith("::pipeline_one")
    pipeline = load_pipeline_from_ref(ref)
    assert pipeline.name == "multi_one"
