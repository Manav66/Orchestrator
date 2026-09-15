"""Sanity tests for the example pipelines shipped in examples/ -- these
are what a recruiter or interviewer would actually run, so a broken
example would be a bad first impression. Runs each directly through
the pure engine (no DB involved) since we only care that they execute
correctly, not about persistence here.
"""
from pathlib import Path

from flowctl.app.loader import load_pipeline_from_file
from flowctl.core.executor import Executor, PipelineStatus

EXAMPLES_DIR = Path(__file__).parent.parent / "examples"


def test_sample_pipeline_runs_successfully():
    # fetch_orders has a ~34% chance of failing per attempt with 2
    # retries (3 attempts); failing all 3 is ~4% per run -- deliberate,
    # since it's what makes the demo history look realistic. Retry a
    # handful of times so this test itself isn't a ~4%-flaky test.
    pipeline = load_pipeline_from_file(EXAMPLES_DIR / "sample_pipeline.py")
    for attempt in range(5):
        result = Executor().execute(pipeline)
        if result.status == PipelineStatus.SUCCESS:
            break
    assert result.status == PipelineStatus.SUCCESS
    assert set(result.task_outcomes.keys()) == {
        "fetch_orders",
        "fetch_inventory",
        "generate_report",
        "send_report",
    }


def test_data_pipeline_runs_successfully_with_full_multi_level_graph():
    pipeline = load_pipeline_from_file(EXAMPLES_DIR / "data_pipeline.py")
    result = Executor().execute(pipeline)

    assert result.status == PipelineStatus.SUCCESS
    assert len(result.task_outcomes) == 9
    assert all(o.status.value == "success" for o in result.task_outcomes.values())

    # Confirm the graph is genuinely multi-level, not just a flat list.
    levels = pipeline.execution_plan()
    assert len(levels) == 6
    assert {t.name for t in levels[0]} == {"extract_users", "extract_orders", "extract_products"}
    assert [t.name for t in levels[-1]] == ["notify_team"]
