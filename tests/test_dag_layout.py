"""Tests for the SVG DAG layout helper -- specifically the box-width
auto-scaling and label-truncation logic added after real usage showed
long task names (e.g. `generate_dashboard`) overflowing a fixed-width
150px box.
"""
from flowctl.core.pipeline import Pipeline
from flowctl.core.task import Task
from flowctl.web.app import _BOX_W_MAX, _BOX_W_MIN, _build_dag, _truncate_label


def test_truncate_label_leaves_short_names_untouched():
    assert _truncate_label("fetch", 10) == "fetch"
    assert _truncate_label("exactly10c", 10) == "exactly10c"


def test_truncate_label_shortens_and_marks_long_names():
    result = _truncate_label("generate_dashboard_summary_report", 12)
    assert len(result) == 12
    assert result.endswith("…")  # ellipsis


def test_build_dag_widens_boxes_for_long_names_but_stays_within_bounds():
    short_task = Task(name="go", func=lambda: None)
    short_pipeline = Pipeline("short_names", [short_task])
    short_dag = _build_dag(short_pipeline, {})
    assert short_dag["nodes"][0]["w"] == _BOX_W_MIN

    long_task = Task(name="generate_dashboard_summary_report_with_a_very_long_name", func=lambda: None)
    long_pipeline = Pipeline("long_names", [long_task])
    long_dag = _build_dag(long_pipeline, {})
    assert long_dag["nodes"][0]["w"] == _BOX_W_MAX
    # The box widened, but the label was truncated to still fit inside it.
    assert len(long_dag["nodes"][0]["label"]) < len(long_task.name)
    assert long_dag["nodes"][0]["label"].endswith("…")


def test_build_dag_label_matches_full_name_when_it_fits():
    task = Task(name="short_and_fits", func=lambda: None)
    pipeline = Pipeline("fits", [task])
    dag = _build_dag(pipeline, {})
    assert dag["nodes"][0]["label"] == "short_and_fits"
    assert dag["nodes"][0]["name"] == "short_and_fits"
