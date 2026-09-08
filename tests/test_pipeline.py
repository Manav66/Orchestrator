"""Tests for DAG construction and topological sort (Pipeline)."""
import pytest

from flowctl.core.pipeline import CycleError, Pipeline
from flowctl.core.task import task


def test_diamond_dependency_ordering():
    """A -> B, A -> C, (B, C) -> D.

    Correct plan: level 0 = [A], level 1 = [B, C] (order doesn't
    matter, both depend only on A), level 2 = [D].
    """

    @task()
    def a():
        return "a"

    @task(depends_on=[a])
    def b(_a):
        return "b"

    @task(depends_on=[a])
    def c(_a):
        return "c"

    @task(depends_on=[b, c])
    def d(_b, _c):
        return "d"

    pipeline = Pipeline("diamond", [a, b, c, d])
    levels = pipeline.execution_plan()

    assert [t.name for t in levels[0]] == ["a"]
    assert {t.name for t in levels[1]} == {"b", "c"}
    assert [t.name for t in levels[2]] == ["d"]


def test_linear_chain_ordering():
    @task()
    def one():
        return 1

    @task(depends_on=[one])
    def two(_one):
        return 2

    @task(depends_on=[two])
    def three(_two):
        return 3

    pipeline = Pipeline("chain", [one, two, three])
    levels = pipeline.execution_plan()

    assert [lvl[0].name for lvl in levels] == ["one", "two", "three"]


def test_cycle_detection_raises_clear_error():
    @task(name="x")
    def x_func():
        return 1

    @task(name="y", depends_on=[x_func])
    def y_func(_x):
        return 2

    # Manually create a cycle: x depends on y, y depends on x.
    x_func.depends_on.append(y_func)

    pipeline = Pipeline("cyclic", [x_func, y_func])

    with pytest.raises(CycleError) as exc_info:
        pipeline.execution_plan()

    assert "cyclic" in str(exc_info.value)


def test_unknown_dependency_raises_value_error():
    @task()
    def outside():
        return 1

    @task(depends_on=[outside])
    def inside(_o):
        return 2

    # `outside` deliberately left out of the pipeline's task list.
    with pytest.raises(ValueError):
        Pipeline("broken", [inside])
