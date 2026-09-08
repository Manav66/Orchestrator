"""Tests for the execution engine (Executor): retries, skip-cascading,
and genuine fan-out parallelism.
"""
import threading

from flowctl.core.executor import Executor, PipelineStatus, TaskStatus
from flowctl.core.pipeline import Pipeline
from flowctl.core.task import task


def test_successful_linear_pipeline_passes_results_between_tasks():
    @task()
    def fetch():
        return {"orders": 42}

    @task(depends_on=[fetch])
    def report(data):
        return f"Report: {data['orders']} orders"

    pipeline = Pipeline("nightly", [fetch, report])
    result = Executor().execute(pipeline)

    assert result.status == PipelineStatus.SUCCESS
    assert result.task_outcomes["fetch"].result == {"orders": 42}
    assert result.task_outcomes["report"].result == "Report: 42 orders"
    assert result.task_outcomes["report"].attempts == 1


def test_task_fails_twice_then_succeeds_via_retry():
    attempts_made = {"count": 0}

    @task(retries=2, retry_delay=0)
    def flaky():
        attempts_made["count"] += 1
        if attempts_made["count"] < 3:
            raise RuntimeError("simulated transient failure")
        return "finally worked"

    pipeline = Pipeline("retry_demo", [flaky])
    result = Executor().execute(pipeline)

    outcome = result.task_outcomes["flaky"]
    assert result.status == PipelineStatus.SUCCESS
    assert outcome.status == TaskStatus.SUCCESS
    assert outcome.attempts == 3
    assert outcome.result == "finally worked"


def test_task_fails_permanently_after_exhausting_retries():
    @task(retries=1, retry_delay=0)
    def always_fails():
        raise RuntimeError("nope")

    pipeline = Pipeline("permanent_failure", [always_fails])
    result = Executor().execute(pipeline)

    outcome = result.task_outcomes["always_fails"]
    assert result.status == PipelineStatus.FAILED
    assert outcome.status == TaskStatus.FAILED
    assert outcome.attempts == 2  # 1 initial attempt + 1 retry
    assert "RuntimeError" in outcome.error


def test_downstream_task_is_skipped_when_dependency_fails():
    @task()
    def upstream_fails():
        raise RuntimeError("boom")

    @task(depends_on=[upstream_fails])
    def downstream(_result):
        return "should never run"

    pipeline = Pipeline("cascade", [upstream_fails, downstream])
    result = Executor().execute(pipeline)

    assert result.status == PipelineStatus.FAILED
    assert result.task_outcomes["upstream_fails"].status == TaskStatus.FAILED
    assert result.task_outcomes["downstream"].status == TaskStatus.SKIPPED


def test_independent_tasks_actually_run_concurrently():
    """Prove real fan-out parallelism using a threading.Barrier instead
    of sleep()+hope.

    Two independent tasks (no dependency between them) both try to
    pass through a 2-party barrier. This only succeeds if both tasks
    are executing at the same time: if the executor ran them one
    after another, the first task would reach the barrier alone,
    time out waiting for a second party, and raise.
    """
    barrier = threading.Barrier(parties=2, timeout=5)

    @task(name="left")
    def left():
        barrier.wait()
        return "left-done"

    @task(name="right")
    def right():
        barrier.wait()
        return "right-done"

    pipeline = Pipeline("fan_out", [left, right])
    result = Executor(max_workers=2).execute(pipeline)

    assert result.status == PipelineStatus.SUCCESS
    assert result.task_outcomes["left"].result == "left-done"
    assert result.task_outcomes["right"].result == "right-done"
