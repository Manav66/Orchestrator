"""A pipeline that always fails, used to test the CLI's failure path
(non-zero exit code, FAILED rendered in output).
"""
from flowctl.core.pipeline import Pipeline
from flowctl.core.task import task


@task()
def always_fails():
    raise RuntimeError("deliberate failure for CLI testing")


pipeline = Pipeline("failing_pipeline", [always_fails])
