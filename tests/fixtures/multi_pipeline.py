"""A file defining two Pipeline objects, used to test that the loader
correctly demands --attr to disambiguate.
"""
from flowctl.core.pipeline import Pipeline
from flowctl.core.task import task


@task()
def a():
    return "a"


@task()
def b():
    return "b"


pipeline_one = Pipeline("multi_one", [a])
pipeline_two = Pipeline("multi_two", [b])
