"""A minimal pipeline used only by CLI/loader tests. Not the polished
demo pipeline -- that's a Phase 6 deliverable (examples/sample_pipeline.py).
"""
from flowctl.core.pipeline import Pipeline
from flowctl.core.task import task


@task()
def fetch():
    return {"orders": 42}


@task(depends_on=[fetch])
def report(data):
    return f"Report: {data['orders']} orders"


pipeline = Pipeline("demo_pipeline", [fetch, report])
