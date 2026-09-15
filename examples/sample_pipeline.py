"""Example pipeline: a nightly sales report.

This is the demo pipeline referenced throughout the project's design
docs -- it's the "why would anyone use this" story made real: pull
data from a couple of independent sources, combine it into a report,
and send it, with automatic retries if a source is temporarily flaky.

Try it:

    flowctl run examples/sample_pipeline.py

Notice fetch_orders and fetch_inventory run in parallel (neither
depends on the other), fetch_orders occasionally fails and retries
automatically, and generate_report only runs once BOTH of its
dependencies have succeeded.

Register it so the scheduler can fire it automatically:

    flowctl register examples/sample_pipeline.py --schedule "0 6 * * *"
"""
from __future__ import annotations

import random

from flowctl.core.pipeline import Pipeline
from flowctl.core.task import task


@task(retries=2, retry_delay=1)
def fetch_orders():
    """Simulates pulling order data from a database that's
    occasionally slow to respond. With 2 retries (3 attempts total),
    a ~34% per-attempt failure rate still succeeds almost every run --
    this is what "automatic retries make flaky sources reliable"
    actually looks like, not just a description of it.
    """
    if random.random() < 0.34:
        raise ConnectionError("orders database timed out")
    return {"total_orders": 128, "revenue": 4521.50}


@task()
def fetch_inventory():
    """No dependency on fetch_orders, so the executor runs this
    concurrently with it, not after it."""
    return {"low_stock_items": ["widget-a", "widget-c"]}


@task(depends_on=[fetch_orders, fetch_inventory])
def generate_report(orders, inventory):
    """Only runs once BOTH fetch_orders and fetch_inventory have
    succeeded -- if either failed, this gets skipped automatically,
    it never runs against half-missing data."""
    low_stock = ", ".join(inventory["low_stock_items"]) or "none"
    return (
        "Nightly Sales Report\n"
        f"  Orders:    {orders['total_orders']}\n"
        f"  Revenue:   ${orders['revenue']:.2f}\n"
        f"  Low stock: {low_stock}"
    )


@task(depends_on=[generate_report])
def send_report(report_text):
    """Stands in for actually emailing/posting the report somewhere --
    printing it is enough to prove the pipeline ran end to end."""
    print("\n---- SENDING REPORT ----")
    print(report_text)
    print("-------------------------\n")
    return "sent"


pipeline = Pipeline(
    "nightly_sales_report",
    [fetch_orders, fetch_inventory, generate_report, send_report],
)
