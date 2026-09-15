r"""Example pipeline: a small ETL/analytics pipeline with a genuinely
multi-level dependency graph (8 tasks, 6 levels) -- meant to show what
flowctl looks like on something bigger than a two-task demo.

    flowctl run examples/data_pipeline.py
    flowctl register examples/data_pipeline.py --schedule "30 5 * * *"

Shape of the graph:

    extract_users   extract_orders   extract_products
          |                |                |
    validate_users  validate_orders         |
          \________________|________________/
                           |
                     merge_datasets
                           |
                    compute_metrics
                           |
                  generate_dashboard
                           |
                      notify_team
"""
from __future__ import annotations

from flowctl.core.pipeline import Pipeline
from flowctl.core.task import task


@task()
def extract_users():
    return {"rows": 1240}


@task()
def extract_orders():
    return {"rows": 8532}


@task()
def extract_products():
    return {"rows": 310}


@task(depends_on=[extract_users])
def validate_users(users):
    return {"rows": users["rows"], "invalid": 3}


@task(depends_on=[extract_orders])
def validate_orders(orders):
    return {"rows": orders["rows"], "invalid": 12}


@task(depends_on=[validate_users, validate_orders, extract_products])
def merge_datasets(users, orders, products):
    return {
        "users": users["rows"],
        "orders": orders["rows"],
        "products": products["rows"],
    }


@task(depends_on=[merge_datasets])
def compute_metrics(merged):
    return {
        "avg_orders_per_user": round(merged["orders"] / merged["users"], 2),
        "catalog_size": merged["products"],
    }


@task(depends_on=[compute_metrics])
def generate_dashboard(metrics):
    return f"Dashboard updated: {metrics}"


@task(depends_on=[generate_dashboard])
def notify_team(dashboard_status):
    print(f"[notify_team] {dashboard_status}")
    return "notified"


pipeline = Pipeline(
    "daily_data_pipeline",
    [
        extract_users,
        extract_orders,
        extract_products,
        validate_users,
        validate_orders,
        merge_datasets,
        compute_metrics,
        generate_dashboard,
        notify_team,
    ],
)
