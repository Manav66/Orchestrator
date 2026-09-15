# flowctl

[![CI](https://github.com/Manav66/Orchestrator/actions/workflows/ci.yml/badge.svg)](https://github.com/Manav66/Orchestrator/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)

A small, local Python task orchestrator: define pipelines of dependent tasks,
run them with automatic retries and parallel execution, keep a full history in
SQLite, trigger them via CLI, an automatic scheduler, or a web dashboard.

```
flowctl run examples/sample_pipeline.py
```

```
Pipeline: nightly_sales_report   Run ID: 1
  SUCCESS  fetch_inventory  (attempts=1)
  SUCCESS  fetch_orders  (attempts=1)
  SUCCESS  generate_report  (attempts=1)
  SUCCESS  send_report  (attempts=1)
Overall: SUCCESS
```

## What this actually solves

A lot of automation work looks like a handful of scripts that need to run in
a specific order: pull some data, validate it, generate a report, send it
somewhere, and if a step fails, retry it before giving up and record what
happened so you're not digging through logs later to find out why last
night's job didn't run. flowctl is that: a dependency-aware task runner with
retries, parallel execution for independent steps, persistent run history,
and three ways to trigger a pipeline (CLI, cron-style scheduler, web
dashboard) that all funnel through the exact same execution path.

## Why not just use Airflow / Prefect / Dagster?

Because this project isn't trying to replace them. Real production
orchestration at scale should use one of those. This project exists to
demonstrate the same underlying ideas (dependency graphs, retries, scheduling,
a monitoring UI) at a scope one person can build, test, and fully explain,
which is exactly why one specific limit is enforced on purpose: **pipelines
with real branching dependencies are always defined in Python code. The web
dashboard's "New Job" form can only create simple, linear jobs** (a single
command, or an ordered list of commands, plus an optional schedule) **--
never a general-purpose DAG editor.** That's not a missing feature, it's the
scope decision that keeps this finishable and honest about what a form can
safely represent.

## Architecture

Three entry points (CLI, scheduler, dashboard) all call into a single
application layer, which is the only code allowed to know about both the
pure execution engine and the database:

```
                  ┌─────────────┐  ┌─────────────┐  ┌──────────────┐
Entry points      │     CLI     │  │  Scheduler  │  │  Dashboard   │
                  │ run/register│  │  due tick   │  │  FastAPI+UI  │
                  │ status/logs │  │             │  │              │
                  └──────┬──────┘  └──────┬──────┘  └──────┬───────┘
                         │                │                 │
                         └────────────┬───┴─────────────────┘
                                       ▼
                         ┌─────────────────────────┐
Application layer        │ run_and_record()        │
                         │ register() / run_from_file()
                         │ create_linear_job()      │
                         │ list / logs / schedule   │
                         └────────────┬────────────┘
                       ┌──────────────┴──────────────┐
                       ▼                              ▼
Engine (pure,                              Persistence
in-memory, no I/O)                         (SQLAlchemy + SQLite)
┌──────────────┐                           ┌─────────────────────┐
│ Task         │                           │ Pipeline, Run,       │
│ Pipeline     │                           │ TaskResult tables    │
│ DAG / topo   │                           └─────────────────────┘
│ Executor     │
│ retries      │
│ parallelism  │
└──────────────┘
```

The execution engine (`flowctl/core/`) has zero knowledge that a database
exists -- it's pure, in-memory, and tested in complete isolation (there's
even a test that scans its source with Python's `ast` module to fail loudly
if anyone ever imports SQLAlchemy in there). Persistence is a *caller* of the
engine, never a side effect inside it. A pipeline created through the
dashboard's "New Job" form and a pipeline defined in a Python file are not
two systems: both become a real `core.Pipeline` of real `Task` objects at run
time and execute through the identical `Executor`.

## Installation

```bash
git clone https://github.com/Manav66/Orchestrator.git
cd Orchestrator
python -m venv .venv
# Windows: .venv\Scripts\activate      macOS/Linux: source .venv/bin/activate
pip install -e ".[dev]"
```

## Quickstart

Run the included example pipeline (a "nightly sales report" -- two
independent data-fetch steps run in parallel, one of them occasionally fails
and retries automatically, then a report gets generated and "sent"):

```bash
flowctl run examples/sample_pipeline.py
```

Register it so it becomes a named, trackable entity (this does not run it):

```bash
flowctl register examples/sample_pipeline.py --schedule "0 6 * * *"
flowctl status
flowctl logs 1
```

There's a second, more substantial example worth a look:
`examples/log_analysis_pipeline.py` generates a realistic web-server access
log, then fans out into three tasks that do genuine analysis -- regex log
parsing, real statistical outlier detection (flagging IPs more than two
standard deviations above the mean request rate, which correctly catches a
simulated brute-forcer and a scraper without their IPs being hardcoded
anywhere), and per-endpoint latency percentiles -- before a final task merges
all three into a written Markdown report:

```bash
flowctl run examples/log_analysis_pipeline.py
```

Start the scheduler so registered pipelines fire automatically on their cron
schedule:

```bash
flowctl scheduler start --tick-seconds 5
```

Start the web dashboard:

```bash
flowctl dashboard start
# open http://127.0.0.1:8000
```

From the dashboard you can see every registered pipeline's status at a
glance (with live-updating badges -- no manual refresh needed while a run is
in progress), click into one to see its real dependency graph (rendered as
an SVG with actual connector lines, not just a box list) and run history
with a Gantt-style timeline of parallel task execution, trigger a run,
pause/resume it, edit its schedule, and create simple linear jobs without
writing any code. The homepage also shows overall stats (success rate,
runs today, currently-running count) and a 14-day activity chart, and
there's a dedicated `/runs` view across every pipeline with status
filtering.

### Loading pre-populated demo data

A ready-made SQLite database with three registered code pipelines
(`examples/sample_pipeline.py`, `examples/data_pipeline.py`,
`examples/log_analysis_pipeline.py`) and two dashboard-created linear
jobs, all with two weeks of realistic mixed success/failure run
history, ships in `demo_data/flowctl_demo.db`. To explore the
dashboard without generating your own history first:

```bash
cp demo_data/flowctl_demo.db ~/.flowctl/flowctl.db   # or set FLOWCTL_DB_PATH
flowctl dashboard start
```

## Writing your own pipeline

```python
from flowctl.core.pipeline import Pipeline
from flowctl.core.task import task

@task()
def fetch_data():
    return {"orders": 42}

@task(depends_on=[fetch_data], retries=2, retry_delay=1)
def send_report(data):
    print(f"Report: {data['orders']} orders")
    return "sent"

pipeline = Pipeline("my_pipeline", [fetch_data, send_report])
```

Save this anywhere and run it with `flowctl run path/to/file.py`. Tasks
declared with `depends_on=[...]` receive their dependencies' return values as
positional arguments; independent tasks (no shared dependency) run
concurrently; a task's `retries`/`retry_delay` control automatic retry
behavior; if a task fails permanently, everything depending on it is skipped
rather than run against incomplete data.

## Project layout

```
src/flowctl/
├── core/         # pure execution engine: Task, Pipeline, Executor -- no I/O
├── storage/      # SQLAlchemy models + SQLite connection setup
├── app/          # the ONLY layer that touches both the engine and storage
├── cli/          # Typer commands: run, register, status, logs, scheduler, dashboard
├── scheduler/    # cron-driven loop; no executor of its own
└── web/          # FastAPI + Jinja dashboard
examples/         # the sample_pipeline.py shown above
tests/            # ~75 tests across every layer
```

## Testing

```bash
pytest -v
```

The suite covers dependency-graph ordering and cycle detection, retry and
failure/skip-cascading behavior, genuine concurrent execution (verified with
a `threading.Barrier`, not `sleep()` and hope), persistence and a real
cross-session SQLite timezone bug that was found and fixed during
development, the CLI end to end, a deterministic scheduler (using an
injectable fake clock rather than waiting on real time), and the web
dashboard over real HTTP requests.

## Known limitations (on purpose)

The dashboard's job-creation form only supports simple, linear jobs -- see
"Why not just use Airflow" above. Linear jobs run their commands via
`subprocess` with `shell=True`, which is fine for a local, single-user tool
but would need hardening before ever being exposed to untrusted input in a
multi-user setting. There's no authentication on the dashboard or API, no
distributed workers, and no message queue -- all deliberate scope limits that
keep this a finishable, explainable project rather than a from-scratch clone
of a production system.

## License

MIT -- see [LICENSE](LICENSE).
