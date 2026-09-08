# flowctl — Project Context

> This file is a self-contained brief for anyone (human or AI assistant) picking up
> this project fresh. It captures what we're building, why, the architecture, the
> phase plan with concrete outcomes, and the implementation rules that must not be
> violated while coding. If you're an AI assistant reading this as context: treat
> the "Implementation rules" section as hard constraints, not suggestions.

## 1. What this project is

`flowctl` is a pip-installable Python task-orchestration tool — a small, local,
honest version of tools like Airflow/Prefect/Dagster. It lets someone define a
pipeline (a set of tasks with dependencies between them), run it, retry failed
steps automatically, run independent steps in parallel, keep a full history of
every run, and trigger pipelines either manually (CLI), automatically (scheduler),
or via a simple web dashboard.

**Purpose:** this is a portfolio/resume project targeting Python developer roles.
The author's background is Python automation/scripting. The goal is to demonstrate
real engineering (graph algorithms, concurrency, database design, CLI design,
testing discipline, packaging/distribution) through a problem that's small enough
to actually finish, not to compete with real orchestration tools.

**Scope discipline (important, do not expand):** no distributed workers, no Redis,
no auth system, no full drag-and-drop DAG builder. The one deliberate scope
decision that makes this finishable: **real branching pipelines are defined in
Python code. The web UI can only create simple, linear jobs (one command, or an
ordered list of commands, plus a schedule).** This is stated openly in the README,
not hidden — it's the answer to "why not just use Airflow?"

## 2. Architecture (the whiteboard version)

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
(the ONLY code that      │ register()              │
knows both sides)        │ create_linear_job()     │
                         │ list / logs              │
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

Key idea: **three doors, one run path, one history.** CLI, scheduler, and
dashboard never call the engine or the database directly — they only call the
application layer. The application layer calls the pure engine, gets back a
plain result object, and only then persists it via SQLAlchemy. The engine has
zero knowledge that a database exists, which is what keeps its test suite fast,
deterministic, and DB-free. The dashboard sits *beside* the CLI, not above it —
both are equal callers of the same application layer and both read/write the
same SQLite history.

Code-defined pipelines and UI-created linear jobs are **not two systems**. A
UI-created job is still just a `Pipeline` containing a short chain of `Task`s
(each a shell command or a small callable) — it runs through the exact same
executor as anything defined in Python. The DAG diagram on the dashboard's
detail page renders whatever is stored, regardless of origin, through the same
view.

## 3. Tech stack

- Python 3.11+
- CLI: Typer (+ `rich` for colored/status output)
- Persistence: SQLAlchemy + SQLite
- Scheduler: `croniter` for cron-expression parsing
- Web dashboard: FastAPI + Jinja2 templates + plain HTML/JS (no separate
  frontend build/Node toolchain, no React — kept inside the Python ecosystem
  deliberately)
- Testing: pytest
- CI: GitHub Actions
- Packaging: `pyproject.toml`, console-script entry point, installable via
  `pip install .` (and eventually published properly)
- Repo: local at `C:\Users\p1_manaverm\Desktop\Orchestrator`, pushed to GitHub
  via GitHub Desktop

## 4. Repo layout

```
Orchestrator/
├── src/flowctl/
│   ├── core/
│   │   ├── task.py          # Task dataclass + @task decorator
│   │   ├── pipeline.py      # DAG construction, topological sort, cycle detection
│   │   └── executor.py      # Pure execution engine: retries, thread-pool parallelism
│   ├── app/
│   │   └── application.py   # run_and_record(), register(), create_linear_job(), list/logs
│   ├── storage/
│   │   ├── models.py        # SQLAlchemy models: Pipeline, Run, TaskResult
│   │   └── db.py             # engine/session setup
│   ├── scheduler/
│   │   └── scheduler.py     # cron tick loop -> calls run_and_record()
│   ├── cli/
│   │   └── main.py          # Typer app: run, register, status, logs, scheduler start
│   └── web/
│       ├── app.py            # FastAPI app, routes call the application layer only
│       ├── templates/        # Jinja templates: homepage, pipeline detail, new-job form
│       └── static/
├── examples/
│   └── sample_pipeline.py   # polished demo pipeline (nightly-report style)
├── tests/
│   ├── test_pipeline.py     # diamond ordering, cycle detection
│   ├── test_executor.py     # retry-then-succeed, fan-out parallelism (barrier/timestamp based)
│   └── test_application.py # run_and_record persists correctly, engine stays untouched by DB
├── pyproject.toml
├── README.md                 # includes the architecture diagram + scoping paragraph
└── .github/workflows/ci.yml
```

## 5. Phase plan with concrete, checkable outcomes

Each phase has a binary proof of "done" — a passing test, a query result, a
command's output, a scheduler tick actually firing, a browser click, a fresh
venv install. No phase is marked done on vibes.

**Phase 1 — Core engine (pure, in-memory, zero I/O)**
Task/Pipeline model + decorators, DAG construction, topological sort, executor
with retries/backoff and thread-pool parallelism. The executor returns a plain
result object; it does not persist anything itself and has no database imports
at all.
- Proof: pytest suite covering (a) diamond-shaped dependency ordering, (b) cycle
  detection raises a clear error, (c) a task that fails twice then succeeds via
  retry, (d) genuine fan-out parallelism — measured with timestamps/tolerance or
  a threading barrier, **not** `sleep()` + hope, since that's an easy test to
  make flaky.

**Phase 2 — Persistence + application layer**
SQLAlchemy models (Pipeline, Run, TaskResult) on SQLite. Build the application
layer: `run_and_record()` calls `engine.execute()` (pure), then persists the
result via plain SQLAlchemy calls (`storage.save()` — no invented extra wrapper
layer around this, just clean SQLAlchemy usage from the application layer).
- Proof: run a pipeline twice, query the DB, see both runs correctly recorded,
  while the Phase 1 engine tests remain completely untouched/DB-free.

**Phase 3 — CLI**
First fully human-usable milestone, built entirely on top of the application
layer (never engine/DB directly).
- `flowctl run <file>` — executes a pipeline **right now** and records the run,
  whether or not it's ever been registered. Ad-hoc runs show up in history too.
- `flowctl register <file>` — stores a pipeline as a named, schedulable entity:
  name, schedule, and a **stable load reference** (e.g. `module:attribute` or an
  absolute file path + pipeline name — NOT a relative path, since the
  scheduler's working directory can't be assumed to match wherever `register`
  was originally run from). This decision is made now, not deferred.
- `flowctl status` — lists pipelines + last-run outcomes (registered pipelines
  and ad-hoc run history both visible).
- `flowctl logs <run-id>` — shows per-task detail for one run.
- Proof: run the example pipeline, see live colored task-by-task output
  including a retry; register it; see it appear in `flowctl status`.

**Phase 4 — Scheduler**
Background loop that queries the DB for registered pipelines that are due
(via `croniter`) and fires them by calling **the exact same** `run_and_record()`
the CLI uses. No separate executor of its own.
- Proof: register a pipeline with a short test schedule, start the scheduler,
  watch it fire automatically without manual intervention, confirm it shows up
  in `flowctl status`.

**Phase 5 — Web dashboard**
FastAPI + Jinja, positioned beside the CLI, reading/writing the same DB.
- Homepage: all pipelines with colored status badges (success/fail/running).
- Pipeline detail page: DAG diagram rendered from whatever's stored (code graph
  or linear UI job, same view either way), run history table, per-run task logs,
  run-now / pause / edit-schedule controls.
- "New Job" form: creates **simple/linear jobs only** (single command or
  ordered list of commands + schedule) via a `create_linear_job()` function in
  the application layer — never raw SQL inline in a FastAPI route. This
  limitation is intentional and stated in the README, not a bug.
- UI-created jobs still require the scheduler process to be running to fire
  automatically, same as code-registered ones.
- Proof: create a job via the form, watch it appear on the homepage, watch it
  execute on schedule; open a code-defined pipeline's detail page and see its
  real multi-node DAG rendered correctly.

**Phase 6 — Packaging, CI, docs**
Finalize `pyproject.toml` with a console-script entry point, verify local
`pip install` works, GitHub Actions running pytest on every push, README
containing the architecture diagram above plus an explicit paragraph on the
Airflow/Prefect scoping decision, and one polished example pipeline
(`examples/sample_pipeline.py`, nightly-report style: fetch → validate →
transform → send).
- Proof: badge shows CI passing; README is skimmable in ~30 seconds and
  explains what/why/how to run.

**Phase 7 — Final verification**
- Proof: full test suite actually run and passing; CLI and dashboard manually
  exercised end-to-end (register a code pipeline, create a UI job, trigger
  both, confirm a retry genuinely happens, confirm the scheduler fires a due
  job, check history in both CLI and dashboard); a completely fresh virtual
  environment installs and runs the package from scratch. One full demo path
  recorded as the "proof of done" artifact.

## 6. Implementation rules (hard constraints, not suggestions)

1. The engine (`core/`) never imports SQLAlchemy or anything database-related.
   It is pure, in-memory, and testable in total isolation.
2. CLI, scheduler, and dashboard route handlers never call the engine or the
   database directly — they only call functions in the application layer
   (`run_and_record`, `register`, `create_linear_job`, list/logs helpers).
3. Persistence itself is plain SQLAlchemy calls from the application layer —
   do not invent an extra wrapper layer around `storage.save()` for its own
   sake.
4. `run` and `register` are independent: `run` always executes + records, even
   for pipelines never registered. `register` is what promotes a pipeline to
   being named/schedulable/visible as a standing entity. `status` shows both
   registered pipelines and ad-hoc run history.
5. The load reference for a registered pipeline must be stable across process
   restarts and working-directory changes (`module:attribute` or absolute path
   + name) — decided in Phase 3, not improvised later.
6. A UI-created linear job is just a `Pipeline` of `Task`s like any other — it
   runs through the same executor, no second/parallel runner is ever built.
7. The fan-out parallelism test must use real synchronization (timestamps with
   a tolerance, or a threading barrier/event) — not `sleep()` and hope.
8. Dashboard writes go through `create_linear_job()` in the application layer,
   never raw SQL inline in a route handler.
9. Scope lock: no distributed workers, no Redis/message queue, no auth system,
   no full drag-and-drop DAG builder. If a request would add one of these,
   flag it explicitly rather than quietly building it.

## 7. Interview framing this is meant to unlock

> "I didn't clone Airflow. I built a local orchestrator with a pure, testable
> execution engine, one shared SQLite run history, and three interfaces — CLI,
> scheduler, and a dashboard — that all funnel through a single application
> layer. Code owns real branching DAGs; the UI only owns simple linear jobs.
> That scope decision is exactly why the project was finishable."

## 8. Current status

Nothing has been built yet as of this writing. Repo location:
`C:\Users\p1_manaverm\Desktop\Orchestrator`. Will be pushed to GitHub via
GitHub Desktop once the initial scaffold + Phase 1 exist locally.
