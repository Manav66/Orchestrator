"""The web dashboard: FastAPI + Jinja templates.

Positioned beside the CLI, not above it -- both read and write the
exact same SQLite history through the exact same application layer.
No route here calls flowctl.core or flowctl.storage directly; every
route opens a session via app_layer.get_session() and calls exactly
one (or a couple of read-only) application-layer functions.

Job creation is deliberately limited to simple/linear jobs (see
create_linear_job's docstring) -- real branching pipelines are always
code-defined via `flowctl register`. The pipeline detail page renders
whatever's actually stored (a code-defined graph or a linear job)
through the same DAG view, since load_pipeline_for_row() turns either
one into a real core.Pipeline before rendering.

The /api/* routes exist purely to power client-side polling (live
auto-refresh) -- they're read-only JSON mirrors of what the HTML pages
already render, not a separate API surface with its own concerns.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional
from urllib.parse import quote

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from flowctl.app import application as app_layer

BASE_DIR = Path(__file__).resolve().parent

app = FastAPI(title="flowctl dashboard")
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

STATUS_COLORS = {
    "success": "#3fb950",
    "failed": "#f85149",
    "skipped": "#d29922",
    "running": "#58a6ff",
}

# Box/graph layout constants for the SVG dependency diagram.
_BOX_W, _BOX_H = 150, 46
_COL_GAP, _ROW_GAP = 36, 64
_PADDING = 24


def _status_color(status: Optional[str]) -> str:
    return STATUS_COLORS.get(status or "", "#6e7681")


def _redirect_with_error(base_url: str, exc: Exception) -> RedirectResponse:
    """Build a redirect carrying an error message, properly URL-encoded.

    Without quoting, a message containing '&', '#', spaces, or other
    URL-meaningful characters would corrupt the query string (or, worse,
    let arbitrary text/characters from an exception message leak
    unescaped into the URL). quote() makes this safe.
    """
    return RedirectResponse(url=f"{base_url}?error={quote(str(exc))}", status_code=303)


def _build_dag(pipeline_obj, statuses: dict) -> dict:
    """Compute pixel coordinates for a real, connector-line dependency
    diagram: one node per task, positioned by its level (row) and
    position within that level (column), plus one edge per actual
    dependency (not just "level N feeds level N+1", but the specific
    box-to-box lines a real DAG viewer would draw).
    """
    levels = pipeline_obj.execution_plan()
    positions: dict[str, tuple[float, float]] = {}
    row_widths = [len(level) * _BOX_W + max(0, len(level) - 1) * _COL_GAP for level in levels]
    max_width = max(row_widths) if row_widths else 0

    for i, level in enumerate(levels):
        x_start = (max_width - row_widths[i]) / 2
        y = i * (_BOX_H + _ROW_GAP)
        for idx, t in enumerate(level):
            positions[t.name] = (x_start + idx * (_BOX_W + _COL_GAP), y)

    nodes = []
    for level in levels:
        for t in level:
            x, y = positions[t.name]
            status = statuses.get(t.name, "never run")
            nodes.append(
                {
                    "name": t.name,
                    "x": x + _PADDING,
                    "y": y + _PADDING,
                    "w": _BOX_W,
                    "h": _BOX_H,
                    "status": status,
                    "color": _status_color(status),
                }
            )

    edges = []
    for t in pipeline_obj.tasks.values():
        for dep in t.depends_on:
            x1, y1 = positions[dep.name]
            x2, y2 = positions[t.name]
            edges.append(
                {
                    "x1": x1 + _BOX_W / 2 + _PADDING,
                    "y1": y1 + _BOX_H + _PADDING,
                    "x2": x2 + _BOX_W / 2 + _PADDING,
                    "y2": y2 + _PADDING,
                }
            )

    height = (len(levels) * (_BOX_H + _ROW_GAP) - _ROW_GAP if levels else 0) + 2 * _PADDING
    return {
        "nodes": nodes,
        "edges": edges,
        "width": max_width + 2 * _PADDING,
        "height": max(height, _BOX_H + 2 * _PADDING),
    }


def _pipeline_summary(session, p) -> dict:
    recent = app_layer.list_runs(session, pipeline_name=p.name, limit=5)
    last = recent[0] if recent else None
    return {
        "name": p.name,
        "schedule": p.schedule,
        "enabled": p.enabled,
        "kind": "code" if p.load_ref else "linear job",
        "last_status": last.status if last else "never run",
        "last_ended": last.ended_at if last else None,
        "color": _status_color(last.status if last else None),
        "recent_dots": [{"status": r.status, "color": _status_color(r.status)} for r in reversed(recent)],
    }


@app.get("/")
def index(request: Request):
    with app_layer.get_session() as session:
        pipelines = app_layer.list_pipelines(session)
        rows = [_pipeline_summary(session, p) for p in pipelines]

        registered_names = {p.name for p in pipelines}
        adhoc = [
            r
            for r in app_layer.list_runs(session, limit=20)
            if r.pipeline_name not in registered_names
        ]

        stats = app_layer.dashboard_stats(session)
        chart_data = app_layer.daily_run_counts(session, days=14)

    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "pipelines": rows,
            "adhoc_runs": adhoc,
            "status_color": _status_color,
            "stats": stats,
            "chart_data": chart_data,
            "active_page": "overview",
        },
    )


@app.get("/api/pipelines")
def api_pipelines():
    with app_layer.get_session() as session:
        pipelines = app_layer.list_pipelines(session)
        return {"pipelines": [_pipeline_summary(session, p) for p in pipelines]}


@app.get("/jobs/new")
def new_job_form(request: Request, error: Optional[str] = None):
    return templates.TemplateResponse(request, "new_job.html", {"error": error})


@app.post("/jobs/new")
def new_job_submit(
    name: str = Form(...),
    commands: str = Form(...),
    schedule: str = Form(""),
):
    command_list = [line.strip() for line in commands.splitlines() if line.strip()]
    schedule_value = schedule.strip() or None
    try:
        with app_layer.get_session() as session:
            app_layer.create_linear_job(name, command_list, session, schedule=schedule_value)
    except ValueError as exc:
        return _redirect_with_error("/jobs/new", exc)

    return RedirectResponse(url=f"/pipelines/{quote(name)}", status_code=303)


@app.get("/pipelines/{name}")
def pipeline_detail(request: Request, name: str, error: Optional[str] = None):
    with app_layer.get_session() as session:
        pipeline_row = app_layer.get_pipeline(session, name)
        if pipeline_row is None:
            raise HTTPException(status_code=404, detail=f"No pipeline named {name!r}")

        pipeline_obj = app_layer.load_pipeline_for_row(pipeline_row)
        statuses = app_layer.latest_task_statuses(session, name)
        dag = _build_dag(pipeline_obj, statuses)
        task_details = app_layer.latest_task_details(session, name)

        runs = app_layer.list_runs(session, pipeline_name=name, limit=15)
        run_rows = [
            {
                "id": r.id,
                "status": r.status,
                "color": _status_color(r.status),
                "started_at": r.started_at,
                "ended_at": r.ended_at,
            }
            for r in runs
        ]

    return templates.TemplateResponse(
        request,
        "pipeline_detail.html",
        {
            "pipeline": pipeline_row,
            "dag": dag,
            "task_details": task_details,
            "runs": run_rows,
            "error": error,
        },
    )


@app.get("/api/pipelines/{name}")
def api_pipeline_detail(name: str):
    with app_layer.get_session() as session:
        pipeline_row = app_layer.get_pipeline(session, name)
        if pipeline_row is None:
            raise HTTPException(status_code=404, detail=f"No pipeline named {name!r}")

        statuses = app_layer.latest_task_statuses(session, name)
        task_details = app_layer.latest_task_details(session, name)
        runs = app_layer.list_runs(session, pipeline_name=name, limit=15)

        return {
            "name": pipeline_row.name,
            "schedule": pipeline_row.schedule,
            "enabled": pipeline_row.enabled,
            "task_statuses": {k: {"status": v, "color": _status_color(v)} for k, v in statuses.items()},
            "task_details": task_details,
            "runs": [
                {
                    "id": r.id,
                    "status": r.status,
                    "color": _status_color(r.status),
                    "started_at": r.started_at,
                    "ended_at": r.ended_at,
                }
                for r in runs
            ],
        }


@app.post("/pipelines/{name}/run")
def run_pipeline_now(name: str):
    try:
        with app_layer.get_session() as session:
            app_layer.run_registered(name, session)
    except ValueError as exc:
        return _redirect_with_error(f"/pipelines/{quote(name)}", exc)
    return RedirectResponse(url=f"/pipelines/{quote(name)}", status_code=303)


@app.post("/pipelines/{name}/schedule")
def update_pipeline_schedule(name: str, schedule: str = Form("")):
    schedule_value = schedule.strip() or None
    try:
        with app_layer.get_session() as session:
            app_layer.update_schedule(name, schedule_value, session)
    except ValueError as exc:
        return _redirect_with_error(f"/pipelines/{quote(name)}", exc)
    return RedirectResponse(url=f"/pipelines/{quote(name)}", status_code=303)


@app.post("/pipelines/{name}/toggle")
def toggle_pipeline(name: str):
    with app_layer.get_session() as session:
        pipeline_row = app_layer.get_pipeline(session, name)
        if pipeline_row is None:
            raise HTTPException(status_code=404, detail=f"No pipeline named {name!r}")
        app_layer.set_enabled(name, not pipeline_row.enabled, session)
    return RedirectResponse(url=f"/pipelines/{quote(name)}", status_code=303)


@app.get("/runs")
def all_runs(request: Request, pipeline: Optional[str] = None, status: Optional[str] = None):
    with app_layer.get_session() as session:
        pipeline_names = [p.name for p in app_layer.list_pipelines(session)]
        runs = app_layer.list_runs(
            session, pipeline_name=pipeline or None, status=status or None, limit=100
        )
        rows = [
            {
                "id": r.id,
                "pipeline_name": r.pipeline_name,
                "status": r.status,
                "color": _status_color(r.status),
                "started_at": r.started_at,
                "ended_at": r.ended_at,
            }
            for r in runs
        ]

    return templates.TemplateResponse(
        request,
        "runs.html",
        {
            "runs": rows,
            "pipeline_names": pipeline_names,
            "selected_pipeline": pipeline or "",
            "selected_status": status or "",
            "active_page": "runs",
        },
    )


@app.get("/api/runs")
def api_runs(pipeline: Optional[str] = None, status: Optional[str] = None):
    with app_layer.get_session() as session:
        runs = app_layer.list_runs(
            session, pipeline_name=pipeline or None, status=status or None, limit=100
        )
        return {
            "runs": [
                {
                    "id": r.id,
                    "pipeline_name": r.pipeline_name,
                    "status": r.status,
                    "color": _status_color(r.status),
                    "started_at": r.started_at,
                    "ended_at": r.ended_at,
                }
                for r in runs
            ]
        }


@app.get("/runs/{run_id}")
def run_detail(request: Request, run_id: int):
    with app_layer.get_session() as session:
        run_row = app_layer.get_run(session, run_id)
        if run_row is None:
            raise HTTPException(status_code=404, detail=f"No run with id {run_id}")

        total_duration = max((run_row.ended_at - run_row.started_at).total_seconds(), 0.001)

        tasks = []
        for tr in run_row.task_results:
            offset_pct = max(0.0, (tr.started_at - run_row.started_at).total_seconds() / total_duration * 100)
            width_pct = max(1.5, (tr.ended_at - tr.started_at).total_seconds() / total_duration * 100)
            tasks.append(
                {
                    "name": tr.task_name,
                    "status": tr.status,
                    "color": _status_color(tr.status),
                    "attempts": tr.attempts,
                    "duration": (tr.ended_at - tr.started_at).total_seconds(),
                    "result": tr.result_repr,
                    "error": tr.error,
                    "offset_pct": offset_pct,
                    "width_pct": min(width_pct, 100 - offset_pct),
                }
            )

        run_data = {
            "id": run_row.id,
            "pipeline_name": run_row.pipeline_name,
            "status": run_row.status,
            "color": _status_color(run_row.status),
            "started_at": run_row.started_at,
            "ended_at": run_row.ended_at,
            "duration": total_duration,
        }

    return templates.TemplateResponse(request, "run_detail.html", {"run": run_data, "tasks": tasks})
