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
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

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
    "success": "#1a7f37",
    "failed": "#cf222e",
    "skipped": "#9a6700",
}


def _status_color(status: Optional[str]) -> str:
    return STATUS_COLORS.get(status or "", "#6e7781")


@app.get("/")
def index(request: Request):
    with app_layer.get_session() as session:
        pipelines = app_layer.list_pipelines(session)
        rows = []
        for p in pipelines:
            recent = app_layer.list_runs(session, pipeline_name=p.name, limit=1)
            last = recent[0] if recent else None
            rows.append(
                {
                    "name": p.name,
                    "schedule": p.schedule,
                    "enabled": p.enabled,
                    "kind": "code" if p.load_ref else "linear job",
                    "last_status": last.status if last else "never run",
                    "last_ended": last.ended_at if last else None,
                    "color": _status_color(last.status if last else None),
                }
            )

        registered_names = {p.name for p in pipelines}
        adhoc = [
            r
            for r in app_layer.list_runs(session, limit=20)
            if r.pipeline_name not in registered_names
        ]

    return templates.TemplateResponse(
        request,
        "index.html",
        {"pipelines": rows, "adhoc_runs": adhoc, "status_color": _status_color},
    )


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
        return RedirectResponse(url=f"/jobs/new?error={exc}", status_code=303)

    return RedirectResponse(url=f"/pipelines/{name}", status_code=303)


@app.get("/pipelines/{name}")
def pipeline_detail(request: Request, name: str, error: Optional[str] = None):
    with app_layer.get_session() as session:
        pipeline_row = app_layer.get_pipeline(session, name)
        if pipeline_row is None:
            raise HTTPException(status_code=404, detail=f"No pipeline named {name!r}")

        pipeline_obj = app_layer.load_pipeline_for_row(pipeline_row)
        levels = pipeline_obj.execution_plan()
        statuses = app_layer.latest_task_statuses(session, name)

        diagram_levels = [
            [
                {
                    "name": t.name,
                    "status": statuses.get(t.name, "never run"),
                    "color": _status_color(statuses.get(t.name)),
                }
                for t in level
            ]
            for level in levels
        ]

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
            "diagram_levels": diagram_levels,
            "runs": run_rows,
            "error": error,
        },
    )


@app.post("/pipelines/{name}/run")
def run_pipeline_now(name: str):
    try:
        with app_layer.get_session() as session:
            app_layer.run_registered(name, session)
    except ValueError as exc:
        return RedirectResponse(url=f"/pipelines/{name}?error={exc}", status_code=303)
    return RedirectResponse(url=f"/pipelines/{name}", status_code=303)


@app.post("/pipelines/{name}/schedule")
def update_pipeline_schedule(name: str, schedule: str = Form("")):
    schedule_value = schedule.strip() or None
    try:
        with app_layer.get_session() as session:
            app_layer.update_schedule(name, schedule_value, session)
    except ValueError as exc:
        return RedirectResponse(url=f"/pipelines/{name}?error={exc}", status_code=303)
    return RedirectResponse(url=f"/pipelines/{name}", status_code=303)


@app.post("/pipelines/{name}/toggle")
def toggle_pipeline(name: str):
    with app_layer.get_session() as session:
        pipeline_row = app_layer.get_pipeline(session, name)
        if pipeline_row is None:
            raise HTTPException(status_code=404, detail=f"No pipeline named {name!r}")
        app_layer.set_enabled(name, not pipeline_row.enabled, session)
    return RedirectResponse(url=f"/pipelines/{name}", status_code=303)


@app.get("/runs/{run_id}")
def run_detail(request: Request, run_id: int):
    with app_layer.get_session() as session:
        run_row = app_layer.get_run(session, run_id)
        if run_row is None:
            raise HTTPException(status_code=404, detail=f"No run with id {run_id}")

        tasks = [
            {
                "name": tr.task_name,
                "status": tr.status,
                "color": _status_color(tr.status),
                "attempts": tr.attempts,
                "duration": (tr.ended_at - tr.started_at).total_seconds(),
                "result": tr.result_repr,
                "error": tr.error,
            }
            for tr in run_row.task_results
        ]
        run_data = {
            "id": run_row.id,
            "pipeline_name": run_row.pipeline_name,
            "status": run_row.status,
            "color": _status_color(run_row.status),
            "started_at": run_row.started_at,
            "ended_at": run_row.ended_at,
        }

    return templates.TemplateResponse(request, "run_detail.html", {"run": run_data, "tasks": tasks})
