"""flowctl's command-line interface.

Every command here does exactly one thing: open a session via
app_layer.get_session(), call one function in flowctl.app.application,
and render the result with rich. No command talks to flowctl.core or
flowctl.storage directly -- app_layer.get_session() is what makes that
strictly true, not just true of the business logic underneath it.
"""
from __future__ import annotations

import time
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from flowctl.app import application as app_layer
from flowctl.scheduler.scheduler import Scheduler

app = typer.Typer(
    help="flowctl: a small, local Python task orchestrator.",
    no_args_is_help=True,
)
scheduler_app = typer.Typer(help="Run or single-step the background scheduler.")
app.add_typer(scheduler_app, name="scheduler")

dashboard_app = typer.Typer(help="Run the web dashboard.")
app.add_typer(dashboard_app, name="dashboard")

console = Console()

STATUS_COLORS = {
    "success": "green",
    "failed": "red",
    "skipped": "yellow",
}


def _print_run(run_row) -> None:
    console.print(f"[bold]Pipeline:[/bold] {run_row.pipeline_name}   [bold]Run ID:[/bold] {run_row.id}")
    for tr in run_row.task_results:
        color = STATUS_COLORS.get(tr.status, "white")
        line = f"  [{color}]{tr.status.upper():8}[/{color}] {tr.task_name}  (attempts={tr.attempts})"
        if tr.error:
            line += f"  [red]error={tr.error}[/red]"
        console.print(line)
    overall_color = STATUS_COLORS.get(run_row.status, "white")
    console.print(f"[bold {overall_color}]Overall: {run_row.status.upper()}[/bold {overall_color}]")


@app.command()
def run(
    file: str = typer.Argument(..., help="Path to a .py file defining a Pipeline."),
    attr: Optional[str] = typer.Option(
        None, "--attr", help="Name of the Pipeline variable, if the file defines more than one."
    ),
):
    """Execute a pipeline right now and record the run.

    Works whether or not the pipeline has ever been registered --
    ad-hoc runs are recorded in history just like registered ones.
    """
    try:
        with app_layer.get_session() as session:
            run_row = app_layer.run_from_file(file, session, attr=attr)
            _print_run(run_row)
            failed = run_row.status != "success"
    except (FileNotFoundError, ValueError) as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1)

    if failed:
        raise typer.Exit(code=1)


@app.command(name="register")
def register_cmd(
    file: str = typer.Argument(..., help="Path to a .py file defining a Pipeline."),
    schedule: Optional[str] = typer.Option(
        None,
        "--schedule",
        help=(
            "Cron expression, e.g. '0 6 * * *'. Used by the scheduler (Phase 4). "
            "Omit entirely to leave an existing schedule untouched when "
            "re-registering; pass --schedule \"\" to clear it on purpose."
        ),
    ),
    attr: Optional[str] = typer.Option(
        None, "--attr", help="Name of the Pipeline variable, if the file defines more than one."
    ),
):
    """Register a pipeline as a named, schedulable entity.

    This does NOT run the pipeline -- it just makes it show up in
    `flowctl status` and become visible to the scheduler. Re-registering
    an already-known pipeline without --schedule leaves its existing
    schedule untouched (it does NOT get wiped back to "none").
    """
    # Typer gives us None both when --schedule was omitted and can't
    # distinguish that from "the user typed --schedule with no value"
    # (which isn't really expressible on a command line anyway). So:
    # omitted -> UNSET (leave existing schedule alone), empty string ->
    # explicit clear, anything else -> the new schedule.
    if schedule is None:
        schedule_arg = app_layer.UNSET
    elif schedule == "":
        schedule_arg = None
    else:
        schedule_arg = schedule

    try:
        with app_layer.get_session() as session:
            pipeline_row = app_layer.register(file, session, attr=attr, schedule=schedule_arg)
    except (FileNotFoundError, ValueError) as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1)

    console.print(
        f"[green]Registered[/green] pipeline '{pipeline_row.name}' "
        f"(schedule={pipeline_row.schedule or 'none'})"
    )


@app.command()
def status():
    """List registered pipelines with their most recent run status,
    plus any recent ad-hoc runs of pipelines that were never registered.
    """
    with app_layer.get_session() as session:
        pipelines = app_layer.list_pipelines(session)

        table = Table(title="Registered pipelines")
        table.add_column("Name")
        table.add_column("Schedule")
        table.add_column("Last run status")
        table.add_column("Last run ended")

        for p in pipelines:
            recent = app_layer.list_runs(session, pipeline_name=p.name, limit=1)
            last = recent[0] if recent else None
            status_str = last.status if last else "never run"
            color = STATUS_COLORS.get(status_str, "white")
            table.add_row(
                p.name,
                p.schedule or "-",
                f"[{color}]{status_str}[/{color}]",
                str(last.ended_at) if last else "-",
            )
        console.print(table)

        registered_names = {p.name for p in pipelines}
        adhoc_runs = [
            r for r in app_layer.list_runs(session, limit=20) if r.pipeline_name not in registered_names
        ]
        if adhoc_runs:
            console.print("\n[bold]Recent ad-hoc runs (not registered):[/bold]")
            for r in adhoc_runs[:10]:
                color = STATUS_COLORS.get(r.status, "white")
                console.print(f"  Run {r.id}: {r.pipeline_name}  [{color}]{r.status}[/{color}]  ended={r.ended_at}")


@app.command()
def logs(run_id: int = typer.Argument(..., help="Run ID, shown after `flowctl run` or in `flowctl status`.")):
    """Show per-task detail (status, attempts, result, error) for one run."""
    with app_layer.get_session() as session:
        run_row = app_layer.get_run(session, run_id)
        if run_row is None:
            console.print(f"[red]No run with id {run_id}[/red]")
            raise typer.Exit(code=1)

        console.print(f"[bold]Run {run_row.id}[/bold] -- pipeline={run_row.pipeline_name} status={run_row.status}")
        console.print(f"started={run_row.started_at}  ended={run_row.ended_at}")
        for tr in run_row.task_results:
            color = STATUS_COLORS.get(tr.status, "white")
            duration = (tr.ended_at - tr.started_at).total_seconds()
            console.print(
                f"  [{color}]{tr.status.upper():8}[/{color}] {tr.task_name}  "
                f"attempts={tr.attempts}  duration={duration:.3f}s"
            )
            if tr.result_repr:
                console.print(f"    result: {tr.result_repr}")
            if tr.error:
                console.print(f"    error: {tr.error}")


def _print_fired(fired: list[dict]) -> None:
    if not fired:
        console.print("[yellow]No pipelines were due.[/yellow]")
        return
    for f in fired:
        color = STATUS_COLORS.get(f["status"], "white")
        console.print(
            f"[bold]Fired[/bold] '{f['pipeline']}' -> Run {f['run_id']} "
            f"[{color}]{f['status'].upper()}[/{color}]"
        )


@scheduler_app.command("tick")
def scheduler_tick():
    """Run exactly one scheduler check right now and report what fired.

    Useful for testing/demoing without waiting for real time to pass:
    register a pipeline with a schedule, then call this repeatedly (or
    combine with a short --tick-seconds `scheduler start` run) to watch
    it become due.
    """
    fired = Scheduler().tick()
    _print_fired(fired)


@scheduler_app.command("start")
def scheduler_start(
    tick_seconds: float = typer.Option(
        5.0, "--tick-seconds", help="How often to check for due pipelines, in seconds."
    ),
):
    """Start the scheduler and leave it running until Ctrl+C.

    Uses the exact same run_and_record() path as `flowctl run` -- the
    scheduler has no executor of its own, it just decides *when* to
    call the same thing the CLI calls on demand.
    """
    console.print(
        f"[bold]Scheduler started[/bold] (checking every {tick_seconds}s). Press Ctrl+C to stop."
    )
    scheduler = Scheduler(tick_seconds=tick_seconds)
    try:
        while True:
            fired = scheduler.tick()
            if fired:  # stay quiet on ticks where nothing was due
                _print_fired(fired)
            time.sleep(tick_seconds)
    except KeyboardInterrupt:
        console.print("\n[bold]Scheduler stopped.[/bold]")


@dashboard_app.command("start")
def dashboard_start(
    host: str = typer.Option("127.0.0.1", "--host"),
    port: int = typer.Option(8000, "--port"),
):
    """Start the web dashboard (reads/writes the exact same DB as the
    CLI and scheduler -- it's another door into the same history, not
    a separate system).
    """
    import uvicorn

    console.print(f"[bold]Dashboard starting[/bold] at http://{host}:{port}  (Ctrl+C to stop)")
    uvicorn.run("flowctl.web.app:app", host=host, port=port)


if __name__ == "__main__":  # pragma: no cover
    app()
