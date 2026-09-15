"""Proves run_and_record's intermediate "running" status is genuinely
observable by a *separate* connection while execution is still in
progress -- not just present in code, but actually visible to a
concurrent reader, which is what makes the dashboard's live
auto-refresh meaningful rather than cosmetic.
"""
import tempfile
import threading
import time
from pathlib import Path

from flowctl.app.application import run_and_record
from flowctl.core.pipeline import Pipeline
from flowctl.core.task import task
from flowctl.storage.db import get_engine, get_session_factory, init_db
from flowctl.storage.models import Run


def test_running_status_is_visible_to_a_concurrent_reader_mid_execution():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "running_status_check.db"

        writer_engine = get_engine(db_path)
        init_db(writer_engine)
        writer_session = get_session_factory(writer_engine)()

        release_task = threading.Event()

        @task()
        def slow_step():
            # Block until the main thread has confirmed it observed
            # "running", proving the row really was committed and
            # readable before execution finished.
            release_task.wait(timeout=5)
            return "done"

        pipeline = Pipeline("slow_pipeline", [slow_step])

        result_holder = {}

        def _run():
            result_holder["run"] = run_and_record(pipeline, writer_session)

        thread = threading.Thread(target=_run)
        thread.start()

        # Poll from a completely separate engine/connection, simulating
        # a different HTTP request hitting the dashboard mid-run.
        reader_engine = get_engine(db_path)
        reader_session = get_session_factory(reader_engine)()

        observed_running = False
        deadline = time.time() + 5
        while time.time() < deadline:
            row = (
                reader_session.query(Run)
                .filter_by(pipeline_name="slow_pipeline")
                .order_by(Run.id.desc())
                .first()
            )
            if row is not None and row.status == "running":
                observed_running = True
                break
            reader_session.expire_all()
            time.sleep(0.02)

        assert observed_running, "never observed a 'running' row from a separate connection"

        release_task.set()
        thread.join(timeout=5)

        reader_session.expire_all()
        final_row = (
            reader_session.query(Run)
            .filter_by(pipeline_name="slow_pipeline")
            .order_by(Run.id.desc())
            .first()
        )
        assert final_row.status == "success"
        assert result_holder["run"].status == "success"

        writer_session.close()
        writer_engine.dispose()
        reader_session.close()
        reader_engine.dispose()
