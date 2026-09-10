"""Tests for register()'s schedule semantics.

This exists specifically to guard against the bug flagged in review:
re-registering an already-scheduled pipeline without repeating
--schedule used to silently wipe its schedule back to None, which
would make the scheduler (Phase 4) quietly stop firing it.
"""
from pathlib import Path

from flowctl.app.application import UNSET, register
from flowctl.storage.db import get_engine, get_session_factory, init_db
from flowctl.storage.models import Pipeline as PipelineModel

FIXTURE = Path(__file__).parent / "fixtures" / "demo_pipeline.py"


def _fresh_session():
    engine = get_engine(":memory:")
    init_db(engine)
    return get_session_factory(engine)()


def test_first_registration_with_schedule_sets_it():
    session = _fresh_session()
    row = register(str(FIXTURE), session, schedule="0 6 * * *")
    assert row.schedule == "0 6 * * *"


def test_first_registration_without_schedule_leaves_it_none():
    session = _fresh_session()
    row = register(str(FIXTURE), session)  # schedule omitted -> UNSET by default
    assert row.schedule is None


def test_reregistering_without_schedule_preserves_existing_schedule():
    session = _fresh_session()
    register(str(FIXTURE), session, schedule="0 6 * * *")

    # Re-register without passing --schedule at all (the CLI maps this
    # to UNSET). This must NOT wipe the schedule back to None.
    row = register(str(FIXTURE), session)

    assert row.schedule == "0 6 * * *"
    # Confirm it's actually durable, not just an in-memory artifact.
    reloaded = session.query(PipelineModel).filter_by(name=row.name).one()
    assert reloaded.schedule == "0 6 * * *"


def test_reregistering_with_new_schedule_replaces_the_old_one():
    session = _fresh_session()
    register(str(FIXTURE), session, schedule="0 6 * * *")
    row = register(str(FIXTURE), session, schedule="0 18 * * *")
    assert row.schedule == "0 18 * * *"


def test_reregistering_with_explicit_none_clears_the_schedule():
    session = _fresh_session()
    register(str(FIXTURE), session, schedule="0 6 * * *")
    row = register(str(FIXTURE), session, schedule=None)  # explicit clear
    assert row.schedule is None


def test_register_rejects_an_invalid_cron_expression():
    import pytest

    session = _fresh_session()
    with pytest.raises(ValueError, match="not a valid cron expression"):
        register(str(FIXTURE), session, schedule="not a cron string")


def test_unset_sentinel_is_distinct_from_none():
    # Sanity check on the sentinel itself: omitting the argument uses
    # the same UNSET object as the default, so callers can compare
    # against it if they ever need to.
    session = _fresh_session()
    row = register(str(FIXTURE), session, schedule=UNSET)
    assert row.schedule is None
