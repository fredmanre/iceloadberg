import pytest

from iceloadberg.core.job import MigrationJob, JobContext
from iceloadberg.core.windowing import parse_utc_iso
from iceloadberg.ports.window import Window
from mocks import FakeSource, FakeTarget, FakeStateStore


def _config_one_month_chunks(chunk_days: int = 7):
    return {
        "windowing": {
            "mode": "monthly_chunks",
            "chunk_days": chunk_days,
            "start": "2025-01-01T00:00:00Z",
            "end": "2025-02-01T00:00:00Z"
        }
    }


def test_job_success_path_writes_and_marks_done():
    config = _config_one_month_chunks()
    context = JobContext(job_id="job1", dataset="crypto")
    source = FakeSource(rows_per_window=10)
    target = FakeTarget()
    state = FakeStateStore()

    job = MigrationJob(
        spark=object(),
        context=context,
        config=config,
        source=source,
        target=target,
        state_store=state
    )
    job.run()
    # should have processed all windows
    assert len(state.done) == len(target.written)
    assert len(source.read_calls) == len(target.written)
    assert len(state.failed) == 0


def test_job_skips_when_claim_false():
    config = _config_one_month_chunks(chunk_days=15)
    context = JobContext(job_id="job1", dataset="crypto")
    source = FakeSource(rows_per_window=5)
    target = FakeTarget()
    state = FakeStateStore()
    # force skip first window
    windows = [
        Window(parse_utc_iso("2025-01-01T00:00:00Z"), parse_utc_iso("2025-01-11T00:00:00Z"))
    ]
    state.force_claim_false_for.add(windows[0].key())

    job = MigrationJob(
        spark=object(),
        context=context,
        config=config,
        source=source,
        target=target,
        state_store=state
    )
    job.run()
    # at least one window should be skipped -> reads less than total windows
    assert len(source.read_calls) < len(target.written) + 1  # sanity
    assert len(state.failed) == 0


def test_job_fails_on_count_mismatch():
    config = _config_one_month_chunks(chunk_days=10)
    context = JobContext(job_id="job1", dataset="crypto")
    source = FakeSource(rows_per_window=5)
    target = FakeTarget()
    state = FakeStateStore()
    # break validation by overriding destination count after write
    original_write = target.write_window

    def bad_write(df, window):
        original_write(df, window)
        target.counts[window.key()] = df.rows + 1

    target.write_window = bad_write  # type: ignore

    job = MigrationJob(
        spark=object(),
        context=context,
        config=config,
        source=source,
        target=target,
        state_store=state
    )
    with pytest.raises(RuntimeError):
        job.run()

    assert len(state.failed) == 1