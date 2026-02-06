from datetime import timezone, timedelta

from iceloadberg.core.windowing import parse_utc_iso, generate_windows


def test_generate_monthly_chunk_weeks():
    configuration = {
        "mode": "monthly_chunks",
        "start": "2025-01-01T00:00:00Z",
        "end": "2025-04-01T00:00:00Z"
    }

    windows = generate_windows(configuration)
    assert len(windows) >= 12
    assert windows[0].start.year == 2025
    assert windows[0].start.tzinfo == timezone.utc
    assert all(week.start < week.end for week in windows)
    # coverage: should start exactly at configuration starts
    assert windows[0].start == parse_utc_iso(configuration["start"])
    # and end exactly at configuration ends for the last window
    assert windows[-1].end == parse_utc_iso(configuration["end"])


def test_monthly_chunks_with_chunk_days():
    configuration = {
        "mode": "monthly_chunks",
        "start": "2025-01-01T00:00:00Z",
        "end": "2025-02-01T00:00:00Z",
        "chunk_days": 10
    }
    windows = generate_windows(configuration)
    assert len(windows) == 4

def test_end_exclusive_includes_last_instant():
    cfg = {
        "mode": "monthly_chunks",
        "chunk_days": 7,
        "start": "2024-01-01T00:00:00Z",
        "end": "2024-02-01T00:00:00Z",
    }
    windows = generate_windows(cfg)
    start = parse_utc_iso(cfg["start"])
    end = parse_utc_iso(cfg["end"])

    last = end - timedelta(microseconds=1)
    assert start <= last < end
    assert any(week.start <= last < week.end for week in windows)


def test_monthly_chunks_cover_exact_range():
    cfg = {
        "mode": "monthly_chunks",
        "chunk_days": 7,
        "start": "2024-01-01T00:00:00Z",
        "end": "2024-02-01T00:00:00Z",
    }
    windows = generate_windows(cfg)
    start = parse_utc_iso(cfg["start"])
    end = parse_utc_iso(cfg["end"])

    assert windows[0].start == start
    assert windows[-1].end == end
    # no gaps, no overlaps
    for week in range(len(windows) - 1):
        assert windows[week].end == windows[week + 1].start
    # total coverage duration matches exactly
    total = sum((w.end - w.start for w in windows), timedelta(0))
    assert total == (end - start)
    # each chunk is <= 7 days, except maybe last if months don't divide evenly
    assert all((week.end - week.start) <= timedelta(days=7) for week in windows)
