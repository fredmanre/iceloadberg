from __future__ import annotations

from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta

from iceloadberg.ports.window import Window


def parse_utc_iso(_timestamp: str) -> datetime:
    """Parse an ISO timestamp in UTC."""
    if not isinstance(_timestamp, str):
        raise ValueError("Expected string timestamp")
    if not _timestamp.endswith("Z"):
        raise ValueError("Only UTC timestamps supported")
    _timestamp = _timestamp.replace("Z", "+00:00")
    _datetime = datetime.fromisoformat(_timestamp).astimezone(timezone.utc)
    return _datetime


def generate_months(start: datetime, end: datetime) -> list[Window]:
    """Generates month windows [month_start, next_month_start)."""
    output = []  # list[Window]
    current = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    while current < end:
        _next = (current + relativedelta(months=1)).replace(day=1)
        output.append(Window(current, min(_next, end)))
        current = _next
    return output


def split_month_into_chunk_days(month_window: Window, chunk_days: int = 7) -> list[Window]:
    """Splits a month window into chunks of chunk_days."""
    start = month_window.start
    end = month_window.end

    output = []  # list[Window]
    current, step = start, timedelta(days=chunk_days)

    while current < end:
        _next = min(current + step, end)
        output.append(Window(current, _next))
        current = _next
    return output


def generate_windows(cfg: dict) -> list[Window]:
    """
    Config example:
      windowing:
        mode: "monthly_chunks"
        start: "2025-01-01T00:00:00Z"
        end: "2025-04-01T00:00:00Z"
    """
    mode = cfg["mode"]
    start = parse_utc_iso(cfg["start"])
    end = parse_utc_iso(cfg["end"])

    if mode == "monthly_chunks":
        chunks_days = int(cfg.get("chunk_days", 7))
        if chunks_days <= 0:
            raise ValueError("chunk_days must be > 0")
        months = generate_months(start, end)
        output: list[Window] = []
        for month in months:
            output.extend(split_month_into_chunk_days(
                month,
                chunk_days=chunks_days,
            ))
        return output

    raise ValueError(f"Unsupported windowing mode: {mode}")
