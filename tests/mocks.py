from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from iceloadberg.ports.window import Window


@dataclass
class FakeDataFrame:
    rows: int

    def count(self) -> int:
        return self.rows


class FakeSource:

    def __init__(self, rows_per_window: int = 10) -> None:
        self.rows_per_window = rows_per_window
        self.read_calls: list[Window] = []

    def fingerprint(self, window: Window) -> str:
        return f"fake:{window.key()}"

    def read_window(self, spark: Any, window: Window) -> FakeDataFrame:
        self.read_calls.append(window)
        return FakeDataFrame(self.rows_per_window)


class FakeTarget:

    def __init__(self) -> None:
        self.written: list[tuple[Window, int]] = []
        self.counts: dict[str, int] = {}

    def write_window(self, df: FakeDataFrame, window: Window) -> None:
        self.written.append((window, df.rows))
        # simulate destination having exactly what we wrote
        self.counts[window.key()] = df.rows

    def count_window(self, spark: Any, window: Window) -> int:
        return self.counts.get(window.key(), 0)


class FakeStateStore:

    def __init__(self) -> None:
        self.claimed: set[str] = set()
        self.done: list[tuple[str, str, Window, int, int]] = []
        self.failed: list[tuple[str, str, Window, str]] = []
        # control behavior per window
        self.force_claim_false_for: set[str] = set()

    def claim(self, job_id: str, dataset: str, window: Window, fingerprint: str) -> bool:
        key = f"{job_id}:{dataset}:{window.key()}"
        if window.key() in self.force_claim_false_for:
            return False
        if key in self.claimed:
            return False
        self.claimed.add(key)
        return True

    def mark_done(self, job_id: str, dataset: str, window: Window, src_count: int, dst_count: int) -> None:
        self.done.append((job_id, dataset, window, src_count, dst_count))

    def mark_failed(self, job_id: str, dataset: str, window: Window, error: str) -> None:
        self.failed.append((job_id, dataset, window, error))
