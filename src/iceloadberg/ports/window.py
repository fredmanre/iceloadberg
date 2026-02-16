from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(frozen=True, slots=True)
class Window:
    """Half-open interval [start, end] in UTC."""
    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        if self.start.tzinfo is None or self.end.tzinfo is None:
            raise ValueError("start and end must be timezone-aware")
        if self.start.tzinfo != timezone.utc or self.end.tzinfo != timezone.utc:
            raise ValueError("start and end must be in UTC")
        if self.end <= self.start:
            raise ValueError("start must be < window.end")

    def key(self) -> str:
        return f"{self.start.isoformat()}__{self.end.isoformat()}"