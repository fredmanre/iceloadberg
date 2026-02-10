from __future__ import annotations

from typing import Protocol, runtime_checkable

from iceloadberg.ports.window import Window


@runtime_checkable
class StateStore(Protocol):
    """
    Port: tracks progress for each (job_id, dataset, window).

    This provides:
      - idempotency (skip DONE windows)
      - concurrency safety (only one worker claims a window)
      - auditability (counts, fingerprints, errors)
    """

    def claim(self, job_id: str, dataset: str, window: Window, fingerprint: str) -> bool:
        """
        Try to claim the window for processing.
        Returns True only if the caller owns the claim.
        """
        pass

    def mark_done(
        self,
        job_id: str,
        dataset: str,
        window: Window,
        src_count: int,
        dst_count: int,
    ) -> None:
        """Mark window DONE with validation metrics."""
        pass

    def mark_failed(self, job_id: str, dataset: str, window: Window, error: str) -> None:
        """Mark window FAILED with last error message."""
        pass
