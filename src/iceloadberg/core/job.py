from __future__ import annotations
from dataclasses import dataclass
from typing import Callable, Optional

from iceloadberg.core.windowing import generate_windows
from iceloadberg.ports.source import Source
from iceloadberg.ports.state_store import StateStore
from iceloadberg.ports.target import Target
from iceloadberg.ports.window import Window


@dataclass(frozen=True, slots=True)
class JobContext:
    job_id: str
    dataset: str


class MigrationJob:
    """
    Core orchestrator (hexagonal): fixed algorithm, pluggable ports.

    It does NOT know:
    - Timescale/JDBC
    - Iceberg specifics
    - Postgres details
    - Glue runtime

    It only coordinates ports.
    """

    def __init__(
        self,
        *,
        spark: object,
        context: JobContext,
        config: dict,
        source: Source,
        target: Target,
        state_store: StateStore,
        validate_counts: bool = True,
        transform: Optional[Callable[[object], object]] = None
    ) -> None:

        self.spark = spark
        self.context = context
        self.config = config
        self.source = source
        self.target = target
        self.state_store = state_store
        self.transform = transform or (lambda df: df)
        self.validate_counts = validate_counts

    def run(self) -> None:
        windows = generate_windows(self.config["windowing"])
        for window in windows:
            self._run_window(window)

    def _run_window(self, window: Window) -> None:
        fingerprint = self.source.fingerprint(window)

        claimed = self.state_store.claim(
            self.context.job_id,
            self.context.dataset,
            window,
            fingerprint,
        )
        if not claimed:
            return

        try:
            df = self.source.read_window(self.spark, window)
            df = self.transform(df)
            # count source (we assume df has count() in Spark; fakes can implement it)
            source_count = int(df.count())

            self.target.write_window(df, window)

            destination_count = int(self.target.count_window(self.spark, window))

            if self.validate_counts and destination_count != source_count:
                raise RuntimeError(f"Rowcount mismatch window={window.key()} src {source_count} dst {destination_count}")

            self.state_store.mark_done(
                self.context.job_id,
                self.context.dataset,
                window,
                source_count,
                destination_count,
            )
        except Exception as err:
            self.state_store.mark_failed(
                self.context.job_id,
                self.context.dataset,
                window,
                str(err),
            )
            raise