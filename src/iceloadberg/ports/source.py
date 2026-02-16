from __future__ import annotations
from typing import Protocol, runtime_checkable, TYPE_CHECKING

from iceloadberg.ports.window import Window



if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
else:
    DataFrame = object
    SparkSession = object


@runtime_checkable
class Source(Protocol):
    """
    Port: a source system that can produce a Spark DataFrame for a time window.

    Examples (future):
      - JDBC (Timescale/Postgres)
      - CSV/Parquet in S3 (not available yet)
      - InfluxDB (via API/export) (not available yet)
    """

    def read_window(self, spark: SparkSession, window: Window) -> DataFrame:
        """Return records for [window.start, window.end) in UTC."""
        raise NotImplementedError("subclass must implement read_window")

    def fingerprint(self, window: Window) -> str:
        """
        Deterministic description of the extraction for audit + safe retries.
        """
        raise NotImplementedError("subclass must implement fingerprint")