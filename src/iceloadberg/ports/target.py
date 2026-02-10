from __future__ import annotations
from typing import Protocol, runtime_checkable

from iceloadberg.ports.window import Window

try:
    from pyspark.sql import DataFrame, SparkSession
except:
    DataFrame = object
    SparkSession = object


@runtime_checkable
class Target(Protocol):
    """
    Port: a destination that can store windowed data and be validated.
    For now: Iceberg tables in S3 (Glue Catalog).
    """

    def write_window(self, df: DataFrame, window: Window) -> None:
        """
        Persist the window's data.
        Must be idempotent under retries (recommended: overwrite partitions).
        """
        pass

    def count_window(self, spark: SparkSession, window: Window) -> int:
        """Count records in destination for the given window (for validation)."""
        pass
