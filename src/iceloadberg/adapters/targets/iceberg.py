from __future__ import annotations
from typing import TYPE_CHECKING, Any

from iceloadberg.ports.target import Target
from iceloadberg.ports.window import Window

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
else:
    try:
        from pyspark.sql import DataFrame, SparkSession
    except Exception:  # pragma: no cover
        DataFrame = object  # type: ignore
        SparkSession = object  # type: ignore


class IcebergTarget(Target):
    """
    Writes to an existing Iceberg table (Glue catalog) and validates counts via spark.
    """
    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.catalog = config["catalog"]
        self.database = config["database"]
        self.table = config["table"]
        self.timestamp_column = config.get("timestamp_column", "trade_timestamp")
        self.write_mode = config.get("write_mode", "overwrite_partitions")

    @property
    def full_name(self) -> str:
        return f"{self.catalog}.{self.database}.{self.table}"

    def write_window(self, df: DataFrame, window: Window) -> None:
        if self.write_mode == "append":
            df.writeTo(self.full_name).append()
            return

        if self.write_mode == "overwrite_partitions":
            # Idempotent under retries (for partitioned tables))
            df.writeTo(self.full_name).overwritePartitions()
            return

        raise ValueError(f"Unsupported write_mode: {self.write_mode}")

    def count_window(self, spark: SparkSession, window: Window) -> int:
        # Validate by counting records in the destination for this window
        start = window.start.isoformat().replace("+00:00", "Z")
        end = window.end.isoformat().replace("+00:00", "Z")
        predicate = (
            f"{self.timestamp_column} >= timestamp '{start}' AND "
            f"{self.timestamp_column} < timestamp '{end}'"
        )
        return spark.table(self.full_name).where(predicate).count()
