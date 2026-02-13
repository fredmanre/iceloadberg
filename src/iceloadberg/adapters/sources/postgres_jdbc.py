from __future__ import annotations

import hashlib

from typing import Any

from iceloadberg.ports.sourcer import Source
from iceloadberg.ports.window import Window

try:
    from pyspark.sql import DataFrame, SparkSession
except:  # pragma: no cover
    DataFrame = object  # type: ignore
    SparkSession = object  # type: ignore


class JDBCSource(Source):
    """Reads windowed data from postgres/Timescale using Spark JDBC."""
    def __init__(self, config: dict[str, Any]):
        self.config = config
        self.query_template = config["query_template"]

    def fingerprint(self, window: Window) -> str:
        """Useful for auditing and safe retries: if SQL or conn changes, fingerprint changes."""
        payload = (
            f"url={self.config.get('jdbc_url')};"
            f"user={self.config.get('user')};"
            f"driver={self.config.get('driver','')};"
            f"fetch_size={self.config.get('fetch_size','')};"
            f"template={self.query_template};"
            f"start={window.start.isoformat()};end={window.end.isoformat()}"
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def read_window(self, spark: SparkSession, window: Window) -> DataFrame:
        range_start = window.start.isoformat().replace("+00:00", "Z")
        range_end = window.end.isoformat().replace("+00:00", "Z")
        sql = self.query_template.format(start=range_start, end=range_end)

        reader = (
            spark.read.format("jdbc")
            .option("url", self.config["jdbc_url"])
            .option("user", self.config["user"])
            .option("password", self.config["password"])
            .option("driver", self.config.get("driver", "org.postgresql.Driver"))
            .option("fetchsize", str(self.config.get("fetch_size", 10_000)))
            .option("query", sql)
        )
        return reader.load()
