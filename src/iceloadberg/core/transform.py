from __future__ import annotations

from dataclasses import dataclass

try:
    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
except ImportError:  # pragma: no cover
    DataFrame = object  # type: ignore


@dataclass(slots=True)
class Transformer:
    """
    Central place for schema normalization.

    Keep it deterministic and explicit:
      - cast timestamps
      - cast decimals
      - enforce required columns
    """

    timestamp_col: str = "trade_timestamp"
    dispatcher_col: str = "dispatcher_time"
    price_col: str = "price"
    volume_col: str = "volume"

    def apply(self, df: DataFrame) -> DataFrame:
        columns = set(df.columns)
        # required column checks (fail fast)
        required = [self.timestamp_col]
        missing = [column for column in required if column not in columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}. Available={sorted(columns)}")

        # timestamps
        if self.timestamp_col in columns:
            df = df.withColumn(self.timestamp_col, F.to_timestamp(F.col(self.timestamp_col)))
        if self.dispatcher_col in columns:
            df = df.withColumn(self.dispatcher_col, F.to_timestamp(F.col(self.dispatcher_col)))

        # decimals
        if self.price_col in columns:
            df = df.withColumn(self.price_col, F.col(self.price_col).cast(T.DecimalType(18, 8)))
        if self.volume_col in columns:
            df = df.withColumn(self.volume_col, F.col(self.volume_col).cast(T.DecimalType(26, 8)))

        return df
