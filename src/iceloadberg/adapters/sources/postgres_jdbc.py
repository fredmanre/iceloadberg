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
