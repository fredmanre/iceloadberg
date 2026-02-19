from __future__ import annotations

import argparse

from iceloadberg.config import load_config
from iceloadberg.core.transform import Transformer
from iceloadberg.core.job import MigrationJob, JobContext
from iceloadberg.ports.registry import Registry
from iceloadberg.adapters.sources.postgres_jdbc import JDBCSource
from iceloadberg.adapters.targets.iceberg import IcebergTarget
from iceloadberg.adapters.states.postgres import PostgresStateStore


try:
    from pyspark.sql import SparkSession
except Exception:
    raise RuntimeError("PySpark is required to run the CLI. Please install pyspark.")


def build_spark(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def main():
    parser = argparse.ArgumentParser(description="Run an Iceloadberg migration job")
    parser.add_argument("-c",
                        "--config",
                        required=True,
                        help="Path to the job configuration file")
    args = parser.parse_args()

    config = load_config(args.config)
    # registries (plugin-ready)
    sources = Registry()
    targets = Registry()
    states = Registry()

    sources.register("jdbc", lambda c: JDBCSource(c))
    targets.register("iceberg", lambda c: IcebergTarget(c))
    states.register("postgres", lambda c: PostgresStateStore(c))

    job_id = config.get("job", {}).get("id") or config.get("job_id")
    if not job_id:
        raise ValueError("Missing required job id. Use 'job.id' (preferred) or 'job_id'.")

    spark = build_spark(job_id)
    context = JobContext(
        job_id=job_id,
        dataset=config["dataset"]["name"]
    )

    source = sources.create(config["source"])
    target = targets.create(config["target"])
    state = states.create(config["state_store"])

    job = MigrationJob(
        spark=spark,
        context=context,
        config=config,
        source=source,
        target=target,
        state_store=state,
        transform=Transformer().apply,
        validate_counts=True
    )
    job.run()
