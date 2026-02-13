from __future__ import annotations

import psycopg

from typing import Any

from iceloadberg.ports.state_store import StateStore
from iceloadberg.ports.window import Window


class PostgresStateStore(StateStore):
    """
    Postgres-backed state store using a migration table control.
    Requires a unique constraint on (job_id, dataset, range_start, range_end).
    """

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.dsn = config["dsn"]
        self.schema = config.get("schema", "public")
        self.table = config.get("table", "migration_control_trades")

    def _fqtn(self) -> str:
        """Fully-qualified table name."""
        return f"{self.schema}.{self.table}"

    def claim(self, job_id: str, dataset: str, window: Window, fingerprint: str) -> bool:
        """
        Claim strategy:
          - INSERT RUNNING if new
          - whether exists and status in (PENDING, FAILED) -> set RUNNING
          - if DONE/RUNNING -> do nothing and return False
        """
        with psycopg.connect(self.dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                        INSERT INTO {self._fqtn()}
                          (job_id, dataset, range_start, range_end, status, fingerprint, started_at)
                        VALUES
                          (%s, %s, %s, %s, 'RUNNING', %s, NOW())
                        ON CONFLICT (job_id, dataset, range_start, range_end)
                        DO UPDATE SET
                          status = CASE
                            WHEN {self._fqtn()}.status IN ('FAILED','PENDING') THEN 'RUNNING'
                            ELSE {self._fqtn()}.status
                          END,
                          fingerprint = CASE
                            WHEN {self._fqtn()}.status IN ('FAILED','PENDING') THEN EXCLUDED.fingerprint
                            ELSE {self._fqtn()}.fingerprint
                          END,
                          started_at = CASE
                            WHEN {self._fqtn()}.status IN ('FAILED','PENDING') THEN NOW()
                            ELSE {self._fqtn()}.started_at
                          END
                        RETURNING status;
                        """,
                    (job_id, dataset, window.start, window.end, fingerprint),
                )
                status = cursor.fetchone()[0]
                return status == "RUNNING"

    def mark_done(self,
                  job_id: str,
                  dataset: str,
                  window: Window,
                  source_count: int,
                  destination_count: int) -> None:
        """Update status to DONE and record counts."""

        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                        UPDATE {self._fqtn()}
                        SET status='DONE',
                            src_row_count=%s,
                            dst_row_count=%s,
                            finished_at=NOW()
                        WHERE job_id=%s AND dataset=%s AND range_start=%s AND range_end=%s;
                        """,
                    (source_count, destination_count, job_id, dataset, window.start, window.end),
                )

    def mark_failed(self, job_id: str, dataset: str, window: Window, error: str) -> None:
        # Keep last_error manageable; you can truncate server-side if needed.
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                        UPDATE {self._fqtn()}
                        SET status='FAILED',
                            last_error=%s,
                            finished_at=NOW()
                        WHERE job_id=%s AND dataset=%s AND range_start=%s AND range_end=%s;
                        """,
                    (error, job_id, dataset, window.start, window.end),
                )
