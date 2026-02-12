from __future__ import annotations

import json
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterator, TYPE_CHECKING

from realbricks.settings import Settings

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
else:
    SparkSession = Any


@dataclass(frozen=True)
class RunContext:
    run_id: str
    run_type: str
    started_at: datetime


class RunTracker:
    def __init__(self, spark: SparkSession, settings: Settings):
        self.spark = spark
        self.settings = settings

    def ensure_tables(self) -> None:
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.settings.run_history_table} (
                run_id STRING,
                run_type STRING,
                status STRING,
                started_at TIMESTAMP,
                finished_at TIMESTAMP,
                duration_ms BIGINT,
                details_json STRING
            ) USING DELTA
            """
        )
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.settings.source_history_table} (
                run_id STRING,
                source_name STRING,
                source_table STRING,
                target_table STRING,
                status STRING,
                row_count BIGINT,
                details_json STRING,
                logged_at TIMESTAMP
            ) USING DELTA
            """
        )

    def log_source_event(
        self,
        run_id: str,
        source_name: str,
        source_table: str,
        target_table: str,
        status: str,
        row_count: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.ensure_tables()
        row = [{
            "run_id": run_id,
            "source_name": source_name,
            "source_table": source_table,
            "target_table": target_table,
            "status": status,
            "row_count": row_count,
            "details_json": json.dumps(details or {}, default=str),
            "logged_at": datetime.now(timezone.utc),
        }]
        self.spark.createDataFrame(row).write.mode("append").saveAsTable(self.settings.source_history_table)

    def _write_run(
        self,
        run_id: str,
        run_type: str,
        status: str,
        started_at: datetime,
        finished_at: datetime,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.ensure_tables()
        duration_ms = int((finished_at - started_at).total_seconds() * 1000)
        row = [{
            "run_id": run_id,
            "run_type": run_type,
            "status": status,
            "started_at": started_at,
            "finished_at": finished_at,
            "duration_ms": duration_ms,
            "details_json": json.dumps(details or {}, default=str),
        }]
        self.spark.createDataFrame(row).write.mode("append").saveAsTable(self.settings.run_history_table)

    @contextmanager
    def track(self, run_type: str) -> Iterator[RunContext]:
        run_id = str(uuid.uuid4())
        started_at = datetime.now(timezone.utc)
        ctx = RunContext(run_id=run_id, run_type=run_type, started_at=started_at)
        try:
            yield ctx
        except Exception as exc:
            self._write_run(
                run_id=run_id,
                run_type=run_type,
                status="failed",
                started_at=started_at,
                finished_at=datetime.now(timezone.utc),
                details={"error": str(exc)},
            )
            raise
        else:
            self._write_run(
                run_id=run_id,
                run_type=run_type,
                status="success",
                started_at=started_at,
                finished_at=datetime.now(timezone.utc),
            )

