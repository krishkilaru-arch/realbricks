from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
else:
    SparkSession = Any


class StateStore(Protocol):
    def write_event(self, deal_id: str, stage: str, payload: dict[str, Any]) -> None:
        ...


@dataclass
class DeltaStateStore:
    spark: SparkSession
    table_name: str

    def _ensure_table(self) -> None:
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                event_time TIMESTAMP,
                deal_id STRING,
                stage STRING,
                payload_json STRING
            ) USING DELTA
            """
        )

    def write_event(self, deal_id: str, stage: str, payload: dict[str, Any]) -> None:
        self._ensure_table()
        row = [{
            "event_time": datetime.now(timezone.utc),
            "deal_id": deal_id,
            "stage": stage,
            "payload_json": json.dumps(payload, default=str),
        }]
        self.spark.createDataFrame(row).write.mode("append").saveAsTable(self.table_name)


@dataclass
class LakebaseStateStore:
    """Optional Lakebase-backed state store via Postgres connection string.

    Requires psycopg to be installed on the job cluster and a valid lakebase DSN.
    """

    dsn: str
    table_name: str = "deal_state"

    def _connect(self):
        try:
            import psycopg  # type: ignore
        except Exception as exc:
            raise RuntimeError(
                "Lakebase backend requires psycopg package. Install psycopg on Databricks cluster."
            ) from exc
        return psycopg.connect(self.dsn)

    def _ensure_table(self, conn) -> None:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    event_time TIMESTAMPTZ NOT NULL,
                    deal_id TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    payload_json JSONB NOT NULL
                )
                """
            )
        conn.commit()

    def write_event(self, deal_id: str, stage: str, payload: dict[str, Any]) -> None:
        with self._connect() as conn:
            self._ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {self.table_name} (event_time, deal_id, stage, payload_json)
                    VALUES (%s, %s, %s, %s::jsonb)
                    """,
                    (
                        datetime.now(timezone.utc),
                        deal_id,
                        stage,
                        json.dumps(payload, default=str),
                    ),
                )
            conn.commit()

