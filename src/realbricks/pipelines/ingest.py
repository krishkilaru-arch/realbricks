from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TYPE_CHECKING

from realbricks.observability import RunTracker
from realbricks.settings import Settings

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
else:
    DataFrame = Any
    SparkSession = Any


@dataclass(frozen=True)
class SourceConfig:
    name: str
    source_table: str
    target_table: str
    mode: str = "overwrite"
    enabled: bool = True

    @staticmethod
    def from_dict(item: dict[str, Any]) -> "SourceConfig":
        return SourceConfig(
            name=item["name"],
            source_table=item["source_table"],
            target_table=item["target_table"],
            mode=item.get("mode", "overwrite"),
            enabled=item.get("enabled", True),
        )


def _table_exists(spark: SparkSession, table_name: str) -> bool:
    return bool(spark.catalog.tableExists(table_name))


def _apply_row_cap(df: DataFrame, settings: Settings) -> DataFrame:
    if settings.max_rows_per_source and settings.max_rows_per_source > 0:
        return df.limit(settings.max_rows_per_source)
    return df


def ensure_schemas(spark: SparkSession, settings: Settings) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {settings.bronze_prefix}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {settings.silver_prefix}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {settings.gold_prefix}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {settings.ops_prefix}")


def run_ingestion(
    spark: SparkSession,
    settings: Settings,
    sources: list[SourceConfig],
    run_id: str = "ad-hoc",
) -> list[str]:
    ensure_schemas(spark, settings)

    loaded: list[str] = []
    tracker = RunTracker(spark, settings)
    for source in sources:
        if not source.enabled:
            continue
        if not _table_exists(spark, source.source_table):
            message = f"Missing source table: {source.source_table}"
            tracker.log_source_event(
                run_id=run_id,
                source_name=source.name,
                source_table=source.source_table,
                target_table=source.target_table,
                status="missing_source",
                details={"message": message},
            )
            if settings.fail_on_missing_source:
                raise RuntimeError(message)
            print(f"[WARN] {message}; skipping")
            continue

        print(f"[INFO] Ingesting {source.name}: {source.source_table} -> {source.target_table}")
        df = spark.table(source.source_table)
        df = _apply_row_cap(df, settings)
        count = df.count()
        df.write.format("delta").mode(source.mode).saveAsTable(source.target_table)
        tracker.log_source_event(
            run_id=run_id,
            source_name=source.name,
            source_table=source.source_table,
            target_table=source.target_table,
            status="loaded",
            row_count=count,
        )
        loaded.append(source.target_table)

    return loaded


def load_source_configs(config_path: str | Path) -> list[SourceConfig]:
    payload = json.loads(Path(config_path).read_text(encoding="utf-8"))
    sources = [SourceConfig.from_dict(item) for item in payload.get("sources", [])]
    if not sources:
        raise RuntimeError(f"No sources found in config: {config_path}")

    allowed_modes = {"overwrite", "append"}
    bad_modes = [s.name for s in sources if s.mode not in allowed_modes]
    if bad_modes:
        raise RuntimeError(
            f"Unsupported write mode in config for sources {bad_modes}; allowed values: {sorted(allowed_modes)}"
        )

    targets = [s.target_table for s in sources]
    dup_targets = sorted({t for t in targets if targets.count(t) > 1})
    if dup_targets:
        raise RuntimeError(f"Duplicate target_table values in config: {dup_targets}")

    return sources

