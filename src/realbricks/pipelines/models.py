from __future__ import annotations

from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType

from realbricks.settings import Settings


def _pick(df: DataFrame, *candidates: str, as_type: str = "string") -> F.Column:
    columns = {c.lower(): c for c in df.columns}
    options = [F.col(columns[c.lower()]) for c in candidates if c.lower() in columns]
    if not options:
        expr = F.lit(None)
    else:
        expr = F.coalesce(*options)

    if as_type == "double":
        return expr.cast(DoubleType())
    if as_type == "string":
        return expr.cast(StringType())
    return expr


def _normalize_listing(df: DataFrame, source_name: str) -> DataFrame:
    return df.select(
        _pick(df, "id", "listing_id", "property_id", "event_id").alias("listing_id"),
        _pick(df, "address", "full_address", "street_address", "parcel_address").alias("address"),
        _pick(df, "zip", "zipcode", "postal_code").alias("zip"),
        _pick(df, "city").alias("city"),
        _pick(df, "state").alias("state"),
        _pick(df, "lat", "latitude").cast("double").alias("latitude"),
        _pick(df, "lon", "lng", "longitude").cast("double").alias("longitude"),
        _pick(df, "listPrice", "list_price", "price", "hist_last_sale_price", as_type="double").alias("list_price"),
        _pick(df, "beds", "bedrooms", as_type="double").alias("beds"),
        _pick(df, "baths", "bathrooms", as_type="double").alias("baths"),
        _pick(df, "sqft", "living_area", "livingArea", "building_livingareasqft", as_type="double").alias("sqft"),
        _pick(df, "lot_sqft", "lot_size", "lotsize", as_type="double").alias("lot_sqft"),
        _pick(df, "year_built", "yearbuilt", as_type="double").alias("year_built"),
        _pick(df, "status", "listing_status").alias("listing_status"),
        F.lit(source_name).alias("source_name"),
        F.current_timestamp().alias("ingested_at"),
    )


def _existing_tables(spark: SparkSession, tables: Iterable[str]) -> list[str]:
    return [table for table in tables if spark.catalog.tableExists(table)]


def build_silver_listing_core(spark: SparkSession, settings: Settings, input_tables: list[str]) -> str:
    valid = _existing_tables(spark, input_tables)
    if not valid:
        raise RuntimeError("No valid input tables found for silver model.")

    normalized: list[DataFrame] = []
    for table in valid:
        df = spark.table(table)
        normalized.append(_normalize_listing(df, source_name=table))

    out = normalized[0]
    for df in normalized[1:]:
        out = out.unionByName(df, allowMissingColumns=True)

    out = (
        out.withColumn("listing_id", F.coalesce(F.col("listing_id"), F.sha2(F.concat_ws("||", F.col("address"), F.col("zip"), F.col("source_name")), 256)))
        .dropDuplicates(["listing_id", "source_name"])
    )

    target = settings.silver_listing_table
    out.write.format("delta").mode("overwrite").saveAsTable(target)
    _validate_listing_quality(spark, settings, target)
    return target


def build_gold_property_rankings(spark: SparkSession, settings: Settings) -> str:
    source = settings.silver_listing_table
    if not spark.catalog.tableExists(source):
        raise RuntimeError(f"Missing source table: {source}")

    df = spark.table(source)
    df = (
        df.withColumn("price_per_sqft", F.when(F.col("sqft") > 0, F.col("list_price") / F.col("sqft")))
        .withColumn("home_age", F.when(F.col("year_built").isNotNull(), F.year(F.current_date()) - F.col("year_built")))
        .withColumn("ranking_score", F.expr("""
            coalesce((1000000 / nullif(list_price, 0)), 0) * 0.25 +
            coalesce((sqft / 3000), 0) * 0.20 +
            coalesce((3 - abs(beds - 3)), 0) * 0.15 +
            coalesce((2 - abs(baths - 2)), 0) * 0.15 +
            coalesce((1 - least(coalesce(home_age, 100) / 100, 1)), 0) * 0.25
        """))
    )

    target = settings.gold_property_rank_table
    df.write.format("delta").mode("overwrite").saveAsTable(target)
    return target


def build_gold_market_kpis(spark: SparkSession, settings: Settings) -> str:
    source = settings.silver_listing_table
    if not spark.catalog.tableExists(source):
        raise RuntimeError(f"Missing source table: {source}")

    df = (
        spark.table(source)
        .groupBy("zip")
        .agg(
            F.count("*").alias("listing_count"),
            F.avg("list_price").alias("avg_list_price"),
            F.expr("percentile_approx(list_price, 0.5)").alias("median_list_price"),
            F.avg(F.when(F.col("sqft") > 0, F.col("list_price") / F.col("sqft"))).alias("avg_price_per_sqft"),
        )
        .withColumn("computed_at", F.current_timestamp())
    )

    target = f"{settings.gold_prefix}.market_kpis"
    df.write.format("delta").mode("overwrite").saveAsTable(target)
    return target


def _validate_listing_quality(spark: SparkSession, settings: Settings, table: str) -> None:
    df = spark.table(table)
    total = df.count()
    if total < settings.min_listing_rows:
        raise RuntimeError(
            f"Quality gate failed for {table}: row_count={total} < min_listing_rows={settings.min_listing_rows}"
        )

    quality = (
        df.select(
            F.avg(F.when(F.col("address").isNotNull(), F.lit(1.0)).otherwise(F.lit(0.0))).alias("address_ratio"),
            F.avg(F.when(F.col("list_price").isNotNull(), F.lit(1.0)).otherwise(F.lit(0.0))).alias("price_ratio"),
        )
        .collect()[0]
    )
    address_ratio = float(quality["address_ratio"] or 0.0)
    price_ratio = float(quality["price_ratio"] or 0.0)
    threshold = settings.min_non_null_ratio
    if address_ratio < threshold or price_ratio < threshold:
        raise RuntimeError(
            "Quality gate failed for listing_core: "
            f"address_ratio={address_ratio:.3f}, price_ratio={price_ratio:.3f}, threshold={threshold:.3f}"
        )

