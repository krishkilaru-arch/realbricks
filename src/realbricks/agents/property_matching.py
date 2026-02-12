from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


class PropertyMatchingAgent:
    def match_top_properties(
        self,
        spark: SparkSession,
        listing_table: str,
        lead: dict,
        top_n: int = 3,
    ) -> list[dict]:
        df = spark.table(listing_table)

        if lead.get("target_zip"):
            df = df.where(F.col("zip") == F.lit(str(lead["target_zip"])))

        if lead.get("budget"):
            df = df.where(F.col("list_price") <= F.lit(float(lead["budget"])))

        beds = lead.get("min_beds")
        if beds is not None:
            df = df.where(F.col("beds") >= F.lit(float(beds)))

        df = (
            df.withColumn("budget_fit", F.when(F.col("list_price") > 0, 1 - (F.col("list_price") / F.lit(max(float(lead.get("budget", 1)), 1.0)))).otherwise(0))
            .withColumn("size_fit", F.when(F.col("sqft").isNotNull(), F.least(F.col("sqft") / F.lit(2500.0), F.lit(1.0))).otherwise(F.lit(0.3)))
            .withColumn("match_score", (F.col("budget_fit") * 0.6) + (F.col("size_fit") * 0.4))
            .orderBy(F.col("match_score").desc_nulls_last())
            .limit(top_n)
        )

        rows = df.select(
            "listing_id",
            "address",
            "zip",
            "list_price",
            "beds",
            "baths",
            "sqft",
            "match_score",
        ).collect()

        return [row.asDict(recursive=True) for row in rows]

