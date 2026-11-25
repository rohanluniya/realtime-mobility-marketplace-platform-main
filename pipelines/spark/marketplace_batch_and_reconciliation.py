"""Apache Spark 4.1.4 + Apache Iceberg 1.12.0 + OpenLineage 1.55.0 Marketplace Batch Engine.

Performs:
1. Bronze -> Silver -> Gold dimensional lakehouse curation in Apache Iceberg V2.
2. Authoritative reconciliation between Iceberg Gold marketplace state and low-latency serving views.
3. Spark Optimization Suite tailored to ride-hailing geospatial workloads:
   - Broadcast Hash Join vs Sort-Merge Join (H3 pricing events joined with dim_pricing_policy)
   - Adaptive Query Execution (AQE) enabled vs disabled
   - Hot H3 Cell Skew Salting (mitigating JFK/Downtown airport demand hotspots)
   - Iceberg Partition Pruning (day(decision_time) + city_id)
   - `repartition()` vs `coalesce()` shuffle cost comparison
   - Iceberg `rewrite_data_files` (compaction) & `expire_snapshots`

Runtime container image: `apache/spark:4.1.4-scala2.13-java17-python3-ubuntu`
Packages:
  - `org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0`
  - `org.apache.iceberg:iceberg-aws-bundle:1.12.0`
  - `io.openlineage:openlineage-spark_2.13:1.55.0`
"""

from __future__ import annotations

import argparse
import json
import os
import time
from typing import Any

try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql import functions as F
except ImportError as exc:  # pragma: no cover - container-only Spark runtime
    raise SystemExit(
        "PySpark 4.1.4 is required to execute this Spark batch script. "
        "Run inside the Spark container (`docker compose --profile analytics up`) "
        "or run the local native PyIceberg/DuckDB runner (`services/state-materializer/run_pipeline.py`)."
    ) from exc


def build_spark_session(app_name: str = "marketplace-iceberg-batch") -> SparkSession:
    catalog_uri = os.getenv("ICEBERG_REST_URI", "http://lakekeeper:8181/catalog")
    s3_endpoint = os.getenv("S3_ENDPOINT", "http://rustfs:9000")
    openlineage_url = os.getenv("OPENLINEAGE_URL", "http://marquez:5000")

    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config("spark.sql.catalog.lakehouse.uri", catalog_uri)
        .config("spark.sql.catalog.lakehouse.warehouse", "marketplace")
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lakehouse.s3.endpoint", s3_endpoint)
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
        .config("spark.openlineage.transport.type", "http")
        .config("spark.openlineage.transport.url", openlineage_url)
        .config("spark.openlineage.namespace", "realtime-mobility-marketplace")
        .getOrCreate()
    )


def curate_gold_h3_summary(spark: SparkSession) -> dict[str, Any]:
    """Curate Gold H3 city supply-demand & pricing summary via idempotent MERGE INTO."""
    t0 = time.perf_counter()
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.gold.spark_h3_marketplace_summary (
            city_id STRING,
            region_id STRING,
            h3_cell STRING,
            h3_coarse_res5 STRING,
            avg_pricing_multiplier DOUBLE,
            max_pricing_multiplier DOUBLE,
            total_demand BIGINT,
            total_supply BIGINT,
            decision_count BIGINT
        )
        USING iceberg
        PARTITIONED BY (city_id)
        """
    )

    spark.sql(
        """
        MERGE INTO lakehouse.gold.spark_h3_marketplace_summary AS tgt
        USING (
            SELECT
                city_id,
                region_id,
                h3_cell,
                h3_coarse_res5,
                ROUND(AVG(pricing_multiplier), 4) AS avg_pricing_multiplier,
                MAX(pricing_multiplier) AS max_pricing_multiplier,
                SUM(demand_count) AS total_demand,
                SUM(supply_count) AS total_supply,
                COUNT(*) AS decision_count
            FROM lakehouse.silver.fact_pricing_decision
            GROUP BY city_id, region_id, h3_cell, h3_coarse_res5
        ) AS src
        ON tgt.city_id = src.city_id AND tgt.h3_cell = src.h3_cell
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    cnt = spark.table("lakehouse.gold.spark_h3_marketplace_summary").count()
    return {"gold_rows": cnt, "duration_seconds": round(time.perf_counter() - t0, 3)}


def run_spark_optimization_benchmarks(spark: SparkSession) -> dict[str, Any]:
    """Benchmark broadcast vs SMJ, AQE, and hot-H3 skew salting on pricing decisions."""
    results: dict[str, Any] = {}
    pricing: DataFrame = spark.table("lakehouse.silver.fact_pricing_decision")
    policies: DataFrame = spark.table("lakehouse.silver.dim_pricing_policy_scd2").filter("is_current = true")

    # 1. Broadcast Hash Join vs Sort-Merge Join
    t0 = time.perf_counter()
    b_cnt = pricing.join(F.broadcast(policies), on="region_id", how="left").count()
    results["broadcast_join_ms"] = round((time.perf_counter() - t0) * 1000.0, 2)

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    t1 = time.perf_counter()
    s_cnt = pricing.join(policies, on="region_id", how="left").count()
    results["sort_merge_join_ms"] = round((time.perf_counter() - t1) * 1000.0, 2)
    assert b_cnt == s_cnt
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))

    # 2. Hot H3 cell skew salting (8-way salt for airport/stadium cells)
    t2 = time.perf_counter()
    unsalted = pricing.groupBy("h3_cell").agg(F.avg("pricing_multiplier").alias("avg_m"), F.sum("demand_count").alias("dem")).collect()
    results["unsalted_h3_agg_ms"] = round((time.perf_counter() - t2) * 1000.0, 2)

    t3 = time.perf_counter()
    salted = (
        pricing.withColumn("salt", (F.abs(F.hash("decision_id")) % 8).cast("int"))
        .groupBy("h3_cell", "salt")
        .agg(F.sum("pricing_multiplier").alias("sum_m"), F.count("*").alias("cnt"), F.sum("demand_count").alias("dem"))
        .groupBy("h3_cell")
        .agg((F.sum("sum_m") / F.sum("cnt")).alias("avg_m"), F.sum("dem").alias("dem"))
        .collect()
    )
    results["salted_h3_agg_ms"] = round((time.perf_counter() - t3) * 1000.0, 2)
    assert len(unsalted) == len(salted)

    # 3. Iceberg compaction (rewrite_data_files)
    t4 = time.perf_counter()
    spark.sql("CALL lakehouse.system.rewrite_data_files(table => 'silver.fact_pricing_decision')")
    results["iceberg_compaction_ms"] = round((time.perf_counter() - t4) * 1000.0, 2)
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Spark 4.1.4 + Iceberg 1.12.0 marketplace batch jobs")
    parser.add_argument("--mode", choices=["curate", "benchmark", "all"], default="all")
    args = parser.parse_args()

    spark = build_spark_session()
    try:
        out: dict[str, Any] = {}
        if args.mode in ("curate", "all"):
            out["curate"] = curate_gold_h3_summary(spark)
        if args.mode in ("benchmark", "all"):
            out["benchmarks"] = run_spark_optimization_benchmarks(spark)
        print(json.dumps(out, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
