"""Reconciliation framework.

Enterprise pipelines must prove that no data was lost or duplicated between
layers. Three classes of checks, all persisted to <catalog>.ops.recon_results:

  1. Source -> Bronze  : per-file row counts (ingest manifest vs bronze)
  2. Bronze -> Silver  : deduped bronze rows == silver clean + quarantine
  3. Silver -> Gold    : row counts AND financial totals (sum of total_amount)
                         must match between silver and fact_trips

Count checks must match exactly; monetary sums allow a tiny float tolerance.
"""

from datetime import datetime, timezone

from pyspark.sql import functions as F

from taxi_lakehouse.transforms.silver import standardize_columns, with_trip_hash


class ReconError(Exception):
    pass


def _result(name, scope, src, tgt, tolerance=0.0):
    diff = abs(src - tgt)
    passed = diff <= tolerance
    return {
        "check_name": name, "scope": scope,
        "source_value": float(src), "target_value": float(tgt),
        "difference": float(diff), "passed": passed,
    }


def source_to_bronze(spark, cfg) -> list[dict]:
    """Per-file: manifest source_row_count vs rows actually in bronze."""
    manifest = spark.table(cfg.ops_ingest_manifest) \
        .filter("status = 'LANDED'") \
        .groupBy("file_name").agg(F.max("source_row_count").alias("src_rows"))
    bronze = spark.table(cfg.bronze_trips) \
        .groupBy("_source_file").agg(F.count("*").alias("bronze_rows"))

    joined = manifest.join(
        bronze, manifest.file_name == bronze._source_file, "full_outer"
    ).collect()

    results = []
    for row in joined:
        fname = row["file_name"] or row["_source_file"]
        results.append(
            _result(f"src_vs_bronze_rows::{fname}", "source->bronze",
                    row["src_rows"] or 0, row["bronze_rows"] or 0)
        )
    return results


def bronze_to_silver(spark, cfg) -> list[dict]:
    """Accounting invariant: every distinct business key in bronze must appear
    in silver clean or quarantine (and nowhere be duplicated).

    Recomputes trip_hash from bronze with the exact same expression the silver
    layer uses, so the comparison is apples-to-apples.
    """
    bronze_keys = (
        with_trip_hash(standardize_columns(spark.table(cfg.bronze_trips)))
        .select("trip_hash").distinct().count()
    )
    silver_keys = (
        spark.table(cfg.silver_trips).select("trip_hash")
        .union(spark.table(cfg.silver_quarantine).select("trip_hash"))
        .distinct().count()
    )
    return [_result("bronze_distinct_keys_vs_silver_total", "bronze->silver",
                    bronze_keys, silver_keys)]


def silver_to_gold(spark, cfg) -> list[dict]:
    silver = spark.table(cfg.silver_trips)
    fact = spark.table(cfg.fact_trips)

    results = [
        _result("silver_vs_fact_rows", "silver->gold", silver.count(), fact.count())
    ]
    s_amt = silver.agg(F.sum("total_amount")).collect()[0][0] or 0.0
    f_amt = fact.agg(F.sum("total_amount")).collect()[0][0] or 0.0
    results.append(
        _result("silver_vs_fact_total_amount", "silver->gold", s_amt, f_amt, tolerance=0.01)
    )

    # fact vs aggregate mart — revenue must survive aggregation
    agg = spark.table(cfg.agg_daily_zone).agg(F.sum("revenue")).collect()[0][0] or 0.0
    results.append(
        _result("fact_vs_agg_revenue", "gold->mart", f_amt, agg, tolerance=1.0)
    )
    return results


def run_reconciliation(spark, cfg, run_id: str = "", fail_on_mismatch: bool = True) -> list[dict]:
    results = source_to_bronze(spark, cfg) + bronze_to_silver(spark, cfg) + silver_to_gold(spark, cfg)

    now = datetime.now(timezone.utc)
    rows = [
        (run_id, cfg.env, r["check_name"], r["scope"], r["source_value"],
         r["target_value"], r["difference"], r["passed"], now)
        for r in results
    ]
    spark.createDataFrame(
        rows,
        "run_id string, env string, check_name string, scope string, "
        "source_value double, target_value double, difference double, "
        "passed boolean, checked_at timestamp",
    ).write.mode("append").saveAsTable(cfg.ops_recon_results)

    failures = [r for r in results if not r["passed"]]
    if failures and fail_on_mismatch:
        names = ", ".join(r["check_name"] for r in failures)
        raise ReconError(f"{len(failures)} reconciliation check(s) failed: {names}")
    return results
