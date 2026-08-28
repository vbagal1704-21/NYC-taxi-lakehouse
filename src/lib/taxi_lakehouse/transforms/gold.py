"""Gold layer: dimensional model (star schema) for BI consumption.

  dim_date          — generated calendar dimension
  dim_zone          — SCD Type 2 (tracks changes to zone/borough attributes)
  dim_vendor        — seeded from the TLC data dictionary
  dim_payment_type  — seeded from the TLC data dictionary
  dim_rate_code     — seeded from the TLC data dictionary
  fact_trips        — one row per valid trip, idempotent MERGE on trip_hash
  agg_*             — pre-aggregated marts for dashboards
"""

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Seed/reference data (from the official TLC yellow-trips data dictionary)
# ---------------------------------------------------------------------------
VENDORS = [(1, "Creative Mobile Technologies, LLC"), (2, "Curb Mobility, LLC"),
           (6, "Myle Technologies Inc"), (7, "Helix")]

PAYMENT_TYPES = [(0, "Flex Fare trip"), (1, "Credit card"), (2, "Cash"),
                 (3, "No charge"), (4, "Dispute"), (5, "Unknown"), (6, "Voided trip")]

RATE_CODES = [(1, "Standard rate"), (2, "JFK"), (3, "Newark"),
              (4, "Nassau or Westchester"), (5, "Negotiated fare"),
              (6, "Group ride"), (99, "Unknown")]


# ---------------------------------------------------------------------------
# Dimensions
# ---------------------------------------------------------------------------
def build_dim_date(spark, start: date, end: date) -> DataFrame:
    """Calendar dimension covering [start, end]."""
    df = spark.sql(
        f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) AS d"
    )
    return df.select(
        F.date_format("d", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("d").alias("date"),
        F.year("d").alias("year"),
        F.quarter("d").alias("quarter"),
        F.month("d").alias("month"),
        F.date_format("d", "MMMM").alias("month_name"),
        F.dayofmonth("d").alias("day_of_month"),
        F.dayofweek("d").alias("day_of_week"),
        F.date_format("d", "EEEE").alias("day_name"),
        F.weekofyear("d").alias("week_of_year"),
        (F.dayofweek("d").isin(1, 7)).alias("is_weekend"),
    )


def refresh_dim_date(spark, cfg) -> int:
    bounds = spark.table(cfg.silver_trips).agg(
        F.min("pickup_date").alias("lo"), F.max("pickup_date").alias("hi")
    ).collect()[0]
    if bounds["lo"] is None:
        return 0
    dim = build_dim_date(spark, bounds["lo"], bounds["hi"])
    dim.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cfg.dim_date)
    return dim.count()


def seed_static_dims(spark, cfg) -> None:
    spark.createDataFrame(VENDORS, "vendor_id int, vendor_name string") \
        .write.mode("overwrite").saveAsTable(cfg.dim_vendor)
    spark.createDataFrame(PAYMENT_TYPES, "payment_type_id int, payment_type string") \
        .write.mode("overwrite").saveAsTable(cfg.dim_payment_type)
    spark.createDataFrame(RATE_CODES, "rate_code_id int, rate_code string") \
        .write.mode("overwrite").saveAsTable(cfg.dim_rate_code)


# ---------------------------------------------------------------------------
# SCD Type 2 (generic) — used for dim_zone
# ---------------------------------------------------------------------------
def scd2_apply(spark, source_df: DataFrame, target_table: str,
               business_key: str, tracked_cols: list[str]) -> dict:
    """Generic SCD2 merge into a Delta table.

    Target must have columns: <business_key>, tracked_cols..., plus
    effective_from, effective_to, is_current.
    """
    src = source_df.withColumn("_hash", F.sha2(F.concat_ws("||", *tracked_cols), 256))

    if not spark.catalog.tableExists(target_table):
        (
            src.withColumn("effective_from", F.current_timestamp())
            .withColumn("effective_to", F.lit(None).cast("timestamp"))
            .withColumn("is_current", F.lit(True))
            .drop("_hash")
            .write.saveAsTable(target_table)
        )
        return {"inserted": src.count(), "expired": 0}

    tgt = spark.table(target_table).filter("is_current = true") \
        .withColumn("_hash", F.sha2(F.concat_ws("||", *tracked_cols), 256))

    changed_or_new = src.alias("s").join(
        tgt.alias("t"), on=business_key, how="left"
    ).filter("t._hash IS NULL OR s._hash != t._hash").select("s.*")

    n_changes = changed_or_new.count()
    if n_changes == 0:
        return {"inserted": 0, "expired": 0}

    changed_or_new.createOrReplaceTempView("_scd2_stage")

    # 1) Expire current versions of changed keys
    spark.sql(
        f"""
        MERGE INTO {target_table} t
        USING _scd2_stage s
        ON t.{business_key} = s.{business_key} AND t.is_current = true
        WHEN MATCHED THEN UPDATE SET
            t.is_current = false,
            t.effective_to = current_timestamp()
        """
    )
    # 2) Insert new current versions
    insert_cols = [business_key] + tracked_cols
    col_list = ", ".join(insert_cols)
    spark.sql(
        f"""
        INSERT INTO {target_table} ({col_list}, effective_from, effective_to, is_current)
        SELECT {col_list}, current_timestamp(), NULL, true FROM _scd2_stage
        """
    )
    return {"inserted": n_changes, "expired": n_changes}


def refresh_dim_zone(spark, cfg) -> dict:
    zones = spark.table(cfg.bronze_zones).select(
        F.col("LocationID").cast("int").alias("zone_id"),
        F.col("Borough").alias("borough"),
        F.col("Zone").alias("zone_name"),
        F.col("service_zone"),
    ).filter("zone_id IS NOT NULL")
    return scd2_apply(spark, zones, cfg.dim_zone,
                      business_key="zone_id",
                      tracked_cols=["borough", "zone_name", "service_zone"])


# ---------------------------------------------------------------------------
# Fact
# ---------------------------------------------------------------------------
FACT_COLUMNS = [
    "trip_hash", "date_key", "pickup_ts", "dropoff_ts",
    "vendor_id", "rate_code_id", "payment_type_id",
    "pickup_zone_id", "dropoff_zone_id",
    "passenger_count", "trip_distance", "trip_minutes", "avg_speed_mph",
    "fare_amount", "tip_amount", "tip_pct", "tolls_amount",
    "congestion_surcharge", "airport_fee", "total_amount",
]


def build_fact(silver_df: DataFrame) -> DataFrame:
    return silver_df.withColumn(
        "date_key", F.date_format("pickup_date", "yyyyMMdd").cast("int")
    ).select(*FACT_COLUMNS)


def refresh_fact_trips(spark, cfg) -> dict:
    fact_src = build_fact(spark.table(cfg.silver_trips))

    if cfg.full_refresh or not spark.catalog.tableExists(cfg.fact_trips):
        fact_src.write.mode("overwrite").option("overwriteSchema", "true") \
            .saveAsTable(cfg.fact_trips)
        try:  # liquid clustering for BI query performance (Databricks runtimes)
            spark.sql(f"ALTER TABLE {cfg.fact_trips} CLUSTER BY (date_key)")
        except Exception:
            pass  # not supported on local/OSS Spark used by unit tests
        n = spark.table(cfg.fact_trips).count()
        return {"mode": "full", "rows": n}

    fact_src.createOrReplaceTempView("_fact_stage")
    spark.sql(
        f"""
        MERGE INTO {cfg.fact_trips} t
        USING _fact_stage s
        ON t.trip_hash = s.trip_hash
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    n = spark.table(cfg.fact_trips).count()
    return {"mode": "incremental_merge", "rows": n}


# ---------------------------------------------------------------------------
# Aggregate marts (rebuilt deterministically from the fact)
# ---------------------------------------------------------------------------
def refresh_aggregates(spark, cfg) -> dict:
    fact = spark.table(cfg.fact_trips)
    zones = spark.table(cfg.dim_zone).filter("is_current = true")

    daily = (
        fact.join(zones, fact.pickup_zone_id == zones.zone_id, "left")
        .groupBy("date_key", "pickup_zone_id", "borough", "zone_name")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.sum("total_amount"), 2).alias("revenue"),
            F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
            F.round(F.avg("tip_pct"), 2).alias("avg_tip_pct"),
            F.round(F.avg("trip_minutes"), 2).alias("avg_trip_minutes"),
        )
    )
    daily.write.mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(cfg.agg_daily_zone)

    monthly = (
        fact.withColumn("month", F.expr("date_key div 100"))
        .groupBy("month")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.sum("total_amount"), 2).alias("revenue"),
            F.round(F.avg("total_amount"), 2).alias("avg_fare"),
            F.round(F.avg("tip_pct"), 2).alias("avg_tip_pct"),
            F.countDistinct("pickup_zone_id").alias("active_pickup_zones"),
        )
    )
    monthly.write.mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(cfg.agg_monthly_kpis)

    return {"daily_rows": daily.count(), "monthly_rows": monthly.count()}
