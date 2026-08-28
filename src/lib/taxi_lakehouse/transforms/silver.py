"""Silver layer: cleansed, conformed, deduplicated trips.

Design (mirrors enterprise practice):
  * pure transformation functions take/return DataFrames -> unit-testable
    without a Databricks workspace
  * every dropped row goes to a quarantine table WITH the reason —
    nothing silently disappears (required for reconciliation)
  * incremental processing: only bronze files not yet present in silver
    are processed on each run
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# Bronze (TLC) -> silver column mapping. TLC files occasionally change the
# casing of columns (e.g. Airport_fee); we normalise defensively.
COLUMN_MAP = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_ts",
    "tpep_dropoff_datetime": "dropoff_ts",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "RatecodeID": "rate_code_id",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "PULocationID": "pickup_zone_id",
    "DOLocationID": "dropoff_zone_id",
    "payment_type": "payment_type_id",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge",
    "airport_fee": "airport_fee",
    "Airport_fee": "airport_fee",
}

BUSINESS_KEY = ["vendor_id", "pickup_ts", "dropoff_ts", "pickup_zone_id", "dropoff_zone_id", "total_amount"]

# Stable output contract of the silver table — insulates silver from upstream
# schema drift (new TLC columns stay in bronze until explicitly promoted).
SILVER_COLUMNS = [
    "vendor_id", "pickup_ts", "dropoff_ts", "passenger_count", "trip_distance",
    "rate_code_id", "store_and_fwd_flag", "pickup_zone_id", "dropoff_zone_id",
    "payment_type_id", "fare_amount", "extra", "mta_tax", "tip_amount",
    "tolls_amount", "improvement_surcharge", "total_amount",
    "congestion_surcharge", "airport_fee",
    "pickup_date", "pickup_hour", "trip_minutes", "avg_speed_mph", "tip_pct",
    "trip_hash", "_source_file", "_ingested_at",
]


def standardize_columns(df: DataFrame) -> DataFrame:
    """Rename to snake_case and cast to canonical types."""
    for old, new in COLUMN_MAP.items():
        if old in df.columns and old != new:
            df = df.withColumnRenamed(old, new)

    casts = {
        "store_and_fwd_flag": "string",
        "vendor_id": "int",
        "pickup_ts": "timestamp",
        "dropoff_ts": "timestamp",
        "passenger_count": "int",
        "trip_distance": "double",
        "rate_code_id": "int",
        "pickup_zone_id": "int",
        "dropoff_zone_id": "int",
        "payment_type_id": "int",
        "fare_amount": "double",
        "extra": "double",
        "mta_tax": "double",
        "tip_amount": "double",
        "tolls_amount": "double",
        "improvement_surcharge": "double",
        "total_amount": "double",
        "congestion_surcharge": "double",
        "airport_fee": "double",
    }
    for col, typ in casts.items():
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast(typ))
        else:
            df = df.withColumn(col, F.lit(None).cast(typ))
    return df


def deduplicate(df: DataFrame) -> DataFrame:
    """Keep one row per business key (latest ingested wins)."""
    order_col = F.col("_ingested_at").desc() if "_ingested_at" in df.columns else F.lit(1)
    w = Window.partitionBy(*BUSINESS_KEY).orderBy(order_col)
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def add_validity_flags(df: DataFrame) -> DataFrame:
    """Attach an array of DQ violation reasons. Empty array == clean row."""
    checks = [
        (F.col("pickup_ts").isNull(), "null_pickup_ts"),
        (F.col("dropoff_ts").isNull(), "null_dropoff_ts"),
        (F.col("dropoff_ts") < F.col("pickup_ts"), "dropoff_before_pickup"),
        (
            (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")) > 86400,
            "trip_longer_than_24h",
        ),
        ((F.col("trip_distance") < 0) | (F.col("trip_distance") > 500), "invalid_trip_distance"),
        (F.col("total_amount") <= 0, "non_positive_total_amount"),
        ((F.col("passenger_count") < 0) | (F.col("passenger_count") > 8), "invalid_passenger_count"),
        (F.col("pickup_zone_id").isNull() | F.col("dropoff_zone_id").isNull(), "null_zone_id"),
    ]
    reasons = F.array_compact(
        F.array(*[F.when(cond, F.lit(name)) for cond, name in checks])
    )
    return df.withColumn("dq_violations", reasons)


def add_derived_columns(df: DataFrame) -> DataFrame:
    df = (
        df.withColumn("pickup_date", F.to_date("pickup_ts"))
        .withColumn("pickup_hour", F.hour("pickup_ts"))
        .withColumn(
            "trip_minutes",
            F.round((F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")) / 60.0, 2),
        )
    )
    df = df.withColumn(
        "avg_speed_mph",
        F.when(
            F.col("trip_minutes") > 0,
            F.round(F.col("trip_distance") / (F.col("trip_minutes") / 60.0), 2),
        ),
    ).withColumn(
        "tip_pct",
        F.when(
            F.col("fare_amount") > 0, F.round(F.col("tip_amount") / F.col("fare_amount") * 100, 2)
        ),
    )
    return with_trip_hash(df)


def with_trip_hash(df: DataFrame) -> DataFrame:
    """Deterministic surrogate key over the business key. NULLs are made
    explicit so different NULL patterns can never hash-collide. Used by the
    silver/gold MERGEs and recomputed identically by reconciliation."""
    key_parts = [F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in BUSINESS_KEY]
    return df.withColumn("trip_hash", F.sha2(F.concat_ws("||", *key_parts), 256))


def transform_to_silver(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Full silver transformation. Returns (clean_df, quarantine_df)."""
    df = standardize_columns(bronze_df)
    df = deduplicate(df)
    df = add_validity_flags(df)
    df = add_derived_columns(df)

    clean = df.filter(F.size("dq_violations") == 0).drop("dq_violations")
    quarantine = df.filter(F.size("dq_violations") > 0)
    return clean, quarantine


def _merge_insert_only(spark, df: DataFrame, target: str, view: str) -> None:
    """Insert-only MERGE keyed on trip_hash: replay-safe, and duplicates that
    span source files / pipeline runs can never enter the table."""
    df.createOrReplaceTempView(view)
    spark.sql(
        f"""
        MERGE INTO {target} t
        USING {view} s
        ON t.trip_hash = s.trip_hash
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def run_silver(spark, cfg) -> dict:
    """Incremental bronze -> silver. Processes only new source files."""
    bronze = spark.table(cfg.bronze_trips)

    full_load = cfg.full_refresh or not spark.catalog.tableExists(cfg.silver_trips)
    if full_load:
        increment = bronze
    else:
        processed = spark.table(cfg.silver_trips).select("_source_file").distinct()
        quarantined = spark.table(cfg.silver_quarantine).select("_source_file").distinct()
        done = processed.union(quarantined).distinct()
        increment = bronze.join(done, on="_source_file", how="left_anti")

    n_input = increment.count()
    if n_input == 0:
        return {"rows_in": 0, "rows_clean": 0, "rows_quarantined": 0, "skipped": True}

    clean, quarantine = transform_to_silver(increment)

    # Stable output contract (drift-proof): only the agreed silver columns.
    clean = clean.select(*SILVER_COLUMNS)
    quarantine = quarantine.select(*SILVER_COLUMNS, "dq_violations")

    if full_load:
        clean.write.mode("overwrite").option("overwriteSchema", "true") \
            .saveAsTable(cfg.silver_trips)
        quarantine.write.mode("overwrite").option("overwriteSchema", "true") \
            .saveAsTable(cfg.silver_quarantine)
    else:
        _merge_insert_only(spark, clean, cfg.silver_trips, "_silver_clean_stage")
        _merge_insert_only(spark, quarantine, cfg.silver_quarantine, "_silver_quar_stage")

    n_clean = clean.count()
    n_quar = quarantine.count()
    return {"rows_in": n_input, "rows_clean": n_clean, "rows_quarantined": n_quar, "skipped": False}
