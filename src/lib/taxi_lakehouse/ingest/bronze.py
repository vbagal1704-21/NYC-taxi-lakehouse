"""Bronze layer: incremental ingestion from the landing Volume with Auto Loader.

Bronze principles applied here (standard enterprise practice):
  * append-only, schema-on-read, no business logic
  * every row is traceable to its source file (_source_file) and load time
  * schema drift is captured, not dropped (_rescued_data)
  * exactly-once incremental processing via Auto Loader checkpoints
"""

from pyspark.sql import functions as F


def load_bronze_trips(spark, cfg) -> int:
    """Auto Loader (availableNow) from landing volume -> bronze.yellow_trips_raw.

    Returns number of rows in bronze after the load (for audit metrics).
    """
    checkpoint = f"{cfg.checkpoints_volume}/bronze_yellow_trips"
    schema_loc = f"{cfg.checkpoints_volume}/bronze_yellow_trips_schema"

    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", schema_loc)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .load(cfg.landing_trips_path)
        .withColumn("_source_file", F.col("_metadata.file_name"))
        .withColumn("_ingested_at", F.current_timestamp())
    )

    (
        stream.writeStream.option("checkpointLocation", checkpoint)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(cfg.bronze_trips)
        .awaitTermination()
    )

    return spark.table(cfg.bronze_trips).count()


def load_bronze_zones(spark, cfg) -> int:
    """Full snapshot load of the small zone-lookup reference file."""
    df = (
        spark.read.option("header", "true")
        .csv(cfg.landing_zones_path)
        .withColumn("_ingested_at", F.current_timestamp())
    )
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cfg.bronze_zones)
    return spark.table(cfg.bronze_zones).count()
