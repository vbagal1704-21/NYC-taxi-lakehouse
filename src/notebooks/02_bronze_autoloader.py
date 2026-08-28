# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bronze (Auto Loader)
# MAGIC Incremental, exactly-once load from the landing Volume into
# MAGIC `bronze.yellow_trips_raw` (+ full snapshot of the zone lookup).

# COMMAND ----------
import os
import sys

sys.path.append(os.path.abspath("../lib"))

from taxi_lakehouse.audit import audited                      # noqa: E402
from taxi_lakehouse.config import config_from_widgets         # noqa: E402
from taxi_lakehouse.ingest.bronze import (                    # noqa: E402
    load_bronze_trips,
    load_bronze_zones,
)

cfg = config_from_widgets(dbutils)
run_id = dbutils.widgets.get("job_run_id")

# COMMAND ----------
with audited(spark, cfg, "bronze_autoload", run_id) as audit:
    n_trips = load_bronze_trips(spark, cfg)
    n_zones = load_bronze_zones(spark, cfg)
    audit.metrics.update({"bronze_trips_total": n_trips, "bronze_zones": n_zones})
    print(f"bronze trips total: {n_trips}, zones: {n_zones}")
