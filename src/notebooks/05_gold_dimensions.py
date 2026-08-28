# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Gold Dimensions
# MAGIC dim_date (generated), static dims (seeded), dim_zone (SCD Type 2).

# COMMAND ----------
import os
import sys

sys.path.append(os.path.abspath("../lib"))

from taxi_lakehouse.audit import audited                      # noqa: E402
from taxi_lakehouse.config import config_from_widgets         # noqa: E402
from taxi_lakehouse.transforms.gold import (                  # noqa: E402
    refresh_dim_date,
    refresh_dim_zone,
    seed_static_dims,
)

cfg = config_from_widgets(dbutils)
run_id = dbutils.widgets.get("job_run_id")

# COMMAND ----------
with audited(spark, cfg, "gold_dimensions", run_id) as audit:
    n_dates = refresh_dim_date(spark, cfg)
    seed_static_dims(spark, cfg)
    scd2 = refresh_dim_zone(spark, cfg)
    audit.metrics.update({"dim_date_rows": n_dates, "dim_zone_scd2": scd2})
    print(f"dim_date: {n_dates} rows, dim_zone scd2: {scd2}")
