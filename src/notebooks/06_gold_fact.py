# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Gold Fact
# MAGIC Idempotent MERGE of silver trips into `gold.fact_trips` on trip_hash.

# COMMAND ----------
import os
import sys

sys.path.append(os.path.abspath("../lib"))

from taxi_lakehouse.audit import audited                      # noqa: E402
from taxi_lakehouse.config import config_from_widgets         # noqa: E402
from taxi_lakehouse.transforms.gold import refresh_fact_trips  # noqa: E402

cfg = config_from_widgets(dbutils)
run_id = dbutils.widgets.get("job_run_id")

# COMMAND ----------
with audited(spark, cfg, "gold_fact", run_id) as audit:
    metrics = refresh_fact_trips(spark, cfg)
    audit.metrics.update(metrics)
    print(metrics)
