# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Silver Transform
# MAGIC Cleanse, conform, deduplicate. Invalid rows -> quarantine with reasons.
# MAGIC Incremental: only bronze source files not yet processed.

# COMMAND ----------
import os
import sys

sys.path.append(os.path.abspath("../lib"))

from taxi_lakehouse.audit import audited                      # noqa: E402
from taxi_lakehouse.config import config_from_widgets         # noqa: E402
from taxi_lakehouse.transforms.silver import run_silver       # noqa: E402

cfg = config_from_widgets(dbutils)
run_id = dbutils.widgets.get("job_run_id")

# COMMAND ----------
with audited(spark, cfg, "silver_transform", run_id) as audit:
    metrics = run_silver(spark, cfg)
    audit.metrics.update(metrics)
    print(metrics)
