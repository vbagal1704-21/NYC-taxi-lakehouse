# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Gold Aggregates
# MAGIC Pre-aggregated marts for BI dashboards (daily zone revenue, monthly KPIs).

# COMMAND ----------
import os
import sys

sys.path.append(os.path.abspath("../lib"))

from taxi_lakehouse.audit import audited                      # noqa: E402
from taxi_lakehouse.config import config_from_widgets         # noqa: E402
from taxi_lakehouse.transforms.gold import refresh_aggregates  # noqa: E402

cfg = config_from_widgets(dbutils)
run_id = dbutils.widgets.get("job_run_id")

# COMMAND ----------
with audited(spark, cfg, "gold_aggregates", run_id) as audit:
    metrics = refresh_aggregates(spark, cfg)
    audit.metrics.update(metrics)
    print(metrics)
