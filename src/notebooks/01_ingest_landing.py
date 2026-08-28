# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingest to Landing
# MAGIC Downloads the month's TLC yellow-taxi parquet + zone lookup into the
# MAGIC landing Volume and records the ingest manifest (source row counts used
# MAGIC later for reconciliation). Idempotent per month.

# COMMAND ----------
import os
import sys

sys.path.append(os.path.abspath("../lib"))

from taxi_lakehouse.audit import audited                      # noqa: E402
from taxi_lakehouse.config import config_from_widgets         # noqa: E402
from taxi_lakehouse.ingest.tlc_downloader import (            # noqa: E402
    ingest_month,
    ingest_zone_lookup,
)

cfg = config_from_widgets(dbutils)
run_id = dbutils.widgets.get("job_run_id")

# COMMAND ----------
with audited(spark, cfg, "ingest_landing", run_id) as audit:
    trips = ingest_month(spark, cfg, cfg.run_month)
    zones = ingest_zone_lookup(spark, cfg)
    audit.metrics.update({"trips": trips, "zones": zones})
    print(trips)
    print(zones)
