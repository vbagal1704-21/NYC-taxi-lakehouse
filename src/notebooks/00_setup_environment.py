# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Environment Setup (run once per environment)
# MAGIC Creates the catalog, schemas, volumes and ops tables for the target
# MAGIC environment. Idempotent — safe to re-run.

# COMMAND ----------
import os
import sys

sys.path.append(os.path.abspath("../lib"))

from taxi_lakehouse.config import SCHEMAS, config_from_widgets  # noqa: E402

cfg = config_from_widgets(dbutils)
print(f"Setting up environment: catalog={cfg.catalog} env={cfg.env}")

# COMMAND ----------
# Catalog + schemas
spark.sql(f"CREATE CATALOG IF NOT EXISTS {cfg.catalog}")
for schema in SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{schema}")

# Volumes: landing zone for raw files, checkpoints for streaming state
spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.catalog}.landing.raw")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.catalog}.ops.checkpoints")

# COMMAND ----------
# Ops tables (audit / DQ / recon / manifest)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {cfg.ops_pipeline_runs} (
    run_id STRING, task_name STRING, env STRING, run_month STRING,
    status STRING, metrics STRING, error STRING,
    elapsed_seconds DOUBLE, logged_at TIMESTAMP
)""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {cfg.ops_dq_results} (
    run_id STRING, env STRING, layer STRING, rule_name STRING,
    table_name STRING, rule_type STRING, severity STRING,
    total_rows BIGINT, failed_rows BIGINT, passed BOOLEAN, checked_at TIMESTAMP
)""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {cfg.ops_recon_results} (
    run_id STRING, env STRING, check_name STRING, scope STRING,
    source_value DOUBLE, target_value DOUBLE, difference DOUBLE,
    passed BOOLEAN, checked_at TIMESTAMP
)""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {cfg.ops_ingest_manifest} (
    file_name STRING, file_path STRING, run_month STRING, source STRING,
    size_bytes BIGINT, source_row_count BIGINT, status STRING, landed_at TIMESTAMP
)""")

print("Setup complete.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Governance note (enterprise pattern)
# MAGIC In a real org you'd now apply grants, e.g.:
# MAGIC ```sql
# MAGIC GRANT USE CATALOG ON CATALOG taxi_prod TO `data_engineers`;
# MAGIC GRANT SELECT ON SCHEMA taxi_prod.gold TO `bi_analysts`;   -- BI sees gold only
# MAGIC ```
# MAGIC Free Edition is single-user, so grants are documented rather than applied.
