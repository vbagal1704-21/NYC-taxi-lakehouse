# Databricks notebook source
# MAGIC %md
# MAGIC # 08 — Reconciliation
# MAGIC Proves no data was lost or duplicated across layers:
# MAGIC source→bronze (per-file counts), bronze→silver (dedup accounting),
# MAGIC silver→gold (counts + financial totals). Results in `ops.recon_results`.

# COMMAND ----------
import os
import sys

sys.path.append(os.path.abspath("../lib"))

from taxi_lakehouse.audit import audited                      # noqa: E402
from taxi_lakehouse.config import config_from_widgets         # noqa: E402
from taxi_lakehouse.quality.recon import run_reconciliation   # noqa: E402

cfg = config_from_widgets(dbutils)
run_id = dbutils.widgets.get("job_run_id")

# COMMAND ----------
with audited(spark, cfg, "reconciliation", run_id) as audit:
    results = run_reconciliation(spark, cfg, run_id=run_id, fail_on_mismatch=True)
    audit.metrics["checks"] = len(results)
    audit.metrics["failed"] = sum(1 for r in results if not r["passed"])
    for r in results:
        print(("PASS " if r["passed"] else "FAIL ") + r["check_name"],
              f"src={r['source_value']} tgt={r['target_value']}")
