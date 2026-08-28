# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Data Quality Checks
# MAGIC Runs the config-driven rules for the layer passed in `dq_layer`
# MAGIC (bronze | silver | gold). severity=error failures stop the pipeline;
# MAGIC all results land in `ops.dq_results`.

# COMMAND ----------
import os
import sys

sys.path.append(os.path.abspath("../lib"))

from taxi_lakehouse.audit import audited                      # noqa: E402
from taxi_lakehouse.config import config_from_widgets         # noqa: E402
from taxi_lakehouse.quality.dq_engine import load_rules, run_dq_checks  # noqa: E402

cfg = config_from_widgets(dbutils)
run_id = dbutils.widgets.get("job_run_id")
dbutils.widgets.text("dq_layer", "silver")
layer = dbutils.widgets.get("dq_layer")

rules_path = os.path.abspath("../../conf/dq_rules.yml")

# COMMAND ----------
with audited(spark, cfg, f"dq_checks_{layer}", run_id) as audit:
    rules = load_rules(rules_path, layer=layer)
    results = run_dq_checks(spark, cfg, rules, run_id=run_id, fail_on_error=True)
    audit.metrics["rules_evaluated"] = len(results)
    audit.metrics["rules_failed"] = sum(1 for r in results if not r["passed"])
    for r in results:
        print(("PASS " if r["passed"] else "FAIL ") + r["rule_name"], f"failed_rows={r['failed_rows']}")
