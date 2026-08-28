"""Config-driven data-quality rule engine.

Rules live in conf/dq_rules.yml — adding a check is a config change, not a
code change (the pattern used by enterprise DQ frameworks and tools like
Great Expectations / dbt tests, implemented here from first principles so
you can explain every line in an interview).

Supported rule types:
  not_null            — column has no NULLs
  unique              — column (or column list) is unique
  accepted_values     — column values within an allowed set
  range               — numeric column within [min, max]
  row_count_min       — table has at least N rows
  freshness           — max(timestamp_col) within N days of now
  referential         — FK column values exist in a reference table column

Severity:
  error -> pipeline fails if the rule fails
  warn  -> recorded, pipeline continues

All results are appended to <catalog>.ops.dq_results.
"""

from datetime import datetime, timezone

import yaml
from pyspark.sql import functions as F


class DQError(Exception):
    """Raised when one or more severity=error rules fail."""


def load_rules(path: str, layer: str | None = None) -> list[dict]:
    with open(path) as f:
        rules = yaml.safe_load(f)["rules"]
    if layer:
        rules = [r for r in rules if r.get("layer") == layer]
    return rules


def _evaluate_rule(spark, rule: dict, catalog: str) -> dict:
    table = f"{catalog}.{rule['table']}"
    rtype = rule["type"]
    df = spark.table(table)
    failed_count, total = 0, df.count()

    if rtype == "not_null":
        failed_count = df.filter(F.col(rule["column"]).isNull()).count()

    elif rtype == "unique":
        cols = rule["column"] if isinstance(rule["column"], list) else [rule["column"]]
        dupes = df.groupBy(*cols).count().filter("count > 1")
        failed_count = dupes.count()

    elif rtype == "accepted_values":
        failed_count = df.filter(
            ~F.col(rule["column"]).isin(rule["values"]) & F.col(rule["column"]).isNotNull()
        ).count()

    elif rtype == "range":
        col = F.col(rule["column"])
        cond = F.lit(False)
        if "min" in rule:
            cond = cond | (col < rule["min"])
        if "max" in rule:
            cond = cond | (col > rule["max"])
        failed_count = df.filter(cond & col.isNotNull()).count()

    elif rtype == "row_count_min":
        failed_count = 0 if total >= rule["threshold"] else 1

    elif rtype == "freshness":
        max_ts = df.agg(F.max(rule["column"]).alias("m")).collect()[0]["m"]
        if max_ts is None:
            failed_count = 1
        else:
            age_days = (datetime.now(timezone.utc) - max_ts.replace(tzinfo=timezone.utc)).days
            failed_count = 0 if age_days <= rule["max_age_days"] else 1

    elif rtype == "referential":
        ref = spark.table(f"{catalog}.{rule['ref_table']}").select(
            F.col(rule["ref_column"]).alias("_ref")
        ).distinct()
        failed_count = (
            df.filter(F.col(rule["column"]).isNotNull())
            .join(ref, df[rule["column"]] == ref["_ref"], "left_anti")
            .count()
        )

    else:
        raise ValueError(f"Unknown rule type: {rtype}")

    return {
        "rule_name": rule["name"],
        "table_name": table,
        "rule_type": rtype,
        "severity": rule.get("severity", "error"),
        "layer": rule.get("layer", ""),
        "total_rows": total,
        "failed_rows": failed_count,
        "passed": failed_count == 0,
    }


def run_dq_checks(spark, cfg, rules: list[dict], run_id: str = "",
                  fail_on_error: bool = True) -> list[dict]:
    """Evaluate all rules, persist results, raise DQError if any error-severity
    rule failed (after persisting — results are never lost)."""
    results = [_evaluate_rule(spark, r, cfg.catalog) for r in rules]

    now = datetime.now(timezone.utc)
    rows = [
        (
            run_id, cfg.env, r["layer"], r["rule_name"], r["table_name"], r["rule_type"],
            r["severity"], r["total_rows"], r["failed_rows"], r["passed"], now,
        )
        for r in results
    ]
    spark.createDataFrame(
        rows,
        "run_id string, env string, layer string, rule_name string, table_name string, "
        "rule_type string, severity string, total_rows long, failed_rows long, "
        "passed boolean, checked_at timestamp",
    ).write.mode("append").saveAsTable(cfg.ops_dq_results)

    hard_failures = [r for r in results if not r["passed"] and r["severity"] == "error"]
    if hard_failures and fail_on_error:
        names = ", ".join(r["rule_name"] for r in hard_failures)
        raise DQError(f"{len(hard_failures)} data-quality rule(s) failed: {names}")
    return results
