"""Unit tests for the DQ rule engine, using the local session catalog
(`spark_catalog`) in place of a Unity Catalog catalog."""

import pytest
from taxi_lakehouse.config import Config
from taxi_lakehouse.quality.dq_engine import DQError, run_dq_checks

CATALOG = "spark_catalog"


@pytest.fixture(scope="module")
def cfg():
    return Config(catalog=CATALOG, env="test")


@pytest.fixture(scope="module", autouse=True)
def sample_tables(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS dqtest")
    spark.createDataFrame(
        [(1, "a", 10.0), (2, "b", 20.0), (3, None, 30.0), (3, "c", -5.0)],
        "id int, name string, amount double",
    ).write.mode("overwrite").saveAsTable("dqtest.sample")
    spark.createDataFrame([(1,), (2,)], "id int") \
        .write.mode("overwrite").saveAsTable("dqtest.ref")
    # ops schema for results
    spark.sql("CREATE DATABASE IF NOT EXISTS ops")
    spark.sql("DROP TABLE IF EXISTS ops.dq_results")


def _run(spark, cfg, rules, **kw):
    return run_dq_checks(spark, cfg, rules, run_id="t1", **kw)


def test_not_null_detects_failures(spark, cfg):
    rules = [{"name": "name_nn", "table": "dqtest.sample", "type": "not_null",
              "column": "name", "severity": "warn"}]
    res = _run(spark, cfg, rules)
    assert res[0]["failed_rows"] == 1 and not res[0]["passed"]


def test_unique_detects_duplicates(spark, cfg):
    rules = [{"name": "id_uq", "table": "dqtest.sample", "type": "unique",
              "column": "id", "severity": "warn"}]
    res = _run(spark, cfg, rules)
    assert not res[0]["passed"]


def test_range_check(spark, cfg):
    rules = [{"name": "amt_rng", "table": "dqtest.sample", "type": "range",
              "column": "amount", "min": 0, "max": 100, "severity": "warn"}]
    res = _run(spark, cfg, rules)
    assert res[0]["failed_rows"] == 1


def test_referential_check(spark, cfg):
    rules = [{"name": "id_fk", "table": "dqtest.sample", "type": "referential",
              "column": "id", "ref_table": "dqtest.ref", "ref_column": "id",
              "severity": "warn"}]
    res = _run(spark, cfg, rules)
    assert res[0]["failed_rows"] == 2  # two rows with id=3


def test_row_count_min_passes(spark, cfg):
    rules = [{"name": "cnt", "table": "dqtest.sample", "type": "row_count_min",
              "threshold": 2, "severity": "error"}]
    res = _run(spark, cfg, rules)
    assert res[0]["passed"]


def test_error_severity_raises(spark, cfg):
    rules = [{"name": "name_nn_hard", "table": "dqtest.sample", "type": "not_null",
              "column": "name", "severity": "error"}]
    with pytest.raises(DQError):
        _run(spark, cfg, rules)


def test_results_persisted(spark, cfg):
    n = spark.table("ops.dq_results").count()
    assert n >= 6  # all checks above were persisted, including the failed one
