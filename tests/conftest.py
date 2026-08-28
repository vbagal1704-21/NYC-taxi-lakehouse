"""Shared pytest fixtures — local SparkSession with Delta Lake enabled so the
same MERGE/SCD2 code that runs on Databricks is tested locally in CI."""

import shutil
import tempfile

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    warehouse = tempfile.mkdtemp(prefix="spark-warehouse-")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("taxi-lakehouse-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.sources.default", "delta")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.ui.enabled", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    yield spark
    spark.stop()
    shutil.rmtree(warehouse, ignore_errors=True)
