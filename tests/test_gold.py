"""Unit tests for the gold layer: date dimension, fact build, SCD Type 2."""

from datetime import date, datetime

import pytest

from taxi_lakehouse.transforms.gold import build_dim_date, build_fact, scd2_apply


def test_dim_date_shape(spark):
    dim = build_dim_date(spark, date(2024, 1, 1), date(2024, 1, 31))
    assert dim.count() == 31
    row = dim.orderBy("date_key").collect()[0]
    assert row["date_key"] == 20240101
    assert row["month_name"] == "January"
    assert row["is_weekend"] in (True, False)


def test_build_fact_columns(spark):
    silver = spark.createDataFrame(
        [
            (
                "h1", datetime(2024, 1, 15, 8), datetime(2024, 1, 15, 9),
                date(2024, 1, 15), 1, 1, 1, 100, 200, 2, 5.0, 60.0, 5.0,
                20.0, 4.0, 20.0, 0.0, 2.5, 0.0, 25.8,
            )
        ],
        "trip_hash string, pickup_ts timestamp, dropoff_ts timestamp, "
        "pickup_date date, vendor_id int, rate_code_id int, payment_type_id int, "
        "pickup_zone_id int, dropoff_zone_id int, passenger_count int, "
        "trip_distance double, trip_minutes double, avg_speed_mph double, "
        "fare_amount double, tip_amount double, tip_pct double, tolls_amount double, "
        "congestion_surcharge double, airport_fee double, total_amount double",
    )
    fact = build_fact(silver)
    assert fact.collect()[0]["date_key"] == 20240115
    assert "trip_hash" in fact.columns and "total_amount" in fact.columns


@pytest.fixture()
def zone_target(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS goldtest")
    spark.sql("DROP TABLE IF EXISTS goldtest.dim_zone")
    return "goldtest.dim_zone"


def _zones(spark, rows):
    return spark.createDataFrame(rows, "zone_id int, borough string, zone_name string")


def test_scd2_initial_load(spark, zone_target):
    res = scd2_apply(spark, _zones(spark, [(1, "Manhattan", "Midtown")]),
                     zone_target, "zone_id", ["borough", "zone_name"])
    assert res["inserted"] == 1
    tgt = spark.table(zone_target).collect()
    assert len(tgt) == 1 and tgt[0]["is_current"] is True


def test_scd2_no_change_is_noop(spark, zone_target):
    src = _zones(spark, [(1, "Manhattan", "Midtown")])
    scd2_apply(spark, src, zone_target, "zone_id", ["borough", "zone_name"])
    res = scd2_apply(spark, src, zone_target, "zone_id", ["borough", "zone_name"])
    assert res == {"inserted": 0, "expired": 0}
    assert spark.table(zone_target).count() == 1


def test_scd2_change_expires_and_versions(spark, zone_target):
    scd2_apply(spark, _zones(spark, [(1, "Manhattan", "Midtown")]),
               zone_target, "zone_id", ["borough", "zone_name"])
    scd2_apply(spark, _zones(spark, [(1, "Manhattan", "Midtown East")]),
               zone_target, "zone_id", ["borough", "zone_name"])

    hist = spark.table(zone_target).orderBy("effective_from").collect()
    assert len(hist) == 2
    old, new = hist
    assert old["is_current"] is False and old["effective_to"] is not None
    assert new["is_current"] is True and new["zone_name"] == "Midtown East"


def test_scd2_new_key_inserted(spark, zone_target):
    scd2_apply(spark, _zones(spark, [(1, "Manhattan", "Midtown")]),
               zone_target, "zone_id", ["borough", "zone_name"])
    res = scd2_apply(
        spark,
        _zones(spark, [(1, "Manhattan", "Midtown"), (2, "Queens", "Astoria")]),
        zone_target, "zone_id", ["borough", "zone_name"],
    )
    assert res["inserted"] == 1
    current = spark.table(zone_target).filter("is_current = true")
    assert current.count() == 2
