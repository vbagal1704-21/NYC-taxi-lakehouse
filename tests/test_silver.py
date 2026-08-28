"""Unit tests for the silver-layer transformations."""

from datetime import datetime

from pyspark.sql import functions as F

from taxi_lakehouse.transforms.silver import (
    add_derived_columns,
    add_validity_flags,
    deduplicate,
    standardize_columns,
    transform_to_silver,
)


def _bronze_row(**overrides):
    base = {
        "VendorID": 1,
        "tpep_pickup_datetime": datetime(2024, 1, 15, 8, 0, 0),
        "tpep_dropoff_datetime": datetime(2024, 1, 15, 8, 30, 0),
        "passenger_count": 2,
        "trip_distance": 5.0,
        "RatecodeID": 1,
        "store_and_fwd_flag": "N",
        "PULocationID": 100,
        "DOLocationID": 200,
        "payment_type": 1,
        "fare_amount": 20.0,
        "extra": 1.0,
        "mta_tax": 0.5,
        "tip_amount": 4.0,
        "tolls_amount": 0.0,
        "improvement_surcharge": 0.3,
        "total_amount": 25.8,
        "congestion_surcharge": 2.5,
        "Airport_fee": 0.0,
        "_ingested_at": datetime(2024, 2, 1, 0, 0, 0),
        "_source_file": "yellow_tripdata_2024-01.parquet",
    }
    base.update(overrides)
    return base


def _bronze_df(spark, rows):
    return spark.createDataFrame([tuple(r.values()) for r in rows], list(rows[0].keys()))


def test_standardize_columns_renames_and_casts(spark):
    df = standardize_columns(_bronze_df(spark, [_bronze_row()]))
    assert "vendor_id" in df.columns
    assert "pickup_ts" in df.columns
    assert "airport_fee" in df.columns          # Airport_fee normalised
    assert dict(df.dtypes)["total_amount"] == "double"
    assert dict(df.dtypes)["pickup_zone_id"] == "int"


def test_deduplicate_keeps_latest(spark):
    dup_late = _bronze_row(_ingested_at=datetime(2024, 2, 2), fare_amount=99.0)
    df = standardize_columns(_bronze_df(spark, [_bronze_row(), dup_late]))
    out = deduplicate(df)
    assert out.count() == 1
    assert out.collect()[0]["fare_amount"] == 99.0  # latest ingest wins


def test_validity_flags_clean_row(spark):
    df = add_validity_flags(standardize_columns(_bronze_df(spark, [_bronze_row()])))
    assert df.collect()[0]["dq_violations"] == []


def test_validity_flags_bad_rows(spark):
    bad = [
        _bronze_row(tpep_dropoff_datetime=datetime(2024, 1, 15, 7, 0, 0)),  # before pickup
        _bronze_row(total_amount=-5.0, PULocationID=101),
        _bronze_row(trip_distance=900.0, PULocationID=102),
    ]
    df = add_validity_flags(standardize_columns(_bronze_df(spark, bad)))
    flags = [set(r["dq_violations"]) for r in df.orderBy("pickup_zone_id").collect()]
    assert {"dropoff_before_pickup"} <= flags[0]
    assert {"non_positive_total_amount"} <= flags[1]
    assert {"invalid_trip_distance"} <= flags[2]


def test_derived_columns(spark):
    df = add_derived_columns(standardize_columns(_bronze_df(spark, [_bronze_row()])))
    row = df.collect()[0]
    assert row["trip_minutes"] == 30.0
    assert row["avg_speed_mph"] == 10.0
    assert row["tip_pct"] == 20.0
    assert row["trip_hash"] is not None and len(row["trip_hash"]) == 64


def test_transform_to_silver_split(spark):
    rows = [
        _bronze_row(),
        _bronze_row(PULocationID=105),                      # clean
        _bronze_row(total_amount=-1.0, PULocationID=106),   # quarantined
        _bronze_row(),                                      # duplicate of row 1
    ]
    clean, quarantine = transform_to_silver(_bronze_df(spark, rows))
    assert clean.count() == 2
    assert quarantine.count() == 1
    # accounting: clean + quarantine == deduped input
    assert clean.count() + quarantine.count() == 3
    assert "dq_violations" in quarantine.columns
    assert "dq_violations" not in clean.columns


def test_null_zone_quarantined(spark):
    rows = [_bronze_row(PULocationID=None)]
    df = add_validity_flags(standardize_columns(_bronze_df(spark, rows)))
    assert "null_zone_id" in df.collect()[0]["dq_violations"]


def test_missing_column_handled(spark):
    """TLC dropped/renamed columns before — pipeline must not break."""
    row = _bronze_row()
    row.pop("Airport_fee")
    df = standardize_columns(_bronze_df(spark, [row]))
    assert "airport_fee" in df.columns
    assert df.filter(F.col("airport_fee").isNull()).count() == 1
