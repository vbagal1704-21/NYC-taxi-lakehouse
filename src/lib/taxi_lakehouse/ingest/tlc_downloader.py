"""Download NYC TLC trip data into the Unity Catalog landing Volume.

Simulates the 'source system drop' step of an enterprise pipeline: files
arrive in a landing zone, and a manifest records what arrived and how many
rows the source says it contains (used later for source-to-bronze
reconciliation).

Free Edition note: serverless egress is restricted to trusted domains.
The TLC CloudFront host is publicly reachable; if your workspace blocks it,
use scripts/upload_landing_from_local.sh to push files from your laptop
into the same Volume via the Databricks CLI — the rest of the pipeline is
unchanged.
"""

import os
import time
from datetime import datetime, timezone

from taxi_lakehouse.config import TLC_TRIP_URL, TLC_ZONE_URL


def download_file(url: str, dest_path: str, max_retries: int = 3, chunk_mb: int = 8) -> int:
    """Stream a file to a Volume path with retries. Returns bytes written."""
    import requests

    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    tmp_path = dest_path + ".part"
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            with requests.get(url, stream=True, timeout=120) as r:
                r.raise_for_status()
                size = 0
                with open(tmp_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=chunk_mb * 1024 * 1024):
                        f.write(chunk)
                        size += len(chunk)
            os.replace(tmp_path, dest_path)
            return size
        except Exception as e:  # noqa: BLE001 — retry on any transport error
            last_err = e
            time.sleep(5 * attempt)
    raise RuntimeError(f"Failed to download {url} after {max_retries} attempts: {last_err}")


def ingest_month(spark, cfg, month: str) -> dict:
    """Land one month of yellow-taxi data (idempotent) and record a manifest row.

    Returns a summary dict for audit metrics.
    """
    file_name = f"yellow_tripdata_{month}.parquet"
    dest = f"{cfg.landing_trips_path}/{file_name}"
    url = TLC_TRIP_URL.format(month=month)

    already_landed = (
        spark.sql(
            f"""
            SELECT count(*) AS c FROM {cfg.ops_ingest_manifest}
            WHERE file_name = '{file_name}' AND status = 'LANDED'
            """
        ).collect()[0]["c"]
        > 0
    )
    if already_landed and not cfg.full_refresh:
        return {"file_name": file_name, "skipped": True, "reason": "already landed"}

    # If the file was placed in the Volume out-of-band (e.g. via
    # scripts/upload_landing_from_local.sh), register it instead of re-downloading.
    if os.path.exists(dest) and not cfg.full_refresh:
        size_bytes = os.path.getsize(dest)
    else:
        size_bytes = download_file(url, dest)

    # Source row count from parquet metadata — this is our reconciliation
    # baseline ("what the source says it sent us").
    src_count = spark.read.parquet(dest).count()

    spark.createDataFrame(
        [
            (
                file_name,
                dest,
                month,
                "yellow",
                size_bytes,
                src_count,
                "LANDED",
                datetime.now(timezone.utc),
            )
        ],
        "file_name string, file_path string, run_month string, source string, "
        "size_bytes long, source_row_count long, status string, landed_at timestamp",
    ).write.mode("append").saveAsTable(cfg.ops_ingest_manifest)

    return {
        "file_name": file_name,
        "size_bytes": size_bytes,
        "source_row_count": src_count,
        "skipped": False,
    }


def ingest_zone_lookup(spark, cfg) -> dict:
    """Land the taxi-zone lookup CSV (small reference/master data feed)."""
    dest = f"{cfg.landing_zones_path}/taxi_zone_lookup.csv"
    size_bytes = download_file(TLC_ZONE_URL, dest)
    return {"file_name": "taxi_zone_lookup.csv", "size_bytes": size_bytes}
