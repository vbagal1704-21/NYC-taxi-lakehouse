#!/usr/bin/env bash
# =============================================================================
# Fallback ingestion path: if the workspace cannot reach the TLC CloudFront
# host (Free Edition restricts serverless egress to trusted domains), download
# the files on your laptop and push them into the landing Volume with the
# Databricks CLI. The pipeline picks them up identically.
#
# Usage: ./scripts/upload_landing_from_local.sh taxi_dev 2024-01 2024-02 ...
# =============================================================================
set -euo pipefail

CATALOG="${1:?Usage: $0 <catalog> <month YYYY-MM> [more months...]}"
shift

VOLUME="dbfs:/Volumes/${CATALOG}/landing/raw"

# Zone lookup (once)
curl -fSL -o /tmp/taxi_zone_lookup.csv \
  "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
databricks fs cp /tmp/taxi_zone_lookup.csv "${VOLUME}/zones/taxi_zone_lookup.csv" --overwrite

for MONTH in "$@"; do
  FILE="yellow_tripdata_${MONTH}.parquet"
  echo ">> downloading ${FILE}"
  curl -fSL -o "/tmp/${FILE}" \
    "https://d37ci6vzurychx.cloudfront.net/trip-data/${FILE}"
  echo ">> uploading ${FILE} to ${VOLUME}/yellow/"
  databricks fs cp "/tmp/${FILE}" "${VOLUME}/yellow/${FILE}" --overwrite
  rm -f "/tmp/${FILE}"
done

echo "Done. Now run the pipeline normally — the ingest task detects the"
echo "pre-uploaded files, skips the download, and registers them in the"
echo "manifest so source->bronze reconciliation has its baseline."
