# Operations Runbook

## The pipeline failed — where do I look?

1. **Jobs & Pipelines UI** → failed task → stdout/stderr.
2. `SELECT * FROM <cat>.ops.pipeline_runs WHERE status='FAILED' ORDER BY logged_at DESC` —
   the `error` column carries the exception, `metrics` what was processed before failure.
3. If the failed task is `dq_*`: `SELECT * FROM <cat>.ops.dq_results WHERE passed=false ORDER BY checked_at DESC`.
4. If `reconciliation`: `SELECT * FROM <cat>.ops.recon_results WHERE passed=false ORDER BY checked_at DESC`.

## Common incidents

| Symptom | Likely cause | Action |
|---|---|---|
| ingest_landing fails with download error | TLC host unreachable from serverless, or month not yet published (TLC lags ~2 months) | Use `scripts/upload_landing_from_local.sh`; check https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page |
| DQ `error` rule failed | Real data issue or rule too strict | Inspect failing rows; either fix upstream/cleansing (code PR) or re-scope the rule (config PR). Never edit prod data by hand. |
| Recon `src_vs_bronze_rows` mismatch | Partial download or double-load | Compare manifest vs `SELECT _source_file, count(*) FROM bronze... GROUP BY 1`; re-land the file, `full_refresh=true` in dev to rebuild |
| Recon `silver_vs_fact_rows` mismatch | Fact merged during a partially failed prior run | Re-run pipeline (MERGE is idempotent); if still off, rebuild fact with `full_refresh=true` |
| Job stuck / task limit | Free Edition: max 5 concurrent tasks | Run months sequentially; keep `max_concurrent_runs: 1` |
| Duplicate rows suspected in silver | dedup key drift | `SELECT trip_hash, count(*) FROM silver.yellow_trips GROUP BY 1 HAVING count(*)>1` (the DQ rule `silver_trip_hash_unique` guards this) |

## Reprocessing

- **One month again**: just re-run with that `run_month` — idempotent end to end.
- **Everything**: run with `full_refresh=true` (rebuilds silver/fact from bronze;
  bronze itself is never destroyed by the pipeline).
- **Data rollback**: Delta time travel, e.g.
  `RESTORE TABLE taxi_prod.gold.fact_trips TO VERSION AS OF <n>;`
  (`DESCRIBE HISTORY` to pick the version).
- **Code rollback**: revert the commit on `main` (dev auto-redeploys), tag a
  patch release for prod.

## Housekeeping

- `OPTIMIZE` + `VACUUM` on fact/silver monthly (serverless supports SQL maintenance).
- Rotate PATs used by GitHub Actions; they are per-environment and independently revocable.
- Ops tables grow slowly; prune > 1-year-old audit rows if desired.
