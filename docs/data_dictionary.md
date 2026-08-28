# Data Dictionary (Gold layer — BI-facing)

## fact_trips
One row per valid taxi trip.

| Column | Type | Description |
|---|---|---|
| trip_hash | string | Deterministic SHA-256 of the business key; primary key |
| date_key | int | FK → dim_date (yyyymmdd of pickup) |
| pickup_ts / dropoff_ts | timestamp | Trip start/end |
| vendor_id | int | FK → dim_vendor |
| rate_code_id | int | FK → dim_rate_code |
| payment_type_id | int | FK → dim_payment_type |
| pickup_zone_id / dropoff_zone_id | int | FK → dim_zone.zone_id (join `is_current = true`, or between effective dates for as-was analysis) |
| passenger_count | int | Reported passengers (0–8 enforced) |
| trip_distance | double | Miles (0–500 enforced) |
| trip_minutes | double | Derived duration |
| avg_speed_mph | double | Derived; NULL when duration is 0 |
| fare_amount, tip_amount, tolls_amount, congestion_surcharge, airport_fee, total_amount | double | USD amounts; `total_amount > 0` enforced |
| tip_pct | double | tip / fare × 100; NULL when fare ≤ 0 |

## dim_date
Calendar dimension: date_key, date, year, quarter, month, month_name,
day_of_month, day_of_week, day_name, week_of_year, is_weekend.

## dim_zone (SCD Type 2)
zone_id (business key), borough, zone_name, service_zone,
effective_from, effective_to, is_current.

## dim_vendor / dim_payment_type / dim_rate_code
Reference decodes from the official TLC data dictionary.

## Aggregate marts
- **agg_daily_zone_revenue**: date_key × pickup zone → trips, revenue,
  avg_distance, avg_tip_pct, avg_trip_minutes.
- **agg_monthly_kpis**: month → trips, revenue, avg_fare, avg_tip_pct,
  active_pickup_zones.

## Ops tables (monitoring)
- **ops.pipeline_runs**: run_id, task_name, status, metrics(JSON), error, elapsed_seconds.
- **ops.dq_results**: one row per rule evaluation (severity, failed_rows, passed).
- **ops.recon_results**: one row per reconciliation check (source vs target values).
- **ops.ingest_manifest**: one row per landed file with source row count.
