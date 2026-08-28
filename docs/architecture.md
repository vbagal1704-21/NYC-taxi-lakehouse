# Architecture & Design Decisions

## Medallion layers

| Layer | Table(s) | Contract |
|---|---|---|
| Landing | `/Volumes/<cat>/landing/raw/{yellow,zones}` | Files exactly as the source sent them; manifest records what arrived (`ops.ingest_manifest`) with source row counts |
| Bronze | `bronze.yellow_trips_raw`, `bronze.taxi_zone_lookup` | Append-only, schema-on-read, lineage columns (`_source_file`, `_ingested_at`), drift captured in `_rescued_data`. No business logic. |
| Silver | `silver.yellow_trips`, `silver.yellow_trips_quarantine` | Typed, snake_case, deduplicated on business key, invalid rows quarantined **with reasons** — never dropped. Derived columns + `trip_hash` surrogate key. |
| Gold | `dim_*`, `fact_trips`, `agg_*` | Kimball star schema. Fact is MERGE-idempotent on `trip_hash`. Aggregates rebuilt deterministically from the fact. BI reads gold only. |
| Ops | `ops.pipeline_runs`, `ops.dq_results`, `ops.recon_results`, `ops.ingest_manifest` | Operational metadata: full audit trail of every run, check and file. |

## Key decisions (and the "why" you'd give in a design review)

**Environments as catalogs, not workspaces.** Free Edition allows one workspace/
metastore. Catalog-per-environment gives the same logical isolation (separate
data, separate jobs, same code parameterised by `catalog`) and is a real pattern
enterprises use inside a workspace; the bundle targets are already structured so
that pointing `uat`/`prod` at different `workspace.host` values later is a
two-line change.

**Thin notebooks, thick library.** Notebooks only parse widgets and call
`taxi_lakehouse.*` functions. All logic is plain PySpark functions over
DataFrames — unit-testable in CI on local Spark + Delta, reviewable in PRs,
reusable if orchestration moves (DLT, Airflow, dbt).

**Auto Loader over COPY INTO / manual reads.** Exactly-once file discovery with
checkpoints; re-running never double-loads; schema evolution is explicit
(`rescue` mode) instead of silent.

**Quarantine over drop.** `bronze_dedup == silver_clean + silver_quarantine` is
an invariant the reconciliation job enforces. Every excluded row is explainable
(`dq_violations` array). This is the difference between a demo and a pipeline
finance will sign off on.

**DQ rules as config.** Adding a rule is a YAML PR, reviewed like any change,
promoted through environments like any change. Severity distinguishes
"stop the pipeline" (`error`) from "record and watch" (`warn`).

**Reconciliation as a first-class task.** Counts must match exactly per source
file (source→bronze), accounting must balance (bronze→silver), and money must
survive transformation (silver→gold→marts, sum of `total_amount` to the cent).
A green run *means* the data balances.

**MERGE on a deterministic hash.** `trip_hash = sha2(business key)` makes the
fact load idempotent and replay-safe — the standard answer to "what if the job
runs twice?".

**SCD Type 2 on dim_zone.** Zones rarely change, which makes it a clean
teaching implementation: generic `scd2_apply()` (expire + insert pattern via
Delta MERGE) reusable for any dimension.

**Promotion model.** PR → CI (lint + tests) → merge to main → auto-deploy dev →
manual UAT dispatch → tagged release + human approval → prod. Code moves;
data never moves between environments — each environment computes its own.

## Data flow guarantees

1. **No loss**: every source row is in silver clean or quarantine (recon check).
2. **No duplication**: Auto Loader checkpoints + dedup + MERGE-on-hash.
3. **Traceability**: any fact row → `trip_hash` → silver → `_source_file` →
   manifest → source URL; any run → `ops.pipeline_runs`.
4. **Fail loud, fail persisted**: DQ/recon failures stop the run *after*
   writing their results.
