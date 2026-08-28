# NYC Taxi Lakehouse — Enterprise-Style Data Pipeline on Databricks

An end-to-end lakehouse project built the way real data-engineering teams build them:
medallion architecture, three environments (dev / UAT / prod), config-driven data
quality, layer-to-layer reconciliation, dimensional modelling with SCD Type 2,
audit logging, unit tests, Databricks Asset Bundles, and CI/CD with GitHub Actions.

**Business problem.** The NYC Taxi & Limousine Commission publishes ~3M yellow-taxi
trips per month. The business (think: a mobility analytics team) needs a trusted,
BI-ready model to answer: Where and when is demand highest? How is revenue trending?
How do tipping and payment behaviour differ by zone and payment type? Where should
supply be positioned by hour and day? Raw source files are messy — duplicates,
negative fares, impossible timestamps, schema drift between months — so the pipeline
must prove correctness, not assume it.

## Architecture

```
TLC public data (monthly parquet, ~3M rows/month)
        │  01 ingest (download + manifest w/ source row counts)
        ▼
┌─ landing Volume ─┐   Auto Loader (exactly-once, schema drift rescued)
│  /Volumes/../raw │ ──────────────────────────────► BRONZE  yellow_trips_raw
└──────────────────┘                                    │  raw + lineage columns
                          DQ gate (bronze rules)        ▼
                                                     SILVER  yellow_trips (clean, deduped, typed)
                                                        │    yellow_trips_quarantine (+reasons)
                          DQ gate (silver rules)        ▼
                                                      GOLD   star schema
                                                        │    dim_date · dim_zone (SCD2) · dim_vendor
                                                        │    dim_payment_type · dim_rate_code
                                                        │    fact_trips (MERGE, idempotent)
                                                        ▼    agg_daily_zone_revenue · agg_monthly_kpis
                          reconciliation (counts + ₹ sums across every layer)
                                                        ▼
                                          Databricks SQL dashboards / Genie
```

Environments (Free Edition = one workspace, so isolation is at catalog level —
the same pattern many enterprises use *within* a workspace):

| Environment | Catalog     | Deployed by                              |
|-------------|-------------|------------------------------------------|
| dev         | `taxi_dev`  | every merge to `main` (auto)             |
| UAT         | `taxi_uat`  | manual workflow dispatch                 |
| prod        | `taxi_prod` | GitHub Release + required approval       |

## Repository layout

```
databricks.yml               # Asset Bundle: one bundle, 3 targets
resources/jobs.yml           # Workflows: setup job + 9-task pipeline job
conf/dq_rules.yml            # data-quality rules (config, not code)
src/notebooks/               # thin entry-point notebooks (00–08)
src/lib/taxi_lakehouse/      # all real logic — unit-testable python package
  ingest/                    #   TLC downloader + Auto Loader bronze
  transforms/                #   silver cleansing, gold dims/fact/aggregates, SCD2
  quality/                   #   DQ rule engine + reconciliation framework
tests/                       # pytest suite (runs on local Spark + Delta in CI)
.github/workflows/           # CI + deploy-dev/uat/prod
bi/queries/                  # dashboard SQL
scripts/                     # fallback local-upload ingestion
docs/                        # implementation guide, architecture, runbook, ...
```

## Quick start

See **[docs/IMPLEMENTATION_GUIDE.md](docs/IMPLEMENTATION_GUIDE.md)** for the full
click-by-click walkthrough. The short version:

```bash
# 1. authenticate the CLI against your Free Edition workspace (PAT)
databricks configure

# 2. deploy + bootstrap dev
databricks bundle deploy -t dev
databricks bundle run -t dev taxi_setup_job

# 3. run the pipeline for one month
databricks bundle run -t dev taxi_pipeline_job --params run_month=2024-01

# 4. local unit tests
pip install -r requirements-dev.txt && pytest
```

## What makes this "enterprise"

Idempotent, incremental everything (Auto Loader checkpoints, MERGE on hashes,
manifest-driven skips) · quarantine-not-drop error handling · DQ as config with
error/warn severities · reconciliation that must balance before a run is green ·
full audit trail in `ops.*` tables · environment promotion gated by tests and
human approval · every artefact (jobs, schedules, code, rules) in Git.

See [docs/enterprise_extensions.md](docs/enterprise_extensions.md) for the
add-ons that take it further (dbt-style docs, alerting, SCD2 on facts, Great
Expectations, Terraform, streaming).
