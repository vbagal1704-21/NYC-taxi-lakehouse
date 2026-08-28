# Implementation Guide — Step by Step

Follow the phases in order. Each phase ends with a checkpoint so you always know
the project is in a working state. Total effort: roughly 4–6 focused sessions.

---

## Phase 0 — Prerequisites (30 min)

1. **Databricks Free Edition account** — sign up at
   https://www.databricks.com/learn/free-edition (use your Google account or email OTP).
   You get one serverless workspace with Unity Catalog.
2. **GitHub account** — free tier is enough.
3. **Local tools** (your laptop):
   ```bash
   # Git, Python 3.11+, then the Databricks CLI (v0.220+ required for bundles)
   # macOS:  brew tap databricks/tap && brew install databricks
   # Windows: winget install Databricks.DatabricksCLI
   # Linux:  curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
   databricks --version
   ```

**Checkpoint:** you can log into the workspace in a browser and `databricks --version` prints ≥ 0.220.

---

## Phase 1 — Workspace authentication (15 min)

1. In the workspace: click your avatar → **Settings → Developer → Access tokens → Manage → Generate new token**. Name it `cli-dev`, copy the value.
   (On Free Edition, PAT is the reliable CLI auth method — OAuth U2M may not be offered.)
2. On your laptop:
   ```bash
   databricks configure
   # Host:  https://<your-workspace>.cloud.databricks.com   (copy from browser URL)
   # Token: <paste>
   databricks current-user me     # should print your user JSON
   ```

**Checkpoint:** `databricks current-user me` works.

---

## Phase 2 — Git repository (30 min)

1. Create an empty GitHub repo, e.g. `nyc-taxi-lakehouse` (private is fine).
2. Push this project:
   ```bash
   cd nyc-taxi-lakehouse
   git init -b main
   git add . && git commit -m "Initial commit: taxi lakehouse scaffold"
   git remote add origin git@github.com:<you>/nyc-taxi-lakehouse.git
   git push -u origin main
   ```
3. **Branch protection** (enterprise practice): repo → Settings → Branches →
   Add rule for `main`: require a pull request before merging, require the
   **CI** status check to pass.
4. **Branching model** you'll use from now on:
   - `feature/<thing>` branches off `main`
   - PR → CI runs lint + unit tests → review → merge to `main`
   - merge to `main` auto-deploys to **dev**; UAT and prod are explicit promotions.

**Checkpoint:** repo on GitHub, `main` protected, CI workflow visible under Actions (it will run and pass once Phase 3's local test run passes — same commands).

---

## Phase 3 — First deployment to dev (45 min)

1. Validate and deploy the bundle:
   ```bash
   databricks bundle validate -t dev
   databricks bundle deploy -t dev
   ```
   This uploads the code to your workspace and creates two Workflows jobs
   (dev mode prefixes them with your username): `taxi_setup_dev` and `taxi_pipeline_dev`.
2. Bootstrap the dev environment (creates catalog `taxi_dev`, schemas
   `landing/bronze/silver/gold/ops`, Volumes, ops tables):
   ```bash
   databricks bundle run -t dev taxi_setup_job
   ```
3. In the workspace, open **Catalog** and confirm `taxi_dev` exists with its five schemas.

**Checkpoint:** setup job green; catalog + schemas + `taxi_dev.landing.raw` volume visible.

---

## Phase 4 — First pipeline run (1 hour)

1. Run the pipeline for one month:
   ```bash
   databricks bundle run -t dev taxi_pipeline_job --params run_month=2024-01
   ```
   Watch it in **Jobs & Pipelines**: ingest → bronze → DQ(bronze) → silver →
   DQ(silver) → dims → fact → aggregates → reconciliation.

   *If the ingest task can't reach the TLC host* (Free Edition restricts
   serverless egress), use the fallback:
   ```bash
   ./scripts/upload_landing_from_local.sh taxi_dev 2024-01
   databricks bundle run -t dev taxi_pipeline_job --params run_month=2024-01
   ```
2. Verify each layer in the SQL editor:
   ```sql
   SELECT count(*) FROM taxi_dev.bronze.yellow_trips_raw;   -- ~3M
   SELECT count(*) FROM taxi_dev.silver.yellow_trips;
   SELECT count(*), array_join(dq_violations, ',') FROM taxi_dev.silver.yellow_trips_quarantine GROUP BY 2;
   SELECT * FROM taxi_dev.gold.agg_monthly_kpis;
   SELECT * FROM taxi_dev.ops.recon_results ORDER BY checked_at DESC;
   SELECT * FROM taxi_dev.ops.pipeline_runs ORDER BY logged_at DESC;
   ```
3. Re-run the same command — everything should skip/no-op (idempotency proof):
   manifest skips the download, Auto Loader ingests nothing new, silver
   processes 0 rows, fact MERGE inserts nothing.

**Checkpoint:** all recon checks `passed=true`; second run is a clean no-op.

---

## Phase 5 — Load history & watch incrementality (30 min)

```bash
for m in 2024-02 2024-03 2024-04 2024-05 2024-06; do
  databricks bundle run -t dev taxi_pipeline_job --params run_month=$m
done
```
(Free Edition allows 5 concurrent tasks — run months sequentially, as above.)
Confirm `agg_monthly_kpis` now shows a row per month and recon still balances
per source file. You now have ~18M rows — a respectable dataset.

**Checkpoint:** 6 months loaded, recon green for every file.

---

## Phase 6 — BI consumption (1–2 hours)

1. **Dashboard**: workspace → **Dashboards** → Create dashboard. Add datasets
   from `bi/queries/*.sql` (point them at `taxi_dev` while developing) and build:
   - KPI counters (total trips, revenue, avg fare — from `agg_monthly_kpis`)
   - Line: monthly trips & revenue trend
   - Bar: top-15 zones by revenue
   - Heatmap: demand by day-of-week × hour
   - Pie/table: payment mix & tipping
   - Ops tile: DQ + recon status from `05_ops_health.sql`
2. **Genie (NL BI)**: Catalog → `taxi_dev.gold` → create a Genie space over the
   gold schema so business users can ask "which borough tips best on weekends?"
3. Add table/column comments in the gold schema (see `docs/data_dictionary.md`)
   — Genie and human analysts both benefit.

**Checkpoint:** a dashboard a stakeholder could actually use.

---

## Phase 7 — CI/CD wiring (1 hour)

1. GitHub repo → Settings → **Environments** → create `dev`, `uat`, `prod`.
   - On `prod`: add yourself under **Required reviewers** (this is the manual
     prod gate).
2. In *each* environment add two secrets (same workspace on Free Edition —
   in a multi-workspace enterprise these would differ per environment):
   - `DATABRICKS_HOST` = your workspace URL
   - `DATABRICKS_TOKEN` = a PAT (generate a fresh one per environment; name
     them `gh-dev`, `gh-uat`, `gh-prod` so they're revocable independently)
3. Test the full promotion path:
   ```bash
   git checkout -b feature/test-cicd
   # make a trivial change, e.g. tweak a comment
   git commit -am "test: exercise CI/CD" && git push -u origin feature/test-cicd
   ```
   - Open a PR → **CI** runs lint + tests → merge → **Deploy to dev** fires automatically.
   - Actions → **Deploy to UAT** → Run workflow (deploys `-t uat`; optionally runs the pipeline).
   - Run `taxi_setup_job` once for uat: `databricks bundle run -t uat taxi_setup_job`
   - Create a GitHub **Release** (tag `v1.0.0`) → **Deploy to prod** waits for
     your approval → approve → deploys `-t prod`. Run prod setup once, then the
     pipeline.

**Checkpoint:** three environment deployments, prod gated by approval, all from Git.

---

## Phase 8 — Production posture (30 min)

- The prod job has a monthly schedule (5th, 06:00 IST) — TLC publishes with
  ~2-month lag, so schedule computes the right `run_month` or run backfills
  explicitly with `--params run_month=YYYY-MM`.
- Email notifications on failure are configured in `resources/jobs.yml` — verify
  the address.
- Rollback drill: `git revert <commit>` on main → dev redeploys; tag a patch
  release to roll prod back. Data-side rollback: Delta time travel
  (`RESTORE TABLE taxi_prod.gold.fact_trips TO VERSION AS OF <n>`).

**Checkpoint:** you can explain (and demo) both code rollback and data rollback.

---

## Phase 9 — Enterprise failure drills (highly recommended)

These are what interviewers ask about. Each is a 20-minute exercise:

1. **DQ gate failure**: temporarily tighten `silver_total_amount_range` max to `100`
   in `conf/dq_rules.yml`, deploy to dev, run — watch the pipeline stop at the DQ
   task with results persisted in `ops.dq_results`. Revert.
2. **Schema drift**: TLC files differ across years (e.g. `Airport_fee` casing).
   Load `2023-01` — `_rescued_data` and the column mapping handle it; check
   what landed in bronze vs silver.
3. **Quarantine triage**: query quarantine reasons, decide a remediation
   (e.g. negative totals are refunds → new rule or a cleansing step), implement
   via PR, watch it promote through environments.
4. **Backfill**: `--params run_month=2023-06,full_refresh=false` — prove
   out-of-order months reconcile.
5. **Late file replay**: delete one month's bronze rows in dev, reset that
   file's checkpoint? No — demonstrate the *correct* enterprise answer:
   `full_refresh=true` rebuild in dev, and explain why you never hand-edit prod.

---

## Phase 10 — Where to take it next

See `docs/enterprise_extensions.md`: alerting via SQL Alerts, Lakehouse
Monitoring, dbt or DLT variant, Great Expectations, Terraform for workspace
config, streaming ingestion, SCD2 fact corrections, cost observability, and a
green/yellow (multi-source) conformance exercise.
