# Enterprise Extensions — where to take the project next

The core project already covers Git, CI/CD, three environments, medallion,
DQ, reconciliation, dimensional modelling, auditing and BI. These add-ons each
deepen one enterprise dimension; do them as later iterations (each is a great
"v2" story).

1. **Alerting & observability.** Databricks SQL Alerts on
   `ops.dq_results`/`ops.recon_results` (`passed = false` in last 24h → email).
   Add Lakehouse Monitoring on `gold.fact_trips` for drift/profile metrics.

2. **Second source + conformance.** Ingest green-taxi (and FHV/Uber-Lyft) files
   into the same model: union into a conformed `fact_trips` with a
   `service_type` dimension. Demonstrates multi-source conformance — a very
   common enterprise problem.

3. **Streaming flavour.** Convert bronze→silver to a continuous stream
   (or a file-arrival trigger on the landing volume) to show near-real-time
   patterns; discuss watermarking and late-arriving data handling.

4. **dbt or DLT variant.** Re-implement silver→gold in dbt (dbt-databricks) or
   as a Lakeflow Declarative Pipeline with expectations — being able to compare
   frameworks is senior-engineer territory.

5. **Great Expectations / dqx.** Swap the hand-rolled DQ engine for a standard
   framework and keep the same rules — shows you understand the concepts, not
   just a tool.

6. **Terraform / IaC.** Manage catalogs, grants and (on a paid workspace)
   workspaces themselves with the Databricks Terraform provider; DABs for
   application resources + Terraform for platform resources is the common split.

7. **Service principals & OAuth.** On a paid/trial workspace, replace PATs with
   a service principal per environment and OIDC federation from GitHub Actions
   (no long-lived secrets) — the current best practice.

8. **Data contracts.** Write a JSON/YAML schema contract for the TLC feed;
   validate landed files against it before bronze and fail fast on breaking
   drift (vs. the current permissive rescue mode).

9. **Semantic layer & governance.** Metric views / certified datasets, column
   tags (PII patterns — e.g. tag nothing here, but document the process),
   lineage walkthrough via Catalog Explorer.

10. **Cost & performance.** Liquid clustering keys review, OPTIMIZE cadence,
    photon/serverless cost analysis from system tables
    (`system.billing.usage`) — build an ops cost dashboard.

11. **Orchestrator integration.** Trigger the Databricks job from Airflow
    (DatabricksRunNowOperator) to mirror shops where Airflow owns scheduling.

12. **SLA & incident process.** Define an SLA (data available by the 6th),
    an on-call runbook (already started), and a post-mortem template.
