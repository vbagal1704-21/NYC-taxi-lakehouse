# BI Layer

`queries/` contains the SQL behind the executive dashboard. Point the catalog
at the environment you're building against (`taxi_dev` while developing,
`taxi_prod` for the real dashboard).

## Building the dashboard (Databricks Dashboards)

1. Workspace → **Dashboards** → *Create dashboard*.
2. In the **Data** tab add each query from `queries/` as a dataset.
3. Suggested canvas:
   - Top row: counters — total trips, total revenue, avg fare, avg tip % (from `01_monthly_kpis.sql` aggregated)
   - Line chart: trips & revenue by month (01)
   - Bar chart: top 15 zones by revenue (02)
   - Heatmap: demand day-of-week × hour (03)
   - Pie + table: payment mix (04)
   - Status table: DQ & reconciliation health (05) — the "can I trust this dashboard?" tile
4. Schedule a dashboard refresh after the monthly pipeline schedule.

## Genie space (natural-language BI)

Create a Genie space over `taxi_prod.gold` (fact + dims + aggs), add the table
comments from `docs/data_dictionary.md`, and seed example questions:
- "Which borough had the highest revenue last month?"
- "How does average tip percentage differ between credit card and cash?"
- "What hour of day is busiest on Saturdays?"
