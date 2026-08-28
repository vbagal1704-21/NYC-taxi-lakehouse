-- Monthly KPI trend (source: gold.agg_monthly_kpis)
-- Dashboard tile: line/combo chart — trips & revenue by month
SELECT
  month,
  trips,
  revenue,
  avg_fare,
  avg_tip_pct
FROM taxi_prod.gold.agg_monthly_kpis
ORDER BY month;
