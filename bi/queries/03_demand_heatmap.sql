-- Demand heatmap: trips by day-of-week x hour (heatmap chart)
SELECT
  d.day_name,
  d.day_of_week,
  HOUR(f.pickup_ts) AS pickup_hour,
  COUNT(*)          AS trips
FROM taxi_prod.gold.fact_trips f
JOIN taxi_prod.gold.dim_date d ON f.date_key = d.date_key
GROUP BY d.day_name, d.day_of_week, HOUR(f.pickup_ts)
ORDER BY d.day_of_week, pickup_hour;
