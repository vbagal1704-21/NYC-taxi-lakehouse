-- Top 15 pickup zones by revenue (bar chart)
SELECT
  zone_name,
  borough,
  SUM(revenue)  AS revenue,
  SUM(trips)    AS trips,
  ROUND(SUM(revenue) / SUM(trips), 2) AS revenue_per_trip
FROM taxi_prod.gold.agg_daily_zone_revenue
GROUP BY zone_name, borough
ORDER BY revenue DESC
LIMIT 15;
