-- Payment mix and tipping behaviour by payment type (pie + table)
SELECT
  p.payment_type,
  COUNT(*)                        AS trips,
  ROUND(SUM(f.total_amount), 2)   AS revenue,
  ROUND(AVG(f.tip_pct), 2)        AS avg_tip_pct
FROM taxi_prod.gold.fact_trips f
LEFT JOIN taxi_prod.gold.dim_payment_type p
  ON f.payment_type_id = p.payment_type_id
GROUP BY p.payment_type
ORDER BY trips DESC;
