-- Operational health: latest DQ + reconciliation status (ops monitoring tile)
SELECT 'DQ' AS kind, rule_name AS check_name, passed, checked_at
FROM taxi_prod.ops.dq_results
WHERE checked_at >= current_date() - INTERVAL 35 DAYS

UNION ALL

SELECT 'RECON' AS kind, check_name, passed, checked_at
FROM taxi_prod.ops.recon_results
WHERE checked_at >= current_date() - INTERVAL 35 DAYS
ORDER BY checked_at DESC;
