/* =========================================================
FINAL OUTPUT #2 — CARRIER 2x2 POSITIONING
Fast vs Expensive (median benchmark)
========================================================= */
WITH carrier_kpi AS (
  SELECT
    carrier,
    COUNT(*) AS shipments,
    ROUND(SUM(cost)/NULLIF(COUNT(*),0),2) AS cost_per_unit,
    ROUND(AVG(transit_days),2) AS avg_transit_days,
    ROUND(SUM(CASE WHEN status='Delayed' THEN 1 ELSE 0 END)::numeric/NULLIF(COUNT(*),0)*100,2) AS delay_rate,
    ROUND(SUM(CASE WHEN status='Returned' THEN 1 ELSE 0 END)::numeric/NULLIF(COUNT(*),0)*100,2) AS returned_rate,
    ROUND(SUM(CASE WHEN status='Delivered' THEN 1 ELSE 0 END)::numeric/NULLIF(COUNT(*),0)*100,2) AS delivered_rate
  FROM log_data
  GROUP BY 1
  HAVING COUNT(*) >= 30
),
bench AS (
  SELECT
    percentile_cont(0.5) WITHIN GROUP (ORDER BY cost_per_unit) AS med_cpu,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY avg_transit_days) AS med_days
  FROM carrier_kpi
)
SELECT
  c.*,
  CASE
    WHEN c.cost_per_unit > b.med_cpu AND c.avg_transit_days < b.med_days THEN 'FAST but EXPENSIVE'
    WHEN c.cost_per_unit <= b.med_cpu AND c.avg_transit_days < b.med_days THEN 'FAST and CHEAP (best)'
    WHEN c.cost_per_unit > b.med_cpu AND c.avg_transit_days >= b.med_days THEN 'SLOW and EXPENSIVE (worst)'
    ELSE 'SLOW but CHEAP'
  END AS carrier_position
FROM carrier_kpi c
CROSS JOIN bench b
ORDER BY shipments DESC;
