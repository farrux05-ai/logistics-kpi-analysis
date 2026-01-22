/* =========================================================
FINAL OUTPUT #3 — WORST ROUTES (origin→destination)
Badness score = cost + delay + return (normalized vs average)
========================================================= */
WITH route_kpi AS (
  SELECT
    origin_warehouse,
    destination,
    COUNT(*) AS shipments,
    ROUND(SUM(cost)/NULLIF(COUNT(*),0),2) AS cost_per_unit,
    ROUND(AVG(transit_days),2) AS avg_transit_days,
    ROUND(SUM(CASE WHEN status='Delayed' THEN 1 ELSE 0 END)::numeric/NULLIF(COUNT(*),0)*100,2) AS delay_rate,
    ROUND(SUM(CASE WHEN status='Returned' THEN 1 ELSE 0 END)::numeric/NULLIF(COUNT(*),0)*100,2) AS returned_rate
  FROM log_data
  GROUP BY 1,2
  HAVING COUNT(*) >= 20
),
bench AS (
  SELECT
    AVG(cost_per_unit) AS avg_cpu,
    AVG(delay_rate) AS avg_delay,
    AVG(returned_rate) AS avg_return
  FROM route_kpi
)
SELECT
  r.*,
  ROUND(
    (r.cost_per_unit / NULLIF(b.avg_cpu,0)) +
    (r.delay_rate / NULLIF(b.avg_delay,0)) +
    (r.returned_rate / NULLIF(b.avg_return,0)),
  2) AS badness_score
FROM route_kpi r
CROSS JOIN bench b
ORDER BY badness_score DESC, shipments DESC;
