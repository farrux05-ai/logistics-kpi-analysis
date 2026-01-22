/* =========================================================
FINAL OUTPUT #1 — WAREHOUSE PRIORITY SCORECARD
(warehouse vs overall: deltas + flags + priority_score)
========================================================= */
WITH overall AS (
  SELECT
    ROUND(SUM(cost)/NULLIF(COUNT(*),0),2) AS cost_per_unit,
    ROUND(SUM(cost)/NULLIF(SUM(weight_kg),0),2) AS cost_per_kg,
    ROUND(SUM(cost)/NULLIF(SUM(distance_miles),0),2) AS cost_per_mile,
    ROUND(AVG(transit_days),2) AS avg_transit_days,
    ROUND(SUM(CASE WHEN status='Delivered' THEN 1 ELSE 0 END)::numeric/NULLIF(COUNT(*),0)*100,2) AS delivered_rate,
    ROUND(SUM(CASE WHEN status='Delayed' THEN 1 ELSE 0 END)::numeric/NULLIF(COUNT(*),0)*100,2) AS delay_rate,
    ROUND(SUM(CASE WHEN status='Lost' THEN 1 ELSE 0 END)::numeric/NULLIF(COUNT(*),0)*100,2) AS lost_rate,
    ROUND(SUM(CASE WHEN status='Returned' THEN 1 ELSE 0 END)::numeric/NULLIF(COUNT(*),0)*100,2) AS returned_rate
  FROM log_data
),
wh AS (
  SELECT
    origin_warehouse,
    COUNT(*) AS shipment_count,
    ROUND(AVG(transit_days),2) AS avg_transit_days,
    ROUND(SUM(cost)/NULLIF(COUNT(*),0),2) AS cost_per_unit,
    ROUND(SUM(cost)/NULLIF(SUM(weight_kg),0),2) AS cost_per_kg,
    ROUND(SUM(cost)/NULLIF(SUM(distance_miles),0),2) AS cost_per_mile,
    ROUND(SUM(CASE WHEN status='Delivered' THEN 1 ELSE 0 END)::numeric/NULLIF(COUNT(*),0)*100,2) AS delivered_rate,
    ROUND(SUM(CASE WHEN status='Delayed' THEN 1 ELSE 0 END)::numeric/NULLIF(COUNT(*),0)*100,2) AS delay_rate,
    ROUND(SUM(CASE WHEN status='Lost' THEN 1 ELSE 0 END)::numeric/NULLIF(COUNT(*),0)*100,2) AS lost_rate,
    ROUND(SUM(CASE WHEN status='Returned' THEN 1 ELSE 0 END)::numeric/NULLIF(COUNT(*),0)*100,2) AS returned_rate
  FROM log_data
  GROUP BY 1
),
scorecard AS (
  SELECT
    w.*,

    -- RATE deltas: percentage points (p.p)
    ROUND(w.delivered_rate - o.delivered_rate,2) AS d_delivered_pp,
    ROUND(w.delay_rate - o.delay_rate,2)         AS d_delay_pp,
    ROUND(w.lost_rate - o.lost_rate,2)           AS d_lost_pp,
    ROUND(w.returned_rate - o.returned_rate,2)   AS d_return_pp,

    -- TIME delta: days
    ROUND(w.avg_transit_days - o.avg_transit_days,2) AS d_transit_days,

    -- COST deltas: relative %
    ROUND((w.cost_per_unit - o.cost_per_unit)/NULLIF(o.cost_per_unit,0)*100,2) AS d_cost_per_unit_pct,
    ROUND((w.cost_per_kg   - o.cost_per_kg)/NULLIF(o.cost_per_kg,0)*100,2)     AS d_cost_per_kg_pct,
    ROUND((w.cost_per_mile - o.cost_per_mile)/NULLIF(o.cost_per_mile,0)*100,2) AS d_cost_per_mile_pct,

    -- FLAGS (thresholds)
    CASE WHEN (w.delivered_rate - o.delivered_rate) <= -2.00 THEN 1 ELSE 0 END AS flag_delivery,
    CASE WHEN (w.delay_rate - o.delay_rate) >=  2.00 THEN 1 ELSE 0 END         AS flag_delay,
    CASE WHEN (w.returned_rate - o.returned_rate) >= 0.70 THEN 1 ELSE 0 END     AS flag_return,
    CASE WHEN (w.lost_rate - o.lost_rate) >= 0.70 THEN 1 ELSE 0 END             AS flag_lost,
    CASE WHEN ((w.cost_per_unit - o.cost_per_unit)/NULLIF(o.cost_per_unit,0)) >= 0.10 THEN 1 ELSE 0 END AS flag_cost_unit,
    CASE WHEN ((w.cost_per_kg   - o.cost_per_kg)/NULLIF(o.cost_per_kg,0))     >= 0.10 THEN 1 ELSE 0 END AS flag_cost_kg
  FROM wh w
  CROSS JOIN overall o
)
SELECT
  origin_warehouse,
  shipment_count,
  d_delivered_pp,
  d_delay_pp,
  d_return_pp,
  d_cost_per_unit_pct,
  d_cost_per_kg_pct,
  (flag_delivery + flag_delay + flag_return + flag_lost + flag_cost_unit + flag_cost_kg) AS flag_count,
  shipment_count * (flag_delivery + flag_delay + flag_return + flag_lost + flag_cost_unit + flag_cost_kg) AS priority_score
FROM scorecard
ORDER BY priority_score DESC, shipment_count DESC;
