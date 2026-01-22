/* =========================
1) QUICK OVERVIEW (EDA)
========================= */
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT origin_warehouse) AS total_warehouses,
  COUNT(DISTINCT destination) AS total_destinations,
  COUNT(DISTINCT carrier) AS total_carriers,
  MIN(shipment_date) AS start_ship_date,
  MAX(shipment_date) AS end_ship_date,
  SUM(cost) AS total_cost,
  ROUND(AVG(cost),2) AS avg_cost,
  ROUND(AVG(weight_kg),2) AS avg_kg,
  ROUND(AVG(transit_days),2) AS avg_transit_days
FROM log_data;


/* =========================
2) OVERALL KPI (benchmark)
========================= */
WITH overall AS (
  SELECT
    ROUND(SUM(cost)/NULLIF(COUNT(*),0),2) AS cost_per_unit,
    ROUND(SUM(cost)/NULLIF(SUM(weight_kg),0),2) AS cost_per_kg,
    ROUND(SUM(cost)/NULLIF(SUM(distance_miles),0),2) AS cost_per_mile,
    ROUND(AVG(transit_days),2) AS avg_transit_days,
    ROUND(AVG(weight_kg),2) AS avg_kg,
    ROUND(SUM(CASE WHEN status='Delivered' THEN 1 ELSE 0 END)::numeric/NULLIF(COUNT(*),0)*100,2) AS delivered_rate,
    ROUND(SUM(CASE WHEN status='Delayed' THEN 1 ELSE 0 END)::numeric/NULLIF(COUNT(*),0)*100,2) AS delay_rate,
    ROUND(SUM(CASE WHEN status='Lost' THEN 1 ELSE 0 END)::numeric/NULLIF(COUNT(*),0)*100,2) AS lost_rate,
    ROUND(SUM(CASE WHEN status='Returned' THEN 1 ELSE 0 END)::numeric/NULLIF(COUNT(*),0)*100,2) AS returned_rate
  FROM log_data
)
SELECT * FROM overall;
