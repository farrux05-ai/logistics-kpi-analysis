/* =========================================================
BUSINESS QUESTION
- Qaysi warehouse -> eng qimmat va sekin yuk jo‘natmoqda?
- Qaysi carrier -> tez, lekin qimmat?
- Qaysi yo‘nalish (origin -> destination) eng foydasiz?

SCOPE / OUTPUT
1) Dataset overview (EDA)
2) Data cleaning + sanity checks
3) KPI analysis
4) Segmentation (warehouse / carrier / route)
5) Time trends (monthly)
6) Insights + recommendations
========================================================= */


/* ============================
0) DATA LOADING (PostgreSQL)
CSV: logistics_shipments_dataset.csv
============================ */

-- STAGING: CSV ni xatosiz yuklash uchun hamma ustun TEXT
DROP TABLE IF EXISTS stg_shipments;
CREATE TABLE stg_shipments (
  shipment_id         TEXT,
  origin_warehouse    TEXT,
  destination         TEXT,
  carrier             TEXT,
  shipment_date       TEXT,
  delivery_date       TEXT,
  weight_kg           TEXT,
  cost                TEXT,
  status              TEXT,
  distance_miles      TEXT,
  transit_days        TEXT
);

-- IMPORT
-- A) Server-side COPY (fayl DB server turgan joyda bo‘lishi kerak)
-- COPY stg_shipments FROM '/ABSOLUTE/PATH/logistics_shipments_dataset.csv'
-- WITH (FORMAT csv, HEADER true);

-- B) Local import (psql tavsiya) — fayl sizning kompyuteringizda bo‘lsa:
-- \copy stg_shipments FROM 'logistics_shipments_dataset.csv' WITH (FORMAT csv, HEADER true);

-- CLEAN TABLE: loyiha bo‘yicha yagona standart jadval
DROP TABLE IF EXISTS log_data;
CREATE TABLE log_data (
  shipment_id         TEXT PRIMARY KEY,   
  origin_warehouse    TEXT NOT NULL,
  destination         TEXT NOT NULL,
  carrier             TEXT NOT NULL,
  shipment_date       DATE NOT NULL,
  delivery_date       DATE NULL,
  weight_kg           NUMERIC(12,3) NULL,
  cost                NUMERIC(14,2) NULL,
  status              TEXT NOT NULL,
  distance_miles      NUMERIC(14,3) NULL,
  transit_days        INT NULL
);

-- STG -> CLEAN
INSERT INTO log_data (
  shipment_id, origin_warehouse, destination, carrier,
  shipment_date, delivery_date,
  weight_kg, cost, status, distance_miles, transit_days
)
SELECT
  COALESCE(NULLIF(TRIM(shipment_id), ''), 'UNKNOWN')                                  AS shipment_id,
  COALESCE(NULLIF(TRIM(origin_warehouse), ''), 'UNKNOWN')                             AS origin_warehouse,
  COALESCE(NULLIF(TRIM(destination), ''), 'UNKNOWN')                                  AS destination,
  COALESCE(NULLIF(TRIM(carrier), ''), 'UNKNOWN')                                      AS carrier,
  NULLIF(TRIM(shipment_date), '')::DATE                                               AS shipment_date,
  CASE
    WHEN NULLIF(TRIM(delivery_date), '') IS NULL THEN NULL
    ELSE NULLIF(TRIM(delivery_date), '')::DATE
  END                                                                                 AS delivery_date,
  NULLIF(REPLACE(TRIM(weight_kg), ',', ''), '')::NUMERIC                              AS weight_kg,
  NULLIF(REPLACE(TRIM(cost), ',', ''), '')::NUMERIC                                   AS cost,
  COALESCE(NULLIF(TRIM(status), ''), 'UNKNOWN')                                       AS status,
  NULLIF(REPLACE(TRIM(distance_miles), ',', ''), '')::NUMERIC                         AS distance_miles,
  NULLIF(TRIM(transit_days), '')::INT                                                 AS transit_days
FROM stg_shipments;

-- MINIMAL CLEANING
-- transit_days ni delivery_date mavjud bo‘lsa qayta hisoblaymiz
UPDATE log_data
SET transit_days = (delivery_date - shipment_date)
WHERE delivery_date IS NOT NULL;

-- cost NULL bo‘lsa AVG bilan to‘ldiramiz
UPDATE log_data
SET cost = sub.avg_cost
FROM (SELECT AVG(cost) AS avg_cost FROM log_data WHERE cost IS NOT NULL) sub
WHERE log_data.cost IS NULL;

-- Data quality flag view (Delivered bo‘lib delivery_date yo‘q bo‘lsa)
DROP VIEW IF EXISTS v_data_issues;
CREATE VIEW v_data_issues AS
SELECT *
FROM log_data
WHERE (status ILIKE 'delivered' AND delivery_date IS NULL)
   OR shipment_date IS NULL;

-- Indekslar
CREATE INDEX IF NOT EXISTS idx_log_data_shipment_date ON log_data (shipment_date);
CREATE INDEX IF NOT EXISTS idx_log_data_origin        ON log_data (origin_warehouse);
CREATE INDEX IF NOT EXISTS idx_log_data_carrier       ON log_data (carrier);
CREATE INDEX IF NOT EXISTS idx_log_data_status        ON log_data (status);


select * from logistic_data limit 5;
-- 1. Data dictionary and EDA
SELECT
	count(*) as total_rows, 
	count(distinct(origin_warehouse)) as total_warehouse, 
	count(distinct(destination)) total_destination, 
	count(distinct(carrier)) as total_carriers, 
	min(shipment_date) as starting_ship_date, 
	max(shipment_date) as ended_ship_date,
	min(delivery_date) as starting_del_date, 
	max(delivery_date) as ended_del_date, 
	sum(cost) as total_cost, 
	count(shipment_id) as total_shipment,
	round(avg(cost), 2) as avg_shipment_cost, 
	sum(weight_kg) as total_kg, 
	round(avg(weight_kg), 2) as avg_kg, 
	round(avg(transit_days), 2) as avg_transit_days 
FROM log_data;

 --2.2 OUTLIERS
 WITH stats as (
	SELECT 
		percentile_cont(0.25) WITHIN GROUP (ORDER BY cost) as q1,
		percentile_cont(0.75) WITHIN GROUP (ORDER BY cost) as q3
	FROM log_data),
	iqr_bounds as (
		select q1, q3, (q3-q1)*1.5 as step FROM stats
	)
	SELECT * FROM log_data, iqr_bounds
	where cost > (q3 + step) or cost < (q1 - step);

-- 3. KPI ANALYSIS

 -- All main KPIs in one query
SELECT
	round(avg(cost), 2) as avg_shipment_cost,
	round(sum(cost) / sum(weight_kg), 2) as cost_per_kg,
 	round(sum(cost) / count(shipment_id), 2) as cost_per_unit,
	round(avg(transit_days), 2) as avg_transit_days,
	round(avg(weight_kg),2) as avg_kg,
	round(SUM(cost) / SUM(distance_miles),2) as cost_per_mile,
	round(sum(case when status='Delivered' then 1 else 0 end) / cast(count(*) as numeric) * 100, 2) as delivery_rate,
	round(sum(case when status='Delayed' then 1 else 0 end) / cast(count(*) as numeric) * 100, 2) as delay_rate,
	round(sum(case when status='Lost' then 1 else 0 end) / cast(count(*) as numeric) * 100, 2) as lost_rate,
	round(sum(case when status='Returned' then 1 else 0 end) / cast(count(*) as numeric) * 100, 2) as returned_rate
FROM log_data;

-- Insight: muammo qayerda (warehouse/carrier/route/time).

-- daily shipment trend
with per_day as(
	select 
		origin_warehouse,
		shipment_date::date as ship_date,
		count(*) as per_day_shipment
	from log_data
	group by origin_warehouse, ship_date
)
select
	origin_warehouse,
	round(avg(per_day_shipment), 2) as avg_daily_shipment
	from per_day
	group by origin_warehouse
	order by avg_daily_shipment desc;

-- monthly shipment trend 
select	
	date_trunc('month', shipment_date) as ship_month,
	count(*) as per_month_shipment
from log_data
group by ship_month
order by ship_month;

-- Month bo‘yicha OTD% trend (yomonlashyaptimi?)
select 
	date_trunc('month', shipment_date) as monthly_shipment,
	round(sum(case when status='Delivered' then 1 else 0 end) / cast(count(*) as numeric)*100,2) as otd_percentage,
	round(avg(transit_days),2) as  avg_transit_days 
	from log_data
	group by monthly_shipment
	order by monthly_shipment asc;

--Qaytarishlar oshyaptimi?
SELECT 
	date_trunc('month', shipment_date) as monthly_ship,
	round(sum(case when status = 'Returned' then 1 else 0 end) / CAST(count(*) as numeric)*100,2) as returned_rate
	from log_data
	group by monthly_ship
	order by monthly_ship asc;

-- cost qanday dinamikada
select	
	date_trunc('month', shipment_date) as monthly,
	sum(cost) as cost_per_month
from log_data
group by monthly
order by monthly asc;

--avg shipment days by month
SELECT 
	date_trunc('month', shipment_date) as monthly_transit,
	round(avg(transit_days),2) as avg_transit_days
from log_data
group by monthly_transit
order by monthly_transit asc;

--  4. SEGMENTATION
-- A) Warehouse bo‘yicha

SELECT
  origin_warehouse,
  COUNT(*) AS shipment_count,
  ROUND(AVG(transit_days), 2) AS avg_transit_days,
  ROUND(SUM(cost) / SUM(weight_kg), 2) AS cost_per_kg,
  ROUND(SUM(cost) / COUNT(shipment_id), 2) AS cost_per_unit,
  ROUND(SUM(weight_kg) / COUNT(shipment_id), 2) AS avg_kg,
  round(SUM(cost) / SUM(distance_miles),2) as cost_per_mile,
  ROUND(SUM(CASE WHEN status = 'Delayed' THEN 1 ELSE 0 END) / CAST(COUNT(*) AS NUMERIC) * 100, 2) AS delay_rate,
  ROUND(SUM(CASE WHEN status = 'Lost' THEN 1 ELSE 0 END) / CAST(COUNT(*) AS NUMERIC) * 100, 2) AS lost_rate,
  ROUND(SUM(CASE WHEN status = 'Returned' THEN 1 ELSE 0 END) / CAST(COUNT(*) AS NUMERIC) * 100, 2) AS returned_rate,
  ROUND(SUM(CASE WHEN status = 'Delivered' THEN 1 ELSE 0 END) / CAST(COUNT(*) AS NUMERIC) * 100, 2) AS otd_percentage
FROM log_data
GROUP BY origin_warehouse
ORDER BY cost_per_kg DESC;

-- Qaysi warehouse sekin, lekin arzon?
-- Qaysi tez, lekin qimmat?
with overall as (
	select
	round(AVG(cost),2) as avg_shipment_cost,
	round(sum(cost) / NULLIF(sum(weight_kg),0),2) as cost_per_kg,
	round(sum(cost) / NULLIF(count(*),0),2) as cost_per_unit,
	round(avg(transit_days), 2) as avg_transit_days,
	round(avg(weight_kg), 2) as avg_kg,
	round(sum(cost) / NULLIF(sum(distance_miles),0),2) as cost_per_mile,
	round(sum(case when status = 'Delivered' then 1 else 0 end)::numeric / NULLIF(count(*),0) * 100,2) as otd_percentage,
	round(sum(case when status = 'Returned' then 1 else 0 end)::numeric / NULLIF(count(*),0) * 100,2) as returned_rate,
	round(sum(case when status = 'Lost'then 1 else 0 end)::numeric / NULLIF(count(*),0) * 100,2) as lost_rate,
	round(sum(case when status = 'Delay' then 1 else 0 end)::numeric / NULLIF(count(*),0) * 100,2) as delay_rate
FROM log_data
),
wh as (
	SELECT
    	origin_warehouse,
    	COUNT(*) AS shipment_count,
    	ROUND(AVG(transit_days), 2) AS avg_transit_days,
    	ROUND(SUM(cost) / NULLIF(SUM(weight_kg),0), 2) AS cost_per_kg,
    	ROUND(SUM(cost) / NULLIF(COUNT(*),0), 2) AS cost_per_unit,
    	ROUND(AVG(weight_kg), 2) AS avg_kg,
    	ROUND(SUM(cost) / NULLIF(SUM(distance_miles),0), 2) AS cost_per_mile,
    	ROUND(SUM(CASE WHEN status='Delivered' THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*),0) * 100, 2) AS otd_percentage,
    	ROUND(SUM(CASE WHEN status='Delayed' THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*),0) * 100, 2) AS delay_rate,
    	ROUND(SUM(CASE WHEN status='Lost' THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*),0) * 100, 2) AS lost_rate,
    	ROUND(SUM(CASE WHEN status='Returned' THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*),0) * 100, 2) AS returned_rate
  FROM log_data
  GROUP BY origin_warehouse
),
scorecard as (
	/* ----deltas---*/
	select
		w.*,
		ROUND(w.otd_percentage - o.otd_percentage,2) as d_otd_pp,
		ROUND(w.delay_rate - o.delay_rate,2) as d_delay_pp,
		ROUND(w.lost_rate - o.lost_rate,2) as d_lost_rate,
		ROUND(w.returned_rate - o.returned_rate,2) as d_returned_rate,
		ROUND(w.avg_transit_days - o.avg_transit_days,2) as d_avg_transit_pp,
		round((w.cost_per_kg - o.cost_per_kg) / NULLIF(o.cost_per_kg,0) * 100,2) as d_cost_per_kg_pct,
		round((w.cost_per_unit - o.cost_per_unit) / NULLIF(o.cost_per_unit,0)* 100,2) as d_cost_per_unit_pct,
		round((w.cost_per_mile - o.cost_per_mile) / NULLIF(o.cost_per_mile,0) * 100, 2) as d_cost_per_mile,

		/*--- flag and threshold------*/
		case when (w.otd_percentage - o.otd_percentage) <= -2.00 then 1 else 0 end as flag_otd,
		case when (w.delay_rate - o.delay_rate) >= 2.00 then 1 else 0 end as flag_delay,
		case when (w.returned_rate - o.returned_rate) >= 0.70 then 1 else 0 end as flag_return,
		case when (w.lost_rate - o.lost_rate) >= 0.70 then 1 else 0 end as flag_lost,
		case when ((w.cost_per_kg - o.cost_per_kg) / NULLIF(o.cost_per_kg,0)) >= 0.10 THEN 1 ELSE 0 END AS flag_cost_per_kg,
		case when ((w.cost_per_mile - o.cost_per_mile) / NULLIF(o.cost_per_mile,0)) >= 0.10 then 1 else 0 end as flag_cost_per_mile,
		case when ((w.cost_per_unit - o.cost_per_unit) / NULLIF(o.cost_per_unit,0)) >= 0.10 then 1 else 0 end as flag_cost_per_unit
	from wh w
	cross join overall o
)
SELECT
  origin_warehouse,
  shipment_count,

  /* deltas */
  d_otd_pp,
  d_delay_pp,
  d_return_pp,
  d_cost_unit_pct,
  d_cost_kg_pct,

  /* flag summary */
  (flag_otd + flag_delay + flag_return + flag_cost_unit) AS flag_count,
  shipment_count * (flag_otd + flag_delay + flag_return + flag_lost + flag_cost_unit +flag_cost_per_kg + flag_cost_per_unit) AS priority_score

FROM scorecard
ORDER BY priority_score DESC, shipment_count DESC;

/* HOU (848) — cost/unit +13%, cost/kg +26.6%, return +1.23pp, OTD -2.68pp

SF (645) — OTD -3.80pp, return +1.66pp, cost/kg +11.4%

MIA (603) — OTD -4.29pp, cost/kg +19.2% (return ok)

Keyingilar:

LA (440) — asosan cost/unit +12% (service ok)

ATL (414) — asosan OTD past

SEA (380) — return +1.03pp va cost/kg +24.6% (lekin OTD yaxshi)

Demak 1-navbat: HOU, SF, MIA.
Agar vaqt bo‘lsa 2-navbat: LA, ATL, SEA.
*/

-- B) Carrier bo‘yicha
SELECT
  origin_warehouse,
  carrier,
  COUNT(*) AS shipments,
  round(avg(transit_days),2) avg_transit_days,
  ROUND(SUM(cost)/NULLIF(COUNT(*),0),2) AS cost_per_unit,
  ROUND(SUM(cost)/NULLIF(SUM(weight_kg),0),2) AS cost_per_kg,
  ROUND(SUM(CASE WHEN status='Delayed' THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100,2) AS delay_rate,
  ROUND(SUM(CASE WHEN status='Returned' THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100,2) AS returned_rate,
  ROUND(SUM(CASE WHEN status='Delivered' THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100,2) AS delivered_rate
FROM log_data
WHERE origin_warehouse IN ('Warehouse_HOU','Warehouse_SF','Warehouse_MIA')
GROUP BY 1,2
HAVING COUNT(*) >= 20
ORDER BY origin_warehouse, shipments DESC;

-- HOU boyicha benchmark
with hou_bench as (
SELECT
  origin_warehouse,
  COUNT(*) AS shipments,
  round(avg(transit_days),2) avg_transit_days,
  ROUND(SUM(cost)/NULLIF(COUNT(*),0),2) AS cost_per_unit,
  ROUND(SUM(cost)/NULLIF(SUM(weight_kg),0),2) AS cost_per_kg,
  ROUND(SUM(CASE WHEN status='Delayed' THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100,2) AS delay_rate,
  ROUND(SUM(CASE WHEN status='Returned' THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100,2) AS returned_rate,
  ROUND(SUM(CASE WHEN status='Delivered' THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100,2) AS delivered_rate
FROM log_data
WHERE origin_warehouse IN ('Warehouse_HOU')
GROUP BY 1
ORDER BY shipments DESC
),
carrier_stats as (
SELECT
  origin_warehouse,
  carrier,
  COUNT(*) AS shipments,
  round(avg(transit_days),2) avg_transit_days,
  ROUND(SUM(cost)/NULLIF(COUNT(*),0),2) AS cost_per_unit,
  ROUND(SUM(cost)/NULLIF(SUM(weight_kg),0),2) AS cost_per_kg,
  ROUND(SUM(CASE WHEN status='Delayed' THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100,2) AS delay_rate,
  ROUND(SUM(CASE WHEN status='Returned' THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100,2) AS returned_rate,
  ROUND(SUM(CASE WHEN status='Delivered' THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100,2) AS delivered_rate
FROM log_data
WHERE origin_warehouse IN ('Warehouse_HOU')
GROUP BY 1,2
ORDER BY shipments DESC
),
scorecard as (
  select 
    h.*,

/* Rate deltas in percentage point */
    round(c.delay_rate - h.delay_rate,2) as d_delay_pp,
    round(c.returned_rate - h.returned_rate,2) as d_return_pp,
    round(c.delivered_rate - h.delivered_rate,2) as d_delivered_rate_pp,

/* time deltas*/
    round(c.avg_transit_days - h.avg_transit_days,2) as d_avg_transit_days,

-- COST deltas in relative %
    round((c.cost_per_unit - h.cost_per_unit) / NULLIF(h.cost_per_unit,0) * 100,2) as d_cost_per_unit_pct,
    round((c.cost_per_kg - h.cost_per_kg) / NULLIF(h.cost_per_kg,0) * 100,2) as d_cost_per_kg_pct,

-- flags
    case when(c.delay_rate - h.delay_rate) >= 2.00 then 1 else 0 end as flag_delay,
    case when(c.returned_rate - h.returned_rate) >= 0.70 then 1 else 0 end as flag_return,
    case when(c.delivered_rate - h.delivered_rate)  <= -2.00 then 1 else 0 end as flag_delivered,

    case when ((c.cost_per_kg - h.cost_per_kg) / NULLIF(h.cost_per_kg,0)) >= 0.10 then 1 else 0 end as flag_cost_per_kg,
    case when((c.cost_per_unit - h.cost_per_unit) / NULLIF(h.cost_per_unit,0)) >= 0.10 then 1 else 0 end as flag_cost_per_unit
from hou_bench h
cross join carrier_stats c
)
select 
  carrier,
  shipments,

  -- deltas
  d_delay_pp,
  d_return_pp,
  d_delivered_rate_pp,
  d_cost_per_kg_pct,
  d_cost_per_unit_pct,
  d_avg_transit_days,
  
  -- flag summary
  (flag_cost_per_kg + flag_cost_per_unit + flag_delay + flag_return + flag_delivered) as flag_count,
  shipments * (flag_cost_per_kg + flag_cost_per_unit + flag_return + flag_delivered) as priority_score
from scorecard
ORDER BY priority_score DESC, shipments DESC;


-- Qaysi carrier delay ko‘p qilmoqda?

-- Qaysi carrier narx/tezlik balansi yaxshi?
WITH carrier_kpi AS (
  SELECT
    carrier,
    COUNT(*) shipments,
    ROUND(SUM(cost)/NULLIF(COUNT(*),0),2) AS cost_per_unit,
    ROUND(AVG(transit_days),2) AS avg_transit_days,
    ROUND(SUM(CASE WHEN status='Delayed' THEN 1 ELSE 0 END)::numeric/COUNT(*)*100,2) AS delay_rate,
    ROUND(SUM(CASE WHEN status='Returned' THEN 1 ELSE 0 END)::numeric/COUNT(*)*100,2) AS returned_rate
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


-- C) Route (origin → destination)
SELECT
  destination,
  COUNT(*) shipments,
  ROUND(AVG(weight_kg),2) avg_kg,
  ROUND(AVG(distance_miles),2) avg_miles,
  ROUND(SUM(cost)/COUNT(*),2) cost_per_unit
FROM log_data
WHERE origin_warehouse='Warehouse_HOU'
  AND carrier='UPS'
GROUP BY 1
HAVING COUNT(*) >= 5
ORDER BY cost_per_unit DESC;

amazon
SELECT
  destination,
  COUNT(*) shipments,
  ROUND(AVG(transit_days),2) avg_transit_days,
  ROUND(SUM(CASE WHEN status='Delayed' THEN 1 ELSE 0 END)::numeric/COUNT(*)*100,2) delay_rate
FROM log_data
WHERE origin_warehouse='Warehouse_HOU'
  AND carrier='Amazon Logistics'
GROUP BY 1
HAVING COUNT(*) >= 10
ORDER BY delay_rate DESC, shipments DESC;


SELECT
  origin_warehouse,
  destination,
  COUNT(*) AS shipments,
  ROUND(SUM(cost)/COUNT(*),2) AS cost_per_unit,
  ROUND(SUM(cost)/NULLIF(SUM(weight_kg),0),2) AS cost_per_kg,
  ROUND(AVG(weight_kg),2) AS avg_kg,
  ROUND(AVG(transit_days),2) AS avg_transit_days,
  ROUND(SUM(CASE WHEN status='Delayed' THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100,2) AS delay_rate,
  ROUND(SUM(CASE WHEN status='Returned' THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100,2) AS returned_rate
FROM log_data
WHERE origin_warehouse IN ('Warehouse_HOU','Warehouse_SF','Warehouse_MIA')
GROUP BY 1,2
HAVING COUNT(*) >= 15
ORDER BY origin_warehouse, shipments DESC;

-- Qaysi yo‘nalishlar sistematik muammo?
-- Route bo‘yicha OTD% (faqat shipment_count threshold bilan)
SELECT 
	origin_warehouse,
	destination,
	COUNT(*) AS shipment_count,
	ROUND(SUM(CASE WHEN status='Delivered' THEN 1 ELSE 0 END) / CAST(COUNT(*) AS NUMERIC) * 100, 2) AS otd_percentage,
	ROUND(AVG(transit_days), 2) AS avg_transit_days
FROM log_data
GROUP BY origin_warehouse, destination
HAVING COUNT(*) >= 20 -- shipment_count threshold (masalan, 30 dan kam bo‘lmasin)
ORDER BY otd_percentage DESC;

WITH route_kpi AS (
  SELECT
    origin_warehouse,
    destination,
    COUNT(*) shipments,
    ROUND(SUM(cost)/NULLIF(COUNT(*),0),2) AS cost_per_unit,
    ROUND(AVG(transit_days),2) AS avg_transit_days,
    ROUND(SUM(CASE WHEN status='Delayed' THEN 1 ELSE 0 END)::numeric/COUNT(*)*100,2) AS delay_rate,
    ROUND(SUM(CASE WHEN status='Returned' THEN 1 ELSE 0 END)::numeric/COUNT(*)*100,2) AS returned_rate,
    ROUND(SUM(CASE WHEN status='Lost' THEN 1 ELSE 0 END)::numeric/COUNT(*)*100,2) AS lost_rate
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
  -- simple “badness” score (tez hisoblash uchun)
  ROUND(
    (r.cost_per_unit / NULLIF(b.avg_cpu,0)) +
    (r.delay_rate / NULLIF(b.avg_delay,0)) +
    (r.returned_rate / NULLIF(b.avg_return,0)),
  2) AS badness_score
FROM route_kpi r
CROSS JOIN bench b
ORDER BY badness_score DESC, shipments DESC;


-- 5. CORRELATION THINKING
-- Og'ir yuklar kechikadimi?
SELECT
  CASE 
    WHEN weight_kg < 10 THEN 'Light (< 10kg)'
    WHEN weight_kg < 50 THEN 'Medium (10-50kg)'
    WHEN weight_kg < 100 THEN 'Heavy (50-100kg)'
    ELSE 'Very Heavy (100kg+)'
  END AS weight_category,
  COUNT(*) AS shipment_count,
  ROUND(AVG(transit_days), 2) AS avg_transit_days,
  ROUND(SUM(CASE WHEN status='Delayed' THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2) AS delay_rate,
  ROUND(SUM(CASE WHEN status='Delivered' THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2) AS delivered_rate
FROM log_data
GROUP BY weight_category
ORDER BY avg_transit_days DESC;

-- Arzon yuklar ko'proq delay qilinadimi?
SELECT
  CASE 
    WHEN cost < 50 THEN 'Budget (< $50)'
    WHEN cost < 100 THEN 'Standard ($50-100)'
    WHEN cost < 200 THEN 'Premium ($100-200)'
    ELSE 'Expensive (200+)'
  END AS cost_category,
  COUNT(*) AS shipment_count,
  ROUND(AVG(transit_days), 2) AS avg_transit_days,
  ROUND(SUM(CASE WHEN status='Delayed' THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2) AS delay_rate,
  ROUND(SUM(CASE WHEN status='Delivered' THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2) AS delivered_rate,
  ROUND(SUM(CASE WHEN status='Lost' THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2) AS lost_rate
FROM log_data
GROUP BY cost_category
ORDER BY delay_rate DESC;

-- Masofa oshgani sari cost qanday o'zgaradi?
SELECT
  CASE 
    WHEN distance_miles < 100 THEN 'Short (< 100mi)'
    WHEN distance_miles < 500 THEN 'Medium (100-500mi)'
    WHEN distance_miles < 1000 THEN 'Long (500-1000mi)'
    ELSE 'Very Long (1000mi+)'
  END AS distance_category,
  COUNT(*) AS shipment_count,
  ROUND(AVG(cost), 2) AS avg_cost,
  ROUND(AVG(transit_days), 2) AS avg_transit_days,
  ROUND(SUM(cost)/NULLIF(SUM(distance_miles),0), 2) AS cost_per_mile,
  ROUND(SUM(CASE WHEN status='Delivered' THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2) AS delivered_rate
FROM log_data
GROUP BY distance_category
ORDER BY shipment_count DESC;

-- TIME-BASED INSIGHT
-- Qaysi warehouse’da kechikish o‘sib bormoqda?
SELECT
  date_trunc('month', shipment_date)::date AS month,
  origin_warehouse,
  COUNT(*) AS shipment_count,
  ROUND(SUM(CASE WHEN status='Delayed' THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2) AS delay_rate,
  ROUND(SUM(CASE WHEN status='Delivered' THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2) AS delivered_rate,
  ROUND(AVG(transit_days), 2) AS avg_transit_days
FROM log_data
WHERE origin_warehouse IN ('Warehouse_HOU','Warehouse_SF','Warehouse_MIA')
GROUP BY month, origin_warehouse
ORDER BY origin_warehouse, month;

-- Qaysi carrier vaqt o‘tishi bilan yomonlashgan?
SELECT
  date_trunc('month', shipment_date)::date AS month,
  carrier,
  COUNT(*) AS shipment_count,
  ROUND(AVG(transit_days), 2) AS avg_transit_days,
  ROUND(SUM(CASE WHEN status='Delivered' THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2) AS delivered_rate,
  ROUND(SUM(CASE WHEN status='Delayed' THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2) AS delay_rate
FROM log_data
WHERE carrier IS NOT NULL
GROUP BY month, carrier
HAVING COUNT(*) >= 5
ORDER BY carrier, month;

-- ANOMALY THINKING & RECOMMENDATIONS
-- Bu outlierlar qayerdan kelmoqda?
-- Qaysi warehouse / carrier doim outlier chiqaradi?
SELECT
  origin_warehouse,
  carrier,
  COUNT(*) AS shipment_count,
  ROUND(AVG(cost), 2) AS avg_cost,
  ROUND(AVG(transit_days), 2) AS avg_transit_days,
  STDDEV(cost) AS cost_stddev,
  STDDEV(transit_days) AS transit_stddev,
  ROUND(SUM(CASE WHEN status IN ('Lost','Returned') THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2) AS problem_rate
FROM log_data
WHERE carrier IS NOT NULL
GROUP BY origin_warehouse, carrier
HAVING COUNT(*) >= 10
ORDER BY problem_rate DESC, cost_stddev DESC;

-- FINAL DECISION & RECOMMENDATION
-- Comprehensive scorecard for action items
WITH warehouse_scores AS (
  SELECT
    origin_warehouse,
    COUNT(*) AS total_shipments,
    ROUND(AVG(cost), 2) AS avg_cost,
    ROUND(SUM(cost)/NULLIF(SUM(weight_kg),0), 2) AS cost_per_kg,
    ROUND(SUM(CASE WHEN status='Delivered' THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2) AS otd_pct,
    ROUND(SUM(CASE WHEN status IN ('Lost','Returned') THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2) AS problem_rate,
    ROUND(AVG(transit_days), 2) AS avg_transit_days,
    CASE 
      WHEN ROUND(SUM(CASE WHEN status='Delivered' THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2) < 85 THEN '🔴 CRITICAL'
      WHEN ROUND(SUM(CASE WHEN status IN ('Lost','Returned') THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2) > 2 THEN '🟠 HIGH RISK'
      WHEN ROUND(SUM(cost)/NULLIF(SUM(weight_kg),0), 2) > 10 THEN '🟡 EXPENSIVE'
      ELSE '🟢 GOOD'
    END AS status_flag
  FROM log_data
  GROUP BY origin_warehouse
)
SELECT
  origin_warehouse,
  total_shipments,
  avg_cost,
  cost_per_kg,
  otd_pct,
  problem_rate,
  avg_transit_days,
  status_flag,
  CASE 
    WHEN status_flag = '🔴 CRITICAL' THEN 'URGENT: Optimize performance + reduce cost'
    WHEN status_flag = '🟠 HIGH RISK' THEN 'PRIORITY: Improve quality control'
    WHEN status_flag = '🟡 EXPENSIVE' THEN 'FOCUS: Negotiate better rates'
    ELSE 'MAINTAIN: Current performance is good'
  END AS recommendation
FROM warehouse_scores
ORDER BY 
  CASE WHEN status_flag = '🔴 CRITICAL' THEN 1 WHEN status_flag = '🟠 HIGH RISK' THEN 2 WHEN status_flag = '🟡 EXPENSIVE' THEN 3 ELSE 4 END,
  problem_rate DESC,
  cost_per_kg DESC;
