/* =========================================================
Logistics KPI Project (PostgreSQL)

BUSINESS QUESTIONS (final outputs)
1) Qaysi warehouse eng muammoli? (cost + service + quality)
2) Qaysi carrier tez, lekin qimmat? (2x2 positioning)
3) Qaysi route (origin→destination) eng foydasiz? (badness score)

NOTES
- status qiymatlari: Delivered / Delayed / Lost / Returned (bir xil ishlatildi)
- "OTD" yo‘q (promise/SLA yo‘q), shuning uchun delivered_rate ishlatiladi
========================================================= */


/* =========================
0) LOAD + CLEAN (optional)
Agar sizda log_data allaqachon bo‘lsa, bu blokni SKIP qiling.
========================= */

-- STAGING (hamma TEXT)
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

-- Import (psql)
-- \copy stg_shipments FROM 'logistics_shipments_dataset.csv' WITH (FORMAT csv, HEADER true);

-- CLEAN TABLE
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
  CASE WHEN NULLIF(TRIM(delivery_date), '') IS NULL THEN NULL
       ELSE NULLIF(TRIM(delivery_date), '')::DATE
  END                                                                                 AS delivery_date,
  NULLIF(REPLACE(TRIM(weight_kg), ',', ''), '')::NUMERIC                              AS weight_kg,
  NULLIF(REPLACE(TRIM(cost), ',', ''), '')::NUMERIC                                   AS cost,
  COALESCE(NULLIF(TRIM(status), ''), 'UNKNOWN')                                       AS status,
  NULLIF(REPLACE(TRIM(distance_miles), ',', ''), '')::NUMERIC                         AS distance_miles,
  NULLIF(TRIM(transit_days), '')::INT                                                 AS transit_days
FROM stg_shipments;

-- Recompute transit_days if delivery_date exists
UPDATE log_data
SET transit_days = (delivery_date - shipment_date)
WHERE delivery_date IS NOT NULL;

-- Fill NULL cost with avg(cost)
UPDATE log_data
SET cost = sub.avg_cost
FROM (SELECT AVG(cost) AS avg_cost FROM log_data WHERE cost IS NOT NULL) sub
WHERE log_data.cost IS NULL;

-- Indexes
CREATE INDEX IF NOT EXISTS idx_log_data_shipment_date ON log_data (shipment_date);
CREATE INDEX IF NOT EXISTS idx_log_data_origin        ON log_data (origin_warehouse);
CREATE INDEX IF NOT EXISTS idx_log_data_carrier       ON log_data (carrier);
CREATE INDEX IF NOT EXISTS idx_log_data_status        ON log_data (status);
