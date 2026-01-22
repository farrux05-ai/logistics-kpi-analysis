# Logistics KPI Analysis (SQL-first)

This project analyzes logistics shipments to answer:
1) Which warehouses are most problematic (cost + service + quality)?
2) Which carriers are fast-but-expensive vs cheap-but-slow?
3) Which routes (origin → destination) are the least efficient?

The analysis is **SQL-first** (PostgreSQL). Optional Python is used only for **simple visualization** from exported CSV outputs.

---

## Business Questions

### Q1 — Warehouse performance
- Which warehouse has the highest cost?
- Which warehouse has the worst service/quality (delays, returns, lost)?

### Q2 — Carrier tradeoff
- Which carrier is fast but expensive?
- Which carrier is cheap but causes delays/returns?

### Q3 — Route efficiency
- Which origin → destination lanes have the worst combination of cost + delay + returns?

---

## Dataset (Columns)
Expected fields:
- `shipment_id`, `origin_warehouse`, `destination`, `carrier`
- `shipment_date`, `delivery_date`
- `weight_kg`, `cost`, `distance_miles`, `transit_days`
- `status` (Delivered / Delayed / Lost / Returned)

> Note: If you don't have SLA/promise date, “OTD” is approximated via `delivered_rate` (share delivered).

---

## Method (Analytical Approach)
1) **EDA & sanity checks**
   - row counts, date range, missing values, outliers (IQR)
2) **Benchmark KPIs (overall)**
   - cost/unit, cost/kg, cost/mile, avg transit days
   - delivered_rate, delay_rate, lost_rate, returned_rate
3) **Warehouse scorecard (triage)**
   - warehouse KPI vs overall KPI
   - deltas + flags + `priority_score = shipment_count × flag_count`
4) **Drill-down**
   - for top warehouses: `warehouse × carrier` (who drives cost/delay/returns?)
   - then `carrier × destination` to isolate lanes
5) **Optional visualization**
   - export CSV from SQL, plot in Python (matplotlib)

---

## Key Findings (Example from my run)
### Warehouse priority (triage)
Top warehouses (priority_score):
- `Warehouse_HOU` — mixed issues: cost ↑, returns ↑, delivered_rate ↓  
- `Warehouse_SF` — service + quality issues  
- `Warehouse_MIA` — service issues, cost/kg often driven by shipment profile

### HOU carrier drivers
- **UPS** → major cost driver (cost per unit/kg much higher than HOU benchmark)
- **Amazon Logistics** → major delay driver
- **LaserShip / USPS** → major returns driver

> The main logic: **High rate alone is not enough** — we focus on segments with both **(bad KPI) + (enough volume)**.

---

## Recommendations (Action Plan)
1) **UPS cost (HOU)**
   - Check if cost is driven by mix (distance/zone/weight/service-level) vs contract rates
   - If mix: re-route certain lanes to alternative carrier
   - If rates: negotiate contract / accessorial fees

2) **Amazon Logistics delays (HOU)**
   - Identify destinations/routes where delay is concentrated (Pareto)
   - If concentrated: lane fix (re-route lanes)
   - If spread: carrier performance/SLA review

3) **LaserShip/USPS returns (HOU)**
   - If return_reason exists: break down by reason
   - If not: check destination/product mix patterns
   - If widespread: packaging/label/pick-pack process audit

---

## Repository Structure
logistics-kpi-analysis/
README.md
sql/
01_load_clean.sql
02_overall_kpi.sql
03_warehouse_scorecard.sql
04_carrier_drilldown.sql
05_route_analysis.sql
outputs/
warehouse_scorecard.csv
hou_carrier_scorecard.csv
worst_routes.csv
screenshots/
warehouse_priority.png
hou_carrier_drivers.png
notebooks/
visuals.ipynb
.gitignore
LICENSE

---

## How to Run (PostgreSQL)

### 1) Load CSV (psql)
- Use `\copy` into staging
- Insert into `log_data` with type casting
- Recompute `transit_days` from dates (if delivery_date is present)

### 2) Run SQL modules
In order:
1. `sql/01_load_clean.sql`
2. `sql/02_overall_kpi.sql`
3. `sql/03_warehouse_scorecard.sql`
4. `sql/04_carrier_drilldown.sql`
5. `sql/05_route_analysis.sql`

### 3) Export outputs as CSV
Example (psql):
```sql
\copy (SELECT * FROM your_warehouse_scorecard_query) TO 'outputs/warehouse_scorecard.csv' CSV HEADER;
\copy (SELECT * FROM your_hou_carrier_scorecard_query) TO 'outputs/hou_carrier_scorecard.csv' CSV HEADER;
\copy (SELECT * FROM your_worst_routes_query) TO 'outputs/worst_routes.csv' CSV HEADER;

### Notes / Assumptions

If SLA/promise date is not available, delivered_rate is used as a proxy metric.

Use minimum volume thresholds (e.g. HAVING COUNT(*) >= 20) to reduce noise

