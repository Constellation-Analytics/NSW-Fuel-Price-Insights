# Table Specification: public.dim_fuel_stations

## 1. Overview

### Purpose
Stores all service station master data for reporting and analytics.  
Represents the authoritative source for station attributes, location, and status.

### Grain
One row per unique service station.

### Primary Key
- `stationid`

### Source
- Populated and maintained via staging tables:
  - `stg_new_stations`
  - `stg_updated_stations`
  - `stg_inactive_stations`
- ETL procedures handle inserts, updates, and deactivations

---

## 2. Columns

| Column Name | Data Type | Nullable | Default | Description | Example |
|-------------|------------|----------|---------|------------|---------|
| stationid | VARCHAR | No | – | Unique station identifier | 12345 |
| brand | VARCHAR(50) | No | – | Station brand | Shell |
| name | VARCHAR(100) | No | – | Trading name of the station | Shell Geelong |
| address | VARCHAR(255) | No | – | Full formatted address | 123 Main St, Geelong |
| street | VARCHAR(100) | No | – | Street component of address | 123 Main St |
| town | VARCHAR(50) | No | – | Town or suburb | Geelong |
| postcode | CHAR(4) | No | – | Australian postcode | 3220 |
| lga | VARCHAR(50) | Yes | – | Local Government Area | Greater Geelong |
| latitude | NUMERIC(9,6) | No | – | Latitude coordinate | -38.149918 |
| longitude | NUMERIC(9,6) | No | – | Longitude coordinate | 144.361719 |
| active | BOOLEAN | No | – | Indicates if the station is currently active | TRUE |
| last_update | DATE | No | – | Last update timestamp from source | 2026-02-27 |
| deletion_flag | INTEGER | Yes | – | Optional flag to mark soft-deleted stations | 1 |

---

## 3. Constraints

### Primary Key
- `fuel_station_dict_pkey` → (`stationid`)

No foreign key constraints enforced, although `stationid` logically joins to fact tables.

---

## 4. Indexes

Primary key index on:
- `stationid`

Additional indexes can be added for:
- `active` (to filter active stations quickly)
- `town` or `lga` for reporting/filtering

---

## 5. Relationships

Logical relationships:

- `stationid` referenced by fact tables (e.g., `fact_fuel_prices`)
- Receives inserts/updates from staging tables
- Used in reporting dashboards for mapping, analytics, and aggregation

---

## 6. Data Lifecycle

### Insert Method
- New stations inserted from `stg_new_stations`

### Update Method
- Existing stations updated from `stg_updated_stations` or staging feeds

### Deactivation Method
- Stations flagged inactive via `stg_inactive_stations` or `deletion_flag`

### Delete Policy
- Soft delete using `active` flag and `deletion_flag`
- Historical records retained for auditing

---

## 7. Design Decisions

### Active / Deletion Flags
Supports slowly changing dimension type 1/2 logic, allowing soft deletes without losing historical references.

### Geographic Precision (`NUMERIC(9,6)`)
Sufficient for mapping and distance calculations.

### No Foreign Keys
Decouples dimension maintenance from pipeline constraints and allows faster loads.

### Not Nullable Core Columns
Ensures all essential station attributes are always present for reporting and joins.
