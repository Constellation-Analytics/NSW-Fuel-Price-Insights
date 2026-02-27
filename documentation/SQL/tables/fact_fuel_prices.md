# Table Specification: public.fact_fuel_prices

## 1. Overview

### Purpose
Stores fuel price observations for service stations.

This is the core fact table used for reporting and price analysis.

### Grain
One row per service station per fuel type per date.

### Primary Key
- `record_id`

### Source
- Populated via ETL pipeline and `update_fact_fuel()` stored procedure
- Data originates from external fuel pricing API

---

## 2. Columns

| Column Name | Data Type | Nullable | Default | Description | Example |
|-------------|------------|----------|---------|------------|---------|
| record_id | VARCHAR(255) | No | – | Unique identifier for the price record | 12345_E10_2026-02-27 |
| stationid | VARCHAR(255) | No | – | Unique identifier of the service station | 12345 |
| fuelcode | VARCHAR(10) | No | – | Fuel type code | E10 |
| date | DATE | No | – | Date the price applies to | 2026-02-27 |
| price | NUMERIC(5,1) | No | – | Fuel price | 189.9 |
| priceupdateddate | DATE | Yes | – | Date the price was last updated | 2026-02-27 |

---

## 3. Constraints

### Primary Key
- `fuel_prices_pkey` → (`record_id`)

No foreign key constraints enforced (logical relationships only).

---

## 4. Indexes

Primary key index on:
- `record_id`

Additional indexes may be considered for:
- `stationid` (join performance)
- `date` (time-based filtering)
- (`stationid`, `fuelcode`, `date`) if frequent composite filtering occurs

---

## 5. Relationships

Logical relationships:

- `stationid` joins to `dim_station.stationid`
- Referenced by:
  - `check_data_quality()`
  - `update_fact_fuel()`
  - Reporting / analytics layer

This table represents measurable business events (fuel price observations).

---

## 6. Data Lifecycle

### Insert Method
- Inserted via ETL process
- Upsert logic applied to prevent duplicate records

### Update Method
- Existing records may be updated if price corrections are received

### Delete Policy
- Subject to retention policy (e.g., remove records older than 24 months)

---

## 7. Design Decisions

### Natural-style Primary Key (`record_id`)
Encodes business uniqueness into a single identifier.
Simplifies upsert logic and ensures idempotent loads.

### VARCHAR Usage
Allows flexibility in handling externally sourced station and fuel identifiers.

### Numeric Precision (`NUMERIC(5,1)`)
Supports prices up to 999.9 with one decimal precision.

### No Foreign Keys
Avoids ingestion failures due to referential timing issues.
Designed for pipeline resilience and performance.
