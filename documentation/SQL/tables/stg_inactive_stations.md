# Table Specification: public.stg_inactive_stations

## 1. Overview

### Purpose
Temporary staging table storing service stations identified as inactive from the source system.

Used to update station status within the warehouse and ensure reporting accuracy.

### Grain
One row per inactive service station.

### Primary Key
- `stationid`

### Source
- External fuel station API
- Loaded via ingestion module

---

## 2. Columns

| Column Name | Data Type | Nullable | Default | Description | Example |
|-------------|------------|----------|---------|------------|---------|
| stationid | VARCHAR | No | – | Unique station identifier from source | 12345 |
| brand | VARCHAR(50) | Yes | – | Station brand | Shell |
| name | VARCHAR(100) | Yes | – | Station trading name | Shell Geelong |
| address | VARCHAR(255) | Yes | – | Full formatted address | 123 Main St, Geelong |
| street | VARCHAR(100) | Yes | – | Street address component | 123 Main St |
| town | VARCHAR(50) | Yes | – | Town or suburb | Geelong |
| postcode | CHAR(4) | Yes | – | Australian postcode | 3220 |
| latitude | NUMERIC(9,6) | Yes | – | Geographic latitude | -38.149918 |
| longitude | NUMERIC(9,6) | Yes | – | Geographic longitude | 144.361719 |
| last_update | DATE | Yes | – | Date of last update from source | 2026-02-27 |

---

## 3. Constraints

### Primary Key
- `staging_inactive_stations_pkey` → (`stationid`)

No foreign key constraints enforced.

---

## 4. Indexes

Primary key index on:
- `stationid`

No additional indexes required due to staging purpose and short lifecycle.

---

## 5. Relationships

Logical flow:

- Loaded from external API
- Used to update station status in dimension table (e.g., `dim_station`)
- Not queried directly by reporting layer

Acts as a control input for station lifecycle management.

---

## 6. Data Lifecycle

### Insert Method
- Refreshed during each ingestion cycle

### Update Method
- Typically replaced in full rather than incrementally updated

### Delete Policy
- Truncated or replaced during next pipeline execution

Short-lived staging table by design.

---

## 7. Design Decisions

### Separate Inactive Feed
Maintains clean separation between active station ingestion and deactivation logic.

### Flexible Nullable Columns
Allows ingestion even if partial station metadata is missing.

### Geographic Precision (`NUMERIC(9,6)`)
Supports ~10cm spatial precision, sufficient for mapping and spatial analysis.

### No Foreign Keys
Prevents ingestion failures and keeps staging decoupled from warehouse constraints.
