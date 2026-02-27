# Table Specification: public.stg_new_stations

## 1. Overview

### Purpose
Temporary staging table storing newly identified service stations from the source system.

Used to insert new records into the station dimension table and maintain warehouse completeness.

### Grain
One row per new service station.

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
| brand | VARCHAR(50) | Yes | – | Station brand | BP |
| name | VARCHAR(100) | Yes | – | Station trading name | BP Southbank |
| address | VARCHAR(255) | Yes | – | Full formatted address | 456 City Rd, Southbank |
| street | VARCHAR(100) | Yes | – | Street address component | 456 City Rd |
| town | VARCHAR(50) | Yes | – | Town or suburb | Southbank |
| postcode | CHAR(4) | Yes | – | Australian postcode | 3006 |
| latitude | NUMERIC(9,6) | Yes | – | Geographic latitude | -37.823421 |
| longitude | NUMERIC(9,6) | Yes | – | Geographic longitude | 144.965123 |
| last_update | DATE | Yes | – | Date of last update from source | 2026-02-27 |

---

## 3. Constraints

### Primary Key
- `staging_new_stations_pkey` → (`stationid`)

No foreign key constraints enforced.

---

## 4. Indexes

Primary key index on:
- `stationid`

No additional indexes required due to staging nature and short lifecycle.

---

## 5. Relationships

Logical flow:

- Loaded from external API
- Used to insert new records into `dim_station`
- Not accessed directly by reporting layer

Acts as an onboarding feed for new dimension members.

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

### Separation of New vs Inactive Stations
Allows explicit lifecycle control:
- `stg_new_stations` → inserts
- `stg_inactive_stations` → deactivations

Improves clarity and reduces merge complexity.

### Flexible Nullable Columns
Ensures ingestion resilience when source metadata is incomplete.

### Geographic Precision (`NUMERIC(9,6)`)
Provides sufficient spatial precision for mapping and geospatial analysis.

### No Foreign Keys
Prevents ingestion failures and maintains loose coupling with warehouse tables.
