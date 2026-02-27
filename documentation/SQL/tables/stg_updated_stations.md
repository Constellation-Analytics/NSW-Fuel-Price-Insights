# Table Specification: public.stg_updated_stations

## 1. Overview

### Purpose
Temporary staging table storing existing service stations whose attributes have changed in the source system.

Used to apply updates to the station dimension table while maintaining controlled warehouse logic.

### Grain
One row per updated service station.

### Primary Key
- `stationid`

### Source
- External fuel station API
- Loaded via ingestion module after change detection logic

---

## 2. Columns

| Column Name | Data Type | Nullable | Default | Description | Example |
|-------------|------------|----------|---------|------------|---------|
| stationid | VARCHAR | No | – | Unique station identifier from source | 12345 |
| brand | VARCHAR(50) | Yes | – | Station brand | Ampol |
| name | VARCHAR(100) | Yes | – | Station trading name | Ampol West End |
| address | VARCHAR(255) | Yes | – | Full formatted address | 789 River Rd, West End |
| street | VARCHAR(100) | Yes | – | Street address component | 789 River Rd |
| town | VARCHAR(50) | Yes | – | Town or suburb | West End |
| postcode | CHAR(4) | Yes | – | Australian postcode | 4101 |
| latitude | NUMERIC(9,6) | Yes | – | Geographic latitude | -27.480321 |
| longitude | NUMERIC(9,6) | Yes | – | Geographic longitude | 153.012456 |
| last_update | DATE | Yes | – | Date of last update from source | 2026-02-27 |

---

## 3. Constraints

### Primary Key
- `staging_updated_stations_pkey` → (`stationid`)

No foreign key constraints enforced.

---

## 4. Indexes

Primary key index on:
- `stationid`

No additional indexes required due to short-lived staging design.

---

## 5. Relationships

Logical flow:

- Loaded from external API
- Compared against `dim_station`
- Used to update existing dimension records
- Not queried directly by reporting layer

Acts as the controlled update feed for station dimension maintenance.

---

## 6. Data Lifecycle

### Insert Method
- Refreshed during each ingestion cycle after change detection

### Update Method
- Typically replaced in full rather than incrementally updated

### Delete Policy
- Truncated or replaced during next pipeline execution

Short-lived staging table by design.

---

## 7. Design Decisions

### Explicit Update Layer
Separating new, inactive, and updated stations simplifies merge logic and improves pipeline clarity.

### Nullable Attributes
Supports ingestion even if some metadata fields are missing or unchanged.

### Geographic Precision (`NUMERIC(9,6)`)
Provides adequate spatial precision for mapping and analytics use cases.

### No Foreign Keys
Maintains loose coupling and prevents ingestion failures due to referential timing issues.
