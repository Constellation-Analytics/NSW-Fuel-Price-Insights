# Table Specification: public.stg_fuel_price

## 1. Overview

### Purpose
Temporary staging table used to store raw fuel price data before transformation and loading into the fact table.

This table represents the raw ingestion layer of the pipeline.

### Grain
One row per service station per fuel type per date (as received from the source system).

### Primary Key
- `record_id`

### Source
- Directly populated from external fuel pricing API
- Loaded via Python ingestion module

---

## 2. Columns

| Column Name | Data Type | Nullable | Default | Description | Example |
|-------------|------------|----------|---------|------------|---------|
| record_id | VARCHAR | No | – | Unique identifier from source system | 12345_E10_2026-02-27 |
| servicestationname | VARCHAR | No | – | Raw service station name | Shell Geelong |
| address | VARCHAR | No | – | Raw service station address | 123 Main St |
| fuelcode | VARCHAR | No | – | Fuel type code | E10 |
| date | DATE | No | – | Date the price applies to | 2026-02-27 |
| price | DOUBLE PRECISION | No | – | Raw fuel price value | 189.9 |
| priceupdateddate | DATE | Yes | – | Date the price was last updated | 2026-02-27 |

---

## 3. Constraints

### Primary Key
- `fuelprice_staging_pkey` → (`record_id`)

No foreign keys enforced.

---

## 4. Indexes

Primary key index on:
- `record_id`

No additional indexes required due to staging nature and short lifecycle.

---

## 5. Relationships

Logical flow:

- Loaded from external API
- Validated via `check_data_quality()`
- Transformed and inserted into `fact_fuel_prices`

This table should not be queried directly by reporting layers.

---

## 6. Data Lifecycle

### Insert Method
- Truncated or refreshed during each pipeline run
- Loaded via ingestion module

### Update Method
- Typically not updated (raw landing table)

### Delete Policy
- Truncated after successful processing
  or
- Fully replaced during next ingestion cycle

Short-lived by design.

---

## 7. Design Decisions

### Staging Layer Separation
Isolates raw data from validated warehouse data.
Prevents corrupted source data from impacting fact tables directly.

### DOUBLE PRECISION for Price
Allows raw numeric flexibility before final casting to `NUMERIC(5,1)` in fact table.

### No Foreign Keys
Keeps ingestion resilient and decoupled from warehouse constraints.

### Minimal Constraints
Staging tables prioritize load speed and flexibility over strict enforcement.
