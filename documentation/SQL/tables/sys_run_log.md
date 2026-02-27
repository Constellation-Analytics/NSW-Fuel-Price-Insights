# Table Specification: public.sys_run_log

## 1. Overview

### Purpose
Stores execution logs for stored procedures and ETL modules.  
Used for monitoring, auditing, and debugging pipeline runs.

### Grain
One row per procedure execution.

### Primary Key
- `log_id`

### Source
- Populated by stored procedures (`check_data_quality()`, `update_fact_fuel()`, etc.)
- Populated by ETL modules at the end of each run

---

## 2. Columns

| Column Name | Data Type | Nullable | Default | Description | Example |
|-------------|------------|----------|---------|------------|---------|
| log_id | INTEGER | No | nextval() | Surrogate key for each log entry | 101 |
| procedure_name | VARCHAR(100) | No | – | Name of the procedure or module executed | check_data_quality |
| description | VARCHAR(100) | Yes | – | Short description of the run | Daily DQ check |
| rows_affected | INTEGER | Yes | 0 | Number of rows updated/inserted/deleted | 42 |
| run_date | TIMESTAMP | Yes | CURRENT_TIMESTAMP | Timestamp of execution | 2026-02-27 10:30:00 |

---

## 3. Constraints

### Primary Key
- `sys_run_log_pkey` → (`log_id`)

No foreign key constraints enforced.

---

## 4. Indexes

Primary key index on:
- `log_id`

Additional indexes could be added if querying frequently by `procedure_name` or `run_date`.

---

## 5. Relationships

Logical flow:

- Populated by stored procedures and ETL modules at end of execution
- Provides a history of pipeline runs for auditing and debugging
- May be referenced by monitoring dashboards

---

## 6. Data Lifecycle

### Insert Method
- Inserted automatically at the end of procedure or module execution

### Update Method
- Typically not updated after insertion

### Delete Policy
- Can be truncated or archived based on retention policy (e.g., logs older than 90 days)

---

## 7. Design Decisions

### Surrogate Key (`log_id`)
Simplifies referencing and ensures uniqueness per log entry.

### Default `rows_affected = 0`
Allows explicit logging even if no rows were affected.

### No Foreign Keys
Prevents execution failures due to table dependencies.

### Lightweight Logging
Optimized for quick inserts and minimal impact on production processes.
