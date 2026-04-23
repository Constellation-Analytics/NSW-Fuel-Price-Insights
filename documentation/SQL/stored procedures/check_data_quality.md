# Stored Procedure Spec: `check_data_quality()`

## 1. Procedure Overview
- **Name:** `check_data_quality()`
- **Type:** PostgreSQL Stored Procedure (`plpgsql`)
- **Purpose:**
  Executes data quality checks across staging and dimension tables, records active defects in `dq_issues`, logs results to `sys_run_log`, and removes resolved defects.
- **Owner:** `neondb_owner`


## 2. Upstream Dependencies
- Python module (`4.data_quality.py`)
- Tables:
  - `stg_fuel_price`
  - `dim_fuel_stations`
  - `stg_new_stations`
  - `stg_updated_stations`
  - `stg_inactive_stations`


## 3. Downstream Dependencies
- `dq_issues`
- `sys_run_log`


## 4. Inputs / Sources

- No parameters
- Reads from:
  - `stg_fuel_price`
  - `dim_fuel_stations`
  - `stg_new_stations`
  - `stg_updated_stations`
  - `stg_inactive_stations`


## 5. Outputs

### Primary Output
- Inserts / updates active defects in:
  - `dq_issues`


### Logging Output
- Inserts records into:
  - `sys_run_log`
    - `procedure_name`
    - `description` (defect ID)
    - `rows_affected`


### Cleanup Output
- Deletes resolved defects:
  - `dq_issues WHERE is_active = false`


## 6. High-Level Logic / Execution Flow

### Step 1 – Reset State
1. Set all existing defects to `is_active = false`
2. Delete previous logs for this procedure from `sys_run_log`


### Step 2 – Execute Data Quality Checks

For each check:
1. Insert detected issues into `dq_issues`
2. Use `ON CONFLICT` to:
   - prevent duplicates
   - reactivate existing defects
3. Capture affected row count
4. If rows > 0:
   - insert summary record into `sys_run_log`


### Step 3 – Cleanup
- Remove defects not re-detected in this run:
  - `DELETE WHERE is_active = false`


## 7. Data Quality Checks Summary

| defect_id | defect_type        | description |
|-----------|------------------|-------------|
| MIS_01 | Missing Data | Fact records not found in dimension or staging tables |
| MIS_02 | Missing Data | Fact records not linked to an **active** dimension record and not in staging |
| INC_01 | Inconsistent Data | Station marked for update but fact still uses old name/address |
| INC_02 | Inconsistent Data | Fact contains both old and new station records |
| INC_03 | Inconsistent Data | Station appears in both new and inactive staging tables |
| INC_04 | Inconsistent Data | New station already exists as active in dimension |
| INC_05 | Inconsistent Data | Duplicate active stations in dimension (name + address) |
| PAR_01 | Parsing Issue | Missing `street` or `town` in `stg_new_stations` |
| PAR_02 | Parsing Issue | Missing `street` or `town` in `stg_updated_stations` |


## 8. Conflict Handling Strategy

Uses:
```sql
ON CONFLICT (dq_id, entity, key_var, key_var1, key_var2)
DO UPDATE SET is_active = TRUE;
```
Ensures:
- No duplicate defects
- Existing defects are reactivated
- Stable defect identity across runs


## 9. Logging Strategy

The procedure uses `GET DIAGNOSTICS` to capture how many rows were affected by each data quality check.

### How It Works
- After each `INSERT INTO dq_issues`, the system variable `ROW_COUNT` is captured:
  - Stored in `affected_rows`
- If `affected_rows > 0`, a log record is written to `sys_run_log`
- If no rows are affected, no log is created for that defect

### Logging Pattern
- Each check logs:
  - `procedure_name` → `check_data_quality`
  - `description` → defect ID (e.g. `MIS_01`)
  - `rows_affected` → number of impacted records


## 10. Idempotency & Safety

The procedure is idempotent because:
- All defects are reset to inactive at start
- Current defects are reactivated or inserted
- Resolved defects are deleted at end
- Conflict handling prevents duplication
- Logs are cleared and recreated per run

Repeated execution produces consistent results.
