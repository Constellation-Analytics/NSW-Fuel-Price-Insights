# Stored Procedure Spec: `update_data_quality_autofix()`

## 1. Procedure Overview
- **Name:** `update_data_quality_autofix()`  
- **Type:** PostgreSQL Stored Procedure (`plpgsql`)  
- **Purpose:**  
  Applies automated remediation actions for selected data quality defects identified in `dq_issues`.  
  Updates or removes problematic records from staging and dimension tables and logs all autofix actions to `sys_run_log`.  
- **Owner:** `neondb_owner`  

## 2. Upstream Dependencies
- Orchestrator Python module (`4.data_quality.py`)
- Stored Procedure:
  - `check_data_quality()`
- `dq_issues` (active defect table)

### Database Tables
- `dim_fuel_stations`
- `stg_updated_stations`

## 3. Downstream Dependencies
- `sys_run_log`
- `dim_fuel_stations`
- `stg_updated_stations`
- Subsequent execution of:
  - `check_data_quality()`

## 4. Inputs / Sources

This procedure:
- Has no parameters
- Reads from:
  - `dq_issues`
  - `dim_fuel_stations`
  - `stg_updated_stations`

## 5. Outputs

### Primary Output
- Updates records in:
  - `dim_fuel_stations`
- Deletes records from:
  - `stg_updated_stations`

### Logging Output
- Inserts execution summaries into:
  - `sys_run_log`
    - `procedure_name`
    - `description`
    - `rows_affected`

## 6. High-Level Logic / Execution Flow

### Step 1 – Reset Previous Logs
1. Remove prior `sys_run_log` records for this procedure

### Step 2 – Execute Autofix Rules

For each supported defect type:

1. Identify affected records from `dq_issues`
2. Apply automated remediation logic
3. Capture affected row count using `GET DIAGNOSTICS`
4. Insert summary log into `sys_run_log` if rows were modified

### Step 3 – Complete Procedure
1. End procedure execution
2. Downstream processes may rerun `check_data_quality()` to validate fixes

## 7. Autofix Rules Summary

| defect_id | defect_type | autofix_action |
|---|---|---|
| INC_01 | Inconsistent Data | Delete station from `stg_updated_stations` if not also flagged as `INC_02` |
| INC_03 | Inconsistent Data | Set `active = FALSE` in `dim_fuel_stations` |
| INC_05 | Inconsistent Data | Set oldest duplicate active station to inactive |
| MIS_02 | Missing Data | Reactivate matching station in `dim_fuel_stations` |

## 8. Conditional Logic

### `INC_01`
- Only removes records from `stg_updated_stations` when:
  - defect exists in `dq_issues`
  - matching `INC_02` defect does not exist

### `INC_05`
- Uses `ROW_NUMBER()` to identify oldest duplicate active station
- Only the oldest duplicate record is deactivated

### Logging
- `sys_run_log` entries are only created when rows are affected

## 9. Logging Mechanism (GET DIAGNOSTICS)

The procedure uses `GET DIAGNOSTICS` to capture how many rows were affected by each autofix action.

### How It Works
- After each `UPDATE` or `DELETE`, the system variable `ROW_COUNT` is captured:
  - Stored in `affected_rows`
- If `affected_rows > 0`, a log record is written to `sys_run_log`
- If no rows are affected, no log is created

### Logging Pattern
- Each autofix logs:
  - `procedure_name` → `update_data_quality_autofix`
  - `description` → defect ID (e.g. `INC_01`)
  - `rows_affected` → number of impacted records

### Key Points
- Logs only when changes occur
- Provides per-defect autofix visibility
- Keeps operational logs clean and lightweight

## 10. Idempotency & Safety

The procedure is designed to be safely rerunnable because:

- Autofixes operate only on active defects in `dq_issues`
- Updates are condition-based
- Duplicate logging is prevented by clearing previous logs at start
- Re-running the procedure produces stable outcomes when no new defects exist
