# Stored Procedure Spec: `check_data_quality()`

## 1. Procedure Overview
- **Name:** `check_data_quality()`  
- **Type:** PostgreSQL Stored Procedure (`plpgsql`)  
- **Purpose:**  
  Executes all data quality checks against staging and dimension tables, records active defects in `dq_issues`, logs row counts to `sys_run_log`, and removes resolved defects.  
- **Owner:** `neondb_owner`  

## 2. Upstream Dependencies
- Orchestrator Python module (`data_quality.py`) which calls this procedure [see specs](/documentation/dataflow%20specs/4.data_quality.md)
- `stg_fuel_price` (fact staging table)  
- `dim_fuel_stations` (dimension table)  
- `stg_new_stations` (station insert staging table)  
- `stg_updated_stations` (station update staging table)  

## 3. Downstream Dependencies
- `dq_issues` (data quality defect table)  
- `sys_run_log` (procedure execution log table)  

## 4. Inputs / Sources

This procedure:
- Has no parameters  
- Reads from:
  - `stg_fuel_price`
  - `dim_fuel_stations`
  - `stg_new_stations`
  - `stg_updated_stations`

## 5. Outputs

### Primary Output
- Inserts or updates active defects in:
  - `dq_issues`

### Logging Output
- Inserts run-level defect counts into:
  - `sys_run_log`
    - `procedure_name`
    - `description` (defect_id)
    - `rows_affected`

### Cleanup Output
- Deletes resolved defects from:
  - `dq_issues` where `is_active = false`

## 6. High-Level Logic / Execution Flow

### Step 1 – Reset State
1. Mark all existing defects as inactive  
2. Remove previous run logs for this procedure  

### Step 2 – Execute Data Quality Checks (AD_01 to AD_05)

For each defect:

1. Insert detected issues into `dq_issues`  
2. Reactivate existing defects if already present  
3. Capture affected row count  
4. If rows were affected:
   - Insert summary record into `sys_run_log`

This ensures:
- Idempotent execution  
- No duplicate defects  
- Full auditability of each run  

### Step 3 – Remove Resolved Defects

After all checks complete:
- Permanently remove defects that were not detected in the current run  

## 7. Data Quality Checks Summary

| defect_id | defect_type        | description |
|------------|-------------------|-------------|
| AD_01 | Missing Data | Station exists in fact table but not in dimension or staging tables |
| AD_02 | Reference Mismatch | Fact table marked for update but still has records under the old station name/address |
| AD_03 | Reference Mismatch | Fact table marked for update but has records under both old and new name/address |
| AD_04 | Parsing Issue | `town` or `street` is NULL in `stg_new_stations` |
| AD_05 | Parsing Issue | `town` or `street` is NULL in `stg_updated_stations` |

## 8. Conflict Handling Strategy

The procedure uses a conflict-handling mechanism that:
- Prevents duplicate defect records  
- Reactivates previously detected defects  
- Preserves historical defect identity  

## 9. Logging Strategy

For each defect check:
- Capture number of affected rows  
- Insert run summary into `sys_run_log` only if rows were detected  

This provides:
- Per-defect row counts  
- Lightweight operational audit trail  
- Run-by-run defect visibility  

## 10. Idempotency & Safety

The procedure is fully idempotent because:

- All defects are first marked inactive  
- Detected defects are reactivated or inserted  
- Resolved defects are removed at the end  
- Conflict handling prevents duplication  
- Logs are reset per run  

Running the procedure multiple times produces consistent and stable results.

## Full Procedure
```sql
-- PROCEDURE: public.check_data_quality()

-- DROP PROCEDURE IF EXISTS public.check_data_quality();

CREATE OR REPLACE PROCEDURE public.check_data_quality(
	)
LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
/*----------------------------------------
			START OF PROCEDURE
----------------------------------------*/

-- mark all defects as inactive
UPDATE dq_issues
SET is_active = false;

/*----------------------------------------
-- CHECK: AD_01
-- DESCRIPTION: Identify stations in the fact table (stg_fuel_price)
--              that do not exist in the dimension or staging tables.
-- TABLES INVOLVED:
--   Fact Table: stg_fuel_price
--   Dimension Table: dim_fuel_stations
--   Staging Tables: stg_new_stations, stg_updated_stations
-- KEY COLUMNS:
--   servicestationname, address
-- DEFECT TYPE: Missing Data
----------------------------------------*/
INSERT INTO
	dq_issues (
		defect_id,
		entity,
		key_var,
		key_var1,
		key_var2,
		attribute_name,
		attribute_value,
		defect_type,
		defect_description
	)
SELECT
	'AD_01' AS defect_id,
	'stg_fuel_price' AS entity,
	f.servicestationname AS key_var,
	f.address AS key_var1,
	'NA' AS key_var2,
	'stationid' AS attribute_name,
	NULL AS attribute_value,
	'Missing Data' AS defect_type,
    'Station exists in fact table but not in dimension or staging tables' AS defect_description
FROM
	stg_fuel_price AS f
	LEFT JOIN dim_fuel_stations AS d ON UPPER(f.servicestationname) = UPPER(d.name)
	    AND f.address = d.address
	LEFT JOIN stg_new_stations AS n ON UPPER(f.servicestationname) = UPPER(n.name)
	    AND f.address = n.address
	LEFT JOIN stg_updated_stations AS u ON UPPER(f.servicestationname) = UPPER(u.name)
	    AND f.address = u.address

WHERE d.stationid is null and n.stationid is null and u.stationid is null
GROUP BY
	f.servicestationname,
	f.address

ON CONFLICT (defect_id, entity, key_var, key_var1, key_var2)
DO UPDATE SET is_active = TRUE;

/*----------------------------------------
-- CHECK: AD_02
-- DESCRIPTION: Identify stations in the fact table (stg_fuel_price)
--              that are marked for update but still have records 
--              under the old station name/address.
-- TABLES INVOLVED:
--   Fact Table: stg_fuel_price
--   Dimension Table: dim_fuel_stations
--   Staging Table: stg_updated_stations
-- KEY COLUMNS:
--   servicestationname, address
-- DEFECT TYPE: Reference Mismatch
----------------------------------------*/
INSERT INTO
	dq_issues (
		defect_id,
		entity,
		key_var,
		key_var1,
		key_var2,
		attribute_name,
		attribute_value,
		defect_type,
		defect_description
	)
SELECT 
    DISTINCT
    'AD_02' AS defect_id,
    'stg_fuel_price' AS entity,
    f.servicestationname AS key_var,
    f.address AS key_var1,
    'NA' AS key_var2,
    'stationid' AS attribute_name,
    u.stationid AS attribute_value,
    'Reference Mismatch' AS defect_type,
    'Fact table marked for update but still have records under the old station name/address' AS defect_description
FROM
	stg_fuel_price AS f
	INNER JOIN dim_fuel_stations AS d ON UPPER(f.servicestationname) = UPPER(d.name)
	    AND f.address = d.address
	INNER JOIN stg_updated_stations AS u ON d.stationid = u.stationid

ON CONFLICT (defect_id, entity, key_var, key_var1, key_var2)
DO UPDATE SET is_active = TRUE;

/*----------------------------------------
-- CHECK: AD_03
-- DESCRIPTION: Identify stations in the fact table (stg_fuel_price)
--              that are marked for update but have records under
--              both the old name/address and new name/address
-- TABLES INVOLVED:
--   Fact Table: stg_fuel_price
--   Dimension Table: dim_fuel_stations
--   Staging Table: stg_updated_stations
-- KEY COLUMNS:
--   stationid
-- DEFECT TYPE: Reference Mismatch
----------------------------------------*/
INSERT INTO
	dq_issues (
		defect_id,
		entity,
		key_var,
		key_var1,
		key_var2,
		attribute_name,
		attribute_value,
		defect_type,
		defect_description
	)
SELECT 
    'AD_03' AS defect_id,
    'stg_fuel_price' AS entity,
    COALESCE(d.stationid, u.stationid) AS key_var,
    'NA' AS key_var1,
    'NA' AS key_var2,
    'stationid' AS attribute_name,
    COALESCE(d.stationid, u.stationid) AS attribute_value,
    'Reference Mismatch' AS defect_type,
    'Fact table marked for update but has records under both the old name/address and new name/address' AS defect_description
FROM
	stg_fuel_price f
	LEFT JOIN dim_fuel_stations d ON UPPER(f.servicestationname) = UPPER(d.name)
	AND UPPER(f.address) = UPPER(d.address)
	LEFT JOIN stg_updated_stations u ON UPPER(f.servicestationname) = UPPER(u.name)
	AND UPPER(f.address) = UPPER(u.address)
WHERE
	COALESCE(d.stationid, u.stationid) IS NOT NULL
GROUP BY
	COALESCE(d.stationid, u.stationid)
HAVING
	COUNT(d.stationid) > 0
	AND COUNT(u.stationid) > 0

ON CONFLICT (defect_id, entity, key_var, key_var1, key_var2)
DO UPDATE SET is_active = TRUE;

/*----------------------------------------
-- CHECK: AD_04
-- DESCRIPTION: Identify stations in the stg_new_stations
--              that are marked for update but have records under
--              both the old name/address and new name/address
-- TABLES INVOLVED:
--   Staging Table: stg_new_stations
-- KEY COLUMNS:
--   street, town
-- DEFECT TYPE: Parsing Issue
----------------------------------------*/
INSERT INTO
	dq_issues (
		defect_id,
		entity,
		key_var,
		key_var1,
		key_var2,
		attribute_name,
		attribute_value,
		defect_type,
		defect_description
	)
SELECT 
    'AD_04' AS defect_id,
    'stg_new_stations' AS entity,
    name AS key_var,
    address AS key_var1,
    'NA' AS key_var2,
    'Town / Street' AS attribute_name,
    'NA' AS attribute_value,
    'Parsing Issue' AS defect_type,
    'Columns "town" or "street" are NULL' AS defect_description
FROM
	stg_new_stations
WHERE
	town is null or street is null

ON CONFLICT (defect_id, entity, key_var, key_var1, key_var2)
DO UPDATE SET is_active = TRUE;

/*----------------------------------------
-- CHECK: AD_05
-- DESCRIPTION: Identify stations in the stg_updated_stations
--              that are marked for update but have records under
--              both the old name/address and new name/address
-- TABLES INVOLVED:
--   Staging Table: stg_updated_stations
-- KEY COLUMNS:
--   street, town
-- DEFECT TYPE: Parsing Issue
----------------------------------------*/
INSERT INTO
	dq_issues (
		defect_id,
		entity,
		key_var,
		key_var1,
		key_var2,
		attribute_name,
		attribute_value,
		defect_type,
		defect_description
	)
SELECT 
    'AD_05' AS defect_id,
    'stg_updated_stations' AS entity,
    name AS key_var,
    address AS key_var1,
    'NA' AS key_var2,
    'Town / Street' AS attribute_name,
    'NA' AS attribute_value,
    'Parsing Issue' AS defect_type,
    'Columns "town" or "street" are NULL' AS defect_description
FROM
	stg_updated_stations
WHERE
	town is null or street is null

ON CONFLICT (defect_id, entity, key_var, key_var1, key_var2)
DO UPDATE SET is_active = TRUE;

/*----------------------------------------
-- CLEANUP: Remove resolved defects
-- DESCRIPTION: Delete any defects that were not detected
--              in the current run (resolved defects)
----------------------------------------*/
DELETE FROM dq_issues
WHERE is_active = false;

/*----------------------------------------
			END OF PROCEDURE
----------------------------------------*/
END
$BODY$;
ALTER PROCEDURE public.check_data_quality()
    OWNER TO neondb_owner;
```
