# Stored Procedure Spec: `truncate_stg_tables()`

## 1. Procedure Overview
- **Name:** `truncate_stg_tables()`  
- **Type:** PostgreSQL Stored Procedure (`plpgsql`)  
- **Purpose:**  
  Clears all staging tables after successful data promotion to ensure the next pipeline run starts with a clean staging environment.  
- **Owner:** `neondb_owner`  

## 2. Upstream Dependencies
- Orchestrator Python module (`5.data_update.py`)
- Successful execution of production update procedures

### Database Tables
- `stg_new_stations`
- `stg_inactive_stations`
- `stg_updated_stations`
- `stg_fuel_price`

## 3. Downstream Dependencies
- Future workflow executions
- Staging table load processes
