# Module Spec: Orchestrator.py

## 1. Module Overview
- **Name / ID:** `orchestrator`  
- **Purpose:** Coordinates execution of all modules, handles logging and error handling.  
- **Author / Date:** Paul – 21 Jan 2026  

## 2. Upstream Dependencies
- GitHub Actions cron job triggers orchestrator 
 
## 3. Downstream Dependencies
- Modules: `file_etl`, `api_etl`, `data_quality`, `data_update` 

## 4. Inputs / Sources
- Log file path
- Config.json file
- Secrets (DB credentials, API keys) 

## 5. Outputs
- Workflow log file (`workflow.log`)  
- Results from individual modules loaded into GitHub  

## 6. Logic / Steps
1. Initialise logging
2. Retrieve parameters from config.json
3. Execute modules sequentially:  
   - `File Retrieval` → `Transform Data` → `API Integration` → `Data Quality` → `Data Update`  
4. Conditional checks:
   - Stop workflow if critical module fails  
5. Log start and finish time of each module

## 7. Error Handling & Logging
- Wrap module calls in `try/except`  
- Log all exceptions with severity  
- Optional notifications for errors  
