# Import packages
# Import necessary libraries
from sqlalchemy import create_engine, text
import argparse
import json
import logging
import os
import pandas as pd
import subprocess

# ----------------------------------------------------------------------------------------------------
#                                       setup variables
# ----------------------------------------------------------------------------------------------------

# Get log file path from orchestrator
parser = argparse.ArgumentParser()
parser.add_argument("--log-file", required=True)
args = parser.parse_args()
log_file = args.log_file

os.makedirs("data and logs", exist_ok=True)

# Set up logging for module
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s -      Module 4    - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Create logger with dummy name so it can be scaled later if needed
logger = logging.getLogger("log_dog")

# Load environment variables from GitHub Secrets
DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")

# Create database engine
engine = create_engine(DB_CONNECTION_STRING)

# Set up the file config
config_file = "config.json"
with open("config.json") as json_file:
    config = json.load(json_file)


# -------------------------------------------------------------------------------------------------
#                                       Define Functions
# -------------------------------------------------------------------------------------------------

def run_stored_procedure(conn, logger, procedure):
    changes = 0

    call = text(f"CALL {procedure}();")
    conn.execute(call)

    query = text("""
        SELECT
            procedure_name,
            description,
            rows_affected
        FROM sys_run_log
        WHERE procedure_name = :procedure
        ORDER BY log_id
    """)

    result = conn.execute(query, {"procedure": procedure})

    for row in result:
        logger.warning(
            "%s | %s | %s rows",
            row.procedure_name,
            row.description,
            row.rows_affected
        )
        changes += row.rows_affected

    return changes

# ----------------------------------------------------------------------------------------------------
#                                     Script Body - Start
# ----------------------------------------------------------------------------------------------------
# exit if the latest file has already been transformed
if config["latest_file"] == config["last_data_update"]:
    logger.info(f"{config['latest_file']} data is already updated in the database - Nothing to check")
    sys.exit(10)
	
logger.info("Running Data Quality Stored Procedure")

with engine.begin() as conn:
    data_quality = run_stored_procedure(conn, logger, "check_data_quality")
    if data_quality > 0:
        run_stored_procedure(conn, logger, "update_data_quality_autofix")
        run_stored_procedure(conn, logger, "check_data_quality")

logger.info("Operation complete")
