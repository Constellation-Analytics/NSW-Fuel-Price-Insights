# Import packages
# Import necessary libraries
from sqlalchemy import create_engine, text
import argparse
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

# -------------------------------------------------------------------------------------------------
#                                       Define Functions
# -------------------------------------------------------------------------------------------------

def run_stored_procedure(engine, logger, procedure):

    call = text(f"CALL {procedure}();")

    with engine.connect() as conn:
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(call)

    query = f"""
        SELECT
            procedure_name,
            description,
            rows_affected
        FROM sys_run_log
        WHERE procedure_name = :procedure
        ORDER BY log_id
    """

    df = pd.read_sql(query, engine, params={"procedure": procedure})

    for row in df.itertuples(index=False):
        logger.info(
            "%s | %s | %s rows",
            row.procedure_name,
            row.description,
            row.rows_affected
        )

# ----------------------------------------------------------------------------------------------------
#                                     Script Body - Start
# ----------------------------------------------------------------------------------------------------

logger.info("Running SQL Stored Procedures")

run_stored_procedure(engine, logger, update_fuel_stations_active)
run_stored_procedure(engine, logger, update_fact_fuel)
run_stored_procedure(engine, logger, update_fuel_stations_inactive)

logger.info("Operation complete")
