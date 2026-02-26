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

# ----------------------------------------------------------------------------------------------------
#                                     Script Body - Start
# ----------------------------------------------------------------------------------------------------

logger.info("Running SQL Stored Procedure")

call = text("CALL check_data_quality();")
with engine.connect() as conn:
    conn = conn.execution_options(isolation_level="AUTOCOMMIT")
    conn.execute(call)

# SQL query to fetch sys_run_log
data_quality_query = f"""
SELECT
	description,
	rows_affected
FROM
	sys_run_log
WHERE
	procedure_name = 'check_data_quality'
ORDER BY
	log_id
"""

# Execute the query
sys_run_log_dbo = pd.read_sql(data_quality_query, engine)

for index, row in sys_run_log_dbo.iterrows():
    logger.warning(
        'defect_id %s has %d rows',
        row["description"],
        row["rows_affected"]
    )

logger.info("Operation complete")
