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
    format="%(asctime)s - %(levelname)s -      Module 5    - %(message)s",
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

# Create date variables
latest_file = config["latest_file"]
latest_file_dt = datetime.strptime(latest_file, "%b%Y")
latest_file_month = latest_file_dt.strftime("%b").lower()
latest_file_year = latest_file_dt.strftime("%Y")
current_monthyear = datetime.now().replace(day=1).strftime("%b%Y").lower()

# timestamp for commits
datetimestamp = datetime.now().strftime("%Y%m%d_%Hh%M")

# -------------------------------------------------------------------------------------------------
#                                       Define Functions
# -------------------------------------------------------------------------------------------------

def run_stored_procedure(engine, logger, procedure):

    call = text(f"CALL {procedure}();")

    with engine.connect() as conn:
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(call)

    query = """
        SELECT
            procedure_name,
            description,
            rows_affected
        FROM sys_run_log
        WHERE procedure_name = %s
        ORDER BY log_id
    """

    sys_run_log_dbo  = pd.read_sql(query, engine, params=(procedure,))

    for index, row in sys_run_log_dbo.iterrows():
        logger.info(
            "%s | %s | %s rows",
            row.procedure_name,
            row.description,
            row.rows_affected
        )


def push_file_to_repo(file_path, commit_message):
    """
    Add, commit, and push a file to a GitHub repository using a GitHub token.

    Args:
        file_path (str): Path to the file to push.
        commit_message (str): Commit message for the Git change.

    Raises:
        subprocess.CalledProcessError: If any git command fails (except when commit has no changes).
    """
    logger.info("pushing file to repo")
    try:
        repo_url = (
            f"https://x-access-token:{os.environ['GITHUB_TOKEN']}"
            f"@github.com/{os.environ['GITHUB_REPOSITORY']}.git"
        )

        subprocess.run(["git", "config", "user.name", "github-actions"], check=True)
        subprocess.run(["git", "config", "user.email", "github-actions@github.com"], check=True)
        subprocess.run(["git", "add", file_path], check=True)
        subprocess.run(
            ["git", "commit", "-m", commit_message],
            check=False  # won't fail if nothing changed
        )
        subprocess.run(["git", "push", repo_url, "HEAD:main"], check=True)

        logger.info(f"Successfully pushed {file_path} to repo")

    except subprocess.CalledProcessError as e:
        logger.exception(f"Failed to push {file_path}: {e}")
        raise


def save_config():
    """
    Save the current configuration to a JSON file and push it to GitHub.

    Writes the global `config` object to 'config.json' with indentation,
    then pushes the file to the repository with a timestamped commit message.

    Raises:
        Exception: If writing the file or pushing to GitHub fails.
    """
    try:
        with open("config.json", "w") as json_file:
            json.dump(config, json_file, indent=4)
        logger.info("Config file updated")
        push_file_to_repo(config_file, f"successful run - configfile updated {datetimestamp}")

    except Exception as e:
        logger.exception(f"Unexpected error saving json config file: {e}")
# ----------------------------------------------------------------------------------------------------
#                                     Script Body - Start
# ----------------------------------------------------------------------------------------------------

# exit if the latest file has already been transformed
if config["latest_file"] == config["last_data_update"]:
    logger.info(f"{config['latest_file']} date is already updated in the database")
    sys.exit(10)

logger.info("Running SQL Stored Procedures")

run_stored_procedure(engine, logger, "update_fuel_stations_active")
run_stored_procedure(engine, logger, "update_fact_fuel")
run_stored_procedure(engine, logger, "update_fuel_stations_inactive")

#update the config 
#config["last_data_update"] = config["latest_file"]
#save_config()

logger.info("Operation complete")
