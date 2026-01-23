import subprocess
import logging
import os
from datetime import datetime
import json

# ----------------------------------------------------------------------------------------------------
#                                       Setup Variables
# ----------------------------------------------------------------------------------------------------

os.makedirs("data and logs", exist_ok=True)
datetimestamp = datetime.now().strftime("%Y%m%d_%Hh%M")
log_file = f"data and logs/workflow_{datetimestamp}.log"
config_file = "config.json"

# Set up logging for orchestrator
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - Orchestrator - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Create logger with dummy name so it can be scaled later if needed
logger = logging.getLogger("log_dog")

# Read the file config
with open("config.json") as json_file:
    config = json.load(json_file)

lastrun = config["last_run_date"]

# ----------------------------------------------------------------------------------------------------
#                                       Setup Functions
# ----------------------------------------------------------------------------------------------------

def push_file_to_repo(file_path, commit_message):
    """Adds, commits, and pushes a file to GitHub using GITHUB_TOKEN"""
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
        print(f"ERROR: Failed to push {file_path}: {e}")
        raise


def run_module(module_path):
    """Runs python files as a subprocess"""
    try:
        logger.info(f"Starting {module_path}")

        subprocess.run(
            ["python", module_path, "--log-file", log_file],
            check=True,
            capture_output=True,
            text=True
        )

        logger.info(f"Finished {module_path}")

    except subprocess.CalledProcessError as e:
        logger.error(f"Module {module_path} failed with exit code {e.returncode}")
        logger.error(f"{module_path} errors before failure:\n{e.stderr}")

        push_file_to_repo(
            log_file,
            f"Workflow log before failure in {module_path}"
        )
        raise

    except Exception as e:
        logger.exception(f"Unexpected error running {module_path}: {e}")

        push_file_to_repo(
            log_file,
            f"Workflow log before failure in {module_path}"
        )
        raise

# ----------------------------------------------------------------------------------------------------
#                                     Script Body - Start
# ----------------------------------------------------------------------------------------------------

logger.info("Starting orchestrator")

config["last_run_date"] = datetimestamp

# run module1.py
run_module("modules/1.file_retrieval.py")

logger.info("Finished orchestrator")

# save the log
push_file_to_repo(
    log_file,
    f"successful run - log file loaded {datetimestamp}"
)

# Update Config
with open("config.json", "w") as json_file:
    json.dump(config, json_file, indent=4) 

push_file_to_repo(
    config_file,
    f"successful run - configfile updated {datetimestamp}"
)

# ----------------------------------------------------------------------------------------------------
#                                     Script Body - End
# ----------------------------------------------------------------------------------------------------
