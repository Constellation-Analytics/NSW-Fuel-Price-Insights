import subprocess
import logging
import os
from datetime import datetime

# ----------------------------------------------------------------------------------------------------
#                                       setup variables
# ----------------------------------------------------------------------------------------------------

os.makedirs("data and logs", exist_ok=True)
filedatestamp = datetime.now().strftime("_%Y%m%d_%Hh%M")
log_file = f"data and logs/workflow{filedatestamp}.log"

# Set up logging for orchestrator
logging.basicConfig(
  filename=log_file,
  level=logging.INFO,
  format="%(asctime)s - %(levelname)s - %(message)s",
  datefmt="%Y-%m-%d %H:%M:%S"
)

# Create logger with dummy name so it can be scaled later if needed
logger = logging.getLogger('log_dog')

# ----------------------------------------------------------------------------------------------------
#                                       setup functions
# ----------------------------------------------------------------------------------------------------

def push_file_to_repo(file_path, commit_message):
    """Adds, commits, and pushes a file to GitHub using GITHUB_TOKEN"""
    try:
        repo_url = f"https://x-access-token:{os.environ['GITHUB_TOKEN']}@github.com/{os.environ['GITHUB_REPOSITORY']}.git"
        subprocess.run(["git", "config", "user.name", "github-actions"], check=True)
        subprocess.run(["git", "config", "user.email", "github-actions@github.com"], check=True)
        subprocess.run(["git", "add", file_path], check=True)
        subprocess.run(["git", "commit", "-m", commit_message], check=False)  # won't fail if nothing changed
        subprocess.run(["git", "push", repo_url, "HEAD:main"], check=True)
        logger.info(f"Successfully pushed {file_path} to repo")
    except subprocess.CalledProcessError as e:
        logger.exception(f"Failed to push {file_path}: {e}")
        print(f"ERROR: Failed to push {file_path}: {e}")  # print error to terminal
        raise

# create a reusable function to call modules with logging and error handling
def run_module(module_path):
    try:
        logger.info(f"Starting {module_path}")
        subprocess.run(["python", module_path], check=True)
        logger.info(f"Finished {module_path}")
    except subprocess.CalledProcessError as e:
        logger.exception(f"Module {module_path} failed with exit code {e.returncode}")
        # Push log before stopping
        push_file_to_repo(log_file, f"Workflow log before failure in {module_path}")
        raise
    except Exception as e:
        logger.exception(f"Unexpected error running {module_path}: {e}")
        # Push log before stopping
        push_file_to_repo(log_file, f"Workflow log before failure in {module_path}")
        raise


# ----------------------------------------------------------------------------------------------------
#                                     Script Body - Start
# ----------------------------------------------------------------------------------------------------

# Run module1.py in its entirety
run_module("modules/1.file_retrieval.py")

# save the log
push_file_to_repo(log_file, f"log file loaded {filedatestamp}")

# ----------------------------------------------------------------------------------------------------
#                                     Script Body - End
# ----------------------------------------------------------------------------------------------------
