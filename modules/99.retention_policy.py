from datetime import datetime, timedelta
import argparse
import glob
import logging
import os
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
    format="%(asctime)s - %(levelname)s - Retention Policy - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Create logger with dummy name so it can be scaled later if needed
logger = logging.getLogger("log_dog")
def cleanup_old_workflow_logs():
    """
    Deletes workflow log files older than 30 days and pushes changes to GitHub.
    Files look like: workflow_20260215_07h33.log
    """
    try:
        repo_url = (
            f"https://x-access-token:{os.environ['GITHUB_TOKEN']}@github.com/{os.environ['GITHUB_REPOSITORY']}.git"
        )
        
        subprocess.run(["git", "config", "user.name", "github-actions"], check=True)
        subprocess.run(["git", "config", "user.email", "github-actions@github.com"], check=True)

        # 30 days ago
        cutoff_date = datetime.now() - timedelta(days=5)
        
        # Name of log folder
        log_folder = "data and logs"

        # Find all workflow logs
        for file_path in glob.glob(os.path.join(log_folder, "workflow_*.log")):
            # Extract date from filename
            base = os.path.basename(file_path)
            try:
                date_str = base.split("_")[1]  # e.g., '20260215'
                file_date = datetime.strptime(date_str, "%Y%m%d")
            except (IndexError, ValueError):
                logger.warning(f"Skipping file with unexpected format: {file_path}")
                continue

            if file_date < cutoff_date:
                os.remove(file_path)
                logger.info(f"Deleted old workflow log: {file_path}")

        # Stage deletions and push
        subprocess.run(["git", "add", "-u"], check=True)
        subprocess.run(["git", "commit", "-m", "Cleanup old workflow logs"], check=False)
        subprocess.run(["git", "push", repo_url, "HEAD:main"], check=True)

        logger.info("Old workflow logs cleaned up and changes pushed.")

    except subprocess.CalledProcessError as e:
        logger.exception(f"Git operation failed: {e}")
        raise

cleanup_old_workflow_logs()
