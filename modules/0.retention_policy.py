import os
import glob
from datetime import datetime, timedelta
import subprocess
import logging

logger = logging.getLogger(__name__)

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
        cutoff_date = datetime.now() - timedelta(days=30)

        # Find all workflow logs
        for file_path in glob.glob("workflow_*.log"):
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
