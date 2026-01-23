import os # to access GitHub repo
import subprocess # to commit in GitHub repo
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pandas as pd
from io import BytesIO, StringIO
import logging
import argparse

# ----------------------------------------------------------------------------------------------------
#                                       setup variables
# ----------------------------------------------------------------------------------------------------

# Get log file path from orchestrator
parser = argparse.ArgumentParser()
parser.add_argument("--log-file", required=True)
args = parser.parse_args()
log_file = args.log_file

os.makedirs("data and logs", exist_ok=True)
datetimestamp = datetime.now().strftime("_%Y%m%d_%Hh%M")

# Set up logging for module
logging.basicConfig(
  filename=log_file,
  level=logging.INFO,
  format="%(asctime)s - %(levelname)s - %(message)s",
  datefmt="%Y-%m-%d %H:%M:%S"
)

# Create logger with dummy name so it can be scaled later if needed
logger = logging.getLogger('log_dog')

# ----------------
logger.info("THIS IS A TEST INSIDE MODULE1")
# url for web scraping
url = "https://data.nsw.gov.au/data/dataset/fuel-check"

first_of_month = datetime.now().replace(day=1)
last_month_date = first_of_month - timedelta(days=1)
last_month_name = last_month_date.strftime('%b').lower()
last_month_year = last_month_date.strftime('%Y')

datafile = f"data and logs/fuelcheck_{last_month_name}{last_month_year}.csv"

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
      
        # check to see if anything has changed since last commit
        status = subprocess.run(
            ["git", "status", "--porcelain"],
            capture_output=True,
            text=True
        )
        if not status.stdout.strip():
            logger.info("No changes detected. Skipping commit and push.")
            return 
        
        subprocess.run(["git", "commit", "-m", commit_message], check=False)  # won't fail if nothing changed
        subprocess.run(["git", "push", repo_url, "HEAD:main"], check=True)
        logger.info(f"Successfully pushed {file_path} to repo")
    except subprocess.CalledProcessError as e:
        logger.exception(f"Failed to push {file_path}: {e}")
        print(f"ERROR: Failed to push {file_path}: {e}")  # print error to terminal
        raise

# ----------------------------------------------------------------------------------------------------
#                                     Script Body - Start
# ----------------------------------------------------------------------------------------------------

response = requests.get(url)
soup = BeautifulSoup(response.text, "html.parser")

# Find links ending with .xlsx or .csv that match last_month_name/last_month_year (only one will be returned)
download_links = [
    a["href"]
    for a in soup.find_all("a", href=True)
    if (href := a["href"].lower()).endswith((".xlsx", ".csv"))
    and last_month_year in href
    and last_month_name in href
]

link = download_links[0]

resp = requests.get(link)

# Read file based on extension
if link.endswith(".xlsx"):
    df = pd.read_excel(BytesIO(resp.content))
elif link.endswith(".csv"):
    df = pd.read_csv(StringIO(resp.content.decode('utf-8')))
    
df.to_csv(datafile, index=False)

# save the log
push_file_to_repo(datafile, f"data file loaded {datetimestamp}")

# ----------------------------------------------------------------------------------------------------
#                                     Script Body - End
# ----------------------------------------------------------------------------------------------------
