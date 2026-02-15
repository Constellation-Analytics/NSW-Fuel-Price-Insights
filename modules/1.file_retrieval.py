from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from io import BytesIO, StringIO # to read the raw xlsx or csv file
import argparse
import json
import logging
import os  # to access GitHub repo
import pandas as pd
import requests
import subprocess  # to commit in GitHub repo
import sys

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
    format="%(asctime)s - %(levelname)s -      Module      - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Create logger with dummy name so it can be scaled later if needed
logger = logging.getLogger("log_dog")

# ----------------
# url for web scraping
url = "https://data.nsw.gov.au/data/dataset/fuel-check"

# Set up the file config
config_file = "config.json"
with open("config.json") as json_file:
    config = json.load(json_file)

# Create date variables
latest_file = config["latest_file"]
nextfile = config["next_file_date"]
nextfile_dt = datetime.strptime(nextfile, "%b%Y")
nextfile_month = nextfile_dt.strftime("%b").lower()
nextfile_year = nextfile_dt.strftime("%Y")
current_monthyear = datetime.now().replace(day=1).strftime("%b%Y").lower()

# timestamp for commits
datetimestamp = datetime.now().strftime("%Y%m%d_%Hh%M")

# Set up the file name
datafile = f"data and logs/fuelcheck_{nextfile}.csv"

# ----------------------------------------------------------------------------------------------------
#                                       setup functions
# ----------------------------------------------------------------------------------------------------

def push_file_to_repo(file_path, commit_message):
    """Adds, commits, and pushes a file to GitHub using GITHUB_TOKEN"""
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

logger.info(f"connecting to {url}")
response = requests.get(url)
soup = BeautifulSoup(response.text, "html.parser")

# Find links ending with .xlsx or .csv that match last_month_name/last_month_year
download_links = [
    a["href"]
    for a in soup.find_all("a", href=True)
    if (href := a["href"].lower()).endswith((".xlsx", ".csv"))
    and nextfile_month in href
    and nextfile_year in href
]

# exit if the most recent file has already been processed
if current_monthyear == nextfile:
    logger.info(f"{latest_file} data file already loaded")
    sys.exit(10)

# exit if the file is not yet available
if len(download_links) == 0:
    logger.info(f"{nextfile} file not yet available")
    sys.exit(10)

# exit if the latest file has not yet been transformed
if config["latest_file"] != config["last_transformation"]:
    logger.info(f"{config['latest_file']} file has not yet been transformed")
    sys.exit(10)

link = download_links[0]

logger.info("downloading file from server")
resp = requests.get(link)

# Read file based on extension
if link.endswith(".xlsx"):
    df = pd.read_excel(BytesIO(resp.content))
elif link.endswith(".csv"):
    df = pd.read_csv(StringIO(resp.content.decode("utf-8")))

logger.info("converting file to csv")
df.to_csv(datafile, index=False)

# save the data file
push_file_to_repo(datafile, f"data file loaded {datetimestamp}")

# chage date variable for readability
latest_file = nextfile

#update the config 
next_file_date = datetime.strptime(latest_file, "%b%Y") + relativedelta(months=1)
config["next_file_date"] = next_file_date.strftime("%b%Y").lower()
config["latest_file"] = latest_file
save_config()

# ----------------------------------------------------------------------------------------------------
#                                     Script Body - End
# ----------------------------------------------------------------------------------------------------
