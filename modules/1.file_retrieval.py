import os # to access GitHub repo
import subprocess # to commit in GitHub repo
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pandas as pd
from io import BytesIO, StringIO

# ----------------------------------------------------------------------------------------------------
#                                       Setup variables and functions
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

url = "https://data.nsw.gov.au/data/dataset/fuel-check"
response = requests.get(url)

soup = BeautifulSoup(response.text, "html.parser")

first_of_month = datetime.now().replace(day=1)
last_month_date = first_of_month - timedelta(days=1)
last_month_name = last_month_date.strftime('%b').lower()
last_month_year = last_month_date.strftime('%Y')

year = last_month_year
month = last_month_name



# Find links ending with .xlsx or .csv that match month/year (only one will be returned)
download_links = [
    a["href"]
    for a in soup.find_all("a", href=True)
    if (href := a["href"].lower()).endswith((".xlsx", ".csv"))
    and year in href
    and month in href
]

link = download_links[0]

resp = requests.get(link)

# Read file based on extension
if link.endswith(".xlsx"):
    df = pd.read_excel(BytesIO(resp.content))
elif link.endswith(".csv"):
    df = pd.read_csv(StringIO(resp.content.decode('utf-8')))
    
df.to_csv(f"data and logs/fuelcheck_{month}{year}.csv", index=False)

# Configure git to use GitHub Actions token
repo_url = f"https://x-access-token:{os.environ['GITHUB_TOKEN']}@github.com/{os.environ['GITHUB_REPOSITORY']}.git"
subprocess.run(["git", "config", "user.name", "github-actions"], check=True)
subprocess.run(["git", "config", "user.email", "github-actions@github.com"], check=True)

# Add files
subprocess.run(["git", "add", "Data and Logs/*"], check=True)

# Commit changes
subprocess.run(["git", "commit", "-m", f"Add fuelcheck_{month}{year}.xlsx"], check=False)

# Push changes
subprocess.run(["git", "push", repo_url, "HEAD:main"], check=True)
