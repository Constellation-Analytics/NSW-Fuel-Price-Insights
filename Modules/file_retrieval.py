import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

url = "https://data.nsw.gov.au/data/dataset/fuel-check"
response = requests.get(url)


soup = BeautifulSoup(response.text, "html.parser")

links = soup.find_all(class_='nsw-button nsw-button--dark resource-url-analytics',href=True)

first_of_month = datetime.now().replace(day=1)
last_month_date = first_of_month - timedelta(days=1)
last_month_name = last_month_date.strftime('%b').lower()
last_month_year = last_month_date.strftime('%Y')


year = last_month_year
month = last_month_name

download_link = [
    href
    for a in soup.find_all("a", href=True)
    if (href := a["href"].lower()).endswith(".xlsx")
    and year in href
    and month in href
]

print(download_link)


import os
import subprocess

# Make sure the folder exists
os.makedirs("Data and Logs", exist_ok=True)

# Example: save a file
with open("Data and Logs/fuelcheck_dec2025.xlsx", "wb") as f:
    f.write(b"Example content")  # Replace with real downloaded content

# Configure git to use GitHub Actions token
repo_url = f"https://x-access-token:{os.environ['GITHUB_TOKEN']}@github.com/{os.environ['GITHUB_REPOSITORY']}.git"

# Add files
subprocess.run(["git", "add", "Data and Logs/*"], check=True)

# Commit changes
subprocess.run(["git", "commit", "-m", "Add fuelcheck_dec2025.xlsx"], check=False)

# Push changes
subprocess.run(["git", "push", repo_url, "HEAD:main"], check=True)
