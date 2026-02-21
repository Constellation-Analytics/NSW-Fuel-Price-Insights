# Import necessary libraries
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import argparse
import hashlib
import json
import logging
import numpy as np
import os
import pandas as pd
import subprocess
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
    format="%(asctime)s - %(levelname)s -      Module 2     - %(message)s",
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

# ----------------------------------------------------------------------------------------------------
#                                       Defining functions
# ----------------------------------------------------------------------------------------------------

# Function to extract the last dat of the previous month
def last_day_of_previous_month(any_date):
    """
    Calculates the last day of the previous month based on a given date.

    Args:
        any_date (datetime): A datetime object.

    Returns:
        datetime.date: The last day of the previous month.
    """
    # Calculate the last day of the previous month
    try:
        return (datetime(any_date.year, any_date.month, 1) - timedelta(days=1)).date()
    except Exception as e:
        logger.exception(f"Error calculating last day of previous month: {e}")
        raise

# Hash function to create deterministic fingerprint of each row
def generate_md5_hash(value: str) -> str:
    """
    Generate an MD5 hash for a given string.

    Args:
        value (str): The input string to hash.

    Returns:
        str: A 32-character hexadecimal MD5 hash of the input string.
    """
    encoded = value.encode("utf-8")  # convert string to bytes (required for hashing)
    hash_object = hashlib.md5(encoded)  # generate MD5 hash object
    return hash_object.hexdigest()  # return 32-character hexadecimal string


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
if config["latest_file"] == config["last_transformation"]:
    logger.info(f"{config['latest_file']} file has already been transformed")
    sys.exit(10)

# ----------------------------------------------------------------------------------------------------
#                                           Block one
# - Import data
# - Fill missing information
# - Convert column to date type
# ----------------------------------------------------------------------------------------------------

# Read the file 
file = f"data and logs/fuelcheck_{latest_file}.csv"
logger.info(f"Reading {file}")

# Forward-fill missing information (if the file was originally excel the cells can be merged vertically causing issues)
df_fuel_data = (
    pd.read_csv(file)
      .ffill()
      .copy()
)

#Convert 'date' to datetime and normalise to reset the time component
df_fuel_data['date'] = (
    pd.to_datetime(
        df_fuel_data['PriceUpdatedDate'],
        errors='raise'
    ).dt.normalize()
)

rowcount = len(df_fuel_data)
logger.info(f"df_fuel_data has {rowcount} rows")


# ----------------------------------------------------------------------------------------------------
#                                           Block Two
# - Set column headers to lowercase  
# - Identify unique station and fuel type combinations for current month
# - Fetch stations and fuel types for the last month
# - Union the two datasets
# ----------------------------------------------------------------------------------------------------
logger.info(f"Starting Block Two")

# Set column headers to lowercase  
df_fuel_data.columns = df_fuel_data.columns.str.lower()

# Identify unique station and fuel type combinations
unique_station_fuelcodes = (
    df_fuel_data[['servicestationname','address','fuelcode']]
    .drop_duplicates()
    .reset_index(drop=True)
)

# SQL query to fetch active stations and fuel types for the last month
station_query = f"""
SELECT DISTINCT
	name,
	address,
	fuelcode
FROM
	public.fact_fuel_prices
	INNER JOIN dim_fuel_stations 
        ON dim_fuel_stations.stationid = fact_fuel_prices.stationid
"""

# Execute the query
station_fuelcode_dbo = pd.read_sql(station_query, engine)

# Combine unique station-fuel combinations with last month's data and remove duplicates
union_data = pd.concat([unique_station_fuelcodes, station_fuelcode_dbo]).drop_duplicates().reset_index(drop=True)

# ----------------------------------------------------------------------------------------------------
#                                           Block Three
# - Create date_range_df 
# - Cross join to unique combinations of 'servicestationname','address','fuelcode' defined in Block 2
# - Get average price per 'servicestationname','address','fuelcode', 'date' from Block 1
# ----------------------------------------------------------------------------------------------------
logger.info(f"Starting Block Three")

# Generate a full date range based on the min and max dates in the dataset
date_range_df = pd.DataFrame(
    pd.date_range(df_fuel_data['date']
                  .min() - timedelta(days=1),
				  df_fuel_data['date'].max()
				 ),columns=['date']
)

# Create a cross join of unique station-fuel combinations with the date range
# Sort the DataFrame ready for forward fill
date_station_fuel_expanded = (
	union_data
	.merge(date_range_df, how='cross')
	.sort_values(by=['servicestationname','address','fuelcode','date'])
)

# Calculate the median price per day for each station and fuel type
daily_median_prices = (
	df_fuel_data
	.groupby(['servicestationname','address','fuelcode','date'])['price']
	.median()
	.reset_index()
)

# ----------------------------------------------------------------------------------------------------
#                                           Block Four - pt1
# - Fetch price data from last month
# - Every station Left join median prices 
# - Every station Left join last_day_of_last_month prices 
# - Create PriceUpdatedDate column date where Price is not Null
# ----------------------------------------------------------------------------------------------------
logger.info(f"Starting Block Four")

# Calculate the last day of the previous month
date = df_fuel_data['date'].min()
last_day = last_day_of_previous_month(date)

# SQL query to fetch fuel price data from last month
price_query = f"""
SELECT 
	name,
	address,
	fuelcode,
	price,
	date
FROM
	public.fact_fuel_prices
	INNER JOIN dim_fuel_stations 
	ON dim_fuel_stations.stationid = fact_fuel_prices.stationid
WHERE
	date = '{last_day}'
"""

# Execute the query
last_month_price_data = pd.read_sql(price_query, engine)

# Convert 'date' to datetime
last_month_price_data['date'] = pd.to_datetime(last_month_price_data['date'])

semijoined_data = (
	date_station_fuel_expanded
	.merge(
		daily_median_prices,
		left_on=['servicestationname', 'address', 'fuelcode', 'date'],
		right_on=['servicestationname', 'address', 'fuelcode', 'date'],
		how='left')
)

joined_data = (
	semijoined_data
	.merge(
		last_month_price_data,
		left_on=['servicestationname', 'address', 'fuelcode', 'date'],
		right_on=['name', 'address', 'fuelcode', 'date'],
           how='left')
)

# Ensure price columns are numeric before combining
joined_data['price_x'] = joined_data['price_x'].astype(float)
joined_data['price_y'] = joined_data['price_y'].astype(float)

# Combine prices into one column
joined_data['price'] = joined_data['price_x'].fillna(joined_data['price_y'])

# Drop redundant price columns if desired
joined_data = joined_data.drop(columns=['price_x', 'price_y'])

# set PriceUpdatedDate to date where Price is not Null
joined_data['priceupdateddate']= joined_data['date'].where(~joined_data['price'].isna(), pd.NaT)

# ----------------------------------------------------------------------------------------------------
#                                           Block Four - pt2
# - Forward fill all prices
# - Remove null prices
# - Remove last month data
# - Add unique id to each row
# ----------------------------------------------------------------------------------------------------

# Forward fill 'Price' within each 'servicestationname', 'address', 'fuelcode' group
joined_data['price'] = joined_data.groupby(['servicestationname', 'address', 'fuelcode'])['price'].ffill()

# Remove null price
drop_nulls = joined_data.dropna(subset = ['price']).reset_index(drop=True)

# Remove last month
max_date = joined_data['date'].max()
output = drop_nulls[(drop_nulls['date'].dt.year == max_date.year) & (drop_nulls['date'].dt.month == max_date.month)].copy()

# Generate deterministic record_id for each fuel price observation
# Step 1: Select key columns and concatenate them into a single string
concat_cols = (
    output[['servicestationname','address','fuelcode','price','date']]  # key columns
    .astype(str)  # ensure consistent string representation before hashing
    .agg('|'.join, axis=1)  # combine columns row-wise using a stable delimiter
)
# Step 2: Apply hash function to each concatenated row to create record_id
output['record_id'] = concat_cols.map(generate_md5_hash)

#order & rename the final output columns
output = output[['record_id', 'servicestationname', 'address', 'fuelcode', 'date', 'price', 'priceupdateddate']]

rowcount = len(output)
logger.info(f"Final output has {rowcount} rows")
# ----------------------------------------------------------------------------------------------------
#                                           Block Four - pt3
# - Insert into database
# ----------------------------------------------------------------------------------------------------

# Insert into database
try:
	logger.info(f"Inserting values into database")
	output.to_sql('stg_fuel_price', engine, if_exists='append', index=False)

except Exception as e:
    logger.exception(f"Unexpected error while inserting values into database: {e}")

#update the config 
config["last_transformation"] = config["latest_file"]
save_config()

logger.info("Operation complete")
