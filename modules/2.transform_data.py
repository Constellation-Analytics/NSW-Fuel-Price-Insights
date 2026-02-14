# Import necessary libraries
import os
import json
import logging
import argparse
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import hashlib

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
    format="%(asctime)s - %(levelname)s -    Module    - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Create logger with dummy name so it can be scaled later if needed
logger = logging.getLogger("log_dog")

# Create database engine
engine = create_engine(DB_CONNECTION_STRING)

# Set up the file config
config_file = "config.json"
with open("config.json") as json_file:
    config = json.load(json_file)

# Create date variables
nextfile = config["next_file_date"]
nextfile_dt = datetime.strptime(nextfile, "%b%Y")
nextfile_month = nextfile_dt.strftime("%b").lower()
nextfile_year = nextfile_dt.strftime("%Y")
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

# ----------------------------------------------------------------------------------------------------
#                                           Block one
# - Import data
# - Fill missing information
# - Convert column to date type
# ----------------------------------------------------------------------------------------------------

# Read the file 
file = f"data and logs/fuelcheck_{nextfile}.csv"
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
        df_fuel_data['date'],
        dayfirst=True,
        errors='raise'
    ).dt.normalize()
)

# ----------------------------------------------------------------------------------------------------
#                                           Block Two
# - Set column headers to lowercase  
# - Identify unique station and fuel type combinations for current month
# - Fetch stations and fuel types for the last month
# - Union the two datasets
# ----------------------------------------------------------------------------------------------------

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
	public.fuel_prices
	INNER JOIN dim_fuel_station_dict 
        ON dim_fuel_station_dict.stationid = fuel_prices.stationid
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
#                                           Block Four
# - Fetch price data from last month
# - Every station Left join median prices 
# - Every station Left join last_day_of_last_month prices 
# - Create PriceUpdatedDate column date where Price is not Null
# ----------------------------------------------------------------------------------------------------

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
	public.fuel_prices
	INNER JOIN dim_fuel_station_dict 
	ON dim_fuel_station_dict.stationid = fuel_prices.stationid
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
#                                           Block Five
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

# add unique id
concat_cols = (
    output[['servicestationname','address','fuelcode','price','date']]
    .astype(str)
    .agg('|'.join, axis=1)
)

output['record_id'] = concat_cols.map(
    lambda x: hashlib
	.md5(x.encode())
	.hexdigest()
)

#order & rename the final output columns
output = output[['record_id', 'servicestationname', 'address', 'fuelcode', 'date', 'price', 'priceupdateddate']]

# ----------------------------------------------------------------------------------------------------
#                                           Block Five
# - Insert into database
# ----------------------------------------------------------------------------------------------------

# Insert into database
try:
	# Insert DataFrame into PostgreSQL
    logger.info(f"Inserting values into database")
	output.to_sql('fuelprice_staging', engine, if_exists='append', index=False)

except Exception as e:
    logger.exception(f"Unexpected error while inserting values into database: {e}")        

logger.info("Operation complete")
