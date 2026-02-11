# Import necessary libraries
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy import text
from tkinter import Tk
from tkinter.filedialog import askopenfilename
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
engine = create_engine(NEON_SECRET)

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
# - Calculate the last day of the previous month 
# - Fetch active stations and fuel types for the last month
# - Union the two datasets
# - Cross join unique station-fuel combinations with the date range
# - Calculate the median price per day for each station and fuel type
# 
# ----------------------------------------------------------------------------------------------------

# Select and rename columns 
df_fuel_data = df_fuel_data[['ServiceStationName','Address','Suburb','Postcode','Brand','FuelCode','PriceUpdatedDate','Price',]]
df_fuel_data.columns =['servicestationname','address','suburb','postcode','brand','fuelcode','priceupdateddate','price',]

# Identify unique station and fuel type combinations
unique_station_fuelcodes = (
    df_fuel_data[['servicestationname','address','fuelcode']]
    .drop_duplicates()
    .reset_index(drop=True)
)

# Calculate the last day of the previous month
date = df_fuel_data['date'].min()
last_day = last_day_of_previous_month(date)


# SQL query to fetch active stations and fuel types for the last month
query = """
SELECT DISTINCT
	name,
	address,
	fuelcode
FROM
	public.fuel_prices
	INNER JOIN dim_fuel_station_dict 
        ON dim_fuel_station_dict.stationid = fuel_prices.stationid
"""

# -----> DEV DONE TO HERE

last_month_station_data = pd.read_sql(query, engine)
# Execute the query
last_month_station_data =sql_select(query)

# Combine unique station-fuel combinations with last month's data and remove duplicates
union_data = pd.concat([unique_station_fuel_combinations, last_month_station_data]).drop_duplicates()

# Generate a full date range based on the min and max dates in the dataset
date_range_df = pd.DataFrame(pd.date_range(df_fuel_data['date'].min()- timedelta(days=1), 
                                           df_fuel_data['date'].max()),
                             columns=['date'])

# Create a cross join of unique station-fuel combinations with the date range
expanded_date_station_fuel_df = union_data.merge(date_range_df, how='cross')

# Sort the DataFrame by station ID, fuel code, and date
expanded_date_station_fuel_df.sort_values(by=['stationid', 'fuelcode', 'date'],
                                          inplace=True)

# Calculate the median price per day for each station and fuel type
daily_median_prices = (df_fuel_data.groupby(['stationid', 'fuelcode', 'date'])['price'].median().reset_index())



# ----------------------------------------------------------------------------------------------------
#                                           Block Three
# - Fetch price data from last month
# - Every station Left join median prices 
# - Every station Left join last_day_of_last_month prices 
# - Create PriceUpdatedDate column date where Price is not Null
# 
# ----------------------------------------------------------------------------------------------------


# SQL query to fetch active stations and fuel types for the last month
call = text(
    f"""
    SELECT stationid, fuelcode, price, date 
    FROM prod.fuel_prices 
    WHERE date = '{last_day}'
    """
)

# Execute the query
last_month_price_data = sql_select(call)

# Convert 'date' to datetime
last_month_price_data['date'] = pd.to_datetime(last_month_price_data['date'])


semijoined_data = expanded_date_station_fuel_df.merge(
    daily_median_prices,
    left_on=['stationid', 'fuelcode', 'date'],
    right_on=['stationid', 'fuelcode', 'date'],
    how='left'
)

joined_data = semijoined_data.merge(
    last_month_price_data,
    left_on=['stationid', 'fuelcode', 'date'],
    right_on=['stationid', 'fuelcode', 'date'],
    how='left'
)

# Ensure price columns are numeric before combining
joined_data['price_x'] = joined_data['price_x'].astype(float)
joined_data['price_y'] = joined_data['price_y'].astype(float)

# Combine prices into one column
joined_data['price'] = joined_data['price_x'].fillna(joined_data['price_y'])

# Drop redundant price columns if desired
joined_data = joined_data.drop(columns=['price_x', 'price_y'])


# set PriceUpdatedDate to date where Price is not Null
joined_data['PriceUpdatedDate'] = joined_data['date'].where(~joined_data['price'].isna(), pd.NaT)


# ----------------------------------------------------------------------------------------------------
#                                           Block Four
# - Forward fill all prices
# - Remove null prices
# - Remove last month data
# - Add unique id to each row
# ----------------------------------------------------------------------------------------------------

# Forward fill 'Price' within each 'StadionID' and 'FuelCode' group
joined_data['price'] = joined_data.groupby(['stationid', 'fuelcode'])['price'].ffill()

# Remove null price
drop_nulls = joined_data.dropna(subset = ['price']).reset_index(drop=True)

# Remove last month
max_month = joined_data['date'].max().month
output = drop_nulls[drop_nulls['date'].dt.month == max_month].copy()

# add unique id
def hash_row(row):
    hash_input = f"{row['stationid']}{row['fuelcode']}{row['price']}{row['date']}"
    return hashlib.md5(hash_input.encode()).hexdigest()
output['record_id'] = output.apply(hash_row, axis=1)

#order & rename the final output columns
output = output[['record_id','stationid', 'fuelcode', 'date', 'price','PriceUpdatedDate']]
output.columns = ['record_id', 'stationid', 'fuelcode', 'date', 'price', 'priceupdateddate']

# quick Checks 
row_count_output = output.shape[0]
print(f"Number of rows output: {row_count_output}")


# ----------------------------------------------------------------------------------------------------
#                                           Block Five
# - Check stations that are not present in the dictonary
# - Insert into database
# ----------------------------------------------------------------------------------------------------

missing_values = merged_fuel_data[merged_fuel_data.isna().any(axis=1)]
missing_values = missing_values[['ServiceStationName','Address','Suburb','Postcode','Brand']]
missing_values.drop_duplicates(inplace=True)

stations = missing_values['ServiceStationName'].unique()
final_output_path = r'C:\Users\paulj\OneDrive\Documents\3. Personal\Projects and Data Challenges\Fuel Analysis 2024\Testing\missing_values.csv'
missing_values.to_csv(final_output_path, index=False)


# Insert into database
if len(missing_values) == 0:
    print("we do not have missing values")
    try:
         # Database connection string
        db_url = "postgresql+psycopg2://paul:postgres@localhost:5432/fuelcheck"
    
        # Create the database engine
        engine = create_engine(db_url)
        
        # Insert DataFrame into PostgreSQL
        with engine.connect() as connection:
            output.to_sql('fuel_prices', connection, schema='prod', if_exists='append', index=False)
        print("Data inserted successfully!")
    except Exception as e:
        print(f"error: {e}")

# -------------------------------------------------------------------------------------------------
#                                       Removing obsolete records
# -------------------------------------------------------------------------------------------------
    print("Removing obsolete records")

    call = text("CALL prod.remove_obsolete_records();")
    with engine.connect() as conn:
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(call)

else: print("we have missing values")

print("Operation complete")
