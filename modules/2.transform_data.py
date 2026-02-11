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

# Function to extract the last dat of the precious month
def last_day_of_previous_month(date):
    """
    Calculates the last day of the previous month based on the earliest date in a given date column.

    Args:
        date_column (pd.Series): A Pandas Series containing date-like values.

    Returns:
        tuple: A tuple (year, month, day) representing the last day of the previous month.
    """
    # Calculate the last day of the previous month
    try:
        first_day_of_current_month = datetime(date.year, date.month, 1)
        last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
        last_day_of_previous_month = last_day_of_previous_month.date()
        return last_day_of_previous_month
    except Exception as e:
        print(f"Error: {e}")
        return None

# ----------------------------------------------------------------------------------------------------
#                                           Block one
# - Import data
# - Join Data
# - Convert column to date type
# ----------------------------------------------------------------------------------------------------

# Read the file 
file = f"data and logs/fuelcheck_{nextfile}.csv"
df = pd.read_csv(file)

# Forward-fill missing information (if the file was originally excel the cells can be merged verticallt causing issues)
df.ffill(inplace=True)

# Drop rows with all NaN values (if any)
df_fuel_data = df.dropna(how='all')

# -----> DEV DONE TO HERE

print(file)







# Forward-fill missing information
df.ffill(inplace=True)

# Drop rows with all NaN values (if any)
df_fuel_data = df.dropna(how='all')


# Active stations
query = text("""SELECT stationid,name, address
            FROM prod.fuel_station_dict
            WHERE deletion_flag IS NULL""")

fuel_station_dict = sql_select(query)

# Convert columns to uppercase for merging
df_fuel_data['ServiceStationName'] = df_fuel_data['ServiceStationName'].str.upper()
df_fuel_data['Address'] = df_fuel_data['Address'].str.upper()
fuel_station_dict['name'] = fuel_station_dict['name'].str.upper()
fuel_station_dict['address'] = fuel_station_dict['address'].str.upper()

# Left join new data to existing dictionary to extract stationid
merged_fuel_data = df_fuel_data.merge(
    fuel_station_dict,
    left_on=['ServiceStationName', 'Address'],
    right_on=['name', 'address'],
    how='left'
)

# Select and rename columns 
df_fuel_data = merged_fuel_data[['stationid','FuelCode','PriceUpdatedDate', 'Price']]
df_fuel_data.columns = ['stationid', 'fuelcode', 'date', 'price']

#Convert 'date' to datetime and normailse to reset the time component
try:
    df_fuel_data.loc[:, 'date'] = pd.to_datetime(df_fuel_data['date'], format='%d/%m/%Y %I:%M:%S %p').dt.normalize()
except:
    try:
        df_fuel_data.loc[:, 'date'] = pd.to_datetime(df_fuel_data['date'], format='%d/%m/%Y %H:%M').dt.normalize()
    except:
        df_fuel_data.loc[:, 'date'] = pd.to_datetime(df_fuel_data['date']).dt.normalize()

# quick Checks 
row_count_df_fuel_data = df_fuel_data.shape[0]
print(f"Number of rows df_fuel_data: {row_count_df_fuel_data}")
row_count_merged_fuel_data = merged_fuel_data.shape[0]
print(f"Number of rows merged_fuel_data: {row_count_merged_fuel_data}")


# ----------------------------------------------------------------------------------------------------
#                                           Block Two
# - Identify unique station and fuel type combinations for current month
# - Calculate the last day of the previous month 
# - Fetch active stations and fuel types for the last month
# - Union the two datasets
# - Cross join unique station-fuel combinations with the date range
# - Calculate the median price per day for each station and fuel type
# 
# ----------------------------------------------------------------------------------------------------

# Identify unique station and fuel type combinations
unique_station_fuel_combinations = (
    df_fuel_data[['stationid', 'fuelcode']]
    .drop_duplicates()
    .reset_index(drop=True)
)

# Calculate the last day of the previous month
date = df_fuel_data['date'].min()
last_day = last_day_of_previous_month(date)


# SQL query to fetch active stations and fuel types for the last month
query = text(
    f"""
    SELECT DISTINCT stationid, fuelcode 
    FROM prod.fuel_prices 
    WHERE date = '{last_day}'
    """
)

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
