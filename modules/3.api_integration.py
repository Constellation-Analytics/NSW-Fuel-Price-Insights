#Import packages
# Import necessary libraries
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import argparse
import json
import logging
import numpy as np
import os
import pandas as pd
import requests
import subprocess
import sys
import uuid

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

# Load environment variables from GitHub Secrets
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
API_AUTHORISATION_HEADER = os.getenv("API_AUTHORISATION_HEADER")

# Authentification Information
access_token_URL = "https://api.onegov.nsw.gov.au/oauth/client_credential/accesstoken"
dict_url = "https://api.onegov.nsw.gov.au/FuelCheckRefData/v2/fuel/lovs"
unique_id = str(uuid.uuid4())

# -------------------------------------------------------------------------------------------------
#                                       Define Functions
# -------------------------------------------------------------------------------------------------


# Create an Access Token
def create_access_token(url, authorisation_header):
    """
    Request an OAuth access token using the client credentials grant type.

    Args:
        url (str): The token endpoint URL.
        authorisation_header (str): Authorization header value (e.g., Basic base64(client_id:secret)).

    Returns:
        str | None: Access token if successful, otherwise None.
    """
    # Define query parameters for the access token request
    querystring = {"grant_type": "client_credentials"}
    
    # Set the headers, including content type and authorization
    headers = {
        'content-type': "application/json",
        'authorization': authorisation_header  
    }
    
    try:
        # Make a GET request to the API
        auth_response = requests.get(url, headers=headers, params=querystring)
        auth_response.raise_for_status()
        
        # Parse JSON response
        data = auth_response.json()
        return data.get("access_token", None)

    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None


def api_data(url, access_token, API_key):
    """
    Retrieve station data from the API and return it as a normalized DataFrame.

    Args:
        url (str): API endpoint URL.
        access_token (str): Bearer token for authorization.
        API_key (str): API key required by the endpoint.

    Returns:
        pd.DataFrame | None: Normalized DataFrame of station data if successful,
        otherwise None.
    """
    headers = {
        'content-type': "application/json",
        'authorization': f"Bearer {access_token}",
        'apikey': API_key,
        'transactionid': unique_id,
        'requesttimestamp': datetimestamp,
        'if-modified-since': datetimestamp
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        # Parse JSON response
        data = response.json()
        if data is None:
            logger.error("API returned no data")
            sys.exit(1)
        stations = data["stations"]["items"]
        return pd.json_normalize(stations)

    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None


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


# -------------------------------------------------------------------------------------------------
#                                       Pull API Information
# -------------------------------------------------------------------------------------------------

logger.info(f"Pulling API Information")

# Generate the access token and access the data
token = create_access_token(access_token_URL,API_AUTHORISATION_HEADER)
data = api_data(dict_url,token,API_KEY)

# Create the new address columns using 
data['street'] = data['address'].str.extract(r'((?:\d+|Corner|Cnr).+?),')
data['street'] = data['street'].str.title()
data['town'] = data['address'].str.extract(r',\s(\D+)\sNSW\s\d+')
data['town'] = data['town'].str.title() 
data['postcode'] = data['address'].str.extract(r'NSW\s(\d+)')

# add timestamp
data['last_update'] = datetimestamp

# Select specific columns
selected_columns = data[['code','brand','name', 'address','street', 'town', 'postcode', 'location.latitude', 'location.longitude','last_update']]

# Renaming Columns
fuel_station_api = selected_columns.rename(columns={'code': 'stationid',
                            'location.latitude': 'latitude',
                            'location.longitude': 'longitude'})

# Changing the data type
fuel_station_api['stationid'] = fuel_station_api['stationid'].astype(str)

# Pull database Information
logger.info(f"Pulling Database Information")

# SQL query to fetch active stations
station_query = f"""
SELECT 
    stationid,
    brand,
    name, 
    address,
    street, 
    town, 
    postcode, 
    latitude, 
    longitude,
    last_update
FROM
    dim_fuel_station_dict
WHERE 
    active = True
"""

# Execute the query
station_fuelcode_dbo = pd.read_sql(station_query, engine)

# Create the datasets
logger.info("Creating the datasets")

# Dict is not in API
deleted = station_fuelcode_dbo[~station_fuelcode_dbo['stationid'].isin(fuel_station_api['stationid'])]

# API is not in Dict
new = fuel_station_api[~fuel_station_api['stationid'].isin(station_fuelcode_dbo['stationid'])]

# Name or Address has changed
updated = fuel_station_api.merge(station_fuelcode_dbo, on='stationid', suffixes=('_api', '_db'))
updated_stations = updated[(updated['name_api'] != updated['name_db']) | (updated['address_api'] != updated['address_db'])]
updated_stations = updated_stations[['stationid', 'brand_api', 'name_api', 'address_api', 'street_api','town_api', 'postcode_api', 'latitude_api', 'longitude_api']].rename(columns=lambda x: x.replace('_api', ''))
updated_stations['last_update'] = datetimestamp

# Truncate the staging tables
logger.info("Truncate the staging tables")

#call = text("CALL truncate_staging_station_tables();")
#with engine.connect() as conn:
#    conn = conn.execution_options(isolation_level="AUTOCOMMIT")
#    conn.execute(call)

# -------------------------------------------------------------------------------------------------
#                                       Insert into database
# -------------------------------------------------------------------------------------------------
logger.info("Inserting data into the database")

try:
    # Insert DataFrame into PostgreSQL
    with engine.connect() as connection:
        deleted.to_sql('staging_inactive_stations', connection, schema='dev', if_exists='append', index=False)
        new.to_sql('staging_new_stations', connection, schema='dev', if_exists='append', index=False)
        updated_stations.to_sql('staging_updated_stations', connection, schema='dev', if_exists='append', index=False)
except Exception as e:
    logger.exception(f"Unexpected error while inserting values into database: {e}")

#update the config 
config["last_API_call"] = datetimestamp
save_config()

logger.info("Operation complete")
