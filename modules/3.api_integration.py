#Import packages
import requests
from datetime import datetime, timezone
import pandas as pd
import uuid
from sqlalchemy import create_engine, text


# Load environment variables from GitHub Secrets
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
API_AUTHORISATION_HEADER = os.getenv("API_AUTHORISATION_HEADER")

# Authentification Information
access_token_URL = "https://api.onegov.nsw.gov.au/oauth/client_credential/accesstoken"
dict_url = "https://api.onegov.nsw.gov.au/FuelCheckRefData/v2/fuel/lovs"

#universal variables
now = datetime.now(timezone.utc).strftime("%Y-%m-%d %I:%M:%S %p")
unique_id = str(uuid.uuid4())

# -------------------------------------------------------------------------------------------------
#                                       Define Functions
# -------------------------------------------------------------------------------------------------

# Create an Access Token
def create_access_token(url, authorisation_header):
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
        auth_response.raise_for_status()  # Raise exception for HTTP errors
        
        # Parse JSON response
        data = auth_response.json()
        return data.get("access_token", None)
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None


def api_data(url,access_token,API_key):

    headers = {
    'content-type': "application/json",
    'authorization': f"Bearer {access_token}",
    'apikey': API_key,
    'transactionid': unique_id,
    'requesttimestamp': now,
    'if-modified-since': now
    }
    try:
        response = requests.get(dict_url, headers=headers)
        response.raise_for_status()  # Raise exception for HTTP errors

        # Parse JSON response
        data = response.json()  
        stations = data["stations"]["items"]
        return pd.json_normalize(stations)
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None

# -------------------------------------------------------------------------------------------------
#                                       Pull API Information
# -------------------------------------------------------------------------------------------------

# Generate the access token and access the data
token = create_access_token(access_token_URL,authorisation_header)
data = api_data(dict_url,token,API_key)

# Create the new address columns
data['street'] = data['address'].str.extract(r'((?:\d+|Corner|Cnr).+?),')
data['street'] = data['street'].str.title()
data['town'] = data['address'].str.extract(r',\s(\D+)\sNSW\s\d+')
data['town'] = data['town'].str.title() 
data['postcode'] = data['address'].str.extract(r'NSW\s(\d+)')

# add timestamp
data['last_update'] = now

# Select specific columns
selected_columns = data[['code','brand','name', 'address','street', 'town', 'postcode', 'location.latitude', 'location.longitude','last_update']]

# Renaming Columns
fuel_station_api = selected_columns.rename(columns={'code': 'stationid',
                            'location.latitude': 'latitude',
                            'location.longitude': 'longitude'})

# Changing the data type
fuel_station_api['stationid'] = fuel_station_api['stationid'].astype(str)

# -------------------------------------------------------------------------------------------------
#                                       Pull database Information
# -------------------------------------------------------------------------------------------------
print("Pulling database Information")

# Database connection string
db_url = "postgresql+psycopg2://paul:postgres@localhost:5432/fuelcheck"

# Create the database engine
engine = create_engine(db_url)

# Active stations
call = text("SELECT stationid,brand,name, address,street, town, postcode, latitude, longitude,last_update \
            FROM dev.fuel_station_dict\
            WHERE active = True")
try:
    # Fetch data from PostgreSQL
    with engine.connect() as conn:
        result = conn.execute(call)
        # Convert the result to a DataFrame
        fuel_station_dict = pd.DataFrame(result.fetchall())
except Exception as e:
    print(f"Error: {e}")

# -------------------------------------------------------------------------------------------------
#                                       Create the datasets
# -------------------------------------------------------------------------------------------------
print("Creating the datasets")

# Dict is not in API
deleted = fuel_station_dict[~fuel_station_dict['stationid'].isin(fuel_station_api['stationid'])]

# API is not in Dict
new = fuel_station_api[~fuel_station_api['stationid'].isin(fuel_station_dict['stationid'])]

# Name or Address has changed
updated = fuel_station_api.merge(fuel_station_dict, on='stationid', suffixes=('_api', '_db'))
updated_stations = updated[(updated['name_api'] != updated['name_db']) | (updated['address_api'] != updated['address_db'])]
updated_stations = updated_stations[['stationid', 'brand_api', 'name_api', 'address_api', 'street_api','town_api', 'postcode_api', 'latitude_api', 'longitude_api']].rename(columns=lambda x: x.replace('_api', ''))
updated_stations['last_update'] = now

# -------------------------------------------------------------------------------------------------
#                                       Truncate the temp tables
# -------------------------------------------------------------------------------------------------
print("Truncating temp tables")

call = text("CALL dev.truncate_temp_station_tables();")
with engine.connect() as conn:
    conn = conn.execution_options(isolation_level="AUTOCOMMIT")
    conn.execute(call)

# -------------------------------------------------------------------------------------------------
#                                       Insert into database
# -------------------------------------------------------------------------------------------------
print("Inserting data into the database")

try:
    # Insert DataFrame into PostgreSQL
    with engine.connect() as connection:
        deleted.to_sql('temp_inactive_stations', connection, schema='dev', if_exists='append', index=False)
        new.to_sql('temp_new_stations', connection, schema='dev', if_exists='append', index=False)
        updated_stations.to_sql('temp_updated_stations', connection, schema='dev', if_exists='append', index=False)
except Exception as e:
    print(f"error: {e}")

print("Operation complete")
