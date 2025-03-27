import os
import time
import json
import requests
import random
import string
from google.cloud import bigquery
import urllib.parse
import hmac
import hashlib
import base64
import dlt
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Load environment variables from secrets.env
load_dotenv("secrets.env")

# Authenticate with google cloud
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GCP_KEY_PATH")

# BigQuery account info
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID")
DATASET_ID_STAGING = os.getenv("DATASET_ID_STAGING")

# NetSuite access secrets
NETSUITE_ACCOUNT_ID = os.getenv("NETSUITE_ACCOUNT_ID")
NETSUITE_REALM = os.getenv("NETSUITE_REALM")
NETSUITE_CONSUMER_KEY = os.getenv("NETSUITE_CONSUMER_KEY")
NETSUITE_CONSUMER_SECRET = os.getenv("NETSUITE_CONSUMER_SECRET")
NETSUITE_TOKEN = os.getenv("NETSUITE_TOKEN")
NETSUITE_TOKEN_SECRET = os.getenv("NETSUITE_TOKEN_SECRET")

# Get BigQuery table IDs
ACCOUNT_TABLE = f"{PROJECT_ID}.{DATASET_ID}.Account"
ACCOUNT_TABLE_TEST = f"{PROJECT_ID}.{DATASET_ID}.Account_test"
ACCOUNT_TABLE_STAGING = f"{PROJECT_ID}.{DATASET_ID_STAGING}.Account_staging"
# TODO: Add the other tables here


ACCOUNT_SCHEMA = [
    bigquery.SchemaField("fullname", "STRING"),
    bigquery.SchemaField("acctnumber", "STRING"),
    bigquery.SchemaField("generalrate", "STRING"),
    bigquery.SchemaField("currency", "STRING"),
    bigquery.SchemaField("lastmodifieddate", "STRING"),
    bigquery.SchemaField("id", "STRING"),
    bigquery.SchemaField("accountsearchdisplayname", "STRING"),
    bigquery.SchemaField("accountsearchdisplaynamecopy", "STRING"),
    bigquery.SchemaField("isinactive", "STRING"),
    bigquery.SchemaField("updated_at", "TIMESTAMP"),
]

ACCOUNT_COLUMNS = ["fullname", "acctnumber", "generalrate", "currency", "lastmodifieddate", "id", "accountsearchdisplayname", "accountsearchdisplaynamecopy", "isinactive"]



# Initialize BigQuery client
client = bigquery.Client()


# NetSuite API Endpoint
HTTP_METHOD = "POST"
BASE_URL = f"https://{NETSUITE_ACCOUNT_ID}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"

LIMIT = 1000



def get_netsuite_data(params, query):
    """
    Input params in the form of:
    params = {
        "limit": "5",
        "offset": "0"
    }

    Input SQL query in the form of:
    query_body = {
        "q": "SELECT acctnumber, fullname, generalrate, currency FROM account"
    }

    """
    
    # Encode parameters for URL
    encoded_params = urllib.parse.urlencode(params)
    
    # Full URL with encoccquery params, used in post request
    url = f"{BASE_URL}?{encoded_params}"
    
    # Generate OAuth Parameters
    timestamp = str(int(time.time()))
    nonce = ''.join(random.choices(string.ascii_letters + string.digits, k=11))
    
    oauth_params = {
        "oauth_consumer_key": NETSUITE_CONSUMER_KEY,
        "oauth_token": NETSUITE_TOKEN,
        "oauth_signature_method": "HMAC-SHA256",
        "oauth_timestamp": timestamp,
        "oauth_nonce": nonce,
        "oauth_version": "1.0"
    }
    
    # Merge OAuth parameters with query parameters for signature
    all_params = {**oauth_params, **params}
    
    # Sort all parameters alphabetically (important for signature)
    sorted_params = sorted(all_params.items())
    encoded_param_string = "&".join([f"{urllib.parse.quote(k, safe='')}={urllib.parse.quote(v, safe='')}" for k, v in sorted_params])
    
    # Construct the Signature Base String
    base_string = f"{HTTP_METHOD}&{urllib.parse.quote(BASE_URL, safe='')}&{urllib.parse.quote(encoded_param_string, safe='')}"
    
    # Generate the HMAC-SHA256 Signature
    signing_key = f"{NETSUITE_CONSUMER_SECRET}&{NETSUITE_TOKEN_SECRET}"
    hashed = hmac.new(signing_key.encode(), base_string.encode(), hashlib.sha256)
    signature = base64.b64encode(hashed.digest()).decode()
    
    # URL Encode Signature Before Sending
    encoded_signature = urllib.parse.quote(signature, safe="")
    
    # Construct OAuth Header
    auth_header = (
        f'OAuth realm="{NETSUITE_REALM}", '
        f'oauth_consumer_key="{NETSUITE_CONSUMER_KEY}", '
        f'oauth_token="{NETSUITE_TOKEN}", '
        f'oauth_signature_method="HMAC-SHA256", '
        f'oauth_timestamp="{timestamp}", '
        f'oauth_nonce="{nonce}", '
        f'oauth_version="1.0", '
        f'oauth_signature="{encoded_signature}"'
    )
    
    # Headers
    headers = {
        "Authorization": auth_header,
        "Content-Type": "application/json",
        "Prefer": "transient",
        "Cache-Control": "no-cache",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }
    
    # Send Request
    response = requests.post(url, json=query, headers=headers)

    if response.status_code == 200:
        # return response.json().get("links", []), response.json().get("items", [])
        return response
    # Handle response error
    else:
        raise Exception(f"NetSuite API Error: {response.text}")
    
    # Check the response
    #print("\n🔹 Response Status Code:", response.status_code)
    #print("\n🔹 Response Body:", response.json())



def load_data_to_bq(query, DESTINATION_TABLE):
    #TODO: remove noted lines, and instead stream to bigQuery after each batch
    
    
    timestamp = time.time()

    # Initial parameters for request
    offset = 0
    params = {
        "limit": str(LIMIT),
        "offset": str(offset)
    }

    # Stop requesting data when this is false, and table has been fully retreived
    has_more = True
    
    while has_more:   
        
        response = get_netsuite_data(params, query)
        response.raise_for_status() # Make sure request was successful
        response_json = response.json()

        # Reached end of data table
        if response_json.get("hasMore") == False:
            has_more = False
        # Table has more data, need to request new batch
        else:
            offset += LIMIT
            params["offset"] = str(offset)
            

        # Delete 'link' column for each record, which seems to be sent over empty no matter what
        batch = response_json.get("items", [])
        for record in batch:
            if 'links' in record:
                del record['links']
            record['updated_at'] = timestamp


        batch_length = len(batch)  # Check the number of rows in the batch
        if batch_length == 0:
            print("No data to insert!")
            return False

        errors = client.insert_rows_json(DESTINATION_TABLE, batch)
        if not errors:
            print(f"✅ Successfully loaded {batch_length} rows into BigQuery.")
            return True
        else:
            print(f"❌ Failed to load data into BigQuery: {errors}")
            return False



def load_full_netsuite_table(table_name, columns):
    """
    Inputs:
        Name of table in netsuite: String
        List of netsuite columns to retreive: List[String]

    Functionality:
        Get LIMIT rows at a time, continuing until the entire table is retreived
        Filter based on input columns

    Returns:
        request object
        
    """

    # Build query from table_name and columns
    query = {
        "q": f"SELECT {', '.join([item for item in columns])} FROM {table_name}"
    }
    
    return load_data_to_bq(query)


# Get NetSuite data from the last 2 days, based on lastmodifieddate
# can maybe change this to 1 day
def load_recent_netsuite_data(ns_table, columns, destination_table):

    # Get the current date and format it in MM/DD/YYYY
    current_date = datetime.now().strftime('%m/%d/%Y')

    # Build query with recent data condition
    query = {
        "q": f"""
        SELECT {', '.join([item for item in columns])} 
        FROM {ns_table}
        WHERE lastmodifieddate >= TO_DATE('{current_date}', 'MM/DD/YYYY') - 200
        """
    }
    
    return load_data_to_bq(query, destination_table)



# merge data into BigQuery, so that existing rows are not duplicated
def merge_into_bigquery(target_table, staging_table, columns, unique_key="id"):
    """
    Merges data from a staging table into the target table using BigQuery's MERGE statement.

    Args:
        target_table (str): The full target table name (e.g., "your_project.your_dataset.target_table").
        staging_table (str): The full staging table name (e.g., "your_project.your_dataset.staging_table").
        columns (list): A list of column names to be merged.
        unique_key (str): The column that uniquely identifies a row (default is "id").
    """

    # Add our generated timestamp column to the list of merged columns
    columns.append("updated_at")
    
    # Construct update clause dynamically
    update_clause = ",\n                ".join([f"T.{col} = S.{col}" for col in columns])

    # Construct insert clause dynamically
    insert_columns = ", ".join(columns)
    insert_values = ", ".join([f"S.{col}" for col in columns])

    query = f"""
        MERGE `{target_table}` AS T
        USING `{staging_table}` AS S
        ON T.{unique_key} = S.{unique_key}
        
        WHEN MATCHED THEN 
            UPDATE SET
                {update_clause}
    
        WHEN NOT MATCHED THEN
            INSERT ({insert_columns})
            VALUES ({insert_values})
    """

    # Run the merge query
    try:
        job = client.query(query)
        job.result()  # Wait for the query to complete
        print(f"✅ Successfully merged data into {target_table}.")
    except Exception as e:
        print(f"❌ Failed to merge data into {target_table}: {e}")


# Input full dataset ID, including project ID
def create_dataset(dataset_id):
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    client.create_dataset(dataset, exists_ok=True)
    print(f"✅ Dataset {dataset_id} created successfully.")


# Input full table ID, including dataset ID and project ID
# Input table schema
def create_table(table_id, schema):
    table = bigquery.Table(table_id, schema=schema)
    client.create_table(table, exists_ok=True)
    print(f"✅ Table {table_id} created successfully.")



def drop_dataset(dataset_id):
    client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
    print(f"✅ Dataset {dataset_id} dropped successfully.")


def drop_table(table_id):    
    # Drop table
    client.delete_table(table_id, not_found_ok=True)
    print(f"✅ Table {table_id} dropped successfully.")



"""  ----------- TEST MERGE SCRIPT -----------  """
STAGING_DATASET_PATH = f"{PROJECT_ID}.{DATASET_ID_STAGING}"
print(STAGING_DATASET_PATH)


# delete account_test and account_staging tables
drop_table(ACCOUNT_TABLE_TEST)
drop_table(ACCOUNT_TABLE_STAGING)
time.sleep(5)

# delete staging dataset
drop_dataset(STAGING_DATASET_PATH)
time.sleep(5)

# create staging dataset
create_dataset(STAGING_DATASET_PATH)
time.sleep(5)

# make account test and account staging tables based on schema
create_table(ACCOUNT_TABLE_STAGING, ACCOUNT_SCHEMA)
create_table(ACCOUNT_TABLE_TEST, ACCOUNT_SCHEMA)
time.sleep(5)


# load data from ns account table, for specified columns, into bq staging table
result = load_recent_netsuite_data("account", ACCOUNT_COLUMNS, ACCOUNT_TABLE_STAGING)
time.sleep(5)

# Merge staging table into account table
if result: merge_into_bigquery(ACCOUNT_TABLE_TEST, ACCOUNT_TABLE_STAGING, ACCOUNT_COLUMNS)

# Delete staging dataset
drop_dataset(STAGING_DATASET_PATH)


print
print("\n\n------------ Script Complete! ------------")

