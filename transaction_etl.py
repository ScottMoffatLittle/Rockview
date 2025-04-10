import os
import time
import json
import requests
import random
import string
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
import urllib.parse
import hmac
import hashlib
import base64
from dotenv import load_dotenv
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials

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


# Initialize BigQuery client
client = bigquery.Client()


# NetSuite API Endpoint
HTTP_METHOD = "POST"
BASE_URL = f"https://{NETSUITE_ACCOUNT_ID}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"

# NetSuite request row limit
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
        return response
    # Handle response error
    else:
        raise Exception(f"NetSuite API Error: {response.text}")
    


def load_data_to_bq(query, schema, destination_table):
    
    # Extract BOOLEAN and DATE columns dynamically from schema
    boolean_columns = {field.name for field in schema if field.field_type.upper() == 'BOOLEAN'}
    date_columns = {field.name for field in schema if field.field_type.upper() == 'DATE'}
    
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
            
        # Delete 'link' column for each record, which seems to be sent over empty no matter what
        batch = response_json.get("items", [])

        batch_length = len(batch)  # Check the number of rows in the batch
        if batch_length == 0:
            print("No data to insert!")
            return False

        for record in batch:
            record.pop('links', None)
            record['updated_at'] = timestamp

            # Apply transformations dynamically
            for col in boolean_columns:
                if col in record:
                    record[col] = fix_boolean(record[col])

            for col in date_columns:
                if col in record:
                    record[col] = fix_date_format(record[col])


        errors = client.insert_rows_json(destination_table, batch)
        if not errors:
            print(f"✅ Successfully loaded {batch_length} rows into {destination_table}, Offset = {offset}")
        else:
            print(f"❌ Failed to load data into {destination_table}: {errors}")
            return False

        # Reached end of data table
        if response_json.get("hasMore") == False:
            has_more = False
        # Table has more data, need to request new batch
        else:
            offset += LIMIT
            params["offset"] = str(offset)


    print(f" -------------- All Data Loaded to {destination_table} Successfully -------------- ")
    return True


# Get NetSuite data from the last 2 days, based on lastmodifieddate
# can maybe change this to 1 day
def load_recent_netsuite_data(ns_table, schema, destination_table):

    columns = [field.name for field in schema]

    if "updated_at" in columns:
        columns.remove("updated_at")

    # Get the current date and format it in MM/DD/YYYY
    current_date = datetime.now().strftime('%m/%d/%Y')

    # Build query with recent data condition
    query = {
        "q": f"""
        SELECT {', '.join([item for item in columns])} 
        FROM {ns_table}
        WHERE lastmodifieddate >= TO_DATE('{current_date}', 'MM/DD/YYYY') - 2
        """
    }
    
    return load_data_to_bq(query, schema, destination_table)



# merge data into BigQuery, so that existing rows are not duplicated
def merge_into_bigquery(target_table, staging_table, schema, unique_key="id"):
    """
    Merges data from a staging table into the target table using BigQuery's MERGE statement.

    Args:
        target_table (str): The full target table name (e.g., "your_project.your_dataset.target_table").
        staging_table (str): The full staging table name (e.g., "your_project.your_dataset.staging_table").
        columns (list): A list of column names to be merged.
        unique_key (str): The column that uniquely identifies a row (default is "id").
    """

    columns = [field.name for field in schema]
    
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


def fix_boolean(value):
    if value == "T":
        return True
    elif value == "F":
        return False
    return value  # Return as-is if it's already valid

def fix_timestamp_format(timestamp_str):
    """Fixes and formats the input timestamp string to 'YYYY-MM-DD HH:MM:SS'."""
    try:
        # Check if timestamp is in a different format and adjust as needed
        if len(timestamp_str) == 10:  # For date-only timestamps like '2022-01-01'
            return datetime.strptime(timestamp_str, "%Y-%m-%d").strftime("%Y-%m-%d %H:%M:%S")
        return datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None  # Return None if the format doesn't match


def fix_date_format(date_str):
    """Fixes and formats the input date string to 'YYYY-MM-DD'."""
    try:
        # Attempt to parse and format the date, checking for common formats like 'MM/DD/YYYY' and 'YYYY-MM-DD'
        date_obj = datetime.strptime(date_str, "%m/%d/%Y")
        return date_obj.strftime("%Y-%m-%d")
    except ValueError:
        return None  # Return None if the format doesn't match


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


def wait_for_table_creation(table_id, timeout=60):
    # Waits until the specified table exists in BigQuery.
    start_time = time.time()

    while time.time() - start_time < timeout:
        try:
            client.get_table(table_id)  # Check if the table exists
            print(f"✅ Table {table_id} is ready.")
            return
        except Exception:
            print(f"⏳ Waiting for table {table_id} to be created...")
            time.sleep(3)  # Wait 3 seconds before retrying

    raise TimeoutError(f"⛔ Table {table_id} did not appear within {timeout} seconds!")

def create_schema_from_sheet(gsheet_rows):
    schema = [
        bigquery.SchemaField(
            field['Column Name'],
            field['Data Type'],
            field['Nullable']
        )
        for field in gsheet_rows
    ]

    return schema



"""  ----------- transaction MERGE SCRIPT -----------  """
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
creds = Credentials.from_service_account_file(os.getenv("GCP_KEY_PATH"), scopes=SCOPES)
gspread_client = gspread.authorize(creds)

spreadsheet = gspread_client.open_by_key("1J05aKxbxBhgV8l3Tj4NxlTNHhkyqmZgNlohgkB8a7KU")
worksheet = spreadsheet.worksheet('transaction')
rows = worksheet.get_all_records()
schema = create_schema_from_sheet(rows)


# Get BigQuery table IDs
STAGING_DATASET_PATH = f"{PROJECT_ID}.{DATASET_ID_STAGING}"
TRANSACTION_TABLE = f"{PROJECT_ID}.{DATASET_ID}.transaction"
TRANSACTION_TABLE_STAGING = f"{PROJECT_ID}.{DATASET_ID_STAGING}.transaction_staging"


# create staging dataset, if it does not already exist
create_dataset(STAGING_DATASET_PATH)


# make transaction staging table based on schema
create_table(TRANSACTION_TABLE_STAGING, schema)
wait_for_table_creation(TRANSACTION_TABLE_STAGING)


# load data from ns transaction table, for specified columns, into bq staging table
# returns false if there is no new data
result = load_recent_netsuite_data("transaction", schema, TRANSACTION_TABLE_STAGING)

# If there is new data, Merge staging table into transaction table
# Otherwise, do not run the merge
if result: merge_into_bigquery(TRANSACTION_TABLE, TRANSACTION_TABLE_STAGING, schema, 'id')

# Delete staging table
drop_table(TRANSACTION_TABLE_STAGING)



print("\n------------ Script Complete! ------------")

