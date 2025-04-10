import os
import time
import json
import math
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
from concurrent.futures import ThreadPoolExecutor, as_completed
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
total_uploaded = 0
total_failed = 0


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




def get_netsuite_data_parallel(offset, query):
    """
    Fetches a batch of data from NetSuite for a given offset.
    """
    params = {
        "limit": str(LIMIT),
        "offset": str(offset)
    }
    try:
        response = get_netsuite_data(params, query)
        response.raise_for_status()
        return response.json().get("items", [])
    except Exception as e:
        print(f"Error fetching offset {offset}: {e}")
        return []

def load_data_to_bq_parallel(ns_table, schema, destination_table, num_threads=5):
    """
    Loads data into BigQuery by fetching from NetSuite in parallel.
    """
    columns = [field.name for field in schema]

    if "updated_at" in columns:
        columns.remove("updated_at")

    # Build query from table_name and columns
    query = {
        "q": f"SELECT {', '.join([item for item in columns])} FROM {ns_table}"
    }


    boolean_columns = {field.name for field in schema if field.field_type.upper() == 'BOOLEAN'}
    date_columns = {field.name for field in schema if field.field_type.upper() == 'DATE'}
    
    timestamp = time.time()
    offset = 0
    has_more = True
    batch_futures = []

    # Using ThreadPoolExecutor for parallel API requests
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        while has_more:
            # Submit multiple requests at the same time
            futures = {executor.submit(get_netsuite_data_parallel, offset + i * LIMIT, query): offset + i * LIMIT for i in range(num_threads)}
            
            for future in as_completed(futures):
                batch = future.result()
                
                if not batch:
                    has_more = False
                    continue
                
                # Process the batch
                for record in batch:
                    record.pop('links', None)
                    record['updated_at'] = timestamp
                    record = fix_integer_fields(record, integer_columns)  # Convert integer fields

                    for col in boolean_columns:
                        if col in record:
                            record[col] = fix_boolean(record[col])
                    for col in date_columns:
                        if col in record:
                            record[col] = fix_date_format(record[col])

                # Submit BigQuery upload in parallel
                batch_futures.append(executor.submit(upload_to_bigquery, destination_table, batch))

            offset += num_threads * LIMIT
            print("Set of batches complete. Offset: " + str(offset))

        # Wait for all BigQuery uploads to complete
        for future in as_completed(batch_futures):
            future.result()  # Ensure all uploads are completed successfully

    print(f"✅ Successfully loaded data into {destination_table} in parallel!")
    return True

def upload_to_bigquery(destination_table, batch):
    """
    Uploads a batch of data to BigQuery.
    """
    if not batch:
        return False
    errors = client.insert_rows_json(destination_table, batch)
    if errors:
        print(f"❌ Failed to load data into {destination_table}: {errors}")
        return False
    print(f"✅ Loaded {len(batch)} rows into {destination_table}")
    return True

def load_data_to_bq(query, schema, destination_table):
    
    # Extract BOOLEAN and TIMESTAMP columns dynamically from schema
    boolean_columns = {field.name for field in schema if field.field_type.upper() == 'BOOLEAN'}
    # timestamp_columns = {field.name for field in schema if field.field_type.upper() == 'TIMESTAMP'}
    date_columns = {field.name for field in schema if field.field_type.upper() == 'DATE'}
    # timestamp_columns.remove("updated_at")
    integer_columns = {field.name for field in schema if field.field_type.upper() == 'INTEGER'}

    
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

        batch_length = len(batch)  # Check the number of rows in the batch
        if batch_length == 0:
            print("No data to insert!")
            return False

        for record in batch:
            record.pop('links', None)
            record['updated_at'] = timestamp
            record = fix_integer_fields(record, integer_columns)  # Convert integer fields


            # Apply transformations dynamically
            for col in boolean_columns:
                if col in record:
                    record[col] = fix_boolean(record[col])

            """
            for col in timestamp_columns:
                if col in record:
                    record[col] = fix_timestamp_format(record[col])
            """

            for col in date_columns:
                if col in record:
                    record[col] = fix_date_format(record[col])


        errors = client.insert_rows_json(destination_table, batch)
        if not errors:
            print(f"✅ Successfully loaded {batch_length} rows into {destination_table}, Offset = {offset}")
        else:
            print(f"❌ Failed to load data into {destination_table}: {errors}")
            return False

    print(f" -------------- All Data Loaded to {destination_table} Successfully -------------- ")
    return True

def load_full_netsuite_table(ns_table, schema, destination_table):
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
    columns = [field.name for field in schema]

    if "updated_at" in columns:
        columns.remove("updated_at")

    # Build query from table_name and columns
    query = {
        "q": f"SELECT {', '.join([item for item in columns])} FROM {ns_table}"
    }
    
    return load_data_to_bq(query, schema, destination_table)

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



def fix_boolean(value):
    if value == "T":
        return True
    elif value == "F":
        return False
    return value  # Return as-is if it's already valid


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


def load_data_by_unique_key(ns_table, schema, destination_table, start_key, max_key, unique_id, num_threads=5):
    """
    Loads NetSuite data into BigQuery using unique_id based batching and concurrent futures.
    """
    global total_uploaded, total_failed  # Declare that we are using the global variables


    timestamp = time.time()
    boolean_columns = {field.name for field in schema if field.field_type.upper() == 'BOOLEAN'}
    date_columns = {field.name for field in schema if field.field_type.upper() == 'DATE'}

    columns = [field.name for field in schema]
    if "updated_at" in columns:
        columns.remove("updated_at")

    def fetch_and_upload_chunk(lower, upper):
        records_uploaded = 0
        records_failed = 0

        # create new client for each thread to fix concurrency issues
        client = bigquery.Client()

        query = {
            "q": f"SELECT {', '.join(columns)} FROM {ns_table} "
                 f"WHERE {unique_id} >= {lower} AND {unique_id} < {upper} "
                 f"ORDER BY {unique_id} ASC"
        }


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
                print(f"Reached end of chunk from {lower}-{upper}")
                has_more = False
            else:
                offset += LIMIT
                params["offset"] = str(offset)
            

            # Delete 'link' column for each record, which seems to be sent over empty no matter what
            batch = response_json.get("items", [])

            if len(batch) == 0:
                print(f"✅ No data with unique_ids between {lower}–{upper}")
                break

            for record in batch:
                record.pop('links', None)
                record['updated_at'] = timestamp

                for col in boolean_columns:
                    if col in record:
                        record[col] = fix_boolean(record[col])
                for col in date_columns:
                    if col in record:
                        record[col] = fix_date_format(record[col])
            errors = client.insert_rows_json(destination_table, batch)
            if errors:
                print(f"❌ Insert error for keys {lower}–{upper}, Offset = {offset}")
                for err in errors:
                    print(f"Error: {err}")
                records_failed += len(batch)
            else:
                print(f"✅ Successfully loaded {len(batch)} rows into {destination_table} for keys {lower}–{upper}, Offset = {offset - LIMIT}")
                records_uploaded += len(batch)

        return records_uploaded, records_failed

    # Use concurrent futures to process batches in parallel
    batch_futures = []
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        for key in range(start_key, max_key + 1, 100000):
            lower = key
            upper = min(key + 100000, max_key + 1)
            future = executor.submit(fetch_and_upload_chunk, lower, upper)
            batch_futures.append(future)

        for future in as_completed(batch_futures):
            uploaded, failed = future.result()
            total_uploaded += uploaded
            total_failed += failed

    print(f"🎉 All data between unique_id {start_key} and {max_key} has been loaded into {destination_table}.")
    print(f"✅ Total records uploaded: {total_uploaded}")
    print(f"❌ Total records failed: {total_failed}")

def get_max_unique_key(ns_table, unique_id):
    """
    Returns the maximum unique_id value from a NetSuite table.
    """

    params = {
        "limit": "1"
    }
    query = {
        "q": f"SELECT {unique_id} FROM {ns_table} ORDER BY {unique_id} DESC"
    }

    # We just need the first row since it's sorted in descending order
    results = get_netsuite_data(params, query).json().get("items", [])
    if results and len(results) > 0:
        return int(results[0][unique_id])

    raise Exception(f"❌ Could not fetch max {unique_id} from {ns_table}")

def get_min_unique_key(ns_table, unique_id):
    """
    Returns the maximum unique_id value from a NetSuite table.
    """

    params = {
        "limit": "1"
    }
    query = {
        "q": f"SELECT {unique_id} FROM {ns_table} ORDER BY {unique_id} ASC"
    }

    # We just need the first row since it's sorted in descending order
    results = get_netsuite_data(params, query).json().get("items", [])
    if results and len(results) > 0:
        return int(results[0][unique_id])

    raise Exception(f"❌ Could not fetch max {unique_id} from {ns_table}")


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


""" ------------ transaction table ETL script --------------"""
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
creds = Credentials.from_service_account_file(os.getenv("GCP_KEY_PATH"), scopes=SCOPES)
gspread_client = gspread.authorize(creds)


spreadsheet = gspread_client.open_by_key("1J05aKxbxBhgV8l3Tj4NxlTNHhkyqmZgNlohgkB8a7KU")
worksheet = spreadsheet.worksheet('transaction')
rows = worksheet.get_all_records()
schema = create_schema_from_sheet(rows)


# Get BigQuery table IDs
TRANSACTION_TABLE = f"{PROJECT_ID}.{DATASET_ID}.transaction"



drop_table(TRANSACTION_TABLE)
create_table(TRANSACTION_TABLE, schema)
wait_for_table_creation(TRANSACTION_TABLE)

# load all data into netsuite
start_time = time.time()
print("Start Time: " + str(start_time))


min_key = get_min_unique_key("transaction", unique_id='id')
max_key = get_max_unique_key("transaction", unique_id='id')

load_data_by_unique_key("transaction", schema, TRANSACTION_TABLE, min_key, max_key, unique_id='id', num_threads=10)


end_time = time.time()

print ("duration: " + str(end_time - start_time) + " seconds")


print("\n------------ Script Complete! ------------")


