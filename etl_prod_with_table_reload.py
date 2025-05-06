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
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load environment variables from secrets.env
load_dotenv("secrets_prod.env")

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

# Gsheet spreasheet key
GSHEET_KEY = os.getenv("GSHEET_KEY")


# Initialize BigQuery client
client = bigquery.Client()


# NetSuite API Endpoint
HTTP_METHOD = "POST"
BASE_URL = f"https://{NETSUITE_ACCOUNT_ID}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"

# NetSuite request row limit
LIMIT = 1000
total_uploaded = 0
total_failed = 0

TABLE_CONFIGS = {
    'Account': {'unique_key': 'id', 'date_col': 'lastmodifieddate'},
    'transactionLine': {'unique_key': 'uniquekey', 'date_col': 'linelastmodifieddate'},
    'AccountingPeriod': {'unique_key': 'id', 'date_col': 'lastmodifieddate'},
    'CUSTOMRECORD_360_COMMISSION_RULE': {'unique_key': 'id', 'date_col': 'lastmodified'},
    'CUSTOMRECORD_360_COMMISSION_TRACKING': {'unique_key': 'id', 'date_col': 'lastmodified'},
    'Quota': {'unique_key': 'id', 'date_col': 'date'},
    'classification': {'unique_key': 'id', 'date_col': 'lastmodifieddate'},
    'employee': {'unique_key': 'id', 'date_col': 'lastmodifieddate'},
    'entity': {'unique_key': 'id', 'date_col': 'lastmodifieddate'},
    'item': {'unique_key': 'id', 'date_col': 'lastmodifieddate'},
    'transaction': {'unique_key': 'id', 'date_col': 'lastmodifieddate'},
    'invoiceSalesTeam': {'unique_key': 'id', 'date_col': 'lastmodifieddate', 'batch_key': 'transaction', 'batch_range': 100000},
    'transactionSalesTeam': {'unique_key': 'id', 'date_col': 'lastmodifieddate', 'batch_key': 'transaction', 'batch_range': 100000}
}

BQ_TYPE_MAP = {
    "FLOAT": "FLOAT64",
    "STRING": "STRING",
    "INTEGER": "INT64",
    "BOOLEAN": "BOOL",
    "DATE": "DATE",
    "DATETIME": "DATETIME",
    "TIMESTAMP": "TIMESTAMP"
}

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
    

def load_full_netsuite_table(ns_table, schema, destination_table):
    # Load full netsuite table into BQ, not accounting for 100,000 record limit or using parallel loads

    columns = [field.name for field in schema]

    if "updated_at" in columns:
        columns.remove("updated_at")

    # Build query from table_name and columns
    query = {
        "q": f"SELECT {', '.join([item for item in columns])} FROM {ns_table}"
    }
    
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


        success = load_with_retries(destination_table, batch)

        if success:
            print(f"✅ Loaded {len(batch)} rows at offset {params['offset']}")
        else:
            print(f"❌ Insert error at offset {params['offset']}: {errors}")
            return False

        # Reached end of data table
        if response_json.get("hasMore") == False:
            has_more = False
        # Table has more data, need to request new batch
        else:
            offset += LIMIT
            params["offset"] = str(offset)


    print(f"All Data Loaded to {destination_table} Successfully")
    return True

def load_data_to_bq_parallel(query, schema, destination_table, total_records, num_threads=15):
    timestamp = time.time()
    offsets = list(range(0, total_records, LIMIT))
    print(f"🚀 Starting parallel load with {len(offsets)} batches")

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for offset in offsets:
            params = {"limit": str(LIMIT), "offset": str(offset)}
            futures.append(executor.submit(load_data_batch, params, query, schema, destination_table, timestamp))

        results = [f.result() for f in as_completed(futures)]

    if all(results):
        print(f"🎉 All data loaded into {destination_table} successfully!")
        return True
    else:
        print(f"⚠️ Some batches failed to load.")
        return False


def get_netsuite_data_with_retry(params, query, max_retries=5):
    delay = 1
    for attempt in range(max_retries):
        response = get_netsuite_data(params, query)
        if response.status_code == 429:
            print(f"429 Too Many Requests. Retrying in {delay} seconds...")
            time.sleep(delay)
            delay *= 2  # Exponential backoff
        else:
            response.raise_for_status()
            return response.json().get("items", [])
    raise Exception("Exceeded max retries due to concurrency limit.")


def load_data_batch(params, query, schema, destination_table, timestamp):
    boolean_columns = {field.name for field in schema if field.field_type.upper() == 'BOOLEAN'}
    date_columns = {field.name for field in schema if field.field_type.upper() == 'DATE'}

    batch = get_netsuite_data_with_retry(params, query)

    for record in batch:
        record.pop('links', None)
        record['updated_at'] = timestamp

        for col in boolean_columns:
            if col in record:
                record[col] = fix_boolean(record[col])
        for col in date_columns:
            if col in record:
                record[col] = fix_date_format(record[col])

    success = load_with_retries(destination_table, batch)

    if success:
        print(f"✅ Loaded {len(batch)} rows at offset {params['offset']}")
        return True
    else:
        print(f"❌ Insert error at offset {params['offset']}: {errors}")
        return False


def get_total_record_count(query):
    # Extract the WHERE clause from the original query
    where_clause = query["q"].split("WHERE", 1)[-1].strip()

    count_query = {
        "q": f"SELECT COUNT(*) as count FROM {query['q'].split('FROM')[1].split('WHERE')[0].strip()} WHERE {where_clause}"
    }

    response = get_netsuite_data({}, count_query)
    response.raise_for_status()

    items = response.json().get("items", [])
    if not items:
        return 0

    count = int(items[0]["count"])
    return count


# Get NetSuite data from the last 2 days, based on lastmodifieddate
def load_recent_netsuite_data(ns_table, schema, destination_table, date_col):

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
        WHERE {date_col} >= TO_DATE('{current_date}', 'MM/DD/YYYY') - 2
        """
    }
    
    record_count = get_total_record_count(query)
    return load_data_to_bq_parallel(query, schema, destination_table, record_count)


def load_data_by_unique_key(ns_table, schema, destination_table, start_key, max_key, unique_id, batch_range, num_threads=5):
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
            
            offset += LIMIT
            params["offset"] = str(offset)
            

            # Delete 'link' column for each record, which seems to be sent over empty no matter what
            batch = response_json.get("items", [])

            if len(batch) == 0:
                print(f"✅ No data with {unique_id}s between {lower}–{upper}")
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
                print(f"❌ Insert error for keys {lower}–{upper}, Offset = {offset - LIMIT}")
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
        for key in range(start_key, max_key + 1, batch_range):
            lower = key
            upper = min(key + batch_range, max_key + 1)
            future = executor.submit(fetch_and_upload_chunk, lower, upper)
            batch_futures.append(future)

        for future in as_completed(batch_futures):
            uploaded, failed = future.result()
            total_uploaded += uploaded
            total_failed += failed

    print(f"🎉 All data between {unique_id} {start_key} and {max_key} has been loaded into {destination_table}.")
    print(f"✅ Total records uploaded: {total_uploaded}")
    print(f"❌ Total records failed: {total_failed}")
    total_uploaded = 0
    total_failed = 0


# merge data into BigQuery, so that existing rows are not duplicated
def merge_into_bigquery(target_table, staging_table, schema, unique_key, table_name, date_col):
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


    query = ""

    # Quota table has no unique ID, need to go off both ID and date to find unique rows
    if table_name == 'Quota':
        query = f"""
        MERGE `{target_table}` AS T
        USING `{staging_table}` AS S
        ON T.{unique_key} = S.{unique_key}
           AND T.{date_col} = S.{date_col}

        WHEN MATCHED THEN 
            UPDATE SET
                {update_clause}

        WHEN NOT MATCHED THEN
            INSERT ({insert_columns})
            VALUES ({insert_values})
        """
    # query to de-duplicate and merge records
    else:
        query = f"""
            MERGE `{target_table}` AS T
            USING (
                SELECT *
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY {unique_key}
                               ORDER BY {date_col} DESC
                           ) AS row_num
                    FROM `{staging_table}`
                )
                WHERE row_num = 1
            ) AS S
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


def load_with_retries(table_id, rows, max_retries=5, delay=3):
    for attempt in range(max_retries):
        try:
            errors = client.insert_rows_json(table_id, rows)
            if not errors:
                return True
            else:
                print(f"⚠️ Insert errors: {errors}")
        except Exception as e:
            print(f"❌ Exception during insert (attempt {attempt + 1}): {e}")
        
        print(f"⏳ Retry insert in {delay} seconds...")
        time.sleep(delay)
    
    raise RuntimeError(f"Failed to insert after {max_retries} attempts.")
    return False


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

def wait_for_table_creation(table_id, timeout=60):
    # Waits until the specified table exists in BigQuery.
    start_time = time.time()
    time.sleep(5)

    while time.time() - start_time < timeout:
        try:
            client.get_table(table_id)  # Check if the table exists
            print(f"✅ Table {table_id} is ready.")
            return
        except Exception:
            print(f"⏳ Waiting for table {table_id} to be created...")
            time.sleep(3)  # Wait 3 seconds before retrying

    raise TimeoutError(f"⛔ Table {table_id} did not appear within {timeout} seconds!")


def wait_for_table_dropping(table_id, timeout=60):
    """Waits until the specified table is dropped in BigQuery."""
    start_time = time.time()
    time.sleep(3)

    while time.time() - start_time < timeout:
        try:
            # Check if the table exists
            client.get_table(table_id)
            print(f"⏳ Table {table_id} is still present, waiting for drop...")
            time.sleep(3)  # Wait for 3 seconds before retrying
        except Exception:
            # If the table does not exist, it's dropped
            print(f"✅ Table {table_id} is successfully dropped.")
            return

    raise TimeoutError(f"⛔ Table {table_id} was not dropped within {timeout} seconds!")

def wait_for_dataset_creation(dataset_id, timeout=60):
    """
    Waits until the specified dataset exists in BigQuery.
    """
    start_time = time.time()
    time.sleep(15)

    while time.time() - start_time < timeout:
        try:
            client.get_dataset(dataset_id)  # Check if the dataset exists
            print(f"✅ Dataset {dataset_id} is ready.")
            return
        except Exception:
            print(f"⏳ Waiting for dataset {dataset_id} to be created...")
            time.sleep(3)

    raise TimeoutError(f"⛔ Dataset {dataset_id} did not appear within {timeout} seconds!")

def wait_for_dataset_dropping(dataset_id, timeout=60):
    """
    Waits until the specified dataset is dropped in BigQuery.
    """
    start_time = time.time()
    time.sleep(15)

    while time.time() - start_time < timeout:
        try:
            client.get_dataset(dataset_id)  # Check if the dataset exists
            print(f"⏳ Dataset {dataset_id} is still present, waiting for drop...")
            time.sleep(3)
        except Exception:
            print(f"✅ Dataset {dataset_id} is successfully dropped.")
            return

    raise TimeoutError(f"⛔ Dataset {dataset_id} was not dropped within {timeout} seconds!")


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

def sync_table_schema(target_table, sheet_schema):
    # Fetch current BigQuery schema
    bq_table = client.get_table(target_table)
    bq_fields = {field.name for field in bq_table.schema}
    sheet_fields = {field.name for field in sheet_schema}

    # Find fields present in sheet but missing in BQ
    missing_fields = sheet_fields - bq_fields

    if missing_fields:
        print(f"🆕 New columns detected in schema: {missing_fields}")

        for field in sheet_schema:
            if field.name in missing_fields:
                field_type = BQ_TYPE_MAP.get(field.field_type.upper(), field.field_type)
                alter_query = f"""
                    ALTER TABLE `{target_table}`
                    ADD COLUMN {field.name} {field_type}
                """
                print(f"📐 Executing: {alter_query.strip()}")
                client.query(alter_query).result()

        # give time for table to update
        time.sleep(60)

    return list(missing_fields)


def backfill_columns(ns_table, target_table, sheet_schema, unique_key, date_col, batch_key, batch_range, new_columns):
    # create query to grab from each row in netsuite: the id and new columns, and insert the into BigQuery
    # Use the load_data_by_unique_key(ns_table, schema, destination_table, start_key, max_key, unique_id, batch_range, num_threads=5) method to insert to bigquery

    min_key = get_min_unique_key(ns_table, unique_key)
    max_key = get_max_unique_key(ns_table, unique_key)

    backfill_schema = ""
    if ns_table == 'Quota':
        backfill_schema = [field for field in sheet_schema if field.name in new_columns or field.name == unique_key or field.name == 'date']
    else:
        backfill_schema = [field for field in sheet_schema if field.name in new_columns or field.name == unique_key]

    load_data_by_unique_key(ns_table, backfill_schema, target_table, min_key, max_key, batch_key, batch_range, num_threads=10)


def replace_bq_table(target_table, staging_table):
    replace_query = f"""
        CREATE OR REPLACE TABLE {target_table} AS
        SELECT * FROM {staging_table};
    """

    try:
        job = client.query(replace_query)
        job.result()  # Wait for the query to complete
        print(f"✅ Successfully replaced data in {target_table}.")
    except Exception as e:
        print(f"❌ Failed to replace data in {target_table}: {e}")

def wait_for_streaming_data():
    time.sleep(30)
    return


""" ------------ Prod ETL script for all tables --------------"""
# create staging dataset, if it does not already exist
STAGING_DATASET = f"{PROJECT_ID}.{DATASET_ID_STAGING}"
drop_dataset(STAGING_DATASET)
wait_for_dataset_dropping(STAGING_DATASET)
create_dataset(STAGING_DATASET)
wait_for_dataset_creation(STAGING_DATASET)


SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_file(os.getenv("GCP_KEY_PATH"), scopes=SCOPES)
gspread_client = gspread.authorize(creds)
spreadsheet = gspread_client.open_by_key(GSHEET_KEY)

SYNC_MODES = {}
sync_worksheet = ""
sync_rows = ""

for sheet in spreadsheet.worksheets():
    table_name = sheet.title
    unique_key = ""
    date_col = ""
    batch_key = ""
    batch_range = 100000


    if table_name == "Table Sync Mode":
        sync_worksheet = spreadsheet.worksheet(table_name)
        sync_rows = sync_worksheet.get_all_records()
        SYNC_MODES = {
            row['Table_Name']: row['Sync_Mode']
            for row in sync_rows if row['Table_Name'] and row['Sync_Mode']
        }
        print('Sync Modes Retrieved')
        continue    # Sync modes retrieved, go onto the next table


    if table_name not in TABLE_CONFIGS:
        continue    # Table not in table configs, go to next GSheet table

    print('Start update for: ' + table_name)

    # Get unique key, date_field from config
    config = TABLE_CONFIGS[table_name]
    unique_key = config['unique_key']
    date_col = config['date_col']

    if 'batch_key' in config:
        batch_key = config['batch_key']
        batch_range = config['batch_range']
    else:
        batch_key = unique_key
        

    # Get schema from gsheet
    worksheet = spreadsheet.worksheet(table_name)
    rows = worksheet.get_all_records()
    schema = create_schema_from_sheet(rows)

    # Get BigQuery table IDs
    bq_table = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    staging_table = f"{PROJECT_ID}.{DATASET_ID_STAGING}.{table_name}_staging"

    # make staging table based on schema
    create_table(staging_table, schema)
    wait_for_table_creation(staging_table)

    # Get sync mode for current table. If table name is not found in GSheet, default to use periodic_refresh
    try:
        sync_mode = SYNC_MODES[table_name]
    except:
        print(f"Error in 'Table Sync Mode' GSheet, {table_name} not found. Defaulting to use Sync Mode: periodic_refresh")
        sync_mode = "periodic_refresh"

    if sync_mode == "periodic_refresh"

        # Check if there are new fields in the GSheet. if so, add them to the prod bq_table
        new_columns = sync_table_schema(bq_table, schema)

        # If there are new GSheet columns, backfill their data into BigQuery
        if new_columns:
            backfill_columns(table_name, staging_table, schema, unique_key, date_col, batch_key, batch_range, new_columns)
            merge_into_bigquery(bq_table, staging_table, schema, unique_key, table_name, date_col)

        # load data from ns transaction table, for specified columns, into bq staging table
        # returns false if there is no new data
        else:
            result = load_recent_netsuite_data(table_name, schema, staging_table, date_col)

            # If there is new data, Merge staging table into transaction table
            # Otherwise, do not run the merge
            if result: merge_into_bigquery(bq_table, staging_table, schema, unique_key, table_name, date_col)

        drop_table(staging_table)
        print ("--- Periodic Update Complete for Table: " + table_name + " ----")

    elif sync_mode in ["reload_once", "reload_always"]:

        if table_name == "Quota":
            # Quota table must be loaded in a different way, since it does not contain unique IDs
            load_full_netsuite_table(table_name, schema, bq_table)
        else:
            min_key = get_min_unique_key(table_name, unique_key)
            max_key = get_max_unique_key(table_name, unique_key)

            # load all NS data into BQ staging table
            load_data_by_unique_key(table_name, schema, staging_table, min_key, max_key, unique_key, batch_range, num_threads=10)

            # Replace existing table with staging table
            wait_for_streaming_data()
            replace_bq_table(bq_table, staging_table)

        # Edit GSheet to change reload_once to periodic_refresh
        if sync_mode == "reload_once":
            updated = False
            # Loop through rows to find the matching table name
            for i, row in enumerate(sync_rows, start=2):  # start=2 to account for header
                if row["Table_Name"] == table_name:
                    sync_worksheet.update_cell(i, 2, "periodic_refresh")
                    print(f"Updated Sync Mode from reload_once to periodic_refresh for table: {table_name}")
                    updated = True
                    break
            if not updated:
                print(f"Unable to update Sync Mode for table: {table_name}, table name not found")


    else:
        print(f"Sync Mode for {table_name} not found or incorrectly spelled")

drop_dataset(STAGING_DATASET)


print("------------ Full Update Complete! ------------")


