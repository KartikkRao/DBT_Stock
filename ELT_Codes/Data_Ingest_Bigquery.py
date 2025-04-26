# This Cloud Run function is triggered by GCS when new object that is in my case csv file is created in bucket 
# Remember to add imports in the requirement.txt otherwise you will get import error also addition add pyarrow in requirements it is used internally in pandas bigquery connection 

import functions_framework
from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
import os
import io

# Set these constants (replace with your actual values)
PROJECT_ID = "dbt-project-457215"
DATASET_ID = "stocks_data"
STOCK_TABLE = "stock"
COMPANY_TABLE = "company"
flag = 0

@functions_framework.cloud_event
def hello_gcs(cloud_event):
    data = cloud_event.data
    file_name = data["name"]
    bucket_name = data["bucket"]

    print(f"New file uploaded: {file_name} in bucket: {bucket_name}")

    # Check which folder it belongs to
    if file_name.startswith("stock_data/"):
        table_name = STOCK_TABLE
        flag = 1
    elif file_name.startswith("company_data/"):
        table_name = COMPANY_TABLE
        flag = 2
    else:
        print("File not in stock_data/ or company_data/ folder. Skipping.")
        return

    # Download the file from GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    # Download as string and load into pandas
    csv_data = blob.download_as_text()
    df = pd.read_csv(io.StringIO(csv_data)) # Handles in-memory reading

    if flag == 1:
        df["load_date"] = pd.to_datetime(df["load_date"], errors="coerce").dt.date
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce").dt.date
        df['close'] = df['close'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['open'] = df['open'].astype(float)
        df['adjusted close'] = df['adjusted close'].astype(float)
        df['volume'] = df['volume'].astype(int)
    elif flag == 2:
        df["load_date"] = pd.to_datetime(df["load_date"], errors="coerce").dt.date
        df['MarketCapitalization'] = df['MarketCapitalization'].astype(int)
        df["Symbol"] = df["Symbol"].astype(str)
        df["AssetType"] = df["AssetType"].astype(str)
        df["Name"] = df["Name"].astype(str)
        df["Description"] = df["Description"].astype(str)
        df["Exchange"] = df["Exchange"].astype(str)
        df["Currency"] = df["Currency"].astype(str)
        df["Country"] = df["Country"].astype(str)
        df["Sector"] = df["Sector"].astype(str)
        df["Industry"] = df["Industry"].astype(str)
    else:
        print("something went wrong")


    # Load into BigQuery
    bq_client = bigquery.Client()
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

    job = bq_client.load_table_from_dataframe(df, table_id)
    job.result()  # Wait for the job to complete

    print(f"Loaded {len(df)} rows into {table_id}.")
