import functions_framework
import pandas as pd
import requests
from io import StringIO
from datetime import datetime
from google.cloud import storage

@functions_framework.http
def data_generate(request):

    symbols = ['AAPL', 'AMZN', 'GOOGL', 'TSLA', 'META', 'NVDA', 'CVX', 'WMT', 'DIS']  # can add more but i am using free api service so less company 


    all_dfs = []
    rows = []

    for symbol in symbols:
        url =f'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY_ADJUSTED&symbol={symbol}&datatype=csv&apikey=9UPZG2UB673LBBWC'
        res = requests.get(url)

        if res.status_code == 200:
            df = pd.read_csv(StringIO(res.text))
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce").dt.date
            df['company'] = symbol
            df['load_date'] = datetime.today().strftime('%Y-%m-%d')
            df["load_date"] = pd.to_datetime(df["load_date"], errors="coerce").dt.date
            df.drop('dividend amount', axis=1, inplace=True)
            all_dfs.append(df)
        else:
            print(f"Failed to fetch data for {symbol}")

        url1 = f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={symbol}&apikey=M0RBPJ69RHSX7P13'
        r = requests.get(url1)
        data = r.json()

        if "Symbol" in data:
            row = (
                data.get("Symbol"),
                data.get("AssetType"),
                data.get("Name"),
                data.get("Description"),
                data.get("Exchange"),
                data.get("Currency"),
                data.get("Country"),
                data.get("Sector"),
                data.get("Industry"),
                data.get("MarketCapitalization"),
                datetime.today().strftime('%Y-%m-%d')
            )
            rows.append(row)
    d = pd.DataFrame(rows, columns=[
        "Symbol", "AssetType", "Name", "Description", "Exchange",
        "Currency", "Country", "Sector", "Industry", "MarketCapitalization","load_date"
        ])
    d["load_date"] = pd.to_datetime(d["load_date"], errors="coerce").dt.date
    
    csv_company = d.to_csv(index=False)

    combined_df = pd.concat(all_dfs, ignore_index=True)
    csv_data = combined_df.to_csv(index=False)

    #GCS Ingestion Start

    bucket_name = 'dbt-project-stocks'
    destination_blob_name1 =f'stock_data/combined_data_{datetime.today().strftime("%Y%m%d")}.csv'
    destination_blob_name2 =f'company_data/combined_data_{datetime.today().strftime("%Y%m%d")}.csv'


    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    blob1 = bucket.blob(destination_blob_name1)
    blob1.upload_from_string(csv_data, content_type='text/csv')

    blob2 = bucket.blob(destination_blob_name2)
    blob2.upload_from_string(csv_company, content_type='text/csv')

    return f"Uploaded {len(combined_df)} stock records and {len(d)} company profiles to GCS."
