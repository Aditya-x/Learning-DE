import os
import requests
import pandas as pd
from pandas_gbq import to_gbq
from time import time
import argparse
from dotenv import load_dotenv
from google.oauth2 import service_account



def main(params):
    # BigQuery configuration
    load_dotenv()

    PROJECT_ID = os.getenv("GCP_PROJECT_ID")
    DATASET_ID = os.getenv("BQ_DATASET")
    TABLE_NAME = os.getenv("BQ_TABLE")
    # URL of the parquet file
    url = params.url

    # Define file paths
    dataset_path = os.path.join("..", "Datasets")
    file_name = os.path.join(dataset_path, "taxi_zone_lookup.csv")
    # csv_file_name = os.path.join(dataset_path, "taxi_data.csv")

    # Make sure the dataset directory exists
    os.makedirs(dataset_path, exist_ok=True)

    try:
        # Download the parquet file
        print("Downloading the Data")
        response = requests.get(url)
        with open(file_name, 'wb') as f:
            f.write(response.content)
        print("Download Completed")

        # Convert parquet to CSV
        # print("Converting Parquet to CSV...")
        # parquet_df = pd.read_parquet(file_name)
        # parquet_df.to_csv(csv_file_name, index=False)
        # print("Conversion Completed")

        # Read CSV in chunks
        df_iter = pd.read_csv(file_name, iterator=True, chunksize=100)
        first_chunk = True  # Flag to control replace vs. append in BigQuery

        # Build full table ID for BigQuery
        credentials = service_account.Credentials.from_service_account_file("keys/bigq-dbt.json")
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"

        # Process each chunk
        while True:
            try:
                t_start = time()
                df = next(df_iter)

                # Convert datetime columns if they exist
                # if 'tpep_pickup_datetime' in df.columns:
                #     df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
                # if 'tpep_dropoff_datetime' in df.columns:
                #     df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

                # Load the chunk into BigQuery
                if first_chunk:
                    to_gbq(df, table_id, project_id=PROJECT_ID, if_exists="replace", credentials=credentials)
                    first_chunk = False
                else:
                    to_gbq(df, table_id, project_id=PROJECT_ID, if_exists="append", credentials=credentials)

                t_end = time()
                print('Inserted chunk, took %.3f seconds' % (t_end - t_start))
            except StopIteration:
                print("Finished ingesting data into BigQuery")
                break

    except Exception as e:
        print(f"An error occurred: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest data to BigQuery')
    parser.add_argument('--url', help='URL of the parquet file to download', required=True)
    args = parser.parse_args()
    main(args)
