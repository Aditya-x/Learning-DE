import os
import requests
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
from time import time


def ingest_callable(user, password, host, port, dbname, table_name, csv_file):
    
    print(table_name, csv_file)


    
    dataset_path = os.path.join("..", "Datasets")
    file_name = os.path.join(dataset_path, "taxi_data.parquet",)
    csv_file_name = os.path.join(dataset_path, "taxi_data.csv")

    #make path if not exists
    os.makedirs(dataset_path, exist_ok=True)

    try:
    # download the csv 
        # print("Downloading the Data")
        # #response = requests.get(url)
        # with open(file_name, 'wb') as f:
        #     f.write(response.content)
        
        print("Download Completed")
        # convert parquet to csv
        parquet_df = pd.read_parquet(file_name)
        parquet_df.to_csv(csv_file_name, index=False)
        csv_df = pd.read_csv(csv_file_name)

        # create engine
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')
        engine.connect()

        # read csv in chunks
        df_iter = pd.read_csv(csv_file_name, iterator=True, chunksize=100000)
        df = next(df_iter)
        # convert datetime columns to datetime
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        #create a schema using the header of the first chunk
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')


        #ingesting data chunk by chunk
        while True:

            try:
                t_start = time()

                df = next(df_iter)

                df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
                df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

                print("Ingesting the Data")
                df.to_sql(name=table_name, con=engine, if_exists='append')
                
                t_end = time()
                print('inserted chunk, took %.3f second' % ( t_end - t_start))


            except StopIteration:
                print("Finished ingesting data into postgres")
                break
    except Exception as e:
        print(f"An error occured: {e}")
        raise
