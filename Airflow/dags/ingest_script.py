import os
# import requests
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
from time import time
import numpy as np

def ingest_callable(user, password, host, port, dbname, table_name, parquet_file, path):
    
    print(table_name, parquet_file)


    
    # dataset_path = os.path.join("..", "Datasets")
    # file_name = os.path.join(dataset_path, "taxi_data.parquet",)
    # csv_file_name = os.path.join(dataset_path, "taxi_data.csv")

    #make path if not exists
    # os.makedirs(dataset_path, exist_ok=True)

    # csv_file = os.path.join(path, "csv_file_output.csv")
    # print(csv_file)

    try:
    # download the csv 
        # print("Downloading the Data")
        # response = requests.get(url)
        # with open(file_name, 'wb') as f:
        #     f.write(response.content)
        
        # print("Download Completed")
        # convert parquet to csv
        print("Entered the Try Block")
        parquet_df = pd.read_parquet(parquet_file)
        df_iter = np.array_split(parquet_df, len(parquet_df) // 100000 + 1)  # This ensures all fields are properly quoted
        

        # create engine
        print("Creating an engine")
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')
        print("Engine Created, Trying to Connect to Postgres Instance")
        engine.connect()
        print(engine.connect() + " --Succecfully connected")


        # read Parquet in chunks
        df = next(df_iter)
        # convert datetime columns to datetime
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        #create a schema using the header of the first chunk
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')


        #ingesting data chunk by chunk
        while True:

            
            t_start = time()

            df = next(df_iter)

            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            print("Ingesting the Data")
            df.to_sql(name=table_name, con=engine, if_exists='append')
                
            t_end = time()
            print('inserted chunk, took %.3f second' % ( t_end - t_start))


            
            print("Finished ingesting data into postgres")
            
    except Exception as e:
        print(f"An error occured: {e}")
        raise
