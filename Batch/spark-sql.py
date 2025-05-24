import pyspark
import argparse
from pyspark.sql import SparkSession, functions as F

'''
 Demo command to run the script: 
    python spark-sql.py \
    --input_green Dataset/pq/green/2020/*/ \
    --input_yellow Dataset/pq/yellow/2020/*/\
    --output Dataset/report/revenue/

    using Spark-submit:
    URL="spark://instance-20250421-081432.us-central1-c.c.de-zoomcamp-457507.internal:7077"
    spark-submit \
        --master="${URL}" \
        spark-sql.py \
        --input_green=gs://zoomcamp-de-taxi-data/pq/green/2020/*/ \
        --input_yellow=gs://zoomcamp-de-taxi-data/pq/yellow/2020/*/ \
        --output=gs://zoomcamp-de-taxi-data/report/revenue/



'''



parser = argparse.ArgumentParser()

parser.add_argument('--input_green', help='file path for green taxi data', required=True)
parser.add_argument('--input_yellow', help='file path for yellow taxi data', required=True)
parser.add_argument('--output', help='file path for output data', required=True)

args = parser.parse_args()

input_green = args.input_green
input_yellow = args.input_yellow
output = args.output

# Create a Spark Session
spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()


df_green = spark.read.parquet(input_green)

df_yellow = spark.read.parquet(input_yellow)

df_green = df_green \
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')


df_yellow = df_yellow \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')


common_columns = []

yellow_columns = set(df_yellow.columns)

for col in df_green.columns:
    if col in yellow_columns:
        common_columns.append(col)


df_green_sel = df_green \
    .select(common_columns) \
    .withColumn('service_type', F.lit('green'))

df_yellow_sel = df_yellow \
    .select(common_columns) \
    .withColumn('service_type', F.lit('yellow'))


df_trips_data = df_green_sel.unionAll(df_yellow_sel)
# print(f"Number of records in the combined DataFrame: {df_trips_data.count()}")

df_trips_data.createOrReplaceTempView('trips_data')

df_result = spark.sql("""
    SELECT 
    -- Revenue grouping 
    PULocationID AS revenue_zone,
    date_trunc("month", "pickup_datetime") AS revenue_month, 

    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,
    
    -- Additional calculations
    AVG(passenger_count) AS avg_monthly_passenger_count,
    AVG(trip_distance) AS avg_monthly_trip_distance

    FROM trips_data
    GROUP BY revenue_zone, revenue_month, service_type
""")


df_result.coalesce(1).write.parquet(output, mode='overwrite')
print("Data written to Dataset/report/revenue/")
# df_result.show(5, False)