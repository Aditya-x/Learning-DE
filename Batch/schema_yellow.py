types.StructType([
    types.StructField('VendorID', types.IntegerType(), True), 
    types.StructField('tpep_pickup_datetime', types.TimestampType(), True), 
    types.StructField('tpep_dropoff_datetime', types.TimestampType(), True), 
    types.StructField('passenger_count', types.IntegerType(), True), 
    types.StructField('trip_distance', types.DoubleType(), True), 
    types.StructField('RatecodeID', types.IntegerType(), True), 
    types.StructField('store_and_fwd_flag', types.StringType(), True), 
    types.StructField('PULocationID', types.IntegerType(), True), 
    types.StructField('DOLocationID', types.IntegerType(), True), 
    types.StructField('payment_type', types.IntegerType(), True), 
    types.StructField('fare_amount', types.DoubleType(), True), 
    types.StructField('extra', types.DoubleType(), True), 
    types.StructField('mta_tax', types.DoubleType(), True), 
    types.StructField('tip_amount', types.DoubleType(), True), 
    types.StructField('tolls_amount', types.DoubleType(), True), 
    types.StructField('improvement_surcharge', types.DoubleType(), True), 
    types.StructField('total_amount', types.StringType(), True), 
    types.StructField('congestion_surcharge', types.DoubleType(), True), 
    types.StructField('airport_fee', types.DoubleType(), True)
])







