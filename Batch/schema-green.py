types.StructType([types.StructField('hvfhs_license_num', types.StringType(), True),
    types.StructField('dispatching_base_num', types.StringType(), True),
    types.StructField('originating_base_num', types.StringType(), True),
    types.StructField('request_datetime', types.TimestampType(), True),
    types.StructField('on_scene_datetime', types.TimestampType(), True),
    types.StructField('pickup_datetime', types.TimestampType(), True),
    types.StructField('dropoff_datetime', types.TimestampType(), True),
    types.StructField('PULocationID', types.IntegerType(), True),
    types.StructField('DOLocationID', types.IntegerType(), True),
    types.StructField('trip_miles', types.DoubleType(), True),
    types.StructField('trip_time', types.IntegerType(), True),
    types.StructField('base_passenger_fare', types.DoubleType(), True),
    types.StructField('tolls', types.DoubleType(), True),
    types.StructField('bcf', types.DoubleType(), True),
    types.StructField('sales_tax', types.DoubleType(), True),
    types.StructField('congestion_surcharge', types.DoubleType(), True),
    types.StructField('airport_fee', types.StringType(), True),
    types.StructField('tips', types.DoubleType(), True),
    types.StructField('driver_pay', types.DoubleType(), True),
    types.StructField('shared_request_flag', types.StringType(), True),
    types.StructField('shared_match_flag', types.StringType(), True),
    types.StructField('access_a_ride_flag', types.StringType(), True),
    types.StructField('wav_request_flag', types.StringType(), True),
    types.StructField('wav_match_flag', types.StringType(), True)
 ])




SELECT 
    -- Revenue grouping 
    pickup_zone AS revenue_zone,
    date_trunc("month", "pickup_datetime") AS revenue_month, 

    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(ehail_fee) AS revenue_monthly_ehail_fee,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,

    -- Additional calculations
    COUNT(tripid) AS total_monthly_trips,
    AVG(passenger_count) AS avg_monthly_passenger_count,
    AVG(trip_distance) AS avg_monthly_trip_distance

    FROM trips_data
    GROUP BY revenue_zone, revenue_month, service_type





    