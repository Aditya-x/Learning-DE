set -e

TAXI_TYPE=$1 # "yellow/Green"

YEAR=$2

URL_PREFIX="https://d37ci6vzurychx.cloudfront.net/trip-data"

for MONTH in {1..12}; do
    FMONTH=`printf "%02d" ${MONTH}`

    URL="${URL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.parquet"

    LOCAL_PREFIX="/home/aditya/Learning-DE/Batch/Dataset/${TAXI_TYPE}/${YEAR}/${FMONTH}"
    LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.parquet"
    LOCAL_PATH=${LOCAL_PREFIX}/${LOCAL_FILE}
    CSV_PATH=${LOCAL_PREFIX}/"${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.csv"

    mkdir -p ${LOCAL_PREFIX} # Make directory
    wget ${URL} -O ${LOCAL_PATH}  downloading parquet file

    # Converting it to CSV
    python /home/aditya/Learning-DE/Batch/convert-to-csv.py "${LOCAL_PATH}" "${CSV_PATH}" 
    
    Deleting the parquet file
    rm -f ${LOCAL_PATH}.gz

    # Compressing the CSV
    gzip ${CSV_PATH}

done