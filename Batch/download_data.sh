#!/bin/bash

set -e

TAXI_TYPE=$1 # "yellow/Green"
YEAR=$2

if [ -z "$TAXI_TYPE" ] || [ -z "$YEAR" ]; then
  echo "Usage: $0 <taxi_type> <year>"
  exit 1
fi

BASE_DIR="/mnt/d/Development/Data-Engineering/Batch/"

URL_PREFIX="https://d37ci6vzurychx.cloudfront.net/trip-data"

for MONTH in {1..12}; do
    FMONTH=`printf "%02d" ${MONTH}`
    
    URL="${URL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.parquet"

    LOCAL_PREFIX="${BASE_DIR}/Dataset/${TAXI_TYPE}/${YEAR}/${FMONTH}"
    LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.parquet"
    LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"
    CSV_PATH="${LOCAL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.csv"
    echo "Month: ${FMONTH}, URL: ${URL}, LOCAL_PATH: ${LOCAL_PATH}"

    mkdir -p "${LOCAL_PREFIX}" # Make directory

    # Downloading parquet file
    echo "Downloading ${URL}..."
    curl -o "${LOCAL_PATH}" "${URL}"


    # Converting it to CSV
    # echo "Converting to CSV..."
    # python3 convert-to-csv.py "${LOCAL_PATH}" "${CSV_PATH}"
    
    # Deleting the parquet file
    # echo "Removing parquet file..."
    # rm -f "${LOCAL_PATH}"

    # Compressing the CSV
    # echo "Compressing CSV..."
    # gzip "${CSV_PATH}"

done
