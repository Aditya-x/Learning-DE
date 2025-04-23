import os 
import sys
from pathlib import Path 
import requests


def download_file(url, local_path):
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
            
        print(f'Donloaded: {local_path}')
    else:
        print(f'Failed to download: {url} (Status Code: {response.status_code})')
        sys.exit(1)

def main():

    if len(sys.argv!=3):
        print(f'Usage: {sys.argv[0]} <taxi_type> <year>')
        sys.exit(1)
    
    taxi_type = sys.argv[1]
    year = sys.argv[2]

    url_prefix = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    home_dir = Path("D:/Development/Data-Engineering/Batch")


    for month in range(1,13):
        fmonth = f"{month:02d}"

        url=f"{url_prefix}/{taxi_type}_tripdata_{year}-{fmonth}.parquet"
        