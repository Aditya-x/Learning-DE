import pandas as pd
import sys

def convert_to_csv(parquet_file: str,csv_file: str):

    try:
    # Read Parquet file
        df = pd.read_parquet(parquet_file)

        # Convert to CSV
        df.to_csv(csv_file, index=False)

        print(f"Converted {parquet_file} to {csv_file}")
    except Exception as e:
        print(f"Error: {e}")




if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python convert_to_csv.py <parquet_file> <csv_file>")
        sys.exit(1)

    parquet_file = sys.argv[1]
    csv_file = sys.argv[2]

    convert_to_csv(parquet_file, csv_file)

