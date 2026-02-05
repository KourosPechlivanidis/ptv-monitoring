import polars as pl
import boto3
import zipfile
import io
import argparse
from datetime import datetime
import requests
from deltalake.writer import write_deltalake
import pyarrow as pa

def process_and_upload(bucket_name, valid_from_date):
    s3_path = f"s3://{bucket_name}/delta"
    ptv_url = "https://data.ptv.vic.gov.au/downloads/gtfs.zip"

    # MODE_MAP = {"2": "train", "3": "tram", "4": "bus"}
    MODE_MAP = {"4": "bus"}
    files_to_process = ['stops.txt', 'routes.txt', 'trips.txt']


    response = requests.get(ptv_url)
    response.raise_for_status()
    
    with zipfile.ZipFile(io.BytesIO(response.content)) as master_zip:
        for folder_id, mode_name in MODE_MAP.items():
            nested_zip_path = f"{folder_id}/google_transit.zip"
            
            if nested_zip_path in master_zip.namelist():
                with master_zip.open(nested_zip_path) as nested_zip_data:
                    with zipfile.ZipFile(io.BytesIO(nested_zip_data.read())) as inner_zip:
                        
                        for file_name in files_to_process:
                            if file_name in inner_zip.namelist():
                                table_name = file_name.replace('.txt', '')
                                
                                with inner_zip.open(file_name) as f:
                                    df = pl.read_csv(f.read(), infer_schema_length=10000)

                                df = df.with_columns([
                                    pl.lit(mode_name).alias("transport_mode"),
                                    pl.lit(valid_from_date).str.to_date().alias("valid_from"),
                                    pl.lit(datetime.now()).alias("ingested_at")
                                ])

                                MAX_ROWS = 100_000  # be conservative

                                table = df.to_arrow()

                                for batch in table.to_batches(max_chunksize=MAX_ROWS):
                                    print("Writing batch")
                                    write_deltalake(
                                        f"{s3_path}/{table_name}",
                                        pa.Table.from_batches([batch]),
                                        mode="append",
                                        partition_by=["transport_mode", "valid_from"],
                                        schema_mode="merge",
                                    )

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket', required=True)
    parser.add_argument('--valid_from', required=True)
    args = parser.parse_args()

    process_and_upload(args.bucket, args.valid_from)