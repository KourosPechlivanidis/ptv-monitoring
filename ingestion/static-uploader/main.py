import polars as pl
import zipfile
import io
import argparse
from datetime import datetime
import requests
from deltalake.writer import write_deltalake
import pyarrow as pa

MODE_MAP = {"2": "metro", "3": "tram", "4": "bus"}

FILES_TO_PROCESS = ["stops.txt", "routes.txt", "trips.txt", "stop_times.txt"]

MAX_ROWS = 100_000


def process_and_upload(bucket_name: str, valid_from_date: str) -> None:
    s3_path = f"s3://{bucket_name}/delta"
    ptv_url = "https://data.ptv.vic.gov.au/downloads/gtfs.zip"

    print("Downloading GTFS static ZIP...")
    response = requests.get(ptv_url, timeout=120)
    response.raise_for_status()
    print("Download complete.")

    with zipfile.ZipFile(io.BytesIO(response.content)) as master_zip:
        for folder_id, mode_name in MODE_MAP.items():
            nested_zip_path = f"{folder_id}/google_transit.zip"

            if nested_zip_path not in master_zip.namelist():
                print(f"[{mode_name}] {nested_zip_path} not found in master ZIP, skipping.")
                continue

            print(f"\n[{mode_name}] Processing...")

            with master_zip.open(nested_zip_path) as nested_zip_data:
                with zipfile.ZipFile(io.BytesIO(nested_zip_data.read())) as inner_zip:
                    for file_name in FILES_TO_PROCESS:
                        if file_name not in inner_zip.namelist():
                            print(f"[{mode_name}] {file_name} not found, skipping.")
                            continue

                        table_name = file_name.replace(".txt", "")

                        with inner_zip.open(file_name) as f:
                            df = pl.read_csv(f.read(), infer_schema_length=10000)

                        df = df.with_columns([
                            pl.lit(mode_name).alias("transport_mode"),
                            pl.lit(valid_from_date).str.to_date().alias("valid_from"),
                            pl.lit(datetime.now()).alias("ingested_at"),
                        ])

                        arrow_table = df.to_arrow()
                        batches = arrow_table.to_batches(max_chunksize=MAX_ROWS)

                        print(f"[{mode_name}] {table_name}: {len(df):,} rows → {len(batches)} batch(es)")

                        for i, batch in enumerate(batches, start=1):
                            print(f"[{mode_name}] {table_name}: writing batch {i}/{len(batches)}...")
                            write_deltalake(
                                f"{s3_path}/{table_name}",
                                pa.Table.from_batches([batch]),
                                mode="append",
                                partition_by=["transport_mode", "valid_from"],
                                schema_mode="merge",
                            )

                        print(f"[{mode_name}] {table_name}: done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Upload PTV GTFS static schedule data to S3 Delta Lake."
    )
    parser.add_argument("--bucket", required=True, help="S3 bucket name (e.g. ptv-gtfs-static)")
    parser.add_argument("--valid_from", required=True, help="Schedule validity date (YYYY-MM-DD)")
    args = parser.parse_args()

    process_and_upload(args.bucket, args.valid_from)
