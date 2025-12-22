from services.fetch_data import fetch_data
from services.write_data_to_minio import write_data_to_minio
import logging

def extract_and_load(api_server, bucket_name, object_name, minio_endpoint, access_key, secret_key, secure=True):
    print("extracting data from API server")
    logging.info("Starting data extraction from API server")
    data = fetch_data(api_server)
    if data:
        print(f"Data extracted {data}")
        logging.info("Data extraction successful, proceeding to load data into MinIO")
        write_data_to_minio(data, bucket_name, object_name, minio_endpoint, access_key, secret_key, secure)
        logging.info("Data loading completed successfully")
    else:
        print("No data extracted from API server")
        logging.warning("Data extraction failed, no data to load into MinIO")