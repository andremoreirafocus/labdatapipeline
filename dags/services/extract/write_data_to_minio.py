from services.extract.get_minio_connection_data import get_minio_connection_data
import json
from minio import Minio
from minio.error import S3Error
from io import BytesIO
import logging


def write_data_to_minio(data, bucket_name, object_name):
    """
    Uploads a variable (Python object) as a JSON file to a MinIO bucket.
    :param data: Python object to upload (will be serialized to JSON)
    :param bucket_name: Name of the MinIO bucket
    :param object_name: Name for the object in the bucket
    :param minio_endpoint: MinIO server endpoint (e.g., 'localhost:9000')
    :param access_key: MinIO access key
    :param secret_key: MinIO secret key
    :param secure: Use HTTPS if True, HTTP if False
    """
    try:
        client = get_minio_connection_data()
        # Create bucket if it does not exist
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
        # Serialize data to JSON and upload
        json_bytes = json.dumps(data).encode("utf-8")
        client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=BytesIO(json_bytes),
            length=len(json_bytes),
            content_type="application/json",
        )
        print(f"Data uploaded to bucket '{bucket_name}' as '{object_name}'")
        logging.info(
            f"Data successfully uploaded to MinIO bucket '{bucket_name}' with object name '{object_name}'"
        )
    except S3Error as err:
        print(f"MinIO error: {err}")
        logging.error(f"Failed to upload data to MinIO: {err}")
