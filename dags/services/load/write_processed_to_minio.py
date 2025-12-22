from services.load.get_minio_connection_data import get_minio_connection_data


def write_processed_to_minio(buffer, destination_bucket_name, destination_object_name):
    """
    Writes a Parquet buffer to MinIO at the specified bucket and object name.
    """
    client = get_minio_connection_data()
    if not client.bucket_exists(destination_bucket_name):
        client.make_bucket(destination_bucket_name)
    client.put_object(
        bucket_name=destination_bucket_name,
        object_name=destination_object_name,
        data=buffer,
        length=buffer.getbuffer().nbytes,
        content_type="application/octet-stream",
    )
    print(
        f"Consolidated file uploaded to bucket '{destination_bucket_name}' as '{destination_object_name}'"
    )
