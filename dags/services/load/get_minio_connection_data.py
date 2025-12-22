from minio import Minio


def get_minio_connection_data():
    """
    Creates and returns a MinIO client connection.
    """
    # minio_endpoint = "localhost:9000"
    minio_endpoint = "minio:9000"
    access_key = "datalake"
    secret_key = "datalake"
    secure = False

    client = Minio(
        minio_endpoint, access_key=access_key, secret_key=secret_key, secure=secure
    )
    return client
