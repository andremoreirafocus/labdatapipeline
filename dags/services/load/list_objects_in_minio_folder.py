from services.load.get_minio_connection_data import get_minio_connection_data


def list_objects_in_minio_folder(
    bucket_name,
    prefix,
):
    """
    Lists files in a MinIO folder (prefix).
    :param bucket_name: MinIO bucket name
    :param prefix: Folder path (prefix) in the bucket
    :param minio_endpoint: MinIO server endpoint
    :param access_key: MinIO access key
    :param secret_key: MinIO secret key
    :param secure: Use HTTPS if True, HTTP if False
    :return: List of file object names
    """
    try:
        client = get_minio_connection_data()
        objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
        return [obj.object_name for obj in objects]
    except Exception as e:
        print(f"Error listing files in MinIO folder: {e}")
        return []
    