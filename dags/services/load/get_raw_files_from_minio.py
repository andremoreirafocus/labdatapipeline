import json
from services.load.get_minio_connection_data import get_minio_connection_data


def get_raw_files_from_minio(object_names, source_bucket_name):
    """
    Reads and flattens all JSON files from MinIO, returning a list of flattened records.
    """
    all_flattened_records = []
    for object_name in object_names:
        print(f"Reading JSON from MinIO: {object_name}")
        json_str = read_json_from_minio(
            bucket_name=source_bucket_name, object_name=object_name
        )
        if json_str:
            try:
                data = json.loads(json_str)
                if isinstance(data, dict):
                    # If the dict has 'results', use the first result (legacy logic)
                    if "results" in data and isinstance(data["results"], list):
                        data = [data["results"][0]]
                    else:
                        data = [data]
                for record in data:
                    flat = flatten_json_string(json.dumps(record))
                    if flat is not None:
                        all_flattened_records.append(flat)
            except Exception as e:
                print(f"Error processing {object_name}: {e}")
    return all_flattened_records


def read_json_from_minio(bucket_name, object_name):
    """
    Reads a JSON file from MinIO and returns its contents as a string.
    :param bucket_name: MinIO bucket name
    :param object_name: Object name for the JSON file in MinIO
    :param minio_endpoint: MinIO server endpoint
    :param access_key: MinIO access key
    :param secret_key: MinIO secret key
    :param secure: Use HTTPS if True, HTTP if False
    :return: JSON file contents as a string
    """
    try:
        client = get_minio_connection_data()
        response = client.get_object(bucket_name, object_name)
        json_str = response.read().decode("utf-8")
        response.close()
        response.release_conn()
        return json_str
    except Exception as e:
        print(f"Error reading JSON from MinIO: {e}")
        return None


def flatten_json_string(json_str, sep="_"):
    """
    Flattens a nested JSON string to a one-level dictionary.
    :param json_str: JSON string (object)
    :param sep: Separator for nested keys (default: '.')
    :return: Flattened dictionary
    """

    def _flatten(obj, parent_key="", sep=sep):
        items = {}
        if isinstance(obj, dict):
            for k, v in obj.items():
                if "picture" in k or "info" in k or "postcode" in k:
                    continue  # Skip keys containing 'picture'
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                items.update(_flatten(v, new_key, sep=sep))
        elif isinstance(obj, list):
            for i, v in enumerate(obj):
                new_key = f"{parent_key}{sep}{i}" if parent_key else str(i)
                items.update(_flatten(v, new_key, sep=sep))
        else:
            items[parent_key] = obj
        return items

    try:
        data = json.loads(json_str)
        return _flatten(data)
    except Exception as e:
        print(f"Error flattening JSON string: {e}")
        return None
