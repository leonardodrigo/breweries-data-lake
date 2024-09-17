from azure.storage.blob import BlobServiceClient
import os
import json


AZURE_STORAGE_ACCOUNT = os.environ["AZURE_STORAGE_ACCOUNT"]
AZURE_TOKEN = os.environ["AZURE_TOKEN"]

storage_url = f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net"

def upload_file_to_blob_storage(file_name, data, container_name):
    blob_service_client = BlobServiceClient(account_url=storage_url, credential=AZURE_TOKEN)
    container_client = blob_service_client.get_container_client(container_name)
    
    # Ensure the container exists. If not, create it
    try:
        container_client.create_container()
    except Exception as e:
        if "ContainerAlreadyExists" not in str(e):
            print(f"Error creating container: {e}")
            return
    
    json_data = json.dumps(data)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
    try:
        blob_client.upload_blob(json_data, overwrite=True)
        print(f"File {file_name} uploaded to {container_name}")
    except Exception as e:
        print(f"Error uploading file {file_name}: {e}")
