from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os
import json


load_dotenv()

client_id     = os.environ['AZURE_CLIENT_ID']
tenant_id     = os.environ["AZURE_TENANT_ID"]
client_secret = os.environ["AZURE_CLIENT_SECRET"]
vault_url     = os.environ["AZURE_VAULT_URL"]
secret_name   = os.environ["AZURE_SECRET_NAME"]
storage_url   = os.environ["AZURE_STORAGE_URL"]

# create a credential 
credentials = ClientSecretCredential(
    client_id = client_id, 
    client_secret = client_secret,
    tenant_id = tenant_id
)

def upload_file_to_blob_storage(file_name, data, container_name):
    blob_service_client = BlobServiceClient(account_url=storage_url, credential=credentials)
    container_client = blob_service_client.get_container_client(container_name)
    
    # Ensure the container exists. If not, we are going to create
    try:
        container_client.create_container()
    except Exception as e:
        pass

    # Upload data to blob storage with overwrite
    blob_storage_client = container_client.get_blob_client(file_name)
    try:
        blob_storage_client.upload_blob(data, overwrite=True)
        print(f"File {file_name} uploaded to {container_name}")
    except Exception as e:
        print(f"Error uploading file {file_name}: {e}")