import argparse
import base64
import os
import time
from datetime import datetime, timedelta, timezone
from rich.console import Console
import rich.traceback
from azure.storage.blob import BlobServiceClient
from azure.search.documents import SearchClient
from azure.core.credentials import AzureKeyCredential

rich.traceback.install()
console = Console()

# Define top-level variables
UPLOAD_CONTAINER_NAME = "upload"
OUTPUT_CONTAINER_NAME = "content"
UPLOAD_FOLDER_NAME = ""
SEARCH_INDEX = "vector-index"

class TestFailedError(Exception):
    """Exception raised when a test fails"""

def parse_arguments():
    """
    Parse command line arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--filename",
        required=True,
        help="The filename of the document to remove from knowledge base."
    )
    parser.add_argument(
        "--search_key",
        required=True,
        help="The Azure Search Service key."
    )
    parser.add_argument(
        "--storage_account_connection_string",
        required=True,
        help="The connection string for the Azure storage account."
    )
    parser.add_argument(
        "--search_service_endpoint",
        required=True,
        help="Azure Search Endpoint")
    parser.add_argument(
        "--folder",
        required=False,
        help="The azure folder name containing the document to be removed from knowledge base."
    )

    return parser.parse_args()

def main(filename, search_key, storage_account_connection_string, search_service_endpoint, folder = None):
    """Main function"""
    try:
        
        # DEBUG
        console.print(storage_account_connection_string)
        console.print(search_service_endpoint)
        console.print(SEARCH_INDEX)
        console.print(search_key)
        console.print(folder)
        console.print(filename)

        storage_blob_service_client = BlobServiceClient.from_connection_string(
            storage_account_connection_string)
        
        remove_knowledge(storage_blob_service_client, search_service_endpoint, SEARCH_INDEX, search_key, filename, folder)
        
    except (Exception, TestFailedError) as ex:
        console.log(f'[red]❌ {ex}[/red]')
        raise ex

def remove_knowledge(blob_service_client, search_service_endpoint, search_index, search_key, filename, folder = None):
    """Function to remove the 'knowledge' for the specific file."""
    console.print("Beginning removal of knowledge.....")

    if folder is None:
            full_file_path = filename
    else:
        full_file_path = f'{folder}/{filename}'

    upload_container_client = blob_service_client.get_container_client(UPLOAD_CONTAINER_NAME)
    output_container_client = blob_service_client.get_container_client(OUTPUT_CONTAINER_NAME)
    azure_search_key_credential = AzureKeyCredential(search_key)
    search_client = SearchClient(
        endpoint=search_service_endpoint,
        index_name=search_index,
        credential=azure_search_key_credential,
    )

    # Delete file form Upload Container
    console.print(f"Deleting blobl: {full_file_path}")
    upload_container_client.delete_blob(f'{full_file_path}')

    # # Delete output container
    blobs = output_container_client.list_blobs(name_starts_with=full_file_path)
    for blob in blobs:
        try:
            output_container_client.delete_blob(blob.name)
            console.print(f"Deleted blob: {blob.name}")
        except Exception as ex:
            console.print(f"Failed to delete blob: {blob.name}. Error: {ex}")

        try:
            # Delete search index
            console.print(f"Removing document from index: {blob.name} : id : {encode_document_id(blob.name)}")
            search_client.delete_documents(documents=[{"id": f"{encode_document_id(blob.name)}"}])
        except Exception as ex:
            console.print(f"Failed to remove document from index: {blob.name} \
                          : id : {encode_document_id(blob.name)}. Error: {ex}")

    console.print("Finished removal of knowledge.")

def encode_document_id(document_id):
    """ encode a path/file name to remove unsafe chars for a cosmos db id """
    safe_id = base64.urlsafe_b64encode(document_id.encode()).decode()
    return safe_id

if __name__ == '__main__':
    args = parse_arguments()
    main(args.filename, args.search_key, args.storage_account_connection_string, args.search_service_endpoint, args.folder)
