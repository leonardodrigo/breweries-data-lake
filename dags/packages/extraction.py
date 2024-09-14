from packages.azure_keys import upload_file_to_blob_storage
import requests
import math
import json


def request(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error getting API metadata: {e}")
        return None

def number_of_breweries():
    ''''
        GET request in metadata endpoint to get number of available breweries.
        This number is going to be used to check how many requests will be made to ingest all data.
    '''
    url = "https://api.openbrewerydb.org/v1/breweries/meta"
    response = request(url)
    return int(response['total'])


def extract_raw_data(container_name, max_per_page=200):
    ''''
        Data extraction from all the available breweries.
        The default number of pages is 50 but we are going to use the maximum available (200) to optimize the number of requests.
    '''
    num_breweries = number_of_breweries()
    if num_breweries:
        num_pages = math.ceil(num_breweries / max_per_page)

        print(f"Number of breweries: {num_breweries}")
        print(f"Number of pages: {num_pages}")

        num_pages = 1

        for page in range(1, num_pages + 1):
            response = request(f"https://api.openbrewerydb.org/v1/breweries?page={page}&per_page={max_per_page}")
            data = json.dumps(response)
            if data is not None:
                file_name = f"breweries_page_{page}.json"
                upload_file_to_blob_storage(file_name, data, container_name)
                print(f"File {file_name} uploaded to {container_name}")
            else:
                print(f"Failed to pull data from page {page}")






