from packages.azure_keys import upload_file_to_blob_storage
import requests
import math
import json


def number_of_breweries():
    '''
        GET request in metadata endpoint to get number of available breweries.
        This number is going to be used to check how many requests will be made to ingest all data.
    '''
    url = "https://api.openbrewerydb.org/v1/breweries/meta"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return int(data["total"])
    except requests.exceptions.RequestException as e:
        print(f"Error getting number of breweries: {e}")
        return None

def extract_raw_data(container_name, max_per_page=50):
    '''
        Data extraction from all the available breweries.
        The default number of pages is 50 but we are going to use the maximum available (200) to optimize the number of requests.
    '''
    num_breweries = number_of_breweries()
    if num_breweries:
        num_pages = math.ceil(num_breweries / max_per_page)

        print(f"Number of breweries: {num_breweries}")
        print(f"Number of pages: {num_pages}")

        for page in range(1, num_pages + 1):
            try:
                response = requests.get(f"https://api.openbrewerydb.org/v1/breweries?page={page}&per_page={max_per_page}")
                response.raise_for_status()
                data = response.json()
                if data:
                    file_name = f"breweries_page_{page}.json"
                    upload_file_to_blob_storage(file_name, data, container_name)
                else:
                    print(f"No data found on page {page}")
            except requests.exceptions.RequestException as e:
                print(f"Failed to pull data from page {page}: {e}")

