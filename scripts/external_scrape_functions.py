## Python Script with functions to aid in fetching external datasets from the web ##

import os
import zipfile
from urllib.request import urlretrieve
import requests



def get_xlsx(url, output_dir, headers=None):
    """
    Downloads an xlsx file from the given URL and saves it to the specified directory
    """

    # Extract the directory
    folder = output_dir[:output_dir.rfind('/')]

    # Ensure the output directory exists
    if not os.path.exists(folder):
        os.makedirs(folder)


    if headers:
        try:
            # Retrieving the population projection data
            response = requests.get(url, headers=headers)

            # Check for a successful request
            if response.status_code == 200:
                # Save the content to a CSV file
                with open(output_dir, "wb") as file:
                    file.write(response.content)
                print(f"Data successfully written to {output_dir}") 
            else:
                print(f"Failed to retrieve data. HTTP Status Code: {response.status_code}")

        except Exception as e:
            print(f"An error occurred: {e}")
    
    else:
        try:
            # Save the .xlsx or .csv file directly
            if not output_dir.endswith(".csv"):
                file_path = f"{output_dir}.xlsx"
            urlretrieve(url, file_path)

            print(f"Data successfully written to {output_dir}")
            
        except Exception as e:
            print(f"An error occurred: {e}")
    
    return


def get_zip(url, output_dir):
    """
    Unzips and extracts data from a zipile at the given url.
    Saves to output_dir
    """

    folder = output_dir[:output_dir.rfind('/')]
    # Creates the directory if it doesn't yet exist
    if not os.path.exists(folder):
            os.makedirs(folder)
    

    zip_path = f"{output_dir}.zip"
    urlretrieve(url, zip_path)
    
    extract_dir = f"{output_dir}_extracted"  # Specify where you want to extract the files
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
    
    return

