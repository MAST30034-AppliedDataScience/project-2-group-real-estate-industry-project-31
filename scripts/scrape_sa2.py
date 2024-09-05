import os
import zipfile
from urllib.request import urlretrieve


def get_shapefile(url, output_dir):
    """
    Gets the SA2 shapefile from the ABS website
    """

    if not os.path.exists(output_dir):
            os.makedirs(output_dir)
    zip_path = f"{output_dir}SA2.zip"
    urlretrieve(url, zip_path)
    
    extract_dir = f"{output_dir}extracted_SA2"  # Specify where you want to extract the files
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
    return