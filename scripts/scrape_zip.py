import os
import zipfile
from urllib.request import urlretrieve


def get_zip(url, output_dir):
    """
    Unzips and extracts data from a zipile at the given url.
    Saves to output_dir
    """
    folder = output_dir[:output_dir.rfind('/')]
    if not os.path.exists(folder):
            os.makedirs(folder)
    zip_path = f"{output_dir}.zip"
    urlretrieve(url, zip_path)
    
    extract_dir = f"{output_dir}_extracted"  # Specify where you want to extract the files
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
    return