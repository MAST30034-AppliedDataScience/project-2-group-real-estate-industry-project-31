import os
from urllib.request import urlretrieve

def get_xlsx(url, output_dir):
    """
    Downloads an xlsx file from the given URL and saves it to the specified directory
    """
    # Ensure the output directory exists
    folder = output_dir[:output_dir.rfind('/')]
    if not os.path.exists(folder):
        os.makedirs(folder)

    # Save the .xlsx file directly
    file_path = f"{output_dir}.xlsx"
    urlretrieve(url, file_path)
    
    return