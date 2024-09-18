## Python script with functions to aid with file management in the notebooks ##

import geopandas as gpd
import zipfile
import os
import requests



def download_file(url, local_path):
    """
    Download a file from a URL to a local path.
    """

    response = requests.get(url, stream=True)
    response.raise_for_status()  # Check for request errors
    
    with open(local_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)



def extract_zip(zip_path, extract_to):
    """
    Extract a ZIP file to a specified directory.
    """

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)




def get_suburb_names():
    """
    This function downloads a shapefile from a URL, extracts it, filters it for Victoria,
    and returns a list of unique suburb names.
    
    Returns:
    list: A list of unique suburb names.
    """

    url = 'https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/digital-boundary-files/SAL_2021_AUST_GDA2020_SHP.zip'
    zip_path = '../data/SA1/SA1.zip'
    extract_to = '../data/SA1/extracted_SA1'
    
    # Download the ZIP file
    download_file(url, zip_path)
    
    # Extract the ZIP file
    extract_zip(zip_path, extract_to)
    
    # Find the shapefile (assuming it's the only .shp file)
    shapefile_name = [f for f in os.listdir(extract_to) if f.endswith('.shp')][0]
    shapefile_path = os.path.join(extract_to, shapefile_name)
    
    # Read the shapefile with geopandas
    gdf = gpd.read_file(shapefile_path)
    gdf.head()
    gdf.tail()
    # Filter for Victoria
    vic_gdf = gdf[gdf['STE_NAME21'] == 'Victoria']
    
    # Delete the last two rows
    vic_gdf = vic_gdf.iloc[:-2]
    
    # Get unique suburb names as a list
    suburb_names = vic_gdf['SAL_NAME21'].unique().tolist()

    suburb_names = [name.split(' (')[0] for name in suburb_names]
    
    return suburb_names