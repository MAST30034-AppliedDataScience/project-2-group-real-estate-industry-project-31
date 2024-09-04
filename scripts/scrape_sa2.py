def get_shapefile():
    """Gets the taxi zones shapefile from the TLC website
    """
    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"
    output_dir = '../data/landing/map/'
    if not os.path.exists(output_dir):
            os.makedirs(output_dir)
    zip_path = f"{output_dir}taxi_zones.zip"
    urlretrieve(url, zip_path)
    
    extract_dir = f"{output_dir}extracted_taxi_zones"  # Specify where you want to extract the files
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
    return