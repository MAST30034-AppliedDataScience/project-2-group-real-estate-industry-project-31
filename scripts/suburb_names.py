import geopandas as gpd

def get_suburb_names():
    
    """
    This function loads a shapefile from a predefined path, filters it for Victoria,
    , and returns a list of unique SA1 names.
    
    Returns:
    list: A list of unique SA1 names.
    """

    # Hardcoded shapefile path
    shapefile_path = "../data/SA1/extracted_SA1/SAL_2021_AUST_GDA2020.shp"
    
    # Load shapefile
    gdf = gpd.read_file(shapefile_path)
    
    gdf.head()

    # Filter for Victoria
    vic_gdf = gdf[gdf['STE_NAME21'] == 'Victoria']

    # Delete the last two rows
    vic_gdf = vic_gdf.iloc[:-2]
    
    # Get unique SA2 names as a list
    sa1_names = vic_gdf['SAL_NAME21'].unique().tolist()
    sa1_names
    
    return sa1_names