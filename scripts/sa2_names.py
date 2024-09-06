import geopandas as gpd

def get_sa2_names():
    
    """
    This function loads a shapefile from a predefined path, filters it for Victoria,
    , and returns a list of unique SA2 names.
    
    Returns:
    list: A list of unique SA2 names.
    """

    # Load shapefile
    shapefile_path = "../data/SA2/extracted_SA2/SA2_2021_AUST_GDA2020.shp"
    
    gdf = gpd.read_file(shapefile_path)
    
    # Filter for Victoria
    vic_gdf = gdf[gdf['STE_NAME21'] == 'Victoria']
    
    # Delete the last two rows
    vic_gdf = vic_gdf.iloc[:-2]
    
    # Get unique SA2 names as a list
    sa2_names = vic_gdf['SA2_NAME21'].unique().tolist()
    
    return sa2_names

