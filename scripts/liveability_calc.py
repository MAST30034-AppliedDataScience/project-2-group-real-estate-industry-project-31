## Python script with functions to help with the livibility calcultions ##

import pandas as pd
import geopandas as gpd


def map_amenities_to_sa2(df_amenities, sa2_gdf):
    '''
    Maps amenities to SA2 regions and returns a Pandas DataFrame with the SA2 name appended.
    '''
    
    # Create a GeoDataFrame for amenities
    gdf_amenities = gpd.GeoDataFrame(df_amenities, 
                                     geometry=gpd.points_from_xy(df_amenities.lon, df_amenities.lat),
                                     crs="EPSG:4326")
    
    # Perform a spatial join to find which SA2 region each amenity belongs to
    gdf_amenities_with_sa2 = gpd.sjoin(gdf_amenities, sa2_gdf[['SA2_NAME21', 'geometry']], how="left", predicate="within")
    
    # Convert the GeoDataFrame back to a Pandas DataFrame and keep only the necessary columns
    df_amenities_with_sa2 = pd.DataFrame(gdf_amenities_with_sa2.drop(columns=['geometry']))

    # Return only the original columns and the SA2 name
    return df_amenities_with_sa2[['id', 'name', 'amenity', 'lat', 'lon', 'SA2_NAME21']]



def merge_ammentity(base_df, ammenity_df, ammenity_name):

    base_df = pd.merge(base_df, ammenity_df, left_on='SA2_name_2021', right_on='SA2_NAME21', how='left')
    base_df.rename(columns={'count': ammenity_name}, inplace=True)
    base_df[ammenity_name] = base_df[ammenity_name].fillna(0)
    base_df = base_df.drop('SA2_NAME21', axis=1)
    
    return base_df



def transform_median_rent(row):

    if row['median_rent'] and row['median_rent'] != 0:  # Ensure median_rent is not zero or missing
        return (773 / (row['median_rent'] / 0.30)) * 100
    
    # If median_rent is zero or missing, return NaN or a default value
    return None

