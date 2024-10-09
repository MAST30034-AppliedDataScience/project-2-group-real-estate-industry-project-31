# IMPORTS 
import pandas as pd
import requests
import geopandas as gpd
import matplotlib.pyplot as plt
from scipy.spatial import distance_matrix
import time
from openrouteservice import Client
import sys
sys.path.append("../")
import overpy
from shapely.geometry import Point

def fetch_amenities(api, node_query, way_query):
    '''
    Calls an api to the Overpass service to retrieve both the nodes and ways for the 
    given amenity types and returns a dataframe of each amenity in Victoria
    '''
    
    # Extract data into a list
    amenities_data = []

    # Execute the query for the nodes
    node_results = api.query(f"[out:json];area[name='Victoria']->.searchArea;({node_query});out body;")
    
    # Process nodes
    for node in node_results.nodes:
        amenities_data.append({
            "id": node.id,
            "name": node.tags.get("name", "N/A"),
            "amenity": node.tags.get("amenity"),
            "lat": node.lat,
            "lon": node.lon
        })

    # Execute the query for the ways
    way_results = api.query(f"[out:json];area[name='Victoria']->.searchArea;({way_query});(._;>;);out body;")

    # Process ways (calculate centroid from nodes)
    for way in way_results.ways:

        way_name = way.tags.get("name", "N/A")
        if way_name == "N/A":
            continue  # Skip this way if the name is missing

        node_latitudes = []
        node_longitudes = []
        
        # Iterate over each node in the way
        for node in way.nodes:
            node_latitudes.append(node.lat)
            node_longitudes.append(node.lon)
        
        # Calculate centroid (average of latitudes and longitudes)
        if node_latitudes and node_longitudes:
            centroid_lat = sum(node_latitudes) / len(node_latitudes)
            centroid_lon = sum(node_longitudes) / len(node_longitudes)
            
            # Append the way data with centroid
            amenities_data.append({
                "id": way.id,
                "name": way_name,
                "amenity": way.tags.get("amenity"),
                "lat": centroid_lat,
                "lon": centroid_lon
            })

    # Convert to DataFrame
    df_amenities = pd.DataFrame(amenities_data)

    return df_amenities


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