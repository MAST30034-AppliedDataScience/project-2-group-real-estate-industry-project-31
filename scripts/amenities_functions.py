import pandas as pd
import folium
import numpy as np
from scipy.spatial import distance_matrix
import time


def fetch_amentities(api, query):

    # Execute the query
    result = api.query(query)

    # Extract data into a list
    amenities_data = []
    for node in result.nodes:
        amenities_data.append({
            "id": node.id,
            "name": node.tags.get("name", "N/A"),
            "amenity": node.tags.get("amenity"),
            "lat": node.lat,
            "lon": node.lon
        })

    # Convert to DataFrame
    df_amenities = pd.DataFrame(amenities_data)

    return df_amenities

# Define a function to choose icons based on amenity type
def get_icon(amenity):
    if amenity == "school":
        return folium.Icon(color="blue", icon="graduation-cap", prefix="fa")
    elif amenity == "hospital":
        return folium.Icon(color="red", icon="plus-square", prefix="fa")
    elif amenity == "supermarket":
        return folium.Icon(color="green", icon="shopping-cart", prefix="fa")
    elif amenity == "park":
        return folium.Icon(color="darkgreen", icon="tree", prefix="fa")
    else:
        return folium.Icon(color="gray", icon="info-sign")
    


def filter_population_data(pop_data):
    # Keep only rows where 'SEX' == 'Persons'
    filtered_data = pop_data[pop_data["SEX"] == "Persons"]
    
    # Select only the required columns
    filtered_data = filtered_data[["YEAR", "SA2_CODE", "SA2_NAME", "Total"]]
    
    return filtered_data



def calculate_closest_amenity(domain_gdf, amenity_gdf, amenity):
    # Extract lat/lon as numpy arrays
    domain_coords = domain_gdf[['latitude', 'longitude']].to_numpy()
    train_coords = amenity_gdf[['latitude', 'longitude']].to_numpy()
    
    # Calculate distance matrix between all domain properties and train stations
    dist_matrix = distance_matrix(domain_coords, train_coords)
    
    # Find the index of the minimum distance (closest station) for each property
    closest_station_indices = dist_matrix.argmin(axis=1)
    
    # Add closest station name, latitude, and longitude to domain_gdf
    domain_gdf['closest_station'] = amenity_gdf.iloc[closest_station_indices]['stop_name'].values
    domain_gdf['train_lat'] = amenity_gdf.iloc[closest_station_indices]['latitude'].values
    domain_gdf['train_long'] = amenity_gdf.iloc[closest_station_indices]['longitude'].values
    domain_gdf['min_distance'] = dist_matrix.min(axis=1)  # Optional: Store the minimum distance as well

    return domain_gdf



def get_batch_distances(df, client, p_lat, p_lon, a_lat, a_lon, batch_size=50):
    all_distances = []
    
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i + batch_size]
        
        # Prepare coordinates: first half are properties, second half are stations
        coords = [[row[p_lon], row[p_lat]] for _, row in batch.iterrows()] + \
                 [[row[a_lon], row[a_lat]] for _, row in batch.iterrows()]
        
        try:
            # ORS Matrix API request for driving distances
            matrix = client.distance_matrix(
                locations=coords, 
                profile='driving-car',
                metrics=['distance'],
                sources=list(range(len(batch))),  # Property indices
                destinations=list(range(len(batch), len(batch)*2))  # Amenity indices
            )
            
            # Get driving distances and append
            for j in range(len(batch)):
                distance = matrix['distances'][j][j]  # Property to amenity distance
                all_distances.append(distance / 1000)  # Convert from meters to kilometers
        except Exception as e:
            print(f"Error with batch {i+1}: {e}")
            if "Rate limit exceeded" in str(e):
                print("Rate limit exceeded. Waiting for 60 seconds...")
                time.sleep(60)  # Wait for a minute before retrying
            else:
                all_distances.extend([None] * len(batch))  # Append None for failed requests
                break  # Exit the loop on other errors

        # Respect the rate limit by adding a delay between batches
        time.sleep(2)  
    
    return all_distances