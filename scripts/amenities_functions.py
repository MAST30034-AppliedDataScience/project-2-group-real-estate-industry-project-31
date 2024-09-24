import pandas as pd
import folium
from scipy.spatial import distance_matrix
import time
from openrouteservice import Client


# Function to fetch amenities from the Overpass API
def fetch_amenities(api, query):
    result = api.query(f"[out:json];area[name='Victoria']->.searchArea;({query});out body;")
    
    amenities_data = []
    for node in result.nodes:
        amenities_data.append({
            "id": node.id,
            "name": node.tags.get("name", "N/A"),
            "amenity": node.tags.get("amenity"),
            "lat": node.lat,
            "lon": node.lon
        })

    return pd.DataFrame(amenities_data)


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



def calculate_closest_amenity(property_df, amenity_df):

    pdf = property_df.copy()
    adf = amenity_df.copy()
    
    # Ensure that the latitude and longitude are converted to floats
    pdf['latitude'] = pdf['latitude'].astype(float)
    pdf['longitude'] = pdf['longitude'].astype(float)
    adf['lat'] = adf['lat'].astype(float)
    adf['lon'] = adf['lon'].astype(float)

    # Extract lat/lon as numpy arrays
    domain_coords = pdf[['latitude', 'longitude']].to_numpy()
    amenity_coords = adf[['lat', 'lon']].to_numpy()
    
    # Calculate distance matrix between all domain properties and the amenity
    dist_matrix = distance_matrix(domain_coords, amenity_coords)
    
    # Find the index of the minimum distance (closest amenity) for each property
    closest_amenity_indices = dist_matrix.argmin(axis=1)
    
    # Add closest amenity name, latitude, and longitude to pdf
    pdf['amenity_lat'] = adf.iloc[closest_amenity_indices]['lat'].values
    pdf['amenity_lon'] = adf.iloc[closest_amenity_indices]['lon'].values

    return pdf



def get_batch_distances(df, api_keys, p_lat, p_lon, a_lat, a_lon, batch_size=50):
    all_distances = []
    current_key = 0
    client = Client(key=api_keys[current_key])
    
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
                # Updating the api key index
                current_key += 1

                if current_key >= len(api_keys):
                    print("All API keys exhausted. Stopping the API calls...")
                    all_distances.extend([None] * len(batch))  # Append None for failed requests
                    break  # Exit the loop on other errors
                
                print(f"Quota limit exceeded for API key {api_keys[current_key]}")
                print("Using a new key...")
        
                client = Client(key=api_keys[current_key])

        # Respect the rate limit by adding a delay between batches
        time.sleep(2)  
    
    return all_distances


def get_amenity_distances(property_df, amenity_dfs, api_keys):
    # Loop through each amenity type and compute the driving distance
    for amenity_type, amenity_df in amenity_dfs.items():
        print(f"Processing {amenity_type}...")
        
        # Step 1: Calculate the closest amenity using Euclidean distance
        property_df = calculate_closest_amenity(property_df, amenity_df)
        
        # Step 2: Make batch ORS API calls to get driving distances
        distances = get_batch_distances(
            property_df, 
            api_keys, 
            p_lat='latitude', 
            p_lon='longitude', 
            a_lat='amenity_lat', 
            a_lon='amenity_lon', 
            batch_size=50
        )
        
        # Step 3: Add the driving distance to the DataFrame with the correct column name
        property_df[f"dist_to_{amenity_type}"] = distances

        # Step 4: Remove the unneded columns
        property_df = property_df.drop(["amenity_lat", "amenity_lon"], axis=1)
    
    return property_df