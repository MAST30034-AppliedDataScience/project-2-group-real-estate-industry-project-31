## Python script with functions to aid in fetching coordinates of different locations and calulating 
## their respective driving distaces to each property

import pandas as pd
from scipy.spatial import distance_matrix
import time
from openrouteservice import Client


def get_cities(api, query):
    '''
    Fetches the cities given in the query using the Overpass API service
    and returns the result as a dataframe
    '''

    # Execute the query
    result = api.query(query)
    
    # Extract the city data into a list
    city_data = []
    for node in result.nodes:
        city_name = node.tags.get("name", "N/A")
        
        # Filter out cities with 'Victoria' in their name
        if "Victoria" not in city_name:
            city_data.append({
                "name": city_name,
                "place_type": node.tags.get("place", "N/A"),
                "lat": node.lat,
                "lon": node.lon
            })
    
    # Convert the city data into a Pandas DataFrame
    df = pd.DataFrame(city_data)
    
    return df



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



def calculate_closest_amenity(property_df, amenity_df):
    '''
    Uses Euclidean distance to compare and find the closest amenity to each 
    given property and amenity dataframes and adds them to the property_df
    '''

    # Creates a copy of each dataframe to remove warning
    pdf = property_df.copy()
    adf = amenity_df.copy()
    
    # Ensure that the latitude and longitude are converted to floats
    pdf['latitude'] = pdf['latitude'].astype(float)
    pdf['longitude'] = pdf['longitude'].astype(float)
    adf['lat'] = adf['lat'].astype(float)
    adf['lon'] = adf['lon'].astype(float)

    # Extract lat/lon as numpy arrays
    property_coords = pdf[['latitude', 'longitude']].to_numpy()
    amenity_coords = adf[['lat', 'lon']].to_numpy()
    
    # Calculate distance matrix between all properties and the amenity
    dist_matrix = distance_matrix(property_coords, amenity_coords)
    
    # Find the index of the minimum distance (closest amenity) for each property
    closest_amenity_indices = dist_matrix.argmin(axis=1)
    
    # Add closest amenity name, latitude, and longitude to pdf
    pdf['amenity_lat'] = adf.iloc[closest_amenity_indices]['lat'].values
    pdf['amenity_lon'] = adf.iloc[closest_amenity_indices]['lon'].values

    return pdf



def get_batch_distances(df, api_keys, p_lat, p_lon, a_lat, a_lon, batch_size=50):
    '''
    Makes batch api calls to Open Route Services to calculate the driving distance between
    each property and amenity pair given, and returns the distances of each pair
    '''

    # Initialising the return list and api key index
    all_distances = []
    current_key = 0

    # Setting the client to make api calls with the given api key
    client = Client(key=api_keys[current_key])
    
    # Loops through the dataframe in the size of one batch at a time
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i + batch_size]
        
        # Prepare coordinates: first half are properties, second half are stations
        coords = [[row[p_lon], row[p_lat]] for _, row in batch.iterrows()] + \
                 [[row[a_lon], row[a_lat]] for _, row in batch.iterrows()]
        
        # Error handling variables
        retries = 0  # Track retries for a batch
        max_retries = 3  # Limit retries to avoid infinite loops

        while retries < max_retries:
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

                    if isinstance(distance, (int, float)):
                        all_distances.append(distance / 1000)  # Convert from meters to kilometers
                    else:
                        all_distances.append(None)
                break # Successfully completed this batch, move to next batch

            except Exception as e:
                print(f"Error with batch {(i/batch_size)+1}: {e}")

                # Handle error in calculation
                if 'unsupported operand type' in str(e):
                    print("Cannot complete batch request on this set of properties. Skipping to next batch.\n")
                    retries=max_retries
                # Handle daily limit exceeded
                elif "403" in str(e) and "Quota exceeded" in str(e):
                    print(f"Quota limit exceeded for API key {api_keys[current_key]}")
                    current_key += 1  # Switch to the next API key

                    if current_key >= len(api_keys):
                        print("All API keys exhausted. Stopping the API calls...\n")
                        all_distances.extend([None] * len(batch))  # Append None for failed requests
                        return all_distances  # Exit if all keys are exhausted
                    
                    # Set new API key and wait before retrying
                    client = Client(key=api_keys[current_key])
                    print("Using a new key...\nWaiting for 5 seconds before continuing.")
                    time.sleep(5)
                    retries += 1  # Increment retries counter

                # Handle rate limit exceeded
                elif "403" in str(e):
                    print("Rate limit exceeded. Waiting for 60 seconds...")
                    time.sleep(60)  # Wait for a minute before retrying
                    retries += 1  # Increment retries counter

                # Handle other errors (e.g., HTTP 502)
                else:
                    print(f"Unexpected error occurred: {e}.\nRetrying after 3 seconds...")
                    time.sleep(3)  # Wait before retrying
                    retries += 1  # Increment retries counter

        # If retries exceeded max_retries, append None for this batch
        if retries>=max_retries:
            print(f"Maximum retries reached for batch {(i/batch_size)+1}, skipping to next batch...\n")
            all_distances.extend([None] * len(batch))

        # Respect the rate limit by adding a delay between batches
        time.sleep(2)  
    
    return all_distances



def get_dist_to_city(property_df, cities_df, api_keys):
    '''
    Sets and runs the pipeline to find the closest city for each property and uses ORS to find the 
    driving distance. Returns the dataframe with the distances to the closest city
    '''
 
    # Step 1: Calculate the closest city using Euclidean distance
    property_df = calculate_closest_amenity(property_df, cities_df)

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
    property_df["dist_to_city"] = distances

    # Step 4: Remove the unneded columns
    property_df = property_df.drop(["amenity_lat", "amenity_lon"], axis=1)

    return property_df



def get_amenity_distances(property_df, amenity_dfs, api_keys):
    '''
    Sets and runs the pipeline to find the closest amenity for each property and uses ORS to find the 
    driving distance. Returns the dataframe with the distances to the closest amenity
    '''

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


