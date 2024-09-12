import pandas as pd
import folium



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