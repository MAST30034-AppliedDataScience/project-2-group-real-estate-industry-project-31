import re
import pandas as pd
from shapely.geometry import Point
import geopandas as gpd

# Function to extract weekly cost
def extract_weekly_cost(cost_text):
    # Define regex patterns for weekly, annual, and monthly costs
    weekly_patterns = [r"(\$[\d,]+\.?\d*)\s*(?:per week|pw|p/w|p\.w\.|week)", r"(\$[\d,]+\.?\d*)\s*weekly"]
    annual_patterns = [r"(\$[\d,]+\.?\d*)\s*(?:p\.a\.|pa|annum|per year|annual|year)"]
    monthly_patterns = [r"(\$[\d,]+\.?\d*)\s*(?:p/m|month|pm|p\.m\.)"]
    seasonal_pattern = r'\b(season)\b'

    # Try matching weekly patterns
    for pattern in weekly_patterns:
        match = re.search(pattern, cost_text, re.IGNORECASE)
        if match:
            return float(match.group(1).replace('$', '').replace(',', ''))

    # Try matching annual patterns and divide by 52 to get weekly cost
    for pattern in annual_patterns:
        match = re.search(pattern, cost_text, re.IGNORECASE)
        if match:
            return float(match.group(1).replace('$', '').replace(',', '')) / 52
    
    # Try matching monthly patterns and divide by 4.3 to get weekly cost
    for pattern in monthly_patterns:
        match = re.search(pattern, cost_text, re.IGNORECASE)
        if match:
            return float(match.group(1).replace('$', '').replace(',', '')) / 4.3

    # Check for seasonal cost (divide by 13 for an estimate of weekly cost)
    if re.search(seasonal_pattern, cost_text, re.IGNORECASE):
        match = re.search(r"\$([\d,]+\.?\d*)", cost_text)
        if match:
            return float(match.group(1).replace('$', '').replace(',', '')) / 13

    # If there's only a number or just a dollar amount, assume it's the weekly cost
    match = re.search(r"^\$?([\d,]+\.?\d*)$", cost_text.strip())
    if match:
        return float(match.group(1).replace('$', '').replace(',', ''))

    # If none of the patterns match, return None
    return None



def extract_house_details(df):
    """
    Extracts the address, suburb, and postcode from the 'name' column of the DataFrame,
    converts them to lowercase, sets the 'date_available' column to '09/24', 
    and drops unnecessary columns For data merging. 
    """

    # Define regex pattern for extraction
    pattern = r'^(.*?[-/]?\d*.*?)\s*,\s*(.*?)\s*VIC\s*(\d{4})$'
    
    # Extract address, suburb, and postcode using regex
    df[['address', 'suburb', 'postcode']] = df['name'].str.extract(pattern)

    # Convert to lowercase
    df['address'] = df['address'].str.lower()
    df['suburb'] = df['suburb'].str.lower()
    df['postcode'] = df['postcode'].str.lower()

    df['beds'] = df['rooms'].apply(lambda x: int(x[0].split()[0]) if isinstance(x, list) and len(x) > 0 else 0)
    df['baths'] = df['rooms'].apply(lambda x: int(x[1].split()[0]) if isinstance(x, list) and len(x) > 1 else 0)


    # Fill NaN values with 0 for beds and baths, then convert to integers
    df['beds'] = df['beds'].fillna(0).astype(int)
    df['baths'] = df['baths'].fillna(0).astype(int)

    # Extract parking spots (assuming 'parking' is in the format "1 Parking")
    #df['parking'] = df['parking'].apply(lambda x: int(x.split()[0]) if isinstance(x, str) and x else 0)
    #df['parking_1'] = df['parking'].apply(lambda x: int(x[0].split()[0]) if isinstance(x, list) and len(x) > 0 else 0)
    df['parking'] = df['parking'].apply(extract_parking)


    # Set date_available column to '09/24'
    df['date_available'] = 2024

    # Drop unnecessary columns
    df = df.drop(columns=['cost_text', 'desc', 'property_features', 'name', 'rooms', 'bond'], errors='ignore')

    return df

# Extract parking spots (assuming 'parking' is in the format ["1 Parking"])
def extract_parking(parking_list):
    if isinstance(parking_list, list) and len(parking_list) > 0:
        try:
            return int(parking_list[0].split()[0])
        except (ValueError, IndexError):
            return 0
    return 0

def extract_latitude(coordinate):
    if isinstance(coordinate, list) and len(coordinate) == 2:
        return coordinate[0]
    else:
        return None

def extract_longitude(coordinate):
    if isinstance(coordinate, list) and len(coordinate) == 2:
        return coordinate[1]
    else:
        return None

def clean_property_type(df):
    """
    Cleans the 'property_type' column by performing the following operations:
    - Drops entries that are 'Carspace', 'Acreage / Semi-Rural', 'Vacant land', or 'New land'
    - Combines 'New House & Land' with 'House'
    - Combines 'New Apartments / ...' with 'Apartment / Unit ...' and renames to 'Unit'
    - Changes 'Block of Units' to 'Units'
    - Renames 'Semi-Detached' to 'Duplex'
    - Drops any rows with 'Carspace', 'Acreage / Semi-Rural', 'Vacant land', 'New land'
    """

    # Define the mapping of property types to their cleaned versions
    property_type_map = {
        'Townhouse': 'Townhouse',
        'Semi-Detached': 'Duplex',
        'Studio': 'Studio',
        'New land': None,  # Mark for dropping
        'Villa': 'Villa',
        'Apartment / Unit / Flat': 'Unit',
        'House': 'House',
        'New Apartments / Unit / Flat': 'Unit',
        'Block of Units': 'Unit',
        'New House & Land': 'House',
        'Terrace': 'Terrace',
        'Vacant land': None,  # Mark for dropping
        'Acreage / Semi-Rural': None,  # Mark for dropping
        'Duplex': 'Duplex',
        'Carspace': None  # Mark for dropping
    }

    # Map the property_type column to the new values
    df['property_type'] = df['property_type'].map(property_type_map)

    # Drop rows where property_type is None (i.e., the dropped types)
    df = df.dropna(subset=['property_type'])

    return df

def combine_SA2(df):
    """
    Accepts a dataframe and column as input. The 'column' input is a string which corresponds to the column name within the dataframe that specifies the coordinates of each listing.
    Returns a dataframe, similar to the 'df' input, with SA2 information appended
    """
    sf = gpd.read_file("../data/SA2/SA2_extracted/SA2_2021_AUST_GDA2020.shp") # read SA2 shapefile
    sf = sf[sf['STE_NAME21'] == 'Victoria'] # remove all instances not in victoria

    # create geometry column in dataframe
    df = df.dropna(subset=['longitude'])
    df['longitude'] = df['longitude'].astype(float)
    df['latitude'] = df['latitude'].astype(float)
    df['point'] = df.apply(lambda row: Point(row['longitude'], row['latitude']), axis=1)

    gdf_points = gpd.GeoDataFrame(df, geometry='point', crs='EPSG:4326')
    gdf_joined = gpd.sjoin(gdf_points, sf, how='left', op='within') # join our SA2 points with all listings
    
    # drop all irrelevant columns
    gdf_joined = gdf_joined.drop(['index_right', 'CHG_FLAG21', 'CHG_LBL21',	'SA3_CODE21', 'LOCI_URI21', 'AUS_NAME21', 'AUS_CODE21', 'STE_NAME21', 'STE_CODE21', 'SA3_NAME21', 'SA4_CODE21', 'SA4_NAME21', 'GCC_CODE21'], axis=1)

    return gdf_joined


def check_empty_or_zero(coord_list):
    """
    Function to check if a list is empty or contains 0s
    """
    if isinstance(coord_list, list):
        return len(coord_list) == 0 or '0' in coord_list
    return False
