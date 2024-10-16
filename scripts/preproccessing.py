## Python script with functions to aid in the preprocessing of the domain property data ##

import re
import os
import pandas as pd
from shapely.geometry import Point
import geopandas as gpd
import numpy as np
import matplotlib.pyplot as plt
import statsmodels.api as sm



def extract_weekly_cost(cost_text):
    '''
    Extracts the weekly price from the given text description and returns the 
    extracted value
    '''

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
    df['parking'] = df['parking'].apply(extract_parking)


    # Set date_available column to '09/24'
    df['date_available'] = 2024

    # Drop unnecessary columns
    df = df.drop(columns=['cost_text', 'desc', 'property_features', 'name', 'rooms', 'bond'], errors='ignore')

    return df



def extract_parking(parking_list):
    '''
    Extracts and returns the number of parking spots from the given list
    (assuming 'parking' is in the format ["1 Parking"])
    '''

    if isinstance(parking_list, list) and len(parking_list) > 0:
        try:
            return int(parking_list[0].split()[0])
        except (ValueError, IndexError):
            return 0
    return 0



def extract_latitude(coordinate):
    '''
    Extracts and returns the latitude from the given coordinates
    '''

    if isinstance(coordinate, list) and len(coordinate) == 2:
        return coordinate[0]
    else:
        return None



def extract_longitude(coordinate):
    '''
    Extracts and returns the longitude from the given coordinates
    '''
        
    if isinstance(coordinate, list) and len(coordinate) == 2:
        return coordinate[1]
    else:
        return None



def clean_property_type(df):
    '''
    Cleans the 'property_type' column by performing the following operations:
    - Drops entries that are 'Carspace', 'Acreage / Semi-Rural', 'Vacant land', or 'New land'
    - Combines 'New House & Land' with 'House'
    - Combines 'New Apartments / ...' with 'Apartment / Unit ...' and renames to 'Unit'
    - Changes 'Block of Units' to 'Units'
    - Renames 'Semi-Detached' to 'Duplex'
    - Drops any rows with 'Carspace', 'Acreage / Semi-Rural', 'Vacant land', 'New land'
    '''

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



def extract_suburb(address):
    '''
    Extracts and returns the suburb name from the given address
    '''

    # Split the address by comma
    parts = address.split(',')

    # If the first part is empty or a number (address), take the second part
    #if parts[0].strip().isdigit() or not parts[0].strip():
    if len(parts) > 1:
        return parts[1].strip()  # Return the second part, stripped of any leading/trailing whitespace
    elif len(parts) == 1 and parts[0].strip():  # Check if the single part is not just whitespace
        # Return the part as it is likely the suburb name without additional details
        return parts[0].strip()
    else:
        # If the first part is not empty or purely numerical, return it as the suburb
        return parts[0].strip()



def combine_SA2(df):
    '''
    Accepts a dataframe and column as input. The 'column' input is a string which corresponds to the column name within the dataframe that specifies the coordinates of each listing.
    Returns a dataframe, similar to the 'df' input, with SA2 information appended
    '''

    sf = gpd.read_file("../data/SA2/SA2_extracted/SA2_2021_AUST_GDA2020.shp") # read SA2 shapefile
    sf = sf[sf['STE_NAME21'] == 'Victoria'] # remove all instances not in victoria

    # create geometry column in dataframe
    df = df.dropna(subset=['longitude'])
    df['longitude'] = df['longitude'].astype(float)
    df['latitude'] = df['latitude'].astype(float)
    df['point'] = df.apply(lambda row: Point(row['longitude'], row['latitude']), axis=1)

    gdf_points = gpd.GeoDataFrame(df, geometry='point', crs='EPSG:4326')
    gdf_joined = gpd.sjoin(gdf_points, sf, how='left', predicate='within') # join our SA2 points with all listings
    
    # drop all irrelevant columns
    gdf_joined = gdf_joined.drop(['index_right', 'CHG_FLAG21', 'CHG_LBL21',	'SA3_CODE21', 'LOCI_URI21', 'AUS_NAME21', 'AUS_CODE21', 'STE_NAME21', 'STE_CODE21', 'SA3_NAME21', 'SA4_CODE21', 'SA4_NAME21', 'GCC_CODE21'], axis=1)

    return gdf_joined



def check_empty_or_zero(coord_list):
    '''
    Function to check if a list is empty or contains 0s
    '''

    if isinstance(coord_list, list):
        return len(coord_list) == 0 or '0' in coord_list
    return False



def get_majority_non_na_columns(df):
    '''
    Function to check if more than 30% of a column in a pandas dataframe is NaN
    Accepts a pandas dataframe 'pd' as input
    Returns a list of column names 'columns' that are more than 70% not NaN
    '''

    columns = []
    
    for column in df.columns:
        non_nan_count = df[column].notna().sum()  # count non NaN
        total_count = df[column].shape[0]  # total number of values
        
        # check if the majority of values are not NaN
        if non_nan_count > (7 * total_count / 10):
            columns.append(column)
    
    return columns



def extend_data(df, data):
    '''
    Function to extend range of data to our required years from 2006-2029
    Accepts a dataframe 'df' with the data that needs to be extended, and a string 'data'
    representing the name of the data we are extending
    Displays a scatterplot of our newly extended data and returns a dataframe, 'extended_df',
    which contains all of our extended data for our desired years
    '''
    
    df.replace('-', np.nan, inplace=True) # replace '-' values with NaN

    # extract the columns that have majority not NaN values
    columns = get_majority_non_na_columns(df)
    df = df[columns]

    # impute any remaining NaN values with the mean
    rows_with_nan = df.index[df.isna().any(axis=1)].tolist()

    # display the result
    print("\nRows with NaN values:", rows_with_nan)

    df.fillna(df.mean(), inplace=True)

    # ensure no imputed values are below 0
    df[df < 0] = 0

    # create years to be extrapolated from
    years = np.array(list(df))
    extended_data = []
    extended_years = np.arange(2006, 2030, 1)

    # extend the data for each desired year for each SA2 region
    for index, row in df.iterrows():
        values = row.values  # get the values for the region

        # set up data for OLS Regression
        X = sm.add_constant(years)  # add constant term
        y = values  

        # fit the model
        model = sm.OLS(y, X).fit()

        # similarly, set up prediciton data
        extended_X = sm.add_constant(extended_years)  

        extended_values = model.predict(extended_X)

        # ensure no extrapolated values are below 0
        extended_values[extended_values < 0] = 0
        
        extended_data.append(extended_values)


    # convert to dataframe
    extended_df = pd.DataFrame(extended_data, columns=extended_years, index=df.index)

    # now lets plot out data:
    sampled_regions = df.sample(n=10, random_state=13)

    # create colours
    colors = plt.cm.viridis(np.linspace(0, 1, len(sampled_regions)))

    plt.figure(figsize=(10, 6))
    for color, (index, row) in zip(colors, sampled_regions.iterrows()):
        # plot extended data
        plt.plot(extended_years, extended_df.loc[index], color=color, label=index + ' Extended')
        # plot original data points
        plt.plot(years, row, 'o', color=color)  

    plt.title(f'Extrapolation of {data} to the years 2006-2029')
    plt.xlabel('Year')
    plt.ylabel(f'{data}')
    plt.xticks(np.arange(2005, 2030, 1), rotation=90)
    plt.legend(loc='center left', bbox_to_anchor=(1, 0.5), title="Regions")

    plt.grid(True)
    plt.show()

    return extended_df



def extend_inflation(df, data):
    '''
    Function to extend range of data to our required years from 2006-2029
    Accepts a dataframe 'df' with the data that needs to be extended, and a string 'data'
    representing the name of the data we are extending
    Displays a scatterplot of our newly extended data and returns a dataframe, 'extended_df',
    which contains all of our extended data for our desired years
    '''
    
    df.replace('-', np.nan, inplace=True) # replace '-' values with NaN

    # extract the columns that have majority not NaN values
    columns = get_majority_non_na_columns(df)
    df = df[columns]

    # impute any remaining NaN values with the mean
    rows_with_nan = df.index[df.isna().any(axis=1)].tolist()

    # Display the result
    print("\nRows with NaN values:", rows_with_nan)

    df.fillna(df.mean(), inplace=True)

    # create years to be extrapolated from
    years = np.array(list(df))
    extended_data = []
    extended_years = np.arange(2006, 2030, 1)

    # extend the data for each desired year for each SA2 region
    for index, row in df.iterrows():
        values = row.values  # get the values for the region

        # set up data for OLS Regression
        X = sm.add_constant(years)  # add constant term
        y = values  

        # fit the model
        model = sm.OLS(y, X).fit()

        # similarly, set up prediciton data
        extended_X = sm.add_constant(extended_years)  

        extended_values = model.predict(extended_X)
        
        extended_data.append(extended_values)

    # convert to dataframe
    extended_df = pd.DataFrame(extended_data, columns=extended_years, index=df.index)

    # now lets plot our data

    # create our colours
    colors = plt.cm.viridis(np.linspace(0, 1, len(extended_df)))

    plt.figure(figsize=(10, 6))

    # Iterate through each row in the DataFrame
    for color, (index, row) in zip(colors, extended_df.iterrows()):
        # Plot extended data
        plt.plot(extended_years, row, color=color, label=index + ' Extended')
        
        # Plot original data points if needed (assuming you have original years and data)
        plt.plot(years, df.loc[index], 'o', color=color)  

    plt.title(f'Extrapolation of {data} to the years 2006-2029')
    plt.xlabel('Year')
    plt.ylabel(f'{data}')
    plt.xticks(np.arange(2006, 2030, 1), rotation=90)
    plt.legend(loc='center left', bbox_to_anchor=(1, 0.5), title="Inflation Type")

    plt.grid(True)
    plt.show()

    return extended_df



def add_data(df):
    '''
    Function created to add external datasets to our houses dataframes, inputting the correct
    values depending on each house's year and SA2 region.
    Accepts 'df' as input, a dataframe of house info. and returns the same dataframe with the 
    external data appended.
    Relies on 'df' having a 'year' column with type string and a 'SA2_NAME21' column.
    '''

    extended_homelessness_df = pd.read_csv('../data/curated/extrapolated_homelessness_data.csv').set_index('SA2_name_2021')
    extended_ave_household_size_df = pd.read_csv('../data/curated/extrapolated_ave_household_size.csv').set_index('SA2_name_2021')
    extended_business_df = pd.read_csv('../data/curated/extrapolated_business.csv').set_index('SA2_name_2021')
    extended_income_df = pd.read_csv('../data/curated/extrapolated_income.csv').set_index('SA2_name_2021')
    extended_housing_index_df = pd.read_csv('../data/curated/extrapolated_housing_index.csv')
    extended_cpi_without_housing_df = pd.read_csv('../data/curated/extrapolated_CPI_without_housing.csv')
    extended_median_age_df = pd.read_csv('../data/curated/extrapolated_median_age.csv').set_index('SA2_name_2021')
    extended_median_rent_df = pd.read_csv('../data/curated/extrapolated_median_rent.csv').set_index('SA2_name_2021')
    extended_percentage_aboriginal_torres_straight_df = pd.read_csv('../data/curated/extrapolated_percentage_aboriginal_torres_straight.csv').set_index('SA2_name_2021')
    extended_percentage_australian_citizen_df = pd.read_csv('../data/curated/extrapolated_percentage_australian_citizen.csv').set_index('SA2_name_2021')
    extended_percentage_overseas_born_df = pd.read_csv('../data/curated/extrapolated_percentage_overseas_born.csv').set_index('SA2_name_2021')
    extended_percentage_rentals_df = pd.read_csv('../data/curated/extrapolated_percentage_rentals.csv').set_index('SA2_name_2021')
    extended_population_df = pd.read_csv('../data/curated/extrapolated_population.csv')
    extended_unemployment_df = pd.read_csv('../data/curated/extrapolated_unemployment.csv').set_index('SA2_name_2021')


    extended_dfs = [
        (extended_homelessness_df, 'num_homeless_persons'),
        (extended_ave_household_size_df, 'avg_household_size'),
        (extended_business_df, 'num_businesses'),
        (extended_income_df, 'median_income'),
        (extended_median_age_df, 'median_age'),
        (extended_median_rent_df, 'median_weekly_rent'),
        (extended_percentage_aboriginal_torres_straight_df, 'percent_aboriginal_torres_strait_islander'),
        (extended_percentage_australian_citizen_df, 'percent_au_citizen'),
        (extended_percentage_overseas_born_df, 'percent_overseas_born'),
        (extended_percentage_rentals_df, 'percent_rental_properties'),
        (extended_population_df, 'population'),
        (extended_unemployment_df, 'percent_unemployed')
    ]

    # Loop through each extended dataframe and merge it with df
    for extended_df, col_name in extended_dfs:
        # Add the new column to the original DataFrame
        df[col_name] = df.apply(lambda row: get_value_or_mean(row['SA2_NAME21'], row['year'], extended_df), axis=1)

    # now lets do the same for inflation
    extended_housing_index_df = extended_housing_index_df.stack().reset_index()
    extended_housing_index_df.columns = ['Metric', "year", 'housing_index']
    extended_housing_index_df = extended_housing_index_df.drop('Metric', axis=1)

    df = df.merge(extended_housing_index_df, on='year', how='left', suffixes=('', '_dup'))

    extended_cpi_without_housing_df = extended_cpi_without_housing_df.stack().reset_index()
    extended_cpi_without_housing_df.columns = ['Metric', "year", 'cpi_without_housing']
    extended_cpi_without_housing_df = extended_cpi_without_housing_df.drop('Metric', axis=1)

    df = df.merge(extended_cpi_without_housing_df, on='year', how='left', suffixes=('', '_dup'))
    # Drop the duplicated columns immediately after each merge
    df.drop([col for col in df.columns if 'dup' in col], axis=1, inplace=True)

    return df



def get_value_or_mean(sa2_name, year, extended_df):
    '''
    Function to extract and return the value or impute the mean 
    for the given year if a value is not found
    '''

    try:
        # Attempt to retrieve the value from the extended DataFrame
        value = extended_df.loc[str(sa2_name), str(year)]
        return value
    except KeyError:
        # If SA2 region or year is not found, return the mean value for that year
        mean_value = extended_df[str(year)].mean()
        return mean_value
    


def split_by_gcc(listings_df, output_dir, data_name):
    '''
    Splits the given dataframe by Greater Melbourne and the rest of Victoria
    then saves the split dataframes in the given directory
    '''

    out_dir = f"{output_dir}/{data_name}/"

    # Creates the directory if it doesn't already exist    
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    # Drop duplicates
    listings_df = listings_df.drop_duplicates()

    # Drop rows where longitude and latitude are NaN, as this is required later
    listings_df = listings_df.dropna(subset=['longitude', 'latitude'])

    # Determine whether property is located in Greater Melbourne or Rest of Victoria
    combined_df = combine_SA2(listings_df)
    combined_df = combined_df.drop(['point', 'AREASQKM21'], axis=1)
    
    greater_melb_pd = combined_df[combined_df['GCC_NAME21'] == "Greater Melbourne"]
    rest_of_vic_pd = combined_df[combined_df['GCC_NAME21'] == "Rest of Vic."]
    
    # Save to CSV
    greater_melb_pd.to_csv(f"{out_dir}/gm_{data_name}.csv")
    rest_of_vic_pd.to_csv(f"{out_dir}/rv_{data_name}.csv")
    
    return


