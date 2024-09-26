from datetime import datetime
import os
from pyspark.sql.functions import lower, udf, col, when, explode, from_json, col, regexp_replace, regexp_extract, expr, trim, split, concat, lit, arrays_zip, ltrim
from pyspark.sql.types import StringType, ArrayType, IntegerType, StructType, StructField, DoubleType
from scripts.preproccessing import combine_SA2
import json
import pandas as pd
import numpy as np
from datetime import datetime



def preprocess_olist():
    read_dir = '../data/raw/oldlisting/'
    out_dir = '../data/raw/oldlisting/'
    if not os.path.exists(out_dir):
            os.makedirs(out_dir)
            
    DATASETS = ['gm_c+a_oldlisting.csv', 'rv_c+a_oldlisting.csv']
    
    for region in DATASETS: 
        
        # Read in dataframe
        listings_df = pd.read_csv(f"{read_dir}{region}")
        
        # Drop Duplicates
        listings_df = listings_df.drop_duplicates()    
        # Preprocess dataframe 

        listings_df = lowercase_string_attributes(listings_df)
        
        # Format suburb name for readability
        listings_df["suburb"] = listings_df["suburb"].str.replace("+", " ")
        
        # Convert dates to yyyy
        listings_df['dates'] = listings_df['dates'].apply(preprocess_dates)
        
        # Replace NULL values of No. Beds, Baths and parking spaces to 0.0. Remove listings with no beds
        listings_df = preprocess_bbp(listings_df)
        
        # Preprocess address
        listings_df = preprocess_address(listings_df)
        
        # Preprocess house types column
        listings_df = preprocess_house_type(listings_df)
        
        # Convert to weekly cost
        listings_df = get_weekly_price(listings_df)
        
        listings_df.show()
        listings_df.printSchema()
        
        if region == 'gm_c+a_oldlisting.csv':
            listings_df.pd.to_csv(f"{out_dir}gm_oldlisting_final.csv")
        else:
            listings_df.pd.to_csv(f"{out_dir}rv_oldlisting_final.csv")
    return

def lowercase_string_attributes(df):
    df['address'] = df['address'].str.lower()
    df['house_type'] = df['house_type'].str.lower()
    df['suburb'] = df['suburb'].str.lower()
    return df

def preprocess_bbp(df):
    df = df.fillna({'baths': 0, 'beds': 0, 'cars': 0})
    
    # Remove listings with 0 beds, 0 baths
    df[['baths', 'beds', 'cars']] = df[['baths', 'beds', 'cars']].fillna(0)
    df = df.rename(columns={'cars': 'parking'})
    return df[df['beds'] != 0]

def preprocess_house_type(df):
    # Remove non-residential listings
    NON_RESIDENTIAL_HOUSE_TYPES = ["commercial farming", "commercial", "industrial/warehouse",
                                   "shop(s)", "industrial/warehouse", "lifestyle", 
                                   "building - warehouse", "hotel/leisure", "retail", "offices", 
                                   "tourism", "warehouse", "medical/consulting", 
                                   "medical consulting","cropping", "other-", "holiday",
                                   "factory, warehouse", "development", "mixed use", 
                                   "land, warehouse", "land/development", "building",
                                   "block of flats", "vacant land", "industrial (com)",
                                   "restaurant/cafe", "industrial", "vacantland", "land"
                                   ]
    df = df[~df['house_type'].isin(NON_RESIDENTIAL_HOUSE_TYPES)]

    # Remove acreage, farm related properties as they are not relevant to analysis
    df = df[~df['house_type'].str.contains('acreage|farm', case=False, na=False)]

    # Apply conditions to modify the 'house_type' column
    conditions = [
        df['house_type'].str.contains('semi', case=False, na=False),
        df['house_type'].str.contains('unit|flat|apartment', case=False, na=False),
        df['house_type'].str.contains('house', case=False, na=False) & ~df['house_type'].str.contains('townhouse', case=False, na=False),
        df['house_type'].str.contains('cottage|home', case=False, na=False),
        df['house_type'].str.contains('residential|rural|alpine|rental|available', case=False, na=False),
    ]
    choices = ['duplex', 'unit', 'house', 'house', 'other']
    df['house_type'] = np.select(conditions, choices, default=df['house_type'])

    # Check if some units were incorrectly classified as other
    condition = (
        (df['house_type'] == 'other') &
        (df['beds'] >= 1) &
        (df['baths'] >= 1) &
        pd.notna(df['unit'])
    )
    df.loc[condition, 'house_type'] = 'unit'

    # Rename the 'house_type' column to 'property_type' and drop the 'unit' column if exists
    df = df.rename(columns={'house_type': 'property_type'})
    if 'unit' in df.columns:
        df = df.drop(columns='unit')

    return df

def preprocess_address(listings_df):
    # Use the suburb to create a regex pattern and remove it from the address
    listings_df['address'] = listings_df.apply(lambda row: row['address'].replace(row['suburb'], ''), axis=1)
    
    # Adding a flag column 'is_unit' to indicate addresses that are units (contains '/')
    listings_df['unit'] = listings_df['address'].apply(lambda x: '/' in x)

    return listings_df

def get_weekly_price(listings_df):
    # Step 1: Normalize JSON-like strings in 'price_str' and parse them into Python lists
    listings_df['price_str'] = listings_df['price_str'].apply(lambda x: json.loads(x.replace("'", '"')))

    # Step 2: Explode 'dates' and 'price_str' into row-wise combinations
    temp = listings_df.apply(lambda row: pd.Series(zip(row['dates'], row['price_str'])), axis=1).stack().reset_index(level=1, drop=True)
    temp_df = temp.apply(pd.Series)
    df_flattened = listings_df.join(temp_df)
    df_flattened.rename(columns={0: 'date', 1: 'ind_price_str'}, inplace=True)

    # Define regex patterns
    range_pattern = r'(\$\d{1,3}(?:,\d{3})*|\d+)\s*-\s*(\$\d{1,3}(?:,\d{3})*|\d+)'
    price_pattern = r'\$?(\d+(?:,\d{3})*|\d+)'
    suffix_pattern = r'\s+([a-zA-Z\s]+)$'

    # Step 3: Extract price information
    df_flattened['range'] = df_flattened['ind_price_str'].str.extract(range_pattern)[0]
    df_flattened['single_price'] = df_flattened['ind_price_str'].str.extract(price_pattern)[0]
    df_flattened['suffix'] = df_flattened['ind_price_str'].str.extract(suffix_pattern)[1]

    # Step 4: Calculate average price if a range is given, otherwise take the single price
    df_flattened['avg_price'] = np.where(df_flattened['range'].notna(),
                                         (df_flattened['range'].str.split('-').str[0].replace('[\$,]', '', regex=True).astype(float) +
                                          df_flattened['range'].str.split('-').str[1].replace('[\$,]', '', regex=True).astype(float)) / 2,
                                         df_flattened['single_price'].replace('[\$,]', '', regex=True).astype(float))

    # Step 5: Classify price frequency based on the suffix
    conditions = [
        df_flattened['suffix'].str.contains('week|pw|wk', case=False, na=False),
        df_flattened['suffix'].str.contains('month|pcm', case=False, na=False),
        df_flattened['suffix'].str.contains('annum|pa|annual', case=False, na=False),
        df_flattened['suffix'].str.contains('season|seasonally', case=False, na=False),
        (df_flattened['suffix'].isna() | df_flattened['suffix'].str.strip() == '') & (df_flattened['avg_price'] >= 50000)
    ]
    choices = ['week', 'month', 'year', 'season', 'sale']
    df_flattened['classification'] = np.select(conditions, choices, default='other')

    # Step 6: Convert all prices to weekly prices based on the classification
    conversion_factors = {'week': 1, 'month': 4.333, 'year': 52, 'season': 13}
    df_flattened['weekly_price'] = df_flattened.apply(
        lambda row: row['avg_price'] / conversion_factors.get(row['classification'], np.nan) if row['classification'] in conversion_factors else np.nan,
        axis=1
    )

    # Step 7: Drop intermediate columns and filter out unwanted classifications
    columns_to_drop = ['range', 'single_price', 'suffix', 'ind_price_str', 'avg_price', 'classification', 'date', 'price_str']
    df_flattened = df_flattened.drop(columns=columns_to_drop)
    df_flattened = df_flattened.dropna(subset=['weekly_price'])  # Filter out rows where weekly price could not be calculated

    return df_flattened

def create_forecast_template(spark):
    read_dir = '../data/raw/oldlisting/oldlisting.parquet'
    out_dir =  '../data/curated/properties.parquet'

    listings_df = spark.read.parquet(read_dir)
    listings_df = listings_df.drop("date", "weekly_price")
    print(listings_df.count())
    unique_properties=  listings_df.dropDuplicates(["address"])
    print(unique_properties.count())
    # Create DataFrame of years, to cross join with properties to create template for forecasting 
    # data 
    years = [2025, 2026, 2027, 2028, 2029]
    years_df = spark.createDataFrame(years, IntegerType()).toDF("year")
    
    template = unique_properties.crossJoin(years_df)
    template.write.mode("overwrite").parquet(out_dir)
    
def split_by_gcc(spark):
    read_dir = "../data/landing/oldlisting/oldlisting.parquet"
    write_dir = '../data/raw/oldlisting/'
    sdf = spark.read.parquet(read_dir)

    sdf = sdf.dropDuplicates() 
    sdf = sdf.dropna(subset=['longitude', 'latitude'])
    
    pandas = sdf.toPandas()

    combined = combine_SA2(pandas)
    combined = combined.drop(['point', 'SA2_NAME21', 'AREASQKM21'], axis=1)
    
    greater_melb_pd = combined[combined['GCC_NAME21'] == "Greater Melbourne"]
    rest_of_vic_pd = combined[combined['GCC_NAME21'] == "Rest of Vic."]
    
    
    greater_melb_pd.to_csv(f"{write_dir}gm_oldlisting.csv")
    rest_of_vic_pd.to_csv(f"{write_dir}rv_oldlisting.csv")
    
    return

def split_domain_by_gcc(spark):
    read_dir = "../data/raw/all_domain_properties.parquet"
    out_dir = '../data/raw/domain/'
    
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
        
    sdf = spark.read.parquet(read_dir)

    sdf = sdf.dropDuplicates() 
    sdf = sdf.dropna(subset=['longitude', 'latitude'])
    
    pandas = sdf.toPandas()

    combined = combine_SA2(pandas)
    combined = combined.drop(['point', 'SA2_NAME21', 'AREASQKM21'], axis=1)
    
    greater_melb_pd = combined[combined['GCC_NAME21'] == "Greater Melbourne"]
    rest_of_vic_pd = combined[combined['GCC_NAME21'] == "Rest of Vic."]
    
    
    greater_melb_pd.to_csv(f"{out_dir}gm_domain.csv", index=False)
    rest_of_vic_pd.to_csv(f"{out_dir}rv_domain.csv", index=False)
    
    return

def preprocess_dates(date_str):
    if not isinstance(date_str, str):
        return ["0000"]
    try:
        # Replace single quotes with double quotes to make it valid JSON
        json_string = date_str.replace("'", '"')
        # Parse the JSON string to a Python list
        dates = json.loads(json_string)
        # Convert each date string to "yyyy" format
        return [datetime.strptime(date, "%B %Y").strftime("%Y") for date in dates]
    except json.JSONDecodeError:
        return ["0000"]
    except ValueError:
        return ["0000"]

