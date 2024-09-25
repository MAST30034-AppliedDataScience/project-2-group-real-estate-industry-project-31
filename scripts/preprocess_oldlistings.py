from datetime import datetime
import os
from pyspark.sql.functions import lower, udf, col, when, explode, from_json, col, regexp_replace, regexp_extract, expr, trim, split, concat, lit, arrays_zip, ltrim
from pyspark.sql.types import StringType, ArrayType, IntegerType
from scripts.preproccessing import combine_SA2
import json


def preprocess_olist(spark):
    read_dir = '../data/landing/oldlisting/oldlisting.parquet'
    out_dir = '../data/raw/oldlisting/'
    
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    
    # Read in dataframe
    listings_df = spark.read.parquet(read_dir)
    
    # Drop Duplicates
    listings_df = listings_df.dropDuplicates()    
    # Preprocess dataframe 


    listings_df = lowercase_string_attributes(listings_df)
    
    # Format suburb name for readability
    listings_df = listings_df.withColumn("suburb", regexp_replace("suburb", "\+", " "))
    
    # Convert dates to mm-yy
    listings_df = listings_df.withColumn("dates", preprocess_dates(listings_df["dates"]))
    
    # Replace NULL values of No. Beds, Baths and parking spaces to 0.0. Remove listings with no beds
    listings_df = preprocess_bbp(listings_df)
    
    # Preprocess address
    listings_df = preprocess_address(listings_df)
    
    # Preprocess house types column
    listings_df = preprocess_house_type(listings_df)
    
    # Convert to weekly cost
    listings_df = get_weekly_price(listings_df)
    
    listings_df.show()
    
    listings_df.write.mode("overwrite").parquet(f"{out_dir}oldlisting.parquet")
    return

def lowercase_string_attributes(df):
    df = df.withColumn("address", lower(df["address"]))
    df = df.withColumn("house_type", lower(df["house_type"]))
    df = df.withColumn("suburb", lower(df["suburb"]))
    return df

def preprocess_bbp(df):
    df = df.fillna({'baths': 0, 'beds': 0, 'cars': 0})
    
    # Remove listings with 0 beds, 0 baths
    filtered_df = df.filter((col("beds") != 0) & (col("baths") != 0))
    df = df.withColumnRenamed("cars", "parking")
    return filtered_df

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
    df = df.filter(~col("house_type").isin(NON_RESIDENTIAL_HOUSE_TYPES))

    # Remove acreage, farm related properties as they are not relevant to anaysis
    df = df.filter(
        ~(col("house_type").contains("acreage") | 
        col("house_type").contains("farm"))
        )
    
    df = df.withColumn(
            "house_type", 
            when(col("house_type").contains("semi"), "duplex")
            .otherwise(
                when(
                    col("house_type").contains("unit") | 
                    col("house_type").contains("flat") |
                    col("house_type").contains("apartment"), "unit")
            .otherwise(
                when(
                    (col("house_type").contains("house")  & ~col("house_type").contains("townhouse")) |
                    col("house_type").contains("cottage") |
                    col("house_type").contains("home"), "house")
            .otherwise(
                when(
                    col("house_type").contains("residential") |
                    col("house_type").contains("rural") |
                    col("house_type").contains("alpine") |  
                    col("house_type").contains("rental") | 
                    col("house_type").contains("available"), "other")
            .otherwise(
                col("house_type"))
            ))))
    
    # Check if some units were incorrectly classified as other 
    df = df.withColumn("house_type", 
                   when(
                       (col("house_type") == "other") &
                       (col("beds") >= 1) & 
                       (col("unit").isNotNull()) & 
                       (col("baths") >= 1), 
                       "unit")
                   .otherwise(col("house_type")))
    df = df.withColumnRenamed("house_type", "property_type").drop("unit")
    return df

def preprocess_address(listings_df):
    # Create a dynamic regex pattern column
    listings_df = listings_df.withColumn("regex_pattern", concat(lit(r',?\s*\b'), col("suburb"), lit(r'\b')))

    # Use this pattern to replace suburb in the address
    listings_df = listings_df.withColumn("address", regexp_replace(col("address"), col("regex_pattern"), ""))
    
    listings_df = listings_df.drop("regex_pattern")
    
    # Split the address at '/' to separate unit and house number
    listings_df = listings_df.withColumn("split_address", split(col("address"), "/"))

    # Extract unit and house number, handling cases where no '/' is present
    listings_df = listings_df.withColumn("unit", when(col("split_address").getItem(1).isNotNull(),
                                                      col("split_address").getItem(0))
                                        .otherwise(None))

    # Optionally, clean up and remove temporary columns
    listings_df = listings_df.drop("street_parts", "split_address")
    
    return listings_df

def get_weekly_price(listings_df):
    listings_df = listings_df.withColumn("price_str", regexp_replace("price_str", "'", '"'))

    # Define the schema of the array inside the string
    array_schema = ArrayType(StringType())

    # Convert the string to an actual array using from_json
    listings_df = listings_df.withColumn("price_str", from_json("price_str", array_schema))

    # Explode the 'costs' array to flatten it
    df_flattened = listings_df.withColumn("date_price", explode(arrays_zip("dates", "price_str")))
    
    # Extract the date and price into separate columns
    df_flattened = df_flattened.withColumn("date", col("date_price.dates")) \
                            .withColumn("ind_price_str", col("date_price.price_str"))

    # Drop the struct column as it's no longer needed
    df_flattened = df_flattened.drop("date_price")
    
    # Define patterns
    range_pattern = r'(\$\d{1,3}(?:,\d{3})*|\d+)\s*-\s*(\$\d{1,3}(?:,\d{3})*|\d+)'  # To correctly capture ranges with or without commas and currency symbols
    # price_pattern = r'\$?(\d{1,3}(?:,\d{3})*|\d+)'  # To capture single prices
    price_pattern = r'\$?(\d+(?:,\d{3})*|\d+)'
    suffix_pattern = r'\s+([a-zA-Z\s]+)$'  # Capture suffixes that are words at the end of the string

    df_processed = df_flattened.withColumn("range", regexp_extract("ind_price_str", range_pattern, 0)) \
                    .withColumn("single_price", regexp_extract("ind_price_str", price_pattern, 0)) \
                    .withColumn("avg_price", when(col("range") != "",
                                                (expr("cast(regexp_replace(split(range, '-')[0], '[\$,]', '') as double)") +
                                                    expr("cast(regexp_replace(split(range, '-')[1], '[\$,]', '') as double)")) / 2)
                                            .otherwise(expr("cast(regexp_replace(single_price, '[\$,]', '') as double)"))) \
                    .withColumn("suffix", trim(regexp_extract("ind_price_str", suffix_pattern, 1)))

    # Define a classification of suffixes based on keywords
    df_classified = df_processed.withColumn("classification", 
                                when(col("suffix") == "", when(col("avg_price") >= 50000, "sale").otherwise("week"))
                                .when(lower(col("suffix")).rlike("(?<!million)week|pw|wk"), "week")
                                .when(lower(col("suffix")).rlike("month|pcm"), "month")
                                .when(lower(col("suffix")).rlike("annum|pa|annual"), "year")
                                .when(lower(col("suffix")).rlike("season|seasonally"), "season")
                                .otherwise("other"))

    df_adjusted = df_classified.withColumn("weekly_price",
                                when(col("classification") == "week", col("avg_price"))
                                .when(col("classification") == "month", col("avg_price") / 4.3)
                                .when(col("classification") == "year", col("avg_price") / 52)
                                .when(col("classification") == "season", col("avg_price") / 13)
                                )

    # Show results
    COLS_TO_DROP = ["price_str", "ind_price_str", "range", "single_price", "suffix", "dates", "classification", "avg_price"]
    df_adjusted = df_adjusted.drop(*COLS_TO_DROP)
    
    df_filtered = df_adjusted.filter(~(col("classification").isin("sale", "other")))
    
    return df_filtered

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
    
    pandas = sdf.toPandas()

    combined = combine_SA2(pandas)
    combined = combined.drop(['point', 'SA2_NAME21', 'AREASQKM21'], axis=1)
    
    greater_melb_pd = combined[combined['GCC_NAME21'] == "Greater Melbourne"]
    rest_of_vic_pd = combined[combined['GCC_NAME21'] == "Rest of Vic."]
    
    
    greater_melb_pd.to_csv(f"{out_dir}gm_domain.csv", index=False)
    rest_of_vic_pd.to_csv(f"{out_dir}rv_domain.csv", index=False)
    
    return
    
@udf(returnType=ArrayType(StringType()))
def preprocess_dates(date_str):
    # print(f" date col: {date_str}")
    # print(type(date_str))
    try:
        # Replace single quotes with double quotes to make it valid JSON
        json_string = date_str.replace("'", '"')
        # Parse the JSON string to a Python list
        dates = json.loads(json_string)
        # Convert each date string to "mm-yy" format
        processed_dates = [datetime.strptime(date, "%B %Y").strftime("%Y") for date in dates]
        return processed_dates
    except json.JSONDecodeError:
        # Return an empty list in case of JSON decode error
        print("JSON DECODE ERROR")
        return []
    except ValueError:
        # Handle cases where the date format is incorrect
        print("Format ERROR")
        return []

