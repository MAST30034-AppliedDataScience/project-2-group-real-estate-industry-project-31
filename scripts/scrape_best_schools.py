# IMPORT LIBRARIES
import re
from json import dump
from tqdm import tqdm
from collections import defaultdict
import urllib.request
from bs4 import BeautifulSoup
from urllib.request import urlopen, Request
import pyarrow as pa
import pyarrow.parquet as pq
from urllib.parse import urlparse, parse_qs
import requests
import pandas as pd
import os

# URL of the page
secondary_url = "https://bettereducation.com.au/school/secondary/vic/vic_top_secondary_schools.aspx"

# Send a GET request
secondary_response = requests.get(secondary_url)
secondary_response.raise_for_status()  # Raises an HTTPError for bad responses

# Parse the HTML content
secondary_soup = BeautifulSoup(secondary_response.text, 'html.parser')

# Find the table containing the data
secondary_table = secondary_soup.find('table')

# Initialise a list to hold the data 
data = []

# Extract school names, postcodes, and types from the table
for row in secondary_table.find_all('tr')[1:]:  # Skip the header row
    columns = row.find_all('td')
    school_name = columns[1].text.strip()
    postcode = columns[2].text.strip()  # Assuming postcode is second last column
    school_type = columns[-2].text.strip()
    education_level = "secondary"

    data.append((school_name, postcode, school_type, education_level))

    
# URL of the page
primary_url = "https://bettereducation.com.au/school/Primary/vic/vic_top_primary_schools.aspx"

# Send a GET request
primary_response = requests.get(primary_url)
primary_response.raise_for_status()  # Raises an HTTPError for bad responses

# Parse the HTML content
primary_soup = BeautifulSoup(primary_response.text, 'html.parser')

# Find the table containing the data
primary_table = primary_soup.find('table')

# Extract school names, postcodes, and types from the table
for row in primary_table.find_all('tr')[1:]:  # Skip the header row
    columns = row.find_all('td')
    school_name = columns[1].text.strip()
    postcode = columns[2].text.strip()  # Assuming postcode is second last column
    school_type = columns[-2].text.strip()
    education_level = "primary"

    data.append((school_name, postcode, school_type, education_level))
    
# Convert to a dataframe and write to parquet       
school_df = pd.DataFrame(data, columns=['school_name', 'postcode', 'school_type', "education_level"])

schema = pa.schema([
    ('school_name', pa.string()),
    ('postcode', pa.string()),
    ('school_type', pa.string()),
    ('education_level', pa.string())
])

# Apply schema
table = pa.Table.from_pandas(school_df, schema=schema)

out_dir = '../data/landing/school_data/'

if not os.path.exists(out_dir):
    os.makedirs(out_dir)
    
# Write to parquet
pq.write_table(table, f"{out_dir}school_data.parquet")

