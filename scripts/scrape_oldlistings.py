import geopandas as gpd
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os

def scrape_postcodes():
    url = "https://www.onlymelbourne.com.au/melbourne-postcodes"

    # Send a request to the URL
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for HTTP errors
    
    # Parse the content using BeautifulSoup
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Extract all text and filter those matching the postcode pattern
    texts = soup.find_all(text=True)
    postcodes = []
    suburbs = []
    
    for text in texts:
        stripped_text = text.strip()
        if stripped_text and stripped_text[:4].isdigit() and " " in stripped_text:  # Check if the text starts with digits and contains a space
            potential_postcode, potential_suburb = stripped_text.split(" ", 1)  # Split at the first space
            if potential_postcode.isdigit() and potential_suburb.isalpha():  # Further validation for format
                postcodes.append(potential_postcode)
                suburbs.append(potential_suburb)

    # Create a DataFrame from the collected data
    df = pd.DataFrame({'Suburb': suburbs, 'Postcode': postcodes})
    out_dir = "../data/landing/postcodes/"
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    df.to_csv(f"{out_dir}postcodes.csv")
    return 