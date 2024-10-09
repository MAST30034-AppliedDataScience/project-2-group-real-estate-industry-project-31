## Python script with functions to help scrape property data from oldlistings.com ##


import geopandas as gpd
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import re
import numpy as np
import time



def scrape_postcodes(url):
    '''
    Retrieves all the postcodes from the given url and saves them as a csv to the 
    given directory
    '''

    try:
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
            # Check if the text starts with digits and contains a space
            if stripped_text and stripped_text[:4].isdigit() and " " in stripped_text:  
                # Split at the first space
                potential_postcode, potential_suburb = stripped_text.split(" ", 1)  
                # Further validation for format
                if potential_postcode.isdigit() and potential_suburb.isalpha():  
                    postcodes.append(potential_postcode)
                    suburbs.append(potential_suburb)

        # Create a DataFrame from the collected data
        df = pd.DataFrame({'suburb_name': suburbs, 'postcode': postcodes})
        out_dir = "../data/landing/postcodes/"
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        df.to_csv(f"{out_dir}postcodes.csv")
    
    except:
        file_path = "../data/landing/postcode_html.txt"

        # Open and read the file
        with open(file_path, 'r', encoding='utf-8') as file:
            html_content = file.read()
        
        # Parse the content using BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Extract all text and filter those matching the postcode pattern
        texts = soup.find_all(text=True)
        postcodes = []
        suburbs = []
        
        for text in texts:
            stripped_text = text.strip()
            if stripped_text and stripped_text[:4].isdigit() and " " in stripped_text:
                potential_postcode, potential_suburb = stripped_text.split(" ", 1)
                if potential_postcode.isdigit():
                    potential_suburb = potential_suburb.strip().replace('.', '').replace(',', '')
                    if all(c.isalpha() or c.isspace() or c.isdigit() or c in ["-", "'", "&"] for c in potential_suburb):
                        postcodes.append(potential_postcode)
                        suburbs.append(potential_suburb)

        # Create a DataFrame from the collected data
        df = pd.DataFrame({'suburb': suburbs, 'postcode': postcodes})
        df = df.drop_duplicates()
        
        out_dir = "../data/landing/postcodes/"
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        df.to_csv(f"{out_dir}postcodes.csv", index=False)
    return 



def get_oldlisting_data():
    """
    DEPRECATED AS OLDLISTINGS HAS CHANGED ITS FORMAT:
    Scrapes historical property data from oldlistings.com, and saves as a .csv for each property 
    with the following features: 'suburb', 'postcode', 'address' 'latitude', 'longitude', 'beds', 
    'baths', 'cars', 'house_type', 'dates', 'price_str'. 
    """
    headers = {'User-Agent': (f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                f"(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 Edg/128.0.0.0"),
               'referer': "https://www.oldlistings.com.au/"}
    
    
    read_dir = "../data/landing/postcodes/postcodes.csv"
    suburbs_df = pd.read_csv(read_dir)
    suburbs_df = prep_suburb_names(suburbs_df)
    
    # List to hold all property data
    properties_list = []
    for index, suburb in suburbs_df.iterrows():
        suburb_name = suburb['suburb']
        postcode = suburb['postcode']
        print(f"Getting data for {suburb_name}, {postcode}. Suburb {index} of {len(suburbs_df)}")
        
        url_template = \
            f"https://www.oldlistings.com.au/real-estate/VIC/{suburb_name}/{postcode}/rent/"
            
        
        try:
            # Fetch the webpage
            response = requests.get(url_template, headers=headers)
            response.raise_for_status()  # Ensure the request was successful

            # Parse the HTML content
            soup = BeautifulSoup(response.text, 'html.parser')

            # Find the <p> tag containing number of records for the suburb
            p_tag = soup.find('p', class_='sub-page-h2')

            # Extract and print the text if the tag is found 
            if p_tag:
                # Get number of records
                num_records = int(re.findall(r'\d+', p_tag.text)[-1])
                records_per_page = float(re.findall(r'\d+', p_tag.text)[1])
                num_pages = int(np.ceil(num_records/records_per_page))
                print(f"num pages: {num_pages}")
            else:
                print("No listings found. Continuing with other URLS.")
                return

            # Only iterate through the first 50 pages per suburb maximum
            for page_num in range(1, min([num_pages + 1, 50])):
                time.sleep(2)
                url = \
                    f"https://www.oldlistings.com.au/real-estate/VIC/{suburb_name}/{postcode}/rent/{page_num}"
                try:
                    # Fetch the webpage
                    response = requests.get(url, headers=headers)
                    response.raise_for_status()  # Ensure the request was successful

                    # Parse the HTML content
                    soup = BeautifulSoup(response.text, 'html.parser')
                    # Find all properties
                    property_classes = ['property odd clearfix', 'property even clearfix']
                    properties = soup.find_all('div', class_=lambda x: x in property_classes)
                    
                    # Extract data from each property
                    for property in properties:
                        
                        # Extract Latitude and Longitude
                        latitude = property.get('data-lat', 'N/A')
                        longitude = property.get('data-lng', 'N/A')
                        
                        # Extract address
                        address_tag = property.find('h2', class_='address')
                        address = address_tag.text.strip() if address_tag else 'N/A'
                        
                        # Extract number of beds
                        beds_tag = property.find('p', class_='property-meta bed')
                        num_beds = beds_tag.find('span').next_sibling.strip() if beds_tag else 'N/A'
                        
                        # Extract number of baths
                        baths_tag = property.find('p', class_='property-meta bath')
                        num_baths = baths_tag.find('span').next_sibling.strip() if baths_tag else 'N/A'
                        
                        # Extract number of cars
                        cars_tag = property.find('p', class_='property-meta car')
                        num_cars = cars_tag.find('span').next_sibling.strip() if cars_tag else 'N/A'
                        
                        # Extract house type
                        type_tag = property.find('p', class_='property-meta type')
                        house_type = type_tag.find('span').next_sibling.strip() if type_tag else 'N/A'
                        
                        # Extract dates and prices
                        dates = []
                        prices = []
                        for li in property.find_all('li'):
                            span = li.find('span')
                            if span:
                                date = span.text.strip()
                                price = li.text.replace(span.text, '').strip()
                                dates.append(date)
                                prices.append(price)

                        # Append the extracted data to the list
                        properties_list.append({
                            'suburb': suburb_name,
                            'postcode': postcode,
                            'address': address,
                            'latitude': latitude,
                            'longitude': longitude,
                            'beds': num_beds,
                            'baths': num_baths,
                            'cars': num_cars,
                            'house_type': house_type,
                            'dates': dates,
                            'price_str': prices
                            
                        })
                except:
                    continue
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                print(f"Error 404: Not found {url_template}. Continuing with other URLs.")
            elif e.response.status_code == 403:
                print(f"Unfortunately you've been blocked")
                print(f"Saving progres... please try again later")
                
                # Save whatever data has been collected
                oldlisting_df = pd.DataFrame(properties_list)
                out_dir = "../data/landing/oldlisting/"
                if not os.path.exists(out_dir):
                    os.makedirs(out_dir)
                oldlisting_df.to_csv(f"{out_dir}oldlisting.csv", index=False)
                
                # Save which suburb scraper was up to
                
                # Filter the DataFrame to include only the specified suburb and all that follow
                remaining_suburbs_df = suburbs_df[suburbs_df['suburb'] >= suburb_name]

                # Save the filtered DataFrame to a new CSV file
                remaining_suburbs_df.to_csv(f"{out_dir}remaining_suburbs.csv", index=False)
                return
            else:
                print(f"HTTP error occurred: {e}")  # Handle other types of HTTP errors
        except requests.exceptions.RequestException as e:
            print(f"Network-related error occurred: {e}")
          
    oldlisting_df = pd.DataFrame(properties_list)
    out_dir = "../data/landing/oldlisting/"
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    oldlisting_df.to_csv(f"{out_dir}oldlisting.csv", index=False)
    return
    
def get_remaining_oldlisting_data():
    """
    DEPRECATED AS OLDLISTINGS HAS CHANGED ITS FORMAT:
    Scrapes the remaining suburbs from oldlisting, will save the newly written listing data into a 
    .csv and automatically update which suburbs are unscraped - function can be rerun multiple times
    to scrape all data
    """
    headers = {'User-Agent': (f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                f"(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 Edg/128.0.0.0"),
               'referer': "https://www.oldlistings.com.au/"}
    
    
    read_dir = "../data/landing/oldlisting/"
    suburbs_df = pd.read_csv(f"{read_dir}remaining_suburbs.csv")
    suburbs_df = prep_suburb_names(suburbs_df)

    prev_data_df = pd.read_csv(f"{read_dir}oldlisting.csv")
    
    # List to hold all property data
    properties_list = []
    for index, suburb in suburbs_df.iterrows():
        
        suburb_name = suburb['suburb']
        postcode = suburb['postcode']
        print(f"Getting data for {suburb_name}, {postcode}. Suburb {index} of {len(suburbs_df)}")
        
        url_template = \
            f"https://www.oldlistings.com.au/real-estate/VIC/{suburb_name}/{postcode}/rent/"
            
        
        try:
            # Fetch the webpage
            response = requests.get(url_template, headers=headers)
            response.raise_for_status()  # Ensure the request was successful

            # Parse the HTML content
            soup = BeautifulSoup(response.text, 'html.parser')

            # Find the <p> tag containing number of records for the suburb
            p_tag = soup.find('p', class_='sub-page-h2')

            # Extract and print the text if the tag is found 
            if p_tag:
                # Get number of records
                num_records = int(re.findall(r'\d+', p_tag.text)[-1])
                records_per_page = float(re.findall(r'\d+', p_tag.text)[1])
                num_pages = int(np.ceil(num_records/records_per_page))
                print(f"num pages: {num_pages}")
            else:
                print("No listings found. Continuing with other URLS.")
                return
            

            for page_num in range(1 , min([num_pages+1, 51])):
                time.sleep(2)
                url = \
                    f"https://www.oldlistings.com.au/real-estate/VIC/{suburb_name}/{postcode}/rent/{page_num}"
                try:
                    # Fetch the webpage
                    response = requests.get(url, headers=headers)
                    response.raise_for_status()  # Ensure the request was successful

                    # Parse the HTML content
                    soup = BeautifulSoup(response.text, 'html.parser')
                    # Find all properties
                    property_classes = ['property odd clearfix', 'property even clearfix']
                    properties = soup.find_all('div', class_=lambda x: x in property_classes)
                    
                    # Extract data from each property
                    for property in properties:
                        
                        # Extract Latitude and Longitude
                        # Extract latitude and longitude
                        latitude = property.get('data-lat', 'N/A')
                        longitude = property.get('data-lng', 'N/A')
                        
                        # Extract address
                        address_tag = property.find('h2', class_='address')
                        address = address_tag.text.strip() if address_tag else 'N/A'
                        
                        # Extract number of beds
                        beds_tag = property.find('p', class_='property-meta bed')
                        num_beds = beds_tag.find('span').next_sibling.strip() if beds_tag else 'N/A'
                        
                        # Extract number of baths
                        baths_tag = property.find('p', class_='property-meta bath')
                        num_baths = baths_tag.find('span').next_sibling.strip() if baths_tag else 'N/A'
                        
                        # Extract number of cars
                        cars_tag = property.find('p', class_='property-meta car')
                        num_cars = cars_tag.find('span').next_sibling.strip() if cars_tag else 'N/A'
                        
                        # Extract house type
                        type_tag = property.find('p', class_='property-meta type')
                        house_type = type_tag.find('span').next_sibling.strip() if type_tag else 'N/A'
                        
                        # Extract dates and prices
                        dates = []
                        prices = []
                        for li in property.find_all('li'):
                            span = li.find('span')
                            if span:
                                date = span.text.strip()
                                price = li.text.replace(span.text, '').strip()
                                dates.append(date)
                                prices.append(price)

                        # Append the extracted data to the list
                        properties_list.append({
                            'suburb': suburb_name,
                            'postcode': postcode,
                            'address': address,
                            'latitude': latitude,
                            'longitude': longitude,
                            'beds': num_beds,
                            'baths': num_baths,
                            'cars': num_cars,
                            'house_type': house_type,
                            'dates': dates,
                            'price_str': prices
                            
                        })
                except:
                    continue
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                print(f"Error 404: Not found {url_template}. Continuing with other URLs.")
            elif e.response.status_code == 403:
                print(f"Unfortunately you've been blocked")
                print(f"Saving progress... please try again later")
                
                # Save whatever data has been collected
                newdata_df = pd.DataFrame(properties_list)
                combined_data_df = pd.concat([prev_data_df, newdata_df], ignore_index=True)
                
                out_dir = "../data/landing/oldlisting/"
                if not os.path.exists(out_dir):
                    os.makedirs(out_dir)
                combined_data_df.to_csv(f"{out_dir}oldlisting.csv", index=False)
                
                # Save which suburb scraper was up to
                
                # Filter the DataFrame to include only the specified suburb and all that follow
                remaining_suburbs_df = suburbs_df[suburbs_df['suburb'] >= suburb_name]

                # Save the filtered DataFrame to a new CSV file
                remaining_suburbs_df.to_csv(f"{out_dir}remaining_suburbs.csv", index=False)
                
                print(f"{index} more suburbs added in this run")
                return
            else:
                print(f"HTTP error occurred: {e}")  # Handle other types of HTTP errors
        except requests.exceptions.RequestException as e:
            print(f"Network-related error occurred: {e}")
           
    newdata_df = pd.DataFrame(properties_list)
    combined_data_df = pd.concat([prev_data_df, newdata_df], ignore_index=True)
    
    out_dir = "../data/landing/oldlisting/"
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    combined_data_df.to_csv(f"{out_dir}oldlisting.csv", index=False)
    return



def prep_suburb_names(suburb_df):
    suburb_df['suburb'] = suburb_df['suburb'].str.replace(' ', '+', regex=False)
    return suburb_df



def convert_csv_to_parquet():
    oldlisting_dir = '../data/landing/oldlisting/'
    df = pd.read_csv(f"{oldlisting_dir}oldlisting.csv")
    df.to_parquet(f"{oldlisting_dir}oldlisting.parquet")
    return

