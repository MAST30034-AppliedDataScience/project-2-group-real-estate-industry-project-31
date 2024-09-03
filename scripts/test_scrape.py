# JUST TESTING SOME SCRAPING OF DOMAIN.COM CURRENTLY FIGURING OUT HOW TO GET MORE ELEMENTS. 

# IMPORT LIBRARIES
import re
from json import dump
from tqdm import tqdm
from collections import defaultdict
import urllib.request
from bs4 import BeautifulSoup
from urllib.request import urlopen, Request
import numpy as np
from urllib.parse import urlparse, parse_qs

# CONSTANTS
BASE_URL = "https://www.domain.com.au"
N_PAGES = range(1, 3) # RIGHT NOW THIS ONLY LOOKS AT THE FIRST 3 PAGES FOR QUICK RUN TIME THIS NEEDS TO BE UPDATED TO
# SEARCH ALL THE PAGES!

# BEGIN SCRAPING:
url_links = []
property_metadata = defaultdict(dict)

# generate list of urls to visit
for page in N_PAGES:
    url = BASE_URL + f"/rent/VIC/?sort=price-desc&page={page}" # change this to just VIC, not melbourne-region-vic
    print(f"Visiting {url}")
    bs_object = BeautifulSoup(urlopen(Request(url, headers={'User-Agent':"PostmanRuntime/7.6.0"})), "lxml")

    # find the unordered list (ul) elements which are the results, then
    # find all href (a) tags that are from the base_url website.
    index_links = bs_object \
        .find(
            "ul",
            {"data-testid": "results"}
        ) \
        .findAll(
            "a",
            href=re.compile(f"{BASE_URL}/*") # the `*` denotes wildcard any
        )

    for link in index_links:
        # if its a property address, add it to the list
        if 'address' in link['class']:
            url_links.append(link['href'])



# for each url, scrape some basic metadata
pbar = tqdm(url_links[1:])
success_count, total_count = 0, 0
for property_url in pbar:
    bs_object = BeautifulSoup(urlopen(Request(property_url, headers={'User-Agent':"PostmanRuntime/7.6.0"})), "lxml")
    total_count += 1
    
    try: 
        # EDDITED WHAT THEY GAVE US TO MAKE IT SHORTER AND NEATER
        # looks for the header class to get property name
        property_metadata[property_url]['name'] = bs_object.find("h1", {"class": "css-164r41r"}).text

        # looks for the div containing a summary title for cost
        property_metadata[property_url]['cost_text'] = bs_object.find(
            "div", {"data-testid": "listing-details__summary-title"}).text

        # get rooms and parking
        rooms = bs_object.find("div", {"data-testid": "property-features"}).findAll(
            "span", {"data-testid": "property-features-text-container"})

        property_metadata[property_url]['rooms'] = [
            re.findall(r'\d+\s[A-Za-z]+', feature.text)[0] for feature in rooms if 'Bed' in feature.text or 'Bath' in feature.text
        ]

        property_metadata[property_url]['parking'] = [
            re.findall(r'\S+\s[A-Za-z]+', feature.text)[0] for feature in rooms if 'Parking' in feature.text
        ]

        # Get the  description
        property_metadata[property_url]['desc'] = bs_object.find("p").get_text(separator='\n').strip()

      
        # ADDITIONAL SCRAPING (OTHER THAN WHAT THEYVE GIVEN US):

        # Property type
        property_metadata[property_url]['property_type'] = bs_object.find(
            "div", {"data-testid": "listing-summary-property-type"}).find("span", {"class": "css-in3yi3"}).text

        # DATE AVAILABLE AND BOND
        ul_element = bs_object.find("div", {"data-testid": "strip-content-list"}).find("ul", {"data-testid": "listing-summary-strip"})
        li_elements = ul_element.find_all("li")

        # Initialize variables for data
        date_available = np.nan
        bond = np.nan

        # Iterate over each li element
        for li in li_elements:
            strong_tag = li.find("strong")
            if strong_tag:
                text = strong_tag.get_text(strip=True)  # Extract text from the strong tag
                li_text = li.get_text(strip=True)  # Extract text from the li element for descriptive context
                if "Date Available:" in li_text:
                    date_available = text
                elif "Bond" in li_text:
                    bond = text

        # Assign values to the dictionary
        property_metadata[property_url]['date_available'] = date_available
        property_metadata[property_url]['bond'] = bond


        # PROPERTY FEATURES:
        # Locate the top-level div
        listing_details_div = bs_object.find("div", {"data-testid": "listing-details__additional-features"})

        # Extract property features with checks for each level (not all properties have features)
        property_features = []
        if listing_details_div:
            expander_wrapper = listing_details_div.find("div", {"data-testid": "expander-wrapper"})
            if expander_wrapper:
                content_div = expander_wrapper.find("div", {"class": "noscript-expander-content css-1mnayj9"})
                if content_div:
                    ul_element = content_div.find("ul", {"class": "css-4ewd2m"})
                    if ul_element:
                        li_elements = ul_element.find_all("li", {"class": "css-vajaaq"})
                        property_features = [li.get_text(strip=True) for li in li_elements]

        # Add the list to the property_metadata dictionary
        property_metadata[property_url]['property_features'] = property_features


        # # EXTRACTING THE COORDINATES: 
        
        # Extract coordinates from the map div
        map_div = bs_object.find("div", {"data-testid": "listing-details__map"}) \
            .find("div", {"class": "css-yjd8ae"}) \
            .find("div", {"class": "listing-details__location-map--default css-79elbk"}) \
            .find("ul", {"class": "css-1vlxv67"}) \
            .find_all("li", {"class": "css-1g3iwis"})[1] \
            .find("a", {"class": "css-1aszeu9"})

        # Initialize latitude and longitude
        latitude, longitude = None, None

        # Extract and parse the href if available
        if map_div and 'href' in map_div.attrs:
            href = map_div['href']
            destination = parse_qs(urlparse(href).query).get('destination', [None])[0]
            if destination:
                coordinates = destination.split(',')
                if len(coordinates) == 2:
                    latitude, longitude = coordinates

        # Update metadata
        property_metadata[property_url]['coordinates'] = [latitude, longitude]



        # Add one to the success to track
        success_count += 1

        
    except AttributeError:
        print(f"Issue with {property_url}")

    pbar.set_description(f"{(success_count/total_count * 100):.0f}% successful")

import os

# from the current `scripts` directory, go back one levels to the `project` directory
output_relative_dir = '../data/'

# check if it exists as it makedir will raise an error if it does exist
if not os.path.exists(output_relative_dir):
    os.makedirs(output_relative_dir)
    
if not os.path.exists(output_relative_dir + 'landing'):
        os.makedirs(output_relative_dir + 'landing')

# output to example json in data/landing/
with open(output_relative_dir + 'landing/testing_scraping.json', 'w') as f:
    dump(property_metadata, f)