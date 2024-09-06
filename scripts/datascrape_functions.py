## Python script with functions to help with the datascraping section ##

import re
from tqdm import tqdm
from collections import defaultdict
from bs4 import BeautifulSoup
from urllib.request import urlopen, Request
import numpy as np
from urllib.parse import urlparse, parse_qs

def generate_url_list(baseurl):
    '''
    Returns a list of domain.com.au URLs for rent listings within a price range.
    The function starts from 150 and increments by 50 until 3000, then uses "3000-any".
    If a price range has more than 1000 listings, it breaks the range into $5 increments.
    For each price range, it fetches up to 50 pages, or stops if no listings are found.
    '''
    print("\nGenerating the list of links...\n")

    # Initializing the URL list
    url_links = []
    
    # Starting price range
    min_price = 150
    max_price = 3000
    
    # Looping through price ranges (150 to 3000 in steps of 50)
    while min_price <= max_price:
        # Handle the final "3000-any" case
        if min_price >= 3000:
            price_range = "3000-any"
            price_increment = 50  # No need for further sub-range checks
        else:
            price_range = f"{min_price}-{min_price + 50}"
            price_increment = 50  # Default increment
        
        print(f"\nFetching listings for price range: {price_range}\n")
        
        # URL to check how many properties are available for the price range
        check_url = f"{baseurl}/rent/?price={price_range}&excludedeposittaken=1&sort=price-asc&state=vic"
        
        try:
            response = urlopen(Request(check_url, headers={'User-Agent':"PostmanRuntime/7.6.0"}))
            bs_object = BeautifulSoup(response, "lxml")
        except Exception as e:
            print(f"Error fetching page {check_url}: {e}")
            min_price += 50
            continue
        
        # Find the total number of listings for the current price range
        try:
            properties_div = bs_object.find("div", {"class": "css-9ny10o"})
            properties_count_str = properties_div.find("h1", {"class": "css-ekkwk0"}).find("strong").text
            total_listings = int(re.sub(r'[^\d]', '', properties_count_str))  # Extract the number of listings
            print(f"Found {total_listings} listings for price range {price_range}")
        except AttributeError:
            print(f"Could not find listing count for {price_range}, assuming no listings.")
            total_listings = 0
        
        # If more than 1000 listings, break the range into $5 intervals
        if total_listings > 1000:
            print(f"Breaking price range {price_range} into $5 intervals due to more than 1000 listings.")
            price_increment = 5
        else:
            price_increment = 50
        
        # Adjust the price range increment accordingly (could be $50 or $5)
        current_min_price = min_price
        
        while current_min_price < min_price + 50:
            if current_min_price >= 3000:
                price_range = "3000-any"
            else:
                price_range = f"{current_min_price}-{current_min_price + price_increment}"
                
            print(f"\nFetching listings for sub-range: {price_range}\n")
            
            # Fetch up to 50 pages for the current sub-range
            for page in range(1, 51):  # Page numbers start from 1 to 50 (max)
                url = f"{baseurl}/rent/?price={price_range}&excludedeposittaken=1&sort=price-asc&state=vic&page={page}"
                print(f"Visiting {url}")
                
                # Request the page content
                try:
                    response = urlopen(Request(url, headers={'User-Agent':"PostmanRuntime/7.6.0"}))
                    bs_object = BeautifulSoup(response, "lxml")
                except Exception as e:
                    print(f"Error fetching page {page}: {e}")
                    break
                
                # Find the listings (ul element with specific data-testid attribute)
                results = bs_object.find("ul", {"data-testid": "results"})
                if not results:
                    print(f"No listings found on page {page} for price range {price_range}.")
                    break  # Exit the page loop if no results found
                
                # Find all href (a) tags that are from the base_url website
                index_links = results.findAll("a", href=re.compile(f"{baseurl}/*"))
                
                # Filter for links with class 'address'
                for link in index_links:
                    if 'address' in link.get('class', []):
                        url_links.append(link['href'])
            
            current_min_price += price_increment
        
        # Move to the next $50 price range
        min_price += 50
    
    url_links = list(set(url_links))
    return url_links


def fetch_rental_data(url_links):
    '''
    Takes in a list of domain.com urls and fetches and returns 
    a dictionary of rental property data from those urls
    '''

    print("\nFetching the rental data...\n")

    # Initiliasing the data dictionary
    property_metadata = defaultdict(dict)


    # for each url, scrape some basic metadata
    pbar = tqdm(url_links[1:])
    success_count, total_count = 0, 0

    # Loping through each url
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

    return property_metadata
    
