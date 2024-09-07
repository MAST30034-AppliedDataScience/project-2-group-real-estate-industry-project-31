############################# IMPORT NECCESSARY LIBRARIES #############################

import re
from tqdm import tqdm
from bs4 import BeautifulSoup
from urllib.request import urlopen, Request
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from collections import defaultdict
import numpy as np
from urllib.parse import urlparse, parse_qs


############################# FINDING ALL THE RENTAL URLS #############################

def fetch_links_for_price_range(baseurl, price_range, page):
    '''
    Fetches the links from a given page for a specific price range.
    '''
    url = f"{baseurl}/rent/?price={price_range}&excludedeposittaken=1&sort=price-asc&state=vic&page={page}"
    print(f"Fetching {url}")
    
    try:
        response = urlopen(Request(url, headers={'User-Agent':"PostmanRuntime/7.6.0"}))
        bs_object = BeautifulSoup(response, "lxml")
    except Exception as e:
        print(f"Error fetching page {page} for price range {price_range}: {e}")
        return []
    
    # Find the listings (ul element with specific data-testid attribute)
    results = bs_object.find("ul", {"data-testid": "results"})
    if not results:
        print(f"No listings found on page {page} for price range {price_range}.")
        return []
    
    # Find all href (a) tags that are from the base_url website
    index_links = results.findAll("a", href=re.compile(f"{baseurl}/*"))
    
    # Filter for links with class 'address'
    links = [link['href'] for link in index_links if 'address' in link.get('class', [])]
    
    return links

def generate_url_list(baseurl):
    '''
    Generating a list of VIC property urls that uses threading to fetch multiple pages 
    concurrently for each price range.
    '''
    print("\nGenerating the list of links...\n")
    url_links = []
    min_price = 150
    max_price = 3000
    
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = []
        
        # Looping through price ranges (150 to 3000 in steps of 50)
        while min_price <= max_price:
            if min_price >= 3000:
                price_range = "3000-any"
                price_increment = 50
            else:
                price_range = f"{min_price}-{min_price + 50}"
                price_increment = 50
            
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
                total_listings = int(re.sub(r'[^\d]', '', properties_count_str))
                print(f"Found {total_listings} listings for price range {price_range}")
            except AttributeError:
                print(f"Could not find listing count for {price_range}, assuming no listings.")
                total_listings = 0
            
            # If more than 1000 listings, break the range into $5 intervals
            if total_listings > 1000:
                price_increment = 5
            
            # Adjust the price range increment accordingly ($50 or $5)
            current_min_price = min_price
            
            while current_min_price < min_price + 50:
                if current_min_price >= 3000:
                    price_range = "3000-any"
                else:
                    price_range = f"{current_min_price}-{current_min_price + price_increment}"
                
                print(f"\nFetching listings for sub-range: {price_range}\n")
                
                # Fetch pages for the current sub-range concurrently
                for page in range(1, 51):  # Up to 50 pages
                    futures.append(executor.submit(fetch_links_for_price_range, baseurl, price_range, page))
                
                current_min_price += price_increment
            
            min_price += 50
        
        # Collect results as they complete
        for future in tqdm(as_completed(futures), total=len(futures)):
            url_links.extend(future.result())
    
    url_links = list(set(url_links))  # Remove duplicates
    return url_links



############################# FETCHING THE DATA FOR EACH RENTAL #############################


def fetch_rental_data(property_url, property_metadata):
    '''
    Fetch the rental data of interest for a particular property
    '''
    headers = {'User-Agent': "PostmanRuntime/7.6.0"} # Define headers to mimic a browser request
    try:
        # Send a GET request to the property URL
        response = requests.get(property_url, headers=headers)
        html = response.text

        # Parse the HTML content using BeautifulSoup 
        bs_object = BeautifulSoup(html, "lxml")

        # Scrape and store the property name
        property_metadata[property_url]['name'] = bs_object.find("h1", {"class": "css-164r41r"}).text

        # Scrape and store the property cost
        property_metadata[property_url]['cost_text'] = bs_object.find("div", {"data-testid": "listing-details__summary-title"}).text

        # Extract room and parking details using regex
        rooms = bs_object.find("div", {"data-testid": "property-features"}).findAll("span", {"data-testid": "property-features-text-container"})
        property_metadata[property_url]['rooms'] = [
            re.findall(r'\d+\s[A-Za-z]+', feature.text)[0] for feature in rooms if 'Bed' in feature.text or 'Bath' in feature.text
        ]
        property_metadata[property_url]['parking'] = [
            re.findall(r'\S+\s[A-Za-z]+', feature.text)[0] for feature in rooms if 'Parking' in feature.text
        ]

        # Scrape and store property description
        property_metadata[property_url]['desc'] = bs_object.find("p").get_text(separator='\n').strip()


        # Scrape and store the property type (e.g., house, apartment)
        property_metadata[property_url]['property_type'] = bs_object.find(
            "div", {"data-testid": "listing-summary-property-type"}).find("span", {"class": "css-in3yi3"}).text

        # Extract additional details such as date available and bond using list items
        ul_element = bs_object.find("div", {"data-testid": "strip-content-list"}).find("ul", {"data-testid": "listing-summary-strip"})
        li_elements = ul_element.find_all("li")

        date_available = np.nan
        bond = np.nan

        for li in li_elements:
            strong_tag = li.find("strong")
            if strong_tag:
                text = strong_tag.get_text(strip=True)
                li_text = li.get_text(strip=True)
                if "Date Available:" in li_text:
                    date_available = text
                elif "Bond" in li_text:
                    bond = text

        # Store the date available and bond information
        property_metadata[property_url]['date_available'] = date_available
        property_metadata[property_url]['bond'] = bond

        # Scrape additional property features if available
        listing_details_div = bs_object.find("div", {"data-testid": "listing-details__additional-features"})
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

        property_metadata[property_url]['property_features'] = property_features

        # Scrape latitude and longitude for the property from the map link
        map_div = bs_object.find("div", {"data-testid": "listing-details__map"}) \
            .find("div", {"class": "css-yjd8ae"}) \
            .find("div", {"class": "listing-details__location-map--default css-79elbk"}) \
            .find("ul", {"class": "css-1vlxv67"}) \
            .find_all("li", {"class": "css-1g3iwis"})[1] \
            .find("a", {"class": "css-1aszeu9"})

        latitude, longitude = None, None

        # Extract coordinates from the map URL if available
        if map_div and 'href' in map_div.attrs:
            href = map_div['href']
            destination = parse_qs(urlparse(href).query).get('destination', [None])[0]
            if destination:
                coordinates = destination.split(',')
                if len(coordinates) == 2:
                    latitude, longitude = coordinates

        property_metadata[property_url]['coordinates'] = [latitude, longitude]

    except Exception as e:
        print(f"Issue with {property_url}: {e}")


def fetch_all_rental_data(url_links):
    '''
    Fetch all the data for rentals in VIC using parallelisation
    '''
    property_metadata = defaultdict(dict) # Initialise a dictionary

    # Use ThreadPoolExecutor to fetch property data concurrently
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_rental_data, url, property_metadata): url for url in url_links}

        # Display a progress bar as tasks complete
        for future in tqdm(as_completed(futures), total=len(futures)):
            future.result()  # To raise exceptions if any occurred and retrieve results

    return property_metadata # Return all the data 


