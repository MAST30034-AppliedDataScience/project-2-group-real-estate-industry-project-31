import re
from tqdm import tqdm
from bs4 import BeautifulSoup
from urllib.request import urlopen, Request
from concurrent.futures import ThreadPoolExecutor, as_completed

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

def generate_url_list_2(baseurl):
    '''
    Optimized version that uses threading to fetch multiple pages concurrently for each price range.
    '''
    print("\nGenerating the list of links...\n")
    url_links = []
    min_price = 150
    max_price = 3000
    
    with ThreadPoolExecutor(max_workers=10) as executor:
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




