## Python Script with functions to aid in fetching the population data from the web ##

import os
import requests



def fetch_yearly_pop_projection(url, output_dir, output_file, headers):
    '''
    Downloads an=d saves the population data from the given link and saves it
    to the given output directory
    '''

    try:
        # Check if the output directory exists, create it if not
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"Created directory: {output_dir}")
        
        # Retrieving the population projection data
        response = requests.get(url, headers=headers)

        # Check for a successful request
        if response.status_code == 200:
            # Save the content to a CSV file
            with open(output_file, "wb") as file:
                file.write(response.content)
            print(f"Data successfully written to {output_file}")
            return response.content
        else:
            print(f"Failed to retrieve data. HTTP Status Code: {response.status_code}")

    except Exception as e:
        print(f"An error occurred: {e}")