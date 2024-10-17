## Python script with functions to help with the affordibility analysis ##
 
import pandas as pd
import numpy as np


def clean_median_rent_excel(df):
    '''
    Cleans a DataFrame containing median rent data.
    '''

    # Select specific columns
    df = df.iloc[:, [0, 1, -2, -1]]

    # Drop the first row
    df = df.drop(index=df.index[0])

    # Drop the first column
    df = df.drop(df.columns[0], axis=1)

    # Rename the columns
    df.columns = ['suburb', 'count', 'median']

    # Replace '-' entries with NaN
    df['median'] = df['median'].replace('-', np.nan)
    df['count'] = df['count'].replace('-', np.nan)

    # Drop the second and third rows and reset the index
    df = df.drop(index=[1, 2]).reset_index(drop=True)

    # Remove rows with 'Group Total' in the 'suburb' column
    df = df[df['suburb'] != 'Group Total'].reset_index(drop=True)
    
    return df



def calculate_affordability_index(median_rent_df, rent_type, rent_mapping, household_type_df):
    '''
    Calculates the Rental Affordability Index (RAI) based on median rent and household income.
    
    The formula for RAI is: (median income / qualifying income) * 100.
    Qualifying income is the income needed to afford rent, calculated as median rent / 0.30.
    '''

    affordability_index_list = []  # Use a list to store results
    median_rent_df['median'] = pd.to_numeric(median_rent_df['median'], errors='coerce')  # Ensure 'median' column is numeric
    household_type = rent_mapping[rent_type]  # Map rent type to household type

    # Loop through each row to calculate RAI for each suburb
    for _, row in median_rent_df.iterrows():
        suburb = row['suburb']
        median_rent = row['median']
        
        # Calculate qualifying income needed for rent (30% of income)
        qualifying_income = median_rent / 0.30 if pd.notna(median_rent) else np.nan
        
        # Retrieve weekly income for the specific household type
        weekly_income = household_type_df.loc[household_type_df['household_type'] == household_type, 'weekly_income'].values[0]
        
        # Calculate the affordability index (RAI)
        affordability_index = (weekly_income / qualifying_income) * 100 if pd.notna(qualifying_income) else np.nan
        
        # Append the results to the list
        affordability_index_list.append({
            'suburb': suburb,
            'Property_Type': rent_type,
            'Affordability_Index': affordability_index
        })

    # Convert the results list to a DataFrame and return it
    return pd.DataFrame(affordability_index_list)



def split_hyphenated_entries(row):
    '''
    Splits hyphenated suburb entries into separate rows.
    
    If a suburb has a hyphen (e.g., "SuburbA - SuburbB"), it splits the entry into separate rows for each suburb
    and assigns the same affordability index to both suburbs.
    '''

    # Check if the 'suburb' contains a hyphen
    if '-' in row['suburb']:
        parts = row['suburb'].split('-')  # Split by hyphen
        # Create a new DataFrame for each part with the same 'Overall_Affordability_Index'
        return pd.DataFrame({
            'suburb': [part.strip() for part in parts],
            'Overall_Affordability_Index': [row['Overall_Affordability_Index']] * len(parts)
        })
    else:
        # Return the original row as a DataFrame if no hyphen is found
        return pd.DataFrame([row])
    
