import pandas as pd
import numpy as np

def clean_median_rent_excel(df):
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
    # Remove rows with 'Group Total' in the 'sa2' column
    df = df[df['suburb'] != 'Group Total'].reset_index(drop=True)
    
    return df

# Function to calculate RAI (Rental Affordability Index) (median income/qualifying income) x 100
def calculate_affordability_index(median_rent_df, rent_type, rent_mapping, household_type_df):
    affordability_index_list = []  # Use a list to store results
    median_rent_df['median'] = pd.to_numeric(median_rent_df['median'], errors='coerce')
    household_type = rent_mapping[rent_type]

    for _, row in median_rent_df.iterrows():
        suburb = row['suburb']
        median_rent = row['median']
        
        # Qualifying income calculation
        qualifying_income = median_rent / 0.30 if pd.notna(median_rent) else np.nan
        
        # Get weekly income from household income DataFrame
        weekly_income = household_type_df.loc[household_type_df['household_type'] == household_type, 'weekly_income'].values[0]
        
        # Affordability index calculation
        affordability_index = (weekly_income / qualifying_income) * 100 if pd.notna(qualifying_income) else np.nan
        
        # Append to the results list
        affordability_index_list.append({
            'suburb': suburb,
            'Property_Type': rent_type,
            'Affordability_Index': affordability_index
        })

    # Create DataFrame from the results list
    return pd.DataFrame(affordability_index_list)

# Create a function to separate entries
def split_hyphenated_entries(row):
    # Split the 'sa2' value if it contains a hyphen
    if '-' in row['suburb']:
        parts = row['suburb'].split('-')
        # Create a new DataFrame with each part and the same index value
        return pd.DataFrame({
            'suburb': [part.strip() for part in parts],
            'Overall_Affordability_Index': [row['Overall_Affordability_Index']] * len(parts)
        })
    else:
        return pd.DataFrame([row])