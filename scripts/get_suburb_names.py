import pandas as pd

def get_suburb_names():
    """
    This function loads excel file from a predefined path, filters it for Victoria,
    , and returns a list of unique suburb names.
    
    Returns:
    list: A list of unique suburb names.
    """
    
    # Define file path and sheet name
    file_path = '../data/socioeconomic/socioeconomic.xls'
    sheet_name = 'Table 1'
    
    # Read the Excel file
    df = pd.read_excel(file_path, sheet_name=sheet_name, skiprows=4)
    
    # Drop the last 4 rows
    df = df.iloc[:-4]
    
    # Drop the first row
    df = df.drop(df.index[0])
    
    # Convert the '2016 State Suburb (SSC) Code' column to string
    df['2016 State Suburb (SSC) Code'] = df['2016 State Suburb (SSC) Code'].astype(str)
    
    # Filter rows where '2016 State Suburb (SSC) Code' starts with '2'
    vic_df = df[df['2016 State Suburb (SSC) Code'].str.startswith('2')]
    
    # Strip whitespace from column names
    vic_df.columns = vic_df.columns.str.strip()
    
    # Remove text from '(' onwards in the '2016 State Suburb (SSC) Name' column
    vic_df['2016 State Suburb (SSC) Name'] = vic_df['2016 State Suburb (SSC) Name'].str.replace(r'\s*\(.*\)', '', regex=True)
    
    # Get unique names
    unique_names = vic_df['2016 State Suburb (SSC) Name'].unique().tolist()
    
    return unique_names

# Example usage
unique_names = get_suburb_names()
print(unique_names)
