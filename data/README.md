# Datasets

# Overview
This folder contains all data files used in the analysis pipeline. The data is organized into three subfolders: landing, raw, and curated, reflecting the different stages of the data's lifecycle—starting from collection, through preprocessing, to final readiness for analysis.

Landing: Data collected from upstream sources, primarily external datasets, and real estate data.
Raw: Data after initial preprocessing, ready for further cleaning and transformation.
Curated: Data that has been fully processed and is ready for analysis, including both external data and final property datasets.
Folder Structure

data/
├── landing/                # Collected raw data from upstream sources
│   ├── business/           # External data related to businesses by suburb
│   ├── community/          # Community-related data by suburb
│   ├── homelessness/       # Homelessness data by suburb
│   ├── income/             # Income levels by suburb
│   ├── inflation/          # Inflation data by suburb
│   ├── population/         # Population data by suburb
│   ├── population_dist/    # Population distribution data by suburb
│   ├── sa2/                # Statistical Area Level 2 (SA2) data
│   ├── sal_2021_aust_gda2020_shp_extracted/  # SA2 shapefiles
│   ├── socioeconomic/      # Socioeconomic data by suburb
│   ├── unemployment/       # Unemployment data by suburb
│   ├── domain/             # Raw property data from domain.com
│   ├── oldlisting/         # Raw property data from oldlisting.com
│   └── postcodes.csv       # List of postcodes in Victoria
│
├── raw/                    # Data after initial preprocessing
│   ├── domain/             # Domain data at various preprocessing stages
│   └── oldlisting/         # Oldlisting data at various preprocessing stages
│
├── curated/                # Final processed data ready for analysis
│   ├── external_data/      # Preprocessed external data by suburb
│   └── final_datasets/     # Final property datasets split by region (Greater Melbourne / Rest of Victoria)
│
└── README.md               # This README file

# Landing
The landing folder contains raw data collected from various upstream sources:

business: Business-related data by suburb.
community: Community statistics for different suburbs.
homelessness: Data on homelessness by suburb.
income: Income levels per suburb.
inflation: Inflation rates per suburb.
population: Population counts by suburb.
population_dist: Population distribution by suburb.
sa2: Statistical Area Level 2 (SA2) geographic boundaries and data.
sal_2021_aust_gda2020_shp_extracted: Extracted shapefiles for SA2 regions.
socioeconomic: Socioeconomic status data by suburb.
unemployment: Unemployment rates by suburb.
domain: Raw property data scraped from domain.com.
oldlisting: Raw property data scraped from oldlisting.com.
postcodes: A CSV file containing postcodes in Victoria.

# Raw
The raw folder contains data that has undergone initial preprocessing steps:

domain: Property data from domain.com in various stages of preprocessing.
oldlisting: Property data from oldlisting.com in various stages of preprocessing.

# Curated
The curated folder contains data that has been fully processed and is ready for analysis:

external_data: Preprocessed external data by suburb, including socioeconomic, business, and population data.
final_datasets: Datasets containing property data, organized by region:
Greater Melbourne
Rest of Victoria

# Usage Guidelines
Landing Data: Data in the landing folder is unprocessed. Users should not use this data directly for analysis.
Raw Data: Data in the raw folder has been partially cleaned but may still require further processing.
Curated Data: The data in the curated folder is fully processed and can be used for analysis. Ensure that the correct region-specific datasets are selected based on the analysis requirements.