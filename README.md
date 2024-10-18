# The University of Melbourne - MAST30034 (Data Science Project)
# Real Estate Consulting - Rental Pricing

* Group Number:
    - Group 31

* Group Members:
    - Nasser Mostafa (1172895)
    - Oscar Li       (1356343)
    - Laura Grant    (1353155)
    - Charlie Amad   (1356239)
    - Uma Barnes     (1277919)

**Project Goal:** 
Predict rental prices in Victoria using *internal* and *external* factors to provide a service for rental property investors by informing their decision for future rental property investments.

## Overview & Summary

To view a summary of the approach and findings from this project, ensure that you have the plots stored in the `plots` folder. The plots have been uploaded to this repository and can be accessed from the remote repo.

If that is complete, then visit the `notebooks` directory and run the `summary_notebook.ipynb`.

## Project Pipeline

To run this pipeline, please ensure your environment satisfies the requirements in the `requirements.txt` file. If that is complete, then visit the `notebooks` directory and run the files in order:
1. `datascrape.ipynb`: This notebook scrapes from domain.com and oldlistings.com.au and scrapes all of our selected external datasets. (~1.5 hr runtime)
2. `preprocessing.ipynb`: This notebook preprocesses the domain and oldlisting datasets, conducting feature engineering and also splits both datasets by Greater Melbourne and Rest of Victoria. It also combines them into a single dataset. (~2.5hrs runtime due to ORS API calls. You will need to include your own API key from the ORS website)
3. `external_data_preprocessing.ipynb`: This notebook preprocesses the external datasets and produces forecasts for the next five years. (~5 min runtime)
4. `modelling_properties.ipynb`: This notebook produces the 4 main models for predicting rental prices across Victoria. Models include one random-forest and one linear regression for both Greater Melbourne and the rest of Vic. (~30 min runtime)

*(the remaining steps can be done in any order as they each serve a unique purpose)*

5. `predicting_region_growth.ipynb`: This notebook models median rental price and predicts the growth rate of each SA2 region within Victoria.
6. `liveability_calculations.ipynb`: This notebook considers all the suburb specific metrics we found and uses them to find the liveability scores of each SA2 region in Victoria.
7. `affordability.ipynb`: This notebook calculates the affordability of each SA2 region within Victoria by both the region as a whole and by household type.
