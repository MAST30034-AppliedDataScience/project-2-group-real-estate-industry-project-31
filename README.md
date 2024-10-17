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
Predict rental prices in Victoria using internal and external factors to provide a service for rental property investors by informing their decision for future rental property investments.


To run this pipeline, please visit the `notebooks` directory and run the files in order:
1. `datascrape.ipynb`: This notebook scrapes from domain.com and oldlistings.com.au and scrapes all of our selected external datasets 
(~1.5 hr runtime)
2. `preprocessing.ipynb`: This notebook preprocesses the domain and oldlisting datasets, conducting feature engineering and also splits both datasets by Greater Melbourne and Rest of Victoria. It also combines them into a single dataset (~2.5hrs runtime due to ORS API calls)
3. `external_data_preprocessing.ipynb`: This notebook preprocesses the external datasets and produces forecasts for the desired years (~5 min runtime)
4. `modelling_properties.ipynb`: This notebook produces the 4 main models for predicting rental prices across Victoria. Models include one random-forest and one linear regression for both Greater Melbourne and the rest of Vic (~30 min runtime)

*(the remaining steps can be done in any order as they each serve a unique purpose)*

5. 



