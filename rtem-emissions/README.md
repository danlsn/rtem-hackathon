# RTEM-Emissions
A Python utility module to estimate greenhouse gas emission intensity for 
buildings using RTEM energy usage monitors, enriching already existing data 
to support energy visibility and decarbonisation.
___

## Modules

### RTEM-EMISSIONS

Main entrypoint for the program.

### ESTIMATOR

- Estimate greenhouse gas emission intensity using publicly available data 
published annually by the US Energy Information Administration (EIA). 
- Provide CO2/kWh figure for each fuel type.

### CALCULATOR

- Calculate % of Total Energy is Renewable Energy
- Calculate GHG Emissions for Time Period

### FETCHER

- Fetch data from various public & private data sources.

### LOADER

- Transform data into target formats.
- Load data into relational database for analysis.
- Publish data as RTEM datapoint.

### FUNCTIONS

- Common functions used by other modules.

### MODELS

- Classes and datastructures used by other modules.

## CLIENT

- Initialises the RTEM Onboard Client.

