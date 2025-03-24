# Sample MSBA 405 Pipeline (Final Project) Group 12

Crime Analytics Pipeline - MSBA 405 Final Project

Overview:
This project analyzes crime patterns in Los Angeles using Spark, DuckDB, and Power BI. The pipeline automates data ingestion, cleaning, transformation, and visualization for efficient crime trend analysis.

Dataset Sources:
LA Crime Data (2020-2024) - crime data (https://data.lacity.org/Public-Safety/Crime-Data-from-2020-to-Present/2nrs-mtv8/about_data)
LA County Demographics (2020) - Demographics_data (https://data.lacounty.gov/maps/e137518f57cf4dbc96ac7139a224631e/about)
LA City Council Districts (2021) - Council Districts (https://geohub.lacity.org/datasets/76104f230e384f38871eb3c4782f903d_13/explore) 
Census Tract to City District (2010) - Census Data (https://geohub.lacity.org/datasets/3d41239eaa564ca699b17dbefd5d2981/explore)

Pipeline Execution
Clone this repository and move the downloaded datasets into the data/ directory.

Run the pipeline using: bash pipeline.sh


Pipeline Steps:
1. Data Acquisition: Fetch datasets via APIs and store them on an EC2 instance.
2. Data Cleaning: Use Spark to remove duplicates, handle missing values, and convert to Parquet format.
3. Data Storage & Querying: Use DuckDB for data transformations and efficient queries.
4. Visualization: Load processed data into Power BI for interactive dashboards.

