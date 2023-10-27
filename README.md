# Multinational Retain Data Centralisation project
- In an effort to help an organisation become more data-driven, I am tasked to make their sales data accessible from one centralised location.
- This will incorporate extracting data from the AWS RDS service into Python, where I have prepared a data processing class to tidy the data then finally the cleaned data is uploaded into my local PostgreSQL server.

## Installation instructions
I am using Visual Studio Code with the Conda 3.11.5 as my Python interpreter. I imported additional packages to run this project including:
- sqlalchemy
- pandas
- numpy
- dateutil
- yaml

## Usage instructions
1. First, the database_utils.py contains classes that initialises a connection with the AWS RDS service and returns a list of database tables. Select a table for extraction
2. The DataExtractor class in the data_extraction.py is used to read in the table from AWS RDS into Python. This enables the table to be manipulated within Python
3. The DataCleaning class in data_cleaning.py contains methods that will tidy the selected table. Example methods are those that tidy date columns, ensures phone numbers have the correct digits etc
4. When the table is cleaned. the database_utils.py in step one has a method enabling the table to be uploaded to your local PostgreSQL server.

