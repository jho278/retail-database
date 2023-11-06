# Multinational Retain Data Centralisation project
- In an effort to help an organisation become more data-driven, I created three Python classes to connect to AWS, extract, and clean data. The aim of the project is to make the organisation's sales data accessible from one centralised location

- Some things I learnt in this project: 
    - I incorporated Object Oriented Principles by abstracting the data cleaning process into smaller methods with straightforward names, then incorporated those methods into a singular  method. This is to show simply what steps was taken to clean the data. 
    - I used the `with XXX as conn:` to ensure that the connection to AWS services is closed after use.
    - I learnt to connect to different services within AWS such as S3, RDS, and it's APIs, and read different data types from PDFs, CSVs, JSON files into Python. 

## Installation instructions
I am using Visual Studio Code with the Conda 3.11.5 as my Python interpreter. I imported additional packages to run this project including:
- sqlalchemy
- pandas
- numpy
- dateutil
- yaml
- boto3
- requests
- tabula
- io

## Usage instructions
1. First, the database_utils.py contains classes that initialises a connection with the AWS RDS service and returns a list of database tables. Select a table for extraction
2. The DataExtractor class in the data_extraction.py is used to read in the table from AWS RDS into Python. This enables the table to be manipulated within Python
3. The DataCleaning class in data_cleaning.py contains methods that will tidy the selected table. Example methods are those that tidy date columns, ensures phone numbers have the correct digits etc
4. When the table is cleaned. the database_utils.py in step one has a method enabling the table to be uploaded to the local PostgreSQL server.

## File structure
- database_utils.py: Methods to connect to AWS services and uploading data to local PostgreSQL 
- data_extraction.py: Methods to read different data formats into python including pdf, json, csv, and bytes
- data_cleaning.py: Methods to clean the data tables including date formatting, removal of randomly generated records. Specific methods that evaluates the elements and returns '0' for incorrect format such as card number digits based on card provider and phone number digits for different countries. 
- example_set_star_scheme.sql: Example scripts used to set the primary and foreign keys linking the dim prefix tables to the orders_table
- query_questions.sql: The SQL scripts to answer queries from the company.
- Data Exploration: Creating the scripts to process the data before converting them into methods in the classes

## License information
MIT License

Copyright (c) [year] [fullname]

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.