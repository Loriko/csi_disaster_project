# csi_disaster_project
Repository for CSI4142 Project

## Encoding
To read data from the csv file into the database, the client encoding of the database must be set to ISO-8859-1.
This can be done with:
set client_encoding to 'ISO-8859-1';

## Data Cleaning
Here are the steps we followed to clean the data

### Location Dimension
1. Removed the , and " characters which have special meaning in SQL or CSV and could cause our INSERTS to fail
2. Escaped the ' characters so that our the intermediate csv we create is interpreted correctly
3. Removed city from all the possible place names
4. Performed minor format changes such as changing U.S.A to USA
