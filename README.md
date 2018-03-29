# csi_disaster_project
Repository for CSI4142 Project

## Data Cleaning
Here are the steps we followed to clean the data

### Location Dimension
1. Removed the , and " characters which have special meaning in SQL or CSV and could cause our INSERTS to fail
2. Escaped the ' characters so that our the intermediate csv we create is interpreted correctly
3. Removed city from all the possible place names
4. Performed minor format changes such as changing U.S.A to USA

### Date Dimension
1. If start_date or end_date is given in an incorrect format, we link the fact row to a null row in our date_dimension table