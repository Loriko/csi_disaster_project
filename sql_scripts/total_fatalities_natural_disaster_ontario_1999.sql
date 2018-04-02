-- determine the total number of fatalities due to natural disasters in Ontario during 1999
SET SEARCH_PATH to 'disaster_db_schema';
SELECT SUM(fatality_number) as total_fatality_number
FROM fact
  INNER JOIN date_dimension ON fact.start_date_key = date_dimension.date_key
  INNER JOIN location_dimension ON fact.location_key = location_dimension.location_key
  INNER JOIN disaster_dimension ON fact.disaster_key = disaster_dimension.disaster_key
WHERE year_actual = 1999 AND province = 'ON' AND disaster_group = 'natural';