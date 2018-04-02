-- list the total number of fatalities that occurred each year due to flooding or wildfires for each province
SET SEARCH_PATH to 'disaster_db_schema';
SELECT  SUM(fatality_number) as total_fatality_number,
  disaster_type,
  year_actual,
  province
FROM fact
  INNER JOIN date_dimension ON fact.start_date_key = date_dimension.date_key
  INNER JOIN location_dimension ON fact.location_key = location_dimension.location_key
  INNER JOIN disaster_dimension ON fact.disaster_key = disaster_dimension.disaster_key
WHERE (disaster_type = 'flood' OR disaster_type = 'wildfire')
GROUP BY year_actual, disaster_type, province
HAVING SUM(fatality_number) > 0
ORDER BY year_actual DESC;