-- contrast the the total number of fatalities in Ontario due to wildfires with the number of fatalities in Ontario due to flooding
SET SEARCH_PATH to 'disaster_db_schema';
SELECT  SUM(fatality_number) as total_fatality_number,
  disaster_type,
  province
FROM fact
  INNER JOIN date_dimension ON fact.start_date_key = date_dimension.date_key
  INNER JOIN location_dimension ON fact.location_key = location_dimension.location_key
  INNER JOIN disaster_dimension ON fact.disaster_key = disaster_dimension.disaster_key
WHERE province = 'ON' AND (disaster_type = 'flood' OR disaster_type = 'wildfire')
GROUP BY disaster_type, province;