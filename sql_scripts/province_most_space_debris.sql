-- determine the province in Canada with the most space debris
SET SEARCH_PATH to 'disaster_db_schema';
SELECT province, count(*) as number_of_space_debris_disasters
FROM fact
  INNER JOIN location_dimension ld ON fact.location_key = ld.location_key
  INNER JOIN disaster_dimension d3 ON fact.disaster_key = d3.disaster_key
WHERE disaster_type = 'space debris' AND country = 'CANADA'
GROUP BY province
ORDER BY count(*) DESC
LIMIT 1;