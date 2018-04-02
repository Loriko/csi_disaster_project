-- determine the 5 cities in Canada with the most riots
SET SEARCH_PATH to 'disaster_db_schema';
SELECT city, count(*) as number_of_riots
FROM fact
INNER JOIN location_dimension ld ON fact.location_key = ld.location_key
INNER JOIN disaster_dimension d3 ON fact.disaster_key = d3.disaster_key
WHERE disaster_type = 'rioting' AND country = 'CANADA'
GROUP BY city
ORDER BY count(*) DESC
LIMIT 5;