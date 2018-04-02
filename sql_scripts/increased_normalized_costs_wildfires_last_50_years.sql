-- determine the increase in normalized costs of wildfires over the last 50 years
SET SEARCH_PATH to 'disaster_db_schema';
SELECT  SUM(normalized_total_cost) as yearly_normalized_cost,
  year_actual,
  disaster_type
FROM fact
  INNER JOIN date_dimension ON fact.start_date_key = date_dimension.date_key
  INNER JOIN disaster_dimension ON fact.disaster_key = disaster_dimension.disaster_key
  INNER JOIN cost_dimension cd ON fact.cost_key = cd.cost_key
WHERE disaster_type = 'wildfire'
  AND year_actual >= date_part('year', CURRENT_DATE) - 50
GROUP BY year_actual, disaster_type
ORDER BY year_actual DESC;