-- determine the trends in fatalities due to riots over the last 100 years
SET SEARCH_PATH to 'disaster_db_schema';
SELECT  SUM(fatality_number) as yearly_fatalities,
  year_actual,
  disaster_group
FROM fact
  INNER JOIN date_dimension ON fact.start_date_key = date_dimension.date_key
  INNER JOIN disaster_dimension ON fact.disaster_key = disaster_dimension.disaster_key
  INNER JOIN cost_dimension cd ON fact.cost_key = cd.cost_key
WHERE disaster_group = 'natural'
      AND year_actual >= date_part('year', CURRENT_DATE) - 100
GROUP BY year_actual, disaster_group
ORDER BY year_actual DESC;