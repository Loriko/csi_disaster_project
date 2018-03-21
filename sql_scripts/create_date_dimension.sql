-- Found at https://medium.com/@duffn/creating-a-date-dimension-table-in-postgresql-af3f8e2941ac
-- Some modifications were made to add meteorological_season, is_holiday and holiday_text
DROP TABLE if exists date_dimension;

CREATE TABLE date_dimension
(
  date_key                 INT NOT NULL,
  date_actual              DATE NOT NULL,
  epoch                    BIGINT NOT NULL,
  day_suffix               VARCHAR(4) NOT NULL,
  day_name                 VARCHAR(9) NOT NULL,
  day_of_week              INT NOT NULL,
  day_of_month             INT NOT NULL,
  day_of_quarter           INT NOT NULL,
  day_of_year              INT NOT NULL,
  week_of_month            INT NOT NULL,
  week_of_year             INT NOT NULL,
  week_of_year_iso         CHAR(10) NOT NULL,
  month_actual             INT NOT NULL,
  month_name               VARCHAR(9) NOT NULL,
  month_name_abbreviated   CHAR(3) NOT NULL,
  quarter_actual           INT NOT NULL,
  quarter_name             VARCHAR(9) NOT NULL,
  year_actual              INT NOT NULL,
  first_day_of_week        DATE NOT NULL,
  last_day_of_week         DATE NOT NULL,
  first_day_of_month       DATE NOT NULL,
  last_day_of_month        DATE NOT NULL,
  first_day_of_quarter     DATE NOT NULL,
  last_day_of_quarter      DATE NOT NULL,
  first_day_of_year        DATE NOT NULL,
  last_day_of_year         DATE NOT NULL,
  mmyyyy                   CHAR(6) NOT NULL,
  mmddyyyy                 CHAR(10) NOT NULL,
  weekend_indr             BOOLEAN NOT NULL,
  is_holiday               BOOLEAN DEFAULT FALSE,
  holiday_text             VARCHAR(50),
  meteorological_season    VARCHAR(10)
);

ALTER TABLE public.date_dimension ADD CONSTRAINT date_dimension_date_key_pk PRIMARY KEY (date_key);

CREATE INDEX date_dimension_date_actual_idx
  ON date_dimension(date_actual);

COMMIT;

INSERT INTO date_dimension
  SELECT TO_CHAR(datum,'yyyymmdd')::INT AS date_key,
         datum AS date_actual,
         EXTRACT(epoch FROM datum) AS epoch,
         TO_CHAR(datum,'fmDDth') AS day_suffix,
         TO_CHAR(datum,'Day') AS day_name,
         EXTRACT(isodow FROM datum) AS day_of_week,
         EXTRACT(DAY FROM datum) AS day_of_month,
         datum - DATE_TRUNC('quarter',datum)::DATE +1 AS day_of_quarter,
         EXTRACT(doy FROM datum) AS day_of_year,
         TO_CHAR(datum,'W')::INT AS week_of_month,
         EXTRACT(week FROM datum) AS week_of_year,
         TO_CHAR(datum,'YYYY"-W"IW-') || EXTRACT(isodow FROM datum) AS week_of_year_iso,
         EXTRACT(MONTH FROM datum) AS month_actual,
         TO_CHAR(datum,'Month') AS month_name,
         TO_CHAR(datum,'Mon') AS month_name_abbreviated,
         EXTRACT(quarter FROM datum) AS quarter_actual,
         CASE
         WHEN EXTRACT(quarter FROM datum) = 1 THEN 'First'
         WHEN EXTRACT(quarter FROM datum) = 2 THEN 'Second'
         WHEN EXTRACT(quarter FROM datum) = 3 THEN 'Third'
         WHEN EXTRACT(quarter FROM datum) = 4 THEN 'Fourth'
         END AS quarter_name,
         EXTRACT(isoyear FROM datum) AS year_actual,
         datum +(1 -EXTRACT(isodow FROM datum))::INT AS first_day_of_week,
         datum +(7 -EXTRACT(isodow FROM datum))::INT AS last_day_of_week,
         datum +(1 -EXTRACT(DAY FROM datum))::INT AS first_day_of_month,
         (DATE_TRUNC('MONTH',datum) +INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month,
         DATE_TRUNC('quarter',datum)::DATE AS first_day_of_quarter,
         (DATE_TRUNC('quarter',datum) +INTERVAL '3 MONTH - 1 day')::DATE AS last_day_of_quarter,
         TO_DATE(EXTRACT(isoyear FROM datum) || '-01-01','YYYY-MM-DD') AS first_day_of_year,
         TO_DATE(EXTRACT(isoyear FROM datum) || '-12-31','YYYY-MM-DD') AS last_day_of_year,
         TO_CHAR(datum,'mmyyyy') AS mmyyyy,
         TO_CHAR(datum,'mmddyyyy') AS mmddyyyy,
         CASE
         WHEN EXTRACT(isodow FROM datum) IN (6,7) THEN TRUE
         ELSE FALSE
         END AS weekend_indr,
  FALSE as is_holiday,
  NULL as holiday_text,
  CASE
  WHEN EXTRACT(MONTH FROM datum) IN (12,1,2) THEN 'Winter'
  WHEN EXTRACT(MONTH FROM datum) IN (3,4,5) THEN 'Spring'
  WHEN EXTRACT(MONTH FROM datum) IN (6,7,8) THEN 'Summer'
  WHEN EXTRACT(MONTH FROM datum) IN (9,10,11) THEN 'Fall'
  ELSE NULL
  END AS meteorological_season


  FROM (SELECT '1900-01-01'::DATE+ SEQUENCE.DAY AS datum
        FROM GENERATE_SERIES (0,29219) AS SEQUENCE (DAY)
        GROUP BY SEQUENCE.DAY) DQ
  ORDER BY 1;

COMMIT;