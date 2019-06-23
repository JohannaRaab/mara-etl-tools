/* Returns which date from same day of the week and week number was the last year */
CREATE OR REPLACE FUNCTION time.get_day_on_same_week_previous_year(i_date DATE DEFAULT current_date)
  RETURNS DATE AS
$$
SELECT
  same_week_last_year._date
  FROM time.day today
  JOIN time.day same_week_last_year ON (today.day_of_week_id = same_week_last_year.day_of_week_id
      AND today.week_id - 100 = same_week_last_year.week_id)
  WHERE today._date = i_date;

$$
  LANGUAGE SQL;

/* Returns date after adding X business days (Monday to Friday) to input date */
CREATE OR REPLACE FUNCTION time.add_business_days(i_from_date DATE, i_number_of_business_days SMALLINT)
  RETURNS DATE AS
$$
SELECT
    i_from_date + extract(DAY FROM to_date::TIMESTAMP - i_from_date::TIMESTAMP)::INTEGER
  FROM (SELECT
          _date AS                           to_date,
          row_number() OVER (ORDER BY _date) rn
          FROM time.day
          WHERE _date > i_from_date
            AND day_of_week_id NOT IN (6, 7)
          LIMIT i_number_of_business_days) bd
  WHERE rn = i_number_of_business_days;
$$
  LANGUAGE SQL
  IMMUTABLE;

/* Returns timestamp after adding X business days (Monday to Friday) to input timestamp */
CREATE OR REPLACE FUNCTION time.add_business_days(i_from_date TIMESTAMP WITH TIME ZONE,
                                                  i_number_of_business_days SMALLINT)
  RETURNS TIMESTAMP WITH TIME ZONE AS
$$
SELECT
  i_from_date + ('' || num_days || 'day')::INTERVAL
  FROM (SELECT
          extract(DAY FROM _date::TIMESTAMP - date_trunc('day', i_from_date))::TEXT AS num_days,
          row_number() OVER (ORDER BY _date)                                           rn
          FROM time.day
          WHERE _date > i_from_date
            AND day_of_week_id NOT IN (6, 7)
          LIMIT i_number_of_business_days) bd
  WHERE rn = i_number_of_business_days;
$$
  LANGUAGE SQL
  IMMUTABLE;
