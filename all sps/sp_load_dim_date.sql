
CREATE OR REPLACE PROCEDURE f1_data_lakehouse.bronze.sp_load_dim_date()
SQL SECURITY INVOKER
AS
BEGIN

-- ------------------------------------------------------------
-- sp_load_dim_date_and_race
-- Sources: bronze.src_races + bronze.src_seasons
-- Transformation:
--   1. Derive DIM_DATE rows from all distinct race dates
--   2. Load DIM_RACE joining to DIM_CIRCUIT and DIM_DATE
-- Why one procedure: DIM_RACE depends on DIM_DATE keys,
--   so we generate dates first, then load races.
-- ------------------------------------------------------------
 
    -- STEP 1: Insert new date rows for any race date not yet in DIM_DATE
    INSERT INTO f1_data_lakehouse.silver.DIM_DATE (
        date_key, full_date, year, quarter, month_num, month_name,
        day_of_month, day_of_week_num, day_of_week_name, is_weekend, f1_season
    )

WITH cleaned_data AS (
    SELECT 
        *,
        -- Step 1: Create a unified DATE object from the mixed string formats
        COALESCE(
            try_to_date(r.date, 'yyyy-MM-dd'),
            try_to_date(r.date, 'dd-MM-yyyy'),
            try_to_date(r.date, 'dd/MM/yyyy'),
            try_to_date(r.date, 'MM/dd/yyyy')
        ) AS clean_date
    FROM f1_data_lakehouse.bronze.races r -- assuming 'r' comes from a table like 'races'
	WHERE NOT EXISTS (
        SELECT 1 FROM f1_data_lakehouse.silver.DIM_DATE d
        WHERE d.full_date = CAST(r.date AS DATE)
)
)
SELECT
    -- date_key: YYYYMMDD as INT
    CAST(date_format(clean_date, 'yyyyMMdd') AS INT) AS date_key,
    
    -- full_date: Actual Date type
    clean_date AS full_date,
    
    -- year, quarter, month
    year(clean_date) AS year,
    quarter(clean_date) AS quarter,
    month(clean_date) AS month_num,
    
    -- month_name: Full name (e.g., 'January')
    date_format(clean_date, 'MMMM') AS month_name,
    
    -- day_of_month
    day(clean_date) AS day_of_month,
    
    -- day_of_week_num: 1 (Sunday) to 7 (Saturday)
    dayofweek(clean_date) AS day_of_week_num,
    
    -- day_of_week_name: Full name (e.g., 'Monday')
    date_format(clean_date, 'EEEE') AS day_of_week_name,
    
    -- is_weekend: Checks if day is 1 (Sun) or 7 (Sat)
    CASE 
        WHEN dayofweek(clean_date) IN (1, 7) THEN 1 
        ELSE 0 
    END AS is_weekend,
    
    -- f1_season
    CAST(year AS SMALLINT) AS f1_season
    
FROM cleaned_data;
 
    
 
END;
