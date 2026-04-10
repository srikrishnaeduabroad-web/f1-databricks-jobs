CREATE OR REPLACE PROCEDURE f1_data_lakehouse.bronze.sp_load_dim_race()
SQL SECURITY INVOKER
AS
BEGIN
    -- STEP 2: Load DIM_RACE joining to DIM_CIRCUIT and DIM_DATE
	
	CREATE TEMP TABLE source 
	as 		
        SELECT
            r.raceId                                            AS race_id,
            CAST(r.year AS INT)                           AS season_year,
            CAST(r.round AS INT)                           AS round_number,
            r.name                                             AS race_name,
			COALESCE(
            try_to_date( CAST(r.date AS DATE), 'yyyy-MM-dd'),
            try_to_date( CAST(r.date AS DATE), 'dd-MM-yyyy'),
            try_to_date( CAST(r.date AS DATE), 'dd/MM/yyyy'),
            try_to_date( CAST(r.date AS DATE), 'MM/dd/yyyy')
        ) AS race_date,
           r.time                          AS race_time,
            dc.circuit_key,
            dd.date_key,
            r.url                                              AS wikipedia_url
        FROM f1_data_lakehouse.bronze.races as r
        -- Link to DIM_CIRCUIT using the natural key
        LEFT JOIN f1_data_lakehouse.silver.DIM_CIRCUIT dc ON dc.circuit_id = r.circuitId
        -- Link to DIM_DATE using the date we just populated
        LEFT JOIN f1_data_lakehouse.silver.DIM_DATE dd 
    ON dd.full_date = COALESCE(
        try_to_date(r.date, 'yyyy-MM-dd'),
        try_to_date(r.date, 'dd-MM-yyyy'),
        try_to_date(r.date, 'dd/MM/yyyy'),
        try_to_date(r.date, 'MM/dd/yyyy')
    )
        WHERE r.raceId IS NOT NULL
    ;
	
    MERGE INTO f1_data_lakehouse.silver.DIM_RACE AS tgt
    USING  AS src ON tgt.race_id = src.race_id
 
    WHEN MATCHED --AND tgt.race_name <> src.race_name 
	THEN
        UPDATE SET
            tgt.season_year    = src.season_year,
            tgt.round_number   = src.round_number,
            tgt.race_name      = src.race_name,
            tgt.race_date      = src.race_date,
            tgt.race_time      = src.race_time,
            tgt.circuit_key    = src.circuit_key,
            tgt.date_key       = src.date_key,
            tgt.wikipedia_url  = src.wikipedia_url
 
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (race_id, season_year, round_number, race_name, race_date,
                race_time, circuit_key, date_key, wikipedia_url)
        VALUES (src.race_id, src.season_year, src.round_number, src.race_name,
                src.race_date, src.race_time, src.circuit_key,
                src.date_key, src.wikipedia_url);
 

END;