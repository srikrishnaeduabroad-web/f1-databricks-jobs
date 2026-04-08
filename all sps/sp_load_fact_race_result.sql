CREATE OR REPLACE PROCEDURE f1_data_lakehouse.bronze.sp_load_fact_race_result()
SQL SECURITY INVOKER
AS
BEGIN
-- ------------------------------------------------------------
-- sp_load_fact_race_result  ← THE MAIN PROCEDURE
-- Sources joined (all via LEFT JOIN so we never lose a result
--   row even if qualifying/pit data is missing):
--
--   src_results          → core race outcome per driver
--   src_qualifying       → Q1/Q2/Q3 lap times, grid position
--   src_pitStops (agg)   → pit stop count, total & avg time
--   src_status           → human-readable finish status label
--   src_driverStandings  → cumulative champ points after race
--   src_constructorResults → constructor race points
--   DIM_RACE / DIM_DRIVER / DIM_CONSTRUCTOR / DIM_CIRCUIT / DIM_DATE
--                        → surrogate key lookups
--
-- Qualifying time conversion:
--   Source stores "1:26.572" (MM:SS.mmm) → converted to ms
-- ------------------------------------------------------------
    
 
    -- Helper CTE: convert "M:SS.mmm" time strings from qualifying to milliseconds
    -- Example: "1:26.572" → (1 * 60 000) + (26 * 1000) + 572 = 86 572 ms
    -- We use string functions: left of ':' = minutes, right of ':' = seconds.ms
    DECLARE VARIABLE race_id INT ;
    SET race_id = NULL ;
          -- pass a specific raceId for incremental load,
                               -- or leave NULL to reload everything
                                             
  TRUNCATE TABLE f1_data_lakehouse.silver.FACT_RACE_RESULT;
 
    INSERT INTO f1_data_lakehouse.silver.FACT_RACE_RESULT (
        race_key,
        driver_key,
        constructor_key,
        circuit_key,
        date_key,
        src_result_id,
        src_race_id,
        src_driver_id,
        src_constructor_id,
 
        -- Qualifying
        qualifying_position,
        q1_time_ms,
        q2_time_ms,
        q3_time_ms,
        best_qualifying_time_ms,
 
        -- Race start
        grid_position,
 
        -- Race outcome
        finish_position,
        position_order,
        position_text,
        points_scored,
        laps_completed,
        race_time_ms,
        finish_status,
 
        -- Fastest lap
        fastest_lap_number,
        fastest_lap_rank,
        fastest_lap_time_ms,
        fastest_lap_speed_kph,
 
        -- Pit stops
        pit_stop_count,
        total_pit_time_ms,
        avg_pit_time_ms,
 
        -- Championship
        driver_champ_points,
        driver_champ_position,
        driver_champ_wins,
        constructor_race_points,
 
        -- Derived
        positions_gained
    )
     WITH  qualifying_ms AS (
        SELECT
        raceId,
        driverId,
        constructorId,
        qualifying_position,

  q1_minute + q1_sec + q1_ms AS q1_time_ms,
  q2_minute + q2_sec + q2_ms AS q2_time_ms,
  q3_minute + q3_sec + q3_ms AS q3_time_ms
  FROM
(   

SELECT
    q.raceId,
    q.driverId,
    q.constructorId,
    q.position AS qualifying_position,

CASE 
  WHEN q1 IS NOT NULL AND TRIM(q1) <> '' AND split(trim(q1), ':')[0] RLIKE '^[0-9]+$'
  THEN CAST(split(trim(q1), ':')[0] AS INT) * 60000
  ELSE 0
END AS q1_minute,

CASE
  WHEN q1 IS NOT NULL AND TRIM(q1) <> '' AND split(trim(q1), ':')[0] RLIKE '^[0-9]+$'
       AND size(split(q1, ':')) > 1      
  THEN CAST(get(split(get(split(q1, ':'), 1), '\\.'), 0) AS INT) * 1000
  ELSE 0
END AS q1_sec,

CASE
  WHEN q1 IS NOT NULL AND TRIM(q1) <> '' AND split(trim(q1), ':')[0] RLIKE '^[0-9]+$'
       AND size(split(q1, ':')) > 1      
  THEN CAST(get(split(get(split(q1, ':'), 1), '\\.'), 1) AS INT) 
  ELSE 0
END AS q1_ms,

CASE 
  WHEN q2 IS NOT NULL AND TRIM(q2) <> '' AND split(trim(q2), ':')[0] RLIKE '^[0-9]+$'
  THEN CAST(split(trim(q2), ':')[0] AS INT) * 60000
  ELSE 0
END AS q2_minute,

CASE
  WHEN q2 IS NOT NULL AND TRIM(q2) <> '' AND split(trim(q2), ':')[0] RLIKE '^[0-9]+$'
       AND size(split(q2, ':')) > 1      
  THEN CAST(get(split(get(split(q2, ':'), 1), '\\.'), 0) AS INT) * 1000
  ELSE 0
END AS q2_sec,

CASE
  WHEN q2 IS NOT NULL AND TRIM(q2) <> '' AND split(trim(q2), ':')[0] RLIKE '^[0-9]+$'
       AND size(split(q2, ':')) > 1      
  THEN CAST(get(split(get(split(q2, ':'), 1), '\\.'), 1) AS INT) 
  ELSE 0
END AS q2_ms,

CASE 
  WHEN q3 IS NOT NULL AND TRIM(q3) <> '' AND split(trim(q3), ':')[0] RLIKE '^[0-9]+$'
  THEN CAST(split(trim(q3), ':')[0] AS INT) * 60000
  ELSE 0
END AS q3_minute,
CASE
  WHEN q3 IS NOT NULL AND TRIM(q3) <> '' AND split(trim(q3), ':')[0] RLIKE '^[0-9]+$'
       AND size(split(q3, ':')) > 1      
  THEN CAST(get(split(get(split(q3, ':'), 1), '\\.'), 0) AS INT) * 1000
  ELSE 0
END AS q3_sec,

CASE
  WHEN q3 IS NOT NULL AND TRIM(q3) <> '' AND split(trim(q3), ':')[0] RLIKE '^[0-9]+$'
       AND size(split(q3, ':')) > 1      
  THEN CAST(get(split(get(split(q3, ':'), 1), '\\.'), 1) AS INT) 
  ELSE 0
END AS q3_ms

FROM f1_data_lakehouse.bronze.qualifying q

)
 ),
    
 
    -- Helper CTE: aggregate pit stop stats per race + driver
    pit_agg AS (
        SELECT
            raceId,
            driverId,
            COUNT(*) AS pit_stop_count,
            SUM(milliseconds) AS total_pit_time_ms,
            AVG(CAST(milliseconds AS DECIMAL)) AS avg_pit_time_ms
        FROM f1_data_lakehouse.bronze.pit_stops
        GROUP BY raceId, driverId
    ),
 
    -- Helper CTE: driver championship standings for this race
    driver_standings AS (
        SELECT
            raceId,
            driverId,
            points     AS driver_champ_points,
            position   AS driver_champ_position,
            wins       AS driver_champ_wins
        FROM f1_data_lakehouse.bronze.driver_standings
    ),
 
    -- Helper CTE: constructor race-level points
    constructor_pts AS (
        SELECT
            raceId,
            constructorId,
            points AS constructor_race_points
        FROM f1_data_lakehouse.bronze.constructors_results
    )
 
    -- MAIN INSERT — delete + reload for the target race(s), then insert
    -- (full truncate-reload pattern; swap for MERGE if you need idempotency)

    SELECT
        -- Dimension surrogate key lookups
        dr.race_key,
        dd.driver_key,
        dc.constructor_key,
        dcirc.circuit_key,
        bronze.date_key,
 
        -- Lineage keys
        r.resultId,
        r.raceId,
        r.driverId,
        r.constructorId,
 
        -- Qualifying (may be NULL if driver withdrew before qualifying)
        q.qualifying_position,
        q.q1_time_ms,
        q.q2_time_ms,
        q.q3_time_ms,
        -- Best qualifying time = lowest non-null among Q1/Q2/Q3
        CASE
            WHEN q1_time_ms < q2_time_ms AND q1_time_ms < q3_time_ms THEN q.q1_time_ms
            WHEN q2_time_ms < q1_time_ms AND q2_time_ms < q3_time_ms THEN q.q2_time_ms
            ELSE q.q3_time_ms
        END AS  best_qualifying_time_ms,
 
        -- Grid position from results (official grid, post-penalties)
        CAST(r.grid AS TINYINT) AS grid_position,
 
        -- Race outcome
        TRY_CAST(r.position AS TINYINT)         AS finish_position,
        CAST(r.positionOrder AS TINYINT)        AS position_order,
        r.positionText                          AS position_text,
        CAST(r.points AS DECIMAL(5,2))          AS points_scored,
        CAST(r.laps AS SMALLINT)                AS laps_completed,
        TRY_CAST(r.milliseconds AS BIGINT)      AS race_time_ms,
        st.status                               AS finish_status,
 
        -- Fastest lap info
        TRY_CAST(r.fastestLap AS SMALLINT)      AS fastest_lap_number,
        TRY_CAST(r.rank       AS TINYINT)       AS fastest_lap_rank,
        -- Convert fastest lap time "1:27.5" → milliseconds
        CASE WHEN NULLIF(r.fastestLapTime,'') IS NOT NULL THEN
            CAST(LEFT(r.fastestLapTime, CHARINDEX(':',r.fastestLapTime)-1) AS INT) * 60000
            + CAST(SUBSTRING(r.fastestLapTime, CHARINDEX(':',r.fastestLapTime)+1, 2) AS INT) * 1000
            + CAST(REPLACE(SUBSTRING(r.fastestLapTime, CHARINDEX('.',r.fastestLapTime)+1, 3),' ','0') AS INT)
        END                                     AS fastest_lap_time_ms,
        TRY_CAST(r.fastestLapSpeed AS DECIMAL(7,3)) AS fastest_lap_speed_kph,
 
        -- Pit stop aggregates (LEFT JOIN → NULLs for no pit stop data)
        COALESCE(p.pit_stop_count, 0)           AS pit_stop_count,
        p.total_pit_time_ms,
        p.avg_pit_time_ms,
 
        -- Championship standings after this race
        ds.driver_champ_points,
        ds.driver_champ_position,
        ds.driver_champ_wins,
 
        -- Constructor points for this race
        cp.constructor_race_points,
 
        -- Derived: positions gained (negative = fell back)
        CASE
            WHEN r.grid > 0 AND TRY_CAST(r.position AS TINYINT) > 0
            THEN CAST(r.grid AS SMALLINT) - TRY_CAST(r.position AS SMALLINT)
        END                                     AS positions_gained
 
    FROM f1_data_lakehouse.bronze.results r
 
    -- Race dimension lookup
    LEFT JOIN f1_data_lakehouse.silver.dim_race  dr    ON dr.race_id  = r.raceId
 
    -- Driver dimension lookup
    LEFT JOIN f1_data_lakehouse.silver.dim_driver dd    ON dd.driver_id = r.driverId
 
    -- Constructor dimension lookup
    LEFT JOIN f1_data_lakehouse.silver.dim_constructor dc    ON dc.constructor_id = r.constructorId
 
    -- Circuit and date from the race (via dim_race)
    LEFT JOIN f1_data_lakehouse.silver.dim_circuit     dcirc ON dcirc.circuit_key = dr.circuit_key
    LEFT JOIN f1_data_lakehouse.silver.dim_date        bronze ON bronze.date_key = dr.date_key
 
    -- Status lookup (LEFT JOIN: status may be blank for historical data)
    LEFT JOIN f1_data_lakehouse.bronze.status   st    ON st.statusId = r.statusId
 
    -- Qualifying (LEFT JOIN: not all races/drivers have qualifying data)
    LEFT JOIN qualifying_ms   q     ON q.raceId = r.raceId AND q.driverId = r.driverId
 
    -- Pit stop aggregates (LEFT JOIN: pit stop data starts from 2011)
    LEFT JOIN pit_agg  p     ON p.raceId = r.raceId AND p.driverId = r.driverId
 
    -- Driver standings (LEFT JOIN: first race of season may lag)
    LEFT JOIN driver_standings ds    ON ds.raceId = r.raceId    AND ds.driverId = r.driverId
 
    -- Constructor race points (LEFT JOIN)
    LEFT JOIN constructor_pts  cp    ON cp.raceId = r.raceId  AND cp.constructorId = r.constructorId
 
    -- Optional filter for incremental/targeted load
    WHERE (race_id IS NULL OR r.raceId = race_id);
 
END
;
