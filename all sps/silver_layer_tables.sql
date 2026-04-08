-- DIM_DRIVER
CREATE OR REPLACE TABLE f1_data_lakehouse.silver.DIM_DRIVER (
    driver_key      BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
    driver_id       INT NOT NULL,          -- natural key from source
    driver_ref      STRING,
    code            STRING,                -- e.g. HAM, VER, ALO
    full_name       STRING,
    forename        STRING,
    surname         STRING,
    date_of_birth   DATE,
    nationality     STRING,
    car_number      TINYINT,
    wikipedia_url   STRING,
    dw_inserted_at  TIMESTAMP ,
    dw_updated_at   TIMESTAMP ,
    CONSTRAINT dim_driver_pk PRIMARY KEY (driver_key)
);

-- DIM_CONSTRUCTOR
CREATE TABLE f1_data_lakehouse.silver.DIM_CONSTRUCTOR (
    constructor_key BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
    constructor_id  INT NOT NULL,
    constructor_ref STRING,
    constructor_name STRING,
    nationality     STRING,
    wikipedia_url   STRING,
    dw_inserted_at  TIMESTAMP ,
    dw_updated_at   TIMESTAMP ,
    CONSTRAINT dim_constructor_pk PRIMARY KEY (constructor_key)
);

-- DIM_CIRCUIT
CREATE TABLE f1_data_lakehouse.silver.DIM_CIRCUIT (
    circuit_key     BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
    circuit_id      INT NOT NULL,
    circuit_ref     STRING,
    circuit_name    STRING,
    location        STRING,
    country         STRING,
    latitude        DECIMAL(9,6),
    longitude       DECIMAL(9,6),
    altitude_m      SMALLINT,
    wikipedia_url   STRING,
    dw_inserted_at  TIMESTAMP ,
    dw_updated_at   TIMESTAMP ,
    CONSTRAINT dim_circuit_pk PRIMARY KEY (circuit_key)
);

-- DIM_DATE
CREATE TABLE f1_data_lakehouse.silver.DIM_DATE (
    date_key         INT NOT NULL,
    full_date        DATE NOT NULL,
    year             SMALLINT,
    quarter          TINYINT,
    month_num        TINYINT,
    month_name       STRING,
    day_of_month     TINYINT,
    day_of_week_num  TINYINT,
    day_of_week_name STRING,
    is_weekend       BOOLEAN,
    f1_season        SMALLINT,
    dw_inserted_at   TIMESTAMP ,
    CONSTRAINT dim_date_pk PRIMARY KEY (date_key)
);

-- DIM_RACE
CREATE TABLE f1_data_lakehouse.silver.DIM_RACE (
    race_key        BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
    race_id         INT NOT NULL,
    season_year     SMALLINT,
    round_number    TINYINT,
    race_name       STRING,
    race_date       DATE,
    race_time       STRING, -- Databricks does not have a TIME type; using STRING
    circuit_key     BIGINT,
    date_key        INT,
    wikipedia_url   STRING,
    dw_inserted_at  TIMESTAMP ,
    CONSTRAINT dim_race_pk PRIMARY KEY (race_key)
);

-- FACT_RACE_RESULT
CREATE TABLE f1_data_lakehouse.silver.FACT_RACE_RESULT (
    result_key                 BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
    
    -- Foreign keys to dimensions
    race_key                   BIGINT NOT NULL,
    driver_key                 BIGINT NOT NULL,
    constructor_key            BIGINT NOT NULL,
    circuit_key                BIGINT NOT NULL,
    date_key                   INT NOT NULL,
 
    -- Source natural keys
    src_result_id              INT,
    src_race_id                INT,
    src_driver_id              INT,
    src_constructor_id         INT,
 
    -- Qualifying metrics
    qualifying_position        TINYINT,
    q1_time_ms                 INT,
    q2_time_ms                 INT,
    q3_time_ms                 INT,
    best_qualifying_time_ms    INT,
 
    -- Race start
    grid_position              TINYINT,
 
    -- Race outcome
    finish_position            TINYINT,
    position_order             TINYINT,
    position_text              STRING,
    points_scored              DECIMAL(5,2),
    laps_completed             SMALLINT,
    race_time_ms               BIGINT,
    finish_status              STRING,
 
    -- Fastest lap
    fastest_lap_number         SMALLINT,
    fastest_lap_rank           TINYINT,
    fastest_lap_time_ms        INT,
    fastest_lap_speed_kph      DECIMAL(7,3),
 
    -- Pit stops
    pit_stop_count             TINYINT,
    total_pit_time_ms          INT,
    avg_pit_time_ms            DECIMAL(8,1),
 
    -- Championship standings
    driver_champ_points        DECIMAL(6,2),
    driver_champ_position      TINYINT,
    driver_champ_wins          TINYINT,
 
    -- Constructor points
    constructor_race_points    DECIMAL(5,2),
 
    -- Grid vs finish delta
    positions_gained           SMALLINT,
 
    dw_inserted_at             TIMESTAMP ,
    CONSTRAINT fact_race_result_pk PRIMARY KEY (result_key)
);