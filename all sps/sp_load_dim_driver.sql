CREATE OR REPLACE PROCEDURE f1_data_lakehouse.bronze.sp_load_dim_driver()
SQL SECURITY INVOKER

BEGIN
    
 
    MERGE INTO f1_data_lakehouse.silver.DIM_DRIVER AS tgt
    USING (
        SELECT
            CAST(driverId AS INT) AS driver_id,
            TRIM(driverRef) AS driver_ref,
            NULLIF(code, '') AS code,
            CONCAT(TRIM(forename), ' ', TRIM(surname)) AS full_name,
            TRIM(forename) AS forename,
            TRIM(surname) AS surname,
            -- Handle multiple date formats (d/MM/yyyy or yyyy-MM-dd)
             date_format( COALESCE( try_to_date(dob, 'yyyy-MM-dd'), try_to_date(dob, 'dd-MM-yyyy'), try_to_date(dob, 'dd/MM/yyyy')), 'yyyy-MM-dd') AS date_of_birth, 
            nationality,
            TRY_CAST(number AS TINYINT)                   AS car_number,
            url                                           AS wikipedia_url
        FROM f1_data_lakehouse.bronze.drivers
        WHERE driverId IS NOT NULL
    ) AS src ON tgt.driver_id = src.driver_id
 
    WHEN MATCHED 
	--AND (
    --    tgt.full_name    <> src.full_name    OR
    --    tgt.nationality  <> src.nationality  OR
    --    tgt.car_number   <> src.car_number   OR
    --    tgt.code         <> src.code
    --) 
	THEN
        UPDATE SET
            tgt.driver_ref    = src.driver_ref,
            tgt.code          = src.code,
            tgt.full_name     = src.full_name,
            tgt.forename      = src.forename,
            tgt.surname       = src.surname,
            tgt.date_of_birth = src.date_of_birth,
            tgt.nationality   = src.nationality,
            tgt.car_number    = src.car_number,
            tgt.wikipedia_url = src.wikipedia_url,
            tgt.dw_updated_at = CURRENT_TIMESTAMP()
 
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (driver_id, driver_ref, code, full_name, forename, surname,
                date_of_birth, nationality, car_number, wikipedia_url)
        VALUES (src.driver_id, src.driver_ref, src.code, src.full_name,
                src.forename, src.surname, src.date_of_birth,
                src.nationality, src.car_number, src.wikipedia_url);
 
    
END;
