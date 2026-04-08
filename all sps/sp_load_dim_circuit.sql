
CREATE OR REPLACE PROCEDURE f1_data_lakehouse.bronze.sp_load_dim_circuit()
SQL SECURITY INVOKER
AS
BEGIN
-- ------------------------------------------------------------
-- sp_load_dim_circuit
-- Source table: bronze.src_circuits  (circuits.csv)
-- Transformation: cast lat/lng to DECIMAL; handle missing alt
-- ------------------------------------------------------------

    MERGE INTO f1_data_lakehouse.silver.DIM_CIRCUIT AS tgt
    USING (
        SELECT
            circuitId                              AS circuit_id,
            circuitRef                             AS circuit_ref,
            name                                   AS circuit_name,
            location,
            country,
            CAST(lat  AS float)         AS latitude,
            CAST(lng  AS float)         AS longitude,
            CAST(alt  AS float)             AS altitude_m,
            url                                    AS wikipedia_url
        FROM f1_data_lakehouse.bronze.circuits
        WHERE circuitId IS NOT NULL
    ) AS src ON tgt.circuit_id = src.circuit_id
 
    WHEN MATCHED 
	--AND (
    --    tgt.circuit_name <> src.circuit_name OR
    --    tgt.location     <> src.location     OR
    --    tgt.country      <> src.country
    --) 
	THEN
        UPDATE SET
            tgt.circuit_ref  = src.circuit_ref,
            tgt.circuit_name = src.circuit_name,
            tgt.location     = src.location,
            tgt.country      = src.country,
            tgt.latitude     = src.latitude,
            tgt.longitude    = src.longitude,
            tgt.altitude_m   = src.altitude_m,
            tgt.wikipedia_url = src.wikipedia_url,
            tgt.dw_updated_at = CURRENT_TIMESTAMP()
 
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (circuit_id, circuit_ref, circuit_name, location, country,
                latitude, longitude, altitude_m, wikipedia_url)
        VALUES (src.circuit_id, src.circuit_ref, src.circuit_name,
                src.location, src.country, src.latitude, src.longitude,
                src.altitude_m, src.wikipedia_url);
 
    
END;
