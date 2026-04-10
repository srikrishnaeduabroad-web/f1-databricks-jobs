
CREATE OR REPLACE PROCEDURE f1_data_lakehouse.bronze.sp_load_dim_constructor()
SQL SECURITY INVOKER
BEGIN
-- ------------------------------------------------------------
-- sp_load_dim_constructor
-- Source table: bronze.src_constructors  (constructors.csv)
-- Strategy: MERGE on constructorId natural key
-- ------------------------------------------------------------

 
    MERGE INTO f1_data_lakehouse.silver.DIM_CONSTRUCTOR AS tgt
    USING (
        SELECT
            CAST(constructorId AS INT)   AS constructor_id,
            constructorRef  AS constructor_ref,
            name            AS constructor_name,
            nationality,
            url             AS wikipedia_url
        FROM f1_data_lakehouse.bronze.constructors
        WHERE constructorId IS NOT NULL
    ) AS src ON tgt.constructor_id = src.constructor_id
 
    WHEN MATCHED 
	--AND (
    --    tgt.constructor_name <> src.constructor_name OR
    --    tgt.nationality      <> src.nationality
    --) 
	THEN
        UPDATE SET
            tgt.constructor_ref  = src.constructor_ref,
            tgt.constructor_name = src.constructor_name,
            tgt.nationality      = src.nationality,
            tgt.wikipedia_url    = src.wikipedia_url,
            tgt.dw_updated_at    = CURRENT_TIMESTAMP()
 
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (constructor_id, constructor_ref, constructor_name,
                nationality, wikipedia_url)
        VALUES (src.constructor_id, src.constructor_ref,
                src.constructor_name, src.nationality, src.wikipedia_url);
 
END;
