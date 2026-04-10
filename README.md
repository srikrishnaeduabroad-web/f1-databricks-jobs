#  Formula 1 Data Lakehouse — Databricks, PySpark & Tableau

An end-to-end data engineering project that ingests 70+ years of Formula 1 race history, transforms it through a Medallion Architecture on Databricks, and delivers analytics through Tableau dashboards — built to answer the ultimate F1 debate: "is the championship won by the driver, or the car?"


## Architecture Overview


Google Drive (Raw CSVs)
        │
        ▼
Unity Catalog Volume          ← Landing zone inside Databricks
        │
        ▼
Bronze Layer                  ← PySpark metadata-driven Delta ingestion
        │
        ▼
Silver Layer                  ← Star Schema via SQL Stored Procedures
   ┌────┴────┐
   │         │
5 Dims     1 Fact
   └────┬────┘
        │
        ▼
Export Notebook               ← Writes Silver data back to Google Drive
        │
        ▼
Tableau                       ← Dashboards & Analysis


---

## Tech Stack

| Layer            | Technology                              |
|------------------|-----------------------------------------|
| Compute          | Databricks                              |
| Storage          | Databricks Unity Catalog, Delta Lake    |
| Ingestion        | PySpark (`spark.read`, Delta write)     |
| Transformation   | Databricks SQL, Stored Procedures       |
| Orchestration    | Databricks Jobs & Pipelines             |
| Visualisation    | Tableau (connected to Google Drive)     |
| Source & Export  | Google Drive                            |

---

## Project Structure


f1-data-lakehouse/

ingestion/file_ingestion.py          # PySpark metadata-driven ingestion notebook

stored_procedures/
  sp_load_dim_circuit.sql
  sp_load_dim_constructor.sql
  sp_load_dim_date.sql
  sp_load_dim_driver.sql
  sp_load_dim_race.sql
  sp_load_fact_race_result.sql

architecture - medallion_diagram.png

## How It Works

### Step 1 — Ingestion into Bronze Layer

Raw CSV files are downloaded from the Ergast F1 dataset and uploaded to "Google Drive", which is then mounted into a "Databricks Unity Catalog Volume" at:


/Volumes/f1_data_lakehouse/inbound_files/raw_files/


Rather than hardcoding any file paths or table names in the ingestion code, a "metadata-driven pattern" is used. A control table stored in Databricks tells the pipeline exactly what to load and where to put it.

#### Control Table — `f1_data_lakehouse.file_ingestion.file_to_ingest`

| Column           | Type    | Description                                      | Example         |
|------------------|---------|--------------------------------------------------|-----------------|
| `file_name`      | STRING  | Name of the file without extension               | `circuits`      |
| `file_type`      | STRING  | File format/extension                            | `csv`           |
| `target_table`   | STRING  | Destination table name in the Bronze layer       | `circuits`      |
| `file_delimiter` | STRING  | Delimiter used in the CSV file                   | `,`             |

The PySpark notebook (`ingest_bronze.py`) does the following on each run:

1. Reads the control table using `spark.sql`
2. Lists all files currently present in the Unity Catalog Volume using `os.listdir`
3. Matches each metadata entry against what is physically in the Volume
4. For matched files — reads them using `spark.read.format("csv")` with inferred schema and writes them as "Delta tables" into `f1_data_lakehouse.bronze.*` using `overwrite` mode
5. Archives each processed file into a `/processed/` subfolder so it is not reloaded on the next run
6. Skips any file listed in the metadata but not yet present in the Volume — no errors, no failures

This means adding a brand new data source requires "zero code changes" — just a single new row in the control table.

#### Bronze Tables Loaded

| Table                                         | Source File            |
|-----------------------------------------------|------------------------|
| `f1_data_lakehouse.bronze.circuits`           | `circuits.csv`         |
| `f1_data_lakehouse.bronze.drivers`            | `drivers.csv`          |
| `f1_data_lakehouse.bronze.constructors`       | `constructors.csv`     |
| `f1_data_lakehouse.bronze.races`              | `races.csv`            |
| `f1_data_lakehouse.bronze.results`            | `results.csv`          |
| `f1_data_lakehouse.bronze.qualifying`         | `qualifying.csv`       |
| `f1_data_lakehouse.bronze.pitstops`           | `pit_stops.csv`        |
| `f1_data_lakehouse.bronze.driverstandings`    | `driver_standings.csv` |
| `f1_data_lakehouse.bronze.constructorresults` | `constructor_results.csv` |
| `f1_data_lakehouse.bronze.status`             | `status.csv`           |

---

### Step 2 — Transformation into Silver Layer

Once all raw data is in the Bronze layer, it is transformed into a "Star Schema" inside the Silver layer — a design optimised for analytical queries and reporting.

The core question driving the schema design was: "what changes slowly and describes an entity, versus what gets measured per race?"

- Things that describe entities (a circuit's location, a driver's nationality, a constructor's name) go into "dimension tables"
- Things that get measured per race event (finishing position, lap times, pit stop counts, championship points) go into the "fact table"

All transformations are executed via "Databricks SQL Stored Procedures" using MERGE-based upserts — meaning every load is safe to re-run without creating duplicates or losing data. This emphasize the use of "SLOWLY CHANGING DIMENSION TYPE 1"

---

#### Dimension Tables

##### `f1_data_lakehouse.silver.DIM_CIRCUIT`

Loaded by: `sp_load_dim_circuit.sql`

| Column         | Type    | Description                              |
|----------------|---------|------------------------------------------|
| `circuit_key`  | INT     | Surrogate key (auto-generated)           |
| `circuit_id`   | INT     | Natural key from source                  |
| `circuit_ref`  | STRING  | Short reference code                     |
| `circuit_name` | STRING  | Full circuit name                        |
| `location`     | STRING  | City or region                           |
| `country`      | STRING  | Country                                  |
| `latitude`     | FLOAT   | Cast from raw string                     |
| `longitude`    | FLOAT   | Cast from raw string                     |
| `altitude_m`   | FLOAT   | Altitude in metres, cast from raw string |
| `wikipedia_url`| STRING  | Reference URL                            |
| `dw_updated_at`| TIMESTAMP | Last updated timestamp                 |

---

##### `f1_data_lakehouse.silver.DIM_DRIVER`

Loaded by: `sp_load_dim_driver.sql`

| Column          | Type      | Description                                                   |
|-----------------|-----------|---------------------------------------------------------------|
| `driver_key`    | INT       | Surrogate key (auto-generated)                                |
| `driver_id`     | INT       | Natural key from source                                       |
| `driver_ref`    | STRING    | Short reference code, trimmed                                 |
| `code`          | STRING    | 3-letter driver code (e.g. HAM), NULL if absent               |
| `full_name`     | STRING    | Concatenation of forename + surname                           |
| `forename`      | STRING    | First name, trimmed                                           |
| `surname`       | STRING    | Last name, trimmed                                            |
| `date_of_birth` | DATE      | Normalised across 3 source formats (yyyy-MM-dd, dd/MM, dd-MM)|
| `nationality`   | STRING    | Driver nationality                                            |
| `car_number`    | TINYINT   | Permanent car number                                          |
| `wikipedia_url` | STRING    | Reference URL                                                 |
| `dw_updated_at` | TIMESTAMP | Last updated timestamp                                        |

---

##### `f1_data_lakehouse.silver.DIM_CONSTRUCTOR`

Loaded by: `sp_load_dim_constructor.sql`

| Column             | Type      | Description                    |
|--------------------|-----------|--------------------------------|
| `constructor_key`  | INT       | Surrogate key (auto-generated) |
| `constructor_id`   | INT       | Natural key from source        |
| `constructor_ref`  | STRING    | Short reference code           |
| `constructor_name` | STRING    | Full team/constructor name     |
| `nationality`      | STRING    | Constructor nationality        |
| `wikipedia_url`    | STRING    | Reference URL                  |
| `dw_updated_at`    | TIMESTAMP | Last updated timestamp         |

---

##### `f1_data_lakehouse.silver.DIM_DATE`

Loaded by: `sp_load_dim_date.sql`

All distinct race dates across the dataset are used to populate this table. The date key format is `YYYYMMDD` as an integer, which makes range filtering and joins fast.

| Column             | Type      | Description                                      |
|--------------------|-----------|--------------------------------------------------|
| `date_key`         | INT       | YYYYMMDD integer format (e.g. 20210328)          |
| `full_date`        | DATE      | Actual date value                                |
| `year`             | INT       | Calendar year                                    |
| `quarter`          | INT       | Quarter (1–4)                                    |
| `month_num`        | INT       | Month number (1–12)                              |
| `month_name`       | STRING    | Full month name (e.g. March)                     |
| `day_of_month`     | INT       | Day of month                                     |
| `day_of_week_num`  | INT       | Day of week — 1 (Sunday) to 7 (Saturday)         |
| `day_of_week_name` | STRING    | Full day name (e.g. Sunday)                      |
| `is_weekend`       | INT       | 1 if Saturday or Sunday, 0 otherwise             |
| `f1_season`        | SMALLINT  | F1 season year                                   |

---

##### `f1_data_lakehouse.silver.DIM_RACE`

Loaded by: `sp_load_dim_race.sql`

This dimension depends on both `DIM_CIRCUIT` and `DIM_DATE` being loaded first, as it joins to both to resolve surrogate keys.

| Column         | Type      | Description                                         |
|----------------|-----------|-----------------------------------------------------|
| `race_key`     | INT       | Surrogate key (auto-generated)                      |
| `race_id`      | INT       | Natural key from source                             |
| `season_year`  | SMALLINT  | F1 season year                                      |
| `round_number` | TINYINT   | Race round within the season                        |
| `race_name`    | STRING    | Full race name (e.g. British Grand Prix)            |
| `race_date`    | DATE      | Normalised race date across 4 source formats        |
| `race_time`    | TIME      | Local start time                                    |
| `circuit_key`  | INT       | FK → DIM_CIRCUIT                                    |
| `date_key`     | INT       | FK → DIM_DATE                                       |
| `wikipedia_url`| STRING    | Reference URL                                       |

---

#### Fact Table

##### `f1_data_lakehouse.silver.FACT_RACE_RESULT`

Loaded by: `sp_load_fact_race_result.sql`

This is the central table of the entire schema. It consolidates "9 Bronze source tables" through 4 CTEs and resolves surrogate keys to all 5 dimensions.

"Why 9 sources?" A single race result for a single driver involves: the result itself, qualifying session performance, pit stop history, finish status, driver championship standings after the race, and constructor points earned — all stored in separate source tables in the Bronze layer.

| Column                    | Type          | Source / Derivation                                      |
|---------------------------|---------------|----------------------------------------------------------|
| `race_key`                | INT           | FK → DIM_RACE                                            |
| `driver_key`              | INT           | FK → DIM_DRIVER                                          |
| `constructor_key`         | INT           | FK → DIM_CONSTRUCTOR                                     |
| `circuit_key`             | INT           | FK → DIM_CIRCUIT (via DIM_RACE)                          |
| `date_key`                | INT           | FK → DIM_DATE (via DIM_RACE)                             |
| `src_result_id`           | INT           | Lineage — source result ID                               |
| `src_race_id`             | INT           | Lineage — source race ID                                 |
| `src_driver_id`           | INT           | Lineage — source driver ID                               |
| `src_constructor_id`      | INT           | Lineage — source constructor ID                          |
| `qualifying_position`     | INT           | Official qualifying position                             |
| `q1_time_ms`              | BIGINT        | Q1 lap time converted from `M:SS.mmm` → milliseconds    |
| `q2_time_ms`              | BIGINT        | Q2 lap time converted from `M:SS.mmm` → milliseconds    |
| `q3_time_ms`              | BIGINT        | Q3 lap time converted from `M:SS.mmm` → milliseconds    |
| `best_qualifying_time_ms` | BIGINT        | MIN(Q1, Q2, Q3) — best qualifying lap                    |
| `grid_position`           | TINYINT       | Official grid position after penalties                   |
| `finish_position`         | TINYINT       | Final classified position                                |
| `position_order`          | TINYINT       | Numeric ordering including DNFs                          |
| `position_text`           | STRING        | Display value (e.g. "1", "R" for retired)                |
| `points_scored`           | DECIMAL(5,2)  | Championship points earned in this race                  |
| `laps_completed`          | SMALLINT      | Total laps completed                                     |
| `race_time_ms`            | BIGINT        | Total race time in milliseconds                          |
| `finish_status`           | STRING        | Human-readable status (e.g. Finished, Engine, Accident)  |
| `fastest_lap_number`      | SMALLINT      | Lap number on which fastest lap was set                  |
| `fastest_lap_rank`        | TINYINT       | Rank among all drivers for fastest lap                   |
| `fastest_lap_time_ms`     | BIGINT        | Fastest lap converted from `M:SS.mmm` → milliseconds    |
| `fastest_lap_speed_kph`   | DECIMAL(7,3)  | Fastest lap average speed in km/h                        |
| `pit_stop_count`          | INT           | Total pit stops made (0 if no data)                      |
| `total_pit_time_ms`       | BIGINT        | Sum of all pit stop durations in milliseconds            |
| `avg_pit_time_ms`         | DECIMAL       | Average pit stop duration in milliseconds                |
| `driver_champ_points`     | DECIMAL       | Cumulative championship points after this race           |
| `driver_champ_position`   | INT           | Championship standings position after this race          |
| `driver_champ_wins`       | INT           | Total wins in championship up to and including this race |
| `constructor_race_points` | DECIMAL       | Constructor points earned in this race                   |
| `positions_gained`        | SMALLINT      | Grid position minus finish position (negative = fell back)|

---

### Step 3 — Orchestration via Databricks Jobs

A "Databricks Job" runs the full pipeline end-to-end in the correct dependency order:


Step 1 → ingest_bronze.py              (PySpark — loads all Bronze Delta tables)
         │
Step 2 → sp_load_dim_date             (no upstream dependency)
Step 3 → sp_load_dim_circuit          (no upstream dependency)
Step 4 → sp_load_dim_driver           (no upstream dependency)
Step 5 → sp_load_dim_constructor      (no upstream dependency)
         │
Step 6 → sp_load_dim_race             (depends on DIM_DATE + DIM_CIRCUIT)
         │
Step 7 → sp_load_fact_race_result     (depends on all 5 dimensions)
         │
Step 8 → export_notebook              (reads Silver layer, writes to Google Drive)


Steps 2–5 can run in parallel since they have no dependency on each other. Step 6 must wait for Steps 2 and 3. Step 7 must wait for all dimension loads to complete. This sequencing guarantees that all surrogate keys exist in the dimension tables before the fact table tries to resolve them.

---

### Step 4 — Analytics via Tableau

Tableau connects directly to the exported star schema files on Google Drive and powers dashboards including:

- "Constructor dominance radial bar chart" — which team has spent the most seasons at the top, decade by decade
- "Driver record analysis" — most wins, pole positions, and fastest laps across all seasons
- "Race performance deep-dives" — positions gained from grid, pit stop strategy impact, qualifying vs race pace comparison

---

## Key Design Decisions

"Metadata-driven ingestion" — No file paths or table names are hardcoded anywhere in the ingestion notebook. The control table drives everything. Adding a new data source is a one-row metadata change, not a code deployment.

"MERGE over truncate-reload" — All dimension stored procedures use MERGE upserts on natural keys. This means loads are idempotent — safe to re-run at any time without producing duplicates or losing historical records.

"Surrogate key resolution at fact load time" — The fact SP joins to all 5 dimensions at load time, so the fact table always contains valid foreign keys. No post-load patching or re-keying is ever needed.

"LEFT JOINs in the fact load" — Qualifying data only exists from a certain era, and pit stop data starts from 2011. Using LEFT JOINs ensures no result row is ever dropped from the fact table just because supplementary data is absent for that period.

"Date normalisation across 4 formats" — Raw F1 data contains dates in mixed formats across different source files. All date columns are normalised using `try_to_date` across `yyyy-MM-dd`, `dd-MM-yyyy`, `dd/MM/yyyy`, and `MM/dd/yyyy` — handled consistently in both the date dimension and the race dimension stored procedures.

"Lap time conversion to milliseconds" — Qualifying and fastest lap times arrive as strings in `M:SS.mmm` format (e.g. `1:26.572`). These are parsed using string functions and converted to integer milliseconds, making them directly sortable, aggregatable, and comparable without any further transformation downstream.

---

