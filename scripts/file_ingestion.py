import gdown
import os
import shutil

FOLDER_ID = "1jBa0pv-oMTXz-cGDnGhG8LkM4CyOSqZU"
base_path = "/Volumes/f1_data_lakehouse/inbound_files/raw_files/"

print("Downloading all files from Google Drive folder...")
gdown.download_folder(
    id=FOLDER_ID,
    output=base_path,
    quiet=False
)
print("All files downloaded!\n")

# ================================================
# Notebook: airflow_ingest_notebook
# Triggered by Airflow — reads metadata and loads files
# ================================================

import os
import shutil

# ================================================
# Base path — Unity Catalog Volume
# ================================================
base_path = "/Volumes/f1_data_lakehouse/inbound_files/raw_files/all csv files n data/"

# ================================================
# Step 1: Read metadata table
# ================================================
metadata_df = spark.sql("""
    SELECT 
        lower(file_name)      AS file_name,
        lower(target_table)   AS target_table,
        lower(file_type)      AS file_type,
        file_delimiter
    FROM f1_data_lakehouse.file_ingestion.file_to_ingest
""")

metadata = metadata_df.collect()
print(f"Found {len(metadata)} entries in file_to_ingest table")

# ================================================
# Step 2: List files in Volume
# ================================================
try:
    files_in_volume = os.listdir(base_path)
    file_names_in_volume = [f.lower() for f in files_in_volume]
    print(f"Files found in Volume: {file_names_in_volume}")
except Exception as e:
    print(f"No files found in Volume path: {base_path}")
    file_names_in_volume = []

# ================================================
# Step 3: Match and Load
# ================================================
loaded = []
skipped = []

for row in metadata:
    file_name_pattern = row["file_name"]       # e.g. "circuits"
    target_table      = row["target_table"]    # e.g. "circuits"
    file_type         = row["file_type"]       # e.g. "csv"
    delimiter         = row["file_delimiter"]  # e.g. ","

    # Build expected file name: circuits.csv
    expected_file = f"{file_name_pattern}.{file_type}"
    full_target   = f"f1_data_lakehouse.bronze.{target_table}"
    full_path     = f"{base_path}{expected_file}"

    # Check if file exists in Volume
    if expected_file in file_names_in_volume:
        print(f"\n Matching: {expected_file} → {full_target}")

        try:
            # Read CSV with correct delimiter
            df = spark.read.format("csv") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("delimiter", delimiter) \
                .load(full_path)

            print(f"    Rows: {df.count()} | Columns: {len(df.columns)}")

            # Ensure bronze schema exists
            spark.sql("CREATE SCHEMA IF NOT EXISTS f1_data_lakehouse.bronze")

            # Load into bronze table
            df.write.format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .saveAsTable(full_target)

            print(f" Loaded into {full_target}")
            loaded.append(expected_file)

            # Archive file after loading
            archive_folder = f"{base_path}processed/"
            os.makedirs(archive_folder, exist_ok=True)
            shutil.move(full_path, f"{archive_folder}{expected_file}")
            print(f"    Archived to {archive_folder}{expected_file}")

        except Exception as e:
            print(f"    Error loading {expected_file}: {str(e)}")
    else:
        print(f"⏭️  Skipping: {expected_file} — not found in Volume")
        skipped.append(expected_file)

# ================================================
# Step 4: Summary
# ================================================
print("\n" + "="*50)
print(f" Successfully loaded : {loaded}")
print(f"  Skipped (not found): {skipped}")
print("="*50)