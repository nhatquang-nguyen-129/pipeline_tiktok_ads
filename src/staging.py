"""
==================================================================
TIKTOK STAGING MODULE
------------------------------------------------------------------
This module transforms raw TikTok Ads data into enriched, normalized 
staging tables in Google BigQuery, acting as the bridge between raw 
API ingestion and final MART-level analytics.

It combines raw ad/campaign/creative data, applies business logic  
(e.g., parsing naming conventions, standardizing fields), and  
prepares clean, query-ready datasets for downstream consumption.

✔️ Joins raw ad insights with creative and campaign metadata  
✔️ Enriches fields such as owner, program code, placement, format  
✔️ Normalizes and writes standardized tables into the staging dataset  
✔️ Validates data integrity and ensures field completeness  
✔️ Supports modular extension for new TikTok Ads entities  

⚠️ This module is strictly responsible for *data transformation*  
into staging format. It does **not** handle API ingestion or final  
MART aggregations.
==================================================================
"""

# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add Python logging ultilities for integration
import logging

# Add Python time ultilities for integration
import time

from contextlib import ExitStack

# Add Python Pandas libraries for integration
import pandas as pd

# Add Google Cloud modules for integration
from google.cloud import bigquery

# Add internal TikTok module for handling
from src.schema import ensure_table_schema
from src.enrich import (
    enrich_campaign_fields,
    enrich_ad_fields
)

# Get environment variable for Company
COMPANY = os.getenv("COMPANY") 

# Get environment variable for Google Cloud Project ID
PROJECT = os.getenv("PROJECT")

# Get environment variable for Platform
PLATFORM = os.getenv("PLATFORM")

# Get environmetn variable for Department
DEPARTMENT = os.getenv("DEPARTMENT")

# Get environment variable for Account
ACCOUNT = os.getenv("ACCOUNT")

# Get nvironment variable for Layer
LAYER = os.getenv("LAYER")

# Get environment variable for Mode
MODE = os.getenv("MODE")

# 1. TRANSFORM TIKTOK ADS INSIGHTS INTO CLEANED STAGING TABLES

# 1.1. Transform TikTok Ads campaign insights from raw tables into cleaned staging tables
def staging_campaign_insights() -> None:
    print("🚀 [STAGING] Starting to build staging TikTok Ads campaign insights table...")
    logging.info("🚀 [STAGING] Starting to build staging TikTok Ads campaign insights table...")
    
    try:

    # 1.1.1. Start timing the TikTok Ads campaign insights staging process
        staging_time_start = time.time()
        staging_section_succeeded = {}
        staging_section_failed = [] 
        staging_table_queried= []
        print(f"🔍 [STAGING] Proceeding to transform TikTok Ads campaign insights into cleaned staging table at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
        logging.info(f"🔍 [STAGING] Proceeding to transform TikTok Ads campaign insights into cleaned staging table at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    # 1.1.2. Prepare table_id for TikTok Ads campaign insights staging process
        raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
        print(f"🔍 [STAGING] Using raw TikTok Ads campaign insights table from Google BigQuery dataset {raw_dataset} to build staging table...")
        logging.info(f"🔍 [STAGING] Using raw TikTok Ads campaign insights table from Google BigQuery dataset {raw_dataset} to build staging table...")
        raw_campaign_metadata = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_metadata"
        print(f"🔍 [STAGING] Using raw TikTok Ads campaign metadata table from Google BigQuery table {raw_campaign_metadata} to build staging table...")
        logging.info(f"🔍 [STAGING] Using raw TikTok Ads campaign metadata table from Google BigQuery table {raw_campaign_metadata} to build staging table...")       
        staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
        staging_table_campaign = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_campaign_insights"
        print(f"🔍 [STAGING] Preparing to build staging table {staging_table_campaign} for TikTok Ads campaign insights...")
        logging.info(f"🔍 [STAGING] Preparing to build staging table {staging_table_campaign} for TikTok Ads campaign insights...")

    # 1.1.3. Initialize Google BigQuery client
        try:
            print(f"🔍 [STAGING] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [STAGING] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            google_bigquery_client = bigquery.Client(project=PROJECT)
            print(f"✅ [STAGING] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            logging.info(f"✅ [STAGING] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            staging_section_succeeded["1.1.3. Initialize Google BigQuery client"] = True
        except Exception as e:
            staging_section_succeeded["1.1.3. Initialize Google BigQuery client"] = False
            staging_section_failed.append("1.1.3s. Initialize Google BigQuery client")
            print(f"❌ [STAGING] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
            logging.error(f"❌ [STAGING] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
            raise RuntimeError(f"❌ [STAGING] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.") from e

    # 1.1.4. Scan all raw TikTok Ads campaign insights table(s)
        try:
            print(f"🔍 [STAGING] Scanning all raw TikTok Ads campaign insights table(s) from Google BigQuery dataset {raw_dataset}...")
            logging.info(f"🔍 [STAGING] Scanning all raw TikTok Ads campaign insights table(s) from Google BigQuery dataset {raw_dataset}...")
            query_campaign_raw = f"""
                SELECT table_name
                FROM `{PROJECT}.{raw_dataset}.INFORMATION_SCHEMA.TABLES`
                WHERE REGEXP_CONTAINS(
                    table_name,
                    r'^{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_m[0-1][0-9][0-9]{{4}}$'
                )
            """
            raw_tables_campaign = [
                row.table_name for row in google_bigquery_client.query(query_campaign_raw).result()
            ]
            raw_tables_campaign = [f"{PROJECT}.{raw_dataset}.{t}" for t in raw_tables_campaign]
            if not raw_tables_campaign:
                raise RuntimeError("❌ [STAGING] Failed to scan raw TikTok Ads campaign insights table(s) due to no tables found.")
            print(f"✅ [STAGING] Successfully found {len(raw_tables_campaign)} raw TikTok Ads campaign insights table(s).")
            logging.info(f"✅ [STAGING] Successfully found {len(raw_tables_campaign)} raw TikTok Ads campaign insights table(s).")
            staging_section_succeeded["1.1.4. Scan all raw TikTok Ads campaign insights table(s)"] = True
        except Exception as e:
            staging_section_succeeded["1.1.4. Scan all raw TikTok Ads campaign insights table(s)"] = False
            staging_section_failed.append("1.1.4. Scan all raw TikTok Ads campaign insights table(s)")
            print(f"❌ [STAGING] Failed to scan raw TikTok Ads campaign insights table(s) due to {e}.")
            logging.error(f"❌ [STAGING] Failed to scan raw TikTok Ads campaign insights table(s) due to {e}.")
            raise RuntimeError(f"❌ [STAGING] Failed to scan raw TikTok Ads campaign insights table(s) due to {e}.") from e

    # 1.1.5. Query all raw TikTok Ads campaign insights table(s)
        for raw_table_campaign in raw_tables_campaign:
            query_campaign_staging = f"""
                SELECT
                    raw.*,
                    metadata.campaign_name,
                    metadata.advertiser_name,
                    metadata.operation_status,
                    metadata.objective_type
                FROM `{raw_table_campaign}` AS raw
                LEFT JOIN `{raw_campaign_metadata}` AS metadata
                    ON CAST(raw.campaign_id AS STRING) = CAST(metadata.campaign_id AS STRING)
                    AND CAST(raw.advertiser_id  AS STRING) = CAST(metadata.advertiser_id AS STRING)
            """
            try:
                print(f"🔄 [STAGING] Querying raw TikTok Ads campaign insights table {raw_table_campaign}...")
                logging.info(f"🔄 [STAGING] Querying raw TikTok Ads campaign insights table {raw_table_campaign}...")
                staging_df_queried = google_bigquery_client.query(query_campaign_staging).to_dataframe()
                staging_table_queried.append(staging_df_queried)
                print(f"✅ [STAGING] Successfully queried {len(staging_df_queried)} row(s) of raw TikTok Ads campaign insights from {raw_table_campaign}.")
                logging.info(f"✅ [STAGING] Successfully queried {len(staging_df_queried)} row(s) of raw TikTok Ads campaign insights from {raw_table_campaign}.")
            except Exception as e:
                print(f"❌ [STAGING] Failed to query raw TikTok Ads campaign insights table {raw_table_campaign} due to {e}.")
                logging.warning(f"❌ [STAGING] Failed to query raw TikTok Ads campaign insights table {raw_table_campaign} due to {e}.")
                continue
        if not staging_table_queried:
            staging_section_succeeded["1.1.5. Query all raw TikTok Ads campaign insights table(s)"] = False
            staging_section_failed.append("1.1.5. Query all raw TikTok Ads campaign insights table(s)")
            raise RuntimeError("❌ [STAGING] Failed to query TiKTok Ads campaign insights due to no raw table found.")
        staging_df_concatenated = pd.concat(staging_table_queried, ignore_index=True)
        print(f"✅ [STAGING] Successfully concatenated total {len(staging_df_concatenated)} row(s) of TikTok Ads campaign insights from {len(staging_table_queried)} table(s).")
        logging.info(f"✅ [STAGING] Successfully concatenated total {len(staging_df_concatenated)} row(s) of TikTok Ads campaign insights from {len(staging_table_queried)} table(s).")
        staging_section_succeeded["1.1.5. Query all raw TikTok Ads campaign insights table(s)"] = True

    # 1.1.6. Enrich TikTok Ads campaign insights
        try:
            print(f"🔄 [STAGING] Triggering to enrich staging TikTok Ads campaign insights field(s) for {len(staging_df_concatenated)} row(s)...")
            logging.info(f"🔄 [STAGING] Triggering to enrich staging TikTok Ads campaign insights field(s) for {len(staging_df_concatenated)} row(s)...")
            staging_df_enriched = enrich_campaign_fields(staging_df_concatenated, table_id=raw_table_campaign)
            if "nhan_su" in staging_df_enriched.columns:
                vietnamese_map = {
                    'á': 'a', 'à': 'a', 'ả': 'a', 'ã': 'a', 'ạ': 'a',
                    'ă': 'a', 'ắ': 'a', 'ằ': 'a', 'ẳ': 'a', 'ẵ': 'a', 'ặ': 'a',
                    'â': 'a', 'ấ': 'a', 'ầ': 'a', 'ẩ': 'a', 'ẫ': 'a', 'ậ': 'a',
                    'đ': 'd',
                    'é': 'e', 'è': 'e', 'ẻ': 'e', 'ẽ': 'e', 'ẹ': 'e',
                    'ê': 'e', 'ế': 'e', 'ề': 'e', 'ể': 'e', 'ễ': 'e', 'ệ': 'e',
                    'í': 'i', 'ì': 'i', 'ỉ': 'i', 'ĩ': 'i', 'ị': 'i',
                    'ó': 'o', 'ò': 'o', 'ỏ': 'o', 'õ': 'o', 'ọ': 'o',
                    'ô': 'o', 'ố': 'o', 'ồ': 'o', 'ổ': 'o', 'ỗ': 'o', 'ộ': 'o',
                    'ơ': 'o', 'ớ': 'o', 'ờ': 'o', 'ở': 'o', 'ỡ': 'o', 'ợ': 'o',
                    'ú': 'u', 'ù': 'u', 'ủ': 'u', 'ũ': 'u', 'ụ': 'u',
                    'ư': 'u', 'ứ': 'u', 'ừ': 'u', 'ử': 'u', 'ữ': 'u', 'ự': 'u',
                    'ý': 'y', 'ỳ': 'y', 'ỷ': 'y', 'ỹ': 'y', 'ỵ': 'y',
                }
                vietnamese_map_upper = {k.upper(): v.upper() for k, v in vietnamese_map.items()}
                full_map = {**vietnamese_map, **vietnamese_map_upper}
                staging_df_enriched["nhan_su"] = staging_df_enriched["nhan_su"].apply(
                    lambda x: ''.join(full_map.get(c, c) for c in x) if isinstance(x, str) else x
                )
            staging_df_renamed = staging_df_enriched.rename(columns={
                "advertiser_id": "account_id",
                "objective": "result_type",
                "advertiser_name": "account_name",
                "operation_status": "delivery_status"
            })
            print(f"✅ [STAGING] Successfully enriched {len(staging_df_renamed)} row(s) from all TikTok Ads raw campaign insights table(s).")
            logging.info(f"✅ [STAGING] Successfully combined {len(staging_df_renamed)} row(s) from all TikTok Ads raw campaign insights table(s).")
            staging_section_succeeded["1.1.6. Enrich TikTok Ads campaign insights"] = True
        except Exception as e:
            staging_section_succeeded["1.1.6. Enrich TikTok Ads campaign insights"] = False
            staging_section_failed.append("1.1.6. Enrich TikTok Ads campaign insights")
            print(f"❌ [STAGING] Failed to trigger enrichment for staging TikTok Ads campaign insights due to {e}.")
            logging.warning(f"❌ [STAGING] Failed to trigger enrichment for staging TikTok Ads campaign insights due to {e}.")
            raise RuntimeError(f"❌ [STAGING] Failed to trigger enrichment for staging TikTok Ads campaign insights due to {e}.") from e

    # 1.1.7. Enforce schema for TikTok Ads campaign insights
        try:
            print(f"🔄 [STAGING] Triggering to enforce schema for {len(staging_df_renamed)} row(s) of staging TikTok Ads campaign insights...")
            logging.info(f"🔄 [STAGING] Triggering to enforce schema for {len(staging_df_renamed)} row(s) of staging TikTok Ads campaign insights...")
            staging_df_enforced = ensure_table_schema(staging_df_renamed, "staging_campaign_insights")
        except Exception as e:
            print(f"❌ [STAGING] Failed to trigger schema enforcement for {len(staging_df_enforced)} row(s) of staging TikTok Ads campaign insights due to {e}.")
            logging.error(f"❌ [STAGING] Failed to trigger schema enforcement for {len(staging_df_enforced)} row(s) of staging TikTok Ads campaign insights due to {e}.")
            raise   

    # 1.1.8. Create new table if not exist     
        staging_df_deduplicated = staging_df_enforced.drop_duplicates()
        table_clusters_filtered = []
        table_schemas_defined = []
        try:
            print(f"🔍 [STAGING] Checking staging TikTok Ads campaign insights table {staging_table_campaign} existence...")
            logging.info(f"🔍 [STAGING] Checking staging TikTok Ads campaign insights table {staging_table_campaign} existence...")
            google_bigquery_client.get_table(staging_table_campaign)
            staging_table_exists = True
        except Exception:
            staging_table_exists = False
        if not staging_table_exists:
            print(f"⚠️ [STAGING] Staging TikTok Ads campaign insights table {staging_table_campaign} not found then new table creation will be proceeding...")
            logging.warning(f"⚠️ [STAGING] Staging TikTok Ads campaign insights table {staging_table_campaign} not found then new table creation will be proceeding...")
            for col, dtype in staging_df_deduplicated.dtypes.items():
                if dtype.name.startswith("int"):
                    google_bigquery_type = "INT64"
                elif dtype.name.startswith("float"):
                    google_bigquery_type = "FLOAT64"
                elif dtype.name == "bool":
                    google_bigquery_type = "BOOL"
                elif "datetime" in dtype.name:
                    google_bigquery_type = "TIMESTAMP"
                else:
                    google_bigquery_type = "STRING"
                table_schemas_defined.append(bigquery.SchemaField(col, google_bigquery_type))
            table_configuration_defined = bigquery.Table(staging_table_campaign, schema=table_schemas_defined)
            table_partition_effective = "date" if "date" in staging_df_deduplicated.columns else None
            if table_partition_effective:
                table_configuration_defined.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=table_partition_effective
                )
            table_clusters_defined = ["chuong_trinh", "ma_ngan_sach_cap_1", "nhan_su"]
            table_clusters_filtered = [f for f in table_clusters_defined if f in staging_df_deduplicated.columns]
            if table_clusters_filtered:  
                table_configuration_defined.clustering_fields = table_clusters_filtered  
            try:
                print(f"🔍 [STAGING] Creating staging TikTok Ads campaign insights table with defined name {staging_table_campaign} and partition on {table_partition_effective}...")
                logging.info(f"🔍 [STAGING] Creating staging TikTok Ads campaign insights table with defined name {staging_table_campaign} and partition on {table_partition_effective}...")
                table_metadata_defined = google_bigquery_client.create_table(table_configuration_defined)
                print(f"✅ [STAGING] Successfully created staging TikTok Ads campaign insights table with actual name {table_metadata_defined.full_table_id}.")
                logging.info(f"✅ [STAGING] Successfully created staging TikTok Ads campaign insights table with actual name {table_metadata_defined.full_table_id}.")
                staging_section_succeeded["1.1.8. Create new table if not exist"] = True
            except Exception as e:
                staging_section_succeeded["1.1.8. Create new table if not exist"] = False
                staging_section_failed.append("1.1.8. Create new table if not exist")
                print(f"❌ [STAGING] Failed to create staging TikTok Ads campaign insights table {staging_table_campaign} due to {e}.")
                logging.error(f"❌ [STAGING] Failed to create staging TikTok Ads campaign insights table {staging_table_campaign} due to {e}.")
                raise RuntimeError(f"❌ [STAGING] Failed to create staging TikTok Ads campaign insights table {staging_table_campaign} due to {e}.") from e
            
    # 1.1.9. Upload staging TikTok Ads campaign insights to Google BigQuery            
        if not staging_table_exists:
            try: 
                print(f"🔍 [STAGING] Uploading {len(staging_df_enforced)} row(s) of staging TikTok Ads campaign insights to new Google BigQuery table {table_metadata_defined.full_table_id}...")
                logging.warning(f"🔍 [STAGING] Uploading {len(staging_df_enforced)} row(s) of staging TikTok Ads campaign insights to Google BigQuery table {table_metadata_defined.full_table_id}...")     
                job_config = bigquery.LoadJobConfig(
                    write_disposition="WRITE_APPEND",
                    time_partitioning=bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field="date"
                    ),
                    clustering_fields=table_clusters_filtered if table_clusters_filtered else None
                )
                load_job = google_bigquery_client.load_table_from_dataframe(
                    staging_df_enforced,
                    staging_table_campaign, 
                    job_config=job_config
                )
                load_job.result()
                staging_df_uploaded = staging_df_enforced.copy()
                print(f"✅ [STAGING] Successfully uploaded {len(staging_df_uploaded)} row(s) of staging TikTok Ads campaign insights to new Google BigQuery table {table_metadata_defined.full_table_id}.")
                logging.info(f"✅ [STAGING] Successfully uploaded {len(staging_df_uploaded)} row(s) of staging TikTok Ads campaign insights to new Google BigQuery table {table_metadata_defined.full_table_id}.")
                staging_section_succeeded["1.1.9. Upload staging TikTok Ads campaign insights to Google BigQuery"] = True
            except Exception as e:
                staging_section_succeeded["1.1.9. Upload staging TikTok Ads campaign insights to Google BigQuery"] = False
                staging_section_failed.append("1.1.9. Upload staging TikTok Ads campaign insights to Google BigQuery")
                print(f"❌ [STAGING] Failed to upload {len(staging_df_enforced)} row(s) of staging TikTok Ads campaign insights to Google BigQuery table {table_metadata_defined.full_table_id} due to {e}.")
                logging.error(f"❌ [STAGING] Failed to upload {len(staging_df_enforced)} row(s) of staging TikTok Ads campaign insights to Google BigQuery table {table_metadata_defined.full_table_id} due to {e}.")      
                raise RuntimeError(f"❌ [STAGING] Failed to upload {len(staging_df_enforced)} row(s) of staging TikTok Ads campaign insights to Google BigQuery table {table_metadata_defined.full_table_id} due to {e}.") from e  
        else:
            try:
                print(f"🔍 [STAGING] Overwriting {len(staging_df_enforced)} row(s) of staging TikTok Ads campaign insights to existing Google BigQuery table {staging_table_campaign}...")
                logging.warning(f"🔍 [STAGING] Overwriting {len(staging_df_enforced)} row(s) of staging TikTok Ads campaign insights to existing Google BigQuery table {staging_table_campaign}...")                    
                job_config = bigquery.LoadJobConfig(
                    write_disposition="WRITE_TRUNCATE",
                )
                load_job = google_bigquery_client.load_table_from_dataframe(
                    staging_df_enforced,
                    staging_table_campaign, 
                    job_config=job_config
                )
                load_job.result()
                staging_df_uploaded = staging_df_enforced.copy()
                print(f"✅ [STAGING] Successfully overwrote {len(staging_df_uploaded)} row(s) of staging TikTok Ads campaign insights to existing Google BigQuery table {staging_table_campaign}.")
                logging.info(f"✅ [STAGING] Successfully overwrote {len(staging_df_uploaded)} row(s) of staging TikTok Ads campaign insights to existing Google BigQuery table {staging_table_campaign}.")
                staging_section_succeeded["1.1.9. Upload staging TikTok Ads campaign insights to Google BigQuery"] = True
            except Exception as e:
                staging_section_succeeded["1.1.9. Upload staging TikTok Ads campaign insights to Google BigQuery"] = False
                staging_section_failed.append("1.1.9. Upload staging TikTok Ads campaign insights to Google BigQuery")
                print(f"❌ [STAGING] Failed to overwrite {len(staging_df_enforced)} row(s) of staging TikTok Ads campaign insights to existing Google BigQuery table {staging_table_campaign} due to {e}.")
                logging.error(f"❌ [STAGING] Failed to overwrite {len(staging_df_enforced)} row(s) of staging TikTok Ads campaign insights to existing Google BigQuery table {staging_table_campaign} due to {e}.")      
                raise RuntimeError(f"❌ [STAGING] Failed to overwrite {len(staging_df_enforced)} row(s) of staging TikTok Ads campaign insights to existing Google BigQuery table {staging_table_campaign} due to {e}.") from e             

    # 1.1.10. Summarize staging result(s)
    finally:
        staging_time_elapsed = round(time.time() - staging_time_start, 2)
        staging_df_final = staging_df_uploaded.copy() if not staging_df_uploaded.empty else pd.DataFrame()
        staging_tables_input = len(raw_tables_campaign)
        staging_tables_succeeded = len(staging_table_queried)
        staging_tables_failed = staging_tables_input - staging_tables_succeeded
        staging_rows_uploaded = len(staging_df_final)
        if staging_section_failed:
            print(f"❌ [STAGING] Failed to complete TikTok Ads campaign insights staging process with all {staging_tables_failed} table(s) failed and 0 row(s) queried in {staging_time_elapsed}s due to section {', '.join(staging_section_failed)}.")
            logging.error(f"❌ [STAGING] Failed to complete TikTok Ads campaign insights staging process with all {staging_tables_failed} table(s) failed and 0 row(s) queried in {staging_time_elapsed}s due to section {', '.join(staging_section_failed)}.")
            staging_status_final = "staging_failed_all"
        elif staging_tables_failed > 0:
            print(f"⚠️ [STAGING] Completed TikTok Ads campaign insights staging process with partial failure of {staging_tables_failed}/{staging_tables_input} table(s) and {staging_rows_uploaded} row(s) queried in {staging_time_elapsed}s.")
            logging.warning(f"⚠️ [STAGING] Completed TikTok Ads campaign insights staging process with partial failure of {staging_tables_failed}/{staging_tables_input} table(s) and {staging_rows_uploaded} row(s) queried in {staging_time_elapsed}s.")
            staging_status_final = "staging_failed_partial"
        else:
            print(f"🏆 [STAGING] Successfully completed TikTok Ads campaign insights staging process for {staging_tables_succeeded} table(s) with {staging_rows_uploaded} row(s) queried in {staging_time_elapsed}s.")
            logging.info(f"🏆 [STAGING] Successfully completed TikTok Ads campaign insights staging process for {staging_tables_succeeded} table(s) with {staging_rows_uploaded} row(s) queried in {staging_time_elapsed}s.")
            staging_status_final = "staging_success_all"
        return {
            "staging_df_final": staging_df_final,
            "staging_status_final": staging_status_final,
            "staging_summary_final": {
                "staging_time_elapsed": staging_time_elapsed,
                "staging_rows_uploaded": staging_rows_uploaded,
                "staging_tables_input": staging_tables_input,
                "staging_tables_succeeded": staging_tables_succeeded,
                "staging_tables_failed": staging_tables_failed,
                "staging_section_failed": staging_section_failed,
            }
        }

# 1.2. Transform TikTok Ads ad insights from raw tables into cleaned staging tables
def staging_ad_insights() -> None:
    print("🚀 [STAGING] Starting to build staging TikTok Ads ad insights table...")
    logging.info("🚀 [STAGING] Starting to build staging TikTok Ads ad insights table...")

    # 1.2.1. Start timing the TikTok Ads ad insights staging process
    staging_time_start = time.time()
    staging_section_succeeded = {}
    staging_section_failed = [] 
    staging_table_queried= []
    print(f"🔍 [STAGING] Proceeding to transform TikTok Ads ad insights into cleaned staging table at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [STAGING] Proceeding to transform TikTok Ads ad insights into cleaned staging table at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    # 1.2.2. Prepare table_id for TikTok Ads ad insighs staging process
    raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
    print(f"🔍 [STAGING] Using raw TikTok Ads ad insights table from Google BigQuery dataset {raw_dataset} to build staging table...")
    logging.info(f"🔍 [STAGING] Using raw TikTok Ads ad insights table from Google BigQuery dataset {raw_dataset} to build staging table...")
    raw_ad_metadata = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_ad_metadata"
    print(f"🔍 [STAGING] Using raw TikTok Ads ad metadata table from Google BigQuery table {raw_ad_metadata} to build staging table...")
    logging.info(f"🔍 [STAGING] Using raw TikTok Ads ad metadata table from Google BigQuery table {raw_ad_metadata} to build staging table...")        
    raw_ad_creative = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_ad_creative"
    print(f"🔍 [STAGING] Using raw TikTok Ads ad creative table from Google BigQuery table {raw_ad_creative} to build staging table...")
    logging.info(f"🔍 [STAGING] Using raw TikTok Ads ad creative table from Google BigQuery table {raw_ad_creative} to build staging table...")   
    staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
    staging_table_ad = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_ad_insights"
    print(f"🔍 [STAGING] Preparing to build staging table {staging_table_ad} for TikTok Ads ad insights...")
    logging.info(f"🔍 [STAGING] Preparing to build staging table {staging_table_ad} for TikTok Ads ad insights...")

    # 1.2.3. Initialize Google BigQuery client
    try:
        print(f"🔍 [STAGING] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
        logging.info(f"🔍 [STAGING] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
        google_bigquery_client = bigquery.Client(project=PROJECT)
        print(f"✅ [STAGING] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
        logging.info(f"✅ [STAGING] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
        staging_section_succeeded["1.2.3. Initialize Google BigQuery client"] = True
    except Exception as e:
        staging_section_succeeded["1.2.3. Initialize Google BigQuery client"] = False
        staging_section_failed.append("1.2.3. Initialize Google BigQuery client")
        print(f"❌ [STAGING] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        logging.error(f"❌ [STAGING] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        raise RuntimeError(f"❌ [STAGING] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.") from e

    # 1.2.4. Scan all raw TikTok Ads ad insights table(s)
    try:
        print(f"🔍 [STAGING] Scanning all raw TikTok Ads ad insights table(s) from Google BigQuery dataset {raw_dataset}...")
        logging.info(f"🔍 [STAGING] Scanning all raw TikTok Ads ad insights table(s) from Google BigQuery dataset {raw_dataset}...")
        query_ad_raw = f"""
            SELECT table_name
            FROM `{PROJECT}.{raw_dataset}.INFORMATION_SCHEMA.TABLES`
            WHERE REGEXP_CONTAINS(
                table_name,
                r'^{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_ad_m[0-1][0-9][0-9]{{4}}$'
            )
        """
        raw_tables_ad = [row.table_name for row in google_bigquery_client.query(query_ad_raw).result()]
        raw_tables_ad = [f"{PROJECT}.{raw_dataset}.{t}" for t in raw_tables_ad]
        if not raw_tables_ad:
            raise RuntimeError("❌ [STAGING] Failed to scan raw TikTok Ads ad insights table(s) due to no tables found.")
        print(f"✅ [STAGING] Successfully found {len(raw_tables_ad)} raw TikTok Ads ad insights table(s).")
        logging.info(f"✅ [STAGING] Successfully found {len(raw_tables_ad)} raw TikTok Ads ad insights table(s).")
        staging_section_succeeded["1.2.4. Scan all raw TikTok Ads ad insights table(s)"] = True
    except Exception as e:
        staging_section_succeeded["1.2.4. Scan all raw TikTok Ads ad insights table(s)"] = False
        staging_section_failed.append("1.2.4. Scan all raw TikTok Ads ad insights table(s)")
        print(f"❌ [STAGING] Failed to scan raw TikTok Ads ad insights table(s) due to {e}.")
        logging.error(f"❌ [STAGING] Failed to scan raw TikTok Ads ad insights table(s) due to {e}.")
        raise RuntimeError(f"❌ [STAGING] Failed to scan raw TikTok Ads ad insights table(s) due to {e}.") from e

    # 1.2.5. Query all raw TikTok Ads ad insights table(s)
    for raw_ad_table in raw_tables_ad:
        query_ad_staging = f"""
            SELECT
                raw.*,
                ad.advertiser_id,
                ad.ad_id,
                ad.ad_name,
                ad.adgroup_id,
                ad.adgroup_name,
                ad.campaign_id,
                ad.campaign_name,
                ad.operation_status,
                ad.ad_format,
                ad.optimization_event,
                ad.video_id,
                creative.video_cover_url,
                creative.preview_url,
                creative.create_time AS creative_create_time
            FROM `{raw_ad_table}` AS raw
            LEFT JOIN `{raw_ad_metadata}` AS ad
                ON CAST(raw.ad_id AS STRING) = CAST(ad.ad_id AS STRING)
                AND CAST(raw.advertiser_id AS STRING) = CAST(ad.advertiser_id AS STRING)
            LEFT JOIN `{raw_ad_creative}` AS creative
                ON CAST(ad.video_id AS STRING) = CAST(creative.video_id AS STRING)
                AND CAST(ad.advertiser_id AS STRING) = CAST(creative.advertiser_id AS STRING)
        """
        try:
            print(f"🔄 [STAGING] Querying raw TikTok Ads ad insights table {raw_ad_table}...")
            logging.info(f"🔄 [STAGING] Querying raw TikTok Ads ad insights table {raw_ad_table}...")
            staging_df_queried = google_bigquery_client.query(query_ad_staging).to_dataframe()
            staging_table_queried.append(staging_df_queried)
            print(f"✅ [STAGING] Successfully queried {len(staging_df_queried)} row(s) of TikTok Ads ad insights from {raw_ad_table}.")
            logging.info(f"✅ [STAGING] Successfully queried {len(staging_df_queried)} row(s) of TikTok Ads ad insights from {raw_ad_table}.")
        except Exception as e:
            print(f"❌ [STAGING] Failed to query raw TikTok Ads ad insights table {raw_ad_table} due to {e}.")
            logging.warning(f"❌ [STAGING] Failed to query raw TikTok Ads ad insights table {raw_ad_table} due to {e}.")
            continue
    if not staging_table_queried:
        staging_section_succeeded["1.2.5. Query all raw TikTok Ads ad insights table(s)"] = False
        staging_section_failed.append("1.2.5. Query all raw TikTok Ads ad insights table(s)")
        raise RuntimeError("❌ [STAGING] Failed to query TiKTok Ads ad insights due to no raw table found.")
    staging_df_concatenated = pd.concat(staging_table_queried, ignore_index=True)
    print(f"✅ [STAGING] Successfully concatenated total {len(staging_df_concatenated)} row(s) of TikTok Ads ad insights from {len(staging_table_queried)} table(s).")
    logging.info(f"✅ [STAGING] Successfully concatenated total {len(staging_df_concatenated)} row(s) row(s) of TikTok Ads ad insights from {len(staging_table_queried)} table(s).")
    staging_section_succeeded["1.2.5. Query all raw TikTok Ads ad insights table(s)"] = True

    # 1.2.6. Enrich TikTok Ads ad insights
    try:
        print(f"🔄 [STAGING] Triggering to enrich staging TikTok Ads ad insights field(s) for {len(staging_df_concatenated)} row(s)...")
        logging.info(f"🔄 [STAGING] Triggering to enrich staging TikTok Ads ad insights field(s) for {len(staging_df_concatenated)} row(s)...")
        staging_df_enriched = enrich_ad_fields(staging_df_concatenated, table_id=raw_ad_table)
        if "nhan_su" in staging_df_enriched.columns:
            vietnamese_map = {
                'á': 'a', 'à': 'a', 'ả': 'a', 'ã': 'a', 'ạ': 'a',
                'ă': 'a', 'ắ': 'a', 'ằ': 'a', 'ẳ': 'a', 'ẵ': 'a', 'ặ': 'a',
                'â': 'a', 'ấ': 'a', 'ầ': 'a', 'ẩ': 'a', 'ẫ': 'a', 'ậ': 'a',
                'đ': 'd',
                'é': 'e', 'è': 'e', 'ẻ': 'e', 'ẽ': 'e', 'ẹ': 'e',
                'ê': 'e', 'ế': 'e', 'ề': 'e', 'ể': 'e', 'ễ': 'e', 'ệ': 'e',
                'í': 'i', 'ì': 'i', 'ỉ': 'i', 'ĩ': 'i', 'ị': 'i',
                'ó': 'o', 'ò': 'o', 'ỏ': 'o', 'õ': 'o', 'ọ': 'o',
                'ô': 'o', 'ố': 'o', 'ồ': 'o', 'ổ': 'o', 'ỗ': 'o', 'ộ': 'o',
                'ơ': 'o', 'ớ': 'o', 'ờ': 'o', 'ở': 'o', 'ỡ': 'o', 'ợ': 'o',
                'ú': 'u', 'ù': 'u', 'ủ': 'u', 'ũ': 'u', 'ụ': 'u',
                'ư': 'u', 'ứ': 'u', 'ừ': 'u', 'ử': 'u', 'ữ': 'u', 'ự': 'u',
                'ý': 'y', 'ỳ': 'y', 'ỷ': 'y', 'ỹ': 'y', 'ỵ': 'y',
            }
            vietnamese_map_upper = {k.upper(): v.upper() for k, v in vietnamese_map.items()}
            full_map = {**vietnamese_map, **vietnamese_map_upper}
            staging_df_enriched["nhan_su"] = staging_df_enriched["nhan_su"].apply(
                lambda text: ''.join(full_map.get(c, c) for c in text) if isinstance(text, str) else text
            )
        staging_df_renamed = staging_df_queried.rename(columns={
            "advertiser_id": "account_id",
            "adgroup_id": "adset_id",
            "adgroup_name": "adset_name",
            "operation_status": "delivery_status"
        })
        print(f"✅ [STAGING] Successfully enriched {len(staging_df_renamed)} row(s) from all TikTok Ads raw ad insights table(s).")
        logging.info(f"✅ [STAGING] Successfully combined {len(staging_df_renamed)} row(s) from all TikTok Ads raw ad insights table(s).")
        staging_section_succeeded["1.2.6. Enrich TikTok Ads ad insights"] = True
    except Exception as e:
        staging_section_succeeded["1.2.6. Enrich TikTok Ads ad insights"] = False
        staging_section_failed.append("1.2.6. Enrich TikTok Ads ad insights")
        print(f"❌ [STAGING] Failed to trigger enrichment for staging TikTok Ads ad insights due to {e}.")
        logging.warning(f"❌ [STAGING] Failed to trigger enrichment for staging TikTok Ads ad insights due to {e}.")
        raise RuntimeError(f"❌ [STAGING] Failed to trigger enrichment for staging TikTok Ads ad insights due to {e}.") from e

    # 1.2.7. Enforce schema for TikTok Ads ad insights
    try:
        print(f"🔄 [STAGING] Triggering to enforce schema for {len(staging_df_concatenated)} row(s) of staging TikTok Ads ad insights...")
        logging.info(f"🔄 [STAGING] Triggering to enforce schema for {len(staging_df_concatenated)} row(s) of staging TikTok Ads ad insights...")
        staging_df_enforced = ensure_table_schema(staging_df_concatenated, "staging_ad_insights")
    except Exception as e:
        print(f"❌ [STAGING] Failed to trigger schema enforcement for {len(staging_df_enforced)} row(s) of staging TikTok Ads ad insights due to {e}.")
        logging.error(f"❌ [STAGING] Failed to trigger schema enforcement for {len(staging_df_enforced)} row(s) of staging TikTok Ads ad insights due to {e}.")
        raise RuntimeError(f"❌ [STAGING] Failed to trigger schema enforcement for {len(staging_df_enforced)} row(s) of staging TikTok Ads ad insights due to {e}.") from e

    # 1.2.8. Create new table if not exist     
    staging_df_deduplicated = staging_df_enforced.drop_duplicates()
    try:
        print(f"🔍 [STAGING] Checking staging TikTok Ads ad insights table {staging_table_ad} existence...")
        logging.info(f"🔍 [STAGING] Checking staging TikTok Ads ad insights table {staging_table_ad} existence...")
        google_bigquery_client.get_table(staging_table_ad)
        staging_table_exists = True
    except Exception:
        staging_table_exists = False
    if not staging_table_exists:
        print(f"⚠️ [STAGING] Staging TikTok Ads ad insights table {staging_table_ad} not found then new table creation will be proceeding...")
        logging.warning(f"⚠️ [STAGING] Staging TikTok Ads ad insights table {staging_table_ad} not found then new table creation will be proceeding...")
        table_schemas_defined = []
        for col, dtype in staging_df_deduplicated.dtypes.items():
            if dtype.name.startswith("int"):
                google_bigquery_type = "INT64"
            elif dtype.name.startswith("float"):
                google_bigquery_type = "FLOAT64"
            elif dtype.name == "bool":
                google_bigquery_type = "BOOL"
            elif "datetime" in dtype.name:
                google_bigquery_type = "TIMESTAMP"
            else:
                google_bigquery_type = "STRING"
            table_schemas_defined.append(bigquery.SchemaField(col, google_bigquery_type))
        table_configuration_defined = bigquery.Table(staging_table_ad, schema=table_schemas_defined)
        table_partition_effective = "date" if "date" in staging_df_deduplicated.columns else None
        if table_partition_effective:
            table_configuration_defined.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=table_partition_effective
            )
        table_clusters_defined = ["chuong_trinh", "ma_ngan_sach_cap_1", "nhan_su"]
        table_clusters_filtered = []
        table_clusters_filtered = [f for f in table_clusters_defined if f in staging_df_deduplicated.columns]
        if table_clusters_filtered:  
            table_configuration_defined.clustering_fields = table_clusters_filtered  
        try:
            print(f"🔍 [STAGING] Creating staging TikTok Ads ad insights table with defined name {staging_table_ad} and partition on {table_partition_effective}...")
            logging.info(f"🔍 [STAGING] Creating staging TikTok Ads ad insights table with defined name {staging_table_ad} and partition on {table_partition_effective}...")
            table_metadata_defined = google_bigquery_client.create_table(table_configuration_defined)
            print(f"✅ [STAGING] Successfully created staging TikTok Ads ad insights table with actual name {table_metadata_defined.full_table_id}.")
            logging.info(f"✅ [STAGING] Successfully created staging TikTok Ads ad insights table with actual name {table_metadata_defined.full_table_id}.")
            staging_section_succeeded["1.2.8. Create new table if not exist"] = True
        except Exception as e:
            staging_section_succeeded["1.2.8. Create new table if not exist"] = False
            staging_section_failed.append("1.2.8. Create new table if not exist")
            print(f"❌ [STAGING] Failed to create staging TikTok Ads ad insights table {staging_table_ad} due to {e}.")
            logging.error(f"❌ [STAGING] Failed to create staging TikTok Ads ad insights table {staging_table_ad} due to {e}.")
            raise RuntimeError(f"❌ [STAGING] Failed to create staging TikTok Ads ad insights table {staging_table_ad} due to {e}.") from e
        
    # 1.2.9. Upload staging TikTok Ads ad insights to Google BigQuery            
    try:
        print(f"🔍 [STAGING] Uploading {len(staging_df_enforced)} row(s) of staging TikTok Ads ad insights to Google BigQuery table {staging_table_ad}...")
        logging.warning(f"🔍 [STAGING] Uploading {len(staging_df_enforced)} row(s) of staging TikTok Ads ad insights to Google BigQuery table {staging_table_ad}...")                    
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="date"
            ),
            clustering_fields=table_clusters_filtered if table_clusters_filtered else None
        )
        load_job = google_bigquery_client.load_table_from_dataframe(
            staging_df_enforced,
            staging_table_ad,
            job_config=job_config
        )
        load_job.result()
        staging_df_uploaded = staging_df_enforced.copy()
        print(f"✅ [STAGING] Successfully uploaded {len(staging_df_uploaded)} row(s) to staging TikTok Ads ad insights table {staging_table_ad}.")
        logging.info(f"✅ [STAGING] Successfully uploaded {len(staging_df_uploaded)} row(s) to staging TikTok Ads ad insights table {staging_table_ad}.")
        staging_section_succeeded["1.2.9. Upload staging TikTok Ads ad insights to Google BigQuery       "] = True
    except Exception as e:
        staging_section_succeeded["1.2.9. Upload staging TikTok Ads ad insights to Google BigQuery       "] = False
        staging_section_failed.append("1.2.9. Upload staging TikTok Ads ad insights to Google BigQuery       ")
        print(f"❌ [STAGING] Failed to upload staging TikTok Ads ad insights to Google BigQuery table {staging_table_ad} due to {e}.")
        logging.error(f"❌ [STAGING] Failed to upload staging TikTok Ads ad insights to Google BigQuery table {staging_table_ad} due to {e}.")      
        raise RuntimeError(f"❌ [STAGING] Failed to upload staging TikTok Ads ad insights to Google BigQuery table {staging_table_ad} due to {e}.") from e    

    # 1.2.10. Summarize staging result(s)
    finally:
        staging_time_elapsed = round(time.time() - staging_time_start, 2)
        staging_df_final = staging_df_uploaded.copy() if not staging_df_uploaded.empty else pd.DataFrame()
        staging_tables_input = len(raw_tables_ad)
        staging_tables_succeeded = len(staging_table_queried)
        staging_tables_failed = staging_tables_input - staging_tables_succeeded
        staging_rows_uploaded = len(staging_df_final)
        if staging_section_failed:
            print(f"❌ [STAGING] Failed to complete TikTok Ads ad insights staging process with all {staging_tables_failed} table(s) failed and 0 row(s) queried in {staging_time_elapsed}s due to section {', '.join(staging_section_failed)}.")
            logging.error(f"❌ [STAGING] Failed to complete TikTok Ads ad insights staging process with all {staging_tables_failed} table(s) failed and 0 row(s) queried in {staging_time_elapsed}s due to section {', '.join(staging_section_failed)}.")
            staging_status_final = "staging_failed_all"
        elif staging_tables_failed > 0:
            print(f"⚠️ [STAGING] Completed TikTok Ads ad insights staging process with partial failure of {staging_tables_failed}/{staging_tables_input} table(s) and {staging_rows_uploaded} row(s) queried in {staging_time_elapsed}s.")
            logging.warning(f"⚠️ [STAGING] Completed TikTok Ads ad insights staging process with partial failure of {staging_tables_failed}/{staging_tables_input} table(s) and {staging_rows_uploaded} row(s) queried in {staging_time_elapsed}s.")
            staging_status_final = "staging_failed_partial"
        else:
            print(f"🏆 [STAGING] Successfully completed TikTok Ads ad insights staging process for {staging_tables_succeeded} table(s) with {staging_rows_uploaded} row(s) queried in {staging_time_elapsed}s.")
            logging.info(f"🏆 [STAGING] Successfully completed TikTok Ads ad insights staging process for {staging_tables_succeeded} table(s) with {staging_rows_uploaded} row(s) queried in {staging_time_elapsed}s.")
            staging_status_final = "staging_success_all"
        return {
            "staging_df_final": staging_df_final,
            "staging_status_final": staging_status_final,
            "staging_summary_final": {
                "staging_time_elapsed": staging_time_elapsed,
                "staging_rows_uploaded": staging_rows_uploaded,
                "staging_tables_input": staging_tables_input,
                "staging_tables_succeeded": staging_tables_succeeded,
                "staging_tables_failed": staging_tables_failed,
                "staging_section_failed": staging_section_failed,
            }
        }