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

✔️ Joins raw ad insights with creative & campaign metadata  
✔️ Enriches fields like owner, program code, placement, format...  
✔️ Normalizes and writes standardized tables into staging dataset  

⚠️ This module is strictly responsible for *data transformation*  
into staging format. It does **not** handle API ingestion or final  
MART aggregations.
==================================================================
"""
# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add logging ultilities for integration
import logging

# Add Python Pandas libraries for integration
import pandas as pd

# Add Google API Core modules for integration
from google.api_core.exceptions import (
    Forbidden,
    GoogleAPICallError,
)

# Add Google Authentication libraries for integration
from google.auth.exceptions import DefaultCredentialsError

# Add Google Cloud modules for integration
from google.cloud import bigquery

# Add internal TikTok module for handling
from config.utils import remove_string_accents
from config.schema import ensure_table_schema
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

# 1. TRANSFORM TIKTOK ADS RAW DATA INTO CLEANED STAGING TABLES

# 1.1. Transform TikTok Ads campaign insights from raw tables into cleaned staging tables
def staging_campaign_insights() -> None:
    print("🚀 [STAGING] Starting to build staging TikTok Ads ad insights table...")
    logging.info("🚀 [STAGING] Starting to build staging TikTok Ads ad insights table...")

    # 1.1.1. Prepare table_id for TikTok Ads campaign insights
    try: 
        raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
        print(f"🔍 [STAGING] Using raw TikTok Ads campaign insights table from Google BigQuery dataset {raw_dataset} to build staging table...")
        logging.info(f"🔍 [STAGING] Using raw TikTok Ads campaign insights table from Google BigQuery dataset {raw_dataset} to build staging table...")
        raw_campaign_metadata = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_metadata"
        print(f"🔍 [STAGING] Using raw TikTok Ads campaign metadata table from Google BigQuery table {raw_campaign_metadata} to build staging table...")
        logging.info(f"🔍 [STAGING] Using raw TikTok Ads campaign metadata table from Google BigQuery table {raw_campaign_metadata} to build staging table...")       
        staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
        staging_campaign_table = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_campaign_insights"
        print(f"🔍 [STAGING] Preparing to build staging table {staging_campaign_table} for TikTok Ads campaign insights...")
        logging.info(f"🔍 [STAGING] Preparing to build staging table {staging_campaign_table} for TikTok Ads campaign insights...")

    # 1.1.2. Initialize Google BigQuery client
        try:
            print(f"🔍 [STAGING] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [STAGING] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            google_bigquery_client = bigquery.Client(project=PROJECT)
            print(f"✅ [STAGING] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            logging.info(f"✅ [STAGING] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
        except DefaultCredentialsError as e:
            raise RuntimeError("❌ [STAGING] Failed to initialize Google BigQuery client due to credentials error.") from e
        except Forbidden as e:
            raise RuntimeError("❌ [STAGING] Failed to initialize Google BigQuery client due to permission denial.") from e
        except GoogleAPICallError as e:
            raise RuntimeError("❌ [STAGING] Failed to initialize Google BigQuery client due to API call error.") from e
        except Exception as e:
            raise RuntimeError(f"❌ [STAGING] Failed to initialize Google BigQuery client due to {e}.") from e

    # 1.1.3. Scan all raw TikTok Ads campaign insights table(s)
        query_campaign_raw = f"""
            SELECT table_name
            FROM `{PROJECT}.{raw_dataset}.INFORMATION_SCHEMA.TABLES`
            WHERE REGEXP_CONTAINS(
                table_name,
                r'^{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_m[0-1][0-9][0-9]{{4}}$'
            )
        """
        print("🔍 [STAGING] Scanning all raw TikTok Ads campaign insights table(s)...")
        logging.info("🔍 [STAGING] Scanning all raw TikTok Ads campaign insights table(s)...")
        raw_campaign_tables = [row.table_name for row in google_bigquery_client.query(query_campaign_raw).result()]
        raw_campaign_tables = [f"{PROJECT}.{raw_dataset}.{t}" for t in raw_campaign_tables]
        if not raw_campaign_tables:
            print("⚠️ [STAGING] No raw TikTok Ads campaign insights table(s) found then staging is skipped.")
            logging.warning("⚠️ [STAGING] No raw TikTok Ads campaign insights table(s) found then staging is skipped.")
            return
        print(f"✅ [STAGING] Successfully found {len(raw_campaign_tables)} raw TikTok Ads campaign insights table(s).")
        logging.info(f"✅ [STAGING] Successfully found {len(raw_campaign_tables)} raw TikTok Ads campaign insights table(s).")

    # 1.1.4. Query all raw TikTok Ads campaign table(s)
        staging_df_combined = []
        for raw_campaign_table in raw_campaign_tables:
            query_campaign_staging = f"""
                SELECT
                    raw.*,
                    metadata.campaign_name,
                    metadata.advertiser_name,
                    metadata.operation_status
                FROM `{raw_campaign_table}` AS raw
                LEFT JOIN `{raw_campaign_metadata}` AS metadata
                    ON CAST(raw.campaign_id AS STRING) = CAST(metadata.campaign_id AS STRING)
                    AND CAST(raw.advertiser_id  AS STRING) = CAST(metadata.advertiser_id AS STRING)
            """
            try:
                print(f"🔄 [STAGING] Querying raw TikTok Ads campaign insights table {raw_campaign_table}...")
                logging.info(f"🔄 [STAGING] Querying raw TikTok Ads campaign insights table {raw_campaign_table}...")
                staging_df_queried = google_bigquery_client.query(query_campaign_staging).to_dataframe()
                print(f"✅ [STAGING] Successfully queried {len(staging_df_queried)} row(s) of raw TikTok Ads campaign insights from {raw_campaign_table}.")
                logging.info(f"✅ [STAGING] Successfully queried {len(staging_df_queried)} row(s) of raw TikTok Ads campaign insights from {raw_campaign_table}.")
            except Exception as e:
                print(f"❌ [STAGING] Failed to query raw TikTok Ads campaign insights table {raw_campaign_table} due to {e}.")
                logging.warning(f"❌ [STAGING] Failed to query raw TikTok Ads campaign insights table {raw_campaign_table} due to {e}.")
                continue

    # 1.1.5. Enrich TikTok Ads campaign insights
        if not staging_df_queried.empty:
            try:
                print(f"🔄 [STAGING] Triggering to enrich staging TikTok Ads campaign insights field(s) for {len(staging_df_queried)} row(s) from {raw_campaign_table}...")
                logging.info(f"🔄 [STAGING] Triggering to enrich staging TikTok Ads campaign insights field(s) for {len(staging_df_queried)} row(s) from {raw_campaign_table}...")
                staging_df_enriched = enrich_campaign_fields(staging_df_queried, table_id=raw_campaign_table)
                if "nhan_su" in staging_df_enriched.columns:
                    staging_df_enriched["nhan_su"] = staging_df_enriched["nhan_su"].apply(remove_string_accents)
                staging_df_renamed = staging_df_enriched.rename(columns={
                    "advertiser_id": "account_id",
                    "objective_type": "result_type",
                    "advertiser_name": "account_name",
                    "operation_status": "delivery_status"
                })                           
                staging_df_combined.append(staging_df_renamed)
            except Exception as e:
                print(f"❌ [STAGING] Failed to trigger enrichment for staging TikTok Ads campaign insights due to {e}.")
                logging.warning(f"❌ [STAGING] Failed to trigger enrichment for staging TikTok Ads campaign insights due to {e}.") 
        if not staging_df_combined:
            print("⚠️ [STAGING] No data found in any raw TikTok Ads campaign insights table(s).")
            logging.warning("⚠️ [STAGING] No data found in any raw TikTok Ads campaign insights table(s).")
            return
        staging_df_concatenated = pd.concat(staging_df_combined, ignore_index=True)
        print(f"✅ [STAGING] Successfully combined {len(staging_df_concatenated)} row(s) from all TikTok Ads raw campaign insights table(s).")
        logging.info(f"✅ [STAGING] Successfully combined {len(staging_df_concatenated)} row(s) from all TikTok Ads raw campaign insights table(s).")

    # 1.1.6. Enforce schema for TikTok Ads campaign insights
        try:
            print(f"🔄 [STAGING] Triggering to enforce schema for {len(staging_df_concatenated)} row(s) of staging TikTok Ads campaign insights...")
            logging.info(f"🔄 [STAGING] Triggering to enforce schema for {len(staging_df_concatenated)} row(s) of staging TikTok Ads campaign insights...")
            staging_df_enforced = ensure_table_schema(staging_df_concatenated, "staging_campaign_insights")
        except Exception as e:
            print(f"❌ [STAGING] Failed to trigger schema enforcement for {len(staging_df_enforced)} row(s) of staging TikTok Ads campaign insights due to {e}.")
            logging.error(f"❌ [STAGING] Failed to trigger schema enforcement for {len(staging_df_enforced)} row(s) of staging TikTok Ads campaign insights due to {e}.")
            raise   

    # 1.1.7. Upload TikTok Ads campaign insigts to Google BigQuery        
        try:
            print(f"🔍 [STAGING] Preparing to upload {len(staging_df_enforced)} row(s) of staging TikTok Ads campaign insights to Google BigQuery table {staging_campaign_table}...")
            logging.info(f"🔍 [STAGING] Preparing to upload {len(staging_df_enforced)} row(s) of staging TikTok Ads campaign insights to Google BigQuery table {staging_campaign_table}...")
            staging_df_deduplicated = staging_df_enforced.drop_duplicates()
            clustering_fields = [f for f in ["chuong_trinh", "ma_ngan_sach_cap_1", "nhan_su"] if f in staging_df_deduplicated.columns]
            try:
                google_bigquery_client.get_table(staging_campaign_table)
                staging_table_exists = True
            except Exception:
                staging_table_exists = False
            if not staging_table_exists:
                print(f"⚠️ [STAGING] Staging TikTok Ads campaign insights table {staging_campaign_table} not found then new table creation will be proceeding...")
                logging.warning(f"⚠️ [STAGING] Staging TikTok Ads campaign insights table {staging_campaign_table} not found then new table creation will be proceeding...")
                new_table_schema = []
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
                    new_table_schema.append(bigquery.SchemaField(col, google_bigquery_type))
                new_table_configuration = bigquery.Table(staging_campaign_table, schema=new_table_schema)
                if "date" in staging_df_deduplicated.columns:
                    new_table_configuration.time_partitioning = bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field="date"
                    )
                    if clustering_fields:  
                        new_table_configuration.clustering_fields = clustering_fields  
                    print(f"🔍 [STAGING] Creating staging TikTok Ads campaign insights table with defined name {staging_campaign_table} and partition on date...")
                    logging.info(f"🔍 [STAGING] Creating staging TikTok Ads campaign insights table with defined name {staging_campaign_table} and partition on date...")
                new_table_metadata = google_bigquery_client.create_table(new_table_configuration)
                print(f"✅ [STAGING] Successfully created staging TikTok Ads campaign insights table with actual name {new_table_metadata.full_table_id}.")
                logging.info(f"✅ [STAGING] Successfully created staging TikTok Ads campaign insights table with actual name {new_table_metadata.full_table_id}.")
                try: 
                    print(f"🔍 [STAGING] Uploading {len(staging_df_enforced)} row(s) of staging TikTok Ads campaign insights to new Google BigQuery table {new_table_metadata.full_table_id}...")
                    logging.info(f"🔍 [STAGING] Uploading {len(staging_df_enforced)} row(s) of staging TikTok Ads campaign insights to new Google BigQuery table {new_table_metadata.full_table_id}...")
                    job_config = bigquery.LoadJobConfig(
                        write_disposition="WRITE_APPEND",
                        source_format=bigquery.SourceFormat.PARQUET
                    )
                    load_job = google_bigquery_client.load_table_from_dataframe(
                        staging_df_enforced,
                        staging_campaign_table,
                        job_config=job_config
                    )
                    load_job.result()
                    print(f"✅ [STAGING] Successfully uploaded {len(staging_df_enforced)} row(s) of staging TikTok Ads campaign insights to new Google BigQuery table {new_table_metadata.full_table_id}.")
                    logging.info(f"✅ [STAGING] Successfully uploaded {len(staging_df_enforced)} row(s) of staging TikTok Ads campaign insights to new Google BigQuery table {new_table_metadata.full_table_id}.")
                except Exception as e:
                    print(f"❌ [STAGING] Failed to upload staging TikTok Ads campaign insights after created new Google BigQuery Table {new_table_metadata.full_table_id} due to {e}.")
                    logging.error(f"❌ [STAGING] Failed to upload staging TikTok Ads campaign insights after created new Google BigQuery Table {new_table_metadata.full_table_id} due to {e}.")    
            else:
                try:
                    print(f"🔄 [STAGING] Found staging TikTok Ads campaign insights table {staging_campaign_table} then data overwrite will be proceeding...")
                    logging.warning(f"🔄 [STAGING] Found staging TikTok Ads campaign insights table {staging_campaign_table} then data overwrite will be proceeding...")                    
                    job_config = bigquery.LoadJobConfig(
                        write_disposition="WRITE_TRUNCATE",
                        source_format=bigquery.SourceFormat.PARQUET,
                        time_partitioning=bigquery.TimePartitioning(
                            type_=bigquery.TimePartitioningType.DAY,
                            field="date"
                        ),
                        clustering_fields=clustering_fields if clustering_fields else None
                    )
                    load_job = google_bigquery_client.load_table_from_dataframe(
                        staging_df_enforced,
                        staging_campaign_table,
                        job_config=job_config
                    )
                    load_job.result()
                    print(f"✅ [STAGING] Successfully replace {len(staging_df_enforced)} row(s) to existed staging TikTok Ads campaign insights table {staging_campaign_table}.")
                    logging.info(f"✅ [STAGING] Successfully replace {len(staging_df_enforced)} row(s) to existed staging TikTok Ads campaign insights table {staging_campaign_table}.")
                except Exception as e:
                    print(f"❌ [STAGING] Failed to upload staging TikTok Ads campaign insights to existing table {staging_campaign_table} due to {e}.")
                    logging.error(f"❌ [STAGING] Failed to upload staging TikTok Ads campaign insights to existing table {staging_campaign_table} due to {e}.")                    
        except Exception as e:
            print(f"❌ [STAGING] Failed to upload TikTok Ads campaign insights due to {e}.")
            logging.error(f"❌ [STAGING] Failed to upload TikTok Ads campaign insights due to {e}.")
    except Exception as e:
        print(f"❌ [STAGING] Faild to build staging TikTok Ads campaign insights table due to {e}.")
        logging.error(f"❌ [STAGING] Faild to build staging TikTok Ads campaign insights table due to {e}.")

# 1.2. Transform TikTok Ads ad insights from raw tables into cleaned staging tables
def staging_ad_insights() -> None:
    print("🚀 [STAGING] Starting to build staging TikTok Ads ad insights table...")
    logging.info("🚀 [STAGING] Starting to build staging TikTok Ads ad insights table...")

    # 1.2.1. Prepare table_id for TikTok Ads ad insights
    try:
        raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
        print(f"🔍 [STAGING] Using raw TikTok Ads ad insights table from Google BigQuery dataset {raw_dataset} to build staging table...")
        logging.info(f"🔍 [STAGING] Using raw TikTok Ads ad insights table from Google BigQuery dataset {raw_dataset} to build staging table...")
        raw_ad_metadata = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_ad_metadata"
        print(f"🔍 [STAGING] Using raw TikTok Ads ad metadata table from Google BigQuery table {raw_ad_metadata} to build staging table...")
        logging.info(f"🔍 [STAGING] Using raw TikTok Ads ad metadata table from Google BigQuery table {raw_ad_metadata} to build staging table...")        
        raw_ad_creative = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_creative_metadata"
        print(f"🔍 [STAGING] Using raw TikTok Ads ad creative table from Google BigQuery table {raw_ad_creative} to build staging table...")
        logging.info(f"🔍 [STAGING] Using raw TikTok Ads ad creative table from Google BigQuery table {raw_ad_creative} to build staging table...")   
        staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
        staging_ad_table = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_ad_insights"
        print(f"🔍 [STAGING] Preparing to build staging table {staging_ad_insights} for TikTok Ads ad insights...")
        logging.info(f"🔍 [STAGING] Preparing to build staging table {staging_ad_insights} for TikTok Ads ad insights...")

    # 1.2.2. Initialize Google BigQuery client
        try:
            print(f"🔍 [STAGING] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [STAGING] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            google_bigquery_client = bigquery.Client(project=PROJECT)
            print(f"✅ [STAGING] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            logging.info(f"✅ [STAGING] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
        except DefaultCredentialsError as e:
            raise RuntimeError("❌ [STAGING] Failed to initialize Google BigQuery client due to credentials error.") from e
        except Forbidden as e:
            raise RuntimeError("❌ [STAGING] Failed to initialize Google BigQuery client due to permission denial.") from e
        except GoogleAPICallError as e:
            raise RuntimeError("❌ [STAGING] Failed to initialize Google BigQuery client due to API call error.") from e
        except Exception as e:
            raise RuntimeError(f"❌ [STAGING] Failed to initialize Google BigQuery client due to {e}.") from e

    # 1.2.3. Scan all raw TikTok Ads ad insights table(s)
        query_ad_raw = f"""
            SELECT table_name
            FROM `{PROJECT}.{raw_dataset}.INFORMATION_SCHEMA.TABLES`
            WHERE REGEXP_CONTAINS(
                table_name,
                r'^{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_ad_m[0-1][0-9][0-9]{{4}}$'
            )
        """
        print("🔍 [STAGING] Scanning all raw TikTok Ads ad insights table(s)...")
        logging.info("🔍 [STAGING] Scanning all raw TikTok Ads ad insights table(s)...")
        raw_ad_tables = [row.table_name for row in google_bigquery_client.query(query_ad_raw).result()]
        raw_ad_tables = [f"{PROJECT}.{raw_dataset}.{t}" for t in raw_ad_tables]
        if not raw_ad_tables:
            print("⚠️ [STAGING] No raw TikTok Ads ad insights table(s) found then staging is skipped.")
            logging.warning("⚠️ [STAGING] No raw TikTok Ads ad insights table(s) found then staging is skipped.")
            return
        print(f"✅ [STAGING] Successfully found {len(raw_ad_tables)} raw TikTok Ads ad insights table(s).")
        logging.info(f"✅ [STAGING] Successfully found {len(raw_ad_tables)} raw TikTok Ads ad insights table(s).")

    # 1.2.4. Query all raw TikTok Ads ad insights table(s)
        staging_df_combined = []
        for raw_ad_table in raw_ad_tables:
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
                    creative.video_id,
                    creative.video_cover_url,
                    creative.preview_url
                FROM `{raw_ad_table}` AS raw
                LEFT JOIN `{raw_ad_metadata}` AS ad
                    ON CAST(raw.ad_id AS STRING) = CAST(ad.ad_id AS STRING)
                    AND CAST(raw.advertiser_id AS STRING) = CAST(ad.advertiser_id AS STRING)
                LEFT JOIN `{raw_ad_creative}` AS creative
                    ON CAST(raw.ad_id AS STRING) = CAST(creative.ad_id AS STRING)
                    AND CAST(raw.advertiser_id AS STRING) = CAST(creative.advertiser_id AS STRING)
            """
            try:
                print(f"🔄 [STAGING] Querying raw TikTok Ads ad insights table {raw_ad_table}...")
                logging.info(f"🔄 [STAGING] Querying raw TikTok Ads ad insights table {raw_ad_table}...")
                staging_df_queried = google_bigquery_client.query(query_ad_staging).to_dataframe()
                print(f"✅ [STAGING] Successfully queried {len(staging_df_queried)} row(s of TikTok Ads ad insights from {raw_ad_table}.")
                logging.info(f"✅ [STAGING] Successfully queried {len(staging_df_queried)} row(s of TikTok Ads ad insights from {raw_ad_table}.")
            except Exception as e:
                print(f"❌ [STAGING] Failed to query raw TikTok Ads ad insights table {raw_ad_table} due to {e}.")
                logging.warning(f"❌ [STAGING] Failed to query raw TikTok Ads ad insights table {raw_ad_table} due to {e}.")
                continue

    # 1.2.5. Enrich TikTok Ads ad insights
            if not staging_df_queried.empty:
                try:
                    print(f"🔄 [STAGING] Triggering to enrich staging TikTok Ads ad insights field(s) for {len(staging_df_queried)} row(s) from {raw_ad_table}...")
                    logging.info(f"🔄 [STAGING] Triggering to enrich staging TikTok Ads ad insights field(s) for {len(staging_df_queried)} row(s) from {raw_ad_table}...")
                    staging_df_enriched = enrich_ad_fields(staging_df_queried, table_id=raw_ad_table)
                    if "nhan_su" in staging_df_enriched.columns:
                        staging_df_enriched["nhan_su"] = staging_df_enriched["nhan_su"].apply(remove_string_accents)
                    staging_df_renamed = staging_df_enriched.rename(columns={
                        "advertiser_id": "account_id",
                        "adgroup_id": "adset_id",
                        "adgroup_name": "adset_name",
                        "operation_status": "delivery_status"
                    })
                    staging_df_combined.append(staging_df_renamed)
                except Exception as e:
                    print(f"❌ [STAGING] Failed to trigger enrichment for staging TikTok Ads ad insights due to {e}.")
                    logging.warning(f"❌ [STAGING] Failed to trigger enrichment for staging TikTok Ads ad insights due to {e}.") 
            if not staging_df_combined:
                print("⚠️ [STAGING] No data found in any raw TikTok Ads ad insights table(s).")
                logging.warning("⚠️ [STAGING] No data found in any raw TikTok Ads ad insights table(s).")
                return
            staging_df_concatenated = pd.concat(staging_df_combined, ignore_index=True)
            print(f"✅ [STAGING] Successfully combined {len(staging_df_concatenated)} row(s) from all TikTok Ads raw ad insights table(s).")
            logging.info(f"✅ [STAGING] Successfully combined {len(staging_df_concatenated)} row(s) from all TikTok Ads raw ad insights table(s).")

    # 1.2.6. Enforce schema for TikTok Ads ad insights
        try:
            print(f"🔄 [STAGING] Triggering to enforce schema for {len(staging_df_concatenated)} row(s) of staging TikTok Ads ad insights...")
            logging.info(f"🔄 [STAGING] Triggering to enforce schema for {len(staging_df_concatenated)} row(s) of staging TikTok Ads ad insights...")
            staging_df_enforced = ensure_table_schema(staging_df_concatenated, "staging_ad_insights")
        except Exception as e:
            print(f"❌ [STAGING] Failed to trigger schema enforcement for {len(staging_df_enforced)} row(s) of staging TikTok Ads ad insights due to {e}.")
            logging.error(f"❌ [STAGING] Failed to trigger schema enforcement for {len(staging_df_enforced)} row(s) of staging TikTok Ads ad insights due to {e}.")
            raise  

    # 1.2.7. Upload TikTok Ads ad insights to Google BigQuery
        try:
            print(f"🔍 [STAGING] Preparing to upload {len(staging_df_enforced)} row(s) of staging TikTok Ads ad insights to Google BigQuery table {staging_ad_table}...")
            logging.info(f"🔍 [STAGING] Preparing to upload {len(staging_df_enforced)} row(s) of staging TikTok Ads ad insights to Google BigQuery table {staging_ad_table}...")
            staging_df_deduplicated = staging_df_enforced.drop_duplicates()
            clustering_fields = [f for f in ["chuong_trinh", "ma_ngan_sach_cap_1", "nhan_su"] if f in staging_df_deduplicated.columns]
            try:
                google_bigquery_client.get_table(staging_ad_table)
                staging_table_exists = True
            except Exception:
                staging_table_exists = False
            if not staging_table_exists:
                new_table_schema = []
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
                    new_table_schema.append(bigquery.SchemaField(col, google_bigquery_type))
                new_table_configuration = bigquery.Table(staging_ad_table, schema=new_table_schema)
                if "date" in staging_df_deduplicated.columns:
                    new_table_configuration.time_partitioning = bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field="date"
                    )
                    if clustering_fields:  
                        new_table_configuration.clustering_fields = clustering_fields                      
                    print(f"🔍 [STAGING] Creating staging TikTok Ads ad insights table with defined name {staging_ad_table} and partition on date...")
                    logging.info(f"🔍 [STAGING] Creating staging TikTok Ads ad insights table with defined name {staging_ad_table} and partition on date...")
                new_table_metadata = google_bigquery_client.create_table(new_table_configuration)    
                print(f"✅ [STAGING] Successfully created staging TikTok Ads ad insights table with actual name {new_table_metadata.full_table_id}.")
                logging.info(f"✅ [STAGING] Successfully created staging TikTok Ads ad insights table with actual name {new_table_metadata.full_table_id}.")
                try: 
                    print(f"🔍 [STAGING] Uploading {len(staging_df_enforced)} row(s) of staging TikTok Ads ad insights to new Google BigQuery table {new_table_metadata.full_table_id}...")
                    logging.info(f"🔍 [STAGING] Uploading {len(staging_df_enforced)} row(s) of staging TikTok Ads ad insights to new Google BigQuery table {new_table_metadata.full_table_id}...")
                    job_config = bigquery.LoadJobConfig(
                        write_disposition="WRITE_APPEND",
                        source_format=bigquery.SourceFormat.PARQUET
                    )
                    load_job = google_bigquery_client.load_table_from_dataframe(
                        staging_df_enforced,
                        staging_ad_table,
                        job_config=job_config
                    )
                    load_job.result()
                    print(f"✅ [STAGING] Successfully uploaded {len(staging_df_enforced)} row(s) of staging TikTok Ads ad insights to new Google BigQuery table {new_table_metadata.full_table_id}.")
                    logging.info(f"✅ [STAGING] Successfully uploaded {len(staging_df_enforced)} row(s) of staging TikTok Ads ad insights to new Google BigQuery table {new_table_metadata.full_table_id}.")
                except Exception as e:
                    print(f"❌ [STAGING] Failed to upload staging TikTok Ads ad insights after created new Google BigQuery Table {new_table_metadata.full_table_id} due to {e}.")
                    logging.error(f"❌ [STAGING] Failed to upload staging TikTok Ads ad insights after created new Google BigQuery Table {new_table_metadata.full_table_id} due to {e}.")                  
            else:
                try:
                    print(f"🔄 [STAGING] Found staging TikTok Ads ad insights table {staging_ad_table} then data overwrite will be proceeding...")
                    logging.warning(f"🔄 [STAGING] Found staging TikTok Ads ad insights table {staging_ad_table} then data overwrite will be proceeding...")     
                    job_config = bigquery.LoadJobConfig(
                        write_disposition="WRITE_TRUNCATE",
                        source_format=bigquery.SourceFormat.PARQUET,
                        time_partitioning=bigquery.TimePartitioning(
                            type_=bigquery.TimePartitioningType.DAY,
                            field="date"
                        ),
                        clustering_fields=clustering_fields if clustering_fields else None
                    )
                    print(f"🔍 [STAGING] Uploading {len(staging_df_enforced)} row(s) of staging TikTok Ads ad insights to existing Google BigQuery table {staging_ad_table}...")
                    logging.info(f"🔍 [STAGING] Uploading {len(staging_df_enforced)} row(s) of staging TikTok Ads ad insights to existing Google BigQuery table {staging_ad_table}...")
                    load_job = google_bigquery_client.load_table_from_dataframe(
                        staging_df_enforced,
                        staging_ad_table,
                        job_config=job_config
                    )
                    load_job.result()
                    print(f"✅ [STAGING] Successfully replace {len(staging_df_enforced)} row(s) to existed staging TikTok Ads ad insights table {staging_ad_table}.")
                    logging.info(f"✅ [STAGING] Successfully replace {len(staging_df_enforced)} row(s) to existed staging TikTok Ads ad insights table {staging_ad_table}.")
                except Exception as e:
                    print(f"❌ [STAGING] Failed to upload staging TikTok Ads ad insights to existing table {staging_ad_table} due to {e}.")
                    logging.error(f"❌ [STAGING] Failed to upload staging TikTok Ads ad insights to existing table {staging_ad_table} due to {e}.")
        except Exception as e:
            print(f"❌ [STAGING] Failed to upload TikTok Ads ad insights due to {e}.")
            logging.error(f"❌ [STAGING] Failed to upload TikTok Ads ad insights due to {e}.")
    except Exception as e:
        print(f"❌ [STAGING] Faild to build staging TikTok Ads ad insights table due to {e}.")
        logging.error(f"❌ [STAGING] Faild to build staging TikTok Ads ad insights table due to {e}.")