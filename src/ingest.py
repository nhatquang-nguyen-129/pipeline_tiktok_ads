"""
==================================================================
TIKTOK INGESTION MODULE
------------------------------------------------------------------
This module ingests raw data from the TikTok Marketing API into 
Google BigQuery by forming the raw data layer of the Ads Data Pipeline.

It orchestrates the full ingestion flow, from authenticating the SDK, 
to fetching data, enriching it, validating schema, and loading into 
BigQuery tables organized by campaign, ad, creative and metadata.

✔️ Supports append or truncate via configurable `write_disposition`  
✔️ Applies schema validation through centralized schema utilities  
✔️ Includes logging and CSV-based error tracking for traceability  
✔️ Handles full-funnel endpoints (campaign, ad, creative, report)  
✔️ Ensures data freshness and integrity across daily ingestions  

⚠️ This module is strictly limited to *raw-layer ingestion*.  
It does **not** handle data transformation, modeling, or aggregation.
==================================================================
"""

# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add Python datetime utilities for integration
from datetime import datetime

# Add Python logging ultilities forintegration
import logging

# Add Python timezone ultilities for integration
import pytz

# Add Python time ultilities for integration
import time

# Add UUID ultilities for integration
import uuid

# Add Python Pandas libraries for integration
import pandas as pd

# Add Google Authentication modules for integration
from google.api_core.exceptions import (
    Forbidden,
    GoogleAPICallError
)
from google.auth.exceptions import DefaultCredentialsError

# Add Google Cloud modules for integration
from google.cloud import bigquery

# Add internal TikTok Ads modules for handling
from src.fetch import (
    fetch_campaign_metadata,
    fetch_ad_metadata,
    fetch_ad_creative,
    fetch_campaign_insights,
    fetch_ad_insights
)
from src.schema import ensure_table_schema

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

# 1. INGEST TIKTOK ADS METADATA

# 1.1. Ingest campaign metadata for TikTok Ads
def ingest_campaign_metadata(campaign_id_list: list) -> pd.DataFrame:
    print(f"🚀 [INGEST] Starting to ingest TikTok Ads campaign metadata for {len(campaign_id_list)} campaign_id(s)...")
    logging.info(f"🚀 [INGEST] Starting to ingest TikTok Ads campaign metadata for {len(campaign_id_list)} campaign_id(s)...")

    # 1.1.1. Start timing the TikTok Ads campaign metadata ingestion process
    start_time = time.time()
    ingest_section_succeeded = {}
    ingest_section_failed = [] 
    print(f"🔍 [INGEST] Proceeding to ingest TikTok Ads campaign metadata for {len(campaign_id_list)} campaign_id(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest TikTok Ads campaign metadata for {len(campaign_id_list)} campaign_id(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    # 1.1.2. Validate input for TikTok Ads campaign metadata ingestion
    if not campaign_id_list:
        print("⚠️ [INGEST] Empty TikTok Ads campaign_id_list provided then ingestion is suspended.")
        logging.warning("⚠️ [INGEST] Empty TikTok Ads campaign_id_list provided then ingestion is suspended.")
        raise ValueError("⚠️ [INGEST] Empty TikTok Ads campaign_id_list provided then ingestion is suspended.")

    # 1.1.3. Prepare table_id for TikTok Ads campaign metadata ingestion
    raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
    table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_metadata"
    print(f"🔍 [INGEST] Proceeding to ingest TikTok Ads campaign metadata for {len(campaign_id_list)} campaign_id(s) with Google BigQuery table {table_id}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest TikTok Ads campaign metadata for {len(campaign_id_list)} campaign_id(s) with Google BigQuery table {table_id}...")

    # 1.1.4. Trigger to fetch TikTok Ads campaign metadata
    try:
        print(f"🔁 [INGEST] Triggering to fetch TikTok Ads campaign metadata for {len(campaign_id_list)} campaign_id(s)...")
        logging.info(f"🔁 [INGEST] Triggering to fetch TikTok Ads campaign metadata for {len(campaign_id_list)} campaign_id(s)...")
        ingest_df_fetched = fetch_campaign_metadata(campaign_id_list=campaign_id_list)
        if ingest_df_fetched.empty:
            print("⚠️ [INGEST] Empty TikTok Ads campaign metadata returned then ingestion is suspended.")
            logging.warning("⚠️ [INGEST] Empty TikTok Ads campaign metadata returned then ingestion is suspended.")
            return pd.DataFrame()
    except Exception as e:
        print(f"❌ [INGEST] Failed to trigger TikTok Ads campaign metadata fetching due to {e}.")
        logging.error(f"❌ [INGEST] Failed to trigger TikTok Ads campaign metadata fetching due to {e}.")
        return pd.DataFrame()

    # 1.1.5. Enforce schema for TikTok Ads campaign metadata
    try:
        print(f"🔄 [INGEST] Triggering to enforce schema for {len(ingest_df_fetched)} row(s) of TikTok Ads campaign metadata...")
        logging.info(f"🔄 [INGEST] Triggering to enforce schema for {len(ingest_df_fetched)} row(s) of TikTok Ads campaign metadata...")
        ingest_df_enforced = ensure_table_schema(ingest_df_fetched, "ingest_campaign_metadata")
    except Exception as e:
        print(f"❌ [INGEST] Failed to trigger schema enforcement for TikTok Ads campaign metadata due to {e}.")
        logging.error(f"❌ [INGEST] Failed to trigger schema enforcement for TikTok Ads campaign metadata due to {e}.")
        raise 

    # 1.1.6. Initialize Google BigQuery client
    try:
        print(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
        logging.info(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
        google_bigquery_client = bigquery.Client(project=PROJECT)
        print(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
        logging.info(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
        ingest_section_succeeded["1.1.6. Initialize Google BigQuery client"] = True
    except Exception as e:
        ingest_section_succeeded["1.1.6. Initialize Google BigQuery client"] = False
        ingest_section_failed.append("1.1.6. Initialize Google BigQuery client")
        print(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        logging.error(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        raise RuntimeError(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.") from e

    # 1.1.7. Delete existing row(s) or create new table if not exist
    try:
        ingest_df_deduplicated = ingest_df_enforced.drop_duplicates()      
        try:
            print(f"🔍 [INGEST] Checking TikTok Ads campaign metadata table {table_id} existence...")
            logging.info(f"🔍 [INGEST] Checking TikTok Ads campaign metadata table {table_id} existence...")
            google_bigquery_client.get_table(table_id)
            table_exists = True
        except Exception:
            table_exists = False
        if not table_exists:
            print(f"⚠️ [INGEST] TikTok Ads campaign metadata table {table_id} not found then table creation will be proceeding...")
            logging.info(f"⚠️ [INGEST] TikTok Ads campaign metadata table {table_id} not found then table creation will be proceeding...")
            schema = []
            for col, dtype in ingest_df_deduplicated.dtypes.items():
                if dtype.name.startswith("int"):
                    bq_type = "INT64"
                elif dtype.name.startswith("float"):
                    bq_type = "FLOAT64"
                elif dtype.name == "bool":
                    bq_type = "BOOL"
                elif "datetime" in dtype.name:
                    bq_type = "TIMESTAMP"
                else:
                    bq_type = "STRING"
                schema.append(bigquery.SchemaField(col, bq_type))
            table = bigquery.Table(table_id, schema=schema)
            effective_partition = "date" if "date" in ingest_df_deduplicated.columns else None
            if effective_partition:
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=effective_partition
                )
            clustering_fields = ["campaign_id", "advertiser_id"]
            filtered_clusters = [f for f in clustering_fields if f in ingest_df_deduplicated.columns]
            if filtered_clusters:
                table.clustering_fields = filtered_clusters
                print(f"🔍 [INGEST] Creating TikTok Ads campaign metadata table {table_id} using clustering on {filtered_clusters} and partition on {effective_partition}...")
                logging.info(f"🔍 [INGEST] Creating TikTok Ads campaign metadata table {table_id} using clustering on {filtered_clusters} and partition on {effective_partition}...")
            table = google_bigquery_client.create_table(table)
            print(f"✅ [INGEST] Successfully created TikTok Ads campaign metadata table {table_id}.")
            logging.info(f"✅ [INGEST] Successfully created TikTok Ads campaign metadata table {table_id}.")
        else:
            print(f"🔄 [INGEST] TikTok Ads campaign metadata table {table_id} exists then existing row(s) deletion will be proceeding...")
            logging.info(f"🔄 [INGEST] TikTok Ads campaign metadata table {table_id} exists then existing row(s) deletion will be proceeding...")
            unique_keys = ingest_df_deduplicated[["campaign_id", "advertiser_id"]].dropna().drop_duplicates()
            if not unique_keys.empty:
                temp_table_id = f"{PROJECT}.{raw_dataset}.temp_table_campaign_metadata_delete_keys_{uuid.uuid4().hex[:8]}"
                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
                google_bigquery_client.load_table_from_dataframe(unique_keys, temp_table_id, job_config=job_config).result()
                join_condition = " AND ".join([
                    f"CAST(main.{col} AS STRING) = CAST(temp.{col} AS STRING)"
                    for col in ["campaign_id", "advertiser_id"]
                ])
                delete_query = f"""
                    DELETE FROM `{table_id}` AS main
                    WHERE EXISTS (
                        SELECT 1 FROM `{temp_table_id}` AS temp
                        WHERE {join_condition}
                    )
                """
                result = google_bigquery_client.query(delete_query).result()
                google_bigquery_client.delete_table(temp_table_id, not_found_ok=True)
                deleted_rows = result.num_dml_affected_rows
                print(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of TikTok Ads campaign metadata table {table_id}.")
                logging.info(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of TikTok Ads campaign metadata table {table_id}.")
            else:
                print(f"⚠️ [INGEST] No unique campaign_id and advertiser_id keys found in TikTok Ads campaign metadata table {table_id} then deletion is skipped.")
                logging.warning(f"⚠️ [INGEST] No unique campaign_id and advertiser_id keys found in TikTok Ads campaign metadata table {table_id} then deletion is skipped.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to create new table or delete existing row(s) of TikTok Ads campaign metadata ingestion due to {e}.")
        logging.error(f"❌ [INGEST] Failed to create new table or delete existing row(s) of TikTok Ads campaign metadata ingestion due to {e}.")
        raise RuntimeError(f"❌ [INGEST] Failed to create new table or delete existing row(s) of TikTok Ads campaign metadata ingestion due to {e}.")

    # 1.1.8. Upload TikTok Ads campaign metadata to Google BigQuery
    try:
        print(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} row(s) of TikTok Ads campaign metadata to Google BigQuery table {table_id}...")
        logging.info(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} row(s) of TikTok Ads campaign metadata to Google BigQuery table {table_id}...")
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        google_bigquery_client.load_table_from_dataframe(ingest_df_deduplicated, table_id, job_config=job_config).result()
        print(f"✅ [INGEST] Successfully uploaded {len(ingest_df_deduplicated)} row(s) of TikTok Ads campaign metadata to Google BigQuery table {table_id}.")
        logging.info(f"✅ [INGEST] Successfully uploaded {len(ingest_df_deduplicated)} row(s) of TikTok Ads campaign metadata to Google BigQuery table {table_id}.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to upload TikTok Ads campaign metadata due to {e}.")
        logging.error(f"❌ [INGEST] Failed to upload TikTok Ads campaign metadata due to {e}.")
        raise RuntimeError(f"❌ [INGEST] Failed to upload TikTok Ads campaign metadata due to {e}.")

    # 1.1.9. Summarize ingestion result(s)
    finally:
        elapsed = round(time.time() - start_time, 2)
        if ingest_section_failed:
            print(f"❌ [INGEST] Failed to complete TikTok Ads campaign metadata ingestion due to unsuccesfull section(s) {', '.join(ingest_section_failed)}.")
            logging.error(f"❌ [INGEST] Failed to complete TikTok Ads campaign metadata ingestion due to unsuccesfull section(s) {', '.join(ingest_section_failed)}.")
            ingest_status_def = "failed"
        else:
            ingest_df_final = ingest_df_deduplicated
            print(f"🏆 [INGEST] Successfully completed TikTok Ads campaign metadata ingestion with {len(ingest_df_final)} row(s) in {elapsed}s.")
            logging.info(f"🏆 [INGEST] Successfully completed TikTok Ads campaign metadata ingestion with {len(ingest_df_final)} row(s) in {elapsed}s.")
            ingest_status_def = "success"
            return ingest_df_final
        return {"ingest_status_def": ingest_status_def, "ingest_seconds_elapsed": elapsed, "ingest_sections_failed": ingest_section_failed}

# 1.2. Ingest ad metadata for TikTok Ads
def ingest_ad_metadata(ad_id_list: list) -> pd.DataFrame:
    print(f"🚀 [INGEST] Starting to ingest TikTok Ads ad metadata for {len(ad_id_list)} ad_id(s)...")
    logging.info(f"🚀 [INGEST] Starting to ingest TikTok Ads ad metadata for {len(ad_id_list)} ad_id(s)...")

    # 1.2.1. Start timing the TikTok Ads ad metadata ingestion process
    start_time = time.time()
    ingest_section_succeeded = {}
    ingest_section_failed = [] 
    print(f"🔍 [INGEST] Proceeding to ingest TikTok Ads ad metadata for {len(ad_id_list)} ad_id(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest TikTok Ads ad metadata for {len(ad_id_list)} ad_id(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    # 1.2.2. Validate input for TikTok Ads ad metadata ingestion
    if not ad_id_list:
        print("⚠️ [INGEST] Empty TikTok Ads ad_id_list provided then ingestion is suspended.")
        logging.warning("⚠️ [INGEST] Empty TikTok Ads ad_id_list provided then ingestion is suspended.")
        raise ValueError("⚠️ [INGEST] Empty TikTok Ads ad_id_list provided then ingestion is suspended.")

    # 1.2.3. Prepare table_id for TikTok Ads ad metadata ingestion
    raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
    table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_ad_metadata"
    print(f"🔍 [INGEST] Proceeding to ingest TikTok ad metadata for {len(ad_id_list)} ad_id(s) with Google BigQuery table {table_id}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest TikTok ad metadata for {len(ad_id_list)} ad_id(s) with Google BigQuery table {table_id}...")

    # 1.2.4. Trigger to fetch TikTok Ads ad metadata
    try:
        print(f"🔁 [INGEST] Triggering to fetch TikTok Ads ad metadata for {len(ad_id_list)} ad_id(s)...")
        logging.info(f"🔁 [INGEST] Triggering to fetch TikTok Ads ad metadata for {len(ad_id_list)} ad_id(s)...")
        ingest_df_fetched = fetch_ad_metadata(ad_id_list=ad_id_list)
        if ingest_df_fetched.empty:
            print("⚠️ [INGEST] Empty TikTok Ads ad metadata returned then ingestion is suspended.")
            logging.warning("⚠️ [INGEST] Empty TikTok Ads ad metadata returned then ingestion is suspended.")
            return pd.DataFrame()
    except Exception as e:
        print(f"❌ [INGEST] Failed to trigger TikTok Ads ad metadata fetching due to {e}.")
        logging.error(f"❌ [INGEST] Failed to trigger TikTok Ads ad metadata fetching due to {e}.")
        return pd.DataFrame()

    # 1.2.5. Enforce schema for TikTok Ads ad metadata
    try:
        print(f"🔄 [INGEST] Trigger to enforce schema for {len(ingest_df_fetched)} row(s) of TikTok ad metadata...")
        logging.info(f"🔄 [INGEST] Trigger to enforce schema for {len(ingest_df_fetched)} row(s) of TikTok ad metadata...")
        ingest_df_enforced = ensure_table_schema(ingest_df_fetched, "ingest_ad_metadata")
    except Exception as e:
        print(f"❌ [INGEST] Failed to trigger schema enforcement for TikTok ad metadata due to {e}.")
        logging.error(f"❌ [INGEST] Failed to trigger schema enforcement for TikTok ad metadata due to {e}.")
        raise RuntimeError(f"❌ [INGEST] Failed to trigger schema enforcement for TikTok ad metadata due to {e}.")

    # 1.2.6. Initialize Google BigQuery client
    try:
        print(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
        logging.info(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
        google_bigquery_client = bigquery.Client(project=PROJECT)
        print(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
        logging.info(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
        ingest_section_succeeded["1.2.6. Initialize Google BigQuery client"] = True
    except Exception as e:
        ingest_section_succeeded["1.2.6. Initialize Google BigQuery client"] = False
        ingest_section_failed.append("1.2.6. Initialize Google BigQuery client")
        print(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        logging.error(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        raise RuntimeError(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.") from e

    # 1.2.7. Delete existing row(s) or create new table if it not exist
    try:
        ingest_df_deduplicated = ingest_df_enforced.drop_duplicates()
        try:
            print(f"🔍 [INGEST] Checking TikTok Ads ad metadata table {table_id} existence...")
            logging.info(f"🔍 [INGEST] Checking TikTok Ads ad metadata table {table_id} existence...")
            google_bigquery_client.get_table(table_id)
            table_exists = True
        except Exception:
            table_exists = False
        if not table_exists:
            print(f"⚠️ [INGEST] TikTok Ads ad metadata table {table_id} not found then table creation will be proceeding...")
            logging.info(f"⚠️ [INGEST] TikTok Ads ad metadata table {table_id} not found then table creation will be proceeding...")
            schema = []
            for col, dtype in ingest_df_deduplicated.dtypes.items():
                if dtype.name.startswith("int"):
                    bq_type = "INT64"
                elif dtype.name.startswith("float"):
                    bq_type = "FLOAT64"
                elif dtype.name == "bool":
                    bq_type = "BOOL"
                elif "datetime" in dtype.name:
                    bq_type = "TIMESTAMP"
                else:
                    bq_type = "STRING"
                schema.append(bigquery.SchemaField(col, bq_type))
            table = bigquery.Table(table_id, schema=schema)
            effective_partition = "date" if "date" in ingest_df_deduplicated.columns else None
            if effective_partition:
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=effective_partition
                )
            clustering_fields = ["ad_id", "advertiser_id"]
            filtered_clusters = [f for f in clustering_fields if f in ingest_df_deduplicated.columns]
            if filtered_clusters:
                table.clustering_fields = filtered_clusters
                print(f"🔍 [INGEST] Creating TikTok Ads ad metadata table {table_id} using clustering on {filtered_clusters} field(s) and partition on {effective_partition}...")
                logging.info(f"🔍 [INGEST] Creating TikTok Ads ad metadata table {table_id} using clustering on {filtered_clusters} field(s) and partition on {effective_partition}...")
            table = google_bigquery_client.create_table(table)
            print(f"✅ [INGEST] Successfully created TikTok Ads ad metadata table {table_id} with clustering on {filtered_clusters} field(s) and partition on {effective_partition}.")
            logging.info(f"✅ [INGEST] Successfully created TikTok Ads ad metadata table {table_id} with clustering on {filtered_clusters} field(s) and partition on {effective_partition}.")
        else:
            print(f"🔄 [INGEST] TikTok Ads ad metadata table {table_id} exists then existing row(s) deletion will be proceeding...")
            logging.info(f"🔄 [INGEST] TikTok Ads ad metadata table {table_id} exists then existing row(s) deletion will be proceeding...")
            unique_keys = ingest_df_deduplicated[["ad_id", "advertiser_id"]].dropna().drop_duplicates()
            if not unique_keys.empty:
                temp_table_id = f"{PROJECT}.{raw_dataset}.temp_table_ad_metadata_delete_keys_{uuid.uuid4().hex[:8]}"
                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
                google_bigquery_client.load_table_from_dataframe(unique_keys, temp_table_id, job_config=job_config).result()
                join_condition = " AND ".join([
                    f"CAST(main.{col} AS STRING) = CAST(temp.{col} AS STRING)"
                    for col in ["ad_id", "advertiser_id"]
                ])
                delete_query = f"""
                    DELETE FROM `{table_id}` AS main
                    WHERE EXISTS (
                        SELECT 1 FROM `{temp_table_id}` AS temp
                        WHERE {join_condition}
                    )
                """
                result = google_bigquery_client.query(delete_query).result()
                google_bigquery_client.delete_table(temp_table_id, not_found_ok=True)
                deleted_rows = result.num_dml_affected_rows
                print(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of TikTok ad metadata table {table_id}.")
                logging.info(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of TikTok ad metadata table {table_id}.")
            else:
                print(f"⚠️ [INGEST] No unique (ad_id and advertiser_id) keys found in TikTok ad metadata table {table_id} then existing row(s) deletion is skipped.")
                logging.warning(f"⚠️ [INGEST] No unique (ad_id and advertiser_id) keys found in TikTok ad metadata table {table_id} then existing row(s) deletion is skipped.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to create new table or delete existing row(s) of TikTok Ads ad metadata due to {e}.")
        logging.error(f"❌ [INGEST] Failed to create new table or delete existing row(s) of TikTok Ads ad metadata due to {e}.")
        raise RuntimeError(f"❌ [INGEST] Failed to create new table or delete existing row(s) of TikTok Ads ad metadata due to {e}.")

    # 1.2.8. Upload TikTok Ads ad metadata to Google BigQuery
    try:
        print(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} row(s) of TikTok Ads ad metadata to Google BigQuery table {table_id}...")
        logging.info(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} row(s) of TikTok Ads ad metadata to Google BigQuery table {table_id}...")
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        google_bigquery_client.load_table_from_dataframe(ingest_df_deduplicated, table_id, job_config=job_config).result()
        print(f"✅ [INGEST] Successfully uploaded {len(ingest_df_deduplicated)} row(s) of TikTok Ads ad metadata to Google BigQuery table {table_id}.")
        logging.info(f"✅ [INGEST] Successfully uploaded {len(ingest_df_deduplicated)} row(s) of TikTok Ads ad metadata to Google BigQuery table {table_id}.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to upload TikTok Ads ad metadata due to {e}.")
        logging.error(f"❌ [INGEST] Failed to upload TikTok Ads ad metadata due to {e}.")
        raise
    
    # 1.2.9. Summarize ingestion result(s)
    finally:
        elapsed = round(time.time() - start_time, 2)
        if ingest_section_failed:
            print(f"❌ [INGEST] Failed to complete TikTok Ads ad metadata ingestion due to unsuccesfull section(s) {', '.join(ingest_section_failed)}.")
            logging.error(f"❌ [INGEST] Failed to complete TikTok Ads ad metadata ingestion due to unsuccesfull section(s) {', '.join(ingest_section_failed)}.")
            ingest_status_def = "failed"
        else:
            ingest_df_final = ingest_df_deduplicated
            print(f"🏆 [INGEST] Successfully completed TikTok Ads ad metadata ingestion with {len(ingest_df_final)} row(s) in {elapsed}s.")
            logging.info(f"🏆 [INGEST] Successfully completed TikTok Ads ad metadata ingestion with {len(ingest_df_final)} row(s) in {elapsed}s.")
            ingest_status_def = "success"
            return ingest_df_final
        return {"ingest_status_def": ingest_status_def, "ingest_seconds_elapsed": elapsed, "ingest_sections_failed": ingest_section_failed}

# 1.3. Ingest ad creative for TikTok Ads
def ingest_ad_creative() -> pd.DataFrame:
    print(f"🚀 [INGEST] Starting to ingest TikTok Ads ad creative ...")
    logging.info(f"🚀 [INGEST] Starting to ingest TikTok Ads ad creative...")

    # 1.3.1. Start timing the TikTok Ads campaign metadata ingestion process
    start_time = time.time()
    ingest_section_succeeded = {}
    ingest_section_failed = [] 
    print(f"🔍 [INGEST] Proceeding to ingest TikTok Ads ad creative at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest TikTok Ads ad creative {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    # 1.3.2. Prepare table_id for TikTok Ads ad creative ingestion
    raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
    table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_ad_creative"
    print(f"🔍 [INGEST] Proceeding to ingest TikTok Ads ad creative with Google BigQuery table {table_id}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest TikTok Ads ad creative with Google BigQuery table {table_id}...")

    # 1.3.3. Trigger to fetch TikTok Ads ad creative
    try:
        print(f"🔁 [INGEST] Triggering to fetch TikTok Ads ad creative...")
        logging.info(f"🔁 [INGEST] Triggering to fetch TikTok Ads ad creative...")
        ingest_df_fetched = fetch_ad_creative()
        if ingest_df_fetched.empty:
            print("⚠️ [INGEST] Empty TikTok Ads ad creative returned then ingestion is suspended.")
            logging.warning("⚠️ [INGEST] Empty TikTok Ads ad creative returned then ingestion is suspended.")
            return pd.DataFrame()
    except Exception as e:
        print(f"❌ [INGEST] Failed to trigger TikTok Ads ad creative fetching due to {e}.")
        logging.error(f"❌ [INGEST] Failed to trigger TikTok Ads ad creative fetching due to {e}.")
        return pd.DataFrame()

    # 1.3.4 Enforce schema for TikTok Ads ad creative
    try:
        print(f"🔄 [INGEST] Triggering to enforce schema for {len(ingest_df_fetched)} row(s) of TikTok Ads ad creative...")
        logging.info(f"🔄 [INGEST] Triggering to enforce schema for {len(ingest_df_fetched)} row(s) of TikTok Ads ad creative...")
        ingest_df_enforced = ensure_table_schema(ingest_df_fetched, "ingest_ad_creative")
    except Exception as e:
        print(f"❌ [INGEST] Failed to trigger schema enforcement for TikTok Ads ad creative metadata due to {e}.")
        logging.error(f"❌ [INGEST] Failed to trigger schema enforcement for TikTok Ads ad creative metadata due to {e}.")
        raise RuntimeError(f"❌ [INGEST] Failed to trigger schema enforcement for TikTok Ads ad creative metadata due to {e}.")

    # 1.3.5. Initialize Google BigQuery client
    try:
        print(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
        logging.info(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
        google_bigquery_client = bigquery.Client(project=PROJECT)
        print(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
        logging.info(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
        ingest_section_succeeded["1.3.5. Initialize Google BigQuery client"] = True
    except Exception as e:
        ingest_section_succeeded["1.3.5. Initialize Google BigQuery client"] = False
        ingest_section_failed.append("1.3.5. Initialize Google BigQuery client")
        print(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        logging.error(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        raise RuntimeError(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.") from e

    # 1.3.6. Delete existing row(s) or create new table if it not exist
    try:
        ingest_df_deduplicated = ingest_df_enforced.drop_duplicates()                   
        try:
            print(f"🔍 [INGEST] Checking TikTok ad creative table {table_id} existence...")
            logging.info(f"🔍 [INGEST] Checking TikTok ad creative table {table_id} existence...")
            google_bigquery_client.get_table(table_id)
            table_exists = True
        except Exception:
            table_exists = False
        if not table_exists:
            print(f"⚠️ [INGEST] TikTok Ads ad creative table {table_id} not found then table creation will be proceeding...")
            logging.info(f"⚠️ [INGEST] TikTok Ads ad creative table {table_id} not found then table creation will be proceeding...")
            schema = []
            for col, dtype in ingest_df_deduplicated.dtypes.items():
                if dtype.name.startswith("int"):
                    bq_type = "INT64"
                elif dtype.name.startswith("float"):
                    bq_type = "FLOAT64"
                elif dtype.name == "bool":
                    bq_type = "BOOL"
                elif "datetime" in dtype.name:
                    bq_type = "TIMESTAMP"
                else:
                    bq_type = "STRING"
                schema.append(bigquery.SchemaField(col, bq_type))
            table = bigquery.Table(table_id, schema=schema)
            effective_partition = "date" if "date" in ingest_df_deduplicated.columns else None
            if effective_partition:
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=effective_partition
                )
            clustering_fields = ["video_id", "advertiser_id"]
            filtered_clusters = [f for f in clustering_fields if f in ingest_df_deduplicated.columns]
            if filtered_clusters:
                table.clustering_fields = filtered_clusters
                print(f"🔍 [INGEST] Creating TikTok Ads ad creative table {table_id} using clustering on {filtered_clusters} field(s) and partition on {effective_partition})...")
                logging.info(f" [INGEST] Creating TikTok Ads ad creative table {table_id} using clustering on {filtered_clusters} field(s) and partition on {effective_partition}...")
            table = google_bigquery_client.create_table(table)
            print(f"✅ [INGEST] Successfully created TikTok Ads ad creative table {table_id} with clustering on {filtered_clusters} field(s) and partition on {effective_partition}.")
            logging.info(f"✅ [INGEST] Successfully created TikTok Ads ad creative table {table_id} with clustering on {filtered_clusters} field(s) and partition on {effective_partition}.")
        else:
            print(f"🔄 [INGEST] TikTok Ads ad creative table {table_id} exists then existing row(s) deletion will be proceeding...")
            logging.info(f"🔄 [INGEST] TikTok Ads ad creative table {table_id} exists then existing row(s) deletion will be proceeding...")
            unique_keys = ingest_df_deduplicated[["video_id", "advertiser_id"]].dropna().drop_duplicates()
            if not unique_keys.empty:
                temp_table_id = f"{PROJECT}.{raw_dataset}.temp_table_ad_creative_delete_keys_{uuid.uuid4().hex[:8]}"
                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
                google_bigquery_client.load_table_from_dataframe(unique_keys, temp_table_id, job_config=job_config).result()
                join_condition = " AND ".join([
                    f"CAST(main.{col} AS STRING) = CAST(temp.{col} AS STRING)"
                    for col in ["video_id", "advertiser_id"]
                ])
                delete_query = f"""
                    DELETE FROM `{table_id}` AS main
                    WHERE EXISTS (
                        SELECT 1 FROM `{temp_table_id}` AS temp
                        WHERE {join_condition}
                    )
                """
                result = google_bigquery_client.query(delete_query).result()
                google_bigquery_client.delete_table(temp_table_id, not_found_ok=True)
                deleted_rows = result.num_dml_affected_rows
                print(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of TikTok Ads ad creative table {table_id}.")
                logging.info(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of TikTok Ads ad creative table {table_id}.")
            else:
                print(f"⚠️ [INGEST] No unique video_id and advertisier_id keys found in TikTok Ads ad creative table {table_id} then existing row(s) deletion is skipped.")
                logging.warning(f"⚠️ [INGEST] No unique video_id and advertiser_id keys found in TikTok Ads ad creative table {table_id} then existing row(s) deletion is skipped.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to create new table or delete existing row(s) of TikTok Ads ad creative ingestion due to {e}.")
        logging.error(f"❌ [INGEST] Failed to create new table or delete existing row(s) of TikTok Ads ad creative ingestion due to {e}.")
        raise RuntimeError(f"❌ [INGEST] Failed to create new table or delete existing row(s) of TikTok Ads ad creative ingestion due to {e}.")

    # 1.3.7. Upload TikTok Ads ad creative to Google BigQuery
    try:
        print(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} row(s) of TikTok Ads ad creative to Google BigQuery table {table_id}...")
        logging.info(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} row(s) of TikTok Ads ad creative to Google BigQuery table {table_id}...")
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        google_bigquery_client.load_table_from_dataframe(ingest_df_deduplicated, table_id, job_config=job_config).result()
        print(f"✅ [INGEST] Successfully uploaded {len(ingest_df_deduplicated)} row(s) of TikTok Ads campaign metadata table {table_id}.")
        logging.info(f"✅ [INGEST] Successfully uploaded {len(ingest_df_deduplicated)} row(s) of TikTok Ads campaign metadata table {table_id}.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to upload TikTok Ads campaign metadata due to {e}.")
        logging.error(f"❌ [INGEST] Failed to upload TikTok Ads campaign metadata due to {e}.")
        raise RuntimeError(f"❌ [INGEST] Failed to upload TikTok Ads campaign metadata due to {e}.")

    # 1.3.8. Summarize ingestion result(s)
    finally:
        elapsed = round(time.time() - start_time, 2)
        if ingest_section_failed:
            print(f"❌ [INGEST] Failed to complete TikTok Ads ad creative ingestion due to unsuccesfull section(s) {', '.join(ingest_section_failed)}.")
            logging.error(f"❌ [INGEST] Failed to complete TikTok Ads ad creative ingestion due to unsuccesfull section(s) {', '.join(ingest_section_failed)}.")
            ingest_status_def = "failed"
        else:
            ingest_df_final = ingest_df_deduplicated
            print(f"🏆 [INGEST] Successfully completed TikTok Ads ad creative ingestion with {len(ingest_df_final)} row(s) in {elapsed}s.")
            logging.info(f"🏆 [INGEST] Successfully completed TikTok Ads ad creative ingestion with {len(ingest_df_final)} row(s) in {elapsed}s.")
            ingest_status_def = "success"
            return ingest_df_final
        return {"ingest_status_def": ingest_status_def, "ingest_seconds_elapsed": elapsed, "ingest_sections_failed": ingest_section_failed}

# 2. INGEST TIKTOK ADS INSIGHTS

# 2.1. Ingest campaign insights for TikToK Ads
def ingest_campaign_insights(start_date: str, end_date: str) -> pd.DataFrame:
    print(f"🚀 [INGEST] Starting to ingest TikTok Ads campaign insights from {start_date} to {end_date}...")
    logging.info(f"🚀 [INGEST] Starting to ingest TikTok Ads campaign insights from {start_date} to {end_date}...")

    # 2.1.1. Start timing the TikTok Ads campaign insights ingestion process
    start_time = time.time()
    ingest_section_succeeded = {}
    ingest_section_failed = [] 
    ingest_date_uploaded = []
    print(f"🔍 [INGEST] Proceeding to ingest TikTok Ads campaign insights from {start_date} to {end_date} at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest TikTok Ads campaign insights from {start_date} to {end_date} at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    
    # 2.1.2. Initialize Google BigQuery client
    try:
        print(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
        logging.info(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
        google_bigquery_client = bigquery.Client(project=PROJECT)
        print(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
        logging.info(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
        ingest_section_succeeded["2.1.2. Initialize Google BigQuery client"] = True
    except Exception as e:
        ingest_section_succeeded["2.1.2. Initialize Google BigQuery client"] = False
        ingest_section_failed.append("2.1.2. Initialize Google BigQuery client")
        print(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        logging.error(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        raise RuntimeError(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.") from e
        
    # 2.1.3. Loop through all date(s)
    ingest_date_list = pd.date_range(start=start_date, end=end_date).strftime("%Y-%m-%d").tolist()
    for ingest_date_separated in ingest_date_list:

    # 2.1.4. Trigger to fetch TikTok Ads campaign insights
        try:
            print(f"🔁 [INGEST] Triggering to fetch TikTok Ads campaign insights for {ingest_date_separated}...")
            logging.info(f"🔁 [INGEST] Triggering to fetch TikTok Ads campaign insights for {ingest_date_separated}...")
            ingest_df_fetched = fetch_campaign_insights(ingest_date_separated, ingest_date_separated)
            if ingest_df_fetched.empty:
                print("⚠️ [INGEST] Empty TikTok Ads campaign insights returned then ingestion is suspended.")
                logging.warning("⚠️ [INGEST] Empty TikTok Ads campaign insights returned then ingestion is suspended.")
                continue
            print(f"✅ [INGEST] Successfully fetched TikTok Ads campaign insights for {ingest_date_separated} with {len(ingest_df_fetched)} row(s).")
            logging.info(f"✅ [INGEST] Successfully fetched TikTok Ads campaign insights for {ingest_date_separated} with {len(ingest_df_fetched)} row(s).")
        except Exception as e:
            print(f"❌ [INGEST] Failed to trigger TikTok Ads campaign insights fetching for {ingest_date_separated} due to {e}.")
            logging.error(f"❌ [INGEST] Failed to trigger TikTok Ads campaign insights fetching for {ingest_date_separated} due to {e}.")
            continue

    # 2.1.5. Enrich TikTok Ads campaign insights
        try:
            print(f"🔁 [INGEST] Trigger to enrich TikTok Ads campaign insights for {ingest_date_separated} with {len(ingest_df_fetched)} row(s)...")
            logging.info(f"🔁 [INGEST] Trigger to enrich TikTok Ads campaign insights for {ingest_date_separated} with {len(ingest_df_fetched)} row(s)...")
            ingest_df_enriched = ingest_df_fetched.copy()
            ingest_df_enriched["date_range"] = f"{ingest_date_separated}_to_{ingest_date_separated}"
            ingest_df_enriched["last_updated_at"] = datetime.utcnow().replace(tzinfo=pytz.UTC)
        except Exception as e:
            print(f"❌ [INGEST] Failed to trigger enrichment TikTok Ads campaign insights for {ingest_date_separated} due to {e}.")
            logging.error(f"❌ [INGEST] Failed to trigger enrichment TikTok Ads campaign insights for {ingest_date_separated} due to {e}.")

    # 2.1.6. Enforce schema for TikTok Ads campaign insights
        try:
            print(f"🔁 [INGEST] Trigger to enforce schema for {len(ingest_df_enriched)} row(s) of TikTok Ads campaign insights...")
            logging.info(f"🔁 [INGEST] Trigger to enforce schema for {len(ingest_df_enriched)} row(s) of TikTok Ads campaign insights...")
            ingest_df_enforced = ensure_table_schema(ingest_df_enriched, schema_type="ingest_campaign_insights")
            ingest_df_enforced["date"] = pd.to_datetime(ingest_df_enforced["stat_time_day"])
            ingest_df_enforced["year"] = ingest_df_enforced["date"].dt.year
            ingest_df_enforced["month"] = ingest_df_enforced["date"].dt.month
            ingest_df_enforced["date_start"] = ingest_df_enforced["date"].dt.strftime("%Y-%m-%d")
            print(f"✅ [INGEST] Successfully enforced schema for {len(ingest_df_enriched)} row(s) of TikTok Ads campaign insights.")
            logging.info(f"✅ [INGEST] Successfully enforced schema for {len(ingest_df_enriched)} row(s) of TikTok Ads campaign insights.")
        except Exception as e:
            print(f"❌ [INGEST] Failed to enforce schema for TikTok Ads campaign insights due to {e}.")
            logging.error(f"❌ [INGEST] Failed to enforce schema for TikTok Ads campaign insights due to {e}.")
            continue

    # 2.1.7. Prepare table_id for TikTok Ads campaign insights ingestion
        first_date = pd.to_datetime(ingest_df_fetched["stat_time_day"].dropna().iloc[0])
        y, m = first_date.year, first_date.month
        raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
        table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_m{m:02d}{y}"
        print(f"🔍 [INGEST] Proceeding to ingest TikTok Ads campaign insights from {ingest_date_separated} to {ingest_date_separated} with Google BigQuery table {table_id}...")
        logging.info(f"🔍 [INGEST] Proceeding to ingest TikTok Ads campaign insights from {ingest_date_separated} to {ingest_date_separated} with Google BigQuery table {table_id}...")

    # 2.1.8. Delete existing row(s) or create new table if not exist
        try:
            ingest_df_deduplicated = ingest_df_enforced.drop_duplicates()          
            try:
                print(f"🔍 [INGEST] Checking TikTok Ads campaign insights table {table_id} existence...")
                logging.info(f"🔍 [INGEST] Checking TikTok Ads campaign insights table {table_id} existence...")
                google_bigquery_client.get_table(table_id)
                table_exists = True
            except Exception:
                table_exists = False
            if not table_exists:
                print(f"⚠️ [INGEST] TikTok Ads campaign insights table {table_id} not found then table creation will be proceeding...")
                logging.warning(f"⚠️ [INGEST] TikTok Ads campaign insights table {table_id} not found then table creation will be proceeding...")
                schema = []
                for col, dtype in ingest_df_deduplicated.dtypes.items():
                    if dtype.name.startswith("int"):
                        bq_type = "INT64"
                    elif dtype.name.startswith("float"):
                        bq_type = "FLOAT64"
                    elif dtype.name == "bool":
                        bq_type = "BOOL"
                    elif "datetime" in dtype.name:
                        bq_type = "TIMESTAMP"
                    else:
                        bq_type = "STRING"
                    schema.append(bigquery.SchemaField(col, bq_type))
                table = bigquery.Table(table_id, schema=schema)
                effective_partition = "date" if "date" in ingest_df_deduplicated.columns else None
                if effective_partition:
                    table.time_partitioning = bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field=effective_partition
                    )
                    print(f"🔍 [INGEST] Creating TikTok Ads campaign insights table {table_id} using partition on {effective_partition}...")
                    logging.info(f"🔍 [INGEST] Creating TikTok Ads campaign insights table {table_id} using partition on {effective_partition}...")
                table = google_bigquery_client.create_table(table)
                print(f"✅ [INGEST] Successfully created TikTok Ads campaign insights table {table_id} with partition on {effective_partition}.")
                logging.info(f"✅ [INGEST] Successfully created TikTok Ads campaign insights table {table_id} with partition on {effective_partition}.")
            else:
                new_dates = ingest_df_deduplicated["date_start"].dropna().unique().tolist()
                query_existing = f"SELECT DISTINCT date_start FROM `{table_id}`"
                existing_dates = [row.date_start for row in google_bigquery_client.query(query_existing).result()]
                overlap = set(new_dates) & set(existing_dates)
                if overlap:
                    print(f"⚠️ [INGEST] Found {len(overlap)} overlapping date(s) {overlap} in TikTok Ads campaign insights {table_id} table then deletion will be proceeding...")
                    logging.warning(f"⚠️ [INGEST] Found {len(overlap)} overlapping date(s) {overlap} in TikTok Ads campaign insights {table_id} table then deletion will be proceeding...")
                    for date_val in overlap:
                        query = f"""
                            DELETE FROM `{table_id}`
                            WHERE date_start = @date_value
                        """
                        job_config = bigquery.QueryJobConfig(
                            query_parameters=[bigquery.ScalarQueryParameter("date_value", "STRING", date_val)]
                        )
                        try:
                            result = google_bigquery_client.query(query, job_config=job_config).result()
                            deleted_rows = result.num_dml_affected_rows
                            print(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) for {date_val} in TikTok Ads campaign insights {table_id} table.")
                            logging.info(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) for {date_val} in TikTok Ads campaign insights {table_id} table.")
                        except Exception as e:
                            print(f"❌ [INGEST] Failed to delete existing rows in TikTok Ads campaign insights {table_id} table for {date_val} due to {e}.")
                            logging.error(f"❌ [INGEST] Failed to delete existing rows in TikTok ADs campaign insights {table_id} table for {date_val} due to {e}.")
                else:
                    print(f"✅ [INGEST] No overlapping dates found in TikTok Ads campaign insights {table_id} table then deletion is skipped.")
                    logging.info(f"✅ [INGEST] No overlapping dates found in TikTok Ads campaign insights {table_id} table then deletion is skipped.")
        except Exception as e:
            print(f"❌ [INGEST] Failed to create new table or delete existing row(s) of TikTok Ads campaign insights due to {e}.")
            logging.error(f"❌ [INGEST] Failed to create new table or delete existing row(s) of TikTok Ads campaign insights due to {e}.")
            continue

    # 2.1.9. Upload TikTok Ads campaign insights to Google BigQuery
        try:
            print(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} row(s) of TikTok Ads campaign insights to Google BigQuery table {table_id}...")
            logging.info(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} row(s) of TikTok ADs campaign insights to Google BigQuery table {table_id}...")
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                source_format=bigquery.SourceFormat.PARQUET,
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="date",
                ),
            )
            load_job = google_bigquery_client.load_table_from_dataframe(
                ingest_df_deduplicated,
                table_id,
                job_config=job_config
            )
            load_job.result()
            print(f"✅ [INGEST] Successfully uploaded {len(ingest_df_deduplicated)} row(s) of TikTok Ads campaign insights to Google BigQuery table {table_id}.")
            logging.info(f"✅ [INGEST] Successfully uploaded {len(ingest_df_deduplicated)} row(s) of TikTok Ads campaign insights to Google BigQuery table {table_id}.")
            ingest_date_uploaded.append(ingest_df_deduplicated.copy())
        except Exception as e:
            print(f"❌ [INGEST] Failed to upload {len(ingest_df_deduplicated)} row(s) of TikTok Ads campaign insights to Google BigQuery table {table_id} due to {e}.")
            logging.error(f"❌ [INGEST] Failed to upload {len(ingest_df_deduplicated)} row(s) of TikTok Ads campaign insights to Google BigQuery table {table_id} due to {e}.")
            continue

    # 2.1.10. Summarize ingestion result(s)
    ingest_df_final = pd.concat(ingest_date_uploaded, ignore_index=True) if ingest_date_uploaded else pd.DataFrame()
    elapsed = round(time.time() - start_time, 2)
    total_rows_uploaded = len(ingest_df_final)
    total_days_succeeded = len(ingest_date_uploaded)
    total_days_failed = len(ingest_date_list) - total_days_succeeded
    if total_days_succeeded == 0:
        print(f"❌ [INGEST] Failed to complete TikTok Ads campaign insights ingestion from {start_date} to {end_date} with all {total_days_failed} day(s) and 0 rows uploaded in {elapsed}s.")
        logging.error(f"❌ [INGEST] Failed to complete TikTok Ads campaign insights ingestion from {start_date} to {end_date} with all {total_days_failed} day(s) and 0 rows uploaded in {elapsed}s.")
        ingest_status_final = "failed_all"
    elif total_days_failed > 0:
        print(f"⚠️ [INGEST] Completed TikTok Ads campaign insights ingestion from {start_date} to {end_date} with partial failure {total_days_failed} day(s) failed and {total_rows_uploaded} row(s) uploaded in {elapsed}s.")
        logging.warning(f"⚠️ [INGEST] Completed TikTok Ads campaign insights ingestion from {start_date} to {end_date} with partial failure {total_days_failed} day(s) failed and {total_rows_uploaded} row(s) uploaded in {elapsed}s.")
        ingest_status_final = "partial_failed"
    else:
        print(f"🏆 [INGEST] Successfully completed TikTok Ads campaign insights ingestion from {start_date} to {end_date} with all {total_days_succeeded} day(s) and {total_rows_uploaded} row(s) uploaded in {elapsed}s.")
        logging.info(f"🏆 [INGEST] Successfully completed TikTok Ads campaign insights ingestion from {start_date} to {end_date} with all {total_days_succeeded} day(s) and {total_rows_uploaded} row(s) uploaded in {elapsed}s.")
        ingest_status_final = "success"
    return {
        "ingest_df_final": ingest_df_final,
        "ingest_status_final": ingest_status_final,
        "ingest_result_final": {
            "ingest_second_elapsed": elapsed,
            "ingest_row_uploaded": total_rows_uploaded,
            "ingest_day_succeeded": total_days_succeeded,
            "ingest_days_failed": total_days_failed,
        }
    }

# 2.2. Ingest ad insights for TikTok Ads
def ingest_ad_insights(start_date: str, end_date: str) -> pd.DataFrame:
    print(f"🚀 [INGEST] Starting to ingest TikTok Ads ad insights from {start_date} to {end_date}...")
    logging.info(f"🚀 [INGEST] Starting to ingest TikTok Ads ad insights from {start_date} to {end_date}...")

    # 2.2.1. Start timing the TikTok Ads ad insights ingestion process
    start_time = time.time()
    ingest_section_succeeded = {}
    ingest_section_failed = [] 
    ingest_date_uploaded = []
    print(f"🔍 [INGEST] Proceeding to ingest TikTok Ads ad insights from {start_date} to {end_date} at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest TikTok Ads ad insights from {start_date} to {end_date} at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    # 2.2.2. Initialize Google BigQuery client
    try:
        print(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
        logging.info(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
        google_bigquery_client = bigquery.Client(project=PROJECT)
        print(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
        logging.info(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
        ingest_section_succeeded["2.1.6. Initialize Google BigQuery client"] = True
    except Exception as e:
        ingest_section_succeeded["2.1.6. Initialize Google BigQuery client"] = False
        ingest_section_failed.append("2.1.6. Initialize Google BigQuery client")
        print(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        logging.error(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        raise RuntimeError(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.") from e

    # 2.2.3. Loop through all date(s)
    ingest_date_list = pd.date_range(start=start_date, end=end_date).strftime("%Y-%m-%d").tolist()
    for ingest_date_separated in ingest_date_list:

    # 2.2.4. Trigger to fetch ad insights for TikTok Ads
        try:
            print(f"🔁 [INGEST] Triggering to fetch TikTok Ads ad insights for {ingest_date_separated}...")
            logging.info(f"🔁 [INGEST] Triggering to fetch TikTok Ads ad insights for {ingest_date_separated}...")
            ingest_df_fetched = fetch_ad_insights(ingest_date_separated, ingest_date_separated)
            if ingest_df_fetched.empty:
                print("⚠️ [INGEST] Empty TikTok Ads ad insights returned then ingestion is suspended.")
                logging.warning("⚠️ [INGEST] Empty TikTok Ads ad insights returned then ingestion is suspended.")
                continue
            print(f"✅ [INGEST] Successfully fetched TikTok Ads ad insights for {ingest_date_separated} with {len(ingest_df_fetched)} row(s).")
            logging.info(f"✅ [INGEST] Successfully fetched TikTok Ads ad insights for {ingest_date_separated} with {len(ingest_df_fetched)} row(s).")
        except Exception as e:
            print(f"❌ [INGEST] Failed to trigger TikTok Ads ad insights fetching for {ingest_date_separated} due to {e}.")
            logging.error(f"❌ [INGEST] Failed to trigger TikTok Ads ad insights fetching for {ingest_date_separated} due to {e}.")
            continue

    # 2.2.5. Enrich TikTok Ads ad insights
        try:
            print(f"🔁 [INGEST] Trigger to enrich TikTok Ads ad insights for {ingest_date_separated} with {len(ingest_df_fetched)} row(s)...")
            logging.info(f"🔁 [INGEST] Trigger to enrich TikTok Ads ad insights from {start_date} to {end_date} with {len(ingest_df_fetched)} row(s)...")
            ingest_df_enriched = ingest_df_fetched.copy()
            ingest_df_enriched["date_range"] = f"{ingest_date_separated}_to_{ingest_date_separated}"
            ingest_df_enriched["last_updated_at"] = datetime.utcnow().replace(tzinfo=pytz.UTC)
        except Exception as e:
            print(f"❌ [INGEST] Failed to trigger enrichment TikTok Ads ad insights for {ingest_date_separated} due to {e}.")
            logging.error(f"❌ [INGEST] Failed to trigger enrichment TikTok Ads ad insights for {ingest_date_separated} due to {e}.")
            continue

    # 2.2.6. Enforce schema for TikTok Ads ad insights
        try:
            print(f"🔁 [INGEST] Trigger to enforce schema for {len(ingest_df_enriched)} row(s) of TikTok Ads ad insights...")
            logging.info(f"🔁 [INGEST] Trigger to enforce schema for {len(ingest_df_enriched)} row(s) of TikTok Ads ad insights...")
            ingest_df_enforced = ensure_table_schema(ingest_df_enriched, schema_type="ingest_ad_insights")
            ingest_df_enforced["date"] = pd.to_datetime(ingest_df_enforced["stat_time_day"])
            ingest_df_enforced["year"] = ingest_df_enforced["date"].dt.year
            ingest_df_enforced["month"] = ingest_df_enforced["date"].dt.month
            ingest_df_enforced["date_start"] = ingest_df_enforced["date"].dt.strftime("%Y-%m-%d")
            print(f"✅ [INGEST] Successfully enforced schema for {len(ingest_df_enriched)} row(s) of TikTok Ads ad insights.")
            logging.info(f"✅ [INGEST] Successfully enforced schema for {len(ingest_df_enriched)} row(s) of TikTok Ads ad insights.")
        except Exception as e:
            print(f"❌ [INGEST] Failed to enforce schema for TikTok Ads ad insights due to {e}.")
            logging.error(f"❌ [INGEST] Failed to enforce schema for TikTok Ads ad insights due to {e}.")
            raise RuntimeError(f"❌ [INGEST] Failed to enforce schema for TikTok Ads ad insights due to {e}.")

    # 2.2.7. Prepare table_id for TikTok Ads ad insights ingestion process
        first_date = pd.to_datetime(ingest_df_fetched["stat_time_day"].dropna().iloc[0])
        y, m = first_date.year, first_date.month
        raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
        table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_ad_m{m:02d}{y}"
        print(f"🔍 [INGEST] Proceeding to ingest TikTok Ads ad insights from {start_date} to {end_date} with Google BigQuery table {table_id}...")
        logging.info(f"🔍 [INGEST] Proceeding to ingest TikTok Ads ad insights from {start_date} to {end_date} with Google BigQuery table {table_id}...")

    # 2.2.8. Delete existing row(s) or create new table if not exist
        try:
            ingest_df_deduplicated = ingest_df_enforced.drop_duplicates()
            try:
                print(f"🔍 [INGEST] Checking TikTok Ads ad insights table {table_id} existence...")
                logging.info(f"🔍 [INGEST] Checking TikTok Ads ad insights table {table_id} existence...")
                google_bigquery_client.get_table(table_id)
                table_exists = True
            except Exception:
                table_exists = False
            if not table_exists:
                print(f"⚠️ [INGEST] TikTok Ads ad insights table {table_id} not found then table creation will be proceeding...")
                logging.warning(f"⚠️ [INGEST] TikTok Ads ad insights table {table_id} not found then table creation will be proceeding...")
                schema = []
                for col, dtype in ingest_df_deduplicated.dtypes.items():
                    if dtype.name.startswith("int"):
                        bq_type = "INT64"
                    elif dtype.name.startswith("float"):
                        bq_type = "FLOAT64"
                    elif dtype.name == "bool":
                        bq_type = "BOOL"
                    elif "datetime" in dtype.name:
                        bq_type = "TIMESTAMP"
                    else:
                        bq_type = "STRING"
                    schema.append(bigquery.SchemaField(col, bq_type))
                table = bigquery.Table(table_id, schema=schema)
                effective_partition = "date" if "date" in ingest_df_deduplicated.columns else None
                if effective_partition:
                    table.time_partitioning = bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field=effective_partition
                    )
                    print(f"🔍 [INGEST] Creating TikTok Ads ad insights table {table_id} using partition on {effective_partition}...")
                    logging.info(f"🔍 [INGEST] Creating TikTok Ads ad insights table {table_id} using partition on {effective_partition}...")
                table = google_bigquery_client.create_table(table)
                print(f"✅ [INGEST] Successfully created TikTok Ads ad insights table {table_id} with partition on {effective_partition}.")
                logging.info(f"✅ [INGEST] Successfully created TikTok Ads ad insights table {table_id} with partition on {effective_partition}.")
            else:
                new_dates = ingest_df_deduplicated["date_start"].dropna().unique().tolist()
                query_existing = f"SELECT DISTINCT date_start FROM `{table_id}`"
                existing_dates = [row.date_start for row in google_bigquery_client.query(query_existing).result()]
                overlap = set(new_dates) & set(existing_dates)
                if overlap:
                    print(f"⚠️ [INGEST] Found {len(overlap)} overlapping date(s) {overlap} in TikTok Ads ad insights table {table_id} then deletion will be proceeding...")
                    logging.warning(f"⚠️ [INGEST] Found {len(overlap)} overlapping date(s) {overlap} in TikTok Ads ad insights table {table_id} then deletion will be proceeding...")
                    for date_val in overlap:
                        query = f"""
                            DELETE FROM `{table_id}`
                            WHERE date_start = @date_value
                        """
                        job_config = bigquery.QueryJobConfig(
                            query_parameters=[bigquery.ScalarQueryParameter("date_value", "STRING", date_val)]
                        )
                        try:
                            result = google_bigquery_client.query(query, job_config=job_config).result()
                            deleted_rows = result.num_dml_affected_rows
                            print(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) for {date_val} in TikTok Ads ad insights table {table_id}.")
                            logging.info(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) for {date_val} in TikTok Ads ad insights table {table_id}.")
                        except Exception as e:
                            print(f"❌ [INGEST] Failed to delete existing rows in TikTok Ads ad insights table {table_id} for {date_val} due to {e}.")
                            logging.error(f"❌ [INGEST] Failed to delete existing rows in TikTok Ads ad insights table {table_id} for {date_val} due to {e}.")
                else:
                    print(f"✅ [INGEST] No overlapping dates found in TikTok Ads ad insights table {table_id} then deletion is skipped.")
                    logging.info(f"✅ [INGEST] No overlapping dates found in TikTok Ads ad insights table {table_id} then deletion is skipped.")
        except Exception as e:
            print(f"❌ [INGEST] Failed to create new table or delete existing row(s) of TikTok Ads ad insights due to {e}.")
            logging.error(f"❌ [INGEST] Failed to create new table or delete existing row(s) of TikTok Ads ad insights due to {e}.")
            continue

    # 2.2.9. Upload TikTok Ads ad insights to Google BigQuery
        try:
            print(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} row(s) of TikTok Ads ad insights to Google BigQuery table {table_id}...")
            logging.info(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} row(s) of TikTok Ads ad insights to Google BigQuery table {table_id}...")
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                source_format=bigquery.SourceFormat.PARQUET,
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="date",
                ),
            )
            load_job = google_bigquery_client.load_table_from_dataframe(
                ingest_df_deduplicated,
                table_id,
                job_config=job_config
            )
            load_job.result()
            ingest_date_uploaded.append(ingest_df_deduplicated.copy())
            print(f"✅ [INGEST] Successfully uploaded {len(ingest_df_deduplicated)} row(s) of TikTok Ads ad insights to Google BigQuery table {table_id}.")
            logging.info(f"✅ [INGEST] Successfully uploaded {len(ingest_df_deduplicated)} row(s) of TikTok Ads ad insights to Google BigQuery table {table_id}.")
        except Exception as e:
            print(f"❌ [INGEST] Failed to upload {len(ingest_df_deduplicated)} row(s) of TikTok Ads ad insights to Google BigQuery table {table_id} due to {e}.")
            logging.error(f"❌ [INGEST] Failed to upload {len(ingest_df_deduplicated)} row(s) of TikTok Ads ad insights to Google BigQuery table {table_id} due to {e}.")
            continue

    # 2.2.10. Summarize ingestion result(s)
    ingest_df_final = pd.concat(ingest_date_uploaded, ignore_index=True) if ingest_date_uploaded else pd.DataFrame()
    elapsed = round(time.time() - start_time, 2)
    total_rows_uploaded = len(ingest_df_final)
    total_days_succeeded = len(ingest_date_uploaded)
    total_days_failed = len(ingest_date_list) - total_days_succeeded
    if total_days_succeeded == 0:
        print(f"❌ [INGEST] Failed to complete TikTok Ads ad insights ingestion from {start_date} to {end_date} with all {total_days_failed} day(s) and 0 rows uploaded in {elapsed}s.")
        logging.error(f"❌ [INGEST] Failed to complete TikTok Ads ad insights ingestion from {start_date} to {end_date} with all {total_days_failed} day(s) and 0 rows uploaded in {elapsed}s.")
        ingest_status_final = "failed_all"
    elif total_days_failed > 0:
        print(f"⚠️ [INGEST] Completed TikTok Ads ad insights ingestion from {start_date} to {end_date} with partial failure {total_days_failed} day(s) failed and {total_rows_uploaded} row(s) uploaded in {elapsed}s.")
        logging.warning(f"⚠️ [INGEST] Completed TikTok Ads ad insights ingestion from {start_date} to {end_date} with partial failure {total_days_failed} day(s) failed and {total_rows_uploaded} row(s) uploaded in {elapsed}s.")
        ingest_status_final = "partial_failed"
    else:
        print(f"🏆 [INGEST] Successfully completed TikTok Ads ad insights ingestion from {start_date} to {end_date} with all {total_days_succeeded} day(s) and {total_rows_uploaded} row(s) uploaded in {elapsed}s.")
        logging.info(f"🏆 [INGEST] Successfully completed TikTok Ads ad insights ingestion from {start_date} to {end_date} with all {total_days_succeeded} day(s) and {total_rows_uploaded} row(s) uploaded in {elapsed}s.")
        ingest_status_final = "success"
    return {
        "ingest_df_final": ingest_df_final,
        "ingest_status_final": ingest_status_final,
        "ingest_result_final": {
            "ingest_second_elapsed": elapsed,
            "ingest_row_uploaded": total_rows_uploaded,
            "ingest_day_succeeded": total_days_succeeded,
            "ingest_days_failed": total_days_failed,
        }
    }