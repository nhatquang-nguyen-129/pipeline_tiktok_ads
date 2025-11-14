"""
==================================================================
TIKTOK INGESTION MODULE
------------------------------------------------------------------
This module ingests raw data from the TikTok Ads fetching module 
into Google BigQuery, establishing the foundational raw layer used 
for centralized storage and historical retention.

It manages the complete ingestion flow — from authentication and 
data fetching, to enrichment, schema validation, and loading into 
BigQuery tables segmented by campaign, ad, creative and metadata.

✔️ Supports append or truncate via configurable `write_disposition`  
✔️ Applies schema validation through centralized schema utilities  
✔️ Includes logging and CSV-based error tracking for traceability  
✔️ Handles multiple ingestion included campaign, ad, creative
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

# Add Google API core modules for integration
from google.api_core.exceptions import NotFound

# Add Python Pandas libraries for integration
import pandas as pd

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
from src.schema import enforce_table_schema

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
def ingest_campaign_metadata(ingest_ids_campaign: list) -> pd.DataFrame:
    print(f"🚀 [INGEST] Starting to ingest raw TikTok Ads campaign metadata for {len(ingest_ids_campaign)} campaign_id(s)...")
    logging.info(f"🚀 [INGEST] Starting to ingest raw TikTok Ads campaign metadata for {len(ingest_ids_campaign)} campaign_id(s)...")

    # 1.1.1. Start timing TikTok Ads campaign metadata ingestion
    ingest_time_start = time.time()
    ingest_sections_status = {}
    ingest_sections_time = {}
    print(f"🔍 [INGEST] Proceeding to ingest raw TiokTok Ads campaign metadata at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest raw TikTok Ads campaign metadata at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    # 1.1.2. Validate input for TikTok Ads campaign metadata ingestion
    ingest_section_name = "[INGEST] Validate input for TikTok Ads campaign metadata ingestion"
    ingest_section_start = time.time()
    try:
        if not ingest_ids_campaign:
            ingest_sections_status[ingest_section_name] = "failed"
            print("⚠️ [INGEST] Empty TikTok Ads ingest_ids_campaign provided then ingestion is suspended.")
            logging.warning("⚠️ [INGEST] Empty TikTok Ads ingest_ids_campaign provided then ingestion is suspended.")
        else:
            ingest_sections_status[ingest_section_name] = "succeed"
            print(f"✅ [INGEST] Successfully validated input for {len(ingest_ids_campaign)} campaign_id(s) of raw TikTok Ads campaign metadata ingestion.")
            logging.info(f"✅ [INGEST] Successfully validated input for {len(ingest_ids_campaign)} campaign_id(s) of raw TikTok Ads campaign metadata ingestion.")
    finally:
        ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    try:

    # 1.1.3. Trigger to fetch TikTok Ads campaign metadata
        ingest_section_name = "[INGEST] Trigger to fetch TikTok Ads campaign metadata"
        ingest_section_start = time.time()
        try:
            print(f"🔁 [INGEST] Triggering to fetch TikTok Ads campaign metadata for {len(ingest_ids_campaign)} campaign_id(s)...")
            logging.info(f"🔁 [INGEST] Triggering to fetch TikTok Ads campaign metadata for {len(ingest_ids_campaign)} campaign_id(s)...")
            ingest_results_fetched = fetch_campaign_metadata(fetch_ids_campaign=ingest_ids_campaign)
            ingest_df_fetched = ingest_results_fetched["fetch_df_final"]
            ingest_status_fetched = ingest_results_fetched["fetch_status_final"]
            ingest_summary_fetched = ingest_results_fetched["fetch_summary_final"]
            if ingest_status_fetched == "fetch_succeed_all":
                print(f"✅ [INGEST] Successfully triggered TikTok Ads campaign metadata fetching for {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                logging.info(f"✅ [INGEST] Successfully triggered TikTok Ads campaign metadata fetching for {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                ingest_sections_status[ingest_section_name] = "succeed"
            elif ingest_status_fetched == "fetch_succeed_partial":
                print(f"⚠️ [INGEST] Partially triggered TikTok Ads campaign metadata fetching {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                logging.warning(f"⚠️ [INGEST] Partially triggered TikTok Ads campaign metadata fetching {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                ingest_sections_status[ingest_section_name] = "partial"
            else:
                ingest_sections_status[ingest_section_name] = "failed"
                print(f"❌ [INGEST] Failed to trigger TikTok Ads campaign metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) due to {', '.join(ingest_summary_fetched['fetch_sections_failed'])} or unknown error in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                logging.error(f"❌ [INGEST] Failed to trigger TikTok Ads campaign metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) due to {', '.join(ingest_summary_fetched['fetch_sections_failed'])} or unknown error in {ingest_summary_fetched['fetch_time_elapsed']}s.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.1.4. Prepare Google BigQuery table_id for ingestion
        ingest_section_name = "[INGEST] Prepare Google BigQuery table_id for ingestion"
        ingest_section_start = time.time()
        try:
            raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
            raw_table_campaign = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_metadata"
            print(f"🔍 [INGEST] Preparing to ingest TikTok Ads campaign metadata for {len(ingest_df_fetched)} fetched row(s) to Google BigQuery table_id {raw_table_campaign}...")
            logging.info(f"🔍 [INGEST] Preparing to ingest TikTok Ads campaign metadata for {len(ingest_df_fetched)} fetched row(s) to Google BigQuery table_id {raw_table_campaign}...")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.1.5. Trigger to enforce schema for TikTok Ads campaign metadata
        ingest_section_name = "[INGEST] Trigger to enforce schema for TikTok Ads campaign metadata"
        ingest_section_start = time.time()
        try:
            print(f"🔄 [INGEST] Triggering to enforce schema for TikTok Ads campaign metadata with {len(ingest_df_fetched)} row(s)...")
            logging.info(f"🔄 [INGEST] Triggering to enforce schema for TikTok Ads campaign metadata with {len(ingest_df_fetched)} row(s)...")
            ingest_results_enforced = enforce_table_schema(ingest_df_fetched, "ingest_campaign_metadata")
            ingest_summary_enforced = ingest_results_enforced["schema_summary_final"]
            ingest_status_enforced = ingest_results_enforced["schema_status_final"]
            ingest_df_enforced = ingest_results_enforced["schema_df_final"]    
            if ingest_status_enforced == "schema_succeed_all":
                print(f"✅ [INGEST] Successfully triggered TikTok Ads campaign metadata schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{len(ingest_df_fetched)} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
                logging.info(f"✅ [INGEST] Successfully triggered TikTok Ads campaign metadata schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{len(ingest_df_fetched)} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
                ingest_sections_status[ingest_section_name] = "succeed"
            else:
                ingest_sections_status[ingest_section_name] = "failed"
                print(f"❌ [INGEST] Failed to trigger TikTok Ads campaign metadata schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{len(ingest_df_fetched)} enforced row(s) due to failed section(s) {', '.join(ingest_summary_enforced['schema_sections_failed'])} in {ingest_summary_enforced['schema_time_elapsed']}s.")
                logging.error(f"❌ [INGEST] Failed to trigger TikTok Ads campaign metadata schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{len(ingest_df_fetched)} enforced row(s) due to failed section(s) {', '.join(ingest_summary_enforced['schema_sections_failed'])} in {ingest_summary_enforced['schema_time_elapsed']}s.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.1.6. Initialize Google BigQuery client
        ingest_section_name = "[INGEST] Initialize Google BigQuery client"
        ingest_section_start = time.time()
        try:
            print(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            google_bigquery_client = bigquery.Client(project=PROJECT)
            print(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            logging.info(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            ingest_sections_status[ingest_section_name] = "succeed"
        except Exception as e:
            ingest_sections_status[ingest_section_name] = "failed"
            print(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
            logging.error(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.1.7. Delete existing row(s) or create new table if it not exist
        ingest_section_name = "[INGEST] Delete existing row(s) or create new table if it not exist"
        ingest_section_start = time.time()
        try:
            ingest_df_deduplicated = ingest_df_enforced.drop_duplicates()           
            table_clusters_defined = ["account_id", "campaign_id"]
            table_clusters_filtered = []
            table_schemas_defined = []
            try:
                print(f"🔍 [INGEST] Checking TikTok Ads campaign metadata table {raw_table_campaign} existence...")
                logging.info(f"🔍 [INGEST] Checking TikTok Ads campaign metadata table {raw_table_campaign} existence...")
                google_bigquery_client.get_table(raw_table_campaign)
                ingest_table_existed = True
            except NotFound:
                ingest_table_existed = False
            except Exception as e:
                print(f"❌ [INGEST] Failed to check TikTok Ads campaign metadata table {raw_table_campaign} existence due to {e}.")
                logging.error(f"❌ [INGEST] Failed to check TikTok Ads campaign metadata table {raw_table_campaign} existence due to {e}.")
                raise RuntimeError(f"❌ [INGEST] Failed to check TikTok Ads campaign metadata table {raw_table_campaign} existence due to {e}.") from e
            if not ingest_table_existed:
                print(f"⚠️ [INGEST] TikTok Ads campaign metadata table {raw_table_campaign} not found then table creation will be proceeding...")
                logging.info(f"⚠️ [INGEST] TikTok Ads campaign metadata table {raw_table_campaign} not found then table creation will be proceeding...")
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
                    table_schemas_defined.append(bigquery.SchemaField(col, bq_type))
                table_configuration_defined = bigquery.Table(raw_table_campaign, schema=table_schemas_defined)
                table_partition_effective = "date" if "date" in ingest_df_deduplicated.columns else None
                if table_partition_effective:
                    table_configuration_defined.time_partitioning = bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field=table_partition_effective
                    )
                table_clusters_filtered = [f for f in table_clusters_defined if f in ingest_df_deduplicated.columns]
                if table_clusters_filtered:  
                    table_configuration_defined.clustering_fields = table_clusters_filtered  
                try:    
                    print(f"🔍 [INGEST] Creating TikTok Ads campaign metadata table defined name {raw_table_campaign} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}...")
                    logging.info(f"🔍 [INGEST] Creating TikTok Ads campaign metadata table defined name {raw_table_campaign} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}...")
                    table_metadata_defined = google_bigquery_client.create_table(table_configuration_defined)
                    print(f"✅ [INGEST] Successfully created TikTok Ads campaign metadata table actual name {table_metadata_defined.full_table_id} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}.")
                    logging.info(f"✅ [INGEST] Successfully created TikTok Ads campaign metadata table actual name {table_metadata_defined.full_table_id} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}.")
                except Exception as e:
                    print(f"❌ [INGEST] Failed to create TikTok Ads campaign metadata table {raw_table_campaign} due to {e}.")
                    logging.error(f"❌ [INGEST] Failed to create TikTok Ads campaign metadata table {raw_table_campaign} due to {e}.")
                    raise RuntimeError(f"❌ [INGEST] Failed to create TikTok Ads campaign metadata table {raw_table_campaign} due to {e}.") from e
            else:
                print(f"🔄 [INGEST] Found TikTok Ads campaign metadata table {raw_table_campaign} then existing row(s) deletion will be proceeding...")
                logging.info(f"🔄 [INGEST] Found TikTok Ads campaign metadata table {raw_table_campaign} then existing row(s) deletion will be proceeding...")
                unique_keys = ingest_df_deduplicated[["campaign_id", "account_id"]].dropna().drop_duplicates()
                if not unique_keys.empty:
                    temp_table_id = f"{PROJECT}.{raw_dataset}.temp_table_campaign_metadata_delete_keys_{uuid.uuid4().hex[:8]}"
                    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
                    google_bigquery_client.load_table_from_dataframe(unique_keys, temp_table_id, job_config=job_config).result()
                    join_condition = " AND ".join([
                        f"CAST(main.{col} AS STRING) = CAST(temp.{col} AS STRING)"
                        for col in ["campaign_id", "account_id"]
                    ])
                    delete_query = f"""
                        DELETE FROM `{raw_table_campaign}` AS main
                        WHERE EXISTS (
                            SELECT 1 FROM `{temp_table_id}` AS temp
                            WHERE {join_condition}
                        )
                    """
                    result = google_bigquery_client.query(delete_query).result()
                    google_bigquery_client.delete_table(temp_table_id, not_found_ok=True)
                    deleted_rows = result.num_dml_affected_rows
                    print(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of TikTok Ads campaign metadata table {raw_table_campaign}.")
                    logging.info(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of TikTok Ads campaign metadata table {raw_table_campaign}.")
                else:
                    print(f"⚠️ [INGEST] No unique campaign_id and account_id keys found in TikTok Ads campaign metadata table {raw_table_campaign} then existing row(s) deletion is skipped.")
                    logging.warning(f"⚠️ [INGEST] No unique campaign_id and account_id keys found in TikTok Ads campaign metadata table {raw_table_campaign} then existing row(s) deletion is skipped.")
            ingest_sections_status[ingest_section_name] = "succeed"
        except Exception as e:
            ingest_sections_status[ingest_section_name] = "failed"
            print(f"❌ [INGEST] Failed to delete existing row(s) or create new table {raw_table_campaign} if it not exist for TikTok Ads campaign metadata due to {e}.")
            logging.error(f"❌ [INGEST] Failed to delete existing row(s) or create new table {raw_table_campaign} if it not exist for TikTok Ads campaign metadata due to {e}.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.1.8. Upload TikTok Ads campaign metadata to Google BigQuery
        ingest_section_name = "[INGEST] Upload TikTok Ads campaign metadata to Google BigQuery"
        ingest_section_start = time.time()
        try:
            print(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} row(s) of TikTok Ads campaign metadata to Google BigQuery table {raw_table_campaign}...")
            logging.info(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} row(s) of TikTok Ads campaign metadata to Google BigQuery table {raw_table_campaign}...")
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            google_bigquery_client.load_table_from_dataframe(ingest_df_deduplicated, raw_table_campaign, job_config=job_config).result()
            ingest_df_uploaded = ingest_df_deduplicated.copy()
            print(f"✅ [INGEST] Successfully uploaded {len(ingest_df_uploaded)} row(s) of TikTok Ads campaign metadata to Google BigQuery table {raw_table_campaign}.")
            logging.info(f"✅ [INGEST] Successfully uploaded {len(ingest_df_uploaded)} row(s) of TikTok Ads campaign metadata to Google BigQuery table {raw_table_campaign}.")
            ingest_sections_status[ingest_section_name] = "succeed"
        except Exception as e:
            ingest_sections_status[ingest_section_name] = "failed"
            print(f"❌ [INGEST] Failed to upload TikTok Ads campaign metadata to Google BigQuery table {raw_table_campaign} due to {e}.")
            logging.error(f"❌ [INGEST] Failed to upload TikTok Ads campaign metadata to Google BigQuery table {raw_table_campaign} due to {e}.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.1.9. Summarize ingestion result(s) for TikTok Ads campaign metadata
    finally:
        ingest_time_elapsed = round(time.time() - ingest_time_start, 2)
        ingest_df_final = (ingest_df_uploaded.copy() if "ingest_df_uploaded" in locals() and not ingest_df_uploaded.empty else pd.DataFrame())
        ingest_sections_total = len(ingest_sections_status) 
        ingest_sections_failed = [k for k, v in ingest_sections_status.items() if v == "failed"] 
        ingest_sections_succeeded = [k for k, v in ingest_sections_status.items() if v == "succeed"]
        ingest_rows_input = len(ingest_ids_campaign)
        ingest_rows_output = len(ingest_df_final)
        ingest_sections_summary = list(dict.fromkeys(
            list(ingest_sections_status.keys()) +
            list(ingest_sections_time.keys())
        ))
        ingest_sections_detail = {
            ingest_section_summary: {
                "status": ingest_sections_status.get(ingest_section_summary, "unknown"),
                "time": ingest_sections_time.get(ingest_section_summary, None),
            }
            for ingest_section_summary in ingest_sections_summary
        }     
        if ingest_sections_failed:
            print(f"❌ [INGEST] Failed to complete Facebook Ads campaign metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) due to {', '.join(ingest_sections_failed)} failed section(s) in {ingest_time_elapsed}s.")
            logging.error(f"❌ [INGEST] Failed to complete Facebook Ads campaign metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) due to {', '.join(ingest_sections_failed)} failed section(s) in {ingest_time_elapsed}s.")
            ingest_status_final = "ingest_failed_all"
        elif ingest_rows_input > 0 and ingest_rows_output < ingest_rows_input:
            print(f"⚠️ [INGEST] Partially completed Facebook Ads campaign metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")
            logging.warning(f"⚠️ [INGEST] Partially completed Facebook Ads campaign metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")
            ingest_status_final = "ingest_succeed_partial"
        else:
            print(f"🏆 [INGEST] Successfully completed Facebook Ads campaign metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")
            logging.info(f"🏆 [INGEST] Successfully completed Facebook Ads campaign metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")
            ingest_status_final = "ingest_succeed_all"
        ingest_results_final = {
            "ingest_df_final": ingest_df_final,
            "ingest_status_final": ingest_status_final,
            "ingest_summary_final": {
                "ingest_time_elapsed": ingest_time_elapsed, 
                "ingest_sections_total": ingest_sections_total,
                "ingest_sections_succeed": ingest_sections_succeeded, 
                "ingest_sections_failed": ingest_sections_failed, 
                "ingest_sections_detail": ingest_sections_detail, 
                "ingest_rows_input": ingest_rows_input, 
                "ingest_rows_output": ingest_rows_output
            },
        }
    return ingest_results_final

# 1.2. Ingest ad metadata for TikTok Ads
def ingest_ad_metadata(ingest_ids_ad: list) -> pd.DataFrame:
    print(f"🚀 [INGEST] Starting to ingest raw TikTok Ads ad metadata for {len(ingest_ids_ad)} ad_id(s)...")
    logging.info(f"🚀 [INGEST] Starting to ingest raw TikTok Ads ad metadata for {len(ingest_ids_ad)} ad_id(s)...")

    # 1.2.1. Start timing TikTok Ads ad metadata ingestion
    ingest_time_start = time.time()
    ingest_sections_status = {}
    ingest_sections_time = {}
    print(f"🔍 [INGEST] Proceeding to ingest TikTok Ads ad metadata at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest TikTok Ads ad metadata at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    # 1.2.2. Validate input for TikTok Ads ad metadata ingestion
    ingest_section_name = "[INGEST] Validate input for TikTok Ads ad metadata ingestion"
    ingest_section_start = time.time()
    try:
        if not ingest_ids_ad:
            ingest_sections_status[ingest_section_name] = "failed"
            print("⚠️ [INGEST] Empty TikTok Ads ad_id_list provided then ingestion is suspended.")
            logging.warning("⚠️ [INGEST] Empty TikTok Ads ad_id_list provided then ingestion is suspended.")
        else:
            ingest_sections_status[ingest_section_name] = "succeed"
            print(f"✅ [INGEST] Successfully validated input for {len(ingest_ids_ad)} ad_id(s) of TikTok Ads ad metadata ingestion.")
            logging.info(f"✅ [INGEST] Successfully validated input for {len(ingest_ids_ad)} ad_id(s) of TikTok Ads ad metadata ingestion.")
    finally:
        ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.2.3. Trigger to fetch TikTok Ads ad metadata
        ingest_section_name = "[INGEST] Trigger to fetch TikTok Ads ad metadata"
        ingest_section_start = time.time()
        try:
            print(f"🔁 [INGEST] Triggering to fetch TikTok Ads ad metadata for {len(ingest_ids_ad)} ad_id(s)...")
            logging.info(f"🔁 [INGEST] Triggering to fetch TikTok Ads ad metadata for {len(ingest_ids_ad)} ad_id(s)...")
            ingest_results_fetched = fetch_ad_metadata(fetch_ids_ad=ingest_ids_ad)
            ingest_df_fetched = ingest_results_fetched["fetch_df_final"]
            ingest_status_fetched = ingest_results_fetched["fetch_status_final"]
            ingest_summary_fetched = ingest_results_fetched["fetch_summary_final"]
            if ingest_status_fetched == "fetch_succeed_all":
                print(f"✅ [INGEST] Successfully triggered TikTok Ads ad metadata fetching for {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                logging.info(f"✅ [INGEST] Successfully triggered TikTok Ads ad metadata fetching for {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                ingest_sections_status[ingest_section_name] = "succeed"
            elif ingest_status_fetched == "fetch_succeed_partial":
                print(f"⚠️ [INGEST] Partially triggered TikTok Ads ad metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                logging.warning(f"⚠️ [INGEST] Partially triggered TikTok Ads ad metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                ingest_sections_status[ingest_section_name] = "partial"
            else:
                ingest_sections_status[ingest_section_name] = "failed"
                print(f"❌ [INGEST] Failed to trigger TikTok Ads ad metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) due to {', '.join(ingest_summary_fetched['fetch_sections_failed'])} or unknown error in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                logging.error(f"❌ [INGEST] Failed to trigger TikTok Ads ad metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) due to {', '.join(ingest_summary_fetched['fetch_sections_failed'])} or unknown error in {ingest_summary_fetched['fetch_time_elapsed']}s.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2) 

    # 1.2.3. Prepare table_id for TikTok Ads ad metadata ingestion
    raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
    raw_table_ad = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_ad_metadata"
    print(f"🔍 [INGEST] Proceeding to ingest TikTok ad metadata for {len(ad_id_list)} ad_id(s) with Google BigQuery table {raw_table_ad}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest TikTok ad metadata for {len(ad_id_list)} ad_id(s) with Google BigQuery table {raw_table_ad}...")

    # 1.2.4. Trigger to fetch TikTok Ads ad metadata
    try:
        print(f"🔁 [INGEST] Triggering to fetch TikTok Ads ad metadata for {len(ad_id_list)} ad_id(s)...")
        logging.info(f"🔁 [INGEST] Triggering to fetch TikTok Ads ad metadata for {len(ad_id_list)} ad_id(s)...")
        ingest_df_fetched = fetch_ad_metadata(ad_id_list=ad_id_list)
        if ingest_df_fetched.empty:
            print("⚠️ [INGEST] Empty TikTok Ads ad metadata returned then ingestion is suspended.")
            logging.warning("⚠️ [INGEST] Empty TikTok Ads ad metadata returned then ingestion is suspended.")
            raise ValueError("⚠️ [INGEST] Empty TikTok Ads ad metadata returned then ingestion is suspended.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to trigger TikTok Ads ad metadata fetching due to {e}.")
        logging.error(f"❌ [INGEST] Failed to trigger TikTok Ads ad metadata fetching due to {e}.")
        raise RuntimeError(f"❌ [INGEST] Failed to trigger TikTok Ads ad metadata fetching due to {e}.")

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
        ingest_sections_status["1.2.6. Initialize Google BigQuery client"] = "succeed"
    except Exception as e:
        ingest_sections_status["1.2.6. Initialize Google BigQuery client"] = "failed"
        print(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        logging.error(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        raise RuntimeError(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.") from e

    # 1.2.7. Delete existing row(s) or create new table if it not exist
    try:
        ingest_df_deduplicated = ingest_df_enforced.drop_duplicates()
        try:
            print(f"🔍 [INGEST] Checking raw TikTok Ads ad metadata table {raw_table_ad} existence...")
            logging.info(f"🔍 [INGEST] Checking raw TikTok Ads ad metadata table {raw_table_ad} existence...")
            google_bigquery_client.get_table(raw_table_ad)
            table_exists = True
        except Exception:
            table_exists = False
        if not table_exists:
            print(f"⚠️ [INGEST] Raw TikTok Ads ad metadata table {raw_table_ad} not found then table creation will be proceeding...")
            logging.info(f"⚠️ [INGEST] Raw TikTok Ads ad metadata table {raw_table_ad} not found then table creation will be proceeding...")
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
            table = bigquery.Table(raw_table_ad, schema=schema)
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
                print(f"🔍 [INGEST] Creating raw TikTok Ads ad metadata table {raw_table_ad} using clustering on {filtered_clusters} field(s) and partition on {effective_partition}...")
                logging.info(f"🔍 [INGEST] Creating raw TikTok Ads ad metadata table {raw_table_ad} using clustering on {filtered_clusters} field(s) and partition on {effective_partition}...")
            table = google_bigquery_client.create_table(table)
            print(f"✅ [INGEST] Successfully created raw TikTok Ads ad metadata table {raw_table_ad} with clustering on {filtered_clusters} field(s) and partition on {effective_partition}.")
            logging.info(f"✅ [INGEST] Successfully raw created TikTok Ads ad metadata table {raw_table_ad} with clustering on {filtered_clusters} field(s) and partition on {effective_partition}.")
        else:
            print(f"🔄 [INGEST] Found raw TikTok Ads ad metadata table {raw_table_ad} then existing row(s) deletion will be proceeding...")
            logging.info(f"🔄 [INGEST] Found raw TikTok Ads ad metadata table {raw_table_ad} then existing row(s) deletion will be proceeding...")
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
                    DELETE FROM `{raw_table_ad}` AS main
                    WHERE EXISTS (
                        SELECT 1 FROM `{temp_table_id}` AS temp
                        WHERE {join_condition}
                    )
                """
                result = google_bigquery_client.query(delete_query).result()
                google_bigquery_client.delete_table(temp_table_id, not_found_ok=True)
                deleted_rows = result.num_dml_affected_rows
                print(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of raw TikTok Ads ad metadata table {raw_table_ad}.")
                logging.info(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of raw TikTok Ads ad metadata table {raw_table_ad}.")
            else:
                print(f"⚠️ [INGEST] No unique ad_id and advertiser_id keys found in TikTok ad metadata table {raw_table_ad} then existing row(s) deletion is skipped.")
                logging.warning(f"⚠️ [INGEST] No unique ad_id and advertiser_id keys found in TikTok ad metadata table {raw_table_ad} then existing row(s) deletion is skipped.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to create new table or delete existing row(s) of TikTok Ads ad metadata due to {e}.")
        logging.error(f"❌ [INGEST] Failed to create new table or delete existing row(s) of TikTok Ads ad metadata due to {e}.")
        raise RuntimeError(f"❌ [INGEST] Failed to create new table or delete existing row(s) of TikTok Ads ad metadata due to {e}.")

    # 1.2.8. Upload TikTok Ads ad metadata to Google BigQuery
    try:
        print(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} row(s) of raw TikTok Ads ad metadata to Google BigQuery table {raw_table_ad}...")
        logging.info(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} row(s) of raw TikTok Ads ad metadata to Google BigQuery table {raw_table_ad}...")
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        google_bigquery_client.load_table_from_dataframe(ingest_df_deduplicated, raw_table_ad, job_config=job_config).result()
        ingest_df_uploaded = ingest_df_deduplicated.copy()
        print(f"✅ [INGEST] Successfully uploaded {len(ingest_df_uploaded)} row(s) of raw TikTok Ads ad metadata to Google BigQuery table {raw_table_ad}.")
        logging.info(f"✅ [INGEST] Successfully uploaded {len(ingest_df_uploaded)} row(s) of raw TikTok Ads ad metadata to Google BigQuery table {raw_table_ad}.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to upload TikTok Ads ad metadata due to {e}.")
        logging.error(f"❌ [INGEST] Failed to upload TikTok Ads ad metadata due to {e}.")
        raise
    
    # 1.2.9. Summarize ingestion result(s)
    finally:
        ingest_time_elapsed = round(time.time() - start_time, 2)
        ingest_df_final = ingest_df_uploaded.copy() if "ingest_df_uploaded" in locals() else pd.DataFrame()
        ingest_rows_input = len(ad_id_list)
        ingest_rows_output = len(ingest_df_final)
        ingest_rows_failed = ingest_rows_input - ingest_rows_output
        ingest_sections_failed = sum(status == "failed" for status in ingest_sections_status.values())
        if ingest_sections_failed > 0:
            print(f"❌ [INGEST] Failed to complete TikTok Ads ad metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested ad_id(s) in {ingest_time_elapsed}s due to failed section(s).")
            logging.error(f"❌ [INGEST] Failed to complete TikTok Ads ad metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested ad_id(s) in {ingest_time_elapsed}s due to failed section(s).")
            ingest_status_final = "ingest_failed_all"
        elif ingest_rows_output < ingest_rows_input:
            print(f"⚠️ [INGEST] Conplete TikTok Ads ad metadata ingestion with partial failure of {ingest_rows_failed}/{ingest_rows_input} ingested ad_id(s) and {len(ingest_df_uploaded)} row(s) uploaded to Google BigQuery table {raw_table_ad} in {ingest_time_elapsed}s.")
            logging.error(f"⚠️ [INGEST] Conplete TikTok Ads ad metadata ingestion with partial failure of {ingest_rows_failed}/{ingest_rows_input} ingested ad_id(s) and {len(ingest_df_uploaded)} row(s) uploaded to Google BigQuery table {raw_table_ad} in {ingest_time_elapsed}s.")
            ingest_status_final = "ingest_failed_partial"
        else:
            print(f"🏆 [INGEST] Successfully completed TikTok Ads ad metadata ingestion with all {ingest_rows_output}/{ingest_rows_input} ingested ad_id(s) and {len(ingest_df_uploaded)} row(s) uploaded to Google BigQuery table {raw_table_ad} in {ingest_time_elapsed}s.")
            logging.info(f"🏆 [INGEST] Successfully completed TikTok Ads ad metadata ingestion with all {ingest_rows_output}/{ingest_rows_input} ingested ad_id(s) and {len(ingest_df_uploaded)} row(s) uploaded to Google BigQuery table {raw_table_ad} in {ingest_time_elapsed}s.")            
        return {
            "ingest_df_final": ingest_df_final,
            "ingest_status_final": ingest_status_final,
            "ingest_summary_final": {
                "ingest_time_elapsed": ingest_time_elapsed,
                "ingest_rows_output": ingest_rows_output,
                "ingest_rows_input": ingest_rows_input,
                "ingest_section_status": ingest_sections_status,
            }
        }

# 1.3. Ingest ad creative for TikTok Ads
def ingest_ad_creative() -> pd.DataFrame:
    print(f"🚀 [INGEST] Starting to ingest raw TikTok Ads ad creative ...")
    logging.info(f"🚀 [INGEST] Starting to ingest raw TikTok Ads ad creative...")

    # 1.3.1. Start timing the TikTok Ads ad creative ingestion process
    start_time = time.time()
    ingest_sections_status = {}
    print(f"🔍 [INGEST] Proceeding to ingest raw TikTok Ads ad creative at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest raw TikTok Ads ad creative {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    # 1.3.2. Prepare table_id for TikTok Ads ad creative ingestion
    raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
    raw_table_creative = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_ad_creative"
    print(f"🔍 [INGEST] Proceeding to ingest TikTok Ads ad creative with Google BigQuery table {raw_table_creative}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest TikTok Ads ad creative with Google BigQuery table {raw_table_creative}...")

    # 1.3.3. Trigger to fetch TikTok Ads ad creative
    try:
        print(f"🔁 [INGEST] Triggering to fetch TikTok Ads ad creative...")
        logging.info(f"🔁 [INGEST] Triggering to fetch TikTok Ads ad creative...")
        ingest_df_fetched = fetch_ad_creative()
        if ingest_df_fetched.empty:
            print("⚠️ [INGEST] Empty TikTok Ads ad creative returned then ingestion is suspended.")
            logging.warning("⚠️ [INGEST] Empty TikTok Ads ad creative returned then ingestion is suspended.")
            raise ValueError("⚠️ [INGEST] Empty TikTok Ads ad creative returned then ingestion is suspended.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to trigger TikTok Ads ad creative fetching due to {e}.")
        logging.error(f"❌ [INGEST] Failed to trigger TikTok Ads ad creative fetching due to {e}.")
        raise RuntimeError(f"❌ [INGEST] Failed to trigger TikTok Ads ad creative fetching due to {e}.") from e

    # 1.3.4 Enforce schema for TikTok Ads ad creative
    try:
        print(f"🔄 [INGEST] Triggering to enforce schema for {len(ingest_df_fetched)} row(s) of TikTok Ads ad creative...")
        logging.info(f"🔄 [INGEST] Triggering to enforce schema for {len(ingest_df_fetched)} row(s) of TikTok Ads ad creative...")
        ingest_df_enforced = ensure_table_schema(ingest_df_fetched, "ingest_ad_creative")
    except Exception as e:
        print(f"❌ [INGEST] Failed to trigger schema enforcement for TikTok Ads ad creative due to {e}.")
        logging.error(f"❌ [INGEST] Failed to trigger schema enforcement for TikTok Ads ad creative due to {e}.")
        raise RuntimeError(f"❌ [INGEST] Failed to trigger schema enforcement for TikTok Ads ad creative due to {e}.")

    # 1.3.5. Initialize Google BigQuery client
    try:
        print(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
        logging.info(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
        google_bigquery_client = bigquery.Client(project=PROJECT)
        print(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
        logging.info(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
        ingest_sections_status["1.3.5. Initialize Google BigQuery client"] = "succeed"
    except Exception as e:
        ingest_sections_status["1.3.5. Initialize Google BigQuery client"] = "failed"
        print(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        logging.error(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        raise RuntimeError(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.") from e

    # 1.3.6. Delete existing row(s) or create new table if it not exist
    try:
        ingest_df_deduplicated = ingest_df_enforced.drop_duplicates()                   
        try:
            print(f"🔍 [INGEST] Checking raw TikTok Ads ad creative table {raw_table_creative} existence...")
            logging.info(f"🔍 [INGEST] Checking raw TikTok ADs ad creative table {raw_table_creative} existence...")
            google_bigquery_client.get_table(raw_table_creative)
            table_exists = True
        except Exception:
            table_exists = False
        if not table_exists:
            print(f"⚠️ [INGEST] Raw TikTok Ads ad creative table {raw_table_creative} not found then table creation will be proceeding...")
            logging.info(f"⚠️ [INGEST] Raw TikTok Ads ad creative table {raw_table_creative} not found then table creation will be proceeding...")
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
            table = bigquery.Table(raw_table_creative, schema=schema)
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
                print(f"🔍 [INGEST] Creating raw TikTok Ads ad creative table {raw_table_creative} using clustering on {filtered_clusters} field(s) and partition on {effective_partition})...")
                logging.info(f" [INGEST] Creating raw TikTok Ads ad creative table {raw_table_creative} using clustering on {filtered_clusters} field(s) and partition on {effective_partition}...")
            table = google_bigquery_client.create_table(table)
            print(f"✅ [INGEST] Successfully created raw TikTok Ads ad creative table {raw_table_creative} with clustering on {filtered_clusters} field(s) and partition on {effective_partition}.")
            logging.info(f"✅ [INGEST] Successfully created raw TikTok Ads ad creative table {raw_table_creative} with clustering on {filtered_clusters} field(s) and partition on {effective_partition}.")
        else:
            print(f"🔄 [INGEST] Found raw TikTok Ads ad creative table {raw_table_creative} then existing row(s) deletion will be proceeding...")
            logging.info(f"🔄 [INGEST] Found raw TikTok Ads ad creative table {raw_table_creative} then existing row(s) deletion will be proceeding...")
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
                    DELETE FROM `{raw_table_creative}` AS main
                    WHERE EXISTS (
                        SELECT 1 FROM `{temp_table_id}` AS temp
                        WHERE {join_condition}
                    )
                """
                result = google_bigquery_client.query(delete_query).result()
                google_bigquery_client.delete_table(temp_table_id, not_found_ok=True)
                deleted_rows = result.num_dml_affected_rows
                print(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of raw TikTok Ads ad creative table {raw_table_creative}.")
                logging.info(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of raw TikTok Ads ad creative table {raw_table_creative}.")
            else:
                print(f"⚠️ [INGEST] No unique video_id and advertisier_id keys found in raw TikTok Ads ad creative table {raw_table_creative} then existing row(s) deletion is skipped.")
                logging.warning(f"⚠️ [INGEST] No unique video_id and advertiser_id keys found in raw TikTok Ads ad creative table {raw_table_creative} then existing row(s) deletion is skipped.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to create new table or delete existing row(s) of raw TikTok Ads ad creative ingestion due to {e}.")
        logging.error(f"❌ [INGEST] Failed to create new table or delete existing row(s) of raw TikTok Ads ad creative ingestion due to {e}.")
        raise RuntimeError(f"❌ [INGEST] Failed to create new table or delete existing row(s) of raw TikTok Ads ad creative ingestion due to {e}.")

    # 1.3.7. Upload TikTok Ads ad creative to Google BigQuery
    try:
        print(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} row(s) of raw TikTok Ads ad creative to Google BigQuery table {raw_table_creative}...")
        logging.info(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} row(s) of raw TikTok Ads ad creative to Google BigQuery table {raw_table_creative}...")
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        google_bigquery_client.load_table_from_dataframe(ingest_df_deduplicated, raw_table_creative, job_config=job_config).result()
        ingest_df_uploaded = ingest_df_deduplicated.copy()
        print(f"✅ [INGEST] Successfully uploaded {len(ingest_df_uploaded)} row(s) of raw TikTok Ads ad creative to Google BigQuery table {raw_table_creative}.")
        logging.info(f"✅ [INGEST] Successfully uploaded {len(ingest_df_uploaded)} row(s) of raw TikTok Ads ad creative to Google BigQuery table {raw_table_creative}.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to upload raw TikTok Ads campaign metadata to Google BigQuery due to {e}.")
        logging.error(f"❌ [INGEST] Failed to upload raw TikTok Ads campaign metadata to Google BigQuery due to {e}.")
        raise RuntimeError(f"❌ [INGEST] Failed to upload raw TikTok Ads campaign metadata to Google BigQuery due to {e}.")

    # 1.3.88. Summarize ingestion result(s)
    finally:
        ingest_time_elapsed = round(time.time() - start_time, 2)
        ingest_df_final = ingest_df_uploaded.copy() if "ingest_df_uploaded" in locals() else pd.DataFrame()
        ingest_rows_output = len(ingest_df_final)
        ingest_sections_failed = sum(status == "failed" for status in ingest_sections_status.values())
        if ingest_sections_failed > 0:
            print(f"❌ [INGEST] Failed to complete TikTok Ads campaign metadata ingestion with {ingest_rows_output} ingested video_id(s) in {ingest_time_elapsed}s due to failed section(s).")
            logging.error(f"❌ [INGEST] Failed to complete TikTok Ads campaign metadata ingestion with {ingest_rows_output} ingested video_id(s) in {ingest_time_elapsed}s due to failed section(s).")
            ingest_status_final = "ingest_failed_all"
        else:
            print(f"🏆 [INGEST] Successfully completed TikTok Ads campaign metadata ingestion with {ingest_rows_output} ingested video_id(s) and {len(ingest_df_uploaded)} row(s) uploaded to Google BigQuery table {raw_table_creative} in {ingest_time_elapsed}s.")
            logging.info(f"🏆 [INGEST] Successfully completed TikTok Ads campaign metadata ingestion with {ingest_rows_output} ingested video_id(s) and {len(ingest_df_uploaded)} row(s) uploaded to Google BigQuery table {raw_table_creative} in {ingest_time_elapsed}s.")            
        return {
            "ingest_df_final": ingest_df_final,
            "ingest_status_final": ingest_status_final,
            "ingest_summary_final": {
                "ingest_time_elapsed": ingest_time_elapsed,
                "ingest_rows_output": ingest_rows_output,
                "ingest_section_status": ingest_sections_status,
            }
        }

# 2. INGEST TIKTOK ADS INSIGHTS

# 2.1. Ingest campaign insights for TikToK Ads
def ingest_campaign_insights(start_date: str, end_date: str) -> pd.DataFrame:
    print(f"🚀 [INGEST] Starting to ingest raw TikTok Ads campaign insights from {start_date} to {end_date}...")
    logging.info(f"🚀 [INGEST] Starting to ingest raw TikTok Ads campaign insights from {start_date} to {end_date}...")

    # 2.1.1. Start timing the TikTok Ads campaign insights ingestion process
    start_time = time.time()
    ingest_sections_status = {}
    ingest_days_uploaded = []
    print(f"🔍 [INGEST] Proceeding to ingest raw TikTok Ads campaign insights from {start_date} to {end_date} at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest raw TikTok Ads campaign insights from {start_date} to {end_date} at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    
    # 2.1.2. Initialize Google BigQuery client
    try:
        print(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
        logging.info(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
        google_bigquery_client = bigquery.Client(project=PROJECT)
        print(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
        logging.info(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
        ingest_sections_status["2.1.2. Initialize Google BigQuery client"] = "succeed"
    except Exception as e:
        ingest_sections_status["2.1.2. Initialize Google BigQuery client"] = "failed"
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
            ingest_days_uploaded.append(ingest_df_deduplicated.copy())
        except Exception as e:
            print(f"❌ [INGEST] Failed to upload {len(ingest_df_deduplicated)} row(s) of TikTok Ads campaign insights to Google BigQuery table {table_id} due to {e}.")
            logging.error(f"❌ [INGEST] Failed to upload {len(ingest_df_deduplicated)} row(s) of TikTok Ads campaign insights to Google BigQuery table {table_id} due to {e}.")
            continue

    # 2.1.10. Summarize ingestion result(s)
    elapsed = round(time.time() - start_time, 2)
    ingest_df_final = pd.concat(ingest_days_uploaded, ignore_index=True) if ingest_days_uploaded else pd.DataFrame()
    ingest_days_failed = [d for d in ingest_date_list if d not in [x['date'] for x in ingest_days_uploaded]]
    total_rows_uploaded = len(ingest_df_final)
    if len(ingest_days_uploaded) == 0:
        print(f"❌ [INGEST] Failed to complete TikTok Ads campaign insights ingestion from {start_date} to {end_date} with all {len(ingest_days_failed)} day(s) and 0 rows uploaded in {elapsed}s.")
        logging.error(f"❌ [INGEST] Failed to complete TikTok Ads campaign insights ingestion from {start_date} to {end_date} with all {len(ingest_days_failed)} day(s) and 0 rows uploaded in {elapsed}s.")
        ingest_status_final = "failed_all"
    elif ingest_days_failed > 0:
        print(f"⚠️ [INGEST] Completed TikTok Ads campaign insights ingestion from {start_date} to {end_date} with partial failure of {len(ingest_days_failed)} day(s) and {total_rows_uploaded} row(s) uploaded in {elapsed}s.")
        logging.warning(f"⚠️ [INGEST] Completed TikTok Ads campaign insights ingestion from {start_date} to {end_date} with partial failure of {len(ingest_days_failed)} day(s) and {total_rows_uploaded} row(s) uploaded in {elapsed}s.")
        ingest_status_final = "partial_failed"
    else:
        print(f"🏆 [INGEST] Successfully completed TikTok Ads campaign insights ingestion from {start_date} to {end_date} with all {len(ingest_days_uploaded)} day(s) and {total_rows_uploaded} row(s) uploaded in {elapsed}s.")
        logging.info(f"🏆 [INGEST] Successfully completed TikTok Ads campaign insights ingestion from {start_date} to {end_date} with all {len(ingest_days_uploaded)} day(s) and {total_rows_uploaded} row(s) uploaded in {elapsed}s.")
        ingest_status_final = "success"
    return {
        "ingest_df_final": ingest_df_final,
        "ingest_status_final": ingest_status_final,
        "ingest_summary_final": {
            "ingest_second_elapsed": elapsed,
            "ingest_rows_uploaded": f"{len(ingest_df_final)}",
            "ingest_day_succeeded": f"{len(ingest_days_uploaded)}",
            "ingest_days_failed": f"{len(ingest_days_failed)}",
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
        "ingest_summary_final": {
            "ingest_second_elapsed": elapsed,
            "ingest_row_uploaded": total_rows_uploaded,
            "ingest_day_succeeded": total_days_succeeded,
            "ingest_days_failed": total_days_failed,
        }
    }