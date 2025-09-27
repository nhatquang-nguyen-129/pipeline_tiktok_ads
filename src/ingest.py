"""
==================================================================
TIKTOK INGESTION MODULE
------------------------------------------------------------------
This module ingests raw data from the TikTok Marketing API into 
Google BigQuery, forming the raw data layer of the Ads Data Pipeline.

It orchestrates the full ingestion flow: from authenticating the SDK, 
to fetching data, enriching it, validating schema, and loading into 
BigQuery tables organized by campaign, ad, creative and metadata.

✔️ Supports append or truncate via configurable `write_disposition`  
✔️ Applies schema validation through centralized schema utilities  
✔️ Includes logging and CSV-based error tracking for traceability

⚠️ This module is strictly limited to *raw-layer ingestion*.  
It does **not** handle data transformation, modeling, or aggregation.
==================================================================
"""
# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add datetime utilities for integration
from datetime import datetime

# Add timezone ultilities for integration
import pytz

# Add JSON ultilities for integration
import json 

# Add logging ultilities forintegration
import logging

# Add UUID ultilities for integration
import uuid

# Add Python Pandas libraries for integration
import pandas as pd

# Add Google Authentication libraries for integration
from google.api_core.exceptions import (
    Forbidden,
    GoogleAPICallError
)
from google.auth.exceptions import DefaultCredentialsError

# Add Google Cloud modules for integration
from google.cloud import bigquery

# Add internal TikTok modules for handling
from src.fetch import (
    fetch_campaign_metadata,
    fetch_adset_metadata,
    fetch_ad_metadata,
    fetch_ad_creative,
    fetch_campaign_insights,
    fetch_ad_insights
)
from config.schema import ensure_table_schema

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
    print("🚀 [INGEST] Starting to ingest TikTok Ads campaign metadata...")
    logging.info("🚀 [INGEST] Starting to ingest TikTok Ads campaign metadata....")

    # 1.1.1. Validate input for TikTok Ads campaign metadata ingestion
    if not campaign_id_list:
        print("⚠️ [INGEST] Empty TikTok Ads campaign_id_list provided then ingestion is suspended.")
        logging.warning("⚠️ [INGEST] Empty TikTok Ads campaign_id_list provided then ingestion is suspended.")
        raise ValueError("⚠️ [INGEST] Empty TikTok Ads campaign_id_list provided then ingestion is suspended.")

    # 1.1.2. Trigger to fetch TikTok Ads campaign metadata
    try:
        print(f"🔁 [INGEST] Triggering to fetch TikTok Ads campaign metadata for {len(campaign_id_list)} campaign_id(s)...")
        logging.info(f"🔍🔁 [INGEST] Triggering to fetch TikTok Ads campaign metadata for {len(campaign_id_list)} campaign_id(s)...")
        df = fetch_campaign_metadata(campaign_id_list=campaign_id_list)
        if df.empty:
            print("⚠️ [INGEST] Empty TikTok Ads campaign metadata returned.")
            logging.warning("⚠️ [INGEST] Empty TikTok Ads campaign metadata returned.")
            return pd.DataFrame()
    except Exception as e:
        print(f"❌ [INGEST] Failed to trigger TikTok Ads campaign metadata fetch due to {e}.")
        logging.error(f"❌ [INGEST] Failed to trigger TikTok Ads campaign metadata fetch due to {e}.")
        return pd.DataFrame()

    # 1.1.3. Prepare table_id for TikTok Ads campaign metadata
    raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
    table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_metadata"
    print(f"🔍 [INGEST] Proceeding to ingest TikTok Ads campaign metadata for {len(campaign_id_list)} campaign_id(s) with table_id {table_id}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest TikTok Ads campaign metadata for {len(campaign_id_list)} campaign_id(s) with table_id {table_id}...")

    # 1.1.4. Enforce schema for TikTok Ads campaign metadata
    try:
        print(f"🔄 [INGEST] Triggering to enforce schema for {len(df)} row(s) of TikTok Ads campaign metadata...")
        logging.info(f"🔄 [INGEST] Triggering to enforce schema for {len(df)} row(s) of TikTok Ads campaign metadata...")
        df = ensure_table_schema(df, "ingest_campaign_metadata")
    except Exception as e:
        print(f"❌ [INGEST] Failed to trigger schema enforcement for TikTok Ads campaign metadata due to {e}.")
        logging.error(f"❌ [INGEST] Failed to trigger schema enforcement for TikTok Ads campaign metadata due to {e}.")
        return df   

    # 1.1.5. Delete existing row(s) or create new table if not exist
    try:
        print(f"🔍 [INGEST] Checking TikTok Ads campaign metadata table {table_id} existence...")
        logging.info(f"🔍 [INGEST] Checking TikTok Ads campaign metadata table {table_id} existence...")
        df = df.drop_duplicates()
        
        try:
            print(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            google_bigquery_client = bigquery.Client(project=PROJECT)
            print(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            logging.info(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
        except DefaultCredentialsError as e:
            raise RuntimeError("❌ [INGEST] Failed to initialize Google BigQuery client due to credentials error.") from e
        except Forbidden as e:
            raise RuntimeError("❌ [INGEST] Failed to initialize Google BigQuery client due to permission denial.") from e
        except GoogleAPICallError as e:
            raise RuntimeError("❌ [INGEST] Failed to initialize Google BigQuery client due to API call error.") from e
        except Exception as e:
            raise RuntimeError(f"❌ [INGEST] Failed to initialize Google BigQuery client due to {e}.") from e

        try:
            google_bigquery_client.get_table(table_id)
            table_exists = True
        except Exception:
            table_exists = False

        if not table_exists:
            print(f"⚠️ [INGEST] TikTok Ads campaign metadata table {table_id} not found then table creation will be proceeding...")
            logging.info(f"⚠️ [INGEST] TikTok Ads campaign metadata table {table_id} not found then table creation will be proceeding...")
            schema = []
            for col, dtype in df.dtypes.items():
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
            effective_partition = "date" if "date" in df.columns else None
            if effective_partition:
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=effective_partition
                )
            clustering_fields = ["campaign_id", "advertiser_id"]
            filtered_clusters = [f for f in clustering_fields if f in df.columns]
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
            unique_keys = df[["campaign_id", "advertiser_id"]].dropna().drop_duplicates()
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
                print(f"⚠️ [INGEST] No unique (campaign_id and advertiser_id) keys found in TikTok Ads campaign metadata table {table_id} then deletion is skipped.")
                logging.warning(f"⚠️ [INGEST] No unique (campaign_id and advertiser_id) keys found in TikTok Ads campaign metadata table {table_id} then deletion is skipped.")
    except Exception as e:
        print(f"❌ [INGEST] Failed during TikTok Ads campaign metadata ingestion due to {e}.")
        logging.error(f"❌ [INGEST] Failed during TikTok Ads campaign metadata ingestion due to {e}.")
        raise

    # 1.1.6. Upload TikTok Ads campaign metadata to Google BigQuery
    try:
        print(f"🔍 [INGEST] Uploading {len(df)} row(s) of TikTok Ads campaign metadata {table_id}...")
        logging.info(f"🔍 [INGEST] Uploading {len(df)} row(s) of TikTok Ads campaign metadata {table_id}...")
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        google_bigquery_client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
        print(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of TikTok Ads campaign metadata table {table_id}.")
        logging.info(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of TikTok Ads campaign metadata table {table_id}.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to upload TikTok Ads campaign metadata due to {e}.")
        logging.error(f"❌ [INGEST] Failed to upload TikTok Ads campaign metadata due to {e}.")
        raise

    return df

# 1.2. Ingest adset metadata for TikTok Ads
def ingest_adset_metadata(adset_id_list: list) -> pd.DataFrame:
    print("🚀 [INGEST] Starting to ingest TikTok Ads adgroup metadata...")
    logging.info("🚀 [INGEST] Starting to ingest TikTok Ads adgroup metadata...")

    # 1.2.1. Validate input for TikTok Ads adset metadata ingestion
    if not adset_id_list:
        print("⚠️ [INGEST] Empty TikTok Ads adgroup_id_list provided then ingestion is suspended.")
        logging.warning("⚠️ [INGEST] Empty TikTok Ads adgroup_id_list provided then ingestion is suspended.")
        raise ValueError("⚠️ [INGEST] Empty TikTok Ads adgroup_id_list provided then ingestion is suspended.")

    # 1.2.2. Trigger to fetch TikTok Ads adset metadata
    try:
        print(f"🔁 [INGEST] Triggering to fetch TikTok Ads adgroup metadata for {len(adset_id_list)} adset_id(s)...")
        logging.info(f"🔁 [INGEST] Triggering to fetch TikTok Ads adgroup metadata for {len(adset_id_list)} adset_id(s)...")
        df = fetch_adset_metadata(adset_id_list=adset_id_list)
        if df.empty:
            print("⚠️ [INGEST] Empty TikTok Ads adgroup metadata returned.")
            logging.warning("⚠️ [INGEST] Empty TikTok Ads adgroup metadata returned.")
            return pd.DataFrame()
    except Exception as e:
        print(f"❌ [INGEST] Failed to trigger TikTok Ads adgroup metadata fetch due to {e}.")
        logging.error(f"❌ [INGEST] Failed to trigger TikTok Ads adgroup metadata fetch due to {e}.")
        return pd.DataFrame()

    # 1.2.3. Prepare full table_id for TikTok Ads adset metadata ingestion
    raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
    table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_adset_metadata"
    print(f"🔍 [INGEST] Proceeding to ingest TikTok Ads adgroup metadata for {len(adset_id_list)} adgroup_id(s) with table_id {table_id}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest TikTok Ads adgroup metadata for {len(adset_id_list)} adgroup_id(s) with table_id {table_id}...")

    # 1.2.4. Enforce schema for TikTok adgroup metadata
    try:
        print(f"🔄 [INGEST] Trigger to enforce schema for {len(df)} row(s) of TikTok adgroup metadata...")
        logging.info(f"🔄 [INGEST] Enforcing schema for {len(df)} row(s) of TikTok adgroup metadata...")
        df = ensure_table_schema(df, "ingest_adgroup_metadata")
    except Exception as e:
        print(f"❌ [INGEST] Failed to trigger schema enforcement for TikTok adgroup metadata due to {e}.")
        logging.error(f"❌ [INGEST] Failed to trigger schema enforcement for TikTok adgroup metadata due to {e}.")
        return df

    # 1.2.5. Delete existing row(s) or create new table if it not exist
    try:
        print(f"🔍 [INGEST] Checking TikTok Ads adgroup metadata table {table_id} existence...")
        logging.info(f"🔍 [INGEST] Checking TikTok Ads adgroup metadata table {table_id} existence...")
        df = df.drop_duplicates()
        
        try:
            print(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            google_bigquery_client = bigquery.Client(project=PROJECT)
            print(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            logging.info(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
        except DefaultCredentialsError as e:
            raise RuntimeError("❌ [INGEST] Failed to initialize Google BigQuery client due to credentials error.") from e
        except Forbidden as e:
            raise RuntimeError("❌ [INGEST] Failed to initialize Google BigQuery client due to permission denial.") from e
        except GoogleAPICallError as e:
            raise RuntimeError("❌ [INGEST] Failed to initialize Google BigQuery client due to API call error.") from e
        except Exception as e:
            raise RuntimeError(f"❌ [INGEST] Failed to initialize Google BigQuery client due to {e}.") from e
        
        try:
            google_bigquery_client.get_table(table_id)
            table_exists = True
        except Exception:
            table_exists = False
        if not table_exists:
            print(f"⚠️ [INGEST] TikTok Ads adgroup metadata table {table_id} not found then table creation will be proceeding...")
            logging.info(f"⚠️ [INGEST] TikTok Ads adgroup metadata table {table_id} not found then table creation will be proceeding...")
            schema = []
            for col, dtype in df.dtypes.items():
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
            effective_partition = "date" if "date" in df.columns else None
            if effective_partition:
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=effective_partition
                )
            clustering_fields = ["adgroup_id", "advertiser_id"]
            filtered_clusters = [f for f in clustering_fields if f in df.columns]
            if filtered_clusters:
                table.clustering_fields = filtered_clusters
                print(f"🔍 [INGEST] Creating TikTok Ads adgroup metadata table {table_id} using clustering on {filtered_clusters} field(s) and partition on {effective_partition}...")
                logging.info(f"🔍 [INGEST] Creating TikTok Ads adgroup metadata table {table_id} using clustering on {filtered_clusters} field(s) and partition on {effective_partition}...")
            table = google_bigquery_client.create_table(table)
            print(f"✅ [INGEST] Successfully created TikTok Ads adgroup metadata table {table_id} with clustering on {filtered_clusters} field(s) and partition on {effective_partition}.")
            logging.info(f"✅ [INGEST] Successfully created TikTok Ads adgroup metadata table {table_id} with clustering on {filtered_clusters} field(s) and partition on {effective_partition}.")
        else:
            print(f"🔄 [INGEST] TikTok Ads adgroup metadata table {table_id} exists then existing row(s) deletion will be proceeding...")
            logging.info(f"🔄 [INGEST] TikTok Ads adgroup metadata table {table_id} exists then existing row(s) deletion will be proceeding...")
            unique_keys = df[["adgroup_id", "advertiser_id"]].dropna().drop_duplicates()
            if not unique_keys.empty:
                temp_table_id = f"{PROJECT}.{raw_dataset}.temp_table_adgroup_metadata_delete_keys_{uuid.uuid4().hex[:8]}"
                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
                google_bigquery_client.load_table_from_dataframe(unique_keys, temp_table_id, job_config=job_config).result()
                join_condition = " AND ".join([
                    f"CAST(main.{col} AS STRING) = CAST(temp.{col} AS STRING)"
                    for col in ["adgroup_id", "advertiser_id"]
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
                print(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of TikTok Ads adgroup metadata {table_id}.")
                logging.info(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of TikTok Ads adgroup metadata {table_id}.")
            else:
                print(f"⚠️ [INGEST] No unique (adgroup_id and advertiser_id) keys found in TikTok Ads adgroup metadata table {table_id} then existing row(s) deletion is skipped.")
                logging.warning(f"⚠️ [INGEST] No unique (adgroup_id and advertiser_id) keys found in TikTok Ads adgroup metadata table {table_id} then existing row(s) deletion is skipped.")
    except Exception as e:
        print(f"❌ [INGEST] Failed during TikTok Ads adgroup metadata ingestion due to {e}.")
        logging.error(f"❌ [INGEST] Failed during TikTok Ads adgroup metadata ingestion due to {e}.")
        raise

    # 1.2.6. Upload TikTok adgroup metadata to Google BigQuery
    try:
        print(f"🔍 [INGEST] Uploading {len(df)} row(s) of TikTok Ads adgroup metadata to {table_id}...")
        logging.info(f"🔍 [INGEST] Uploading {len(df)} row(s) of TikTok Ads adgroup metadata to {table_id}...")
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        google_bigquery_client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
        print(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of TikTok Ads adgroup metadata table {table_id}.")
        logging.info(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of TikTok Ads adgroup metadata table {table_id}.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to upload TikTok Ads adgroup metadata due to {e}.")
        logging.error(f"❌ [INGEST] Failed to upload TikTok Ads adgroup metadata due to {e}.")
        raise
    return df

# 1.3. Ingest ad metadata for TikTok Ads
def ingest_ad_metadata(ad_id_list: list) -> pd.DataFrame:
    print("🚀 [INGEST] Starting to ingest TikTok Ads ad metadata...")
    logging.info("🚀 [INGEST] Starting TikTok Ads ad metadata...")

    # 1.3.1. Validate input for TikTok Ads ad metadata ingestion
    if not ad_id_list:
        print("⚠️ [INGEST] Empty TikTok ad_id_list provided.")
        logging.warning("⚠️ [INGEST] Empty TikTok ad_id_list provided.")
        raise ValueError("⚠️ [INGEST] Empty TikTok ad_id_list provided.")

    # 1.3.2. Call TikTok API
    try:
        print(f"🔍 [INGEST] Trigger to fetch TikTok ad metadata for {len(ad_id_list)} ad_id(s)...")
        logging.info(f"🔍 [INGEST] Trigger to fetch TikTok ad metadata for {len(ad_id_list)} ad_id(s)...")
        df = fetch_ad_metadata(ad_id_list=ad_id_list)
        if df.empty:
            print("⚠️ [INGEST] Empty TikTok ad metadata returned.")
            logging.warning("⚠️ [INGEST] Empty TikTok ad metadata returned.")
            return pd.DataFrame()
    except Exception as e:
        print(f"❌ [INGEST] Failed to trigger TikTok ad metadata fetch due to {e}.")
        logging.error(f"❌ [INGEST] Failed to trigger TikTok ad metadata fetch due to {e}.")
        return pd.DataFrame()

    # 1.3.3. Prepare full table_id for raw layer in BigQuery
    raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
    table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_ad_metadata"
    print(f"🔍 [INGEST] Proceeding to ingest TikTok ad metadata for {len(ad_id_list)} ad_id(s) with table_id {table_id}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest TikTok ad metadata for {len(ad_id_list)} ad_id(s) with table_id {table_id}...")

    # 1.3.4. Enforce schema for TikTok ad metadata
    try:
        print(f"🔄 [INGEST] Trigger to enforce schema for {len(df)} row(s) of TikTok ad metadata...")
        logging.info(f"🔄 [INGEST] Enforcing schema for {len(df)} row(s) of TikTok ad metadata...")
        df = ensure_table_schema(df, "ingest_ad_metadata")
    except Exception as e:
        print(f"❌ [INGEST] Failed to trigger schema enforcement for TikTok ad metadata due to {e}.")
        logging.error(f"❌ [INGEST] Failed to trigger schema enforcement for TikTok ad metadata due to {e}.")
        return df

    # 1.3.5. Delete existing row(s) or create new table if it not exist
    try:
        print(f"🔍 [INGEST] Checking TikTok ad metadata table {table_id} existence...")
        logging.info(f"🔍 [INGEST] Checking TikTok ad metadata table {table_id} existence...")
        df = df.drop_duplicates()
        try:
            client = bigquery.Client(project=PROJECT)
        except DefaultCredentialsError as e:
            raise RuntimeError("❌ [INGEST] Failed to initialize Google BigQuery client due to credentials error.") from e
        try:
            client.get_table(table_id)
            table_exists = True
        except Exception:
            table_exists = False
        if not table_exists:
            print(f"⚠️ [INGEST] TikTok ad metadata table {table_id} not found then table creation will be proceeding...")
            logging.info(f"⚠️ [INGEST] TikTok ad metadata table {table_id} not found then table creation will be proceeding...")
            schema = []
            for col, dtype in df.dtypes.items():
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
            effective_partition = "date" if "date" in df.columns else None
            if effective_partition:
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=effective_partition
                )
            clustering_fields = ["ad_id", "advertiser_id"]
            filtered_clusters = [f for f in clustering_fields if f in df.columns]
            if filtered_clusters:
                table.clustering_fields = filtered_clusters
                print(f"🔍 [INGEST] Creating TikTok ad metadata table {table_id} using clustering on {filtered_clusters} field(s) and partition on {effective_partition}...")
                logging.info(f"🔍 [INGEST] Creating TikTok ad metadata table {table_id} using clustering on {filtered_clusters} field(s) and partition on {effective_partition}...")
            table = client.create_table(table)
            print(f"✅ [INGEST] Successfully created TikTok ad metadata table {table_id} with clustering on {filtered_clusters} field(s) and partition on {effective_partition}.")
            logging.info(f"✅ [INGEST] Successfully created TikTok ad metadata table {table_id} with clustering on {filtered_clusters} field(s) and partition on {effective_partition}.")
        else:
            print(f"🔄 [INGEST] TikTok ad metadata table {table_id} exists then existing row(s) deletion will be proceeding...")
            logging.info(f"🔄 [INGEST] TikTok ad metadata table {table_id} exists then existing row(s) deletion will be proceeding...")
            unique_keys = df[["ad_id", "advertiser_id"]].dropna().drop_duplicates()
            if not unique_keys.empty:
                temp_table_id = f"{PROJECT}.{raw_dataset}.temp_table_ad_metadata_delete_keys_{uuid.uuid4().hex[:8]}"
                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
                client.load_table_from_dataframe(unique_keys, temp_table_id, job_config=job_config).result()
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
                result = client.query(delete_query).result()
                client.delete_table(temp_table_id, not_found_ok=True)
                deleted_rows = result.num_dml_affected_rows
                print(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of TikTok ad metadata table {table_id}.")
                logging.info(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of TikTok ad metadata table {table_id}.")
            else:
                print(f"⚠️ [INGEST] No unique (ad_id and advertiser_id) keys found in TikTok ad metadata table {table_id} then existing row(s) deletion is skipped.")
                logging.warning(f"⚠️ [INGEST] No unique (ad_id and advertiser_id) keys found in TikTok ad metadata table {table_id} then existing row(s) deletion is skipped.")
    except Exception as e:
        print(f"❌ [INGEST] Failed during TikTok ad metadata ingestion due to {e}.")
        logging.error(f"❌ [INGEST] Failed during TikTok ad metadata ingestion due to {e}.")
        raise

    # 1.3.6. Upload TikTok Ads ad metadata to Google BigQuery
    try:
        print(f"🔍 [INGEST] Uploading {len(df)} row(s) of TikTok ad metadata to {table_id}...")
        logging.info(f"🔍 [INGEST] Uploading {len(df)} row(s) of TikTok ad metadata to {table_id}...")
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
        print(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of TikTok ad metadata table {table_id}.")
        logging.info(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of TikTok ad metadata table {table_id}.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to upload TikTok ad metadata due to {e}.")
        logging.error(f"❌ [INGEST] Failed to upload TikTok ad metadata due to {e}.")
        raise
    return df

# 1.4. Ingest ad creative for TikTok Ads
def ingest_ad_creative(ad_id_list: list) -> pd.DataFrame:
    print("🚀 [INGEST] Starting to ingest TikTok Ads ad creative...")
    logging.info("🚀 [INGEST] Starting to ingest TikTok Ads ad creative...")

    # 1.4.1. Validate input for TikTok Ads ad creative ingestion
    if not ad_id_list:
        print("⚠️ [INGEST] Empty TikTok Ads ad_id_list provided then ingestion is suspended.")
        logging.warning("⚠️ [INGEST] Empty TikTok Ads ad_id_list provided then ingestion is suspended.")
        raise ValueError("⚠️ [INGEST] Empty TikTok Ads ad_id_list provided then ingestion is suspended.")

    # 1.4.2. Trigger to fetch TikTok Ads ad creative
    try:
        print(f"🔁 [INGEST] Triggering to fetch TikTok Ads creative metadata for {len(ad_id_list)} ad_id(s)...")
        logging.info(f"🔁 [INGEST] Triggering to fetch TikTok Ads creative metadata for {len(ad_id_list)} ad_id(s)...")
        df = fetch_ad_creative(ad_id_list)
        if df.empty:
            print("⚠️ [INGEST] No TikTok Ads ad creative returned.")
            logging.warning("⚠️ [INGEST] No TikTok Ads ad creative returned.")
            return pd.DataFrame()
    except Exception as e:
        print(f"❌ [INGEST] Failed to trigger TikTok Ads ad creative fetch due to {e}.")
        logging.error(f"❌ [INGEST] Failed to trigger TikTok Ads ad creative fetch due to {e}.")
        return pd.DataFrame()

    # 1.4.3. Prepare table_id for TikTok Ads ad creative ingestion
    raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
    table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_creative_metadata"
    print(f"🔍 [INGEST] Proceeding to ingest TikTok Ads ad creative for {len(ad_id_list)} ad_id(s) with table_id {table_id}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest TikTok Ads ad creative for {len(ad_id_list)} ad_id(s) with table_id {table_id}...")

    # 1.4.4 Enforce schema for TikTok Ads ad creative
    try:
        print(f"🔁 [INGEST] Triggering to enforce schema for {len(df)} row(s) of TikTok Ads ad creative...")
        logging.info(f"🔁 [INGEST] Triggering to enforce schema for {len(df)} row(s) of TikTok Ads ad creative...")
        df = ensure_table_schema(df, schema_type="ingest_ad_creative")
    except Exception as e:
        print(f"❌ [INGEST] Failed to trigger schema enforcement for TikTok Ads ad creative due to {e}.")
        logging.error(f"❌ [INGEST] Failed to trigger schema enforcement for TikTok Ads ad creative due to {e}.")
        return df

    # 1.4.5. Delete existing row(s) or create new table if it not exist
    try:
        print(f"🔍 [INGEST] Checking TikTok ad creative table {table_id} existence...")
        logging.info(f"🔍 [INGEST] Checking TikTok ad creative table {table_id} existence...")
        df = df.drop_duplicates()
        
        try:
            print(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            google_bigquery_client = bigquery.Client(project=PROJECT)
            print(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            logging.info(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
        except DefaultCredentialsError as e:
            raise RuntimeError("❌ [INGEST] Failed to initialize Google BigQuery client due to credentials error.") from e
        except Forbidden as e:
            raise RuntimeError("❌ [INGEST] Failed to initialize Google BigQuery client due to permission denial.") from e
        except GoogleAPICallError as e:
            raise RuntimeError("❌ [INGEST] Failed to initialize Google BigQuery client due to API call error.") from e
        except Exception as e:
            raise RuntimeError(f"❌ [INGEST] Failed to initialize Google BigQuery client due to {e}.") from e
        
        try:
            google_bigquery_client.get_table(table_id)
            table_exists = True
        except Exception:
            table_exists = False
        if not table_exists:
            print(f"⚠️ [INGEST] TikTok Ads ad creative table {table_id} not found then table creation will be proceeding...")
            logging.info(f"⚠️ [INGEST] TikTok Ads ad creative table {table_id} not found then table creation will be proceeding...")
            schema = []
            for col, dtype in df.dtypes.items():
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
            effective_partition = "date" if "date" in df.columns else None
            if effective_partition:
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=effective_partition
                )
            clustering_fields = ["ad_id", "advertiser_id"]
            filtered_clusters = [f for f in clustering_fields if f in df.columns]
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
            unique_keys = df[["ad_id", "advertiser_id"]].dropna().drop_duplicates()
            if not unique_keys.empty:
                temp_table_id = f"{PROJECT}.{raw_dataset}.temp_table_ad_creative_delete_keys_{uuid.uuid4().hex[:8]}"
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
                print(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of TikTok Ads ad creative table {table_id}.")
                logging.info(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of TikTok Ads ad creative table {table_id}.")
            else:
                print(f"⚠️ [INGEST] No unique (ad_id and advertisier_id) keys found in TikTok Ads ad creative table {table_id} then existing row(s) deletion is skipped.")
                logging.warning(f"⚠️ [INGEST] No unique (ad_id and advertiser_id) keys found in TikTok Ads ad creative table {table_id} then existing row(s) deletion is skipped.")
    except Exception as e:
        print(f"❌ [INGEST] Failed during TikTok Ads ad creative ingestion due to {e}.")
        logging.error(f"❌ [INGEST] Failed during TikTok Ads ad creative ingestion due to {e}.")
        raise

    # 1.4.6. Upload TikTok Ads ad creative to Google BigQuery
    try:
        print(f"🔍 [INGEST] Uploading {len(df)} row(s) of TikTok Ads ad creative to {table_id}...")
        logging.info(f"🔍 [INGEST] Uploading {len(df)} row(s) of TikTok Ads ad creative to {table_id}...")
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        google_bigquery_client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
        print(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of TikTok Ads ad creative.")
        logging.info(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of TikTok Ads ad creative.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to upload TikTok Ads ad creative due to {e}.")
        logging.error(f"❌ [INGEST] Failed to upload TikTok Ads ad creative due to {e}.")
        raise
    return df

# 2. INGEST TIKTOK ADS INSIGHTS

# 2.1. Ingest campaign insights for TikToK Ads
def ingest_campaign_insights(start_date: str, end_date: str) -> pd.DataFrame:
    print(f"🚀 [INGEST] Starting to ingest TikTok Ads campaign insights from {start_date} to {end_date}...")
    logging.info(f"🚀 [INGEST] Starting to ingest TikTok Ads campaign insights from {start_date} to {end_date}...")

    # 2.1.1. Trigger to fetch TikTok Ads campaign insights
    print("🔁 [INGEST] Triggering to fetch TikTok Ads campaigns insights...")
    logging.info("🔁 [INGEST] Triggering to fetch TikTok Ads campaigns insights...")
    df = fetch_campaign_insights(start_date, end_date)    
    if df.empty:
        print("⚠️ [INGEST] Empty TikTok campaign insights returned.")
        logging.warning("⚠️ [INGEST] Empty TikTok campaign insights returned.")    
        return df

    # 2.1.2. Prepare table_id for TikTok Ads campaign insights ingestion
    first_date = pd.to_datetime(df["stat_time_day"].dropna().iloc[0])
    y, m = first_date.year, first_date.month
    raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
    table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_m{m:02d}{y}"
    print(f"🔍 [INGEST] Proceeding to ingest TikTok Ads campaign insights from {start_date} to {end_date} with table_id {table_id}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest TikTok Ads campaign insights from {start_date} to {end_date} with table_id {table_id}...")

    # 2.1.3. Enrich TikTok Ads campaign insights
    try:
        print(f"🔁 [INGEST] Trigger to enrich TikTok Ads campaign insights from {start_date} to {end_date} with {len(df)} row(s)...")
        logging.info(f"🔁 [INGEST] Trigger to enrich TikTok Ads campaign insights from {start_date} to {end_date} with {len(df)} row(s)...")
        df["date_range"] = f"{start_date}_to_{end_date}"
        df["last_updated_at"] = datetime.utcnow().replace(tzinfo=pytz.UTC)
    except Exception as e:
        print(f"❌ [INGEST] Failed to trigger enrichment TikTok Ads campaign insights from {start_date} to {end_date} due to {e}.")
        logging.error(f"❌ [INGEST] Failed to trigger enrichment TikTok Ads campaign insights from {start_date} to {end_date} due to {e}.")
        raise

    # 2.1.4. Enforce schema for TikTok Ads campaign insights
    try:
        print(f"🔁 [INGEST] Triggering to enforce schema for {len(df)} row(s) of TikTok Ads campaign insights...")
        logging.info(f"🔁 [INGEST] Triggering to enforce schema for {len(df)} row(s) of TikTok Ads campaign insights...")
        df = ensure_table_schema(df, schema_type="ingest_campaign_insights")
    except Exception as e:
        print(f"❌ [INGEST] Failed to trigger schema enforcement for TikTok Ads campaign insights due to {e}.")
        logging.error(f"❌ [INGEST] Failed to trigger schema enforcement for TikTok Ads campaign insights due to {e}.")
        raise

    # 2.1.6. Parse date column(s)
    try:
        print(f"🔁 [INGEST] Parsing TikTok Ads campaign insights {df.columns.tolist()} date column(s)...")
        logging.info(f"🔁 [INGEST] Parsing TikTok Ads campaign insights {df.columns.tolist()} date column(s)...")
        df["date"] = pd.to_datetime(df["stat_time_day"])
        df["year"] = df["date"].dt.year
        df["month"] = df["date"].dt.month
        df["date_start"] = df["date"].dt.strftime("%Y-%m-%d")
        print(f"✅ [INGEST] Successfully parsed date column(s) for TikTok Ads campaign insights.")
        logging.info(f"✅ [INGEST] Successfully parsed date column(s) for TikTok Ads campaign insights.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to parse date column(s) for TikTok Ads campaign insights due to {e}.")
        logging.error(f"❌ [INGEST] Failed to parse date column(s) for TikTok Ads campaign insights due to {e}.")
        raise

    # 2.1.7. Delete existing row(s) or create new table if not exist
    try:
        print(f"🔍 [INGEST] Checking TikTok Ads campaign insights table {table_id} existence...")
        logging.info(f"🔍 [INGEST] Checking TikTok Ads campaign insights table {table_id} existence...")
        df = df.drop_duplicates()
        
        try:
            print(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            google_bigquery_client = bigquery.Client(project=PROJECT)
            print(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            logging.info(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
        except DefaultCredentialsError as e:
            raise RuntimeError("❌ [INGEST] Failed to initialize Google BigQuery client due to credentials error.") from e
        except Forbidden as e:
            raise RuntimeError("❌ [INGEST] Failed to initialize Google BigQuery client due to permission denial.") from e
        except GoogleAPICallError as e:
            raise RuntimeError("❌ [INGEST] Failed to initialize Google BigQuery client due to API call error.") from e
        except Exception as e:
            raise RuntimeError(f"❌ [INGEST] Failed to initialize Google BigQuery client due to {e}.") from e
        try:

            google_bigquery_client.get_table(table_id)
            table_exists = True
        except Exception:
            table_exists = False
        if not table_exists:
            print(f"⚠️ [INGEST] TikTok Ads campaign insights table {table_id} not found then table creation will be proceeding...")
            logging.warning(f"⚠️ [INGEST] TikTok Ads campaign insights table {table_id} not found then table creation will be proceeding...")
            schema = []
            for col, dtype in df.dtypes.items():
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
            effective_partition = "date" if "date" in df.columns else None
            if effective_partition:
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=effective_partition
                )
                print(f"🔍 [INGEST] Creating TikTok Ads campaign insights {table_id} using partition on {effective_partition}...")
                logging.info(f"🔍 [INGEST] Creating TikTok Ads campaign insights {table_id} using partition on {effective_partition}...")
            table = google_bigquery_client.create_table(table)
            print(f"✅ [INGEST] Successfully created TikTok Ads campaign insights table {table_id} with partition on {effective_partition}.")
            logging.info(f"✅ [INGEST] Successfully created TikTok Ads campaign insights table {table_id} with partition on {effective_partition}.")
        else:
            new_dates = df["date_start"].dropna().unique().tolist()
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
        print(f"❌ [INGEST] Failed during TikTok Ads campaign insights ingestion due to {e}.")
        logging.error(f"❌ [INGEST] Failed during TikTok Ads campaign insights ingestion due to {e}.")
        raise

    # 2.1.8. Upload TikTok Ads campaign insights to Google BigQuery
    try:
        print(f"🔍 [INGEST] Uploading {len(df)} row(s) of TikTok Ads campaign insights to table {table_id}...")
        logging.info(f"🔍 [INGEST] Uploading {len(df)} row(s) of TikTok ADs campaign insights to table {table_id}...")
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            source_format=bigquery.SourceFormat.PARQUET,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="date",
            ),
        )
        load_job = google_bigquery_client.load_table_from_dataframe(
            df,
            table_id,
            job_config=job_config
        )
        load_job.result()
        print(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of TikTok Ads campaign insights.")
        logging.info(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of TikTok Ads campaign insights.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to upload TikTok Ads campaign insights due to {e}.")
        logging.error(f"❌ [INGEST] Failed to upload TikTok Ads campaign insights due to {e}.")
        raise
    return df

# 2.2. Ingest ad insights for TikTok Ads
def ingest_ad_insights(start_date: str, end_date: str) -> pd.DataFrame:
    print(f"🚀 [INGEST] Starting to ingest TikTok Ads ad insights from {start_date} to {end_date}...")
    logging.info(f"🚀 [INGEST] Starting to ingest TikTok Ads ad insights from {start_date} to {end_date}...")

    # 2.1.1. Trigger to fetch ad insights for TikTok Ads
    print("🔁 [INGEST] Triggering to fetch TikTok Ads ad insights...")
    logging.info("🔁 [INGEST] Triggering to fetch TikTok Ads ad insights...")
    df = fetch_ad_insights(start_date, end_date)    
    if df.empty:
        print("⚠️ [INGEST] Empty TikTok Ads ad insights returned.")
        logging.warning("⚠️ [INGEST] Empty TikTok Ads ad insights returned.")    
        return df

    # 2.1.2. Prepare table_id for TikTok Ads ad insights
    first_date = pd.to_datetime(df["stat_time_day"].dropna().iloc[0])
    y, m = first_date.year, first_date.month
    raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
    table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_ad_m{m:02d}{y}"
    print(f"🔍 [INGEST] Proceeding to ingest TikTok Ads ad insights from {start_date} to {end_date} with table_id {table_id}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest TikTok Ads ad insights from {start_date} to {end_date} with table_id {table_id}...")

    # 2.1.3. Enrich TikTok Ads ad insights
    try:
        print(f"🔁 [INGEST] Trigger to enrich TikTok Ads ad insights from {start_date} to {end_date} with {len(df)} row(s)...")
        logging.info(f"🔁 [INGEST] Trigger to enrich TikTok Ads ad insights from {start_date} to {end_date} with {len(df)} row(s)...")
        df["date_range"] = f"{start_date}_to_{end_date}"
        df["last_updated_at"] = datetime.utcnow().replace(tzinfo=pytz.UTC)
    except Exception as e:
        print(f"❌ [INGEST] Failed to trigger enrichment TikTok Ads ad insights from {start_date} to {end_date} due to {e}.")
        logging.error(f"❌ [INGEST] Failed to trigger enrichment TikTok ADs ad insights from {start_date} to {end_date} due to {e}.")
        raise

    # 2.1.4. Enforce schema for TikTok Ads ad insights
    try:
        print(f"🔁 [INGEST] Triggering to enforce schema for {len(df)} row(s) of TikTok Ads ad insights...")
        logging.info(f"🔁 [INGEST] Triggering to enforce schema for {len(df)} row(s) of TikTok Ads ad insights...")
        df = ensure_table_schema(df, schema_type="ingest_ad_insights")
    except Exception as e:
        print(f"❌ [INGEST] Failed to trigger schema enforcement for TikTok Ads ad insights due to {e}.")
        logging.error(f"❌ [INGEST] Failed to trigger schema enforcement for TikTok Ads ad insights due to {e}.")
        raise

    # 2.1.5. Parse date column(s)
    try:
        print(f"🔁 [INGEST] Parsing TikTok Ads ad insights {df.columns.tolist()} date column(s)...")
        logging.info(f"🔁 [INGEST] Parsing TikTok Ads ad insights {df.columns.tolist()} date column(s)...")
        df["date"] = pd.to_datetime(df["stat_time_day"])
        df["year"] = df["date"].dt.year
        df["month"] = df["date"].dt.month
        df["date_start"] = df["date"].dt.strftime("%Y-%m-%d")
        print(f"✅ [INGEST] Successfully parsed date column(s) for TikTok Ads ad insights.")
        logging.info(f"✅ [INGEST] Successfully parsed date column(s) for TikTok Ads ad insights.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to parse date column(s) for TikTok Ads ad insights due to {e}.")
        logging.error(f"❌ [INGEST] Failed to parse date column(s) for TikTok Ads ad insights due to {e}.")
        raise

    # 2.1.6. Delete existing row(s) or create new table if not exist
    try:
        print(f"🔍 [INGEST] Checking TikTok Ads ad insights table {table_id} existence...")
        logging.info(f"🔍 [INGEST] Checking TikTok Ads ad insights table {table_id} existence...")
        df = df.drop_duplicates()
        try:
            print(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            google_bigquery_client = bigquery.Client(project=PROJECT)
            print(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            logging.info(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
        except DefaultCredentialsError as e:
            raise RuntimeError("❌ [INGEST] Failed to initialize Google BigQuery client due to credentials error.") from e
        except Forbidden as e:
            raise RuntimeError("❌ [INGEST] Failed to initialize Google BigQuery client due to permission denial.") from e
        except GoogleAPICallError as e:
            raise RuntimeError("❌ [INGEST] Failed to initialize Google BigQuery client due to API call error.") from e
        except Exception as e:
            raise RuntimeError(f"❌ [INGEST] Failed to initialize Google BigQuery client due to {e}.") from e
        try:
            google_bigquery_client.get_table(table_id)
            table_exists = True
        except Exception:
            table_exists = False
        if not table_exists:
            print(f"⚠️ [INGEST] TikTok Ads ad insights table {table_id} not found then table creation will be proceeding...")
            logging.warning(f"⚠️ [INGEST] TikTok Ads ad insights table {table_id} not found then table creation will be proceeding...")
            schema = []
            for col, dtype in df.dtypes.items():
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
            effective_partition = "date" if "date" in df.columns else None
            if effective_partition:
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=effective_partition
                )
                print(f"🔍 [INGEST] Creating TikTok Ads ad insights {table_id} using partition on {effective_partition}...")
                logging.info(f"🔍 [INGEST] Creating TikTok Ads ad insights {table_id} using partition on {effective_partition}...")
            table = google_bigquery_client.create_table(table)
            print(f"✅ [INGEST] Successfully created TikTok Ads ad insights table {table_id} with partition on {effective_partition}.")
            logging.info(f"✅ [INGEST] Successfully created TikTok Ads ad insights table {table_id} with partition on {effective_partition}.")
        else:
            new_dates = df["date_start"].dropna().unique().tolist()
            query_existing = f"SELECT DISTINCT date_start FROM `{table_id}`"
            existing_dates = [row.date_start for row in google_bigquery_client.query(query_existing).result()]
            overlap = set(new_dates) & set(existing_dates)
            if overlap:
                print(f"⚠️ [INGEST] Found {len(overlap)} overlapping date(s) {overlap} in TikTok Ads ad insights {table_id} table then deletion will be proceeding...")
                logging.warning(f"⚠️ [INGEST] Found {len(overlap)} overlapping date(s) {overlap} in TikTok Ads ad insights {table_id} table then deletion will be proceeding...")
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
                        print(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) for {date_val} in TikTok Ads ad insights {table_id} table.")
                        logging.info(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) for {date_val} in TikTok Ads ad insights {table_id} table.")
                    except Exception as e:
                        print(f"❌ [INGEST] Failed to delete existing rows in TikTok Ads ad insights {table_id} table for {date_val} due to {e}.")
                        logging.error(f"❌ [INGEST] Failed to delete existing rows in TikTok Ads ad insights {table_id} table for {date_val} due to {e}.")
            else:
                print(f"✅ [INGEST] No overlapping dates found in TikTok Ads ad insights {table_id} table then deletion is skipped.")
                logging.info(f"✅ [INGEST] No overlapping dates found in TikTok Ads ad insights {table_id} table then deletion is skipped.")
    except Exception as e:
        print(f"❌ [INGEST] Failed during TikTok Ads ad insights ingestion due to {e}.")
        logging.error(f"❌ [INGEST] Failed during TikTok Ads ad insights ingestion due to {e}.")
        raise

    # 2.1.7. Upload TikTok Ads ad insights to Google BigQuery
    try:
        print(f"🔍 [INGEST] Uploading {len(df)} row(s) of TikTok Ads ad insights to table {table_id}...")
        logging.info(f"🔍 [INGEST] Uploading {len(df)} row(s) of TikTok Ads ad insights to table {table_id}...")
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            source_format=bigquery.SourceFormat.PARQUET,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="date",
            ),
        )
        load_job = google_bigquery_client.load_table_from_dataframe(
            df,
            table_id,
            job_config=job_config
        )
        load_job.result()
        print(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of TikTok Ads ad insights.")
        logging.info(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of TikTok Ads ad insights.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to upload TikTok Ads ad insights due to {e}.")
        logging.error(f"❌ [INGEST] Failed to upload TikTok Ads ad insights due to {e}.")
        raise
    return df

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run Facebook Campaign Backfill")
    parser.add_argument("--start_date", type=str, required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end_date", type=str, required=True, help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    ingest_ad_insights(start_date=args.start_date, end_date=args.end_date)
