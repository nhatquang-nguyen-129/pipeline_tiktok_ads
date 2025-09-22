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

# Add Google Authentication modules for integration
from google.auth.exceptions import DefaultCredentialsError

# Add Google Cloud modules for integration
from google.cloud import bigquery

# Add internal TikTok modules for handling
from src.fetch import (
    fetch_campaign_metadata,
    fetch_adset_metadata,
    fetch_ad_metadata,
    fetch_campaign_insights,
    fetch_ad_insights
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

# 1. INGEST TIKTOK ADS METADATA

# 1.1. Ingest campaign metadata for TikTok Ads
def ingest_campaign_metadata(campaign_id_list: list) -> pd.DataFrame:
    print("🚀 [INGEST] Starting to ingest TikTok campaign metadata...")
    logging.info("🚀 [INGEST] Starting TikTok campaign metadata...")

    # 1.1.1. Validate input
    if not campaign_id_list:
        print("⚠️ [INGEST] Empty TikTok campaign_id_list provided then ingestion is suspended.")
        logging.warning("⚠️ [INGEST] Empty TikTok campaign_id_list provided then ingestion is suspended.")
        raise ValueError("❌ [INGEST] TikTok campaign_id_list must be provided and not empty.")

    # 1.1.2. Call TikTok API
    try:
        print(f"🔍 [INGEST] Triggering to fetch TikTok campaign metadata for {len(campaign_id_list)} campaign_id(s) from API...")
        logging.info(f"🔍 [INGEST] Triggering to fetch TikTok campaign metadata for {len(campaign_id_list)} campaign_id(s) from API...")
        df = fetch_campaign_metadata(campaign_id_list=campaign_id_list)  # <-- gọi hàm fetch TikTok
        if df.empty:
            print("⚠️ [INGEST] Empty TikTok campaign metadata returned.")
            logging.warning("⚠️ [INGEST] Empty TikTok campaign metadata returned.")
            return pd.DataFrame()
    except Exception as e:
        print(f"❌ [INGEST] Failed to trigger TikTok campaign metadata fetch due to {e}.")
        logging.error(f"❌ [INGEST] Failed to trigger TikTok campaign metadata fetch due to {e}.")
        return pd.DataFrame()

    # 1.1.3. Prepare table_id
    raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
    table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_metadata"
    print(f"🔍 [INGEST] Proceeding to ingest TikTok campaign metadata for {len(campaign_id_list)} campaign_id(s) with table_id {table_id}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest TikTok campaign metadata for {len(campaign_id_list)} campaign_id(s) with table_id {table_id}...")

    # 1.1.4. Enforce schema
    try:
        print(f"🔄 [INGEST] Triggering to enforce schema for {len(df)} row(s) of TikTok campaign metadata...")
        logging.info(f"🔄 [INGEST] Triggering to enforce schema for {len(df)} row(s) of TikTok campaign metadata...")
        df = ensure_table_schema(df, "ingest_campaign_metadata")
    except Exception as e:
        print(f"❌ [INGEST] Failed to trigger schema enforcement for TikTok campaign metadata due to {e}.")
        logging.error(f"❌ [INGEST] Failed to trigger schema enforcement for TikTok campaign metadata due to {e}.")
        return df
    
    # 1.1.5. Delete existing row(s) or create new table if not exist
    try:
        print(f"🔍 [INGEST] Checking TikTok campaign metadata table {table_id} existence...")
        logging.info(f"🔍 [INGEST] Checking TikTok campaign metadata table {table_id} existence...")
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
            print(f"⚠️ [INGEST] TikTok campaign metadata table {table_id} not found then table creation will be proceeding...")
            logging.info(f"⚠️ [INGEST] TikTok campaign metadata table {table_id} not found then table creation will be proceeding...")
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
            clustering_fields = ["campaign_id", "account_id"]
            filtered_clusters = [f for f in clustering_fields if f in df.columns]
            if filtered_clusters:
                table.clustering_fields = filtered_clusters
                print(f"🔍 [INGEST] Creating TikTok campaign metadata table {table_id} using clustering on {filtered_clusters} and partition on {effective_partition}...")
                logging.info(f"🔍 [INGEST] Creating TikTok campaign metadata table {table_id} using clustering on {filtered_clusters} and partition on {effective_partition}...")
            table = client.create_table(table)
            print(f"✅ [INGEST] Successfully created TikTok campaign metadata table {table_id}.")
            logging.info(f"✅ [INGEST] Successfully created TikTok campaign metadata table {table_id}.")
        else:
            print(f"🔄 [INGEST] TikTok campaign metadata table {table_id} exists then existing row(s) deletion will be proceeding...")
            logging.info(f"🔄 [INGEST] TikTok campaign metadata table {table_id} exists then existing row(s) deletion will be proceeding...")
            unique_keys = df[["campaign_id", "account_id"]].dropna().drop_duplicates()
            if not unique_keys.empty:
                temp_table_id = f"{PROJECT}.{raw_dataset}.temp_table_campaign_metadata_delete_keys_{uuid.uuid4().hex[:8]}"
                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
                client.load_table_from_dataframe(unique_keys, temp_table_id, job_config=job_config).result()
                join_condition = " AND ".join([
                    f"CAST(main.{col} AS STRING) = CAST(temp.{col} AS STRING)"
                    for col in ["campaign_id", "account_id"]
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
                print(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of TikTok campaign metadata table {table_id}.")
                logging.info(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of TikTok campaign metadata table {table_id}.")
            else:
                print(f"⚠️ [INGEST] No unique (campaign_id, account_id) keys found in TikTok campaign metadata table {table_id} then deletion is skipped.")
                logging.warning(f"⚠️ [INGEST] No unique (campaign_id, account_id) keys found in TikTok campaign metadata table {table_id} then deletion is skipped.")
    except Exception as e:
        print(f"❌ [INGEST] Failed during TikTok campaign metadata ingestion due to {e}.")
        logging.error(f"❌ [INGEST] Failed during TikTok campaign metadata ingestion due to {e}.")
        raise

    # 1.1.6. Upload TikTok campaign metadata to Google BigQuery raw table
    try:
        print(f"🔍 [INGEST] Uploading {len(df)} row(s) of TikTok campaign metadata {table_id}...")
        logging.info(f"🔍 [INGEST] Uploading {len(df)} row(s) of TikTok campaign metadata {table_id}...")
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
        print(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of TikTok campaign metadata table {table_id}.")
        logging.info(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of TikTok campaign metadata table {table_id}.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to upload TikTok campaign metadata due to {e}.")
        logging.error(f"❌ [INGEST] Failed to upload TikTok campaign metadata due to {e}.")
        raise

    return df

# 1.2. Ingest adset metadata for TikTok Ads
def ingest_adset_metadata(adset_id_list: list) -> pd.DataFrame:
    print("🚀 [INGEST] Starting to ingest TikTok adgroup metadata...")
    logging.info("🚀 [INGEST] Starting TikTok adgroup metadata...")

    # 1.2.1. Validate input
    if not adset_id_list:
        print("⚠️ [INGEST] Empty TikTok adgroup_id_list provided.")
        logging.warning("⚠️ [INGEST] Empty TikTok adgroup_id_list provided.")
        raise ValueError("⚠️ [INGEST] Empty TikTok adgroup_id_list provided.")

    # 1.2.2. Call TikTok API
    try:
        print(f"🔍 [INGEST] Triggering to fetch TikTok adgroup metadata for {len(adset_id_list)} adset_id(s) from API...")
        logging.info(f"🔍 [INGEST] Triggering to fetch TikTok adgroup metadata for {len(adset_id_list)} adgroup_id(s) from API...")
        df = fetch_adset_metadata(adset_id_list=adset_id_list)
        if df.empty:
            print("⚠️ [INGEST] Empty TikTok adgroup metadata returned.")
            logging.warning("⚠️ [INGEST] Empty TikTok adgroup metadata returned.")
            return pd.DataFrame()
    except Exception as e:
        print(f"❌ [INGEST] Failed to trigger TikTok adgroup metadata fetch due to {e}.")
        logging.error(f"❌ [INGEST] Failed to trigger TikTok adgroup metadata fetch due to {e}.")
        return pd.DataFrame()

    # 1.2.3. Prepare full table_id for raw layer in BigQuery
    raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
    table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_adset_metadata"
    print(f"🔍 [INGEST] Proceeding to ingest TikTok adgroup metadata for {len(adset_id_list)} adgroup_id(s) with table_id {table_id}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest TikTok adgroup metadata for {len(adset_id_list)} adgroup_id(s) with table_id {table_id}...")

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
        print(f"🔍 [INGEST] Checking TikTok adgroup metadata table {table_id} existence...")
        logging.info(f"🔍 [INGEST] Checking TikTok adgroup metadata table {table_id} existence...")
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
            print(f"⚠️ [INGEST] TikTok adgroup metadata table {table_id} not found then table creation will be proceeding...")
            logging.info(f"⚠️ [INGEST] TikTok adgroup metadata table {table_id} not found then table creation will be proceeding...")
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
                print(f"🔍 [INGEST] Creating TikTok adgroup metadata table {table_id} using clustering on {filtered_clusters} field(s) and partition on {effective_partition}...")
                logging.info(f"🔍 [INGEST] Creating TikTok adgroup metadata table {table_id} using clustering on {filtered_clusters} field(s) and partition on {effective_partition}...")
            table = client.create_table(table)
            print(f"✅ [INGEST] Successfully created TikTok adgroup metadata table {table_id} with clustering on {filtered_clusters} field(s) and partition on {effective_partition}.")
            logging.info(f"✅ [INGEST] Successfully created TikTok adgroup metadata table {table_id} with clustering on {filtered_clusters} field(s) and partition on {effective_partition}.")
        else:
            print(f"🔄 [INGEST] TikTok adgroup metadata table {table_id} exists then existing row(s) deletion will be proceeding...")
            logging.info(f"🔄 [INGEST] TikTok adgroup metadata table {table_id} exists then existing row(s) deletion will be proceeding...")
            unique_keys = df[["adgroup_id", "advertiser_id"]].dropna().drop_duplicates()
            if not unique_keys.empty:
                temp_table_id = f"{PROJECT}.{raw_dataset}.temp_table_adgroup_metadata_delete_keys_{uuid.uuid4().hex[:8]}"
                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
                client.load_table_from_dataframe(unique_keys, temp_table_id, job_config=job_config).result()
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
                result = client.query(delete_query).result()
                client.delete_table(temp_table_id, not_found_ok=True)
                deleted_rows = result.num_dml_affected_rows
                print(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of TikTok adgroup metadata {table_id}.")
                logging.info(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of TikTok adgroup metadata {table_id}.")
            else:
                print(f"⚠️ [INGEST] No unique (adgroup_id, advertiser_id) keys found in TikTok adgroup metadata table {table_id} then existing row(s) deletion is skipped.")
                logging.warning(f"⚠️ [INGEST] No unique (adgroup_id, advertiser_id) keys found in TikTok adgroup metadata table {table_id} then existing row(s) deletion is skipped.")
    except Exception as e:
        print(f"❌ [INGEST] Failed during TikTok adgroup metadata ingestion due to {e}.")
        logging.error(f"❌ [INGEST] Failed during TikTok adgroup metadata ingestion due to {e}.")
        raise

    # 1.2.6. Upload TikTok adgroup metadata to Google BigQuery raw table
    try:
        print(f"🔍 [INGEST] Uploading {len(df)} row(s) of TikTok adgroup metadata to {table_id}...")
        logging.info(f"🔍 [INGEST] Uploading {len(df)} row(s) of TikTok adgroup metadata to {table_id}...")
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
        print(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of TikTok adgroup metadata table {table_id}.")
        logging.info(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of TikTok adgroup metadata table {table_id}.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to upload TikTok adgroup metadata due to {e}.")
        logging.error(f"❌ [INGEST] Failed to upload TikTok adgroup metadata due to {e}.")
        raise
    return df

# 1.3. Ingest ad metadata for TikTok Ads
def ingest_ad_metadata(ad_id_list: list) -> pd.DataFrame:
    print("🚀 [INGEST] Starting to ingest TikTok ad metadata...")
    logging.info("🚀 [INGEST] Starting TikTok ad metadata...")

    # 1.3.1. Validate input
    if not ad_id_list:
        print("⚠️ [INGEST] Empty TikTok ad_id_list provided.")
        logging.warning("⚠️ [INGEST] Empty TikTok ad_id_list provided.")
        raise ValueError("⚠️ [INGEST] Empty TikTok ad_id_list provided.")

    # 1.3.2. Call TikTok API
    try:
        print(f"🔍 [INGEST] Trigger to fetch TikTok ad metadata for {len(ad_id_list)} ad_id(s) from API...")
        logging.info(f"🔍 [INGEST] Trigger to fetch TikTok ad metadata for {len(ad_id_list)} ad_id(s) from API...")
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
                print(f"⚠️ [INGEST] No unique (ad_id, advertiser_id) keys found in TikTok ad metadata table {table_id} then existing row(s) deletion is skipped.")
                logging.warning(f"⚠️ [INGEST] No unique (ad_id, advertiser_id) keys found in TikTok ad metadata table {table_id} then existing row(s) deletion is skipped.")
    except Exception as e:
        print(f"❌ [INGEST] Failed during TikTok ad metadata ingestion due to {e}.")
        logging.error(f"❌ [INGEST] Failed during TikTok ad metadata ingestion due to {e}.")
        raise

    # 1.3.6. Upload to Google BigQuery
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
    print("🚀 [INGEST] Starting to ingest TikTok ad creative...")
    logging.info("🚀 [INGEST] Starting to ingest TikTok ad creative...")

    # 1.4.1. Validate input
    if not ad_id_list:
        print("⚠️ [INGEST] Empty TikTok ad_id_list provided.")
        logging.warning("⚠️ [INGEST] Empty TikTok ad_id_list provided.")
        raise ValueError("⚠️ [INGEST] Empty TikTok ad_id_list provided.")

    # 1.4.2. Call TikTok API
    try:
        print(f"🔍 [INGEST] Triggering to fetch TikTok creative metadata for {len(ad_id_list)} ad_id(s) from API...")
        logging.info(f"🔍 [INGEST] Triggering to fetch TikTok creative metadata for {len(ad_id_list)} ad_id(s) from API...")
        df = fetch_ad_creative(ad_id_list)
        if df.empty:
            print("⚠️ [INGEST] No TikTok ad creative returned.")
            logging.warning("⚠️ [INGEST] No TikTok ad creative returned.")
            return pd.DataFrame()
    except Exception as e:
        print(f"❌ [INGEST] Failed to trigger TikTok ad creative fetch due to {e}.")
        logging.error(f"❌ [INGEST] Failed to trigger TikTok ad creative fetch due to {e}.")
        return pd.DataFrame()

    # 1.4.3. Prepare full table_id for raw layer in BigQuery
    raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
    table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_creative_metadata"
    print(f"🔍 [INGEST] Proceeding to ingest TikTok ad creative for {len(ad_id_list)} ad_id(s) with table_id {table_id}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest TikTok ad creative for {len(ad_id_list)} ad_id(s) with table_id {table_id}...")

    # 1.4.4 Enforce schema
    try:
        print(f"🔍 [INGEST] Triggering to enforce schema for {len(df)} row(s) of TikTok ad creative...")
        logging.info(f"🔍 [INGEST] Triggering to enforce schema for {len(df)} row(s) of TikTok ad creative...")
        df = ensure_table_schema(df, schema_type="ingest_ad_creative")
    except Exception as e:
        print(f"❌ [INGEST] Failed to trigger schema enforcement for TikTok ad creative due to {e}.")
        logging.error(f"❌ [INGEST] Failed to trigger schema enforcement for TikTok ad creative due to {e}.")
        return df

    # 1.4.5. Delete existing row(s) or create new table if it not exist
    try:
        print(f"🔍 [INGEST] Checking TikTok ad creative table {table_id} existence...")
        logging.info(f"🔍 [INGEST] Checking TikTok ad creative table {table_id} existence...")
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
            print(f"⚠️ [INGEST] TikTok ad creative table {table_id} not found then table creation will be proceeding...")
            logging.info(f"⚠️ [INGEST] TikTok ad creative table {table_id} not found then table creation will be proceeding...")
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
            clustering_fields = ["ad_id", "account_id"]
            filtered_clusters = [f for f in clustering_fields if f in df.columns]
            if filtered_clusters:
                table.clustering_fields = filtered_clusters
                print(f"🔍 [INGEST] Creating TikTok ad creative table {table_id} using clustering on {filtered_clusters} field(s) and partition on {effective_partition})...")
                logging.info(f" [INGEST] Creating TikTok ad creative table {table_id} using clustering on {filtered_clusters} field(s) and partition on {effective_partition}...")
            table = client.create_table(table)
            print(f"✅ [INGEST] Successfully created TikTok ad creative table {table_id} with clustering on {filtered_clusters} field(s) and partition on {effective_partition}.")
            logging.info(f"✅ [INGEST] Successfully created TikTok ad creative table {table_id} with clustering on {filtered_clusters} field(s) and partition on {effective_partition}.")
        else:
            print(f"🔄 [INGEST] TikTok ad creative table {table_id} exists then existing row(s) deletion will be proceeding...")
            logging.info(f"🔄 [INGEST] TikTok ad creative table {table_id} exists then existing row(s) deletion will be proceeding...")
            unique_keys = df[["ad_id", "account_id"]].dropna().drop_duplicates()
            if not unique_keys.empty:
                temp_table_id = f"{PROJECT}.{raw_dataset}.temp_table_ad_creative_delete_keys_{uuid.uuid4().hex[:8]}"
                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
                client.load_table_from_dataframe(unique_keys, temp_table_id, job_config=job_config).result()
                join_condition = " AND ".join([
                    f"CAST(main.{col} AS STRING) = CAST(temp.{col} AS STRING)"
                    for col in ["ad_id", "account_id"]
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
                print(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of TikTok ad creative table {table_id}.")
                logging.info(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) of TikTok ad creative table {table_id}.")
            else:
                print(f"⚠️ [INGEST] No unique (ad_id, account_id) keys found in TikTok ad creative table {table_id} then existing row(s) deletion is skipped.")
                logging.warning(f"⚠️ [INGEST] No unique (ad_id, account_id) keys found in TikTok ad creative table {table_id} then existing row(s) deletion is skipped.")
    except Exception as e:
        print(f"❌ [INGEST] Failed during TikTok ad creative ingestion due to {e}.")
        logging.error(f"❌ [INGEST] Failed during TikTok ad creative ingestion due to {e}.")
        raise

    # 1.4.6. Upload to Google BigQuery
    try:
        print(f"🔍 [INGEST] Uploading {len(df)} row(s) of TikTok ad creative to {table_id}...")
        logging.info(f"🔍 [INGEST] Uploading {len(df)} row(s) of TikTok ad creative to {table_id}...")
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
        print(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of TikTok ad creative.")
        logging.info(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of TikTok ad creative.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to upload TikTok ad creative due to {e}.")
        logging.error(f"❌ [INGEST] Failed to upload TikTok ad creative due to {e}.")
        raise
    return df

# 2. INGEST TIKTOK ADS INSIGHTS

# 2.1. Ingest campaign insights for TikToK Ads
def ingest_campaign_insights(
    start_date: str,
    end_date: str,
    write_disposition: str = "WRITE_APPEND"
) -> pd.DataFrame:

    print(f"🚀 [INGEST] Starting to ingest TikTok campaign insights from {start_date} to {end_date}...")
    logging.info(f"🚀 [INGEST] Starting to ingest TikTok campaign insights from {start_date} to {end_date}...")

    # 2.1.1. Call TikTok API to fetch campaign insights
    print("🔍 [INGEST] Triggering to fetch TikTok campaigns insights from API...")
    logging.info("🔍 [INGEST] Triggering to fetch TikTok campaigns insights from API...")
    df = fetch_campaign_insights(start_date, end_date)    
    if df.empty:
        print("⚠️ [INGEST] Empty TikTok campaign insights returned.")
        logging.warning("⚠️ [INGEST] Empty TikTok campaign insights returned.")    
        return df

    # 2.1.2. Prepare table_id
    first_date = pd.to_datetime(df["stat_time_day"].dropna().iloc[0])
    y, m = first_date.year, first_date.month
    raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
    table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_m{m:02d}{y}"
    print(f"🔍 [INGEST] Proceeding to ingest TikTok campaign insights from {start_date} to {end_date} with table_id {table_id}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest TikTok campaign insights from {start_date} to {end_date} with table_id {table_id}...")

    # 2.1.3. Enrich TikTok campaign insights
    try:
        print(f"🔁 [INGEST] Trigger to enrich TikTok campaign insights from {start_date} to {end_date} with {len(df)} row(s)...")
        logging.info(f"🔁 [INGEST] Trigger to enrich TikTok campaign insights from {start_date} to {end_date} with {len(df)} row(s)...")
        df = enrich_campaign_insights(df)
        df["account_name"] = fetch_account_name()
        df["date_range"] = f"{start_date}_to_{end_date}"
        df["last_updated_at"] = datetime.utcnow().replace(tzinfo=pytz.UTC)
    except Exception as e:
        print(f"❌ [INGEST] Failed to trigger enrichment TikTok campaign insights from {start_date} to {end_date} due to {e}.")
        logging.error(f"❌ [INGEST] Failed to trigger enrichment TikTok campaign insights from {start_date} to {end_date} due to {e}.")
        raise

    # 2.1.4. Cast TikTok numeric fields to float
    try:
        numeric_fields = [
            "spend",
            "impressions",
            "clicks",
            "cpc",
            "cpm",
            "ctr",
            "conversion",
            "conversion_rate"
        ]
        print(f"🔁 [INGEST] Casting TikTok campaign insights {numeric_fields} numeric field(s)...")
        logging.info(f"🔁 [INGEST] Casting TikTok campaign insights {numeric_fields} numeric field(s)...")
        for col in numeric_fields:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        print(f"✅ [INGEST] Successfully casted TikTok campaign insights {numeric_fields} numeric field(s) to float.")
        logging.info(f"✅ [INGEST] Successfully casted TikTok campaign insights {numeric_fields} numeric field(s) to float.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to cast TikTok numeric field(s) to float due to {e}.")
        logging.error(f"❌ [INGEST] Failed to cast TikTok numeric field(s) to float due to {e}.")
        raise

    # 2.1.5. Enforce schema
    try:
        print(f"🔁 [INGEST] Triggering to enforce schema for {len(df)} row(s) of TikTok campaign insights...")
        logging.info(f"🔁 [INGEST] Triggering to enforce schema for {len(df)} row(s) of TikTok campaign insights...")
        df = ensure_table_schema(df, schema_type="ingest_campaign_insights")
    except Exception as e:
        print(f"❌ [INGEST] Failed to trigger schema enforcement for TikTok campaign insights due to {e}.")
        logging.error(f"❌ [INGEST] Failed to trigger schema enforcement for TikTok campaign insights due to {e}.")
        raise

    # 2.1.6. Parse date column(s)
    try:
        print(f"🔁 [INGEST] Parsing TikTok campaign insights {df.columns.tolist()} date column(s)...")
        logging.info(f"🔁 [INGEST] Parsing TikTok campaign insights {df.columns.tolist()} date column(s)...")
        df["date"] = pd.to_datetime(df["stat_time_day"])
        df["year"] = df["date"].dt.year
        df["month"] = df["date"].dt.month
        df["date_start"] = df["date"].dt.strftime("%Y-%m-%d")
        print(f"✅ [INGEST] Successfully parsed date column(s) for TikTok campaign insights.")
        logging.info(f"✅ [INGEST] Successfully parsed date column(s) for TikTok campaign insights.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to parse date column(s) for TikTok campaign insights due to {e}.")
        logging.error(f"❌ [INGEST] Failed to parse date column(s) for TikTok campaign insights due to {e}.")
        raise

    # 2.1.7. Delete existing row(s) or create new table if not exist
    try:
        print(f"🔍 [INGEST] Checking TikTok campaign insights table {table_id} existence...")
        logging.info(f"🔍 [INGEST] Checking TikTok campaign insights table {table_id} existence...")
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
            print(f"⚠️ [INGEST] TikTok campaign insights table {table_id} not found then table creation will be proceeding...")
            logging.warning(f"⚠️ [INGEST] TikTok campaign insights table {table_id} not found then table creation will be proceeding...")
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
                print(f"🔍 [INGEST] Creating TikTok campaign insights {table_id} using partition on {effective_partition}...")
                logging.info(f"🔍 [INGEST] Creating TikTok campaign insights {table_id} using partition on {effective_partition}...")
            table = client.create_table(table)
            print(f"✅ [INGEST] Successfully created TikTok campaign insights table {table_id} with partition on {effective_partition}.")
            logging.info(f"✅ [INGEST] Successfully created TikTok campaign insights table {table_id} with partition on {effective_partition}.")
        else:
            new_dates = df["date_start"].dropna().unique().tolist()
            query_existing = f"SELECT DISTINCT date_start FROM `{table_id}`"
            existing_dates = [row.date_start for row in client.query(query_existing).result()]
            overlap = set(new_dates) & set(existing_dates)
            if overlap:
                print(f"⚠️ [INGEST] Found {len(overlap)} overlapping date(s) {overlap} in TikTok campaign insights {table_id} table then deletion will be proceeding...")
                logging.warning(f"⚠️ [INGEST] Found {len(overlap)} overlapping date(s) {overlap} in TikTok campaign insights {table_id} table then deletion will be proceeding...")
                for date_val in overlap:
                    query = f"""
                        DELETE FROM `{table_id}`
                        WHERE date_start = @date_value
                    """
                    job_config = bigquery.QueryJobConfig(
                        query_parameters=[bigquery.ScalarQueryParameter("date_value", "STRING", date_val)]
                    )
                    try:
                        result = client.query(query, job_config=job_config).result()
                        deleted_rows = result.num_dml_affected_rows
                        print(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) for {date_val} in TikTok campaign insights {table_id} table.")
                        logging.info(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) for {date_val} in TikTok campaign insights {table_id} table.")
                    except Exception as e:
                        print(f"❌ [INGEST] Failed to delete existing rows in TikTok campaign insights {table_id} table for {date_val} due to {e}.")
                        logging.error(f"❌ [INGEST] Failed to delete existing rows in TikTok campaign insights {table_id} table for {date_val} due to {e}.")
            else:
                print(f"✅ [INGEST] No overlapping dates found in TikTok campaign insights {table_id} table then deletion is skipped.")
                logging.info(f"✅ [INGEST] No overlapping dates found in TikTok campaign insights {table_id} table then deletion is skipped.")
    except Exception as e:
        print(f"❌ [INGEST] Failed during TikTok campaign insights ingestion due to {e}.")
        logging.error(f"❌ [INGEST] Failed during TikTok campaign insights ingestion due to {e}.")
        raise

    # 2.1.8. Upload to BigQuery
    try:
        print(f"🔍 [INGEST] Uploading {len(df)} row(s) of TikTok campaign insights to table {table_id}...")
        logging.info(f"🔍 [INGEST] Uploading {len(df)} row(s) of TikTok campaign insights to table {table_id}...")
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            source_format=bigquery.SourceFormat.PARQUET,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="date",
            ),
        )
        load_job = client.load_table_from_dataframe(
            df,
            table_id,
            job_config=job_config
        )
        load_job.result()
        print(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of TikTok campaign insights.")
        logging.info(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of TikTok campaign insights.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to upload TikTok campaign insights due to {e}.")
        logging.error(f"❌ [INGEST] Failed to upload TikTok campaign insights due to {e}.")
        raise
    return df

# 2.2. Ingest TikTok Ad insights to Google BigQuery raw tables
def ingest_tiktok_ad_insights(
    start_date: str,
    end_date: str,
    write_disposition: str = "WRITE_APPEND"
) -> pd.DataFrame:
    print(f"🚀 [INGEST] Starting to ingest TikTok ad insights from {start_date} to {end_date}...")
    logging.info(f"🚀 [INGEST] Starting to ingest TikTok ad insights from {start_date} to {end_date}...")

    # 2.2.1. Call TikTok API to fetch ad insights
    print("🔍 [INGEST] Triggering to fetch TikTok ad insights from API...")
    logging.info("🔍 [INGEST] Triggering to fetch TikTok ad insights from API...")
    df = fetch_tiktok_ad_insights(start_date, end_date)
    if df.empty:
        print("⚠️ [INGEST] Empty TikTok ad insights returned.")
        logging.warning("⚠️ [INGEST] Empty TikTok ad insights returned.")    
        return df

    # 2.2.2. Prepare full table_id for raw layer in BigQuery
    first_date = pd.to_datetime(df["date"].dropna().iloc[0])
    y, m = first_date.year, first_date.month
    raw_dataset = f"{COMPANY}_dataset_tiktok_api_raw"
    table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_tiktok_{DEPARTMENT}_{ACCOUNT}_ad_m{m:02d}{y}"
    print(f"🔍 [INGEST] Proceeding to ingest TikTok ad insights from {start_date} to {end_date} with table_id {table_id}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest TikTok ad insights from {start_date} to {end_date} with table_id {table_id}...")
 
    # 2.2.3. Enrich TikTok ad insights
    try:
        print(f"🔁 [INGEST] Triggering to enrich TikTok ad insights from {start_date} to {end_date} with {len(df)} row(s)...")
        logging.info(f"🔁 [INGEST] Triggering to enrich TikTok ad insights from {start_date} to {end_date} with {len(df)} row(s)...")
        df = enrich_tiktok_ad_insights(df)
        df["date_range"] = f"{start_date}_to_{end_date}"
        df["last_updated_at"] = datetime.utcnow().replace(tzinfo=pytz.UTC)
    except Exception as e:
        print(f"❌ [INGEST] Failed to trigger enrichment TikTok ad insights due to {e}.")
        logging.error(f"❌ [INGEST] Failed to trigger enrichment TikTok ad insights due to {e}.")
        raise

    # 2.2.4. Cast TikTok numeric fields to float
    try:
        numeric_fields = [
            "spend",
            "impressions",
            "clicks",
            "reach",
            "conversions",
            "cpc",
            "cpm",
            "ctr"
        ]
        print(f"🔁 [INGEST] Casting TikTok ad insights {numeric_fields} numeric field(s)...")
        logging.info(f"🔁 [INGEST] Casting TikTok ad insights {numeric_fields} numeric field(s)...")
        for col in numeric_fields:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        print(f"✅ [INGEST] Successfully casted TikTok ad insights {numeric_fields} numeric field(s) to float.")
        logging.info(f"✅ [INGEST] Successfully casted TikTok ad insights {numeric_fields} numeric field(s) to float.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to cast TikTok numeric field(s) to float due to {e}.")
        logging.error(f"❌ [INGEST] Failed to cast TikTok numeric field(s) to float due to {e}.")
        raise

    # 2.2.5. Enforce schema for TikTok ad insights
    try:
        print(f"🔁 [INGEST] Triggering to enforce schema for {len(df)} row(s) of TikTok ad insights...")
        logging.info(f"🔁 [INGEST] Triggering to enforce schema for {len(df)} row(s) of TikTok ad insights...")
        df = ensure_table_schema(df, "ingest_tiktok_ad_insights")
    except Exception as e:
        print(f"❌ [INGEST] Failed to enforce schema for TikTok ad insights due to {e}.")
        logging.error(f"❌ [INGEST] Failed to enforce schema for TikTok ad insights due to {e}.")
        raise

    # 2.2.6. Parse date column(s)
    try:
        print(f"🔁 [INGEST] Parsing TikTok ad insights date columns...")
        logging.info(f"🔁 [INGEST] Parsing TikTok ad insights date columns...")
        df["date"] = pd.to_datetime(df["date"])
        df["date"] = df["date"].dt.strftime("%Y-%m-%d")
        print(f"✅ [INGEST] Successfully parsed date column(s) for TikTok ad insights.")
        logging.info(f"✅ [INGEST] Successfully parsed date column(s) for TikTok ad insights.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to parse date column(s) for TikTok ad insights due to {e}.")
        logging.error(f"❌ [INGEST] Failed to parse date column(s) for TikTok ad insights due to {e}.")
        raise

    # 2.2.7. Delete existing row(s) or create new table if not exist
    try:
        print(f"🔍 [INGEST] Checking TikTok ad insights table {table_id} existence...")
        logging.info(f"🔍 [INGEST] Checking TikTok ad insights table {table_id} existence...")
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
            print(f"⚠️ [INGEST] TikTok ad insights table {table_id} not found → creating...")
            logging.warning(f"⚠️ [INGEST] TikTok ad insights table {table_id} not found → creating...")
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
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="date"
            )
            table = client.create_table(table)
            print(f"✅ [INGEST] Successfully created TikTok ad insights table {table_id}.")
            logging.info(f"✅ [INGEST] Successfully created TikTok ad insights table {table_id}.")
        else:
            new_dates = df["date"].dropna().unique().tolist()
            query_existing = f"SELECT DISTINCT date FROM `{table_id}`"
            existing_dates = [row.date for row in client.query(query_existing).result()]
            overlap = set(new_dates) & set(existing_dates)
            if overlap:
                print(f"⚠️ [INGEST] Found overlapping dates {overlap} → deleting...")
                logging.warning(f"⚠️ [INGEST] Found overlapping dates {overlap} → deleting...")
                for date_val in overlap:
                    query = f"""
                        DELETE FROM `{table_id}`
                        WHERE date = @date_value
                    """
                    job_config = bigquery.QueryJobConfig(
                        query_parameters=[bigquery.ScalarQueryParameter("date_value", "STRING", date_val)]
                    )
                    client.query(query, job_config=job_config).result()
                    print(f"✅ [INGEST] Deleted existing rows for {date_val}.")
                    logging.info(f"✅ [INGEST] Deleted existing rows for {date_val}.")
            else:
                print("✅ [INGEST] No overlapping dates found → skipping deletion.")
                logging.info("✅ [INGEST] No overlapping dates found → skipping deletion.")
    except Exception as e:
        print(f"❌ [INGEST] Failed during TikTok ad insights ingestion due to {e}.")
        logging.error(f"❌ [INGEST] Failed during TikTok ad insights ingestion due to {e}.")
        raise

    # 2.2.8. Upload to BigQuery
    try:
        print(f"🔍 [INGEST] Uploading {len(df)} row(s) of TikTok ad insights to {table_id}...")
        logging.info(f"🔍 [INGEST] Uploading {len(df)} row(s) of TikTok ad insights to {table_id}...")
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            source_format=bigquery.SourceFormat.PARQUET,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="date",
            ),
        )
        client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
        print(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of TikTok ad insights.")
        logging.info(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of TikTok ad insights.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to upload TikTok ad insights due to {e}.")
        logging.error(f"❌ [INGEST] Failed to upload TikTok ad insights due to {e}.")
        raise

    return df