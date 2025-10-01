"""
==================================================================
TIKTOK STAGING MODULE
------------------------------------------------------------------
This module transforms raw TikTok Ads data into enriched,  
normalized **staging tables** in BigQuery, acting as the bridge  
between raw API ingestion and final MART-level analytics.

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
    NotFound
)

# Add Google Authentication libraries for integration
from google.auth.exceptions import DefaultCredentialsError

# Add Google Cloud modules for integration
from google.cloud import bigquery

# Add internal Facebook module for handling
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

# 1.1. Transform TikTok Ads campaigninsights from raw tables into cleaned staging tables
def staging_campaign_insights() -> None:
    print("🚀 [STAGING] Starting unified staging process for TikTok Ads campaign insights...")
    logging.info("🚀 [STAGING] Starting unified staging process for TikTok Ads campaign insights...")

    # 1.1.1. Prepare table_id for TikTok Ads campaign insights
    try: 
        raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
        raw_campaign_metadata = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_metadata"
        print(f"🔍 [STAGING] Using raw table metadata {raw_dataset} to build staging table for TikTok Ads campaign insights...")
        logging.info(f"🔍 [STAGING] Using raw table metadata {raw_dataset} to build staging table for TikTok Ads campaign insights....")
        staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
        staging_campaign_insights = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_campaign_insights"
        print(f"🔍 [STAGING] Preparing to build staging table {staging_campaign_insights} for TikTok Ads campaign insights...")
        logging.info(f"🔍 [STAGING] Preparing to build staging table {staging_campaign_insights} for TikTok Ads campaign insights...")

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

    # 1.1.3. Scan all raw TikTok Ads campaign table(s)
        print("🔍 [STAGING] Scanning all raw TikTok Ads campaign insights table(s)...")
        logging.info("🔍 [STAGING] Scanning all raw TikTok Ads campaign insights table(s)...")
        query_tables = f"""
            SELECT table_name
            FROM `{PROJECT}.{raw_dataset}.INFORMATION_SCHEMA.TABLES`
            WHERE REGEXP_CONTAINS(
                table_name,
                r'^{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_m[0-1][0-9][0-9]{{4}}$'
            )
        """
        raw_tables = [row.table_name for row in google_bigquery_client.query(query_tables).result()]
        raw_tables = [f"{PROJECT}.{raw_dataset}.{t}" for t in raw_tables]
        if not raw_tables:
            print("⚠️ [STAGING] No raw TikTok Ads campaign insights table(s) found then staging is skipped.")
            logging.warning("⚠️ [STAGING] No raw TikTok Ads campaign insights table(s) found then staging is skipped.")
            return
        print(f"✅ [STAGING] Successfully found {len(raw_tables)} raw TikTok Ads campaign insights table(s).")
        logging.info(f"✅ [STAGING] Successfully found {len(raw_tables)} raw TikTok Ads campaign insights table(s).")

    # 1.1.3. Query raw table(s)
        all_dfs = []
        for raw_table in raw_tables:
            print(f"🔄 [STAGING] Querying raw Facebook campaign insights table {raw_table}...")
            logging.info(f"🔄 [STAGING] Querying raw Facebook campaign insights table {raw_table}...")
            query = f"""
                SELECT
                    raw.*,
                    metadata.campaign_name,
                    metadata.advertiser_name,
                    metadata.operation_status
                FROM `{raw_table}` AS raw
                LEFT JOIN `{raw_campaign_metadata}` AS metadata
                    ON CAST(raw.campaign_id AS STRING) = CAST(metadata.campaign_id AS STRING)
                    AND CAST(raw.advertiser_id  AS STRING) = CAST(metadata.advertiser_id  AS STRING)
            """
            try:
                df_month = google_bigquery_client.query(query).to_dataframe()
                print(f"✅ [STAGING] Successfully queried {len(df_month)} row(s) of raw Facebook campaign insights from {raw_table}.")
                logging.info(f"✅ [STAGING] Successfully queried {len(df_month)} row(s) of raw Facebook campaign insights from {raw_table}.")
            except Exception as e:
                print(f"❌ [STAGING] Failed to query Facebook campaign insights raw table {raw_table} due to {e}.")
                logging.warning(f"❌ [STAGING] Failed to query Facebook campaign insights raw table {raw_table} due to {e}.")
                continue

    # 1.1.5. Enrich TikTok Ads insights
        if not df_month.empty:
            try:
                print(f"🔄 [STAGING] Triggering to normalize and enrich staging TikTok Ads campaign insights field(s) for {len(df_month)} row(s) from {raw_table}...")
                logging.info(f"🔄 [STAGING] Triggering to normalize and enrich staging TikTok Ads campaign insights field(s) for {len(df_month)} row(s) from {raw_table}...")
                df_month = df_month.rename(columns={
                    "advertiser_id": "account_id",
                    "objective_type": "result_type",
                    "advertiser_name": "account_name",
                    "operation_status": "delivery_status"
                })               
                df_month = enrich_campaign_fields(df_month, table_id=raw_table)
                if "nhan_su" in df_month.columns:
                    df_month["nhan_su"] = df_month["nhan_su"].apply(remove_string_accents)
                all_dfs.append(df_month)
            except Exception as e:
                print(f"❌ [STAGING] Failed to trigger enrichment for staging TikTok Ads campaign insights due to {e}.")
                logging.warning(f"❌ [STAGING] Failed to trigger enrichment for staging TikTok Ads campaign insights due to {e}.") 
        if not all_dfs:
            print("⚠️ [STAGING] No data found in any raw TikTok Ads campaign insights table(s).")
            logging.warning("⚠️ [STAGING] No data found in any raw TikTok Ads campaign insights table(s).")
            return
        df_all = pd.concat(all_dfs, ignore_index=True)
        print(f"✅ [STAGING] Successfully combined {len(df_all)} row(s) from all TikTok Ads raw campaign insights table(s).")
        logging.info(f"✅ [STAGING] Successfully combined {len(df_all)} row(s) from all TikTok Ads raw campaign insights table(s).")

    # 1.1.6. Enforce schema for TikTok Ads campaign insights
        try:
            print(f"🔄 [STAGING] Triggering to enforce schema for {len(df_all)} row(s) of staging TikTok Ads campaign insights...")
            logging.info(f"🔄 [STAGING] Triggering to enforce schema for {len(df_all)} row(s) of staging TikTok Ads campaign insights...")
            df_all = ensure_table_schema(df_all, "staging_campaign_insights")
        except Exception as e:
            print(f"❌ [STAGING] Failed to trigger schema enforcement for {len(df_all)} row(s) of staging Facebook campaign insights due to {e}.")
            logging.error(f"❌ [STAGING] Failed to trigger schema enforcement for {len(df_all)} row(s) of staging Facebook campaign insights due to {e}.")
            raise   

    # 1.1.7. Upload TikTok Ads campaign insigts to Google BigQuery        
        try:
            print(f"🔍 [STAGING] Uploading {len(df_all)} row(s) of staging TikTok Ads campaign insights to Google BigQuery table {staging_campaign_insights}...")
            logging.info(f"🔍 [STAGING] Uploading {len(df_all)} row(s) of staging TikTok Ads campaign insights to Google BigQuery table {staging_campaign_insights}...")
            df_all = df_all.drop_duplicates()
            clustering_fields = [f for f in ["chuong_trinh", "ma_ngan_sach_cap_1", "nhan_su"] if f in df_all.columns]
            try:
                google_bigquery_client.get_table(staging_campaign_insights)
                table_exists = True
            except Exception:
                table_exists = False
            if not table_exists:
                schema = []
                for col, dtype in df_all.dtypes.items():
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
                table = bigquery.Table(staging_campaign_insights, schema=schema)
                if "date" in df_all.columns:
                    table.time_partitioning = bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field="date"
                    )
                    if clustering_fields:  
                        table.clustering_fields = clustering_fields  
                    print(f"🔍 [STAGING] Creating staging TikTok Ads campaign insights table {staging_campaign_insights} with partition on date...")
                    logging.info(f"🔍 [STAGING] Creating staging TikTok Ads campaign insights table {staging_campaign_insights} with partition on date...")
                table = google_bigquery_client.create_table(table)
                print(f"✅ [STAGING] Successfully created staging TikTok Ads campaign insights table {staging_campaign_insights}.")
                logging.info(f"✅ [STAGING] Successfully created staging TikTok Ads campaign insights table {staging_campaign_insights}.")
            else:
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
                    df_all,
                    staging_campaign_insights,
                    job_config=job_config
                )
                load_job.result()
                print(f"✅ [STAGING] Successfully uploaded {len(df_all)} row(s) to staging TikTok Ads campaign insights table {staging_campaign_insights}.")
                logging.info(f"✅ [STAGING] Successfully uploaded {len(df_all)} row(s) to staging TikTok Ads campaign insights table {staging_campaign_insights}.")
        except Exception as e:
            print(f"❌ [STAGING] Failed to upload staging TikTok Ads campaign insights due to {e}.")
            logging.error(f"❌ [STAGING] Failed to upload staging TikTok Ads campaign insights due to {e}.")
    except Exception as e:
        print(f"❌ [STAGING] Faild to unify staging TikTok Ads campaign insights table due to {e}.")
        logging.error(f"❌ [STAGING] Faild to unify staging TikTok Ads campaign insights table due to {e}.")

# 1.2. Transform TikTok Ads ad insights from raw tables into cleaned staging tables
def staging_ad_insights() -> None:
    print("🚀 [STAGING] Starting unified staging process for TikTok Ads ad insights...")
    logging.info("🚀 [STAGING] Starting unified staging process for TikTok Ads ad insights...")

    # 1.2.1. Prepare table_id for TikTok Ads ad insights
    try:
        raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
        raw_campaign_metadata = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_metadata"
        raw_ad_metadata = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_ad_metadata"
        raw_ad_creative = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_creative_metadata"
        print(f"🔍 [STAGING] Using raw table metadata {raw_dataset} to build staging table for TikTok Ads ad insights...")
        logging.info(f"🔍 [STAGING] Using raw table metadata {raw_dataset} to build staging table for TikTok Ads ad insights...")
        staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
        staging_ad_insights = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_ad_insights"
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
        print("🔍 [STAGING] Scanning all raw TikTok Ads ad insights table(s)...")
        logging.info("🔍 [STAGING] Scanning all raw TikTok Ads ad insights table(s)...")
        query_tables = f"""
            SELECT table_name
            FROM `{PROJECT}.{raw_dataset}.INFORMATION_SCHEMA.TABLES`
            WHERE REGEXP_CONTAINS(
                table_name,
                r'^{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_ad_m[0-1][0-9][0-9]{{4}}$'
            )
        """
        raw_tables = [row.table_name for row in google_bigquery_client.query(query_tables).result()]
        raw_tables = [f"{PROJECT}.{raw_dataset}.{t}" for t in raw_tables]
        if not raw_tables:
            print("⚠️ [STAGING] No raw TikTok Ads ad insights table(s) found then staging is skipped.")
            logging.warning("⚠️ [STAGING] No raw TikTok Ads ad insights table(s) found then staging is skipped.")
            return
        print(f"✅ [STAGING] Successfully found {len(raw_tables)} raw TikTok Ads ad insights table(s).")
        logging.info(f"✅ [STAGING] Successfully found {len(raw_tables)} raw TikTok Ads ad insights table(s).")

    # 1.2.3. Query raw TikTok Ads ad insights table(s)
        all_dfs = []
        for raw_table in raw_tables:
            print(f"🔄 [STAGING] Querying raw TikTok Ads ad insights table {raw_table}...")
            logging.info(f"🔄 [STAGING] Querying raw TikTok Ads ad insights table {raw_table}...")
            query = f"""
                SELECT
                    raw.*,
                    ad.ad_name,
                    adset.adset_name,
                    campaign.campaign_name,
                    creative.thumbnail_url,
                    ad.effective_status AS delivery_status
                FROM `{raw_table}` AS raw
                LEFT JOIN `{raw_ad_metadata}` AS ad
                    ON CAST(raw.ad_id AS STRING) = CAST(ad.ad_id AS STRING)
                    AND CAST(raw.account_id AS STRING) = CAST(ad.account_id AS STRING)
                LEFT JOIN `{raw_adset_metadata}` AS adset
                    ON CAST(raw.adset_id AS STRING) = CAST(adset.adset_id AS STRING)
                    AND CAST(raw.account_id AS STRING) = CAST(adset.account_id AS STRING)
                LEFT JOIN `{raw_campaign_metadata}` AS campaign
                    ON CAST(raw.campaign_id AS STRING) = CAST(campaign.campaign_id AS STRING)
                    AND CAST(raw.account_id AS STRING) = CAST(campaign.account_id AS STRING)
                LEFT JOIN `{raw_creative_metadata}` AS creative
                    ON CAST(raw.ad_id AS STRING) = CAST(creative.ad_id AS STRING)
                    AND CAST(raw.account_id AS STRING) = CAST(creative.account_id AS STRING)
            """
            try:
                df_month = google_bigquery_client.query(query).to_dataframe()
                print(f"✅ [STAGING] Successfully queried {len(df_month)} row(s) of TikTok Ads ad insights from {raw_table}.")
                logging.info(f"✅ [STAGING] Successfully queried {len(df_month)} row(s) of TikTok Ads ad insights from {raw_table}.")
            except Exception as e:
                print(f"❌ [STAGING] Failed to query TikTok Ads ad insights raw table {raw_table} due to {e}.")
                logging.warning(f"❌ [STAGING] Failed to query TikTok Ads ad insights raw table {raw_table} due to {e}.")
                continue

    # 1.2.4. Enrich insights
            if not df_month.empty:
                try:
                    print(f"🔄 [STAGING] Triggering to enrich staging TikTok Ads ad insights field(s) for {len(df_month)} row(s) from {raw_table}...")
                    logging.info(f"🔄 [STAGING] Triggering to enrich staging TikTok Ads ad insights field(s) for {len(df_month)} row(s) from {raw_table}...")
                    df_month = enrich_ad_fields(df_month, table_id=raw_table)
                    if "nhan_su" in df_month.columns:
                        df_month["nhan_su"] = df_month["nhan_su"].apply(remove_string_accents)
                    all_dfs.append(df_month)
                except Exception as e:
                    print(f"❌ [STAGING] Failed to trigger enrichment for TikTok Ads ad insights due to {e}.")
                    logging.warning(f"❌ [STAGING] Failed to trigger enrichment for TikTok Ads ad insights due to {e}.")
                    continue
        if not all_dfs:
            print("⚠️ [STAGING] No data found in any raw TikTok Ads ad insights table(s).")
            logging.warning("⚠️ [STAGING] No data found in any raw TikTok Ads ad insights table(s).")
            return
        df_all = pd.concat(all_dfs, ignore_index=True)
        print(f"✅ [STAGING] Successfully combined {len(df_all)} row(s) from all raw TikTok Ads ad insights table(s).")
        logging.info(f"✅ [STAGING] Successfully combined {len(df_all)} row(s) from all raw TikTok Ads ad insights table(s).")

    # 1.2.5. Enforce schema
        try:
            print(f"🔄 [STAGING] Triggering to enforce schema for {len(df_all)} row(s) of staging TikTok Ads ad insights...")
            logging.info(f"🔄 [STAGING] Triggering to enforce schema for {len(df_all)} row(s) of staging TikTok Ads ad insights...")
            df_all = ensure_table_schema(df_all, "staging_ad_insights")
        except Exception as e:
            print(f"❌ [STAGING] Failed to trigger schema enforcement for {len[df_all]} row(s) of staging TikTok Ads ad insights due to {e}.")
            logging.error(f"❌ [STAGING] Failed to trigger schema enforcement for {len(df_all)} row(s) of staging TikTok Ads ad insights due to {e}.")
            raise

    # 1.2.6. Upload to Google BigQuery
        try:
            print(f"🔍 [STAGING] Uploading {len(df_all)} row(s) of staging TikTok Ads ad insights to Google BigQuery table {staging_ad_insights}...")
            logging.info(f"🔍 [STAGING] Uploading {len(df_all)} row(s) of staging TikTok Ads ad insights to Google BigQuery table {staging_ad_insights}...")
            df_all = df_all.drop_duplicates()
            clustering_fields = [f for f in ["chuong_trinh", "ma_ngan_sach_cap_1", "nhan_su"] if f in df_all.columns]
            try:
                google_bigquery_client.get_table(staging_ad_insights)
                table_exists = True
            except Exception:
                table_exists = False
            if not table_exists:
                schema = []
                for col, dtype in df_all.dtypes.items():
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
                table = bigquery.Table(staging_ad_insights, schema=schema)
                if "date" in df_all.columns:
                    table.time_partitioning = bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field="date"
                    )
                    if clustering_fields:
                        table.clustering_fields = clustering_fields
                    print(f"🔍 [STAGING] Creating staging TikTok Ads ad insights table {staging_ad_insights} with partition on 'date'...")
                    logging.info(f"🔍 [STAGING] Creating staging TikTok Ads ad insights table {staging_ad_insights} with partition on 'date'...")
                table = google_bigquery_client.create_table(table)
                print(f"✅ [STAGING] Successfully created staging TikTok Ads ad insights table {staging_ad_insights}.")
                logging.info(f"✅ [STAGING] Successfully created staging TikTok Ads ad insights table {staging_ad_insights}.")
            else:
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
                    df_all,
                    staging_ad_insights,
                    job_config=job_config
                )
                load_job.result()
                print(f"✅ [STAGING] Successfully uploaded {len(df_all)} row(s) to staging TikTok Ads ad insights table {staging_ad_insights}.")
                logging.info(f"✅ [STAGING] Successfully uploaded {len(df_all)} row(s) to staging TikTok Ads ad insights table {staging_ad_insights}.")
        except Exception as e:
            print(f"❌ [STAGING] Failed during staging TikTok Ads ad insights upload due to {e}.")
            logging.error(f"❌ [STAGING] Failed during staging TikTok Ads ad insights upload due to {e}.")
    except Exception as e:
        print(f"❌ [STAGING] Faild to unify staging TikTok Ads ad insights table due to {e}.")
        logging.error(f"❌ [STAGING] Faild to unify staging TikTok Ads ad insights table due to {e}.")