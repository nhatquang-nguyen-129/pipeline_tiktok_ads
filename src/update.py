"""
==================================================================
TIKTOK UPDATE MODULE
------------------------------------------------------------------
This module performs **incremental updates** to TikTok Ads data  
at the raw layer, enabling day-by-day ingestion for specific layers  
such as campaigns, ads, or creatives without reprocessing entire months.

It is designed to support near-real-time refresh, daily sync jobs,  
or ad-hoc patching of recent TikTok Ads data.

✔️ Supports selective layer updates via parameterized control  
✔️ Reloads data by day to maintain data freshness and accuracy  
✔️ Reuses modular ingest functions for consistency across layers  

⚠️ This module is responsible for *RAW layer updates only*. It does  
not transform data or generate staging/MART tables directly.
==================================================================
"""
# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add logging ultilities for integration
import logging

# Add Python 'datetime' libraries for integration
from datetime import (
    datetime,
    timedelta,
    timezone
)

# Add Python Pandas libraries for integration
import pandas as pd

# Add Python "re" libraries for integration
import re

# Add Python 'time' libraries for integration
import time

# Add Google Authentication libraries for integration
from google.api_core.exceptions import (
    Forbidden,
    GoogleAPICallError
)
from google.auth.exceptions import DefaultCredentialsError


# Add Google API Core modules for integration
from google.api_core.exceptions import NotFound

# Add Google Cloud modules for integration
from google.cloud import bigquery

# Add Google Secret Manager modules for integration
from google.cloud import secretmanager

# Add internal TikTok Ads module for handling
from src.ingest import (
    ingest_campaign_metadata,
    ingest_ad_metadata,
    ingest_ad_creative,
    ingest_campaign_insights,
    ingest_ad_insights,
)
from src.staging import (
    staging_campaign_insights,
    staging_ad_insights
)
from src.mart import (
    mart_campaign_all,
    mart_creative_all
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

# 1. UPDATE TIKTOK ADS DATA FOR A GIVEN DATE RANGE

# 1.1. Update TikTok Ads campaign insights data for a given date range
def update_campaign_insights(start_date: str, end_date: str):
    print(f"🚀 [UPDATE] Starting to update TikTok Ads campaign insights from {start_date} to {end_date}...")
    logging.info(f"🚀 [UPDATE] Starting to update TikTok Ads campaign insights from {start_date} to {end_date}...")

    # 1.1.1. Start timing the update process
    start_time = time.time()
    print(f"🔍 [UPDATE] Proceeding to update TikTok campaign insights from {start_date} to {end_date} at {time.strftime('%Y-%m-%d %H:%M:%S')}.")
    logging.info(f"🔍 [UPDATE] Proceeding to update TikTok campaign insights from {start_date} to {end_date} at {time.strftime('%Y-%m-%d %H:%M:%S')}.")

    try:

    # 1.1.2. Triger to ingest TikTok Ads campaign insights
        try:
            print(f"🔄 [UPDATE] Triggering to ingest TikTok Ads campaign insights from {start_date} to {end_date}...")
            logging.info(f"🔄 [UPDATE] Triggering to ingest TikTok Ads campaign insights from {start_date} to {end_date}...")
            update_df_ingested = ingest_campaign_insights(
                start_date=start_date,
                end_date=end_date
            )
            updated_campaign_ids = set()
            if "campaign_id" in update_df_ingested.columns:
                updated_campaign_ids.update(update_df_ingested["campaign_id"].dropna().unique())
        except Exception as e:
            print(f"❌ [UPDATE] Failed to trigger TikTok Ads campaign insights ingestion from {start_date} to {end_date} due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to trigger TikTok Ads campaign insights ingestion from {start_date} to {end_date} due to {e}.")

    # 1.1.3. Trigger to ingest TikTok Ads campaign metadata
        if updated_campaign_ids:
            print(f"🔄 [UPDATE] Triggering to ingest TikTok Ads campaign metadata for {len(updated_campaign_ids)} campaign_id(s)...")
            logging.info(f"🔄 [UPDATE] Triggering to ingest TikTok Ads campaign metadata for {len(updated_campaign_ids)} campaign_id(s)...")
            try:
                ingest_campaign_metadata(campaign_id_list=list(updated_campaign_ids))
            except Exception as e:
                print(f"❌ [UPDATE] Failed to trigger TikTok Ads campaign metadata ingestion for {len(updated_campaign_ids)} campaign_id(s) due to {e}.")
                logging.error(f"❌ [UPDATE] Failed to trigger TikTok Ads campaign metadata ingestion for {len(updated_campaign_ids)} campaign_id(s) due to {e}.")
        else:
            print("⚠️ [UPDATE] No updated campaign_ids for TikTok Ads campaign metadata then ingestion is skipped.")
            logging.warning("⚠️ [UPDATE] No updated campaign_ids for TikTok Ads campaign metadata then ingestion is skipped.")

    # 1.1.4 Trigger to rebuild staging TikTok Ads campaign insights table
        if updated_campaign_ids:
            print("🔄 [UPDATE] Triggering to rebuild staging TikTok Ads campaign insights table...")
            logging.info("🔄 [UPDATE] Triggering to rebuild staging TikTok Ads campaign insights table...")
            try:
                staging_campaign_insights()
            except Exception as e:
                print(f"❌ [UPDATE] Failed to trigger staging table rebuild for TikTok Ads campaign insights due to {e}.")
                logging.error(f"❌ [UPDATE] Failed to trigger staging table rebuild for TikTok Ads campaign insights due to {e}.")
        else:
            print("⚠️ [UPDATE] No updates for TikTok Ads campaign insights then staging table rebuild is skipped.")
            logging.warning("⚠️ [UPDATE] No updates for TikTok Ads campaign insights then staging table rebuild is skipped.")

    # 1.1.5. Trigger to rebuild materialized TikTok Ads campaign performance table
        if updated_campaign_ids:
            print("🔄 [UPDATE] Triggering to rebuild materialized TikTok Ads campaign performance table...")
            logging.info("🔄 [UPDATE] Triggering to rebuild materialized TikTok Ads campaign performance table...")
            try:
                mart_campaign_all()
            except Exception as e:
                print(f"❌ [UPDATE] Failed to trigger materialized table rebuild for TikTok Ads campaign performance due to {e}.")
                logging.error(f"❌ [UPDATE] Failed to trigger materialized table rebuild for TikTok Ads campaign performance due to {e}.")
        else:
            print("⚠️ [UPDATE] No updates for TikTok Ads campaign insights then skip building materialized table(s).")
            logging.warning("⚠️ [UPDATE] No updates for TikTok Ads campaign insights then skip building materialized table(s).")

    # 1.1.6. Summarize update result(s)
    except Exception as e:
        print(f"❌ [UPDATE] Failed to update TikTok Ads campaign insights from {start_date} to {end_date} due to {e}.")
        logging.error(f"❌ [UPDATE] Failed to update TikTok Ads campaign insights from {start_date} to {end_date} due to {e}.")
    elapsed = round(time.time() - start_time, 2)
    print(f"🏆 [UPDATE] Successfully completed TikTok Ads campaign insights update from {start_date} to {end_date} in {elapsed}s.")
    logging.info(f"🏆 [UPDATE] Successfully completed TikTok Ads campaign insights update from {start_date} to {end_date} in {elapsed}s.")

# 1.2. Update Facebook ad insights data for a given date range
def update_ad_insights(start_date: str, end_date: str):
    print(f"🚀 [UPDATE] Starting TikTok Ads ad insights update from {start_date} to {end_date}...")
    logging.info(f"🚀 [UPDATE] Starting TikTok Ads ad insights update from {start_date} to {end_date}...")

    # 1.2.1. Start timing the update process
    start_time = time.time()

    # 1.2.2. Prepare table_id
    raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
    print(f"🔍 [UPDATE] Proceeding to update Facebook ad insights from {start_date} to {end_date}...")
    logging.info(f"🔍 [UPDATE] Proceeding to update Facebook ad insights from {start_date} to {end_date}...")

    # 1.2.3. Triger to ingest TikTok Ads Âd insights
    try:
        print(f"🔄 [UPDATE] Triggering to ingest TikTok Ads ad insights from {start_date} to {end_date}...")
        logging.info(f"🔄 [UPDATE] Triggering to ingest TikTok Ads ad insights from {start_date} to {end_date}...")
        df = ingest_ad_insights(
            start_date=start_date,
            end_date=end_date
        )
        updated_ad_ids = set()
        if "ad_id" in df.columns:
            updated_ad_ids.update(df["ad_id"].dropna().unique())
    except Exception as e:
        print(f"❌ [UPDATE] Failed to ingest TikTok Ads ad insights from {start_date} to {end_date} due to {e}.")
        logging.error(f"❌ [UPDATE] Failed to ingest TikTok Ads ad insights from {start_date} to {end_date} due to {e}.")

    # 1.2.7. Ingest Facebook ad metadata
    if updated_ad_ids:
        print(f"🔄 [UPDATE] Triggering to ingest TikTok Ads ad metadata for {len(updated_ad_ids)} ad_id(s)...")
        logging.info(f"🔄 [UPDATE] Triggering to ingest TikTok Ads ad metadata for {len(updated_ad_ids)} ad_id(s)...")
        try:
            ingest_ad_metadata(ad_id_list=list(updated_ad_ids))
        except Exception as e:
            print(f"❌ [UPDATE] Failed to trigger TikTok Ads ad metadata ingestion for {len(updated_ad_ids)} ad_id(s) due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to trigger TikTok ad metadata ingestion for {len(updated_ad_ids)} ad_id(s) due to {e}.")

    # 1.2.9. Ingest Facebook ad creative
        print(f"🔄 [UPDATE] Triggering to ingest Facebook ad creative for {len(updated_ad_ids)} ad_id(s)...")
        logging.info(f"🔄 [UPDATE] Triggering to ingest Facebook ad creative for {len(updated_ad_ids)} ad_id(s)...")
        try:
            ingest_ad_creative(ad_id_list=list(updated_ad_ids))
        except Exception as e:
            print(f"❌ [UPDATE] Failed to trigger Facebook ad creative ingestion for {len(updated_ad_ids)} ad_id(s) due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to trigger Facebook ad creative ingestion for {len(updated_ad_ids)} ad_id(s) due to {e}.")
    else:
        print("⚠️ [UPDATE] No updated ad_id(s) for Facebook ad metadata then ingestion is skipped.")
        logging.warning("⚠️ [UPDATE] No updated ad_id(s) for Facebook ad metadata then ingestion is skipped.")

    # 1.2.10. Rebuild staging Facebook ad insights table
    if updated_ad_ids:
        print("🔄 [UPDATE] Triggering to rebuild staging Facebook ad insights table...")
        logging.info("🔄 [UPDATE] Triggering to rebuild staging Facebook ad insights table...")
        try:
            staging_ad_insights()
        except Exception as e:
            print(f"❌ [UPDATE] Failed to trigger staging table rebuild for Facebook ad insights due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to trigger staging table rebuild for Facebook ad insights due to {e}.")
    else:
        print("⚠️ [UPDATE] No updated for Facebook ad insights then staging table rebuild is skipped.")
        logging.warning("⚠️ [UPDATE] No updated for Facebook ad insights then staging table rebuild is skipped.")

    # 1.2.11. Rebuild materialized Facebook creative performance
    if updated_ad_ids:
        print("🔄 [UPDATE] Triggering to rebuild materialized Facebook creative performance table...")
        logging.info("🔄 [UPDATE] Triggering to rebuild materialized Facebook creative performance table...")
        try:
            mart_creative_all()
        except Exception as e:
            print(f"❌ [UPDATE] Failed to trigger materialized table rebuild for Facebook creative performance due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to trigger materialized table rebuild for Facebook creative performance due to {e}.")

    else:
        print("⚠️ [UPDATE] No updated for Facebook ad insights then skip building festival creative materialized table.")
        logging.warning("⚠️ [UPDATE] No updated for Facebook ad insights then skip building festival creative materialized table.")

    # 1.2.13. Measure the total execution time
    elapsed = round(time.time() - start_time, 2)
    print(f"✅ [UPDATE] Successfully completed Facebook Ads ad insights update in {elapsed}s.")
    logging.info(f"✅ [UPDATE] Successfully completed Facebook Ads ad insights update in {elapsed}s.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run Facebook Campaign Backfill")
    parser.add_argument("--start_date", type=str, required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end_date", type=str, required=True, help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    update_ad_insights(start_date=args.start_date, end_date=args.end_date)
