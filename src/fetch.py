"""
==================================================================
TIKTOK FETCHING MODULE
------------------------------------------------------------------
This module provides a robust interface for fetching raw advertising 
data directly from the TikTok Marketing API. It manages authentication, 
pagination, and error handling to ensure reliable extraction of campaign, 
ad group, and ad-level insights for downstream analytics.

By isolating API-fetching responsibilities, this module ensures 
clean separation of concerns—making the ingestion layer more maintainable, 
testable, and resilient within the broader data pipeline architecture.

✔️ Initializes and authenticates a secure TikTok Marketing API client  
✔️ Retrieves raw campaign, ad group, and ad-level performance data  
✔️ Handles pagination, retries, and error responses gracefully  
✔️ Normalizes and structures JSON responses into tabular format  
✔️ Supports parameterized date range and dimension-based querying  

⚠️ This module is strictly limited to *data retrieval operations*.  
It does **not** perform data transformation, schema validation, or 
persistence tasks such as loading data into BigQuery or other storage systems.
==================================================================
"""

# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add Python JSON ultilities for integration
import json

# Add Python logging ultilties for integration
import logging

# Add Python Pandas libraries for integration
import pandas as pd

# Add Python requests ultilities for integration
import requests

# Add Python time ultilities for integration
import time

# Add Google Authentication libraries for integration
from google.api_core.exceptions import (
    GoogleAPICallError,
    NotFound,
    PermissionDenied, 
)
from google.auth import default
from google.auth.exceptions import DefaultCredentialsError

# Add Google Secret Manager modules for integration
from google.cloud import secretmanager

# Add internal TikTok Ads modules for handling
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

# 1. FETCH TIKTOK ADS METADATA

# 1.1. Fetch campaign metadata for TikTok Ads
def fetch_campaign_metadata(campaign_id_list: list[str]) -> pd.DataFrame:
    print(f"🚀 [FETCH] Starting to fetch TikTok Ads campaign metadata for {len(campaign_id_list)} campaign_id(s)...")
    logging.info(f"🚀 [FETCH] Starting to fetch TikTok Ads campaign metadata for {len(campaign_id_list)} campaign_id(s)...")

    # 1.1.1. Start timing the TikTok Ads campaign metadata fetching process
    start_time = time.time()
    fetch_section_succeeded = {}
    fetch_section_failed = [] 
    print(f"🔍 [FETCH] Proceeding to fetch TikTok Ads campaign metadata for {len(campaign_id_list)} campaign_id(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}.")
    logging.info(f"🔍 [FETCH] Proceeding to fetch TikTok Ads campaign metadata for {len(campaign_id_list)} campaign_id(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}.")

    # 1.1.2. Validate input for TikTok Ads campaign metadata fetching
    if not campaign_id_list:
        fetch_section_succeeded["1.1.2. Validate input for TikTok Ads campaign metadata"] = False
        fetch_section_failed.append("1.1.2. Validate input for TikTok Ads campaign metadata")
        print("⚠️ [FETCH] Empty TikTok Ads campaign_id_list provided then fetching is suspended.")
        logging.warning("⚠️ [FETCH] Empty TikTok Ads campaign_id_list provided then fetching is suspended.")
        raise ValueError("⚠️ [FETCH] Empty TikTok Ads campaign_id_list provided then fetching is suspended.")

    # 1.1.3. Prepare field(s) for TikTok Ads campaign metadata fetching
    fetch_fields_default = [
        "advertiser_id",
        "campaign_id",
        "campaign_name",
        "operation_status",
        "objective_type",
        "objective",
        "create_time"
    ]
    all_records = []
    print(f"🔍 [FETCH] Preparing to fetch TikTok Ads campaign metadata with {fetch_fields_default} field(s)...")
    logging.info(f"🔍 [FETCH] Preparing to fetch TikTok Ads campaign metadata with {fetch_fields_default} field(s)...")
    
    # 1.1.4 Initialize Google Secret Manager client
    try:
        print(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
        logging.info(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
        google_secret_client = secretmanager.SecretManagerServiceClient()
        print(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
        logging.info(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
        fetch_section_succeeded["1.1.4 Initialize Google Secret Manager client"] = True
    except Exception as e:
        fetch_section_succeeded["1.1.4 Initialize Google Secret Manager client"] = False
        fetch_section_failed.append("1.1.4 Initialize Google Secret Manager client")
        print(f"❌ [FETCH] Failed to initialize Google Secret Manager client for Google Cloud Platform project {PROJECT} due to {e}.")
        logging.error(f"❌ [FETCH] Failed to initialize Google Secret Manager client for Google Cloud Platform project {PROJECT} due to {e}.")
        raise RuntimeError(f"❌ [FETCH] Failed to initialize Google Secret Manager client for Google Cloud Platform project {PROJECT} due to {e}.") from e

    # 1.1.5. Get TikTok Ads access token from Google Secret Manager
    try: 
        token_secret_id = f"{COMPANY}_secret_all_{PLATFORM}_token_access_user"
        token_secret_name = f"projects/{PROJECT}/secrets/{token_secret_id}/versions/latest"
        token_secret_response = google_secret_client.access_secret_version(request={"name": token_secret_name})
        token_access_user = token_secret_response.payload.data.decode("utf-8")
        print(f"✅ [FETCH] Successfully retrieved TikTok access token for account {ACCOUNT} from Google Secret Manager.")
        logging.info(f"✅ [FETCH] Successfully retrieved TikTok access token for account {ACCOUNT} from Google Secret Manager.")
        fetch_section_succeeded["1.1.5. Get TikTok Ads access token from Google Secret Manager"] = True
    except Exception as e:
        fetch_section_succeeded["1.1.5. Get TikTok Ads access token from Google Secret Manager"] = False
        fetch_section_failed.append("1.1.5. Get TikTok Ads access token from Google Secret Manager")
        print(f"❌ [FETCH] Failed to retrieve TikTok access token for {ACCOUNT} from Google Secret Manager due to {e}.")
        logging.error(f"❌ [FETCH] Failed to retrieve TikTok access token for {ACCOUNT} from Google Secret Manager due to {e}.")
        raise RuntimeError(f"❌ [FETCH] Failed to retrieve TikTok access token for {ACCOUNT} from Google Secret Manager due to {e}.")

    # 1.1.6. Get TikTok Ads advertiser_id from Google Secret Manager
    try:
        print(f"🔍 [FETCH] Retrieving TikTok Ads access_token for account {ACCOUNT} from Google Secret Manager...")
        logging.info(f"🔍 [FETCH] Retrieving TikTok Ads access_token for account {ACCOUNT} from Google Secret Manager...")
        advertiser_secret_id = f"{COMPANY}_secret_{DEPARTMENT}_tiktok_account_id_{ACCOUNT}"
        advertiser_secret_name = f"projects/{PROJECT}/secrets/{advertiser_secret_id}/versions/latest"
        advertiser_secret_response = google_secret_client.access_secret_version(request={"name": advertiser_secret_name})
        advertiser_id = advertiser_secret_response.payload.data.decode("utf-8")
        print(f"✅ [FETCH] Successfully retrieved TikTok Ads advertiser_id {advertiser_id} from Google Secret Manager.")
        logging.info(f"✅ [FETCH] Successfully retrieved TikTok Ads advertiser_id {advertiser_id} from Google Secret Manager.")
        fetch_section_succeeded["1.1.6. Get TikTok Ads advertiser_id from Google Secret Manager"] = True
    except Exception as e:
        fetch_section_succeeded["1.1.6. Get TikTok Ads advertiser_id from Google Secret Manager"] = False
        fetch_section_failed.append("1.1.6. Get TikTok Ads advertiser_id from Google Secret Manager")
        print(f"❌ [FETCH] Failed to retrieve TikTok Ads access token for {ACCOUNT} from Google Secret Manager due to {e}.")
        logging.error(f"❌ [FETCH] Failed to retrieve TikTok Ads access token for {ACCOUNT} from Google Secret Manager due to {e}.")
        raise RuntimeError(f"❌ [FETCH] Failed to retrieve TikTok Ads access token for {ACCOUNT} from Google Secret Manager due to {e}.")

    # 1.1.7. Make TikTok Ads API call for advertiser endpoint
    advertiser_info_url = "https://business-api.tiktok.com/open_api/v1.3/advertiser/info/"
    advertiser_info_headers = {
        "Access-Token": token_access_user,
        "Content-Type": "application/json"
    }
    try: 
        print(f"🔍 [FETCH] Retrieving advertiser_name for TikTok Ads advertiser_id {advertiser_id}...")
        logging.info(f"🔍 [FETCH] Retrieving advertiser_name for TikTok Ads advertiser_id {advertiser_id}...")
        payload = {"advertiser_ids": [advertiser_id]}
        response = requests.get(advertiser_info_url, headers=advertiser_info_headers, json=payload)
        advertiser_name = response.json()["data"]["list"][0]["name"]       
        print(f"✅ [FETCH] Successfully retrieved advertiser_name {advertiser_name} for TikTok Ads advertiser_id {advertiser_id}.")
        logging.info(f"✅ [FETCH] Successfully retrieved advertiser_name {advertiser_name} for TikTok Ads advertiser_id {advertiser_id}.")
        fetch_section_succeeded["1.1.7. Make TikTok Ads API call for advertiser endpoint"] = True
    except Exception as e:
        fetch_section_succeeded["1.1.7. Make TikTok Ads API call for advertiser endpoint"] = False
        fetch_section_failed.append("1.1.7. Make TikTok Ads API call for advertiser endpoint")
        print(f"❌ [FETCH] Failed to fetch advertiser_name for TikTok Ads advertiser_id {advertiser_id} due to {e}.")
        logging.error(f"❌ [FETCH] Failed to fetch advertiser_name for TikTok Ads advertiser_id {advertiser_id} due to {e}.")

    # 1.1.8. Make TikTok Ads API call for campaign endpoint
    campaign_get_url = "https://business-api.tiktok.com/open_api/v1.3/campaign/get/"
    campaign_get_headers = {
        "Access-Token": token_access_user,
        "Content-Type": "application/json"
    }
    try:
        print(f"🔍 [FETCH] Retrieving TikTok Ads campaign metadata for {len(campaign_id_list)} campaign_id(s)...")
        logging.info(f"🔍 [FETCH] Retrieving TikTok Ads campaign metadata for {len(campaign_id_list)} campaign_id(s)...")
        for campaign_id in campaign_id_list:
            try:
                payload = {
                    "advertiser_id": advertiser_id,
                    "filtering": {"campaign_ids": [campaign_id]},
                    "fields": fetch_fields_default
                }
                response = requests.get(campaign_get_url, headers=campaign_get_headers, json=payload)
                response.raise_for_status()
                data = response.json()
                record = data["data"]["list"][0]
                record["advertiser_name"] = advertiser_name
                all_records.append(record)
            except Exception as e:
                print(f"⚠️ [FETCH] Failed to retrieve TikTok Ads campaign metadata for campaign_id {campaign_id} due to {e}.")
                logging.warning(f"⚠️ [FETCH] Failed to retrieve TikTok Ads campaign metadata for campaign_id {campaign_id} due to {e}.")
        if not all_records:
            raise RuntimeError("❌ [FETCH] Failed to retrieve TikTok Ads campaign metadata for any campaign_id then fetching is suspened.")
        print(f"✅ [FETCH] Successfully retrieved {len(all_records)} row(s) of TikTok Ads campaign metadata.")
        logging.info(f"✅ [FETCH] Successfully retrieved {len(all_records)} row(s) of TikTok Ads campaign metadata.")
        fetch_section_succeeded["1.1.8. Make TikTok Ads API call for campaign endpoint"] = True
    except Exception as e:
        fetch_section_succeeded["1.1.8. Make TikTok Ads API call for campaign endpoint"] = False
        fetch_section_failed.append("1.1.8. Make TikTok Ads API call for campaign endpoint")
        print(f"❌ [FETCH] Failed to retrieve TikTok Ads campaign metadata due to {e}.")
        logging.error(f"❌ [FETCH] Failed to retrieve TikTok Ads campaign metadata due to {e}.")
        return pd.DataFrame()

    # 1.1.9. Convert TikTok Ads campaign metadata to Python DataFrame
    try:
        print(f"🔄 [FETCH] Converting TikTok Ads campaign metadata to Python DataFrame...")
        logging.info(f"🔄 [FETCH] Converting TikTok Ads campaign metadata to Python DataFrame...")
        fetch_df_flattened = pd.DataFrame(all_records)
        print(f"✅ [FETCH] Successfully Converted TikTok Ads campaign metadata to Python DataFrame with {len(fetch_df_flattened)} row(s).")
        logging.info(f"✅ [FETCH] Successfully Converted TikTok Ads campaign metadata to Python DataFrame with {len(fetch_df_flattened)} row(s).")
        fetch_section_succeeded["1.1.9. Convert TikTok Ads campaign metadata to Python DataFrame"] = True
    except Exception as e:
        fetch_section_succeeded["1.1.9. Convert TikTok Ads campaign metadata to Python DataFrame"] = False
        fetch_section_failed.append("1.1.9. Convert TikTok Ads campaign metadata to Python DataFrame")
        print(f"❌ [FETCH] Failed to convert TikTok Ads campaign metadata to Python DataFrame due to {e}.")
        logging.error(f"❌ [FETCH] Failed to convert TikTok Ads campaign metadata to Python DataFrame due to {e}.")
        return pd.DataFrame()

    # 1.1.10. Enforce schema for Python DataFrame
    try:
        print(f"🔄 [FETCH] Trigger to enforce schema for {len(fetch_df_flattened)} row(s) of TikTok Ads campaign metadata...")
        logging.info(f"🔄 [FETCH] Trigger to enforce schema for {len(fetch_df_flattened)} row(s) of TikTok Ads campaign metadata....")
        fetch_df_enforced = ensure_table_schema(fetch_df_flattened, "fetch_campaign_metadata")
    except Exception as e:
        fetch_section_succeeded["1.1.10. Enforce schema for Python DataFrame"] = False
        fetch_section_failed.append("1.1.10. Enforce schema for Python DataFrame")
        print(f"❌ [FETCH] Failed to trigger schema enforcement for TikTok Ads campaign metadata due to {e}.")
        logging.error(f"❌ [FETCH] Failed to trigger schema enforcement for TikTok Ads campaign metadata due to {e}.")
        return pd.DataFrame()

    # 1.2.11. Summarize ingestion result(s)
    finally:
        elapsed = round(time.time() - start_time, 2)
        if fetch_section_failed:
            print(f"❌ [FETCH] Failed to completed TikTok Ads campaign metadata fetching due to unsuccesfull section(s) {', '.join(fetch_section_failed)}.")
            logging.error(f"❌ [FETCH] Failed to completed TikTok Ads campaign metadata fetching due to unsuccesfull section(s) {', '.join(fetch_section_failed)}.")
            mart_status_def = "failed"
        else:
            fetch_df_final = fetch_df_enforced
            print(f"🏆 [FETCH] Successfully completed TikTok Ads campaign metadata fetching with {len(fetch_df_final)} row(s) in {elapsed}s.")
            logging.info(f"🏆 [FETCH] Successfully completed TikTok Ads campaign metadata fetching with {len(fetch_df_final)} row(s) in {elapsed}s.")
            mart_status_def = "success"
            return fetch_df_final
        return {"status": mart_status_def, "elapsed_seconds": elapsed, "failed_sections": mart_section_failed}

# 1.2. Fetch ad metadata for TikTok Ads
def fetch_ad_metadata(ad_id_list: list[str]) -> pd.DataFrame:
    print(f"🚀 [FETCH] Starting to fetch TikTok Ads ad metadata(s) for {len(ad_id_list)} ad_id(s)...")
    logging.info(f"🚀 [FETCH] Starting to fetch TikTok Ads ad metadata(s) for {len(ad_id_list)} ad_id(s)...")

    # 1.2.1. Start timing the TikTok Ads ad metadata fetching process
    start_time = time.time()
    fetch_section_succeeded = {}
    fetch_section_failed = [] 
    print(f"🔍 [FETCH] Proceeding to fetch TikTok Ads ad metadata for {len(ad_id_list)} ad_id(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [FETCH] Proceeding to fetch TikTok Ads ad metadata for {len(ad_id_list)} ad_id(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    # 1.2.1. Validate input for TikTok Ads ad metadata fetching
    if not ad_id_list:
        fetch_section_succeeded["1.2.1. Validate input for TikTok Ads ad metadata"] = False
        fetch_section_failed.append("1.2.1. Validate input for TikTok Ads ad metadata")
        print("⚠️ [FETCH] Empty TikTok Ads ad_id_list provided then fetching is suspended.")
        logging.warning("⚠️ [FETCH] Empty TikTok Ads ad_id_list provided then fetching is suspended.")
        raise ValueError("⚠️ [FETCH] Empty TikTok Ads ad_id_list provided then fetching is suspended.")

    # 1.2.3. Prepare field(S) for TikTok Ads ad metadata fetching
    fetch_fields_default = [
        "advertiser_id",
        "ad_id",
        "ad_name",
        "adgroup_id",
        "adgroup_name",
        "campaign_id",
        "campaign_name",
        "operation_status",
        "create_time",
        "ad_format",
        "optimization_event",
        "video_id",
        "image_ids"
    ]
    all_records = []
    print(f"🔍 [FETCH] Preparing to fetch TikTok ad metadata with {fetch_fields_default} field(s)...")
    logging.info(f"🔍 [FETCH] Preparing to fetch TikTok ad metadata with {fetch_fields_default} field(s)...")

    # 1.2.4 Initialize Google Secret Manager client
    try:
        print(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
        logging.info(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
        google_secret_client = secretmanager.SecretManagerServiceClient()
        print(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
        logging.info(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
        fetch_section_succeeded["1.2.4 Initialize Google Secret Manager client"] = True
    except Exception as e:
        fetch_section_succeeded["1.2.4 Initialize Google Secret Manager client"] = False
        fetch_section_failed.append("1.2.4 Initialize Google Secret Manager client")
        print(f"❌ [FETCH] Failed to initialize Google Secret Manager client for Google Cloud Platform project {PROJECT} due to {e}.")
        logging.error(f"❌ [FETCH] Failed to initialize Google Secret Manager client for Google Cloud Platform project {PROJECT} due to {e}.")
        raise RuntimeError(f"❌ [FETCH] Failed to initialize Google Secret Manager client for Google Cloud Platform project {PROJECT} due to {e}.") from e

    # 1.2.5. Get TikTok Ads access token from Google Secret Manager
    try: 
        token_secret_id = f"{COMPANY}_secret_all_{PLATFORM}_token_access_user"
        token_secret_name = f"projects/{PROJECT}/secrets/{token_secret_id}/versions/latest"
        token_secret_response = google_secret_client.access_secret_version(request={"name": token_secret_name})
        token_access_user = token_secret_response.payload.data.decode("utf-8")
        print(f"✅ [FETCH] Successfully retrieved TikTok access token for account {ACCOUNT} from Google Secret Manager.")
        logging.info(f"✅ [FETCH] Successfully retrieved TikTok access token for account {ACCOUNT} from Google Secret Manager.")
        fetch_section_succeeded["1.2.5. Get TikTok Ads access token from Google Secret Manager"] = True
    except Exception as e:
        fetch_section_succeeded["1.2.5. Get TikTok Ads access token from Google Secret Manager"] = False
        fetch_section_failed.append("1.2.5. Get TikTok Ads access token from Google Secret Manager")
        print(f"❌ [FETCH] Failed to retrieve TikTok access token for {ACCOUNT} from Google Secret Manager due to {e}.")
        logging.error(f"❌ [FETCH] Failed to retrieve TikTok access token for {ACCOUNT} from Google Secret Manager due to {e}.")
        raise RuntimeError(f"❌ [FETCH] Failed to retrieve TikTok access token for {ACCOUNT} from Google Secret Manager due to {e}.")

    # 1.2.6. Get TikTok Ads advertiser_id from Google Secret Manager
    try:
        print(f"🔍 [FETCH] Retrieving TikTok Ads access_token for account {ACCOUNT} from Google Secret Manager...")
        logging.info(f"🔍 [FETCH] Retrieving TikTok Ads access_token for account {ACCOUNT} from Google Secret Manager...")
        advertiser_secret_id = f"{COMPANY}_secret_{DEPARTMENT}_tiktok_account_id_{ACCOUNT}"
        advertiser_secret_name = f"projects/{PROJECT}/secrets/{advertiser_secret_id}/versions/latest"
        advertiser_secret_response = google_secret_client.access_secret_version(request={"name": advertiser_secret_name})
        advertiser_id = advertiser_secret_response.payload.data.decode("utf-8")
        print(f"✅ [FETCH] Successfully retrieved TikTok Ads advertiser_id {advertiser_id} from Google Secret Manager.")
        logging.info(f"✅ [FETCH] Successfully retrieved TikTok Ads advertiser_id {advertiser_id} from Google Secret Manager.")
        fetch_section_succeeded["1.1.6. Get TikTok Ads advertiser_id from Google Secret Manager"] = True
    except Exception as e:
        fetch_section_succeeded["1.1.6. Get TikTok Ads advertiser_id from Google Secret Manager"] = False
        fetch_section_failed.append("1.1.6. Get TikTok Ads advertiser_id from Google Secret Manager")
        print(f"❌ [FETCH] Failed to retrieve TikTok Ads access token for {ACCOUNT} from Google Secret Manager due to {e}.")
        logging.error(f"❌ [FETCH] Failed to retrieve TikTok Ads access token for {ACCOUNT} from Google Secret Manager due to {e}.")
        raise RuntimeError(f"❌ [FETCH] Failed to retrieve TikTok Ads access token for {ACCOUNT} from Google Secret Manager due to {e}.")
    
    # 1.2.7. Make TikTok Ads API call for advertiser endpoint
    advertiser_info_url = "https://business-api.tiktok.com/open_api/v1.3/advertiser/info/"
    advertiser_info_headers = {
        "Access-Token": token_access_user,
        "Content-Type": "application/json"
    }
    try: 
        print(f"🔍 [FETCH] Retrieving advertiser_name for TikTok Ads advertiser_id {advertiser_id}...")
        logging.info(f"🔍 [FETCH] Retrieving advertiser_name for TikTok Ads advertiser_id {advertiser_id}...")
        payload = {"advertiser_ids": [advertiser_id]}
        response = requests.get(advertiser_info_url, headers=advertiser_info_headers, json=payload)
        advertiser_name = response.json()["data"]["list"][0]["name"]       
        print(f"✅ [FETCH] Successfully retrieved advertiser_name {advertiser_name} for TikTok Ads advertiser_id {advertiser_id}.")
        logging.info(f"✅ [FETCH] Successfully retrieved advertiser_name {advertiser_name} for TikTok Ads advertiser_id {advertiser_id}.")
        fetch_section_succeeded["1.2.7. Make TikTok Ads API call for advertiser endpoint"] = True
    except Exception as e:
        fetch_section_succeeded["1.2.7. Make TikTok Ads API call for advertiser endpoint"] = False
        fetch_section_failed.append("1.2.7. Make TikTok Ads API call for advertiser endpoint")
        print(f"❌ [FETCH] Failed to fetch advertiser_name for TikTok Ads advertiser_id {advertiser_id} due to {e}.")
        logging.error(f"❌ [FETCH] Failed to fetch advertiser_name for TikTok Ads advertiser_id {advertiser_id} due to {e}.")

    # 1.2.8. Make TikTok Ads API call for ad endpoint
    ad_get_url = "https://business-api.tiktok.com/open_api/v1.3/ad/get/"
    ad_get_headers = {
        "Access-Token": token_access_user,
        "Content-Type": "application/json"
    }
    try:
        print(f"🔍 [FETCH] Retrieving TikTok Ads ad metadata for {len(ad_id_list)} ad_id(s)...")
        logging.info(f"🔍 [FETCH] Retrieving TikTok Ads ad metadata for {len(ad_id_list)} ad_id(s)...")
        for ad_id in ad_id_list:
            try:
                payload = {
                    "advertiser_id": advertiser_id,
                    "filtering": {"ad_ids": [str(ad_id)]},
                    "fields": fetch_fields_default
                }
                response = requests.get(ad_get_url, headers=ad_get_headers, json=payload)
                response.raise_for_status()
                data = response.json()
                if data.get("code") == 0 and data.get("data", {}).get("list"):
                    record = data["data"]["list"][0]
                    record["advertiser_name"] = advertiser_name
                    all_records.append(record)
                else:
                    print(f"⚠️ [FETCH] No ad metadata returned for TikTok Ads ad_id {ad_id}.")
                    logging.warning(f"⚠️ [FETCH] No ad metadata returned for TikTok Ads ad_id {ad_id}.")
            except Exception as e:
                print(f"⚠️ [FETCH] Failed to retrieve TikTok Ads ad metadata for ad_id {ad_id} due to {e}.")
                logging.warning(f"⚠️ [FETCH] Failed to retrieve TikTok Ads ad metadata for ad_id {ad_id} due to {e}.")
        if not all_records:
            raise RuntimeError("❌ [FETCH] Failed to retrieve TikTok Ads ad metadata for any ad_id then fetching is suspended.")
        print(f"✅ [FETCH] Successfully retrieved {len(all_records)} row(s) of TikTok Ads ad metadata.")
        logging.info(f"✅ [FETCH] Successfully retrieved {len(all_records)} row(s) of TikTok Ads ad metadata.")
        fetch_section_succeeded["1.2.8. Make TikTok Ads API call for ad endpoint"] = True
    except Exception as e:
        fetch_section_succeeded["1.2.8. Make TikTok Ads API call for ad endpoint"] = False
        fetch_section_failed.append("1.2.8. Make TikTok Ads API call for ad endpoint")
        print(f"❌ [FETCH] Failed to retrieve TikTok Ads ad metadata due to {e}.")
        logging.error(f"❌ [FETCH] Failed to retrieve TikTok Ads ad metadata due to {e}.")
        return pd.DataFrame()

    # 1.2.9. Convert TikTok Ads ad metadata to Python DataFrame
    try:
        print(f"🔄 [FETCH] Converting TikTok Ads ad metadata to Python DataFrame...")
        logging.info(f"🔄 [FETCH] Converting TikTok Ads ad metadata to Python DataFrame...")
        fetch_df_flattened = pd.DataFrame(all_records)
        print(f"✅ [FETCH] Successfully converted TikTok Ads ad metadata to Python DataFrame with {len(fetch_df_flattened)} row(s).")
        logging.info(f"✅ [FETCH] Successfully converted TikTok Ads ad metadata to Python DataFrame with {len(fetch_df_flattened)} row(s).")
        fetch_section_succeeded["1.2.9. Convert TikTok Ads ad metadata to Python DataFrame"] = True
    except Exception as e:
        fetch_section_succeeded["1.2.9. Convert TikTok Ads ad metadata to Python DataFrame"] = False
        fetch_section_failed.append("1.2.9. Convert TikTok Ads ad metadata to Python DataFrame")
        print(f"❌ [FETCH] Failed to convert TikTok Ads ad metadata to Python DataFrame due to {e}.")
        logging.error(f"❌ [FETCH] Failed to convert TikTok Ads ad metadata to Python DataFrame due to {e}.")
        return pd.DataFrame()

    # 1.210. Enforce schema for Python DataFrame
    try:
        print(f"🔄 [FETCH] Trigger to enforce schema for {len(fetch_df_flattened)} row(s) of TikTok Ads ad metadata...")
        logging.info(f"🔄 [FETCH] Trigger to enforce schema for {len(fetch_df_flattened)} row(s) of TikTok Ads ad metadata....")
        fetch_df_enforced = ensure_table_schema(fetch_df_flattened, "fetch_ad_metadata")
    except Exception as e:
        print(f"❌ [FETCH] Failed to trigger schema enforcement for TikTok Ads ad metadata due to {e}.")
        logging.error(f"❌ [FETCH] Failed to trigger schema enforcement for TikTok Ads ad metadata due to {e}.")
        return pd.DataFrame()

    # 1.2.11. Summarize ingestion result(s)
    finally:
        elapsed = round(time.time() - start_time, 2)
        if fetch_section_failed:
            print(f"❌ [FETCH] Failed to completed TikTok Ads ad metadata fetching due to unsuccesfull section(s) {', '.join(fetch_section_failed)}.")
            logging.error(f"❌ [FETCH] Failed to completed TikTok Ads ad metadata fetching due to unsuccesfull section(s) {', '.join(fetch_section_failed)}.")
            mart_status_def = "failed"
        else:
            fetch_df_final = fetch_df_enforced
            print(f"🏆 [FETCH] Successfully completed TikTok Ads ad metadata fetching with {len(fetch_df_final)} row(s) in {elapsed}s.")
            logging.info(f"🏆 [FETCH] Successfully completed TikTok Ads ad metadata fetching with {len(fetch_df_final)} row(s) in {elapsed}s.")
            mart_status_def = "success"
            return fetch_df_final
        return {"status": mart_status_def, "elapsed_seconds": elapsed, "failed_sections": fetch_section_failed}

# 1.3. Fetch ad creative for TikTok Ads
def fetch_ad_creative() -> pd.DataFrame:
    print("🚀 [FETCH] Starting to fetch TikTok Ads ad creative(s)...")
    logging.info("🚀 [FETCH] Starting to fetch TikTok Ads ad creative(s)...")

    start_time = time.time()
    fetch_section_succeeded = {}
    fetch_section_failed = []

    # 1.3.1. Initialize Google Secret Manager client
    try:
        google_secret_client = secretmanager.SecretManagerServiceClient()
        fetch_section_succeeded["1.3.1 Initialize Google Secret Manager client"] = True
    except Exception as e:
        fetch_section_succeeded["1.3.1 Initialize Google Secret Manager client"] = False
        fetch_section_failed.append("1.3.1 Initialize Google Secret Manager client")
        print(f"❌ [FETCH] Failed to initialize Google Secret Manager client due to {e}.")
        logging.error(f"❌ [FETCH] Failed to initialize Google Secret Manager client due to {e}.")
        raise RuntimeError(e)

    # 1.3.2. Get TikTok Ads access token
    try:
        token_secret_id = f"{COMPANY}_secret_all_{PLATFORM}_token_access_user"
        token_secret_name = f"projects/{PROJECT}/secrets/{token_secret_id}/versions/latest"
        token_secret_response = google_secret_client.access_secret_version(request={"name": token_secret_name})
        token_access_user = token_secret_response.payload.data.decode("utf-8")
        fetch_section_succeeded["1.3.2 Get TikTok Ads access token"] = True
    except Exception as e:
        fetch_section_succeeded["1.3.2 Get TikTok Ads access token"] = False
        fetch_section_failed.append("1.3.2 Get TikTok Ads access token")
        print(f"❌ [FETCH] Failed to retrieve TikTok access token due to {e}.")
        logging.error(f"❌ [FETCH] Failed to retrieve TikTok access token due to {e}.")
        raise RuntimeError(e)

    # 1.3.3. Get advertiser_id
    try:
        advertiser_secret_id = f"{COMPANY}_secret_{DEPARTMENT}_tiktok_account_id_{ACCOUNT}"
        advertiser_secret_name = f"projects/{PROJECT}/secrets/{advertiser_secret_id}/versions/latest"
        advertiser_secret_response = google_secret_client.access_secret_version(request={"name": advertiser_secret_name})
        advertiser_id = advertiser_secret_response.payload.data.decode("utf-8")
        fetch_section_succeeded["1.3.3 Get TikTok Ads advertiser_id"] = True
    except Exception as e:
        fetch_section_succeeded["1.3.3 Get TikTok Ads advertiser_id"] = False
        fetch_section_failed.append("1.3.3 Get TikTok Ads advertiser_id")
        print(f"❌ [FETCH] Failed to retrieve advertiser_id due to {e}.")
        logging.error(f"❌ [FETCH] Failed to retrieve advertiser_id due to {e}.")
        raise RuntimeError(e)

    # 1.3.4. Make TikTok Ads API call for file/video/ad/search endpoint
    video_records = []
    video_search_url = "https://business-api.tiktok.com/open_api/v1.3/file/video/ad/search/"
    video_search_headers = {
        "Access-Token": token_access_user,
        "Content-Type": "application/json"
    }

    try:
        print(f"🔍 [FETCH] Retrieving video creatives for advertiser_id {advertiser_id}...")
        logging.info(f"🔍 [FETCH] Retrieving video creatives for advertiser_id {advertiser_id}...")

        page = 1
        has_more = True

        while has_more:
            payload = {
                "advertiser_id": advertiser_id,
                "page_size": 100,  # Max allowed by API
                "page": page
            }
            response = requests.get(video_search_url, headers=video_search_headers, json=payload)
            response.raise_for_status()
            data = response.json()

            if data.get("code") == 0 and data.get("data", {}).get("list"):
                for record in data["data"]["list"]:
                    video_records.append({
                        "advertiser_id": advertiser_id,
                        "video_id": record.get("video_id"),
                        "video_cover_url": record.get("cover_url"),
                        "preview_url": record.get("preview_url"),
                        "create_time": record.get("create_time")
                    })
                has_more = data["data"].get("page_info", {}).get("total_page", 1) > page
                page += 1
            else:
                has_more = False

        if not video_records:
            raise RuntimeError("❌ [FETCH] No video creative record retrieved then fetching is suspended.")

        fetch_section_succeeded["1.3.4. Make TikTok Ads API call for file/video/ad/search endpoint"] = True
        print(f"✅ [FETCH] Successfully retrieved {len(video_records)} TikTok Ads video creative record(s).")

    except Exception as e:
        fetch_section_succeeded["1.3.4. Make TikTok Ads API call for file/video/ad/search endpoint"] = False
        fetch_section_failed.append("1.3.4. Make TikTok Ads API call for file/video/ad/search endpoint")
        print(f"❌ [FETCH] Failed to retrieve video creatives due to {e}.")
        logging.error(f"❌ [FETCH] Failed to retrieve video creatives due to {e}.")
        return pd.DataFrame()

    # 1.3.5. Enforce schema
    try:
        df_creative = pd.DataFrame(video_records)
        fetch_df_enforced = ensure_table_schema(df_creative, "fetch_ad_creative")
        fetch_section_succeeded["1.3.5. Enforce schema for TikTok Ads ad creative"] = True
    except Exception as e:
        fetch_section_succeeded["1.3.5. Enforce schema for TikTok Ads ad creative"] = False
        fetch_section_failed.append("1.3.5. Enforce schema for TikTok Ads ad creative")
        print(f"❌ [FETCH] Failed to enforce schema for TikTok Ads ad creative due to {e}.")
        logging.error(f"❌ [FETCH] Failed to enforce schema for TikTok Ads ad creative due to {e}.")
        return pd.DataFrame()

    # 1.3.6. Summarize
    finally:
        elapsed = round(time.time() - start_time, 2)
        if fetch_section_failed:
            print(f"❌ [FETCH] TikTok Ads ad creative fetching failed in sections: {', '.join(fetch_section_failed)}.")
            logging.error(f"❌ [FETCH] TikTok Ads ad creative fetching failed in sections: {', '.join(fetch_section_failed)}.")
            return {"status": "failed", "elapsed_seconds": elapsed, "failed_sections": fetch_section_failed}
        else:
            print(f"🏆 [FETCH] Successfully completed TikTok Ads ad creative fetching with {len(fetch_df_enforced)} row(s) in {elapsed}s.")
            logging.info(f"🏆 [FETCH] Successfully completed TikTok Ads ad creative fetching with {len(fetch_df_enforced)} row(s) in {elapsed}s.")
            return fetch_df_enforced

# 2. FETCH TIKTOK ADS INSIGHTS

# 2.1. Fetch campaign insights for TikTok Ads
def fetch_campaign_insights(start_date: str, end_date: str) -> pd.DataFrame:
    print(f"🚀 [FETCH] Starting to fetch TikTok campaign insights from {start_date} to {end_date}...")
    logging.info(f"🚀 [FETCH] Starting to fetch TikTok campaign insights from {start_date} to {end_date}...")    

    try:
        
    # 2.1.1 Initialize Google Secret Manager client
        try:
            print(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            google_secret_client = secretmanager.SecretManagerServiceClient()
            print(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
            logging.info(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
        except DefaultCredentialsError as e:
            raise RuntimeError("❌ [FETCH] Failed to initialize Google Secret Manager client due to credentials error.") from e
        except PermissionDenied as e:
            raise RuntimeError("❌ [FETCH] Failed to initialize Google Secret Manager client due to permission denial.") from e
        except NotFound as e:
            raise RuntimeError("❌ [FETCH] Failed to initialize Google Secret Manager client because secret not found.") from e
        except GoogleAPICallError as e:
            raise RuntimeError("❌ [FETCH] Failed to initialize Google Secret Manager client due to API call error.") from e
        except Exception as e:
            raise RuntimeError(f"❌ [FETCH] Failed to initialize Google Secret Manager client due to unexpected error {e}.") from e

    # 2.1.2. Get TikTok Ads access token from Google Secret Manager
        try: 
            token_secret_id = f"{COMPANY}_secret_all_{PLATFORM}_token_access_user"
            token_secret_name = f"projects/{PROJECT}/secrets/{token_secret_id}/versions/latest"
            token_secret_response = google_secret_client.access_secret_version(request={"name": token_secret_name})
            token_access_user = token_secret_response.payload.data.decode("utf-8")
            print(f"✅ [FETCH] Successfully retrieved TikTok access token for account {ACCOUNT} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved TikTok access token for account {ACCOUNT} from Google Secret Manager.")
        except Exception as e:
            print(f"❌ [FETCH] Failed to retrieve TikTok access token for {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve TikTok access token for {ACCOUNT} from Google Secret Manager due to {e}.")
            raise RuntimeError(f"❌ [FETCH] Failed to retrieve TikTok access token for {ACCOUNT} from Google Secret Manager due to {e}.")

    # 2.1.3. Get TikTok Ads advertiser_id from Google Secret Manager
        try:
            print(f"🔍 [FETCH] Retrieving TikTok Ads access_token for account {ACCOUNT} from Google Secret Manager...")
            logging.info(f"🔍 [FETCH] Retrieving TikTok Ads access_token for account {ACCOUNT} from Google Secret Manager...")
            advertiser_secret_id = f"{COMPANY}_secret_{DEPARTMENT}_tiktok_account_id_{ACCOUNT}"
            advertiser_secret_name = f"projects/{PROJECT}/secrets/{advertiser_secret_id}/versions/latest"
            advertiser_secret_response = google_secret_client.access_secret_version(request={"name": advertiser_secret_name})
            advertiser_id = advertiser_secret_response.payload.data.decode("utf-8")
            print(f"✅ [FETCH] Successfully retrieved TikTok Ads advertiser_id {advertiser_id} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved TikTok Ads advertiser_id {advertiser_id} from Google Secret Manager.")
        except Exception as e:
            print(f"❌ [FETCH] Failed to retrieve TikTok Ads access token for {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve TikTok Ads access token for {ACCOUNT} from Google Secret Manager due to {e}.")
            raise RuntimeError(f"❌ [FETCH] Failed to retrieve TikTok Ads access token for {ACCOUNT} from Google Secret Manager due to {e}.")

    # 2.1.4. Make TikTok Ads API call for BASIC report type
        campaign_report_url = "https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/"
        campaign_report_params = {
            "advertiser_id": advertiser_id,
            "report_type": "BASIC",
            "data_level": "AUCTION_CAMPAIGN",
            "dimensions": ["campaign_id", "stat_time_day"],
            "metrics": [
                "objective_type",
                "result",
                "spend",
                "impressions",
                "clicks",
                "engaged_view_15s",
                "purchase",
                "complete_payment",
                "onsite_total_purchase",
                "offline_shopping_events",
                "onsite_shopping",
                "messaging_total_conversation_tiktok_direct_message"
            ],
            "start_date": start_date,
            "end_date": end_date,
            "page_size": 1000,
            "page": 1
        }
        all_basic_records = []     
        print(f"🔍 [FETCH] Retrieving TikTok Ads campaign insights for advertiser_id {advertiser_id} with BASIC report_type...")
        logging.info(f"🔍 [FETCH] Retrieving TikTok Ads campaign insights for advertiser_id {advertiser_id} with BASIC report_type...")
        for attempt in range(2):
            try:
                while True:
                    resp = requests.get(
                        campaign_report_url,
                        headers={
                            "Access-Token": token_access_user,
                            "Content-Type": "application/json",
                        },
                        json=campaign_report_params,
                        timeout=60
                    )
                    resp_json = resp.json()
                    if resp_json.get("code") != 0:
                        raise Exception(
                            f"❌ [FETCH] Failed to retrieve TikTok Ads campaign insights with BASIC report_type due to API error {resp_json.get('message')}."
                        )
                    data_list = resp_json["data"].get("list", [])
                    all_basic_records.extend(data_list)
                    if len(data_list) < campaign_report_params["page_size"]:
                        break
                    campaign_report_params["page"] += 1
                flattened_basic = []
                for record in all_basic_records:
                    row = {}
                    row.update(record.get("dimensions", {}))
                    row.update(record.get("metrics", {}))
                    row["advertiser_id"] = campaign_report_params["advertiser_id"]
                    flattened_basic.append(row)
                fetch_df_flattened = pd.DataFrame(flattened_basic)
                print(f"✅ [FETCH] Successfully retrieved {len(fetch_df_flattened)} rows of TikTok Ads campaign insights with BASIC report_type.")
                logging.info(f"✅ [FETCH] Successfully retrieved {len(fetch_df_flattened)} rows of TikTok Ads campaign insights with BASIC report_type.")
                break
            except Exception as e:
                if attempt < 1:
                    print(f"⚠️ [FETCH] Failed to retrieve TikTok Ads campaign insights with BASIC report_type attempt {attempt+1} due to {e} then retrying...")
                    logging.warning(f"⚠️ [FETCH] Failed to retrieve TikTok Ads campaign insights with BASIC report_type attempt {attempt+1} due to {e} then retrying...")
                    time.sleep(1)
                else:
                    print(f"❌ [FETCH] Failed to retrieve TikTok Ads campaign insights with BASIC report_type after all attempt(s) due to {e}.")
                    logging.error(f"❌ [FETCH] Failed to retrieve TikTok Ads campaign insights with BASIC report_type after all attempt(s) due to {e}.")
                    fetch_df_flattened = pd.DataFrame()
    
    # 2.1.5. Enforce Python DataFrame schema
        try:
            print(f"🔄 [FETCH] Enforcing schema for TikTok Ads campaign insights with {len(fetch_df_flattened)} row(s)...")
            logging.info(f"🔄 [FETCH] Enforcing schema for TikTok Ads campaign insights with {len(fetch_df_flattened)} row(s)...")
            fetch_df_enforced = ensure_table_schema(fetch_df_flattened, "fetch_campaign_insights")
            print(f"✅ [FETCH] Successfully enforced schema for {len(fetch_df_enforced)} row(s) of TikTok campaign insights.")
            logging.info(f"✅ [FETCH] Successfully enforced schema for {len(fetch_df_enforced)} row(s) of TikTok campaign insights.")
        except Exception as e:
            print(f"❌ [FETCH] Failed to enforce TikTok campaign insights schema due to {e}.")
            logging.error(f"❌ [FETCH] Failed to enforce TikTok campaign insights schema due to {e}.")
            return pd.DataFrame()
        return fetch_df_enforced
   
    except Exception as e_outer:
        print(f"❌ [FETCH] Outer error while fetching TikTok campaign insights: {e_outer}")
        logging.error(f"❌ [FETCH] Outer error while fetching TikTok campaign insights: {e_outer}")
        return pd.DataFrame()

# 2.2. Fetch ad insights for TikTok Ads    
def fetch_ad_insights(start_date: str, end_date: str) -> pd.DataFrame:
    print(f"🚀 [FETCH] Starting to fetch TikTok Ad insights from {start_date} to {end_date}...")
    logging.info(f"🚀 [FETCH] Starting to fetch TikTok Ad insights from {start_date} to {end_date}...")    

    try:
        
    # 2.2.1 Initialize Google Secret Manager client
        try:
            print(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            google_secret_client = secretmanager.SecretManagerServiceClient()
            print(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
            logging.info(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
        except DefaultCredentialsError as e:
            raise RuntimeError("❌ [FETCH] Failed to initialize Google Secret Manager client due to credentials error.") from e
        except PermissionDenied as e:
            raise RuntimeError("❌ [FETCH] Failed to initialize Google Secret Manager client due to permission denial.") from e
        except NotFound as e:
            raise RuntimeError("❌ [FETCH] Failed to initialize Google Secret Manager client because secret not found.") from e
        except GoogleAPICallError as e:
            raise RuntimeError("❌ [FETCH] Failed to initialize Google Secret Manager client due to API call error.") from e
        except Exception as e:
            raise RuntimeError(f"❌ [FETCH] Failed to initialize Google Secret Manager client due to unexpected error {e}.") from e

    # 2.2.2. Get TikTok Ads access token from Google Secret Manager
        try: 
            token_secret_id = f"{COMPANY}_secret_all_{PLATFORM}_token_access_user"
            token_secret_name = f"projects/{PROJECT}/secrets/{token_secret_id}/versions/latest"
            token_response = google_secret_client.access_secret_version(request={"name": token_secret_name})
            token_access_user = token_response.payload.data.decode("utf-8")
            print(f"✅ [FETCH] Successfully retrieved TikTok access token for account {ACCOUNT} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved TikTok access token for account {ACCOUNT} from Google Secret Manager.")
        except Exception as e:
            print(f"❌ [FETCH] Failed to retrieve TikTok access token for {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve TikTok access token for {ACCOUNT} from Google Secret Manager due to {e}.")
            raise RuntimeError(f"❌ [FETCH] Failed to retrieve TikTok access token for {ACCOUNT} from Google Secret Manager due to {e}.")

    # 2.2.3. Get TikTok Ads advertiser_id from Google Secret Manager
        try:
            print(f"🔍 [FETCH] Retrieving TikTok Ads advertiser_id for account {ACCOUNT} from Google Secret Manager...")
            logging.info(f"🔍 [FETCH] Retrieving TikTok Ads advertiser_id for account {ACCOUNT} from Google Secret Manager...")
            advertiser_secret_id = f"{COMPANY}_secret_{DEPARTMENT}_tiktok_account_id_{ACCOUNT}"
            advertiser_secret_name = f"projects/{PROJECT}/secrets/{advertiser_secret_id}/versions/latest"
            advertiser_secret_response = google_secret_client.access_secret_version(request={"name": advertiser_secret_name})
            advertiser_id = advertiser_secret_response.payload.data.decode("utf-8")
            print(f"✅ [FETCH] Successfully retrieved TikTok advertiser_id {advertiser_id} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved TikTok advertiser_id {advertiser_id} from Google Secret Manager.")
        except Exception as e:
            print(f"❌ [FETCH] Failed to retrieve TikTok advertiser_id for {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve TikTok advertiser_id for {ACCOUNT} from Google Secret Manager due to {e}.")
            raise RuntimeError(f"❌ [FETCH] Failed to retrieve TikTok advertiser_id for {ACCOUNT} from Google Secret Manager due to {e}.")

    # 2.2.4. Make TikTok Ads API call for BASIC report_type at AD level
        tiktok_ad_url = "https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/"
        tiktok_ad_params = {
            "advertiser_id": advertiser_id,
            "report_type": "BASIC",
            "data_level": "AUCTION_AD",
            "dimensions": ["ad_id", "stat_time_day"],
            "metrics": [
                "result",
                "spend",
                "impressions",
                "clicks",
                "engaged_view_15s",
                "purchase",
                "complete_payment",
                "onsite_total_purchase",
                "offline_shopping_events",
                "onsite_shopping",
                "messaging_total_conversation_tiktok_direct_message"
            ],
            "start_date": start_date,
            "end_date": end_date,
            "page_size": 1000,
            "page": 1
        }

        all_ad_records = []     
        print(f"🔍 [FETCH] Retrieving TikTok Ads ad-level insights for advertiser_id {advertiser_id} with BASIC report_type...")
        logging.info(f"🔍 [FETCH] Retrieving TikTok Ads ad-level insights for advertiser_id {advertiser_id} with BASIC report_type...")

        for attempt in range(2):
            try:
                while True:
                    resp = requests.get(
                        tiktok_ad_url,
                        headers={
                            "Access-Token": token_access_user,
                            "Content-Type": "application/json",
                        },
                        json=tiktok_ad_params,
                        timeout=60
                    )
                    resp_json = resp.json()
                    if resp_json.get("code") != 0:
                        raise Exception(
                            f"❌ [FETCH] Failed to retrieve TikTok Ads ad-level insights with BASIC report_type due to API error {resp_json.get('message')}."
                        )

                    data_list = resp_json["data"].get("list", [])
                    all_ad_records.extend(data_list)

                    if len(data_list) < tiktok_ad_params["page_size"]:
                        break
                    tiktok_ad_params["page"] += 1

                flattened_ad = []
                for record in all_ad_records:
                    row = {}
                    row.update(record.get("dimensions", {}))
                    row.update(record.get("metrics", {}))
                    row["advertiser_id"] = tiktok_ad_params["advertiser_id"]
                    flattened_ad.append(row)

                df_ad = pd.DataFrame(flattened_ad)
                print(f"✅ [FETCH] Successfully retrieved {len(df_ad)} rows of TikTok Ads ad-level insights with BASIC report_type.")
                logging.info(f"✅ [FETCH] Successfully retrieved {len(df_ad)} rows of TikTok Ads ad-level insights with BASIC report_type.")
                break

            except Exception as e:
                if attempt < 1:
                    print(f"⚠️ [FETCH] TikTok Ads ad-level insights with BASIC report_type attempt {attempt+1} failed due to {e} then retrying...")
                    logging.warning(f"⚠️ [FETCH] TikTok Ads ad-level insights with BASIC report_type attempt {attempt+1} failed due to {e} then retrying...")
                    time.sleep(1)
                else:
                    print(f"❌ [FETCH] Failed to retrieve TikTok Ads ad-level insights with BASIC report_type after all attempt(s) due to {e}.")
                    logging.error(f"❌ [FETCH] Failed to retrieve TikTok Ads ad-level insights with BASIC report_type after all attempt(s) due to {e}.")
                    df_ad = pd.DataFrame()
    
    # 2.2.5. Enforce Python DataFrame schema
        try:
            print(f"🔄 [FETCH] Enforcing schema for TikTok Ads ad-level insights with {len(df_ad)} row(s)...")
            logging.info(f"🔄 [FETCH] Enforcing schema for TikTok Ads ad-level insights with {len(df_ad)} row(s)...")
            df = ensure_table_schema(df_ad, "fetch_ad_insights")
            print(f"✅ [FETCH] Successfully enforced schema for {len(df)} row(s) of TikTok Ads ad-level insights.")
            logging.info(f"✅ [FETCH] Successfully enforced schema for {len(df)} row(s) of TikTok Ads ad-level insights.")
        except Exception as e:
            print(f"❌ [FETCH] Failed to enforce TikTok Ads ad-level insights schema due to {e}.")
            logging.error(f"❌ [FETCH] Failed to enforce TikTok Ads ad-level insights schema due to {e}.")
            return pd.DataFrame()
        return df
       
    except Exception as e_outer:
        print(f"❌ [FETCH] Outer error while fetching TikTok Ads ad-level insights: {e_outer}")
        logging.error(f"❌ [FETCH] Outer error while fetching TikTok Ads ad-level insights: {e_outer}")
        return pd.DataFrame()