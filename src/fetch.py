"""
==================================================================
TIKTOK ADS FETCHING MODULE
------------------------------------------------------------------
This module is responsible for direct, authenticated access to the 
TikTok Marketing API, encapsulating all logic required to 
fetch raw campaign, ad group, ad, and performance insight records.

It provides a clean interface to centralize API-related operations, 
enabling reusable, testable, and isolated logic for data ingestion 
pipelines without mixing transformation or storage responsibilities.

✔️ Initializes secure TikTok Marketing API client and retrieves 
   credentials dynamically from Google Secret Manager  
✔️ Fetches data via API calls (with pagination and retries) and 
   returns structured DataFrames  
✔️ Supports campaign-level, adgroup-level, and ad-level performance metrics  

⚠️ This module focuses only on *data retrieval from the API*. 
It does not handle schema validation, data transformation, or 
storage operations such as uploading to BigQuery.
==================================================================
"""
# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add Python logging ultilties for integration
import logging

# Add Python requests ultilities for integration
import requests

# Add Python time ultilities for integration
import time

# Add Python Pandas libraries for integration
import pandas as pd

# Add Google Authentication libraries for integration
from google.api_core.exceptions import (
    GoogleAPICallError,
    NotFound,
    PermissionDenied, 
)
from google.auth import default
from google.auth.exceptions import DefaultCredentialsError
from google.auth.transport.requests import AuthorizedSession

# Add Google Secret Manager modules for integration
from google.cloud import secretmanager

# Add internal TikTok Ads modules for handling
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

# 1. FETCH TIKTOK ADS METADATA

# 1.1. Fetch campaign metadata for TikTok Ads
def fetch_campaign_metadata(campaign_id_list: list[str], fields: list[str] = None) -> pd.DataFrame:
    print(f"🚀 [FETCH] Starting to fetch TikTok campaign metadata for {len(campaign_id_list)} campaign_id(s)...")
    logging.info(f"🚀 [FETCH] Starting to fetch TikTok campaign metadata for {len(campaign_id_list)} campaign_id(s)...")

    # 1.1.1. Validate input
    if not campaign_id_list:
        print("⚠️ [FETCH] Empty TikTok campaign_id_list provided.")
        logging.warning("⚠️ [FETCH] Empty TikTok campaign_id_list provided.")
        return pd.DataFrame()

    # 1.1.2. Prepare fields
    default_fields = [
        "campaign_id",
        "campaign_name",
        "objective_type",
        "campaign_type",
        "budget_mode",
        "budget",
        "status"
    ]
    fetch_fields = fields if fields else default_fields
    all_records = []
    print(f"🔍 [FETCH] Preparing to fetch TikTok campaign metadata with {fetch_fields} field(s)...")
    logging.info(f"🔍 [FETCH] Preparing to fetch TikTok campaign metadata with {fetch_fields} field(s)...")

    try:
    # 1.1.3. Initialize Google Secret Manager client
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
        
    # 1.1.4. Get TikTok access token from Google Secret Manager
        try:
            print(f"🔍 [FETCH] Retrieving TikTok access token for {ACCOUNT} from Google Secret Manager...")
            logging.info(f"🔍 [FETCH] Retrieving TikTok access token for {ACCOUNT} from Google Secret Manager...")
            google_secret_id = f"{COMPANY}_secret_{DEPARTMENT}_{PLATFORM}_access_token_{ACCOUNT}"
            google_secret_name = f"projects/{PROJECT}/secrets/{google_secret_id}/versions/latest"
            response = google_secret_client.access_secret_version(request={"name": google_secret_name})
            tiktok_access_token = response.payload.data.decode("utf-8")
            print(f"✅ [FETCH] Successfully retrieved TikTok access token for {ACCOUNT} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved TikTok access token for {ACCOUNT} from Google Secret Manager.")
        except Exception as e:
            print(f"❌ [FETCH] Failed to retrieve TikTok access token for {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve TikTok access token for {ACCOUNT} from Google Secret Manager due to {e}.")
            raise RuntimeError(f"❌ [FETCH] Failed to retrieve TikTok access token for {ACCOUNT} from Google Secret Manager due to {e}.")

    # 1.1.4. Loop through all campaign_id(s)
        print(f"🔍 [FETCH] Retrieving metadata for {len(campaign_id_list)} TikTok campaign_id(s).")
        logging.info(f"🔍 [FETCH] Retrieving metadata for {len(campaign_id_list)} TikTok campaign_id(s).")

        base_url = "https://business-api.tiktok.com/open_api/v1.3/campaign/get/"
        headers = {"Access-Token": tiktok_access_token}

        for campaign_id in campaign_id_list:
            try:
                params = {"advertiser_id": ADVERTISER_ID, "filtering": {"campaign_ids": [campaign_id]}, "fields": fetch_fields}
                response = requests.get(base_url, headers=headers, params={"advertiser_id": ADVERTISER_ID, "filtering": json.dumps({"campaign_ids": [campaign_id]})})
                data = response.json()
                if data.get("code") == 0 and data.get("data", {}).get("list"):
                    record = data["data"]["list"][0]
                    all_records.append(record)
                else:
                    print(f"⚠️ [FETCH] No metadata returned for TikTok campaign_id {campaign_id}.")
                    logging.warning(f"⚠️ [FETCH] No metadata returned for TikTok campaign_id {campaign_id}.")
            except Exception as e:
                print(f"❌ [FETCH] Failed to fetch TikTok metadata for campaign_id {campaign_id} due to {e}.")
                logging.error(f"❌ [FETCH] Failed to fetch TikTok metadata for campaign_id {campaign_id} due to {e}.")

    # 1.1.5. Convert to dataframe
        if not all_records:
            print("⚠️ [FETCH] No TikTok campaign metadata fetched.")
            logging.warning("⚠️ [FETCH] No TikTok campaign metadata fetched.")
            return pd.DataFrame()
        try:
            df = pd.DataFrame(all_records)
            print(f"✅ [FETCH] Successfully converted TikTok campaign metadata to dataframe with {len(df)} row(s).")
            logging.info(f"✅ [FETCH] Successfully converted TikTok campaign metadata to dataframe with {len(df)} row(s).")
        except Exception as e:
            print(f"❌ [FETCH] Failed to convert TikTok campaign metadata due to {e}.")
            logging.error(f"❌ [FETCH] Failed to convert TikTok campaign metadata due to {e}.")
            return pd.DataFrame()

    # 1.1.6. Enforce schema
        try:
            print(f"🔄 [FETCH] Enforcing schema for {len(df)} row(s) of TikTok campaign metadata...")
            logging.info(f"🔄 [FETCH] Enforcing schema for {len(df)} row(s) of TikTok campaign metadata...")
            df = ensure_table_schema(df, "fetch_campaign_metadata")
            print(f"✅ [FETCH] Successfully enforced schema for {len(df)} row(s) of TikTok campaign metadata.")
            logging.info(f"✅ [FETCH] Successfully enforced schema for {len(df)} row(s) of TikTok campaign metadata.")
        except Exception as e:
            print(f"❌ [FETCH] Failed to enforce schema for TikTok campaign metadata due to {e}.")
            logging.error(f"❌ [FETCH] Failed to enforce schema for TikTok campaign metadata due to {e}.")
            return pd.DataFrame()

        return df
    except Exception as e:
        print(f"❌ [FETCH] Failed to fetch TikTok campaign metadata due to {e}.")
        logging.error(f"❌ [FETCH] Failed to fetch TikTok campaign metadata due to {e}.")
        return pd.DataFrame()

# 1.2. Fetch adset metdata for TikTok Ads
def fetch_adset_metadata(adgroup_id_list: list[str], fields: list[str] = None) -> pd.DataFrame:
    print(f"🚀 [FETCH] Starting to fetch TikTok {len(adgroup_id_list)} adgroup metadata(s)...")
    logging.info(f"🚀 [FETCH] Starting to fetch TikTok {len(adgroup_id_list)} adgroup metadata(s)...")

    # 1.2.1. Validate input
    if not adgroup_id_list:
        print("⚠️ [FETCH] Empty TikTok adgroup_id_list provided.")
        logging.warning("⚠️ [FETCH] Empty TikTok adgroup_id_list provided.")
        return pd.DataFrame()

    # default field mapping
    default_fields = [
        "adgroup_id",
        "adgroup_name",
        "campaign_id",
        "adgroup_status",
        "budget",
        "billing_event",
        "optimization_goal"
    ]
    fetch_fields = fields if fields else default_fields
    all_records = []

    try:
        # Lấy access_token từ Secret Manager
        secret_client = secretmanager.SecretManagerServiceClient()
        secret_id = f"{COMPANY}_secret_{DEPARTMENT}_tiktok_access_token_{ACCOUNT}"
        secret_name = f"projects/{PROJECT}/secrets/{secret_id}/versions/latest"
        response = secret_client.access_secret_version(request={"name": secret_name})
        access_token = response.payload.data.decode("utf-8")

        # Gọi API TikTok
        for adgroup_id in adgroup_id_list:
            url = "https://business-api.tiktokglobalshop.com/open_api/v1.3/adgroup/get/"
            payload = {
                "adgroup_ids": [adgroup_id],
                "fields": fetch_fields
            }
            headers = {"Access-Token": access_token}
            resp = requests.post(url, json=payload, headers=headers).json()

            if resp.get("code") == 0:
                for adgroup in resp["data"]["list"]:
                    record = {f: adgroup.get(f, None) for f in fetch_fields}
                    all_records.append(record)
            else:
                print(f"⚠️ [FETCH] API error {resp.get('message')} for adgroup {adgroup_id}")
                logging.warning(f"⚠️ [FETCH] API error {resp.get('message')} for adgroup {adgroup_id}")

        if not all_records:
            print("⚠️ [FETCH] No TikTok adgroup metadata fetched.")
            logging.warning("⚠️ [FETCH] No TikTok adgroup metadata fetched.")
            return pd.DataFrame()

        # Convert to DataFrame
        df = pd.DataFrame(all_records)
        df = ensure_table_schema(df, "fetch_adgroup_metadata")
        print(f"✅ [FETCH] Successfully fetched TikTok adgroup metadata with {len(df)} row(s).")
        return df

    except Exception as e:
        print(f"❌ [FETCH] Failed to fetch TikTok adgroup metadata due to {e}")
        logging.error(f"❌ [FETCH] Failed to fetch TikTok adgroup metadata due to {e}")
        return pd.DataFrame()

# 1.3. Fetch TikTok Ads ad metadata
def fetch_ad_metadata(ad_id_list: list[str], fields: list[str] = None) -> pd.DataFrame:
    print("🚀 [FETCH] Starting to fetch TikTok ad metadata...")
    logging.info("🚀 [FETCH] Starting to fetch TikTok ad metadata...")

    if not ad_id_list:
        print("⚠️ [FETCH] Empty TikTok ad_id_list provided.")
        logging.warning("⚠️ [FETCH] Empty TikTok ad_id_list provided.")
        return pd.DataFrame()

    MAX_RETRIES = 3
    SLEEP_BETWEEN_RETRIES = 2

    default_fields = [
        "ad_id",
        "ad_name",
        "adgroup_id",
        "campaign_id",
        "status",
        "opt_status"
    ]
    fetch_fields = fields if fields else default_fields
    all_records = []

    print(f"🔍 [FETCH] Preparing to fetch TikTok ad metadata with {fetch_fields} field(s)...")
    logging.info(f"🔍 [FETCH] Preparing to fetch TikTok ad metadata with {fetch_fields} field(s)...")

    # 1. Get TikTok access token
    try:
        secret_client = secretmanager.SecretManagerServiceClient()
        secret_id = f"{COMPANY}_secret_{DEPARTMENT}_tiktok_access_token_{ACCOUNT}"
        secret_name = f"projects/{PROJECT}/secrets/{secret_id}/versions/latest"
        response = secret_client.access_secret_version(request={"name": secret_name})
        access_token = response.payload.data.decode("utf-8")
        print(f"✅ [FETCH] Successfully retrieved TikTok access token for {ACCOUNT}.")
        logging.info(f"✅ [FETCH] Successfully retrieved TikTok access token for {ACCOUNT}.")
    except Exception as e:
        print(f"❌ [FETCH] Failed to retrieve TikTok access token for {ACCOUNT} due to {e}.")
        logging.error(f"❌ [FETCH] Failed to retrieve TikTok access token for {ACCOUNT} due to {e}.")
        return pd.DataFrame()

    base_url = "https://business-api.tiktokglobalplatform.com/open_api/v1.3/ad/get/"
    headers = {"Access-Token": access_token}

    # 2. Loop through ad_id(s)
    print(f"🔍 [FETCH] Retrieving metadata for {len(ad_id_list)} TikTok ad(s)...")
    logging.info(f"🔍 [FETCH] Retrieving metadata for {len(ad_id_list)} TikTok ad(s)...")

    for ad_id in ad_id_list:
        for attempt in range(MAX_RETRIES):
            try:
                params = {
                    "advertiser_id": ADVERTISER_ID,
                    "filtering": json.dumps({"ad_ids": [ad_id]}),
                    "fields": fetch_fields,
                }
                response = requests.get(base_url, headers=headers, params=params)
                data = response.json()
                if data.get("code") == 0 and data.get("data", {}).get("list"):
                    record = data["data"]["list"][0]
                    all_records.append(record)
                    break
                else:
                    print(f"⚠️ [FETCH] No metadata returned for TikTok ad_id {ad_id}.")
                    logging.warning(f"⚠️ [FETCH] No metadata returned for TikTok ad_id {ad_id}.")
                    break
            except Exception as e:
                print(f"❌ [FETCH] Failed to fetch TikTok ad metadata for ad_id {ad_id} due to {e}. Attempt {attempt+1}")
                logging.error(f"❌ [FETCH] Failed to fetch TikTok ad metadata for ad_id {ad_id} due to {e}. Attempt {attempt+1}")
                time.sleep(SLEEP_BETWEEN_RETRIES ** (attempt + 1))

    if not all_records:
        print("⚠️ [FETCH] No TikTok ad metadata fetched.")
        logging.warning("⚠️ [FETCH] No TikTok ad metadata fetched.")
        return pd.DataFrame()

    # 3. Convert to dataframe
    try:
        df = pd.DataFrame(all_records)
        print(f"✅ [FETCH] Successfully converted TikTok ad metadata to DataFrame with {len(df)} row(s).")
        logging.info(f"✅ [FETCH] Successfully converted TikTok ad metadata to DataFrame with {len(df)} row(s).")
    except Exception as e:
        print(f"❌ [FETCH] Failed to convert TikTok ad metadata to DataFrame due to {e}.")
        logging.error(f"❌ [FETCH] Failed to convert TikTok ad metadata to DataFrame due to {e}.")
        return pd.DataFrame()

    # 4. Enforce schema
    try:
        df = ensure_table_schema(df, "fetch_ad_metadata_tiktok")
        print(f"✅ [FETCH] Successfully enforced schema for TikTok ad metadata with {len(df)} row(s).")
        logging.info(f"✅ [FETCH] Successfully enforced schema for TikTok ad metadata with {len(df)} row(s).")
    except Exception as e:
        print(f"❌ [FETCH] Failed to enforce schema for TikTok ad metadata due to {e}.")
        logging.error(f"❌ [FETCH] Failed to enforce schema for TikTok ad metadata due to {e}.")
        return pd.DataFrame()

    return df

# 1.4. Fetch ad creative for TikTok Ads
def fetch_ad_creative(ad_id_list: list[str]) -> pd.DataFrame:
    print("🚀 [FETCH] Starting to fetch TikTok ad creatives (dynamic thumbnail)...")
    logging.info("🚀 [FETCH] Starting to fetch TikTok ad creatives (dynamic thumbnail)...")

    if not ad_id_list:
        print("⚠️ [FETCH] Empty TikTok ad_id_list provided.")
        logging.warning("⚠️ [FETCH] Empty TikTok ad_id_list provided.")
        return pd.DataFrame()

    MAX_RETRIES = 3
    SLEEP_BETWEEN_RETRIES = 2
    all_records = []

    # 🔑 Lấy account_id từ Secret Manager
    try:
        secret_client = secretmanager.SecretManagerServiceClient()
        secret_id = f"{COMPANY}_secret_{DEPARTMENT}_{PLATFORM}_account_id_{ACCOUNT}"
        secret_name = f"projects/{PROJECT}/secrets/{secret_id}/versions/latest"
        response = secret_client.access_secret_version(request={"name": secret_name})
        account_id = response.payload.data.decode("utf-8")
        print(f"✅ [FETCH] Retrieved TikTok account ID {account_id}.")
    except Exception as e:
        print(f"❌ [FETCH] Failed to retrieve TikTok account ID due to {e}.")
        return pd.DataFrame()

    # 🔍 Loop qua từng ad_id
    for ad_id in ad_id_list:
        for attempt in range(MAX_RETRIES):
            try:
                url = "https://business-api.tiktokglobalshop.com/open_api/v1.3/ad/get/"
                headers = {"Access-Token": get_tiktok_access_token()}
                payload = {"advertiser_id": account_id, "filtering": {"ad_ids": [ad_id]}, "fields": ["creative"]}

                res = requests.post(url, headers=headers, json=payload)
                res.raise_for_status()
                data = res.json()

                ad = data.get("data", {}).get("list", [])[0]
                creative = ad.get("creative", {})
                creative_id = creative.get("creative_id")

                thumbnail_url = ""
                if creative.get("video_info", {}).get("cover_url"):
                    thumbnail_url = creative["video_info"]["cover_url"]
                elif creative.get("image_info", {}).get("image_url"):
                    thumbnail_url = creative["image_info"]["image_url"]

                all_records.append({
                    "ad_id": ad_id,
                    "creative_id": creative_id,
                    "thumbnail_url": thumbnail_url,
                    "account_id": account_id,
                })
                break

            except Exception as e:
                print(f"⚠️ [FETCH] Failed to fetch TikTok creative for ad_id {ad_id} (attempt {attempt+1}) due to {e}.")
                time.sleep(SLEEP_BETWEEN_RETRIES ** (attempt + 1))

    if not all_records:
        print("⚠️ [FETCH] No TikTok creative metadata fetched.")
        return pd.DataFrame()

    df = pd.DataFrame(all_records)
    print(f"✅ [FETCH] Successfully fetched {len(df)} TikTok ad creative(s).")
    return df

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
            token_response = google_secret_client.access_secret_version(request={"name": token_secret_name})
            tiktok_access_token = token_response.payload.data.decode("utf-8")
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
            tiktok_advertiser_id = advertiser_secret_response.payload.data.decode("utf-8")
            print(f"✅ [FETCH] Successfully retrieved TikTok advertiser_id {tiktok_advertiser_id} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved TikTok advertiser_id {tiktok_advertiser_id} from Google Secret Manager.")
        except Exception as e:
            print(f"❌ [FETCH] Failed to retrieve TikTok access token for {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve TikTok access token for {ACCOUNT} from Google Secret Manager due to {e}.")
            raise RuntimeError(f"❌ [FETCH] Failed to retrieve TikTok access token for {ACCOUNT} from Google Secret Manager due to {e}.")

        # 3.1.2. Define parameters
        url = "https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/"
        params = {
            "advertiser_id": tiktok_advertiser_id,
            "report_type": "BASIC",
            "data_level": "AUCTION_CAMPAIGN",
            "dimensions": ["campaign_id", "stat_time_day"],
            "metrics": ["spend", "impressions", "clicks"],
            "start_date": start_date,
            "end_date": end_date,
            "page_size": 1000,
            "page": 1
        }
        print(f"🔍 [FETCH] Preparing TikTok API request with {params} parameter(s).")
        logging.info(f"🔍 [FETCH] Preparing TikTok API request with {params} parameter(s).")

        # 3.1.3. Make TikTok API call
        all_records = []
        for attempt in range(2):
            try:
                print(f"🔍 [FETCH] Fetching TikTok campaign insights for advertiser_id {tiktok_advertiser_id} from {start_date} to {end_date}, attempt {attempt+1}...")
                logging.info(f"🔍 [FETCH] Fetching TikTok campaign insights for advertiser_id {tiktok_advertiser_id} from {start_date} to {end_date}, attempt {attempt+1}...")

                while True:
                    resp = requests.get(
                        url,
                        headers={
                            "Access-Token": tiktok_access_token,
                            "Content-Type": "application/json",
                        },
                        json=params,
                        timeout=60
                    )

                    try:
                        resp_json = resp.json()
                    except Exception:
                        print(f"❌ [FETCH] Non-JSON response: {resp.status_code}, {resp.text[:200]}")
                        raise

                    if resp_json.get("code") != 0:
                        raise Exception(f"TikTok API error: {resp_json.get('message')}")

                    data_list = resp_json["data"].get("list", [])
                    all_records.extend(data_list)

                    if len(data_list) < params["page_size"]:
                        break
                    params["page"] += 1

                if not all_records:
                    print("⚠️ [FETCH] No data returned from TikTok API.")
                    logging.warning("⚠️ [FETCH] No data returned from TikTok API.")
                    return pd.DataFrame()
                df = pd.DataFrame(all_records)
                # 👀 Debug sample raw data
                
                print("🔍 [DEBUG] Sample TikTok API response row(s):")
                print(df.head(5).to_dict(orient="records"))                
                
                print(f"✅ [FETCH] Successfully retrieved {len(df)} row(s) for TikTok campaign insights from {start_date} to {end_date}.")
                logging.info(f"✅ [FETCH] Successfully retrieved {len(df)} row(s) for TikTok campaign insights from {start_date} to {end_date}.")

        # 3.1.4. Flatten TikTok API nested JSON (metrics + dimensions)
                # ⚠️ [SECTION] TikTok JSON flattening: khác với Facebook, TikTok trả nested metrics/dimensions
                flattened_records = []
                for record in all_records:
                    flat_row = {}
                    # Copy dimensions
                    flat_row.update(record.get("dimensions", {}))
                    # Copy metrics
                    flat_row.update(record.get("metrics", {}))
                    flattened_records.append(flat_row)

                df = pd.DataFrame(flattened_records)
                print("🔍 [DEBUG] Sample TikTok API response after flattening:")
                print(df.head(5).to_dict(orient="records"))

        # 3.1.5. Enforce schema
                print(f"🔄 [FETCH] Enforcing schema for TikTok campaign insights from {start_date} to {end_date}...")
                logging.info(f"🔄 [FETCH] Enforcing schema for TikTok campaign insights from {start_date} to {end_date}...")
                df = ensure_table_schema(df, "fetch_campaign_insights")
                print(f"✅ [FETCH] Successfully enforced TikTok campaign insights schema with {len(df)} row(s).")
                logging.info(f"✅ [FETCH] Successfully enforced TikTok campaign insights schema with {len(df)} row(s).")

                return df

            except Exception as e_inner:
                print(f"❌ [FETCH] Failed to fetch TikTok campaign insights from {start_date} to {end_date}, attempt {attempt+1}, due to {e_inner}.")
                logging.error(f"❌ [FETCH] Failed to fetch TikTok campaign insights from {start_date} to {end_date}, attempt {attempt+1}, due to {e_inner}.")
                if attempt == 1:
                    return pd.DataFrame()
                time.sleep(1)

    except Exception as e_outer:
        print(f"❌ [FETCH] Outer error while fetching TikTok campaign insights: {e_outer}")
        logging.error(f"❌ [FETCH] Outer error while fetching TikTok campaign insights: {e_outer}")
        return pd.DataFrame()

# 2.2. Fetch ad insights for TikTok Ads    
def fetch_ad_insights(start_date: str, end_date: str) -> pd.DataFrame:
    print(f"🚀 [FETCH] Starting to fetch TikTok ad insights from {start_date} to {end_date}...")
    logging.info(f"🚀 [FETCH] Starting to fetch TikTok ad insights from {start_date} to {end_date}...")

    try:
        # 1. Retrieve TikTok Advertiser ID from Secret Manager
        print("🔍 [FETCH] Retrieving TikTok advertiser ID from Google Secret Manager...")
        logging.info("🔍 [FETCH] Retrieving TikTok advertiser ID from Google Secret Manager...")
        try:
            secret_client = secretmanager.SecretManagerServiceClient()
            secret_id = f"{COMPANY}_secret_{DEPARTMENT}_{PLATFORM}_advertiser_id_{ACCOUNT}"
            secret_name = f"projects/{PROJECT}/secrets/{secret_id}/versions/latest"
            response = secret_client.access_secret_version(request={"name": secret_name})
            advertiser_id = response.payload.data.decode("utf-8")
        except Exception as e:
            print(f"❌ [FETCH] Failed to retrieve TikTok advertiser ID for {ACCOUNT} account due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve TikTok advertiser ID for {ACCOUNT} account due to {e}.")
            return pd.DataFrame()

        print(f"✅ [FETCH] Successfully retrieved TikTok advertiser ID {advertiser_id} for {ACCOUNT} account.")
        logging.info(f"✅ [FETCH] Successfully retrieved TikTok advertiser ID {advertiser_id} for {ACCOUNT} account.")

        # 2. Define request parameters
        params = {
            "advertiser_id": advertiser_id,
            "report_type": "BASIC",
            "data_level": "AUCTION_AD",
            "dimensions": ["ad_id", "campaign_id", "adgroup_id", "stat_time_day"],
            "metrics": ["spend", "impressions", "clicks", "reach", "cpc", "cpm"],
            "start_date": start_date,
            "end_date": end_date,
            "page_size": 1000,
        }
        print(f"🔍 [FETCH] Preparing TikTok API request with params: {params}.")
        logging.info(f"🔍 [FETCH] Preparing TikTok API request with params: {params}.")

        # 3. Fetch data with retry logic
        for attempt in range(2):
            try:
                print(f"🔍 [FETCH] Fetching TikTok ad insights attempt {attempt+1}...")
                logging.info(f"🔍 [FETCH] Fetching TikTok ad insights attempt {attempt+1}...")

                # Example TikTok API call (adjust based on SDK you use)
                response = tiktok_business_api.report.inquiry(params=params)
                records = response.get("data", {}).get("list", [])

                if not records:
                    print("⚠️ [FETCH] No data returned from TikTok API.")
                    logging.warning("⚠️ [FETCH] No data returned from TikTok API.")
                    return pd.DataFrame()

                df = pd.DataFrame(records)
                print(f"✅ [FETCH] Successfully retrieved {len(df)} row(s) for TikTok ad insights.")
                logging.info(f"✅ [FETCH] Successfully retrieved {len(df)} row(s) for TikTok ad insights.")

                # 4. Enforce schema
                print("🔄 [FETCH] Enforcing schema for TikTok ad insights...")
                logging.info("🔄 [FETCH] Enforcing schema for TikTok ad insights...")
                df = ensure_table_schema(df, "fetch_ad_insights")

                print(f"✅ [FETCH] Successfully enforced TikTok ad insights schema with {len(df)} row(s).")
                logging.info(f"✅ [FETCH] Successfully enforced TikTok ad insights schema with {len(df)} row(s).")
                return df

            except Exception as e_inner:
                print(f"❌ [FETCH] TikTok API call failed on attempt {attempt+1} due to {e_inner}.")
                logging.error(f"❌ [FETCH] TikTok API call failed on attempt {attempt+1} due to {e_inner}.")
                if attempt == 1:
                    return pd.DataFrame()
                time.sleep(1)

    except Exception as e_outer:
        print(f"❌ [FETCH] Failed to fetch TikTok ad insights from {start_date} to {end_date} due to {e_outer}.")
        logging.error(f"❌ [FETCH] Failed to fetch TikTok ad insights from {start_date} to {end_date} due to {e_outer}.")
        return pd.DataFrame()
