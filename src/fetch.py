"""
==================================================================
TIKTOK FETCHING MODULE
------------------------------------------------------------------
This module handles authenticated data retrieval from the TikTok 
Marketing API, consolidating all campaign, ad, creative, and metadata 
fetching logic into a unified, maintainable structure for ingestion.

It ensures reliable access to TikTok Ads data with controlled rate 
limits, standardized field mapping, and structured outputs for 
downstream enrichment and transformation stages.

✔️ Initializes secure TikTok SDK sessions and retrieves credentials  
✔️ Fetches campaign, ad, and creative data via authenticated API calls  
✔️ Handles pagination, rate limiting and error retries automatically  
✔️ Returns normalized and schema-ready DataFrames for processing  
✔️ Logs detailed runtime information for monitoring and debugging  

⚠️ This module focuses solely on *data retrieval and extraction*.  
It does **not** perform schema enforcement, data enrichment, or 
storage operations such as uploading to BigQuery.
==================================================================
"""

# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add Python logging ultilties for integration
import logging

# Add Python Pandas libraries for integration
import pandas as pd

# Add Python requests ultilities for integration
import requests

# Add Python time ultilities for integration
import time

# Add Google Secret Manager modules for integration
from google.cloud import secretmanager

# Add internal TikTok Ads modules for handling
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

# 1. FETCH TIKTOK ADS METADATA

# 1.1. Fetch campaign metadata for TikTok Ads
def fetch_campaign_metadata(fetch_ids_campaign: list[str]) -> pd.DataFrame:
    print(f"🚀 [FETCH] Starting to fetch TikTok Ads campaign metadata for {len(fetch_ids_campaign)} campaign_id(s)...")
    logging.info(f"🚀 [FETCH] Starting to fetch TikTok Ads campaign metadata for {len(fetch_ids_campaign)} campaign_id(s)...")

    # 1.1.1. Start timing the TikTok Ads campaign metadata fetching
    fetch_time_start = time.time()   
    fetch_sections_status = {}
    fetch_sections_time = {}
    print(f"🔍 [FETCH] Proceeding to fetch raw TikTok Ads campaign metadata at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [FETCH] Proceeding to fetch raw TikTok Ads campaign metadata at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    try:

    # 1.1.2. Validate input for the TikTok Ads campaign metadata fetching
        fetch_section_name = "[FETCH] Validate input for the TikTok Ads campaign metadata fetching"
        fetch_section_start = time.time()    
        try:
            if not fetch_ids_campaign:
                fetch_sections_status[fetch_section_name] = "failed"        
                print("⚠️ [FETCH] Empty TikTok Ads campaign_id_list provided then fetching is suspended.")
                logging.warning("⚠️ [FETCH] Empty TikTok Ads campaign_id_list provided then fetching is suspended.")  
            else:
                fetch_sections_status[fetch_section_name] = "succeed"
                print(f"✅ [FETCH] Successfully validated input for {len(fetch_ids_campaign)} campaign_id(s) of raw TikTok Ads campaign metadata fetching.")
                logging.info(f"✅ [FETCH] Successfully validated input for {len(fetch_ids_campaign)} campaign_id(s) of raw TikTok Ads campaign metadata fetching.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 1.1.3. Prepare fields for TikTok Ads campaign metadata fetching
        fetch_section_name = "[FETCH] Prepare fields for TikTok Ads campaign metadata fetching"
        fetch_section_start = time.time()        
        try:
            fetch_fields_default = [
                "advertiser_id",
                "campaign_id",
                "campaign_name",
                "operation_status",
                "objective_type",
                "create_time"
            ]
            fetch_sections_status[fetch_section_name] = "succeed"
            print(f"🔍 [FETCH] Preparing to fetch TikTok Ads campaign metadata with {fetch_fields_default} field(s)...")
            logging.info(f"🔍 [FETCH] Preparing to fetch TikTok Ads campaign metadata with {fetch_fields_default} field(s)...")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)         
    
    # 1.1.4 Initialize Google Secret Manager client
        fetch_section_name = "[FETCH] Initialize Google Secret Manager client"
        fetch_section_start = time.time()                
        try:
            print(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            google_secret_client = secretmanager.SecretManagerServiceClient()
            print(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
            logging.info(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
            fetch_sections_status[fetch_section_name] = "succeed"
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to initialize Google Secret Manager client for Google Cloud Platform project {PROJECT} due to {e}.")
            logging.error(f"❌ [FETCH] Failed to initialize Google Secret Manager client for Google Cloud Platform project {PROJECT} due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2) 

    # 1.1.5. Get TikTok Ads access token from Google Secret Manager
        fetch_section_name = "[FETCH] Get TikTok Ads access token from Google Secret Manager"
        fetch_section_start = time.time()              
        try: 
            token_secret_id = f"{COMPANY}_secret_all_{PLATFORM}_token_access_user"
            token_secret_name = f"projects/{PROJECT}/secrets/{token_secret_id}/versions/latest"
            token_secret_response = google_secret_client.access_secret_version(request={"name": token_secret_name})
            token_access_user = token_secret_response.payload.data.decode("utf-8")
            print(f"✅ [FETCH] Successfully retrieved TikTok Ads access token for account {ACCOUNT} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved TikTok Ads access token for account {ACCOUNT} from Google Secret Manager.")
            fetch_sections_status[fetch_section_name] = "succeed"
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to retrieve TikTok access token for {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve TikTok access token for {ACCOUNT} from Google Secret Manager due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)
    
    # 1.1.6. Get TikTok Ads advertiser_id from Google Secret Manager
        fetch_section_name = "[FETCH] Get TikTok Ads advertiser_id from Google Secret Manager"
        fetch_section_start = time.time()        
        try:
            print(f"🔍 [FETCH] Retrieving TikTok Ads advertiser_id for account {ACCOUNT} from Google Secret Manager...")
            logging.info(f"🔍 [FETCH] Retrieving TikTok Ads advertiser_id for account {ACCOUNT} from Google Secret Manager...")
            advertiser_secret_id = f"{COMPANY}_secret_{DEPARTMENT}_tiktok_account_id_{ACCOUNT}"
            advertiser_secret_name = f"projects/{PROJECT}/secrets/{advertiser_secret_id}/versions/latest"
            advertiser_secret_response = google_secret_client.access_secret_version(request={"name": advertiser_secret_name})
            advertiser_id = advertiser_secret_response.payload.data.decode("utf-8")
            print(f"✅ [FETCH] Successfully retrieved TikTok Ads advertiser_id {advertiser_id} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved TikTok Ads advertiser_id {advertiser_id} from Google Secret Manager.")
            fetch_sections_status[fetch_section_name] = "succeed"
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to retrieve TikTok Ads advertiser_id for {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve TikTok Ads advertiser_id for {ACCOUNT} from Google Secret Manager due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 1.1.7. Make TikTok Ads API call for advertiser endpoint
        fetch_section_name = "[FETCH] Make TikTok Ads API call for advertiser endpoint"
        fetch_section_start = time.time()     
        try: 
            print(f"🔍 [FETCH] Retrieving advertiser_name for TikTok Ads advertiser_id {advertiser_id}...")
            logging.info(f"🔍 [FETCH] Retrieving advertiser_name for TikTok Ads advertiser_id {advertiser_id}...")
            advertiser_info_url = "https://business-api.tiktok.com/open_api/v1.3/advertiser/info/"
            advertiser_info_headers = {
                "Access-Token": token_access_user,
                "Content-Type": "application/json"
            }            
            payload = {"advertiser_ids": [advertiser_id]}
            response = requests.get(advertiser_info_url, headers=advertiser_info_headers, json=payload)
            advertiser_name = response.json()["data"]["list"][0]["name"]       
            print(f"✅ [FETCH] Successfully retrieved advertiser_name {advertiser_name} for TikTok Ads advertiser_id {advertiser_id}.")
            logging.info(f"✅ [FETCH] Successfully retrieved advertiser_name {advertiser_name} for TikTok Ads advertiser_id {advertiser_id}.")
            fetch_sections_status[fetch_section_name] = "succeed"
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to fetch advertiser_name for TikTok Ads advertiser_id {advertiser_id} due to {e}.")
            logging.error(f"❌ [FETCH] Failed to fetch advertiser_name for TikTok Ads advertiser_id {advertiser_id} due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 1.1.8. Make TikTok Ads API call for campaign endpoint
        fetch_section_name = "[FETCH] Make TikTok Ads API call for campaign metadata"
        fetch_section_start = time.time()    
        fetch_metadatas_campaign = []
        try:
            print(f"🔍 [FETCH] Retrieving TikTok Ads campaign metadata for {len(fetch_ids_campaign)} campaign_id(s)...")
            logging.info(f"🔍 [FETCH] Retrieving TikTok Ads campaign metadata for {len(fetch_ids_campaign)} campaign_id(s)...")
            campaign_get_url = "https://business-api.tiktok.com/open_api/v1.3/campaign/get/"
            campaign_get_headers = {
                "Access-Token": token_access_user,
                "Content-Type": "application/json"
            }
            for fetch_id_campaign in fetch_ids_campaign:
                try:
                    payload = {
                        "advertiser_id": advertiser_id,
                        "filtering": {"campaign_ids": [fetch_id_campaign]},
                        "fields": fetch_fields_default
                    }
                    response = requests.get(campaign_get_url, headers=campaign_get_headers, json=payload)
                    response.raise_for_status()
                    data = response.json()
                    fetch_metadata_campaign = data["data"]["list"][0]
                    fetch_metadata_campaign["advertiser_name"] = advertiser_name
                    fetch_metadatas_campaign.append(fetch_metadata_campaign)
                except Exception as e:
                    print(f"⚠️ [FETCH] Failed to retrieve TikTok Ads campaign metadata for campaign_id {fetch_id_campaign} due to {e}.")
                    logging.warning(f"⚠️ [FETCH] Failed to retrieve TikTok Ads campaign metadata for campaign_id {fetch_id_campaign} due to {e}.")
            fetch_df_flattened = pd.DataFrame(fetch_metadatas_campaign)
            if len(fetch_metadatas_campaign) == len(fetch_ids_campaign):
                fetch_sections_status[fetch_section_name] = "succeed"
                print(f"✅ [FETCH] Successfully retrieved TikTok Ads campaign metadata with {len(fetch_metadatas_campaign)}/{len(fetch_ids_campaign)} campaign_id(s) for advertiser_id {advertiser_id}.")
                logging.info(f"✅ [FETCH] Successfully retrieved TikTok Ads campaign metadata with {len(fetch_metadatas_campaign)}/{len(fetch_ids_campaign)} campaign_id(s) for advertiser_id {advertiser_id}.")
            elif 0 < len(fetch_metadatas_campaign) < len(fetch_ids_campaign):
                fetch_sections_status[fetch_section_name] = "partial"
                print(f"⚠️ [FETCH] Partially retrieved TikTok Ads campaign metadata with {len(fetch_metadatas_campaign)}/{len(fetch_ids_campaign)} campaign_id(s) for advertiser_id {advertiser_id}.")
                logging.warning(f"⚠️ [FETCH] Partially retrieved TikTok Ads campaign metadata with {len(fetch_metadatas_campaign)}/{len(fetch_ids_campaign)} campaign_id(s) for advertiser_id {advertiser_id}.")
            else:
                fetch_sections_status[fetch_section_name] = "failed"
                print(f"❌ [FETCH] Failed to retrieve TikTok Ads campaign metadata with {len(fetch_metadatas_campaign)}/{len(fetch_ids_campaign)} campaign_id(s) for advertiser_id {advertiser_id}.")
                logging.error(f"❌ [FETCH] Failed to retrieve TikTok Ads campaign metadata with {len(fetch_metadatas_campaign)}/{len(fetch_ids_campaign)} campaign_id(s) for advertiser_id {advertiser_id}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 1.1.9. Trigger to enforce schema for TikTok Ads campaign metadata
        fetch_section_name = "[FETCH] Trigger to enforce schema for TikTok Ads campaign metadata"
        fetch_section_start = time.time()
        try:
            print(f"🔄 [FETCH] Trigger to enforce schema for TikTok Ads campaign metadata with {len(fetch_df_flattened)} row(s)...")
            logging.info(f"🔄 [FETCH] Trigger to enforce schema for TikTok Ads campaign metadata with {len(fetch_df_flattened)} row(s)...")            
            fetch_results_schema = enforce_table_schema(fetch_df_flattened, "fetch_campaign_metadata")            
            fetch_summary_enforced = fetch_results_schema["schema_summary_final"]
            fetch_status_enforced = fetch_results_schema["schema_status_final"]
            fetch_df_enforced = fetch_results_schema["schema_df_final"]    
            if fetch_status_enforced == "schema_succeed_all":
                print(f"✅ [FETCH] Successfully triggered to enforce schema for TikTok Ads campaign metadata with {fetch_summary_enforced['schema_rows_output']} row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
                logging.info(f"✅ [FETCH] Successfully triggered to enforce schema for TikTok Ads campaign metadata with {fetch_summary_enforced['schema_rows_output']} row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
                fetch_sections_status[fetch_section_name] = "succeed"
            else:
                fetch_sections_status[fetch_section_name] = "failed"
                print(f"❌ [FETCH] Failed to retrieve schema enforcement final results(s) for TikTok Ads campaign metadata with failed sections "f"{', '.join(fetch_summary_enforced['schema_sections_failed'])}.")
                logging.error(f"❌ [FETCH] Failed to retrieve schema enforcement final results(s) for TikTok Ads campaign metadata with failed sections "f"{', '.join(fetch_summary_enforced['schema_sections_failed'])}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 1.1.10. Summarize fetch result(s) for TikTok Ads campaign metadata
    finally:
        fetch_time_elapsed = round(time.time() - fetch_time_start, 2)
        fetch_df_final = fetch_df_enforced.copy() if "fetch_df_enforced" in locals() and not fetch_df_enforced.empty else pd.DataFrame()
        fetch_sections_total = len(fetch_sections_status) 
        fetch_sections_failed = [k for k, v in fetch_sections_status.items() if v == "failed"] 
        fetch_sections_succeeded = [k for k, v in fetch_sections_status.items() if v == "succeed"]
        fetch_rows_input = len(fetch_ids_campaign)
        fetch_rows_output = len(fetch_df_final)
        fetch_sections_summary = list(dict.fromkeys(
            list(fetch_sections_status.keys()) +
            list(fetch_sections_time.keys())
        ))
        fetch_sections_detail = {
            fetch_section_summary: {
                "status": fetch_sections_status.get(fetch_section_summary, "unknown"),
                "time": fetch_sections_time.get(fetch_section_summary, None),
            }
            for fetch_section_summary in fetch_sections_summary
        }          
        if fetch_sections_failed:
            print(f"❌ [FETCH] Failed to complete TikTok Ads campaign metadata fetching with {fetch_rows_output}/{fetch_rows_input} fetched row(s) due to  {', '.join(fetch_sections_failed)} failed section(s) in {fetch_time_elapsed}s.")
            logging.error(f"❌ [FETCH] Failed to complete TikTok Ads campaign metadata fetching with {fetch_rows_output}/{fetch_rows_input} fetched row(s) due to  {', '.join(fetch_sections_failed)} failed section(s) in {fetch_time_elapsed}s.")
            fetch_status_final = "fetch_failed_all"
        elif fetch_rows_output < fetch_rows_input:
            print(f"⚠️ [FETCH] Partially completed TikTok Ads campaign metadata fetching with {fetch_rows_output}/{fetch_rows_input} fetched row(s) in {fetch_time_elapsed}s.")
            logging.warning(f"⚠️ [FETCH] Partially completed TikTok Ads campaign metadata fetching with {fetch_rows_output}/{fetch_rows_input} fetched row(s) in {fetch_time_elapsed}s.")
            fetch_status_final = "fetch_succeed_partial"
        else:
            print(f"🏆 [FETCH] Successfully completed TikTok Ads campaign metadata fetching with {fetch_rows_output}/{fetch_rows_input} fetched row(s) in {fetch_time_elapsed}s.")
            logging.info(f"🏆 [FETCH] Successfully completed TikTOk Ads campaign metadata fetching with {fetch_rows_output}/{fetch_rows_input} fetched row(s) in {fetch_time_elapsed}s.")
            fetch_status_final = "fetch_succeed_all"    
        fetch_results_final = {
            "fetch_df_final": fetch_df_final,
            "fetch_status_final": fetch_status_final,
            "fetch_summary_final": {
                "fetch_time_elapsed": fetch_time_elapsed, 
                "fetch_sections_total": fetch_sections_total,
                "fetch_sections_succeed": fetch_sections_succeeded, 
                "fetch_sections_failed": fetch_sections_failed, 
                "fetch_sections_detail": fetch_sections_detail, 
                "fetch_rows_input": fetch_rows_input, 
                "fetch_rows_output": fetch_rows_output
            },
        }
    return fetch_results_final

# 1.2. Fetch ad metadata for TikTok Ads
def fetch_ad_metadata(fetch_ids_ad: list[str]) -> pd.DataFrame:
    print(f"🚀 [FETCH] Starting to fetch raw TikTok Ads ad metadata for {len(fetch_ids_ad)} ad_id(s)...")
    logging.info(f"🚀 [FETCH] Starting to fetch raw TikTok Ads ad metadata for {len(fetch_ids_ad)} ad_id(s)...")

    # 1.2.1. Start timing the TikTok Ads ad metadata fetching
    fetch_time_start = time.time()   
    fetch_sections_status = {}
    fetch_sections_time = {}
    print(f"🔍 [FETCH] Proceeding to fetch raw TikTok Ads ad metadata at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [FETCH] Proceeding to fetch raw TikTok Ads ad metadata at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    try:

    # 1.2.2. Validate input for TikTok Ads ad metadata fetching
        fetch_section_name = "[FETCH] Validate input for TikTok Ads ad metadata fetching"
        fetch_section_start = time.time()        
        try:
            if not fetch_ids_ad:
                fetch_sections_status[fetch_section_name] = "failed"        
                print("⚠️ [FETCH] Empty TikTok Ads fetch_ids_ad list provided then fetching is suspended.")
                logging.warning("⚠️ [FETCH] Empty TikTok Ads fetch_ids_ad list provided then fetching is suspended.")
            else:
                fetch_sections_status[fetch_section_name] = "succeed"
                print(f"✅ [FETCH] Successfully validated input for {len(fetch_ids_ad)} ad_id(s) of raw TikTok Ads ad metadata fetching.")
                logging.info(f"✅ [FETCH] Successfully validated input for {len(fetch_ids_ad)} ad_id(s) of raw TikTok Ads ad metadata fetching.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 1.2.3. Prepare field(s) for TikTok Ads ad metadata fetching
        fetch_section_name = "[FETCH] Prepare field(s) for TikTok Ads ad metadata fetching"
        fetch_section_start = time.time()
        try:
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
                "video_id"
            ]
            fetch_sections_status[fetch_section_name] = "succeed" 
            print(f"🔍 [FETCH] Preparing to fetch TikTok Ads ad metadata with {fetch_fields_default} field(s)...")
            logging.info(f"🔍 [FETCH] Preparing to fetch TikTok Ads ad metadata with {fetch_fields_default} field(s)...")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 1.2.4 Initialize Google Secret Manager client
        fetch_section_name = "[FETCH] Initialize Google Secret Manager client"
        fetch_section_start = time.time()          
        try:
            print(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            google_secret_client = secretmanager.SecretManagerServiceClient()
            print(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
            logging.info(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
            fetch_sections_status[fetch_section_name] = "succeed"
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to initialize Google Secret Manager client for Google Cloud Platform project {PROJECT} due to {e}.")
            logging.error(f"❌ [FETCH] Failed to initialize Google Secret Manager client for Google Cloud Platform project {PROJECT} due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2) 

    # 1.2.5. Get TikTok Ads access token from Google Secret Manager
        fetch_section_name = "[FETCH] Get TikTok Ads access token from Google Secret Manager"
        fetch_section_start = time.time()              
        try: 
            token_secret_id = f"{COMPANY}_secret_all_{PLATFORM}_token_access_user"
            token_secret_name = f"projects/{PROJECT}/secrets/{token_secret_id}/versions/latest"
            token_secret_response = google_secret_client.access_secret_version(request={"name": token_secret_name})
            token_access_user = token_secret_response.payload.data.decode("utf-8")
            print(f"✅ [FETCH] Successfully retrieved TikTok Ads access token for account {ACCOUNT} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved TikTok Ads access token for account {ACCOUNT} from Google Secret Manager.")
            fetch_sections_status[fetch_section_name] = "succeed"
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to retrieve TikTok access token for {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve TikTok access token for {ACCOUNT} from Google Secret Manager due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 1.2.6. Get TikTok Ads advertiser_id from Google Secret Manager
        fetch_section_name = "[FETCH] Get TikTok Ads advertiser_id from Google Secret Manager"
        fetch_section_start = time.time()        
        try:
            print(f"🔍 [FETCH] Retrieving TikTok Ads advertiser_id for account {ACCOUNT} from Google Secret Manager...")
            logging.info(f"🔍 [FETCH] Retrieving TikTok Ads advertiser_id for account {ACCOUNT} from Google Secret Manager...")
            advertiser_secret_id = f"{COMPANY}_secret_{DEPARTMENT}_tiktok_account_id_{ACCOUNT}"
            advertiser_secret_name = f"projects/{PROJECT}/secrets/{advertiser_secret_id}/versions/latest"
            advertiser_secret_response = google_secret_client.access_secret_version(request={"name": advertiser_secret_name})
            advertiser_id = advertiser_secret_response.payload.data.decode("utf-8")
            print(f"✅ [FETCH] Successfully retrieved TikTok Ads advertiser_id {advertiser_id} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved TikTok Ads advertiser_id {advertiser_id} from Google Secret Manager.")
            fetch_sections_status[fetch_section_name] = "succeed"
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to retrieve TikTok Ads advertiser_id for {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve TikTok Ads advertiser_id for {ACCOUNT} from Google Secret Manager due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)
    
    # 1.2.7. Make TikTok Ads API call for advertiser endpoint
        fetch_section_name = "[FETCH] Make TikTok Ads API call for advertiser endpoint"
        fetch_section_start = time.time()     
        try: 
            print(f"🔍 [FETCH] Retrieving advertiser_name for TikTok Ads advertiser_id {advertiser_id}...")
            logging.info(f"🔍 [FETCH] Retrieving advertiser_name for TikTok Ads advertiser_id {advertiser_id}...")
            advertiser_info_url = "https://business-api.tiktok.com/open_api/v1.3/advertiser/info/"
            advertiser_info_headers = {
                "Access-Token": token_access_user,
                "Content-Type": "application/json"
            }            
            payload = {"advertiser_ids": [advertiser_id]}
            response = requests.get(advertiser_info_url, headers=advertiser_info_headers, json=payload)
            advertiser_name = response.json()["data"]["list"][0]["name"]       
            print(f"✅ [FETCH] Successfully retrieved advertiser_name {advertiser_name} for TikTok Ads advertiser_id {advertiser_id}.")
            logging.info(f"✅ [FETCH] Successfully retrieved advertiser_name {advertiser_name} for TikTok Ads advertiser_id {advertiser_id}.")
            fetch_sections_status[fetch_section_name] = "succeed"
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to fetch advertiser_name for TikTok Ads advertiser_id {advertiser_id} due to {e}.")
            logging.error(f"❌ [FETCH] Failed to fetch advertiser_name for TikTok Ads advertiser_id {advertiser_id} due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 1.2.8. Make TikTok Ads API call for ad endpoint
        fetch_section_name = "[FETCH] Make TikTok Ads API call for ad endpoint"
        fetch_section_start = time.time()    
        fetch_metadatas_ad = []
        try:
            print(f"🔍 [FETCH] Retrieving TikTok Ads ad metadata for {len(fetch_ids_ad)} ad_id(s)...")
            logging.info(f"🔍 [FETCH] Retrieving TikTok Ads ad metadata for {len(fetch_ids_ad)} ad_id(s)...")
            ad_get_url = "https://business-api.tiktok.com/open_api/v1.3/ad/get/"
            ad_get_headers = {
                "Access-Token": token_access_user,
                "Content-Type": "application/json"
            }
            for fetch_id_ad in fetch_ids_ad:
                try:
                    payload = {
                        "advertiser_id": advertiser_id,
                        "filtering": {"ad_ids": [fetch_id_ad]},
                        "fields": fetch_fields_default
                    }
                    response = requests.get(ad_get_url, headers=ad_get_headers, json=payload)
                    response.raise_for_status()
                    data = response.json()
                    fetch_metadata_ad = data["data"]["list"][0]
                    fetch_metadata_ad["advertiser_name"] = advertiser_name
                    fetch_metadatas_ad.append(fetch_metadata_ad)                    
                except Exception as e:
                    print(f"⚠️ [FETCH] Failed to retrieve TikTok Ads ad metadata for ad_id {fetch_id_ad} due to {e}.")
                    logging.warning(f"⚠️ [FETCH] Failed to retrieve TikTok Ads ad metadata for ad_id {fetch_id_ad} due to {e}.")
            fetch_df_flattened = pd.DataFrame(fetch_metadatas_ad)
            if len(fetch_metadatas_ad) == len(fetch_ids_ad):
                fetch_sections_status[fetch_section_name] = "succeed"
                print(f"✅ [FETCH] Successfully retrieved TikTok Ads ad metadata with {len(fetch_metadatas_ad)}/{len(fetch_ids_ad)} ad_id(s) for advertiser_id {advertiser_id}.")
                logging.info(f"✅ [FETCH] Successfully retrieved TikTok Ads ad metadata with {len(fetch_metadatas_ad)}/{len(fetch_ids_ad)} ad_id(s) for advertiser_id {advertiser_id}.")
            elif 0 < len(fetch_metadatas_ad) < len(fetch_ids_ad):
                fetch_sections_status[fetch_section_name] = "partial"
                print(f"⚠️ [FETCH] Partially retrieved TikTok Ads ad metadata with {len(fetch_metadatas_ad)}/{len(fetch_ids_ad)} ad_id(s) for advertiser_id {advertiser_id}.")
                logging.warning(f"⚠️ [FETCH] Partially retrieved TikTok Ads ad metadata with {len(fetch_metadatas_ad)}/{len(fetch_ids_ad)} ad_id(s) for advertiser_id {advertiser_id}.")
            else:
                fetch_sections_status[fetch_section_name] = "failed"
                print(f"❌ [FETCH] Failed to retrieve TikTok Ads ad metadata with {len(fetch_metadatas_ad)}/{len(fetch_ids_ad)} ad_id(s) for advertiser_id {advertiser_id}.")
                logging.error(f"❌ [FETCH] Failed to retrieve TikTok Ads ad metadata with {len(fetch_metadatas_ad)}/{len(fetch_ids_ad)} ad_id(s) for advertiser_id {advertiser_id}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 1.2.9. Trigger to enforce schema for TikTok Ads ad metadata
        fetch_section_name = "[FETCH] Trigger to enforce schema for TikTok Ads ad metadata"
        fetch_section_start = time.time()
        try:
            print(f"🔄 [FETCH] Trigger to enforce schema for TikTok Ads ad metadata with {len(fetch_df_flattened)} row(s)...")
            logging.info(f"🔄 [FETCH] Trigger to enforce schema for TikTok Ads ad metadata with {len(fetch_df_flattened)} row(s)...")            
            fetch_results_schema = enforce_table_schema(fetch_df_flattened, "fetch_ad_metadata")            
            fetch_summary_enforced = fetch_results_schema["schema_summary_final"]
            fetch_status_enforced = fetch_results_schema["schema_status_final"]
            fetch_df_enforced = fetch_results_schema["schema_df_final"]    
            if fetch_status_enforced == "schema_succeed_all":
                print(f"✅ [FETCH] Successfully triggered to enforce schema for TikTok Ads ad metadata with {fetch_summary_enforced['schema_rows_output']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
                logging.info(f"✅ [FETCH] Successfully triggered to enforce schema for TikTok Ads ad metadata with {fetch_summary_enforced['schema_rows_output']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
                fetch_sections_status[fetch_section_name] = "succeed"
            else:
                fetch_sections_status[fetch_section_name] = "failed"
                print(f"❌ [FETCH] Failed to retrieve schema enforcement final results(s) for TikTok Ads ad metadata with failed sections "f"{', '.join(fetch_summary_enforced['schema_sections_failed'])}.")
                logging.error(f"❌ [FETCH] Failed to retrieve schema enforcement final results(s) for TikTok Ads ad metadata with failed sections "f"{', '.join(fetch_summary_enforced['schema_sections_failed'])}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 1.1.10. Summarize fetch result(s) for TikTok Ads ad metadata
    finally:
        fetch_time_elapsed = round(time.time() - fetch_time_start, 2)
        fetch_df_final = fetch_df_enforced.copy() if "fetch_df_enforced" in locals() and not fetch_df_enforced.empty else pd.DataFrame()
        fetch_sections_total = len(fetch_sections_status) 
        fetch_sections_failed = [k for k, v in fetch_sections_status.items() if v == "failed"] 
        fetch_sections_succeeded = [k for k, v in fetch_sections_status.items() if v == "succeed"]
        fetch_rows_input = len(fetch_ids_ad)
        fetch_rows_output = len(fetch_df_final)
        fetch_sections_summary = list(dict.fromkeys(
            list(fetch_sections_status.keys()) +
            list(fetch_sections_time.keys())
        ))
        fetch_sections_detail = {
            fetch_section_summary: {
                "status": fetch_sections_status.get(fetch_section_summary, "unknown"),
                "time": fetch_sections_time.get(fetch_section_summary, None),
            }
            for fetch_section_summary in fetch_sections_summary
        }          
        if fetch_sections_failed:
            print(f"❌ [FETCH] Failed to complete TikTok Ads ad metadata fetching with {fetch_rows_output}/{fetch_rows_input} fetched row(s) due to  {', '.join(fetch_sections_failed)} failed section(s) in {fetch_time_elapsed}s.")
            logging.error(f"❌ [FETCH] Failed to complete TikTok Ads ad metadata fetching with {fetch_rows_output}/{fetch_rows_input} fetched row(s) due to  {', '.join(fetch_sections_failed)} failed section(s) in {fetch_time_elapsed}s.")
            fetch_status_final = "fetch_failed_all"
        elif fetch_rows_output < fetch_rows_input:
            print(f"⚠️ [FETCH] Partially completed TikTok Ads ad metadata fetching with {fetch_rows_output}/{fetch_rows_input} fetched row(s) in {fetch_time_elapsed}s.")
            logging.warning(f"⚠️ [FETCH] Partially completed TikTok Ads ad metadata fetching with {fetch_rows_output}/{fetch_rows_input} fetched row(s) in {fetch_time_elapsed}s.")
            fetch_status_final = "fetch_succeed_partial"
        else:
            print(f"🏆 [FETCH] Successfully completed TikTok Ads ad metadata fetching with {fetch_rows_output}/{fetch_rows_input} fetched row(s) in {fetch_time_elapsed}s.")
            logging.info(f"🏆 [FETCH] Successfully completed TikTOk Ads ad metadata fetching with {fetch_rows_output}/{fetch_rows_input} fetched row(s) in {fetch_time_elapsed}s.")
            fetch_status_final = "fetch_succeed_all"    
        fetch_results_final = {
            "fetch_df_final": fetch_df_final,
            "fetch_status_final": fetch_status_final,
            "fetch_summary_final": {
                "fetch_time_elapsed": fetch_time_elapsed, 
                "fetch_sections_total": fetch_sections_total,
                "fetch_sections_succeed": fetch_sections_succeeded, 
                "fetch_sections_failed": fetch_sections_failed, 
                "fetch_sections_detail": fetch_sections_detail, 
                "fetch_rows_input": fetch_rows_input, 
                "fetch_rows_output": fetch_rows_output
            },
        }
    return fetch_results_final

# 1.3. Fetch ad creative for TikTok Ads
def fetch_ad_creative() -> pd.DataFrame:
    print("🚀 [FETCH] Starting to fetch TikTok Ads ad creative(s)...")
    logging.info("🚀 [FETCH] Starting to fetch TikTok Ads ad creative(s)...")

    # 1.3.1. Start timing the TikTok Ads ad creative
    fetch_time_start = time.time()   
    fetch_sections_status = {}
    fetch_sections_time = {}
    print(f"🔍 [FETCH] Proceeding to fetch raw TikTok Ads ad creative at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [FETCH] Proceeding to fetch raw TikTok Ads ad creative at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    try:

    # 1.3.2 Initialize Google Secret Manager client
        fetch_section_name = "[FETCH] Initialize Google Secret Manager client"
        fetch_section_start = time.time()                
        try:
            print(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            google_secret_client = secretmanager.SecretManagerServiceClient()
            print(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
            logging.info(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
            fetch_sections_status[fetch_section_name] = "succeed"
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to initialize Google Secret Manager client for Google Cloud Platform project {PROJECT} due to {e}.")
            logging.error(f"❌ [FETCH] Failed to initialize Google Secret Manager client for Google Cloud Platform project {PROJECT} due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 1.3.3. Get TikTok Ads access token from Google Secret Manager
        fetch_section_name = "[FETCH] Get TikTok Ads access token from Google Secret Manager"
        fetch_section_start = time.time()              
        try: 
            token_secret_id = f"{COMPANY}_secret_all_{PLATFORM}_token_access_user"
            token_secret_name = f"projects/{PROJECT}/secrets/{token_secret_id}/versions/latest"
            token_secret_response = google_secret_client.access_secret_version(request={"name": token_secret_name})
            token_access_user = token_secret_response.payload.data.decode("utf-8")
            print(f"✅ [FETCH] Successfully retrieved TikTok Ads access token for account {ACCOUNT} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved TikTok Ads access token for account {ACCOUNT} from Google Secret Manager.")
            fetch_sections_status[fetch_section_name] = "succeed"
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to retrieve TikTok access token for {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve TikTok access token for {ACCOUNT} from Google Secret Manager due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 1.3.4. Get TikTok Ads advertiser_id from Google Secret Manager
        fetch_section_name = "[FETCH] Get TikTok Ads advertiser_id from Google Secret Manager"
        fetch_section_start = time.time()        
        try:
            print(f"🔍 [FETCH] Retrieving TikTok Ads advertiser_id for account {ACCOUNT} from Google Secret Manager...")
            logging.info(f"🔍 [FETCH] Retrieving TikTok Ads advertiser_id for account {ACCOUNT} from Google Secret Manager...")
            advertiser_secret_id = f"{COMPANY}_secret_{DEPARTMENT}_tiktok_account_id_{ACCOUNT}"
            advertiser_secret_name = f"projects/{PROJECT}/secrets/{advertiser_secret_id}/versions/latest"
            advertiser_secret_response = google_secret_client.access_secret_version(request={"name": advertiser_secret_name})
            advertiser_id = advertiser_secret_response.payload.data.decode("utf-8")
            print(f"✅ [FETCH] Successfully retrieved TikTok Ads advertiser_id {advertiser_id} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved TikTok Ads advertiser_id {advertiser_id} from Google Secret Manager.")
            fetch_sections_status[fetch_section_name] = "succeed"
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to retrieve TikTok Ads advertiser_id for {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve TikTok Ads advertiser_id for {ACCOUNT} from Google Secret Manager due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 1.3.5. Make TikTok Ads API call for file/video/ad/search endpoint
        fetch_section_name = "[FETCH] Make TikTok Ads API call for file/video/ad/search endpoint"
        fetch_section_start = time.time()           
        try:
            print(f"🔍 [FETCH] Retrieving TikTok Ads video creative(s) for advertiser_id {advertiser_id}...")
            logging.info(f"🔍 [FETCH] Retrieving TikTok Ads video creative(s) for advertiser_id {advertiser_id}...")
            fetch_creatives_ad = []
            video_search_url = "https://business-api.tiktok.com/open_api/v1.3/file/video/ad/search/"
            video_search_headers = {
                "Access-Token": token_access_user,
                "Content-Type": "application/json"
            }
            page = 1
            has_more = True
            while has_more:
                payload = {
                    "advertiser_id": advertiser_id,
                    "page_size": 100,
                    "page": page
                }
                response = requests.get(video_search_url, headers=video_search_headers, json=payload)
                response.raise_for_status()
                data = response.json()
                if data.get("code") == 0 and data.get("data", {}).get("list"):
                    for record in data["data"]["list"]:
                        fetch_creatives_ad.append({
                            "advertiser_id": advertiser_id,
                            "video_id": record.get("video_id"),
                            "video_cover_url": record.get("video_cover_url"),
                            "preview_url": record.get("preview_url"),
                            "create_time": record.get("create_time")
                        })
                    page_info = data["data"].get("page_info", {})
                    total_page = page_info.get("total_page", 1)
                    has_more = page < total_page
                    page += 1
                else:
                    has_more = False
            fetch_df_flattened = pd.DataFrame(fetch_creatives_ad)
            print(f"✅ [FETCH] Successfully retrieved TikTok Ads ad creative for {len(fetch_df_flattened)} row(s) for TikTok Ads advertiser_id {advertiser_id}.")
            logging.info(f"✅ [FETCH] Successfully retrieved TikTok Ads ad creative for {len(fetch_df_flattened)} row(s) for TikTok Ads advertiser_id {advertiser_id}.")
            fetch_sections_status[fetch_section_name] = "succeed"
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to retrieve TikTok Ads ad creative for advertiser_id {advertiser_id} due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve TikTok Ads ad creative for advertiser_id {advertiser_id} due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)
    
    # 1.3.6. Trigger to enforce schema for TikTok Ads ad creative
        fetch_section_name = "[FETCH] Trigger to enforce schema for TikTok Ads ad creative"
        fetch_section_start = time.time()
        try:
            print(f"🔄 [FETCH] Trigger to enforce schema for TikTok Ads ad creative with {len(fetch_df_flattened)} row(s)...")
            logging.info(f"🔄 [FETCH] Trigger to enforce schema for TikTok Ads ad creative with {len(fetch_df_flattened)} row(s)...")
            fetch_results_schema = enforce_table_schema(fetch_df_flattened, "fetch_ad_creative")            
            fetch_summary_enforced = fetch_results_schema["schema_summary_final"]
            fetch_status_enforced = fetch_results_schema["schema_status_final"]
            fetch_df_enforced = fetch_results_schema["schema_df_final"]    
            if fetch_status_enforced == "schema_succeed_all":
                print(f"✅ [FETCH] Successfully triggered to enforce schema for TikTok Ads ad creative with {fetch_summary_enforced['schema_rows_output']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
                logging.info(f"✅ [FETCH] Successfully triggered to enforce schema for TikTok Ads ad creative with {fetch_summary_enforced['schema_rows_output']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
                fetch_sections_status[fetch_section_name] = "succeed"
            else:
                fetch_sections_status[fetch_section_name] = "failed"
                print(f"❌ [FETCH] Failed to retrieve schema enforcement final results(s) for TikTok Ads ad creative with failed sections "f"{', '.join(fetch_summary_enforced['schema_sections_failed'])}.")
                logging.error(f"❌ [FETCH] Failed to retrieve schema enforcement final results(s) for TikTok Ads ad creative with failed sections "f"{', '.join(fetch_summary_enforced['schema_sections_failed'])}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 1.3.7. Summarize fetch result(s) for TikTok Ads ad creative
    finally:
        fetch_time_elapsed = round(time.time() - fetch_time_start, 2)
        fetch_df_final = fetch_df_enforced.copy() if "fetch_df_enforced" in locals() and not fetch_df_enforced.empty else pd.DataFrame()
        fetch_sections_total = len(fetch_sections_status) 
        fetch_sections_failed = [k for k, v in fetch_sections_status.items() if v == "failed"] 
        fetch_sections_succeeded = [k for k, v in fetch_sections_status.items() if v == "succeed"]
        fetch_rows_output = len(fetch_df_final)
        fetch_sections_summary = list(dict.fromkeys(
            list(fetch_sections_status.keys()) +
            list(fetch_sections_time.keys())
        ))
        fetch_sections_detail = {
            fetch_section_summary: {
                "status": fetch_sections_status.get(fetch_section_summary, "unknown"),
                "time": fetch_sections_time.get(fetch_section_summary, None),
            }
            for fetch_section_summary in fetch_sections_summary
        }          
        if fetch_sections_failed:
            print(f"❌ [FETCH] Failed to complete TikTok Ads ad creative fetching with {fetch_rows_output} fetched row(s) due to  {', '.join(fetch_sections_failed)} failed section(s) in {fetch_time_elapsed}s.")
            logging.error(f"❌ [FETCH] Failed to complete TikTok Ads ad creative fetching with {fetch_rows_output} fetched row(s) due to  {', '.join(fetch_sections_failed)} failed section(s) in {fetch_time_elapsed}s.")
            fetch_status_final = "fetch_failed_all"
        else:
            print(f"🏆 [FETCH] Successfully completed TikTok Ads ad creative fetching with {fetch_rows_output} fetched row(s) in {fetch_time_elapsed}s.")
            logging.info(f"🏆 [FETCH] Successfully completed TikTok Ads ad creative fetching with {fetch_rows_output} fetched row(s) in {fetch_time_elapsed}s.")
            fetch_status_final = "fetch_succeed_all"    
        fetch_results_final = {
            "fetch_df_final": fetch_df_final,
            "fetch_status_final": fetch_status_final,
            "fetch_summary_final": {
                "fetch_time_elapsed": fetch_time_elapsed, 
                "fetch_sections_total": fetch_sections_total,
                "fetch_sections_succeed": fetch_sections_succeeded, 
                "fetch_sections_failed": fetch_sections_failed, 
                "fetch_sections_detail": fetch_sections_detail, 
                "fetch_rows_output": fetch_rows_output
            },
        }
    return fetch_results_final

# 2. FETCH TIKTOK ADS INSIGHTS

# 2.1. Fetch campaign insights for TikTok Ads
def fetch_campaign_insights(start_date: str, end_date: str) -> pd.DataFrame:
    print(f"🚀 [FETCH] Starting to fetch raw TikTok Ads campaign insights from {start_date} to {end_date}...")
    logging.info(f"🚀 [FETCH] Starting to fetch raw TikTok Ads campaign insights from {start_date} to {end_date}...")      

    # 2.1.1. Start timing the TikTok Ads campaign insights fetching
    fetch_time_start = time.time()   
    fetch_sections_status = {}
    fetch_sections_time = {}
    print(f"🔍 [FETCH] Proceeding to fetch TikTok Ads campaign insights from {start_date} to {end_date} at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [FETCH] Proceeding to fetch TikTok Ads campaign insights from {start_date} to {end_date} at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    try:
        
    # 2.1.2. Initialize Google Secret Manager client
        fetch_section_name = "[FETCH] Initialize Google Secret Manager client"
        fetch_section_start = time.time()           
        try:
            print(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            google_secret_client = secretmanager.SecretManagerServiceClient()
            print(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
            logging.info(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
            fetch_sections_status[fetch_section_name] = "succeed"
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to initialize Google Secret Manager client for Google Cloud Platform project {PROJECT} due to {e}.")
            logging.error(f"❌ [FETCH] Failed to initialize Google Secret Manager client for Google Cloud Platform project {PROJECT} due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 2.1.3. Get Facebook Ads access token from Google Secret Manager
        fetch_section_name = "[FETCH] Get Facebook Ads access token from Google Secret Manager"
        fetch_section_start = time.time()          
        try: 
            print(f"🔍 [FETCH] Retrieving Facebook Ads access token for account {ACCOUNT} from Google Secret Manager...")
            logging.info(f"🔍 [FETCH] Retrieving Facebook Ads access token for account {ACCOUNT} from Google Secret Manager...")
            token_secret_id = f"{COMPANY}_secret_all_{PLATFORM}_token_access_user"
            token_secret_name = f"projects/{PROJECT}/secrets/{token_secret_id}/versions/latest"
            token_secret_response = google_secret_client.access_secret_version(request={"name": token_secret_name})
            token_access_user = token_secret_response.payload.data.decode("utf-8")
            print(f"✅ [FETCH] Successfully retrieved Facebook Ads access token for account {ACCOUNT} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved Facebook Ads access token for account {ACCOUNT} from Google Secret Manager.")
            fetch_sections_status[fetch_section_name] = "succeed"
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to retrieve Facebook Ads access token for {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve Facebook Ads access token for {ACCOUNT} from Google Secret Manager due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 2.1.4. Get TikTok Ads advertiser_id from Google Secret Manager
        fetch_section_name = "[FETCH] Get TikTok Ads advertiser_id from Google Secret Manager"
        fetch_section_start = time.time()        
        try:
            print(f"🔍 [FETCH] Retrieving TikTok Ads advertiser_id for account {ACCOUNT} from Google Secret Manager...")
            logging.info(f"🔍 [FETCH] Retrieving TikTok Ads advertiser_id for account {ACCOUNT} from Google Secret Manager...")
            advertiser_secret_id = f"{COMPANY}_secret_{DEPARTMENT}_tiktok_account_id_{ACCOUNT}"
            advertiser_secret_name = f"projects/{PROJECT}/secrets/{advertiser_secret_id}/versions/latest"
            advertiser_secret_response = google_secret_client.access_secret_version(request={"name": advertiser_secret_name})
            advertiser_id = advertiser_secret_response.payload.data.decode("utf-8")
            print(f"✅ [FETCH] Successfully retrieved TikTok Ads advertiser_id {advertiser_id} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved TikTok Ads advertiser_id {advertiser_id} from Google Secret Manager.")
            fetch_sections_status[fetch_section_name] = "succeed"
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to retrieve TikTok Ads advertiser_id for {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve TikTok Ads advertiser_id for {ACCOUNT} from Google Secret Manager due to {e}.")

    # 2.1.5. Make TikTok Ads API call for campaign insights
        fetch_section_name = "[FETCH] Make TikTok Ads API call for campaign insights"
        fetch_section_start = time.time()
        fetch_insights_campaign = []
        fetch_retries_campaign = 2
        try:
            print(f"🔍 [FETCH] Retrieving TikTok Ads campaign insights for advertiser_id {advertiser_id} with BASIC report_type...")
            logging.info(f"🔍 [FETCH] Retrieving TikTok Ads campaign insights for advertiser_id {advertiser_id} with BASIC report_type...")
            campaign_records_json = []
            campaign_report_url = "https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/"
            campaign_report_params = {
                "advertiser_id": advertiser_id,
                "report_type": "BASIC",
                "data_level": "AUCTION_CAMPAIGN",
                "dimensions": ["campaign_id", "stat_time_day"],
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
            for attempt in range(fetch_retries_campaign):
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
                        campaign_records_json.extend(data_list)
                        if len(data_list) < campaign_report_params["page_size"]:
                            break
                        campaign_report_params["page"] += 1                  
                    for campaign_record_json in campaign_records_json:
                        fetch_insight_campaign = {}
                        fetch_insight_campaign.update(campaign_record_json.get("dimensions", {}))
                        fetch_insight_campaign.update(campaign_record_json.get("metrics", {}))
                        fetch_insight_campaign["advertiser_id"] = campaign_report_params["advertiser_id"]
                        fetch_insights_campaign.append(fetch_insight_campaign)
                    fetch_df_flattened = pd.DataFrame(fetch_insights_campaign)
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
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)                        
   
    # 2.1.6. Trigger to enforce schema for TikTok Ads campaign insights
        fetch_section_name = "[FETCH] Trigger to enforce schema for TikTok Ads campaign insights"
        fetch_section_start = time.time()        
        try:            
            print(f"🔄 [FETCH] Trigger to enforce schema for TiKTok Ads campaign insights from {start_date} to {end_date} with {len(fetch_df_flattened)} row(s)...")
            logging.info(f"🔄 [FETCH] Trigger to enforce schema for TiKTok Ads campaign insights from {start_date} to {end_date} with {len(fetch_df_flattened)} row(s)...")
            fetch_results_schema = enforce_table_schema(fetch_df_flattened, "fetch_campaign_insights")            
            fetch_summary_enforced = fetch_results_schema["schema_summary_final"]
            fetch_status_enforced = fetch_results_schema["schema_status_final"]
            fetch_df_enforced = fetch_results_schema["schema_df_final"]    
            if fetch_status_enforced == "schema_succeed_all":
                print(f"✅ [FETCH] Successfully triggered to enforce schema for TikTok Ads campaign insights from {start_date} to {end_date} with {fetch_summary_enforced['schema_rows_output']} row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
                logging.info(f"✅ [FETCH] Successfully triggered to enforce schema for TikTok Ads campaign insights from {start_date} to {end_date} with {fetch_summary_enforced['schema_rows_output']} row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
                fetch_sections_status[fetch_section_name] = "succeed"
            else:
                fetch_sections_status[fetch_section_name] = "failed"
                print(f"❌ [FETCH] Failed to retrieve schema enforcement final results(s) for TikTok Ads campaign insights from {start_date} to {end_date} with failed sections "f"{', '.join(fetch_summary_enforced['schema_sections_failed'])}.")
                logging.error(f"❌ [FETCH] Failed to retrieve schema enforcement final results(s) for TikTok Ads campaign insights from {start_date} to {end_date} with failed sections "f"{', '.join(fetch_summary_enforced['schema_sections_failed'])}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 2.1.7. Summarize fetch result(s) for TikTok Ads campaign insights
    finally:
        fetch_time_elapsed = round(time.time() - fetch_time_start, 2)
        fetch_df_final = fetch_df_enforced.copy() if "fetch_df_enforced" in locals() and not fetch_df_enforced.empty else pd.DataFrame()
        fetch_sections_total = len(fetch_sections_status) 
        fetch_sections_failed = [k for k, v in fetch_sections_status.items() if v == "failed"] 
        fetch_sections_succeeded = [k for k, v in fetch_sections_status.items() if v == "succeed"]
        fetch_days_input = ((pd.to_datetime(end_date) - pd.to_datetime(start_date)).days + 1)
        fetch_days_output = (fetch_df_final["stat_time_day"].nunique() if not fetch_df_final.empty and "stat_time_day" in fetch_df_final.columns else 0)
        fetch_rows_output = len(fetch_df_final)
        fetch_sections_summary = list(dict.fromkeys(
            list(fetch_sections_status.keys()) +
            list(fetch_sections_time.keys())
        ))
        fetch_sections_detail = {
            fetch_section_summary: {
                "status": fetch_sections_status.get(fetch_section_summary, "unknown"),
                "time": fetch_sections_time.get(fetch_section_summary, None),
            }
            for fetch_section_summary in fetch_sections_summary
        }        
        if fetch_sections_failed:
            print(f"❌ [FETCH] Failed to complete TikTok Ads campaign insights fetching from {start_date} to {end_date} with {fetch_days_output}/{fetch_days_input} fetched days(s) due to {', '.join(fetch_sections_failed)} failed section(s) in {fetch_time_elapsed}s.")
            logging.error(f"❌ [FETCH] Failed to complete TikTok Ads campaign insights fetching from {start_date} to {end_date} with {fetch_days_output}/{fetch_days_input} fetched days(s) due to {', '.join(fetch_sections_failed)} failed section(s) in {fetch_time_elapsed}s.")
            fetch_status_final = "fetch_failed_all"
        elif fetch_days_output < fetch_days_input:
            print(f"⚠️ [FETCH] Partially completed TikTok Ads campaign insights fetching from {start_date} to {end_date} with {fetch_days_output}/{fetch_days_input} fetched day(s) in {fetch_time_elapsed}s.")
            logging.warning(f"⚠️ [FETCH] Partially completed TikTok Ads campaign insights fetching from {start_date} to {end_date} with {fetch_days_output}/{fetch_days_input} fetched day(s) in {fetch_time_elapsed}s.")
            fetch_status_final = "fetch_succeed_partial"
        else:
            print(f"🏆 [FETCH] Successfully completed TikTok Ads campaign insights fetching from {start_date} to {end_date} with {fetch_days_output}/{fetch_days_input} fetched day(s) in {fetch_time_elapsed}s.")
            logging.info(f"🏆 [FETCH] Successfully completed TikTok Ads campaign insights fetching from {start_date} to {end_date} with {fetch_days_output}/{fetch_days_input} fetched day(s) in {fetch_time_elapsed}s.")
            fetch_status_final = "fetch_succeed_all"                     
        fetch_results_final = {
            "fetch_df_final": fetch_df_final,
            "fetch_status_final": fetch_status_final,
            "fetch_summary_final": {
                "fetch_time_elapsed": fetch_time_elapsed,
                "fetch_days_input": fetch_days_input,
                "fetch_days_output": fetch_days_output,
                "fetch_sections_total": fetch_sections_total,
                "fetch_sections_succeed": fetch_sections_succeeded,
                "fetch_sections_failed": fetch_sections_failed,
                "fetch_sections_detail": fetch_sections_detail,
                "fetch_rows_output": fetch_rows_output,
            },
        }
    return fetch_results_final

# 2.2. Fetch ad insights for TikTok Ads    
def fetch_ad_insights(start_date: str, end_date: str) -> pd.DataFrame:
    print(f"🚀 [FETCH] Starting to fetch raw TikTok Ads ad insights from {start_date} to {end_date}...")
    logging.info(f"🚀 [FETCH] Starting to fetch raw TikTok Ads ad insights from {start_date} to {end_date}...")       

    # 2.2.1. Start timing the TikTok Ads ad insights fetching
    fetch_time_start = time.time()   
    fetch_sections_status = {}
    fetch_sections_time = {}
    print(f"🔍 [FETCH] Proceeding to fetch TikTok Ads ad insights from {start_date} to {end_date} at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [FETCH] Proceeding to fetch TikTok Ads ad insights from {start_date} to {end_date} at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    try:

    # 2.2.2. Initialize Google Secret Manager client
        fetch_section_name = "[FETCH] Initialize Google Secret Manager client"
        fetch_section_start = time.time()           
        try:
            print(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            google_secret_client = secretmanager.SecretManagerServiceClient()
            print(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
            logging.info(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
            fetch_sections_status[fetch_section_name] = "succeed"
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to initialize Google Secret Manager client for Google Cloud Platform project {PROJECT} due to {e}.")
            logging.error(f"❌ [FETCH] Failed to initialize Google Secret Manager client for Google Cloud Platform project {PROJECT} due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 2.2.3. Get Facebook Ads access token from Google Secret Manager
        fetch_section_name = "[FETCH] Get Facebook Ads access token from Google Secret Manager"
        fetch_section_start = time.time()          
        try: 
            print(f"🔍 [FETCH] Retrieving Facebook Ads access token for account {ACCOUNT} from Google Secret Manager...")
            logging.info(f"🔍 [FETCH] Retrieving Facebook Ads access token for account {ACCOUNT} from Google Secret Manager...")
            token_secret_id = f"{COMPANY}_secret_all_{PLATFORM}_token_access_user"
            token_secret_name = f"projects/{PROJECT}/secrets/{token_secret_id}/versions/latest"
            token_secret_response = google_secret_client.access_secret_version(request={"name": token_secret_name})
            token_access_user = token_secret_response.payload.data.decode("utf-8")
            print(f"✅ [FETCH] Successfully retrieved Facebook Ads access token for account {ACCOUNT} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved Facebook Ads access token for account {ACCOUNT} from Google Secret Manager.")
            fetch_sections_status[fetch_section_name] = "succeed"
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to retrieve Facebook Ads access token for {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve Facebook Ads access token for {ACCOUNT} from Google Secret Manager due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 2.2.4. Get TikTok Ads advertiser_id from Google Secret Manager
        fetch_section_name = "[FETCH] Get TikTok Ads advertiser_id from Google Secret Manager"
        fetch_section_start = time.time()        
        try:
            print(f"🔍 [FETCH] Retrieving TikTok Ads advertiser_id for account {ACCOUNT} from Google Secret Manager...")
            logging.info(f"🔍 [FETCH] Retrieving TikTok Ads advertiser_id for account {ACCOUNT} from Google Secret Manager...")
            advertiser_secret_id = f"{COMPANY}_secret_{DEPARTMENT}_tiktok_account_id_{ACCOUNT}"
            advertiser_secret_name = f"projects/{PROJECT}/secrets/{advertiser_secret_id}/versions/latest"
            advertiser_secret_response = google_secret_client.access_secret_version(request={"name": advertiser_secret_name})
            advertiser_id = advertiser_secret_response.payload.data.decode("utf-8")
            print(f"✅ [FETCH] Successfully retrieved TikTok Ads advertiser_id {advertiser_id} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved TikTok Ads advertiser_id {advertiser_id} from Google Secret Manager.")
            fetch_sections_status[fetch_section_name] = "succeed"
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to retrieve TikTok Ads advertiser_id for {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve TikTok Ads advertiser_id for {ACCOUNT} from Google Secret Manager due to {e}.")

    # 2.2.5. Make TikTok Ads API call for BASIC report_type at AD level
        fetch_section_name = "[FETCH] Make TikTok Ads API call for campaign insights"
        fetch_section_start = time.time()
        fetch_insights_ad = []
        fetch_retries_ad = 2
        try:
            print(f"🔍 [FETCH] Retrieving TikTok Ads ad insights for advertiser_id {advertiser_id} from {start_date} to {end_date}...")
            logging.info(f"🔍 [FETCH] Retrieving TikTok Ads ad insights for advertiser_id {advertiser_id} from {start_date} to {end_date}...")        
            ad_records_json = []
            ad_report_url = "https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/"
            ad_report_params = {
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
            for attempt in range(fetch_retries_ad):
                try:
                    while True:
                        resp = requests.get(
                            ad_report_url,
                            headers={
                                "Access-Token": token_access_user,
                                "Content-Type": "application/json",
                            },
                            json=ad_report_params,
                            timeout=60
                        )
                        resp_json = resp.json()                  
                        if resp_json.get("code") != 0:
                            raise Exception(
                                f"❌ [FETCH] Failed to retrieve TikTok Ads ad-level insights with BASIC report_type due to API error {resp_json.get('message')}."
                            )
                        data_list = resp_json["data"].get("list", [])
                        ad_records_json.extend(data_list)
                        if len(data_list) < ad_report_params["page_size"]:
                            break
                        ad_report_params["page"] += 1
                    for ad_record_json in ad_records_json:
                        fetch_insight_ad = {}
                        fetch_insight_ad.update(ad_record_json.get("dimensions", {}))
                        fetch_insight_ad.update(ad_record_json.get("metrics", {}))
                        fetch_insight_ad["advertiser_id"] = ad_report_params["advertiser_id"]
                        fetch_insights_ad.append(fetch_insight_ad)
                    fetch_df_flattened = pd.DataFrame(fetch_insights_ad)
                    print(fetch_df_flattened.head(10).to_string())
                    print(f"✅ [FETCH] Successfully retrieved {len(fetch_df_flattened)} rows ...")                    
                    print(f"✅ [FETCH] Successfully retrieved {len(fetch_df_flattened)} rows of TikTok Ads ad insights with BASIC report_type.")
                    logging.info(f"✅ [FETCH] Successfully retrieved {len(fetch_df_flattened)} rows of TikTok Ads ad insights with BASIC report_type.")
                    break
                except Exception as e:
                    if attempt < 1:
                        print(f"⚠️ [FETCH] Failed to retrieve TikTok Ads ad insights with BASIC report_type attempt {attempt+1} due to {e} then retrying...")
                        logging.warning(f"⚠️ [FETCH] Failed to retrieve TikTok Ads ad insights with BASIC report_type attempt {attempt+1} due to {e} then retrying...")
                        time.sleep(1)
                    else:
                        print(f"❌ [FETCH] Failed to retrieve TikTok Ads ad insights with BASIC report_type after all attempt(s) due to {e}.")
                        logging.error(f"❌ [FETCH] Failed to retrieve TikTok Ads ad insights with BASIC report_type after all attempt(s) due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)     
    
    # 2.2.6. Trigger to enforce schema for TikTok Ads ad insights
        fetch_section_name = "[FETCH] Trigger to enforce schema for TikTok Ads ad insights"
        fetch_section_start = time.time()        
        try:            
            print(f"🔄 [FETCH] Trigger to enforce schema for TiKTok Ads ad insights from {start_date} to {end_date} with {len(fetch_df_flattened)} row(s)...")
            logging.info(f"🔄 [FETCH] Trigger to enforce schema for TiKTok Ads ad insights from {start_date} to {end_date} with {len(fetch_df_flattened)} row(s)...")
            fetch_results_schema = enforce_table_schema(fetch_df_flattened, "fetch_ad_insights")            
            fetch_summary_enforced = fetch_results_schema["schema_summary_final"]
            fetch_status_enforced = fetch_results_schema["schema_status_final"]
            fetch_df_enforced = fetch_results_schema["schema_df_final"]    
            if fetch_status_enforced == "schema_succeed_all":
                print(f"✅ [FETCH] Successfully triggered to enforce schema for TikTok Ads ad insights from {start_date} to {end_date} with {fetch_summary_enforced['schema_rows_output']} row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
                logging.info(f"✅ [FETCH] Successfully triggered to enforce schema for TikTok Ads ad insights from {start_date} to {end_date} with {fetch_summary_enforced['schema_rows_output']} row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
                fetch_sections_status[fetch_section_name] = "succeed"
            else:
                fetch_sections_status[fetch_section_name] = "failed"
                print(f"❌ [FETCH] Failed to retrieve schema enforcement final results(s) for TikTok Ads ad insights from {start_date} to {end_date} with failed sections "f"{', '.join(fetch_summary_enforced['schema_sections_failed'])}.")
                logging.error(f"❌ [FETCH] Failed to retrieve schema enforcement final results(s) for TikTok Ads ad insights from {start_date} to {end_date} with failed sections "f"{', '.join(fetch_summary_enforced['schema_sections_failed'])}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 2.2.7. Summarize fetch result(s) for TikTok Ads ad insights
    finally:
        fetch_time_elapsed = round(time.time() - fetch_time_start, 2)
        fetch_df_final = fetch_df_enforced.copy() if "fetch_df_enforced" in locals() and not fetch_df_enforced.empty else pd.DataFrame()
        fetch_sections_total = len(fetch_sections_status) 
        fetch_sections_failed = [k for k, v in fetch_sections_status.items() if v == "failed"] 
        fetch_sections_succeeded = [k for k, v in fetch_sections_status.items() if v == "succeed"]
        fetch_days_input = ((pd.to_datetime(end_date) - pd.to_datetime(start_date)).days + 1)
        fetch_days_output = (fetch_df_final["stat_time_day"].nunique() if not fetch_df_final.empty and "stat_time_day" in fetch_df_final.columns else 0)
        fetch_rows_output = len(fetch_df_final)
        fetch_sections_summary = list(dict.fromkeys(
            list(fetch_sections_status.keys()) +
            list(fetch_sections_time.keys())
        ))
        fetch_sections_detail = {
            fetch_section_summary: {
                "status": fetch_sections_status.get(fetch_section_summary, "unknown"),
                "time": fetch_sections_time.get(fetch_section_summary, None),
            }
            for fetch_section_summary in fetch_sections_summary
        }        
        if fetch_sections_failed:
            print(f"❌ [FETCH] Failed to complete TikTok Ads ad insights fetching from {start_date} to {end_date} with {fetch_days_output}/{fetch_days_input} fetched days(s) due to {', '.join(fetch_sections_failed)} failed section(s) in {fetch_time_elapsed}s.")
            logging.error(f"❌ [FETCH] Failed to complete TikTok Ads ad insights fetching from {start_date} to {end_date} with {fetch_days_output}/{fetch_days_input} fetched days(s) due to {', '.join(fetch_sections_failed)} failed section(s) in {fetch_time_elapsed}s.")
            fetch_status_final = "fetch_failed_all"
        elif fetch_days_output < fetch_days_input:
            print(f"⚠️ [FETCH] Partially completed TikTok Ads ad insights fetching from {start_date} to {end_date} with {fetch_days_output}/{fetch_days_input} fetched day(s) in {fetch_time_elapsed}s.")
            logging.warning(f"⚠️ [FETCH] Partially completed TikTok Ads ad insights fetching from {start_date} to {end_date} with {fetch_days_output}/{fetch_days_input} fetched day(s) in {fetch_time_elapsed}s.")
            fetch_status_final = "fetch_succeed_partial"
        else:
            print(f"🏆 [FETCH] Successfully completed TikTok Ads ad insights fetching from {start_date} to {end_date} with {fetch_days_output}/{fetch_days_input} fetched day(s) in {fetch_time_elapsed}s.")
            logging.info(f"🏆 [FETCH] Successfully completed TikTok Ads caadmpaign insights fetching from {start_date} to {end_date} with {fetch_days_output}/{fetch_days_input} fetched day(s) in {fetch_time_elapsed}s.")
            fetch_status_final = "fetch_succeed_all"                     
        fetch_results_final = {
            "fetch_df_final": fetch_df_final,
            "fetch_status_final": fetch_status_final,
            "fetch_summary_final": {
                "fetch_time_elapsed": fetch_time_elapsed,
                "fetch_days_input": fetch_days_input,
                "fetch_days_output": fetch_days_output,
                "fetch_sections_total": fetch_sections_total,
                "fetch_sections_succeed": fetch_sections_succeeded,
                "fetch_sections_failed": fetch_sections_failed,
                "fetch_sections_detail": fetch_sections_detail,
                "fetch_rows_output": fetch_rows_output,
            },
        }
    return fetch_results_final