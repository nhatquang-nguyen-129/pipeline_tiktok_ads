"""
==================================================================
TIKTOK FETCHING TESTS
------------------------------------------------------------------
This module validates the correctness and reliability of the 
TikTok Marketing API fetcher functions used in the ingestion 
pipeline. It runs both unit-style mock tests and live integration 
tests against the API.

It ensures that the fetch logic behaves as expected under different 
conditions, returning structured DataFrames consistently across 
mocked and real API calls.

✔️ Runs isolated logic tests using mocked API responses to verify 
   flattening, pagination, and DataFrame construction  
✔️ Executes real API calls with authenticated credentials (via 
   environment variables) to confirm connectivity and capture 
   live debug responses  
✔️ Prints debug outputs of raw API responses and flattened DataFrames 
   for validation of new or experimental metrics  

⚠️ This test module focuses only on *verification of data fetching*.  
It does not cover downstream responsibilities such as schema 
validation, transformation, or storage in BigQuery.
==================================================================
"""
# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add Python Parser CLI for integration
import argparse

# Add Python logging ultilties for integration
import logging

# Add Python requests ultilities for integration
import requests

# Add Python Pandas libraries for integration
import pandas as pd

# Add internal TikTok Ads modules for handling
from src.fetch import (
    fetch_campaign_metadata,
    fetch_adset_metadata,
    fetch_ad_metadata,
    fetch_ad_creative,
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

# 2. FETCH TIKTOK ADS INSIGHTS

# 2.1. Fetch campaign insights for TikTok Ads
def fetch_campaign_insights(start_date: str, end_date: str):
    print(f"🚀 [FETCH] Testing to fetch TikTok Ads campaign insights from {start_date} to {end_date}...")
    logging.info(f"🚀 [FETCH] Testing to fetch TikTok Ads campaign insights from {start_date} to {end_date}...")    

    # 2.1.1. Prepare dummy API response for logical test
    print("🔍 [FETCH] Preparing logic test using dummy TikTok API response...")
    logging.info("🔍 [FETCH] Preparing logic test using dummy TikTok API response...")
    mock_data = {
        "code": 0,
        "data": {
            "list": [
                {
                    "dimensions": {"campaign_id": "123", "stat_time_day": start_date},
                    "metrics": {
                        "spend": 100,
                        "impressions": 5000,
                        "clicks": 200,
                        "video_watched_2s": 300,
                        "purchase": 5,
                        "complete_payment": 3,
                        "onsite_total_purchase": 2,
                        "offline_shopping_events": 1,
                        "onsite_shopping": 0,
                        "messaging_total_conversation_tiktok_direct_message": 7,
                    },
                }
            ]
        },
    }

    class DummyResp:
        def json(self):
            return mock_data

    def dummy_get(*args, **kwargs):
        return DummyResp()

    old_get = requests.get
    requests.get = dummy_get
    print("✅ [FETCH] Sucessfully prepared dummy TikTok API response for logic test.")
    logging.info("✅ [FETCH] Sucessfully prepared dummy TikTok API response for logic test.")
    
    # 2.2.2. Run logical test with dummy API response
    try:
        print(f"🔄 [FETCH] Running TikTok Ads campaign insights logical test with dummy API response...")
        logging.info(f"🔄 [FETCH] Running TikTok Ads campaign insights logical test with dummy API response...")
        df_mock = fetch_campaign_insights(start_date, end_date)
    except Exception as e:
        print(f"❌ [FETCH] Error while executing fetch_campaign_insights for mock test: {e}")
        logging.exception("❌ [FETCH] Error while executing fetch_campaign_insights for mock test")
        raise
    finally:
        requests.get = old_get

    # 2.2.3. Preview Python Dataframe before assertions(s)
    print("\n✅ [FETCH] Previewing Python DataFrame flatten)...")
    print(df_mock.head())

    # Assertions with detailed logging
    try:
        assert isinstance(df_mock, pd.DataFrame), "Result is not a pandas DataFrame"
        assert not df_mock.empty, "DataFrame is empty"
        assert df_mock.loc[0, "spend"] == 100, f"Unexpected spend: {df_mock.loc[0,'spend']!r}"
        assert df_mock.loc[0, "purchase"] == 5, f"Unexpected purchase: {df_mock.loc[0,'purchase']!r}"

        print("✅ [ASSERT] All logical-test assertions passed.")
        logging.info("✅ [ASSERT] All logical-test assertions passed for dummy response.")
    except AssertionError as ae:
        # Log detailed debug info then re-raise so CI/test runner fails
        print("❌ [ASSERT] Logical test assertions failed:", ae)
        logging.error("❌ [ASSERT] Logical test assertions failed: %s", ae)

        # thêm info debug: head, dtypes, full row 0 repr
        print("\n🐞 [DEBUG MOCK] DataFrame head (for failed assert):")
        print(df_mock.head().to_string(index=False))
        print("\n🐞 [DEBUG MOCK] DataFrame dtypes:")
        print(df_mock.dtypes.to_string())

        # nếu có row 0, in chi tiết giá trị row 0 để biết sai trường nào
        if not df_mock.empty and 0 in df_mock.index:
            print("\n🐞 [DEBUG MOCK] Row[0] values:")
            print(df_mock.loc[0].to_dict())

        # cũng log đầy đủ traceback để dễ điều tra
        logging.exception("Detailed failure context for mock test")

        # re-raise để test/CI báo fail
        raise

    # 3. Run real API test
    print("\n==================================================================")
    print("🔄 [FETCH] Running TikTok Ads campaign insights REAL API test...")
    logging.info("🔄 [FETCH] Running TikTok Ads campaign insights REAL API test...")

    try:
        # --- Call API thật ---
        tiktok_basic_url = "https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/"
        tiktok_basic_params = {
            "advertiser_id": tiktok_advertiser_id,
            "report_type": "BASIC",
            "data_level": "AUCTION_CAMPAIGN",
            "dimensions": ["campaign_id", "stat_time_day"],
            "metrics": [
                "objective_type",
                "result",
                "spend",
                "impressions",
                "clicks",
                "video_watched_2s",
                "purchase",
                "complete_payment",
                "onsite_total_purchase",
                "offline_shopping_events",
                "onsite_shopping",
                "messaging_total_conversation_tiktok_direct_message",
            ],
            "start_date": start_date,
            "end_date": end_date,
            "page_size": 1000,
            "page": 1,
        }

        all_basic_records = []
        while True:
            resp = requests.get(
                tiktok_basic_url,
                headers={
                    "Access-Token": tiktok_access_token,
                    "Content-Type": "application/json",
                },
                json=tiktok_basic_params,
                timeout=60,
            )
            resp_json = resp.json()

            if resp_json.get("code") != 0:
                raise Exception(f"❌ [FETCH] API error: {resp_json.get('message')}")

            data_list = resp_json["data"].get("list", [])
            all_basic_records.extend(data_list)

            if len(data_list) < tiktok_basic_params["page_size"]:
                break
            tiktok_basic_params["page"] += 1

        # 1️⃣ Debug raw JSON
        print("\n🐞 [DEBUG REAL - RAW API JSON] Sample (first record):")
        if all_basic_records:
            print(all_basic_records[0])
        else:
            print("⚠️ No records in API response.")

        # 2️⃣ Flatten
        flattened_basic = []
        for record in all_basic_records:
            row = {}
            row.update(record.get("dimensions", {}))
            row.update(record.get("metrics", {}))
            row["advertiser_id"] = tiktok_basic_params["advertiser_id"]
            flattened_basic.append(row)

        print("\n🐞 [DEBUG REAL - FLATTENED LIST] Sample (first record):")
        if flattened_basic:
            print(flattened_basic[0])
        else:
            print("⚠️ No records after flatten.")

        # 3️⃣ DataFrame
        df_real = pd.DataFrame(flattened_basic)
        print("\n🐞 [DEBUG REAL - FINAL DataFrame] Preview (5 rows):")
        print(df_real.head())

        # ✅ Passed
        assert isinstance(df_real, pd.DataFrame), "Result is not a DataFrame"
        print("✅ [ASSERT] API test passed.")

    except Exception as e:
        print(f"❌ [ASSERT] API test failed due to {e}")
        logging.error("❌ [ASSERT] API test failed due to %s", e)
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_date", required=True, help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end_date", required=True, help="End date in YYYY-MM-DD format")
    args = parser.parse_args()

    run_tests(args.start_date, args.end_date)
