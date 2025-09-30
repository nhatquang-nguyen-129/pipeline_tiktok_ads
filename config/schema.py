
"""
===================================================================
TIKTOK SCHEMA MODULE
-------------------------------------------------------------------
This module defines and manages **schema-related logic** for the  
TikTok Ads data pipeline, acting as a single source of truth  
for all required fields across different data layers.

It plays a key role in ensuring schema alignment and preventing  
data inconsistency between raw → staging → mart layers.

✔️ Declares expected column names for each entity type   
✔️ Supports schema enforcement in validation and ETL stages  
✔️ Prevents schema mismatch when handling dynamic Facebook API fields  

⚠️ This module does *not* fetch or transform data.  
It only provides schema utilities to support other pipeline components.
===================================================================
"""
# Add external logging libraries for integration
import logging

# Add external Python Pandas libraries for integration
import pandas as pd

# Add external Python NumPy libraries for integration
import numpy as np

# 1. ENSURE SCHEMA FOR GIVEN PYTHON DATAFRAME

# 1.1. Ensure that the given DataFrame contains all required columns with correct datatypes
def ensure_table_schema(df: pd.DataFrame, schema_type: str) -> pd.DataFrame:
    
    # 1.1.1. Define schema mapping for Facebook data type
    mapping_tiktok_schema = {
        "fetch_campaign_metadata": {
            "advertiser_id": str,
            "advertiser_name": str,
            "campaign_id": str,
            "campaign_name": str,
            "operation_status": str,
            "create_time": "datetime64[ns, UTC]"
        },
        "fetch_adset_metadata": {
            "adgroup_id": str,
            "adgroup_name": str,
            "campaign_id": str,
            "placement_type": str,
            "status": str,
            "create_time": "datetime64[ns, UTC]",
            "advertiser_id": str,
        },
        "fetch_ad_metadata": {
            "advertiser_id": str,
            "ad_id": str,
            "ad_name": str,
            "adgroup_id": str,
            "adgroup_name": str,
            "campaign_id": str,
            "campaign_name": str,
            "operation_status": str,
            "create_time": "datetime64[ns, UTC]",
            "ad_format": str,
            "optimization_event": str
        },
        "fetch_ad_creative": {
            "ad_id": str,
            "advertiser_id": str,
            "video_id": str,
            "video_cover_url": str,
            "preview_url": str,
            "create_time": "datetime64[ns, UTC]"
        },
        "fetch_campaign_insights": {
            "advertiser_id": str,
            "campaign_id": str,
            "stat_time_day": str,
            "objective_type": str,
            "result": str,
            "spend": float,
            "impressions": int,
            "clicks": int,
            "video_watched_2s": int,
            "purchase": int,                                  # Unique purchases (app)
            "complete_payment": int,                          # Purchases (website)
            "onsite_total_purchase": int,                     # Purchases (TikTok)
            "offline_shopping_events": int,                   # Purchases (offline)
            "onsite_shopping": int,                           # Purchases (TikTok Shop)
            "messaging_total_conversation_tiktok_direct_message": int  # Conversations (TikTok direct message)
        },
        "fetch_ad_insights": {
            "advertiser_id": str,
            "ad_id": str,
            "stat_time_day": str,
            "objective_type": str,
            "result": str,
            "spend": float,
            "impressions": int,
            "clicks": int,
            "video_watched_2s": int,
            "purchase": int,                                  # Unique purchases (app)
            "complete_payment": int,                          # Purchases (website)
            "onsite_total_purchase": int,                     # Purchases (TikTok)
            "offline_shopping_events": int,                   # Purchases (offline)
            "onsite_shopping": int,                           # Purchases (TikTok Shop)
            "messaging_total_conversation_tiktok_direct_message": int  # Conversations (TikTok direct message)
        },
        "ingest_campaign_metadata": {
            "advertiser_id": str,
            "advertiser_name": str,
            "campaign_id": str,
            "campaign_name": str,
            "operation_status": str,
            "create_time": "datetime64[ns, UTC]"
        },
        "ingest_ad_metadata": {
            "advertiser_id": str,
            "ad_id": str,
            "ad_name": str,
            "adgroup_id": str,
            "adgroup_name": str,
            "campaign_id": str,
            "campaign_name": str,
            "operation_status": str,
            "create_time": "datetime64[ns, UTC]",
            "ad_format": str,
            "optimization_event": str
        },
        "ingest_ad_creative": {
            "ad_id": str,
            "advertiser_id": str,
            "video_id": str,
            "video_cover_url": str,
            "preview_url": str,
            "create_time": "datetime64[ns, UTC]"
        },
        "ingest_campaign_insights": {
            "advertiser_id": str,
            "campaign_id": str,
            "objective_type": str,
            "result": str,
            "stat_time_day": str,
            "spend": float,
            "impressions": int,
            "clicks": int,
            "video_watched_2s": int,
            "purchase": int,                                  # Unique purchases (app)
            "complete_payment": int,                          # Purchases (website)
            "onsite_total_purchase": int,                     # Purchases (TikTok)
            "offline_shopping_events": int,                   # Purchases (offline)
            "onsite_shopping": int,                           # Purchases (TikTok Shop)
            "messaging_total_conversation_tiktok_direct_message": int,  # Conversations (TikTok direct message)
            "last_updated_at": "datetime64[ns, UTC]"
        },
        "ingest_ad_insights": {
            "advertiser_id": str,
            "ad_id": str,
            "objective_type": str,
            "result": str,
            "stat_time_day": str,
            "spend": float,
            "impressions": int,
            "clicks": int,
            "video_watched_2s": int,
            "purchase": int,                                  # Unique purchases (app)
            "complete_payment": int,                          # Purchases (website)
            "onsite_total_purchase": int,                     # Purchases (TikTok)
            "offline_shopping_events": int,                   # Purchases (offline)
            "onsite_shopping": int,                           # Purchases (TikTok Shop)
            "messaging_total_conversation_tiktok_direct_message": int,  # Conversations (TikTok direct message)
            "last_updated_at": "datetime64[ns, UTC]"
        },
        "staging_campaign_insights": {
            "account_id": str,
            "account_name": str,
            "campaign_id": str,
            "campaign_name": str,
            "delivery_status": str,
            "result_type": str,
            "result": str,
            "date_start": str,
            "spend": float,
            "impressions": int,
            "clicks": int,
            "video_watched_2s": int,
            "purchase": int,                                  # Unique purchases (app)
            "complete_payment": int,                          # Purchases (website)
            "onsite_total_purchase": int,                     # Purchases (TikTok)
            "offline_shopping_events": int,                   # Purchases (offline)
            "onsite_shopping": int,                           # Purchases (TikTok Shop)
            "messaging_total_conversation_tiktok_direct_message": int,  # Conversations (TikTok direct message)
            "last_updated_at": "datetime64[ns, UTC]",
            "invalid_campaign_name": str,

            # Các field mapping nội bộ
            "hinh_thuc": str,
            "ma_ngan_sach_cap_1": str,
            "ma_ngan_sach_cap_2": str,
            "khu_vuc": str,
            "nhan_su": str,
            "nganh_hang": str,
            "chuong_trinh": str,
            "noi_dung": str,
            "thang": str,
            "nen_tang": str,
            "phong_ban": str,
            "tai_khoan": str,
            "date": "datetime64[ns, UTC]"
        },
        "staging_ad_insights": {
            "ad_id": str,
            "ad_name": str,
            "campaign_id": str,
            "adset_id": str,
            "adset_name": str,
            "campaign_name": str,
            "date_start": str,
            "date_stop": str,
            "spend": float,
            "delivery_status": str,
            "hinh_thuc": str,
            "ma_ngan_sach_cap_1": str,
            "ma_ngan_sach_cap_2": str,
            "khu_vuc": str,
            "nhan_su": str,
            "nganh_hang": str,
            "chuong_trinh": str,
            "noi_dung": str,
            "thang": str,
            "campaign_name_invalid": bool,
            "vi_tri": str,
            "doi_tuong": str,
            "dinh_dang": str,
            "adset_name_invalid": bool,
            "nen_tang": str,
            "phong_ban": str,
            "tai_khoan": str,
            "thumbnail_url": str,
            "result": int,
            "result_type": str,
            "purchase": int,
            "reach": int,
            "impressions": int,
            "clicks": int,
            "messaging_conversations_started": int,
            "date": "datetime64[ns, UTC]",
        },
    }
    
    # 1.1.2. Validate that the given schema_type exists
    if schema_type not in mapping_tiktok_schema:
        raise ValueError(f"❌ [SCHEMA] Unknown schema_type {schema_type}.")

    # 1.1.3. Retrieve the expected schema definition
    expected_columns = mapping_tiktok_schema[schema_type]
    
    # 1.1.4. Iterate through each expected column
    for col, dtype in expected_columns.items():
        if col not in df.columns:
            df[col] = pd.NA

    # 1.1.5. Handle numeric type
        try:
            if dtype in [int, float]:
                df[col] = df[col].apply(
                    lambda x: x if isinstance(x, (int, float, np.number, type(None))) 
                    else (float(x.replace(",", "")) if isinstance(x, str) and x.replace(",", "").replace(".", "").isdigit() else np.nan)
                )
                df[col] = df[col].fillna(0).astype(dtype)

    # 1.1.6. Handle datetime type
            elif dtype == "datetime64[ns, UTC]":
                df[col] = pd.to_datetime(df[col], errors="coerce")
                if df[col].dt.tz is None:
                    df[col] = df[col].dt.tz_localize("UTC")
                else:
                    df[col] = df[col].dt.tz_convert("UTC")

    # 1.1.7. Handle string or other object types
            else:
                df[col] = df[col].astype(dtype, errors="ignore")
    
    # 1.1.8. Reorder columns
        except Exception as e:
            print(f"❌ [SCHEMA] Failed to coerce column {col} to {dtype} due to {e}.")
            logging.warning(f"❌ [SCHEMA] Failed to coerce column {col} to {dtype} due to {e}.")
    df = df[[col for col in expected_columns]]
    return df