"""
==================================================================
TIKTOK ENRICHMENT MODULE
------------------------------------------------------------------
This module transforms raw TikTok Ads insights into a clean, 
BigQuery-ready format optimized for downstream analytics and reporting.

It provides a centralized structure for mapping business logic and 
cleaning performance data while promoting clarity, consistency, 
and maintainability across the data pipeline.

✔️ Maps `optimization_goal` to the relevant action type  
✔️ Extracts standardized performance metrics and ensures schema consistency  
✔️ Reduces payload by removing unnecessary fields such as the full `actions` array

⚠️ This module focuses *only* on enrichment.  
It does **not** handle data fetching, ingestion, or metric modeling.
==================================================================
"""
# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add logging ultilities for integraton
import logging

# Add Python Pandas libraries for integration
import pandas as pd

# Add Python "re" libraries for integraton
import re

# 1. ENRICH TIKTOK INSIGHTS FROM STAGING PHASE

# 1.1. Enrich TikTok Ads structured campaign-level field(s) from campaign name
def enrich_campaign_fields(df: pd.DataFrame, table_id: str) -> pd.DataFrame:
    print("🚀 [ENRICH] Starting to enrich staging TikTok Ads campaign field(s)...")
    logging.info("🚀 [ENRICH] Starting to enrich staging TikTok Ads campaign field(s)...")  

    try:
    
    # 1.1.1. Enrich table-level field(s)
        table_name = table_id.split(".")[-1]
        match = re.search(
            r"^(?P<company>\w+)_table_(?P<platform>\w+)_(?P<department>\w+)_(?P<account>\w+)_campaign_m\d{6}$",
            table_name
        )
        if match:
            df["nen_tang"] = match.group("platform")
            df["phong_ban"] = match.group("department")
            df["tai_khoan"] = match.group("account")

    # 1.1.2. Enrich campaign-level field(s)
        def safe_split(name: str, idx: int):
            parts = str(name).split("_")
            return parts[idx] if len(parts) > idx else None

        df["hinh_thuc"]         = df["campaign_name"].apply(lambda x: safe_split(x, 0))
        df["khu_vuc"]           = df["campaign_name"].apply(lambda x: safe_split(x, 1))
        df["ma_ngan_sach_cap_1"]= df["campaign_name"].apply(lambda x: safe_split(x, 2))
        df["ma_ngan_sach_cap_2"]= df["campaign_name"].apply(lambda x: safe_split(x, 3))
        df["nganh_hang"]        = df["campaign_name"].apply(lambda x: safe_split(x, 4))
        df["nhan_su"]           = df["campaign_name"].apply(lambda x: safe_split(x, 5))
        df["chuong_trinh"]      = df["campaign_name"].apply(lambda x: safe_split(x, 7))
        df["noi_dung"]          = df["campaign_name"].apply(lambda x: safe_split(x, 8))
        df["thang"]             = pd.to_datetime(df["date_start"]).dt.strftime("%Y-%m")

    # 1.1.3. Add invalid campaign_name warning
        df["invalid_campaign_name"] = df["campaign_name"].apply(
            lambda x: len(str(x).split("_")) < 9
        )

        # 1.1.4. Warnings if invalid campaigns exist
        invalid_count = df["invalid_campaign_name"].sum()
        if invalid_count > 0:
            print(f"⚠️ [ENRICH] Found {invalid_count} invalid campaign_name(s) with insufficient parts.")
            logging.warning(f"⚠️ [ENRICH] Found {invalid_count} invalid campaign_name(s) with insufficient parts.")

        # 1.1.5. Summarize enrichment result(s)
        print(f"✅ [ENRICH] Successfully enriched field(s) for staging TikTok Ads campaign insights with {len(df)} row(s).")
        logging.info(f"✅ [ENRICH] Successfully enriched field(s) for staging TikTok Ads campaign insights with {len(df)} row(s).")

    except Exception as e:
        print(f"❌ [ENRICH] Failed to enrich staging TikTok Ads campaign field(s) due to {e}.")
        logging.error(f"❌ [ENRICH] Failed to enrich staging TikTok Ads campaign field(s) due to {e}.")
    return df

# 1.2. Enrich structured ad-level fields from adset_name and campaign_name
def enrich_ad_fields(df: pd.DataFrame, table_id: str) -> pd.DataFrame:   
    print("🚀 [ENRICH] Starting to enrich TikTok Ads staging ad field(s)...")
    logging.info("🚀 [ENRICH] Starting to enrich TikTok Ads staging ad field(s)...")    

    # 1.2.1. Enrich table-level field(s)
    try:
        table_name = table_id.split(".")[-1]
        match = re.search(
            r"^(?P<company>\w+)_table_(?P<platform>\w+)_(?P<department>\w+)_(?P<account>\w+)_ad_m\d{6}$",
            table_name
        )
        if match:
            df["nen_tang"] = match.group("platform")
            df["phong_ban"] = match.group("department")
            df["tai_khoan"] = match.group("account")   

    # 1.2.2. Enrich adset-level field(s)
        if "adset_name" in df.columns:
            adset_parts = df["adset_name"].fillna("").str.split("_")
            df["vi_tri"] = adset_parts.str[0].fillna("unknown")
            df["doi_tuong"] = adset_parts.str[1].fillna("unknown")
            df["dinh_dang"] = adset_parts.str[2].fillna("unknown")
            df["adset_name_invalid"] = adset_parts.str.len() < 3

    # 1.2.3. Enrich campaign-level field(s)
        if "campaign_name" in df.columns:
            camp_parts = df["campaign_name"].fillna("").str.split("_")
            df["hinh_thuc"] = camp_parts.str[0].fillna("unknown")
            df["khu_vuc"] = camp_parts.str[1].fillna("unknown")
            df["ma_ngan_sach_cap_1"] = camp_parts.str[2].fillna("unknown")
            df["ma_ngan_sach_cap_2"] = camp_parts.str[3].fillna("unknown")
            df["nganh_hang"] = camp_parts.str[4].fillna("unknown")
            df["nhan_su"]   = camp_parts.str[5].fillna("unknown")
            df["chuong_trinh"] = camp_parts.str[7].fillna("unknown")
            df["noi_dung"] = camp_parts.str[8].fillna("unknown")
            df["campaign_name_invalid"] = camp_parts.str.len() < 9

    # 1.2.4. Enrich other ad-level field(s)
        if "date_start" in df.columns:
           df["thang"] = pd.to_datetime(df["date_start"]).dt.strftime("%Y-%m")

    # 1.2.5. Summarize enrich result(s)
        print(f"✅ [ENRICH] Successfully enriched fields for staging TikTok Ads ad insights with {len(df)} row(s).")
        logging.info(f"✅ [ENRICH] Successfully enriched fields for staging TikTok Ads ad insights with {len(df)} row(s).")
    except Exception as e:
        print(f"❌ [ENRICH] Failed to enrich fields for staging TikTok Ads ad insights due to {e}.")
        logging.warning(f"❌ [ENRICH] Failed to enrich fields for staging TikTok Ads ad insights due to {e}.")    
    return df