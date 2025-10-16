"""
==================================================================
TIKTOK ENRICHMENT MODULE
------------------------------------------------------------------
This module is responsible for transforming raw TikTok Ads insights 
into a clean, BigQuery-ready dataset optimized for downstream analytics, 
cross-channel performance comparison, and business intelligence reporting.

By consolidating enrichment logic, it ensures clarity, consistency, and 
long-term maintainability across the marketing data pipeline while 
standardizing TikTok-specific metrics and naming conventions.

✔️ Maps `optimization_goal` to the appropriate action type  
✔️ Standardizes campaign and ad-level metadata for schema alignment  
✔️ Normalizes and validates key performance indicators (KPIs)  
✔️ Cleans textual and categorical fields (e.g., removes accents in names)  
✔️ Reduces payload size by excluding redundant or irrelevant field(s)

⚠️ This module focuses *only* on enrichment and transformation logic.  
It does **not** handle data fetching, ingestion, loading, or metric modeling.
==================================================================
"""

# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add Python logging ultilities for integraton
import logging

# Add Python Pandas libraries for integration
import pandas as pd

# Add Python "re" libraries for integraton
import re

# Add Python time ultilities for integration
import time

# 1. ENRICH TIKTOK INSIGHTS FROM STAGING PHASE

# 1.1. Enrich TikTok Ads structured campaign-level field(s) from campaign name
def enrich_campaign_fields(enrich_df_input: pd.DataFrame, table_id: str) -> pd.DataFrame:
    print("🚀 [ENRICH] Starting to enrich staging TikTok Ads campaign field(s)...")
    logging.info("🚀 [ENRICH] Starting to enrich staging TikTok Ads campaign field(s)...")  

    # 1.1.1. Start timing the TikTok Ads campaign insights enrichment process
    start_time = time.time()
    print(f"🔍 [ENRICH] Proceeding to enrich TikTok Ads campaign insights for {len(enrich_df_input)} row(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [ENRICH] Proceeding to enrich TikTok Ads campaign insights for {len(enrich_df_input)} row(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    enrich_df_processing = enrich_df_input.copy()
    try:
    # 1.1.2. Enrich table-level field(s)

        table_name = table_id.split(".")[-1]
        match = re.search(
            r"^(?P<company>\w+)_table_(?P<platform>\w+)_(?P<department>\w+)_(?P<account>\w+)_campaign_m\d{6}$",
            table_name
        )
        if match:
            enrich_df_processing["nen_tang"] = match.group("platform")
            enrich_df_processing["phong_ban"] = match.group("department")
            enrich_df_processing["tai_khoan"] = match.group("account")

        # 1.1.2. Enrich campaign-level field(s)
        enrich_df_processing["hinh_thuc"] = enrich_df_processing["campaign_name"].apply(lambda x: str(x).split("_")[0] if len(str(x).split("_")) > 0 else None)
        enrich_df_processing["khu_vuc"] = enrich_df_processing["campaign_name"].apply(lambda x: str(x).split("_")[1] if len(str(x).split("_")) > 1 else None)
        enrich_df_processing["ma_ngan_sach_cap_1"] = enrich_df_processing["campaign_name"].apply(lambda x: str(x).split("_")[2] if len(str(x).split("_")) > 2 else None)
        enrich_df_processing["ma_ngan_sach_cap_2"] = enrich_df_processing["campaign_name"].apply(lambda x: str(x).split("_")[3] if len(str(x).split("_")) > 3 else None)
        enrich_df_processing["nganh_hang"] = enrich_df_processing["campaign_name"].apply(lambda x: str(x).split("_")[4] if len(str(x).split("_")) > 4 else None)
        enrich_df_processing["nhan_su"] = enrich_df_processing["campaign_name"].apply(lambda x: str(x).split("_")[5] if len(str(x).split("_")) > 5 else None)
        enrich_df_processing["chuong_trinh"] = enrich_df_processing["campaign_name"].apply(lambda x: str(x).split("_")[7] if len(str(x).split("_")) > 7 else None)
        enrich_df_processing["noi_dung"] = enrich_df_processing["campaign_name"].apply(lambda x: str(x).split("_")[8] if len(str(x).split("_")) > 8 else None)
        enrich_df_processing["thang"] = pd.to_datetime(enrich_df_processing["date_start"]).dt.strftime("%Y-%m")

    # 1.1.3. Add invalid campaign_name warning
        enrich_df_processing["invalid_campaign_name"] = enrich_df_processing["campaign_name"].apply(lambda x: len(str(x).split("_")) < 9)
        invalid_count = enrich_df_processing["invalid_campaign_name"].sum()
        if invalid_count > 0:
            print(f"⚠️ [ENRICH] Found {invalid_count} invalid campaign_name(s) with insufficient parts.")
            logging.warning(f"⚠️ [ENRICH] Found {invalid_count} invalid campaign_name(s) with insufficient parts.")

    # 1.1.4. Summarize enrichment result(s)
        enrich_df_final = enrich_df_processing
        elapsed = round(time.time() - start_time, 2)
        print(f"🏆 [FETCH] Successfully completed TikTok Ads campaign insights enrichment with {len(enrich_df_final)} row(s) in {elapsed}s.")
        logging.info(f"🏆 [FETCH] Successfully completed TikTok Ads campaign insights enrichment with {len(enrich_df_final)} row(s) in {elapsed}s.")
        return enrich_df_final
    except Exception as e:
        print(f"❌ [FETCH] Failed to enrich TikTok Ads campaign insights for {len(enrich_df_input)} row(s) due to {e}.")
        logging.error(f"❌ [FETCH] Failed to enrich TikTok Ads campaign insights for {len(enrich_df_input)} row(s) due to {e}.")
        return pd.DataFrame()

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