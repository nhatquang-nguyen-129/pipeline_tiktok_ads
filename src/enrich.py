"""
==================================================================
TIKTOK ENRICHMENT MODULE
------------------------------------------------------------------
This module is responsible for transforming raw TikTok Ads insights 
into a clean, BigQuery-ready dataset optimized for downstream analytics, 
cross-channel performance comparison, and business intelligence reporting.

By consolidating enrichment logic, it ensures clarity, consistency and 
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
def enrich_campaign_fields(enrich_df_input: pd.DataFrame, enrich_table_id: str) -> pd.DataFrame:
    print("🚀 [ENRICH] Starting to enrich staging TikTok Ads campaign field(s)...")
    logging.info("🚀 [ENRICH] Starting to enrich staging TikTok Ads campaign field(s)...")  

    # 1.1.1. Start timing the TikTok Ads campaign insights enrichment
    enrich_time_start = time.time()
    enrich_sections_status = {}
    print(f"🔍 [ENRICH] Proceeding to enrich staging TikTok Ads campaign insights for {len(enrich_df_input)} row(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [ENRICH] Proceeding to enrich staging TikTok Ads campaign insights for {len(enrich_df_input)} row(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    
    # 1.1.2. Validate input for the staging TikTok Ads campaign insights enrichment
    if enrich_df_input.empty:
        enrich_sections_status["1.1.2. Validate input for the staging TikTok Ads campaign insights enrichment"] = "failed"
        print("⚠️ [ENRICH] Empty staging TikTok Ads campaign insights provided then enrichment is suspended.")
        logging.warning("⚠️ [ENRICH] Empty staging TikTok Ads campaign insights provided then enrichment is suspended.")
        raise ValueError("⚠️ [ENRICH] Empty staging TikTok Ads campaign insights provided then enrichment is suspended.")
    else:
        enrich_sections_status["1.1.2. Validate input for the staging TikTok Ads campaign insights enrichment"] = "succeed"
        print("✅ [ENRICH] Successfully validated input for staging TikTok Ads campaign insights enrichment.")
        logging.info("✅ [ENRICH] Successfully validated input for staging TikTok Ads campaign insights enrichment.")

    try:
    
    # 1.1.3. Enrich table-level field(s) for staging TikTok Ads campaign insights
        try: 
            print(f"🔍 [ENRICH] Enriching table-level field(s) for staging TikTok Ads campaign insights with {len(enrich_df_input)} row(s)...")
            logging.info(f"🔍 [ENRICH] Enriching table-level field(s) for staging TikTok Ads campaign insights with {len(enrich_df_input)} row(s)...")
            enrich_df_table = enrich_df_input.copy()
            enrich_df_table["spend"] = pd.to_numeric(enrich_df_table["spend"], errors="coerce").fillna(0)
            enrich_table_name = enrich_table_id.split(".")[-1]
            match = re.search(
                r"^(?P<company>\w+)_table_(?P<platform>\w+)_(?P<department>\w+)_(?P<account>\w+)_campaign_m\d{6}$",
                enrich_table_name
            )
            if match:
                enrich_df_table["nen_tang"] = match.group("platform")
                enrich_df_table["phong_ban"] = match.group("department")
                enrich_df_table["tai_khoan"] = match.group("account")
            print(f"✅ [ENRICH] Successfully enriched table-level field(s) for staging TikTok Ads campaign insights with {len(enrich_df_table)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched table-level field(s) for staging TikTok Ads campaign insights with {len(enrich_df_table)} row(s).")
            enrich_sections_status["1.1.3. Enrich table-level field(s) for staging TikTok Ads campaign insights"] = "succeed"
        except Exception as e:
            enrich_sections_status["1.1.3. Enrich table-level field(s) for staging TikTok Ads campaign insights"] = "failed"
            print(f"❌ [ENRICH] Failed to enrich table-level field(s) for staging TikTok Ads campaign insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich table-level field(s) for staging TikTok Ads campaign insights due to {e}.")
            raise RuntimeError(f"❌ [ENRICH] Failed to enrich table-level field(s) for staging TikTok Ads campaign insights due to {e}.")

    # 1.1.4. Enrich campaign-level field(s) for staging TikTok Ads campaign insights
        try:
            print(f"🔍 [ENRICH] Enriching campaign-level field(s) for staging TikTok Ads campaign insights with {len(enrich_df_table)} row(s)...")
            logging.info(f"🔍 [ENRICH] Enriching campaign-level field(s) for staging TikTok Ads campaign insights with {len(enrich_df_table)} row(s)...")
            enrich_df_campaign = enrich_df_table.copy()
            enrich_df_campaign["hinh_thuc"] = enrich_df_campaign["campaign_name"].str.split("_").str    [0]
            enrich_df_campaign["khu_vuc"] = enrich_df_campaign["campaign_name"].str.split("_").str[1]
            enrich_df_campaign["ma_ngan_sach_cap_1"] = enrich_df_campaign["campaign_name"].str.split("_").str[2]
            enrich_df_campaign["ma_ngan_sach_cap_2"] = enrich_df_campaign["campaign_name"].str.split("_").str[3]
            enrich_df_campaign["nganh_hang"] = enrich_df_campaign["campaign_name"].str.split("_").str[4]
            enrich_df_campaign["nhan_su"] = enrich_df_campaign["campaign_name"].str.split("_").str[5]
            enrich_df_campaign["chuong_trinh"] = enrich_df_campaign["campaign_name"].str.split("_").str[7]
            enrich_df_campaign["noi_dung"] = enrich_df_campaign["campaign_name"].str.split("_").str[8]
            enrich_df_campaign["thang"] = pd.to_datetime(enrich_df_campaign["date_start"]).dt.strftime("%Y-%m")
            enrich_df_campaign["invalid_campaign_name"] = enrich_df_campaign["campaign_name"].apply(lambda x: len(str(x).split("_")) < 9)
            invalid_count = enrich_df_campaign["invalid_campaign_name"].sum()
            if invalid_count > 0:
                print(f"⚠️ [ENRICH] Found {invalid_count} invalid campaign_name(s) with insufficient parts.")
                logging.warning(f"⚠️ [ENRICH] Found {invalid_count} invalid campaign_name(s) with insufficient parts.")
            print(f"✅ [ENRICH] Successfully enriched campaign-level field(s) for staging TikTok Ads campaign insights with {len(enrich_df_campaign)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched campaign-level field(s) for staging TikTok Ads campaign insights with {len(enrich_df_campaign)} row(s).")
            enrich_sections_status["1.1.4. Enrich campaign-level field(s) for staging TikTok Ads campaign insights"] = "succeed"
        except Exception as e:
            enrich_sections_status["1.1.4. Enrich campaign-level field(s) for staging TikTok Ads campaign insights"] = "failed"
            print(f"❌ [ENRICH] Failed to enrich campaign-level field(s) for staging TikTok Ads campaign insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich campaign-level field(s) for staging TikTok Ads campaign insights due to {e}.")
            raise RuntimeError(f"❌ [ENRICH] Failed to enrich campaign-level field(s) for staging TikTok Ads campaign insights due to {e}.")

    # 1.1.5. Summarize enrichment result(s) for staging TikTok Ads campaign insights
    finally:
        enrich_time_elapsed = round(time.time() - enrich_time_start, 2)
        enrich_df_final = enrich_df_campaign.copy() if not enrich_df_campaign.empty else pd.DataFrame()
        enrich_sections_failed = [k for k, v in enrich_sections_status.items() if v == "failed"]
        enrich_rows_output = len(enrich_df_final)
        if any(v == "failed" for v in enrich_sections_status.values()):
            print(f"❌ [ENRICH] Failed to complete staging TikTok Ads campaign insights enrichment due to section(s) {', '.join(enrich_sections_failed)} in {enrich_time_elapsed}s.")
            logging.error(f"❌ [ENRICH] Failed to complete staging TikTok Ads campaign insights enrichment due to section(s) {', '.join(enrich_sections_failed)} in {enrich_time_elapsed}s.")
            enrich_status_final = "enrich_failed_all"        
        else:
            print(f"🏆 [ENRICH] Successfully completed staging TikTok Ads campaign insights enrichment for all {len(enrich_sections_status)} section(s) with {enrich_rows_output} row(s) output in {enrich_time_elapsed}s.")
            logging.info(f"🏆 [ENRICH] Successfully completed staging TikTok Ads campaign insights enrichment for all {len(enrich_sections_status)} section(s) with {enrich_rows_output} row(s) output in {enrich_time_elapsed}s.")
            enrich_status_final = "enrich_success_all"
        return {
            "enrich_df_final": enrich_df_final,
            "enrich_status_final": enrich_status_final,
            "enrich_summary_final": {
                "enrich_time_elapsed": enrich_time_elapsed,
                "enrich_rows_output": enrich_rows_output,
                "enrich_sections_total": len(enrich_sections_status),
                "enrich_sections_failed": [k for k, v in enrich_sections_status.items() if v == "failed"],
            },
        }

# 1.2. Enrich structured ad-level fields from adgroup_name and campaign_name
def enrich_ad_fields(enrich_df_input: pd.DataFrame, enrich_table_id: str) -> pd.DataFrame:   
    print(f"🚀 [ENRICH] Starting to enrich staging TikTok Ads ad insights for {len(enrich_df_input)}...")
    logging.info(f"🚀 [ENRICH] Starting to enrich staging TikTok Ads ad insights for {len(enrich_df_input)}...")       
    
    # 1.2.1. Start timing the staging TikTok Ads ad insights enrichment process
    enrich_time_start = time.time()
    enrich_sections_status = {}
    print(f"🔍 [ENRICH] Proceeding to enrich staging TikTok Ads ad insights for {len(enrich_df_input)} row(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [ENRICH] Proceeding to enrich staging TikTok Ads ad insights for {len(enrich_df_input)} row(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    
    # 1.2.2. Validate input for the staging TikTok Ads ad insights enrichment
    if enrich_df_input.empty:
        enrich_sections_status["1.2.2. Validate input for the staging TikTok Ads ad insights enrichment"] = "failed"
        print("⚠️ [ENRICH] Empty staging TikTok Ads ad insights provided then enrichment is suspended.")
        logging.warning("⚠️ [ENRICH] Empty staging TikTok Ads ad insights provided then enrichment is suspended.")
        raise ValueError("⚠️ [ENRICH] Empty staging TikTok Ads ad insights provided then enrichment is suspended.")
    else:
        enrich_sections_status["1.2.2. Validate input for the staging TikTok Ads ad insights enrichment"] = "succeed"
        print("✅ [ENRICH] Successfully validated input for staging TikTok Ads ad insights enrichment.")
        logging.info("✅ [ENRICH] Successfully validated input for staging TikTok Ads ad insights enrichment.")

    try:

    # 1.2.3. Enrich table-level field(s) for staging TikTok Ads ad insights
        try:
            print(f"🔍 [ENRICH] Enriching table-level field(s) for staging TikTok Ads ad insights with {len(enrich_df_input)} row(s)...")
            logging.info(f"🔍 [ENRICH] Enriching table-level field(s) for staging TikTok Ads ad insights with {len(enrich_df_input)} row(s)...")   
            enrich_df_table = enrich_df_input.copy()
            enrich_df_table["spend"] = pd.to_numeric(enrich_df_table["spend"], errors="coerce").fillna(0)
            enrich_table_name = enrich_table_id.split(".")[-1]
            match = re.search(
                r"^(?P<company>\w+)_table_(?P<platform>\w+)_(?P<department>\w+)_(?P<account>\w+)_ad_m\d{6}$",
                enrich_table_name
            )
            if match:
                enrich_df_table["nen_tang"] = match.group("platform")
                enrich_df_table["phong_ban"] = match.group("department")
                enrich_df_table["tai_khoan"] = match.group("account")
            print(f"✅ [ENRICH] Successfully enriched table-level field(s) for staging TikTok Ads ad insights with {len(enrich_df_table)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched table-level field(s) for staging TikTok Ads ad insights with {len(enrich_df_table)} row(s).")
            enrich_sections_status["1.2.3. Enrich table-level field(s) for staging TikTok Ads ad insights"] = "succeed"
        except Exception as e:
            enrich_sections_status["1.2.3. Enrich table-level field(s) for staging TikTok Ads ad insights"] = "failed"
            print(f"❌ [ENRICH] Failed to enrich table-level field(s) for staging TikTok Ads ad insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich table-level field(s) for staging TikTok Ads ad insights due to {e}.")
            raise RuntimeError(f"❌ [ENRICH] Failed to enrich table-level field(s) for staging TikTok Ads ad insights due to {e}.")   

    # 1.2.4. Enrich campaign-level field(s) for staging TikTok Ads ad insights
        try:
            print(f"🔍 [ENRICH] Enriching campaign-level field(s) for staging TikTok Ads ad insights with {len(enrich_df_table)} row(s)...")
            logging.info(f"🔍 [ENRICH] Enriching campaign-level field(s) for staging TikTok Ads ad insights with {len(enrich_df_table)} row(s)...")
            enrich_df_campaign = enrich_df_table.copy()
            camp_parts = enrich_df_campaign["campaign_name"].fillna("").str.split("_")
            enrich_df_campaign["hinh_thuc"] = camp_parts.str[0].fillna("unknown")
            enrich_df_campaign["khu_vuc"] = camp_parts.str[1].fillna("unknown")
            enrich_df_campaign["ma_ngan_sach_cap_1"] = camp_parts.str[2].fillna("unknown")
            enrich_df_campaign["ma_ngan_sach_cap_2"] = camp_parts.str[3].fillna("unknown")
            enrich_df_campaign["nganh_hang"] = camp_parts.str[4].fillna("unknown")
            enrich_df_campaign["nhan_su"]   = camp_parts.str[5].fillna("unknown")
            enrich_df_campaign["chuong_trinh"] = camp_parts.str[7].fillna("unknown")
            enrich_df_campaign["noi_dung"] = camp_parts.str[8].fillna("unknown")
            enrich_df_campaign["campaign_name_invalid"] = camp_parts.str.len() < 9
            print(f"✅ [ENRICH] Successfully enriched campaign-level field(s) for staging TikTok Ads ad insights with {len(enrich_df_campaign)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched campaign-level field(s) for staging TikTok Ads ad insights with {len(enrich_df_campaign)} row(s).")
            enrich_sections_status["1.2.4. Enrich campaign-level field(s) for staging TikTok Ads ad insights"] = "succeed"
        except Exception as e:
            enrich_sections_status["1.2.4. Enrich campaign-level field(s) for staging TikTok Ads ad insights"] = "failed"
            print(f"❌ [ENRICH] Failed to enrich campaign-level field(s) for staging TikTok Ads ad insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich campaign-level field(s) for staging TikTok Ads ad insights due to {e}.")
            raise RuntimeError(f"❌ [ENRICH] Failed to enrich campaign-level field(s) for staging TikTok Ads ad insights due to {e}.")

    # 1.2.5. Enrich adgroup-level field(s) for staging TikTok Ads ad insights
        try:
            print(f"🔍 [ENRICH] Enriching adgroup-level field(s) for staging TikTok Ads ad insights with {len(enrich_df_campaign)} row(s)...")
            logging.info(f"🔍 [ENRICH] Enriching adgroup-level field(s) for staging TikTok Ads ad insights with {len(enrich_df_campaign)} row(s)...")
            enrich_df_adgroup = enrich_df_campaign.copy() 
            adset_parts = enrich_df_adgroup["adgroup_name"].fillna("").str.split("_")
            enrich_df_adgroup["vi_tri"] = adset_parts.str[0].fillna("unknown")
            enrich_df_adgroup["doi_tuong"] = adset_parts.str[1].fillna("unknown")
            enrich_df_adgroup["dinh_dang"] = adset_parts.str[2].fillna("unknown")
            enrich_df_adgroup["adgroup_name_invalid"] = adset_parts.str.len() < 3
            print(f"✅ [ENRICH] Successfully enriched adgroup-level field(s) for staging TikTok Ads ad insights with {len(enrich_df_adgroup)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched adgroup-level field(s) for staging TikTok Ads ad insights with {len(enrich_df_adgroup)} row(s).")
            enrich_sections_status["1.2.5. Enrich adgroup-level field(s) for staging TikTok Ads ad insights"] = "succeed"
        except Exception as e:
            enrich_sections_status["1.2.5. Enrich adgroup-level field(s) for staging TikTok Ads ad insights"] = "failed"
            print(f"❌ [ENRICH] Failed to enrich adgroup-level field(s) for staging TikTok Ads ad insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich adgroup-level field(s) for staging TikTok Ads ad insights due to {e}.")
            raise RuntimeError(f"❌ [ENRICH] Failed to enrich adgroup-level field(s) for staging TikTok Ads ad insights due to {e}.")

    # 1.2.6. Enrich other ad-level field(s) for staging TikTok Ads ad insights
        try:
            print(f"🔍 [ENRICH] Enriching ad-level field(s) for staging TikTok Ads ad insights with {len(enrich_df_adgroup)} row(s)...")
            logging.info(f"🔍 [ENRICH] Enriching ad-level field(s) for staging TikTok Ads ad insights with {len(enrich_df_adgroup)} row(s)...")
            enrich_df_ad = enrich_df_adgroup.copy()
            enrich_df_ad["thang"] = pd.to_datetime(enrich_df_ad["date_start"]).dt.strftime("%Y-%m")
            print(f"✅ [ENRICH] Successfully enriched ad-level field(s) for staging TikTok Ads ad insights with {len(enrich_df_ad)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched ad-level field(s) for staging TikTok Ads ad insights with {len(enrich_df_ad)} row(s).")
            enrich_sections_status["1.2.6. Enrich other ad-level field(s) for staging TikTok Ads ad insights"] = "succeed"
        except Exception as e:
            enrich_sections_status["1.2.6. Enrich other ad-level field(s) for staging TikTok Ads ad insights"] = "failed"
            print(f"❌ [ENRICH] Failed to enrich ad-level field(s) for staging TikTok Ads ad insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich ad-level field(s) for staging TikTok Ads ad insights due to {e}.")
            raise RuntimeError(f"❌ [ENRICH] Failed to enrich ad-level field(s) for staging TikTok Ads ad insights due to {e}.")

    # 1.2.7. Summarize enrich result(s)
    finally:
        enrich_time_elapsed = round(time.time() - enrich_time_start, 2)
        enrich_df_final = enrich_df_ad.copy() if not enrich_df_ad.empty else pd.DataFrame()
        enrich_sections_failed = [k for k, v in enrich_sections_status.items() if v == "failed"]
        enrich_rows_output = len(enrich_df_final)
        if any(v == "failed" for v in enrich_sections_status.values()):
            print(f"❌ [ENRICH] Failed to complete staging TikTok Ads ad insights enrichment due to section(s) {', '.join(enrich_sections_failed)} in {enrich_time_elapsed}s.")
            logging.error(f"❌ [ENRICH] Failed to complete staging TikTok Ads ad insights enrichment due to section(s) {', '.join(enrich_sections_failed)} in {enrich_time_elapsed}s.")
            enrich_status_final = "enrich_failed_all"        
        else:
            print(f"🏆 [ENRICH] Successfully completed staging TikTok Ads ad insights enrichment for all {len(enrich_sections_status)} section(s) with {enrich_rows_output} row(s) output in {enrich_time_elapsed}s.")
            logging.info(f"🏆 [ENRICH] Successfully completed staging TikTok Ads ad insights enrichment for all {len(enrich_sections_status)} section(s) with {enrich_rows_output} row(s) output in {enrich_time_elapsed}s.")
            enrich_status_final = "enrich_success_all"
        return {
            "enrich_df_final": enrich_df_final,
            "enrich_status_final": enrich_status_final,
            "enrich_summary_final": {
                "enrich_time_elapsed": enrich_time_elapsed,
                "enrich_rows_output": enrich_rows_output,
                "enrich_sections_total": len(enrich_sections_status),
                "enrich_sections_failed": [k for k, v in enrich_sections_status.items() if v == "failed"],
            },
        }