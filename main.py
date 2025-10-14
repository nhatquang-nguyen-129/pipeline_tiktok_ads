"""
==================================================================
MAIN ENTRYPOINT FOR PLATFORM-AGNOSTIC DATA UPDATES
------------------------------------------------------------------
This script serves as the unified CLI **controller** for triggering  
ads data updates across multiple platforms (e.g., Facebook, Google),  
based on command-line arguments and environment variables.

It supports **incremental daily ingestion** for selected data layers  
(e.g., campaign, ad) and allows flexible control over date ranges.

✔️ Dynamic routing to the correct update module based on PLATFORM  
✔️ CLI flags to select data layers and date range mode  
✔️ Shared logging and error handling across update jobs  
✔️ Supports scheduled jobs or manual on-demand executions  

⚠️ This script does *not* contain data processing logic itself.  
It simply delegates update tasks to platform-specific modules  
(e.g., services.facebook.update, services.budget.update).
==================================================================
"""
# Add project root to sys.path to enable absolute imports
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add argparse to parse comman-line arguments
import argparse

# Add dynamic platform-specific modules
import importlib

# Import datetime to calculate time
from datetime import datetime, timedelta

# Add logging capability for tracking process execution and errors
import logging

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

# Get validated environment variables
if not all([COMPANY, PLATFORM, ACCOUNT, LAYER, MODE]):
    raise EnvironmentError("❌ [MAIN] Failed to trigger entry point for TikTok Ads update due to missing required environment variables.")
if PLATFORM != "tiktok":
    raise ValueError(f"❌ [MAIN] Failed to trigger entry point for TikTok Ads update due to unsupported platform '{PLATFORM}'.")

# 1. DYNAMIC IMPORT MODULE BASED ON PLATFORM

# 1.1. Main entry point path
try:
    update_module = importlib.import_module(f"src.update")
except ModuleNotFoundError:
    raise ImportError(f"❌ [MAIN] Failed to trigger entry point for TikTok Ads update due to module not found and expected path is 'services/{PLATFORM}/update.py'.")

# 1.2. Main entrypoint function
def main():
    
    # 1.2.1. Initialize runtime context
    today = datetime.today()

    # 1.2.2. Load required function(s) from TikTok Ads update module(s)
    try:
        update_campaign_insights = update_module.update_campaign_insights
        update_ad_insights = update_module.update_ad_insights
    except AttributeError:
        raise ImportError("⚠️ [MAIN] Failed to get TikTok Ads update module in src/update.py.")

    # 1.2.3. Validate layer for TikTok Ads main entry point
    layers = [layer.strip() for layer in LAYER.split(",") if layer.strip()]
    if len(layers) != 1:
        raise ValueError("⚠️ [MAIN] Failed to execute more than one layer for TikTok Ads main entry point.")
    layer = layers[0]

    # 1.2.4. Validate computation date range for TikTok Ads main entry point
    if MODE == "today":
        start_date = end_date = today.strftime("%Y-%m-%d")
    elif MODE == "last3days":
        start = today - timedelta(days=3)
        start_date = start.strftime("%Y-%m-%d")
        end_date = today.strftime("%Y-%m-%d")
    elif MODE == "last7days":
        start = today - timedelta(days=7)
        start_date = start.strftime("%Y-%m-%d")
        end_date = today.strftime("%Y-%m-%d")
    elif MODE == "thismonth":
        start = today.replace(day=1)
        start_date = start.strftime("%Y-%m-%d")
        end_date = today.strftime("%Y-%m-%d")
    elif MODE == "lastmonth":
        first_day_this_month = today.replace(day=1)
        last_day_last_month = first_day_this_month - timedelta(days=1)
        first_day_last_month = last_day_last_month.replace(day=1)
        start_date = first_day_last_month.strftime("%Y-%m-%d")
        end_date = last_day_last_month.strftime("%Y-%m-%d")
    else:
        raise ValueError(f"⚠️ [MAIN] Failed to excute mode {MODE} for TikTok Ads main entry point, please use one of 'today', 'last3days', 'last7days', 'thismonth' and 'lastmonth'.")

    # 1.2.5. Execute TikTok Ads main entry point based on layer
    if layer == "campaign":
        print(f"🚀 [MAIN] Trigger to update TikTok Ads campaign insights update for company {COMPANY}, deparment {DEPARTMENT}, account {ACCOUNT} and mode {MODE} from {start_date} to {end_date}...")
        logging.info(f"🚀 [MAIN] Trigger to update TikTok Ads campaign insights update for company {COMPANY}, deparment {DEPARTMENT}, account {ACCOUNT} and mode {MODE} from {start_date} to {end_date}...")
        update_campaign_insights(start_date=start_date, end_date=end_date)
    elif layer == "ad":
        print(f"🚀 [MAIN] Trigger to update TikTok Ads ad insights update for company {COMPANY}, deparment {DEPARTMENT}, account {ACCOUNT} and mode {MODE} from {start_date} to {end_date}...")
        logging.info(f"🚀 [MAIN] Trigger to update TikTok Ads ad insights update for company {COMPANY}, deparment {DEPARTMENT}, account {ACCOUNT} and mode {MODE} from {start_date} to {end_date}...")
        update_ad_insights(start_date=start_date, end_date=end_date)
    else:
        raise ValueError(f"⚠️ [MAIN] Failed to excute layer {MODE} for TikTok Ads main entry point and please use one of 'campaign' or 'ad'.")

# 1.3. Entrypoint guard
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"❌ Update failed: {e}")
        print(f"❌ Update failed: {e}")
        sys.exit(1)