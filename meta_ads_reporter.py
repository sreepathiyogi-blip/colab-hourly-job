# FULLY PATCHED META ADS TRACKER SCRIPT WITH CATCHUP LOGIC
# Designed for GitHub Actions hourly cron job execution
# NEW: Automatically detects and processes missed hours
# FIXED: Robust duplicate prevention using hour-based comparison
# FIXED v2: Logs actual Meta API error messages for debugging 400 errors

import sys
import os
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import time
import gspread
from gspread_dataframe import set_with_dataframe
from typing import List, Dict, Optional
import logging

# ============================================
# ENVIRONMENT DETECTION
# ============================================
IN_COLAB = 'google.colab' in sys.modules

if IN_COLAB:
    from IPython.display import display, Javascript
    from threading import Thread
    from google.colab import auth
    from google.auth import default

    def keep_colab_alive():
        """Prevents Colab from disconnecting during long runs."""
        display(Javascript('''
            function KeepAlive(){document.body.dispatchEvent(new Event('mousemove'));} 
            setInterval(KeepAlive, 60000);
        '''))

    def heartbeat():
        while True:
            time.sleep(300)
            print(f"💚 Heartbeat: Session active at {datetime.now().strftime('%H:%M:%S')}")

    Thread(target=heartbeat, daemon=True).start()
    keep_colab_alive()
else:
    from google.oauth2 import service_account

# ============================================
# CONFIGURATION
# ============================================
class Config:
    ACCESS_TOKEN = os.environ.get('META_ACCESS_TOKEN', "")
    AD_ACCOUNT_IDS = [
        "act_1820431671907314",
        "act_24539675529051798"
    ]
    API_VERSION = "v21.0"
    SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID', "1Ka_DkNGCVi2h_plNN55-ZETW7M9MmFpTHocE7LZcYEM")

    HOURLY_WORKSHEET = "Hourly Data"

    IST = timezone(timedelta(hours=5, minutes=30))

    REQUEST_TIMEOUT = 60
    MAX_RETRIES = 3
    RETRY_DELAY = 5
    COMMON_FIELDS = "date_start,date_stop,impressions,clicks,spend,actions,action_values,cpm,cpc,ctr"

    # Catchup configuration
    MAX_CATCHUP_HOURS = 24
    ENABLE_AUTO_CATCHUP = True

# ============================================
# LOGGING
# ============================================
logging.basicConfig(level=logging.INFO, format='[%(asctime)s IST] %(levelname)s: %(message)s', datefmt='%m/%d/%Y %H:%M:%S')
logging.Formatter.converter = lambda *args: datetime.now(Config.IST).timetuple()
logger = logging.getLogger(__name__)

# ============================================
# GOOGLE SHEETS MANAGER
# ============================================
class GoogleSheetsManager:
    def __init__(self):
        self.client = None
        self.spreadsheet = None

    def setup(self) -> bool:
        try:
            if IN_COLAB:
                auth.authenticate_user()
                creds, _ = default()
                self.client = gspread.authorize(creds)
            else:
                creds_file = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', 'service-account.json')
                if not os.path.exists(creds_file):
                    logger.warning(f"Credentials file not found: {creds_file}. Sheets disabled.")
                    return False
                scopes = [
                    'https://www.googleapis.com/auth/spreadsheets',
                    'https://www.googleapis.com/auth/drive'
                ]
                creds = service_account.Credentials.from_service_account_file(creds_file, scopes=scopes)
                self.client = gspread.authorize(creds)

            if not Config.SPREADSHEET_ID:
                logger.warning("SPREADSHEET_ID not set. Sheets disabled.")
                return False

            self.spreadsheet = self.client.open_by_key(Config.SPREADSHEET_ID)
            self._ensure_worksheets_exist()
            logger.info("✅ Google Sheets setup completed")
            return True
        except Exception as e:
            logger.error(f"Google Sheets setup failed: {e}")
            return False

    def _ensure_worksheets_exist(self):
        try:
            self.spreadsheet.worksheet(Config.HOURLY_WORKSHEET)
        except Exception:
            self.spreadsheet.add_worksheet(title=Config.HOURLY_WORKSHEET, rows=20000, cols=50)

    def _parse_timestamp_to_hour(self, timestamp_str: str) -> Optional[str]:
        if not timestamp_str or not timestamp_str.strip():
            return None

        formats = [
            '%m/%d/%Y %H:%M:%S',
            '%Y-%m-%d %H:%M:%S',
            '%m/%d/%Y %H:%M',
            '%Y-%m-%d %H:%M',
            '%m/%d/%Y %H',
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(timestamp_str.strip(), fmt)
                return dt.strftime('%m/%d/%Y %H:00')
            except ValueError:
                continue

        try:
            parts = timestamp_str.strip().split()
            if len(parts) >= 2:
                date_part = parts[0]
                time_part = parts[1].split(':')[0]
                return f"{date_part} {time_part}:00"
        except:
            pass

        logger.warning(f"⚠️  Could not parse timestamp: {timestamp_str}")
        return None

    def get_last_hourly_timestamp(self) -> Optional[datetime]:
        try:
            ws = self.spreadsheet.worksheet(Config.HOURLY_WORKSHEET)
            all_values = ws.get_all_values()

            if len(all_values) <= 1:
                logger.info("📝 No previous hourly data found")
                return None

            last_row = all_values[-1]
            if len(last_row) < 2 or not last_row[1]:
                logger.warning("⚠️  Last row has no timestamp")
                return None

            timestamp_str = last_row[1]

            for fmt in ['%m/%d/%Y %H:%M:%S', '%Y-%m-%d %H:%M:%S', '%m/%d/%Y %H:%M']:
                try:
                    naive_dt = datetime.strptime(timestamp_str, fmt)
                    return naive_dt.replace(tzinfo=Config.IST)
                except ValueError:
                    continue

            logger.warning(f"⚠️  Could not parse timestamp: {timestamp_str}")
            return None

        except Exception as e:
            logger.error(f"❌ Error getting last timestamp: {e}")
            return None

    def write_error(self, error_message: str):
        try:
            ws = self.spreadsheet.worksheet(Config.HOURLY_WORKSHEET)
            existing = ws.get_all_values()
            row = len(existing) + 1
            timestamp = datetime.now(Config.IST).strftime('%m/%d/%Y %H:%M:%S')
            ws.update(values=[[f"❌ Error at {timestamp}: {error_message}"]], range_name=f"A{row}")
        except Exception as e:
            logger.error(f"Failed to write error to sheet: {e}")

    def update_hourly(self, df: pd.DataFrame) -> bool:
        try:
            ws = self.spreadsheet.worksheet(Config.HOURLY_WORKSHEET)
            existing = ws.get_all_values()

            if df.empty or 'Timestamp' not in df.columns:
                logger.error("❌ DataFrame is empty or missing Timestamp column")
                return False

            new_timestamp = df['Timestamp'].iloc[0]
            new_hour_key = self._parse_timestamp_to_hour(new_timestamp)

            if not new_hour_key:
                logger.error(f"❌ Could not parse new timestamp: {new_timestamp}")
                return False

            logger.info(f"🔍 Checking for duplicates in same hour: {new_hour_key}")

            duplicate_row = None
            for idx, row in enumerate(existing[1:], start=2):
                if len(row) > 1 and row[1]:
                    existing_hour_key = self._parse_timestamp_to_hour(row[1])
                    if existing_hour_key and existing_hour_key == new_hour_key:
                        duplicate_row = idx
                        logger.info(f"⚠️  Found data for same hour at row {idx}: {row[1]}")
                        break

            if duplicate_row:
                logger.info(f"🔄 Updating row {duplicate_row} with new data (timestamp: {new_timestamp})")
                num_cols = len(df.columns)
                ws.update(range_name=f"A{duplicate_row}:{chr(65 + num_cols - 1)}{duplicate_row}",
                         values=[['']*num_cols])
                time.sleep(1)
                set_with_dataframe(ws, df, include_column_header=False, row=duplicate_row, col=1, resize=False)
                logger.info("✅ Hourly sheet updated (replaced previous run)")
                return True
            else:
                row = len(existing) + 1
                logger.info(f"➕ Appending new row {row} (timestamp: {new_timestamp})")
                set_with_dataframe(ws, df, include_column_header=(row == 1), row=row, resize=False)
                logger.info("✅ Hourly sheet updated (new row)")
                return True

        except Exception as e:
            logger.error(f"❌ Hourly update failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
            self.write_error(f"Hourly update: {e}")
            return False

# ============================================
# CATCHUP MANAGER
# ============================================
class CatchupManager:
    @staticmethod
    def get_hours_to_process(last_timestamp: Optional[datetime]) -> List[datetime]:
        current_time = datetime.now(Config.IST)
        current_hour = current_time.replace(minute=0, second=0, microsecond=0)

        manual_catchup = os.environ.get('CATCHUP_HOURS', '').strip()
        if manual_catchup and manual_catchup.isdigit():
            hours_back = int(manual_catchup)
            if hours_back > 0:
                logger.info(f"🔧 Manual catchup mode: {hours_back} hours")
                hours = []
                for i in range(hours_back, 0, -1):
                    hours.append(current_hour - timedelta(hours=i))
                hours.append(current_hour)
                return hours

        if not Config.ENABLE_AUTO_CATCHUP:
            logger.info("✅ Auto-catchup disabled, processing current hour only")
            return [current_hour]

        if last_timestamp is None:
            logger.info("📝 First run detected, processing current hour only")
            return [current_hour]

        hours_diff = int((current_hour - last_timestamp).total_seconds() / 3600)

        if hours_diff <= 1:
            logger.info("✅ No missed hours detected")
            return [current_hour]

        if hours_diff > Config.MAX_CATCHUP_HOURS:
            logger.warning(f"⚠️  {hours_diff} hours missed, limiting to last {Config.MAX_CATCHUP_HOURS} hours")
            hours_diff = Config.MAX_CATCHUP_HOURS

        logger.info(f"🔄 Catchup mode: Processing {hours_diff} hour(s)")

        hours_to_process = []
        for i in range(hours_diff, 0, -1):
            hours_to_process.append(current_hour - timedelta(hours=i))
        hours_to_process.append(current_hour)

        return hours_to_process

# ============================================
# META API CLIENT  ← KEY FIX IS HERE
# ============================================
class MetaAPIClient:
    def __init__(self, access_token: str):
        self.access_token = access_token
        self.base = f"https://graph.facebook.com/{Config.API_VERSION}"

    def _validate_token(self) -> bool:
        """
        Quick token health check before running the main loop.
        Calls /me endpoint — if it fails, logs the exact error and returns False.
        """
        try:
            r = requests.get(
                f"{self.base}/me",
                params={'access_token': self.access_token, 'fields': 'id,name'},
                timeout=15
            )
            data = r.json()
            if 'error' in data:
                err = data['error']
                logger.error(f"🔴 TOKEN VALIDATION FAILED!")
                logger.error(f"   Error Code    : {err.get('code')}")
                logger.error(f"   Error Type    : {err.get('type')}")
                logger.error(f"   Error Message : {err.get('message')}")
                logger.error(f"   Subcode       : {err.get('error_subcode', 'N/A')}")
                logger.error(f"")
                logger.error(f"   ➡️  HOW TO FIX:")
                logger.error(f"      1. Go to https://developers.facebook.com/tools/debug/accesstoken/")
                logger.error(f"      2. Paste your token and check if it's expired or invalid")
                logger.error(f"      3. Generate a new System User token with 'ads_read' permission")
                logger.error(f"      4. Update your GitHub Secret: META_ACCESS_TOKEN")
                return False
            else:
                logger.info(f"✅ Token valid — authenticated as: {data.get('name', data.get('id', 'Unknown'))}")
                return True
        except Exception as e:
            logger.error(f"❌ Token validation request failed: {e}")
            return False

    def _paginate(self, url: str, params: Dict) -> List[Dict]:
        """
        Paginate through Meta API results.
        FIXED: Now logs Meta's actual error body before retrying so you can see
        exactly why requests are failing (expired token, bad permissions, etc.)
        """
        results = []
        attempt = 0
        while attempt < Config.MAX_RETRIES:
            try:
                r = requests.get(url, params=params, timeout=Config.REQUEST_TIMEOUT)

                # ✅ KEY FIX: Log Meta's actual error response BEFORE raising
                if r.status_code != 200:
                    try:
                        error_body = r.json()
                        meta_error = error_body.get('error', error_body)
                        logger.error(f"🔴 Meta API returned {r.status_code}:")
                        logger.error(f"   Code    : {meta_error.get('code', 'N/A')}")
                        logger.error(f"   Type    : {meta_error.get('type', 'N/A')}")
                        logger.error(f"   Message : {meta_error.get('message', 'N/A')}")
                        logger.error(f"   Subcode : {meta_error.get('error_subcode', 'N/A')}")
                        logger.error(f"   FB Trace: {meta_error.get('fbtrace_id', 'N/A')}")
                    except Exception:
                        logger.error(f"🔴 Meta API returned {r.status_code}: {r.text[:300]}")

                r.raise_for_status()
                data = r.json()
                results.extend(data.get('data', []))
                next_url = data.get('paging', {}).get('next')
                if not next_url:
                    break
                url = next_url
                params = {}

            except requests.exceptions.RequestException as e:
                attempt += 1
                logger.warning(f"Request failed (attempt {attempt}): {e}")
                if attempt < Config.MAX_RETRIES:
                    time.sleep(Config.RETRY_DELAY)
                else:
                    logger.error(f"Max retries reached: {e}")
                    break
        return results

    def fetch_ad_insights(self, account_id: str, target_hour: Optional[datetime] = None) -> List[Dict]:
        """
        Fetch account-level insights for today.
        Note: Meta API only supports 'today' preset, so target_hour is logged but not used in API call.
        """
        url = f"{self.base}/{account_id}/insights"
        params = {
            'access_token': self.access_token,
            'fields': Config.COMMON_FIELDS,
            'date_preset': 'today',
            'level': 'account'
        }

        if target_hour:
            logger.info(f"📊 Fetching data for {target_hour.strftime('%Y-%m-%d %H:00')} (Note: API returns 'today' data)")

        return self._paginate(url, params)

# ============================================
# DATA PROCESSOR
# ============================================
class MetricsProcessor:
    @staticmethod
    def _safe_float(v, default=0.0):
        try:
            return float(v or default)
        except:
            return default

    @staticmethod
    def _safe_int(v, default=0):
        try:
            return int(float(v or default))
        except:
            return default

    @staticmethod
    def extract_actions(item: Dict) -> Dict[str, int]:
        actions = {"link_clicks":0, "landing_page_views":0, "add_to_cart":0, "initiate_checkout":0, "purchases":0}
        for a in item.get('actions', []) or []:
            t = a.get('action_type')
            val = MetricsProcessor._safe_int(a.get('value'))
            if t == 'link_click': actions['link_clicks'] += val
            elif t == 'landing_page_view': actions['landing_page_views'] += val
            elif t == 'add_to_cart': actions['add_to_cart'] += val
            elif t == 'initiate_checkout': actions['initiate_checkout'] += val
            elif t == 'offsite_conversion.fb_pixel_purchase': actions['purchases'] += val
        return actions

    @staticmethod
    def extract_purchase_value(item: Dict) -> float:
        for av in item.get('action_values', []) or []:
            if av.get('action_type') == 'offsite_conversion.fb_pixel_purchase':
                return MetricsProcessor._safe_float(av.get('value'))
        return 0.0

    @staticmethod
    def create_hourly_report(metrics: Dict, target_hour: datetime) -> pd.DataFrame:
        actual_time = datetime.now(Config.IST)
        return pd.DataFrame([{
            'Date': actual_time.strftime('%m/%d/%Y'),
            'Timestamp': actual_time.strftime('%m/%d/%Y %H:%M:%S'),
            'Spend': f"₹{round(metrics['Spend'],2)}",
            'Purchases Value': f"₹{round(metrics['Purchases Value'],2)}",
            'Purchases': metrics['Purchases'],
            'Impressions': metrics['Impressions'],
            'Link Clicks': metrics['Link Clicks'],
            'Landing Page Views': metrics['Landing Page Views'],
            'Add to Cart': metrics['Add to Cart'],
            'Initiate Checkout': metrics['Initiate Checkout'],
            'ROAS': round(metrics['ROAS'],2),
            'CPC': f"₹{round(metrics['CPC'],2)}",
            'CTR': f"{round(metrics['CTR'],2)}%",
            'LC TO LPV': f"{round(metrics['LC TO LPV'],2)}%",
            'LPV TO ATC': f"{round(metrics['LPV TO ATC'],2)}%",
            'ATC TO CI': f"{round(metrics['ATC TO CI'],2)}%",
            'CI TO ORDERED': f"{round(metrics['CI TO ORDERED'],2)}%",
            'CVR': f"{round(metrics['CVR'],2)}%",
            'CPM': f"₹ {round(metrics['CPM'],2)}"
        }])

# ============================================
# RUNNER
# ============================================
class MetaAdsTracker:
    def __init__(self):
        self.sheets_manager = GoogleSheetsManager()
        self.api_client = MetaAPIClient(Config.ACCESS_TOKEN)
        self.processor = MetricsProcessor()
        self.catchup_manager = CatchupManager()

    def process_single_hour(self, target_hour: datetime, sheets_ok: bool) -> bool:
        logger.info(f"\n{'='*60}")
        logger.info(f"⏰ Processing: {target_hour.strftime('%Y-%m-%d %H:00 IST')}")
        logger.info(f"{'='*60}")

        all_ad_items = []
        for acct in Config.AD_ACCOUNT_IDS:
            items = self.api_client.fetch_ad_insights(acct, target_hour)
            logger.info(f"📊 Fetched {len(items)} records from {acct}")
            all_ad_items.extend(items)

        if not all_ad_items:
            logger.warning('⚠️  No ad-level data returned for this hour')
            return False

        metrics = {
            'Spend': 0.0, 'Purchases Value': 0.0, 'Purchases': 0,
            'Impressions': 0, 'Link Clicks': 0, 'Landing Page Views': 0,
            'Add to Cart': 0, 'Initiate Checkout': 0
        }

        for it in all_ad_items:
            metrics['Spend'] += MetricsProcessor._safe_float(it.get('spend'))
            metrics['Impressions'] += MetricsProcessor._safe_int(it.get('impressions'))
            acts = MetricsProcessor.extract_actions(it)
            metrics['Link Clicks'] += acts.get('link_clicks', 0)
            metrics['Landing Page Views'] += acts.get('landing_page_views', 0)
            metrics['Add to Cart'] += acts.get('add_to_cart', 0)
            metrics['Initiate Checkout'] += acts.get('initiate_checkout', 0)
            metrics['Purchases'] += acts.get('purchases', 0)
            metrics['Purchases Value'] += MetricsProcessor.extract_purchase_value(it)

        metrics['ROAS'] = metrics['Purchases Value'] / metrics['Spend'] if metrics['Spend'] > 0 else 0
        metrics['CPC'] = metrics['Spend'] / metrics['Link Clicks'] if metrics['Link Clicks'] > 0 else 0
        metrics['CPM'] = (metrics['Spend'] / metrics['Impressions']) * 1000 if metrics['Impressions'] > 0 else 0
        metrics['CTR'] = (metrics['Link Clicks'] / metrics['Impressions']) * 100 if metrics['Impressions'] > 0 else 0
        metrics['LC TO LPV'] = (metrics['Landing Page Views'] / metrics['Link Clicks']) * 100 if metrics['Link Clicks'] > 0 else 0
        metrics['LPV TO ATC'] = (metrics['Add to Cart'] / metrics['Landing Page Views']) * 100 if metrics['Landing Page Views'] > 0 else 0
        metrics['ATC TO CI'] = (metrics['Initiate Checkout'] / metrics['Add to Cart']) * 100 if metrics['Add to Cart'] > 0 else 0
        metrics['CI TO ORDERED'] = (metrics['Purchases'] / metrics['Initiate Checkout']) * 100 if metrics['Initiate Checkout'] > 0 else 0
        metrics['CVR'] = (metrics['Purchases'] / metrics['Link Clicks']) * 100 if metrics['Link Clicks'] > 0 else 0

        hourly_df = MetricsProcessor.create_hourly_report(metrics, target_hour)

        if sheets_ok:
            try:
                self.sheets_manager.update_hourly(hourly_df)
            except Exception as e:
                logger.error(f"❌ Failed to update hourly sheet: {e}")

        logger.info(f"✅ Completed processing for {target_hour.strftime('%Y-%m-%d %H:00 IST')}")
        return True

    def run(self) -> bool:
        logger.info("🚀 META ADS TRACKER STARTED (v2 — WITH ERROR DIAGNOSTICS)")
        logger.info(f"📅 Current time: {datetime.now(Config.IST).strftime('%Y-%m-%d %H:%M:%S IST')}")

        # ✅ NEW: Validate token FIRST before doing anything else
        if not self.api_client.access_token:
            token = input('Enter Meta ACCESS TOKEN (or set META_ACCESS_TOKEN env): ').strip()
            self.api_client.access_token = token

        logger.info("🔑 Validating Meta access token...")
        token_ok = self.api_client._validate_token()
        if not token_ok:
            logger.error("❌ Aborting — token is invalid. Fix your META_ACCESS_TOKEN secret and re-run.")
            return False

        sheets_ok = self.sheets_manager.setup()

        last_timestamp = None
        if sheets_ok:
            last_timestamp = self.sheets_manager.get_last_hourly_timestamp()
            if last_timestamp:
                logger.info(f"📊 Last recorded run: {last_timestamp.strftime('%Y-%m-%d %H:%M:%S IST')}")

        hours_to_process = self.catchup_manager.get_hours_to_process(last_timestamp)

        logger.info(f"\n📋 PROCESSING PLAN:")
        logger.info(f"   Total hours to process: {len(hours_to_process)}")
        for idx, hour in enumerate(hours_to_process, 1):
            logger.info(f"   {idx}. {hour.strftime('%Y-%m-%d %H:00 IST')}")
        logger.info("")

        success_count = 0
        for hour in hours_to_process:
            try:
                if self.process_single_hour(hour, sheets_ok):
                    success_count += 1
                if len(hours_to_process) > 1:
                    time.sleep(2)
            except Exception as e:
                logger.error(f"❌ Error processing {hour.strftime('%Y-%m-%d %H:00')}: {e}")
                continue

        logger.info(f"\n{'='*60}")
        logger.info(f"✅ TRACKER COMPLETED")
        logger.info(f"   Processed: {success_count}/{len(hours_to_process)} hours")
        logger.info(f"   Finished at: {datetime.now(Config.IST).strftime('%Y-%m-%d %H:%M:%S IST')}")
        logger.info(f"{'='*60}\n")

        return success_count > 0

# ============================================
# ENTRY POINT
# ============================================
if __name__ == '__main__':
    tracker = MetaAdsTracker()
    ok = tracker.run()
    if ok:
        print('✅ Script completed successfully')
    else:
        print('⚠️  Script finished with no data or errors')
