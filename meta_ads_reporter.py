# META ADS TRACKER - HOURLY SNAPSHOT
# Designed for GitHub Actions hourly cron job execution
# NOTE: Catchup is DISABLED - Meta API only returns today's running total,
#       not historical hourly snapshots. Catchup would write fake/duplicate data.

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

    # ⚠️  CATCHUP IS INTENTIONALLY DISABLED
    # Meta's API only supports date_preset='today' — it returns the current running
    # total, NOT what the numbers were at a specific past hour.
    # Enabling catchup would stamp today's total onto every missed hour label,
    # producing rows that look like real hourly data but are completely fabricated.
    # If hours are missed, accept the gap. Do not backfill.
    ENABLE_AUTO_CATCHUP = False

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
        """Parse timestamp and return normalized hour string (MM/DD/YYYY HH:00)"""
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
        """
        Update hourly sheet.
        - If a row for this hour already exists (e.g. script ran twice in same hour), replace it.
        - Otherwise append a new row.
        """
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

            logger.info(f"🔍 Checking for existing row in same hour: {new_hour_key}")

            duplicate_row = None
            for idx, row in enumerate(existing[1:], start=2):
                if len(row) > 1 and row[1]:
                    existing_hour_key = self._parse_timestamp_to_hour(row[1])
                    if existing_hour_key and existing_hour_key == new_hour_key:
                        duplicate_row = idx
                        logger.info(f"⚠️  Found existing row for this hour at row {idx}: {row[1]}")
                        break

            if duplicate_row:
                logger.info(f"🔄 Replacing row {duplicate_row} (timestamp: {new_timestamp})")
                num_cols = len(df.columns)
                ws.update(range_name=f"A{duplicate_row}:{chr(65 + num_cols - 1)}{duplicate_row}",
                         values=[['']*num_cols])
                time.sleep(1)
                set_with_dataframe(ws, df, include_column_header=False, row=duplicate_row, col=1, resize=False)
                logger.info("✅ Hourly sheet updated (replaced same-hour row)")
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
# META API CLIENT
# ============================================
class MetaAPIClient:
    def __init__(self, access_token: str):
        self.access_token = access_token
        self.base = f"https://graph.facebook.com/{Config.API_VERSION}"

    def _paginate(self, url: str, params: Dict) -> List[Dict]:
        results = []
        attempt = 0
        while attempt < Config.MAX_RETRIES:
            try:
                r = requests.get(url, params=params, timeout=Config.REQUEST_TIMEOUT)
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

    def fetch_ad_insights(self, account_id: str) -> List[Dict]:
        """Fetch account-level insights for today (running total)."""
        url = f"{self.base}/{account_id}/insights"
        params = {
            'access_token': self.access_token,
            'fields': Config.COMMON_FIELDS,
            'date_preset': 'today',
            'level': 'account'
        }
        logger.info(f"📊 Fetching today's data from {account_id}")
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
    def create_hourly_report(metrics: Dict) -> pd.DataFrame:
        """Create hourly report stamped with actual current time."""
        now = datetime.now(Config.IST)
        return pd.DataFrame([{
            'Date': now.strftime('%m/%d/%Y'),
            'Timestamp': now.strftime('%m/%d/%Y %H:%M:%S'),
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

    def run(self) -> bool:
        logger.info("🚀 META ADS TRACKER STARTED")
        logger.info(f"📅 Current time: {datetime.now(Config.IST).strftime('%Y-%m-%d %H:%M:%S IST')}")
        logger.info("ℹ️  Mode: Single hourly snapshot (catchup disabled)")
        logger.info("ℹ️  Reason: Meta API returns today's running total only — backfilling past hours")
        logger.info("            would write today's numbers onto historical hour labels (fake data).")

        sheets_ok = self.sheets_manager.setup()

        if not self.api_client.access_token:
            token = input('Enter Meta ACCESS TOKEN (or set META_ACCESS_TOKEN env): ').strip()
            self.api_client.access_token = token

        # Fetch from all accounts
        all_ad_items = []
        for acct in Config.AD_ACCOUNT_IDS:
            items = self.api_client.fetch_ad_insights(acct)
            logger.info(f"   → {len(items)} record(s) from {acct}")
            all_ad_items.extend(items)

        if not all_ad_items:
            logger.warning('⚠️  No data returned from Meta API')
            return False

        # Aggregate metrics
        metrics = {
            'Spend': 0.0,
            'Purchases Value': 0.0,
            'Purchases': 0,
            'Impressions': 0,
            'Link Clicks': 0,
            'Landing Page Views': 0,
            'Add to Cart': 0,
            'Initiate Checkout': 0
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

        # Derived metrics
        metrics['ROAS'] = metrics['Purchases Value'] / metrics['Spend'] if metrics['Spend'] > 0 else 0
        metrics['CPC'] = metrics['Spend'] / metrics['Link Clicks'] if metrics['Link Clicks'] > 0 else 0
        metrics['CPM'] = (metrics['Spend'] / metrics['Impressions']) * 1000 if metrics['Impressions'] > 0 else 0
        metrics['CTR'] = (metrics['Link Clicks'] / metrics['Impressions']) * 100 if metrics['Impressions'] > 0 else 0
        metrics['LC TO LPV'] = (metrics['Landing Page Views'] / metrics['Link Clicks']) * 100 if metrics['Link Clicks'] > 0 else 0
        metrics['LPV TO ATC'] = (metrics['Add to Cart'] / metrics['Landing Page Views']) * 100 if metrics['Landing Page Views'] > 0 else 0
        metrics['ATC TO CI'] = (metrics['Initiate Checkout'] / metrics['Add to Cart']) * 100 if metrics['Add to Cart'] > 0 else 0
        metrics['CI TO ORDERED'] = (metrics['Purchases'] / metrics['Initiate Checkout']) * 100 if metrics['Initiate Checkout'] > 0 else 0
        metrics['CVR'] = (metrics['Purchases'] / metrics['Link Clicks']) * 100 if metrics['Link Clicks'] > 0 else 0

        # Log summary
        logger.info(f"\n📊 TODAY'S SNAPSHOT:")
        logger.info(f"   Spend:           ₹{round(metrics['Spend'], 2)}")
        logger.info(f"   Purchases Value: ₹{round(metrics['Purchases Value'], 2)}")
        logger.info(f"   Purchases:       {metrics['Purchases']}")
        logger.info(f"   ROAS:            {round(metrics['ROAS'], 2)}")
        logger.info(f"   Impressions:     {metrics['Impressions']}")

        # Write to sheet
        hourly_df = MetricsProcessor.create_hourly_report(metrics)

        if sheets_ok:
            self.sheets_manager.update_hourly(hourly_df)

        logger.info(f"\n✅ TRACKER COMPLETED at {datetime.now(Config.IST).strftime('%Y-%m-%d %H:%M:%S IST')}\n")
        return True

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
