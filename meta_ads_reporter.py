# FULLY PATCHED META ADS TRACKER SCRIPT
# Includes: formatting fixes, CTR f-string bug fixed, ad-level funnel removed

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
    DAILY_WORKSHEET = "Daily Sales Report"
    AD_LEVEL_WORKSHEET = "Ad Level Daily Sales"

    IST = timezone(timedelta(hours=5, minutes=30))

    REQUEST_TIMEOUT = 60
    MAX_RETRIES = 3
    RETRY_DELAY = 5
    COMMON_FIELDS = "date_start,date_stop,impressions,clicks,spend,actions,action_values,cpm,cpc,ctr"

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
        for ws_name in [Config.HOURLY_WORKSHEET, Config.DAILY_WORKSHEET, Config.AD_LEVEL_WORKSHEET]:
            try:
                self.spreadsheet.worksheet(ws_name)
            except Exception:
                self.spreadsheet.add_worksheet(title=ws_name, rows=20000, cols=50)

    def write_error(self, error_message: str):
        try:
            ws = self.spreadsheet.worksheet(Config.HOURLY_WORKSHEET)
            existing = ws.get_all_values()
            row = len(existing) + 1
            timestamp = datetime.now(Config.IST).strftime('%m/%d/%Y %H:%M:%S')
            ws.update(f"A{row}", [[f"❌ Error at {timestamp}: {error_message}"]])
        except Exception as e:
            logger.error(f"Failed to write error to sheet: {e}")

    def update_hourly(self, df: pd.DataFrame) -> bool:
        try:
            ws = self.spreadsheet.worksheet(Config.HOURLY_WORKSHEET)
            existing = ws.get_all_values()
            row = len(existing) + 1
            set_with_dataframe(ws, df, include_column_header=(row == 1), row=row, resize=False)
            logger.info("✅ Hourly sheet updated")
            return True
        except Exception as e:
            logger.error(f"Hourly update failed: {e}")
            self.write_error(f"Hourly update: {e}")
            return False

    def update_daily(self, df: pd.DataFrame) -> bool:
        try:
            ws = self.spreadsheet.worksheet(Config.DAILY_WORKSHEET)
            existing = ws.get_all_values()
            current_date = datetime.now(Config.IST).strftime('%m/%d/%Y')
            update_row = None
            for idx, row in enumerate(existing[1:], start=2):
                if row and row[0] == current_date:
                    update_row = idx
                    break
            if update_row:
                set_with_dataframe(ws, df, include_column_header=False, row=update_row, col=1)
                logger.info(f"✅ Updated daily summary for {current_date}")
            else:
                row = len(existing) + 1
                set_with_dataframe(ws, df, include_column_header=(row == 1), row=row)
                logger.info(f"✅ Added new daily summary for {current_date}")
            return True
        except Exception as e:
            logger.error(f"Daily update failed: {e}")
            self.write_error(f"Daily update: {e}")
            return False

    def update_ad_level(self, df: pd.DataFrame, date_label: str) -> bool:
        try:
            ws = self.spreadsheet.worksheet(Config.AD_LEVEL_WORKSHEET)
            existing = ws.get_all_values()
            if not existing:
                set_with_dataframe(ws, df, include_column_header=True, row=1, col=1)
                logger.info("Ad Level sheet initialized")
                return True
            headers = existing[0]
            rows = existing[1:]
            if rows:
                df_existing = pd.DataFrame(rows, columns=headers)
            else:
                df_existing = pd.DataFrame(columns=headers)
            date_col = next((c for c in df_existing.columns if c.lower() == 'date'), None)
            if date_col is None:
                df_existing.insert(0, 'Date', '')
                date_col = 'Date'
            df_historical = df_existing[df_existing[date_col] != date_label].copy()
            if 'Date' not in df.columns:
                df.insert(0, 'Date', date_label)
            else:
                df['Date'] = date_label
            all_cols = list(df.columns)
            for c in df_historical.columns:
                if c not in all_cols:
                    all_cols.append(c)
            df_historical = df_historical.reindex(columns=all_cols, fill_value=0)
            df_new_aligned = df.reindex(columns=all_cols, fill_value=0)
            df_combined = pd.concat([df_historical, df_new_aligned], ignore_index=True)
            ws.clear()
            set_with_dataframe(ws, df_combined, include_column_header=True, row=1, col=1)
            logger.info(f"Updated Ad Level sheet: {len(df_new_aligned)} rows added | Total: {len(df_combined)}")
            return True
        except Exception as e:
            logger.error(f"Failed to update Ad Level sheet: {e}")
            self.write_error(f"Ad Level update: {e}")
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
        """Fetch ad-level insights for today (no time_range support)."""
        fields = Config.COMMON_FIELDS + ',ad_id,ad_name'
        url = f"{self.base}/{account_id}/insights"
        params = {
            'access_token': self.access_token,
            'fields': fields,
            'date_preset': 'today',
            'level': 'ad'
        }
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
    def calculate_metrics(data_items: List[Dict]) -> Dict:
        spend = impressions = clicks = 0.0
        link_clicks = landing_page_views = add_to_cart = 0
        initiate_checkout = purchases = 0
        purchases_value = 0.0
        for item in data_items:
            spend += MetricsProcessor._safe_float(item.get('spend'))
            impressions += MetricsProcessor._safe_int(item.get('impressions'))
            clicks += MetricsProcessor._safe_int(item.get('clicks'))
            acts = MetricsProcessor.extract_actions(item)
            link_clicks += acts['link_clicks']
            landing_page_views += acts['landing_page_views']
            add_to_cart += acts['add_to_cart']
            initiate_checkout += acts['initiate_checkout']
            purchases += acts['purchases']
            purchases_value += MetricsProcessor.extract_purchase_value(item)
        roas = purchases_value / spend if spend > 0 else 0
        cpc = spend / clicks if clicks > 0 else 0
        cpm = (spend / impressions) * 1000 if impressions > 0 else 0
        ctr = (clicks / impressions) * 100 if impressions > 0 else 0
        lc_to_lpv = (landing_page_views / link_clicks) * 100 if link_clicks > 0 else 0
        lpv_to_atc = (add_to_cart / landing_page_views) * 100 if landing_page_views > 0 else 0
        atc_to_ci = (initiate_checkout / add_to_cart) * 100 if add_to_cart > 0 else 0
        ci_to_ordered = (purchases / initiate_checkout) * 100 if initiate_checkout > 0 else 0
        cvr = (purchases / link_clicks) * 100 if link_clicks > 0 else 0
        return {
            'Spend': spend,
            'Purchases Value': purchases_value,
            'Purchases': purchases,
            'Impressions': impressions,
            'Link Clicks': link_clicks,
            'Landing Page Views': landing_page_views,
            'Add to Cart': add_to_cart,
            'Initiate Checkout': initiate_checkout,
            'ROAS': roas,
            'CPC': cpc,
            'CTR': ctr,
            'CPM': cpm,
            'LC TO LPV': lc_to_lpv,
            'LPV TO ATC': lpv_to_atc,
            'ATC TO CI': atc_to_ci,
            'CI TO ORDERED': ci_to_ordered,
            'CVR': cvr
        }

    @staticmethod
    def create_hourly_report(metrics: Dict, timestamp: str) -> pd.DataFrame:
        return pd.DataFrame([{
            'Date': datetime.now(Config.IST).strftime('%m/%d/%Y'),
            'Timestamp': timestamp,
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

    @staticmethod
    def create_daily_report(metrics: Dict) -> pd.DataFrame:
        hourly_df = MetricsProcessor.create_hourly_report(metrics, datetime.now(Config.IST).strftime('%m/%d/%Y %H:%M:%S'))
        return hourly_df.drop(columns=['Timestamp'])

    @staticmethod
    def create_ad_level_report(ad_data: List[Dict], today_str: str) -> pd.DataFrame:
        # Build ad-level report using lowercase grouping keys and return formatted df_final
        if not ad_data:
            return pd.DataFrame(columns=[
                "Date", "Ad ID", "Ad Name", "Spend", "Revenue", "Orders",
                "Impressions", "Clicks", "Link Clicks", "Landing Page Views",
                "Add to Cart", "Initiate Checkout", "ROAS", "CPC", "CTR", "CPM"
            ])
        records = []
        for item in ad_data:
            acts = MetricsProcessor.extract_actions(item)
            rec = {
                'ad_id': item.get('ad_id',''),
                'ad_name': item.get('ad_name',''),
                'spend': MetricsProcessor._safe_float(item.get('spend')),
                'impressions': MetricsProcessor._safe_int(item.get('impressions')),
                'clicks': MetricsProcessor._safe_int(item.get('clicks')),
                'link_clicks': acts['link_clicks'],
                'landing_page_views': acts['landing_page_views'],
                'add_to_cart': acts['add_to_cart'],
                'initiate_checkout': acts['initiate_checkout'],
                'purchases': acts['purchases'],
                'purchases_value': MetricsProcessor.extract_purchase_value(item)
            }
            records.append(rec)
        df = pd.DataFrame(records)
        # Aggregate by ad (in case same ad appears in multiple accounts)
        df_agg = df.groupby(['ad_id', 'ad_name'], as_index=False).sum(numeric_only=True)
        # Calculate metrics
        df_agg['ROAS'] = np.where(df_agg['spend'] > 0, df_agg['purchases_value'] / df_agg['spend'], 0)
        df_agg['CPC'] = np.where(df_agg['clicks'] > 0, df_agg['spend'] / df_agg['clicks'], 0)
        df_agg['CPM'] = np.where(df_agg['impressions'] > 0, (df_agg['spend'] / df_agg['impressions']) * 1000, 0)
        df_agg['CTR'] = np.where(df_agg['impressions'] > 0, (df_agg['clicks'] / df_agg['impressions']), 0)
        df_agg = df_agg.sort_values('spend', ascending=False).reset_index(drop=True)

        df_final = pd.DataFrame({
            "Date": today_str,
            "Ad ID": df_agg["ad_id"],
            "Ad Name": df_agg["ad_name"],
            "Spend": df_agg["spend"].apply(lambda x: f"₹{round(x, 2)}"),
            "Revenue": df_agg["purchases_value"].apply(lambda x: f"₹{round(x, 2)}"),
            "Orders": df_agg["purchases"].astype(int),
            "Impressions": df_agg["impressions"].astype(int),
            "Clicks": df_agg["clicks"].astype(int),
            "Link Clicks": df_agg["link_clicks"].astype(int),
            "Landing Page Views": df_agg["landing_page_views"].astype(int),
            "Add to Cart": df_agg["add_to_cart"].astype(int),
            "Initiate Checkout": df_agg["initiate_checkout"].astype(int),
            "ROAS": df_agg["ROAS"].round(2),
            "CPC": df_agg["CPC"].apply(lambda x: f"₹{round(x, 2)}"),
            "CTR": df_agg["CTR"].apply(lambda x: f"{round(x * 100, 2)}%"), 
            "CPM": df_agg["CPM"].apply(lambda x: f"₹{round(x, 2)}")
        })
        return df_final

# ============================================
# RUNNER
# ============================================
class MetaAdsTracker:
    def __init__(self):
        self.sheets_manager = GoogleSheetsManager()
        self.api_client = MetaAPIClient(Config.ACCESS_TOKEN)
        self.processor = MetricsProcessor()

    def run(self) -> bool:
        logger.info("META ADS TRACKER STARTED")
        sheets_ok = self.sheets_manager.setup()
        if not self.api_client.access_token:
            token = input('Enter Meta ACCESS TOKEN (or set META_ACCESS_TOKEN env): ').strip()
            self.api_client.access_token = token
        all_ad_items = []
        for acct in Config.AD_ACCOUNT_IDS:
            items = self.api_client.fetch_ad_insights(acct)
            logger.info(f"Fetched {len(items)} records from {acct}")
            all_ad_items.extend(items)
        if not all_ad_items:
            logger.warning('No ad-level data returned.')
            return False
        today_str = datetime.now(Config.IST).strftime('%m/%d/%Y')
        ad_df = self.processor.create_ad_level_report(all_ad_items, today_str)
        if sheets_ok:
            try:
                self.sheets_manager.update_ad_level(ad_df, today_str)
            except Exception as e:
                logger.error(f"Failed to update ad-level sheet: {e}")
        # aggregate metrics for daily/hourly
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
            metrics['Link Clicks'] += acts.get('link_clicks',0)
            metrics['Landing Page Views'] += acts.get('landing_page_views',0)
            metrics['Add to Cart'] += acts.get('add_to_cart',0)
            metrics['Initiate Checkout'] += acts.get('initiate_checkout',0)
            metrics['Purchases'] += acts.get('purchases',0)
            metrics['Purchases Value'] += MetricsProcessor.extract_purchase_value(it)
        metrics['ROAS'] = metrics['Purchases Value'] / metrics['Spend'] if metrics['Spend']>0 else 0
        metrics['CPC'] = metrics['Spend'] / metrics['Link Clicks'] if metrics['Link Clicks']>0 else 0
        metrics['CPM'] = (metrics['Spend'] / metrics['Impressions'])*1000 if metrics['Impressions']>0 else 0
        metrics['CTR'] = (metrics['Link Clicks'] / metrics['Impressions'])*100 if metrics['Impressions']>0 else 0
        hourly_df = pd.DataFrame([{
            'Date': datetime.now(Config.IST).strftime('%m/%d/%Y'),
            'Timestamp': datetime.now(Config.IST).strftime('%m/%d/%Y %H:%M:%S'),
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
            'LC TO LPV': f"{round((metrics['Landing Page Views']/metrics['Link Clicks'])*100,2)}%" if metrics['Link Clicks']>0 else "0.00%",
            'LPV TO ATC': f"{round((metrics['Add to Cart']/metrics['Landing Page Views'])*100,2)}%" if metrics['Landing Page Views']>0 else "0.00%",
            'ATC TO CI': f"{round((metrics['Initiate Checkout']/metrics['Add to Cart'])*100,2)}%" if metrics['Add to Cart']>0 else "0.00%",
            'CI TO ORDERED': f"{round((metrics['Purchases']/metrics['Initiate Checkout'])*100,2)}%" if metrics['Initiate Checkout']>0 else "0.00%",
            'CVR': f"{round((metrics['Purchases']/metrics['Link Clicks'])*100,2)}%" if metrics['Link Clicks']>0 else "0.00%",
            'CPM': f"₹ {round(metrics['CPM'],2)}"
        }])
        if sheets_ok:
            try:
                self.sheets_manager.update_hourly(hourly_df)
            except Exception as e:
                logger.error(f"Failed to update hourly sheet: {e}")
        daily_df = hourly_df.drop(columns=['Timestamp'])
        if sheets_ok:
            try:
                self.sheets_manager.update_daily(daily_df)
            except Exception as e:
                logger.error(f"Failed to update daily sheet: {e}")
        # save ad-level CSV locally for Colab download
        try:
            ad_df.to_csv('ad_level.csv', index=False)
            if IN_COLAB:
                from google.colab import files
                files.download('ad_level.csv')
        except Exception:
            pass
        logger.info('Done')
        return True

# ============================================
# ENTRY POINT
# ============================================
if __name__ == '__main__':
    tracker = MetaAdsTracker()
    ok = tracker.run()
    if ok:
        print('Script completed successfully')
    else:
        print('Script finished with no data or errors')
