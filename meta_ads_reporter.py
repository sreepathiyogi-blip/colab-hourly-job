import sys
import os
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import time
import gspread
from gspread_dataframe import set_with_dataframe
from typing import List, Dict, Optional, Tuple
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
            function KeepAlive(){
                var timestamp = new Date().toLocaleTimeString();
                console.log("🔄 Keep-alive ping at: " + timestamp);
                try {
                    var connectButton = document.querySelector("colab-connect-button");
                    if (connectButton) {
                        connectButton.shadowRoot.querySelector("#connect").click();
                    }
                } catch(e) {}
                document.body.dispatchEvent(new Event('mousemove'));
                document.body.dispatchEvent(new Event('keypress'));
            }
            setInterval(KeepAlive, 60000);
            console.log("✅ Keep-alive system activated!");
        '''))
    
    def heartbeat():
        """Periodic heartbeat to show session is active."""
        while True:
            time.sleep(300)
            print(f"💚 Heartbeat: Session active at {datetime.now().strftime('%H:%M:%S')}")
    
    Thread(target=heartbeat, daemon=True).start()
    print("✅ Keep-alive enabled for Google Colab")
    keep_colab_alive()
else:
    from google.oauth2 import service_account

# ============================================
# CONFIGURATION
# ============================================
class Config:
    """Centralized configuration management."""
    ACCESS_TOKEN = os.environ.get('META_ACCESS_TOKEN', 
                                   "EAAHeR1E5PKUBP19I9GXYVw8kWusULp7l7ZBbyHf1qZCzBdPZA7enpZAbLZBQGajtASZCJWbesZCthHzV0K8xd2KfDKYZBRZAGjbMDtOZCmlX3jlRpMQUlAp8OedkqBD12rr35FnL4InZCrqhfV3fPTVACozb5YWZC7KmXZBgRabEbE1rwuKnZBJwsHYn0oOPtyZBm504dFJgE1ZA3KTw")
    
    AD_ACCOUNT_IDS = [
        "act_1820431671907314",
        "act_24539675529051798"
    ]
    
    API_VERSION = "v21.0"
    SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID', "1Ka_DkNGCVi2h_plNN55-ZETW7M9MmFpTHocE7LZcYEM")
    
    # Worksheet names
    HOURLY_WORKSHEET = "Hourly Data"
    DAILY_WORKSHEET = "Daily Sales Report"
    AD_LEVEL_WORKSHEET = "Ad Level Daily Sales"
    
    # Timezone
    IST = timezone(timedelta(hours=5, minutes=30))
    
    # API settings
    REQUEST_TIMEOUT = 60
    MAX_RETRIES = 3
    RETRY_DELAY = 5

# ============================================
# LOGGING SETUP
# ============================================
def setup_logging():
    """Configure logging with IST timestamps."""
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s IST] %(levelname)s: %(message)s',
        datefmt='%m/%d/%Y %H:%M:%S'
    )
    # Set timezone for logging
    logging.Formatter.converter = lambda *args: datetime.now(Config.IST).timetuple()

setup_logging()
logger = logging.getLogger(__name__)

# ============================================
# GOOGLE SHEETS MANAGER
# ============================================
class GoogleSheetsManager:
    """Manages Google Sheets authentication and operations."""
    
    def __init__(self):
        self.client = None
        self.spreadsheet = None
    
    def setup(self) -> bool:
        """Initialize Google Sheets client and ensure worksheets exist."""
        try:
            if IN_COLAB:
                auth.authenticate_user()
                creds, _ = default()
                self.client = gspread.authorize(creds)
            else:
                creds_file = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', 'service-account.json')
                if not os.path.exists(creds_file):
                    logger.error(f"Credentials file not found: {creds_file}")
                    return False
                
                scopes = [
                    'https://www.googleapis.com/auth/spreadsheets',
                    'https://www.googleapis.com/auth/drive'
                ]
                creds = service_account.Credentials.from_service_account_file(creds_file, scopes=scopes)
                self.client = gspread.authorize(creds)
            
            self.spreadsheet = self.client.open_by_key(Config.SPREADSHEET_ID)
            self._ensure_worksheets_exist()
            
            logger.info("✅ Google Sheets setup completed")
            return True
            
        except Exception as e:
            logger.error(f"❌ Google Sheets setup failed: {e}")
            return False
    
    def _ensure_worksheets_exist(self):
        """Create worksheets if they don't exist."""
        for ws_name in [Config.HOURLY_WORKSHEET, Config.DAILY_WORKSHEET, Config.AD_LEVEL_WORKSHEET]:
            try:
                self.spreadsheet.worksheet(ws_name)
                logger.info(f"✓ Worksheet '{ws_name}' found")
            except gspread.WorksheetNotFound:
                self.spreadsheet.add_worksheet(title=ws_name, rows=20000, cols=50)
                logger.info(f"✓ Worksheet '{ws_name}' created")
    
    def write_error(self, error_message: str):
        """Log error to the hourly worksheet."""
        try:
            ws = self.spreadsheet.worksheet(Config.HOURLY_WORKSHEET)
            existing = ws.get_all_values()
            row = len(existing) + 1
            timestamp = datetime.now(Config.IST).strftime('%m/%d/%Y %H:%M:%S')
            ws.update(f"A{row}", [[f"❌ Error at {timestamp}: {error_message}"]])
        except Exception as e:
            logger.error(f"Failed to write error to sheet: {e}")
    
    def update_hourly(self, df: pd.DataFrame) -> bool:
        """Append hourly data to the Hourly Data worksheet."""
        try:
            ws = self.spreadsheet.worksheet(Config.HOURLY_WORKSHEET)
            existing = ws.get_all_values()
            row = len(existing) + 1
            
            set_with_dataframe(
                ws, df,
                include_column_header=(row == 1),
                row=row,
                resize=False
            )
            
            logger.info("✅ Hourly sheet updated")
            return True
            
        except Exception as e:
            logger.error(f"❌ Hourly sheet update failed: {e}")
            self.write_error(f"Hourly update: {e}")
            return False
    
    def update_daily(self, df: pd.DataFrame) -> bool:
        """Upsert daily summary (update if exists, insert if new)."""
        try:
            ws = self.spreadsheet.worksheet(Config.DAILY_WORKSHEET)
            existing = ws.get_all_values()
            current_date = datetime.now(Config.IST).strftime('%m/%d/%Y')
            
            # Find existing row for today
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
            logger.error(f"❌ Daily summary update failed: {e}")
            self.write_error(f"Daily update: {e}")
            return False
    
    def update_ad_level(self, df: pd.DataFrame, today_str: str) -> bool:
        """Update ad-level data, preserving historical records."""
        try:
            ws = self.spreadsheet.worksheet(Config.AD_LEVEL_WORKSHEET)
            existing = ws.get_all_values()
            
            if not existing:
                # Empty sheet - just write new data
                set_with_dataframe(ws, df, include_column_header=True, row=1, col=1)
                logger.info("✅ Ad Level sheet initialized")
                return True
            
            headers = existing[0]
            rows = existing[1:]
            
            if rows:
                df_existing = pd.DataFrame(rows, columns=headers)
            else:
                df_existing = pd.DataFrame(columns=headers)
            
            # Find date column
            date_col = next((col for col in df_existing.columns 
                           if col.lower() == "date"), None)
            
            if date_col is None:
                df_existing.insert(0, "Date", "")
                date_col = "Date"
            
            # Keep historical data (all rows except today)
            df_historical = df_existing[df_existing[date_col] != today_str].copy()
            
            logger.info(f"📌 Preserving {len(df_historical)} historical rows")
            logger.info(f"🔄 Replacing data for {today_str}")
            
            # Ensure date column in new data
            if "Date" not in df.columns:
                df.insert(0, "Date", today_str)
            else:
                df["Date"] = today_str
            
            # Align columns
            all_cols = list(df.columns)
            for col in df_historical.columns:
                if col not in all_cols:
                    all_cols.append(col)
            
            df_historical = df_historical.reindex(columns=all_cols, fill_value=0)
            df_new_aligned = df.reindex(columns=all_cols, fill_value=0)
            
            # Combine and write
            df_combined = pd.concat([df_historical, df_new_aligned], ignore_index=True)
            
            ws.clear()
            set_with_dataframe(ws, df_combined, include_column_header=True, row=1, col=1)
            
            logger.info(f"✅ Ad Level updated: {len(df_new_aligned)} new rows | Total: {len(df_combined)} rows")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ad Level update failed: {e}")
            self.write_error(f"Ad Level update: {e}")
            return False

# ============================================
# META API CLIENT
# ============================================
class MetaAPIClient:
    """Handles all Meta API interactions."""
    
    BASE_URL = f"https://graph.facebook.com/{Config.API_VERSION}"
    COMMON_FIELDS = "date_start,date_stop,impressions,clicks,spend,actions,action_values,cpm,cpc,ctr"
    
    def __init__(self, access_token: str):
        self.access_token = access_token
    
    def validate_token(self) -> bool:
        """Verify access token is valid."""
        url = f"{self.BASE_URL}/me"
        try:
            response = requests.get(
                url,
                params={"access_token": self.access_token},
                timeout=15
            )
            data = response.json()
            
            if "error" in data:
                logger.error(f"Token validation failed: {data['error']['message']}")
                return False
            
            logger.info(f"✅ Token validated for user: {data.get('name', 'Unknown')}")
            return True
            
        except Exception as e:
            logger.error(f"Token validation error: {e}")
            return False
    
    def _paginate(self, url: str, params: Dict) -> List[Dict]:
        """Handle API pagination."""
        results = []
        attempt = 0
        
        while attempt < Config.MAX_RETRIES:
            try:
                response = requests.get(url, params=params, timeout=Config.REQUEST_TIMEOUT)
                response.raise_for_status()
                data = response.json()
                
                results.extend(data.get("data", []))
                
                # Check for next page
                next_url = data.get("paging", {}).get("next")
                if not next_url:
                    break
                
                url = next_url
                params = {}  # Next URL contains all params
                attempt = 0  # Reset retry counter on success
                
            except requests.exceptions.RequestException as e:
                attempt += 1
                logger.warning(f"Request failed (attempt {attempt}/{Config.MAX_RETRIES}): {e}")
                
                if attempt < Config.MAX_RETRIES:
                    time.sleep(Config.RETRY_DELAY)
                else:
                    logger.error(f"Max retries reached for {url}")
                    break
        
        return results
    
    def fetch_account_insights(self, account_id: str) -> List[Dict]:
        """Fetch account-level insights for today."""
        url = f"{self.BASE_URL}/{account_id}/insights"
        params = {
            "access_token": self.access_token,
            "fields": self.COMMON_FIELDS,
            "date_preset": "today",
            "level": "account"
        }
        
        logger.info(f"📊 Fetching account insights: {account_id}")
        return self._paginate(url, params)
    
    def fetch_ad_insights(self, account_id: str) -> List[Dict]:
        """Fetch ad-level insights for today."""
        fields = self.COMMON_FIELDS + ",ad_id,ad_name"
        url = f"{self.BASE_URL}/{account_id}/insights"
        params = {
            "access_token": self.access_token,
            "fields": fields,
            "date_preset": "today",
            "level": "ad"
        }
        
        logger.info(f"📊 Fetching ad-level insights: {account_id}")
        return self._paginate(url, params)

# ============================================
# DATA PROCESSOR
# ============================================
class MetricsProcessor:
    """Processes raw API data into formatted reports."""
    
    @staticmethod
    def _safe_float(value, default=0.0) -> float:
        """Safely convert to float."""
        try:
            return float(value or default)
        except (ValueError, TypeError):
            return default
    
    @staticmethod
    def _safe_int(value, default=0) -> int:
        """Safely convert to int."""
        try:
            return int(float(value or default))
        except (ValueError, TypeError):
            return default
    
    @staticmethod
    def extract_actions(item: Dict) -> Dict[str, int]:
        """Extract action metrics from API response."""
        actions = {
            "link_clicks": 0,
            "landing_page_views": 0,
            "add_to_cart": 0,
            "initiate_checkout": 0,
            "purchases": 0
        }
        
        for action in item.get("actions", []):
            action_type = action.get("action_type")
            value = MetricsProcessor._safe_int(action.get("value"))
            
            if action_type == "link_click":
                actions["link_clicks"] += value
            elif action_type == "landing_page_view":
                actions["landing_page_views"] += value
            elif action_type == "add_to_cart":
                actions["add_to_cart"] += value
            elif action_type == "initiate_checkout":
                actions["initiate_checkout"] += value
            elif action_type == "offsite_conversion.fb_pixel_purchase":
                actions["purchases"] += value
        
        return actions
    
    @staticmethod
    def extract_purchase_value(item: Dict) -> float:
        """Extract purchase value from action_values."""
        for action_value in item.get("action_values", []):
            if action_value.get("action_type") == "offsite_conversion.fb_pixel_purchase":
                return MetricsProcessor._safe_float(action_value.get("value"))
        return 0.0
    
    @staticmethod
    def calculate_metrics(data_items: List[Dict]) -> Dict:
        """Calculate aggregated metrics from API data."""
        spend = impressions = clicks = 0.0
        link_clicks = landing_page_views = add_to_cart = 0
        initiate_checkout = purchases = 0
        purchases_value = 0.0
        
        for item in data_items:
            spend += MetricsProcessor._safe_float(item.get("spend"))
            impressions += MetricsProcessor._safe_int(item.get("impressions"))
            clicks += MetricsProcessor._safe_int(item.get("clicks"))
            
            actions = MetricsProcessor.extract_actions(item)
            link_clicks += actions["link_clicks"]
            landing_page_views += actions["landing_page_views"]
            add_to_cart += actions["add_to_cart"]
            initiate_checkout += actions["initiate_checkout"]
            purchases += actions["purchases"]
            
            purchases_value += MetricsProcessor.extract_purchase_value(item)
        
        # Calculate derived metrics
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
            "Spend": spend,
            "Purchases Value": purchases_value,
            "Purchases": purchases,
            "Impressions": impressions,
            "Link Clicks": link_clicks,
            "Landing Page Views": landing_page_views,
            "Add to Cart": add_to_cart,
            "Initiate Checkout": initiate_checkout,
            "ROAS": roas,
            "CPC": cpc,
            "CTR": ctr,
            "CPM": cpm,
            "LC TO LPV": lc_to_lpv,
            "LPV TO ATC": lpv_to_atc,
            "ATC TO CI": atc_to_ci,
            "CI TO ORDERED": ci_to_ordered,
            "CVR": cvr
        }
    
    @staticmethod
    def create_hourly_report(metrics: Dict, timestamp: str) -> pd.DataFrame:
        """Create hourly report dataframe with NUMERIC values (Google Sheets handles formatting)."""
        return pd.DataFrame([{
            "Date": datetime.now(Config.IST).strftime('%m/%d/%Y'),
            "Timestamp": timestamp,
            "Spend": round(metrics['Spend'], 2),
            "Purchases Value": round(metrics['Purchases Value'], 2),
            "Purchases": metrics["Purchases"],
            "Impressions": metrics["Impressions"],
            "Link Clicks": metrics["Link Clicks"],
            "Landing Page Views": metrics["Landing Page Views"],
            "Add to Cart": metrics["Add to Cart"],
            "Initiate Checkout": metrics["Initiate Checkout"],
            "ROAS": round(metrics["ROAS"], 2),
            "CPC": round(metrics['CPC'], 2),
            "CTR": round(metrics['CTR'], 2),
            "LC TO LPV": round(metrics['LC TO LPV'], 2),
            "LPV TO ATC": round(metrics['LPV TO ATC'], 2),
            "ATC TO CI": round(metrics['ATC TO CI'], 2),
            "CI TO ORDERED": round(metrics['CI TO ORDERED'], 2),
            "CVR": round(metrics['CVR'], 2),
            "CPM": round(metrics['CPM'], 2)
        }])
    
    @staticmethod
    def create_daily_report(metrics: Dict) -> pd.DataFrame:
        """Create daily report (same as hourly without timestamp)."""
        hourly_df = MetricsProcessor.create_hourly_report(
            metrics,
            datetime.now(Config.IST).strftime('%m/%d/%Y %H:%M:%S')
        )
        return hourly_df.drop(columns=["Timestamp"])
    
    @staticmethod
    def create_ad_level_report(ad_data: List[Dict], today_str: str) -> pd.DataFrame:
        """Create ad-level performance report."""
        if not ad_data:
            return pd.DataFrame(columns=[
                "Date", "Ad ID", "Ad Name", "Spend", "Revenue", "Orders",
                "Impressions", "Clicks", "Link Clicks", "Landing Page Views",
                "Add to Cart", "Initiate Checkout", "ROAS", "CPC", "CTR", "CPM",
                "LC to LPV", "LPV to ATC", "ATC to CI", "CI to Order", "CVR"
            ])
        
        records = []
        for item in ad_data:
            actions = MetricsProcessor.extract_actions(item)
            
            record = {
                "ad_id": item.get("ad_id", ""),
                "ad_name": item.get("ad_name", ""),
                "spend": MetricsProcessor._safe_float(item.get("spend")),
                "impressions": MetricsProcessor._safe_int(item.get("impressions")),
                "clicks": MetricsProcessor._safe_int(item.get("clicks")),
                "link_clicks": actions["link_clicks"],
                "landing_page_views": actions["landing_page_views"],
                "add_to_cart": actions["add_to_cart"],
                "initiate_checkout": actions["initiate_checkout"],
                "purchases": actions["purchases"],
                "purchases_value": MetricsProcessor.extract_purchase_value(item)
            }
            records.append(record)
        
        df = pd.DataFrame(records)
        
        # Aggregate by ad (in case same ad appears in multiple accounts)
        df_agg = df.groupby(["ad_id", "ad_name"], as_index=False).sum(numeric_only=True)
        
        # Calculate metrics
        df_agg["ROAS"] = np.where(df_agg["spend"] > 0, df_agg["purchases_value"] / df_agg["spend"], 0)
        df_agg["CPC"] = np.where(df_agg["clicks"] > 0, df_agg["spend"] / df_agg["clicks"], 0)
        df_agg["CPM"] = np.where(df_agg["impressions"] > 0, (df_agg["spend"] / df_agg["impressions"]) * 1000, 0)
        df_agg["CTR"] = np.where(df_agg["impressions"] > 0, (df_agg["clicks"] / df_agg["impressions"]), 0)
        
        # Conversion rates as decimals (0.05 = 5% when formatted as percentage in Sheets)
        df_agg["LC_to_LPV"] = np.where(df_agg["link_clicks"] > 0, (df_agg["landing_page_views"] / df_agg["link_clicks"]), 0)
        df_agg["LPV_to_ATC"] = np.where(df_agg["landing_page_views"] > 0, (df_agg["add_to_cart"] / df_agg["landing_page_views"]), 0)
        df_agg["ATC_to_CI"] = np.where(df_agg["add_to_cart"] > 0, (df_agg["initiate_checkout"] / df_agg["add_to_cart"]), 0)
        df_agg["CI_to_Order"] = np.where(df_agg["initiate_checkout"] > 0, (df_agg["purchases"] / df_agg["initiate_checkout"]), 0)
        df_agg["CVR"] = np.where(df_agg["link_clicks"] > 0, (df_agg["purchases"] / df_agg["link_clicks"]), 0)
        
        # Sort by spend
        df_agg = df_agg.sort_values("spend", ascending=False).reset_index(drop=True)
        
        # Format output with CLEAN column names matching the calculated columns
        return pd.DataFrame({
            "Date": today_str,
            "Ad ID": df_agg["ad_id"],
            "Ad Name": df_agg["ad_name"],
            "Spend": df_agg["spend"].round(2),
            "Revenue": df_agg["purchases_value"].round(2),
            "Orders": df_agg["purchases"].astype(int),
            "Impressions": df_agg["impressions"].astype(int),
            "Clicks": df_agg["clicks"].astype(int),
            "Link Clicks": df_agg["link_clicks"].astype(int),
            "Landing Page Views": df_agg["landing_page_views"].astype(int),
            "Add to Cart": df_agg["add_to_cart"].astype(int),
            "Initiate Checkout": df_agg["initiate_checkout"].astype(int),
            "ROAS": df_agg["ROAS"].round(2),
            "CPC": df_agg["CPC"].round(2),
            "CTR": df_agg["CTR"].round(2),
            "CPM": df_agg["CPM"].round(2),
            "LC to LPV": df_agg["LC_to_LPV"].round(2),
            "LPV to ATC": df_agg["LPV_to_ATC"].round(2),
            "ATC to CI": df_agg["ATC_to_CI"].round(2),
            "CI to Order": df_agg["CI_to_Order"].round(2),
            "CVR": df_agg["CVR"].round(2)
        })

# ============================================
# MAIN RUNNER
# ============================================
class MetaAdsTracker:
    """Main orchestrator for the Meta Ads tracking system."""
    
    def __init__(self):
        self.sheets_manager = GoogleSheetsManager()
        self.api_client = MetaAPIClient(Config.ACCESS_TOKEN)
        self.processor = MetricsProcessor()
    
    def run(self) -> bool:
        """Execute the complete tracking workflow."""
        logger.info("🎯 META ADS DAILY TRACKER STARTED")
        
        # Setup
        if not self.sheets_manager.setup():
            logger.error("❌ Google Sheets setup failed")
            return False
        
        if not self.api_client.validate_token():
            logger.error("❌ Meta API token validation failed")
            self.sheets_manager.write_error("Invalid Meta API token")
            return False
        
        # Fetch data from all accounts
        all_account_data = []
        all_ad_data = []
        
        for account_id in Config.AD_ACCOUNT_IDS:
            account_insights = self.api_client.fetch_account_insights(account_id)
            ad_insights = self.api_client.fetch_ad_insights(account_id)
            
            all_account_data.extend(account_insights)
            all_ad_data.extend(ad_insights)
        
        if not all_account_data:
            logger.error("❌ No account data retrieved")
            self.sheets_manager.write_error("No account data returned from Meta API")
            return False
        
        logger.info(f"✅ Retrieved {len(all_account_data)} account records, {len(all_ad_data)} ad records")
        
        # Process and update reports
        timestamp = datetime.now(Config.IST).strftime('%m/%d/%Y %H:%M:%S')
        today_str = datetime.now(Config.IST).strftime('%m/%d/%Y')
        
        # 1. Hourly Report (append)
        metrics = self.processor.calculate_metrics(all_account_data)
        hourly_df = self.processor.create_hourly_report(metrics, timestamp)
        self.sheets_manager.update_hourly(hourly_df)
        
        # 2. Daily Summary (upsert)
        daily_df = self.processor.create_daily_report(metrics)
        self.sheets_manager.update_daily(daily_df)
        
        # 3. Ad Level Report (preserve history, update today)
        ad_level_df = self.processor.create_ad_level_report(all_ad_data, today_str)
        self.sheets_manager.update_ad_level(ad_level_df, today_str)
        
        logger.info("✅ All reports updated successfully")
        return True

# ============================================
# ENTRY POINT
# ============================================
def main():
    """Main entry point."""
    tracker = MetaAdsTracker()
    success = tracker.run()
    
    if success:
        logger.info("🎉 Tracking cycle completed successfully")
    else:
        logger.error("⚠️ Tracking cycle completed with errors")
    
    return success

if __name__ == "__main__":
    main()
