import sys
import os
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
import time
import gspread
from gspread_dataframe import set_with_dataframe

# Detect environment
IN_COLAB = 'google.colab' in sys.modules

# Conditional Colab imports and keep-alive
if IN_COLAB:
    from IPython.display import display, Javascript
    from threading import Thread
    from google.colab import auth
    from google.auth import default
    
    def keep_colab_alive():
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

        def background_heartbeat():
            while True:
                time.sleep(300)
                current_time = time.strftime('%Y-%m-%d %H:%M:%S')
                print(f"💚 Heartbeat: Session active at {current_time}")

        heartbeat_thread = Thread(target=background_heartbeat, daemon=True)
        heartbeat_thread.start()

        print("=" * 60)
        print("✅ KEEP-ALIVE SYSTEM ACTIVATED!")
        print("=" * 60)
        print("📱 Browser/app must stay open")
        print("🔌 Keep device plugged in for best results")
        print("⏰ JavaScript pings every 60 seconds")
        print("💚 Python heartbeat every 5 minutes")
        print("=" * 60)
    
    keep_colab_alive()
else:
    # GitHub Actions environment
    from google.oauth2 import service_account

# CONFIGURATION
ACCESS_TOKEN = os.environ.get('META_ACCESS_TOKEN', "EAAHeR1E5PKUBP19I9GXYVw8kWusULp7l7ZBbyHf1qZCzBdPZA7enpZAbLZBQGajtASZCJWbesZCthHzV0K8xd2KfDKYZBRZAGjbMDtOZCmlX3jlRpMQUlAp8OedkqBD12rr35FnL4InZCrqhfV3fPTVACozb5YWZC7KmXZBgRabEbE1rwuKnZBJwsHYn0oOPtyZBm504dFJgE1ZA3KTw")
AD_ACCOUNT_IDS = ["act_1820431671907314", "act_24539675529051798"]
API_VERSION = "v21.0"
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID', "1Ka_DkNGCVi2h_plNN55-ZETW7M9MmFpTHocE7LZcYEM")
WORKSHEET_NAME = "Facebook Campaign Data"
IST = timezone(timedelta(hours=5, minutes=30))
DROP_THRESHOLD = 0.10

sheets_client = None
sheet = None

def log(message):
    timestamp = datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {message}")

def setup_google_sheets():
    global sheets_client, sheet
    try:
        if IN_COLAB:
            log("🔐 Authenticating with Google Colab...")
            auth.authenticate_user()
            creds, _ = default()
            sheets_client = gspread.authorize(creds)
            log("✅ Successfully authenticated with Google Colab")
        else:
            log("🔐 Authenticating with Service Account...")
            creds_file = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', 'service-account.json')
            
            if not os.path.exists(creds_file):
                log(f"❌ Credentials file not found: {creds_file}")
                return False
            
            scopes = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            
            creds = service_account.Credentials.from_service_account_file(
                creds_file,
                scopes=scopes
            )
            
            sheets_client = gspread.authorize(creds)
            log("✅ Successfully authenticated with Service Account")
        
        sheet = sheets_client.open_by_key(SPREADSHEET_ID)
        log(f"✅ Opened spreadsheet with ID: {SPREADSHEET_ID}")
        
        worksheets = sheet.worksheets()
        campaign_data_exists = False
        
        for ws in worksheets:
            if ws.title == WORKSHEET_NAME:
                campaign_data_exists = True
            elif ws.title != WORKSHEET_NAME:
                sheet.del_worksheet(ws)
                log(f"🗑️ Deleted extra worksheet: {ws.title}")
        
        if not campaign_data_exists:
            worksheet = sheet.add_worksheet(title=WORKSHEET_NAME, rows=10000, cols=21)
            log(f"✅ Created worksheet: {WORKSHEET_NAME}")
        else:
            log(f"✅ Found existing worksheet: {WORKSHEET_NAME}")
        
        sheet_url = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}"
        log(f"📊 Google Sheet link: {sheet_url}")
        
        if IN_COLAB:
            try:
                from IPython.display import display, HTML
                display(HTML(f'<a href="{sheet_url}" target="_blank">🌐 Open Google Sheet</a>'))
            except:
                pass
        
        return True
    except Exception as e:
        log(f"❌ Google Sheets setup failed: {str(e)}")
        return False

def write_error_to_sheet(error_message):
    try:
        worksheet = sheet.worksheet(WORKSHEET_NAME)
        existing = worksheet.get_all_values()
        start_row = len(existing) + 1
        error_row = [f"Error at {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')}: {error_message}"]
        worksheet.update(range_name=f'A{start_row}', values=[error_row])
        log(f"✅ Wrote error to sheet: {error_message}")
    except Exception as e:
        log(f"❌ Failed to write error to sheet: {str(e)}")

def validate_token():
    log("🔐 Validating Meta Ads access token...")
    test_url = f"https://graph.facebook.com/{API_VERSION}/me"
    test_params = {"access_token": ACCESS_TOKEN}
    try:
        resp = requests.get(test_url, params=test_params, timeout=10).json()
        if 'error' in resp:
            error_msg = f"Token invalid: {resp['error']['message']}"
            log(f"❌ {error_msg}")
            write_error_to_sheet(error_msg)
            return False
        log("✅ Token is valid for user: " + resp.get("name", "Unknown"))
        return True
    except Exception as e:
        error_msg = f"Token validation error: {str(e)}"
        log(f"❌ {error_msg}")
        write_error_to_sheet(error_msg)
        return False

def test_ad_account(account_id):
    log(f"🔍 Testing ad account access for {account_id}")
    test_url = f"https://graph.facebook.com/{API_VERSION}/{account_id}"
    params = {"access_token": ACCESS_TOKEN, "fields": "name"}
    try:
        resp = requests.get(test_url, params=params, timeout=10).json()
        if 'error' in resp:
            error_msg = f"Ad account access failed: {resp['error']['message']}"
            log(f"❌ {error_msg}")
            write_error_to_sheet(error_msg)
            return False
        log(f"✅ Ad account valid: {resp.get('name')}")
        return True
    except Exception as e:
        error_msg = f"Ad account test error: {str(e)}"
        log(f"❌ {error_msg}")
        write_error_to_sheet(error_msg)
        return False

def fetch_meta_data():
    log(f"📊 Fetching cumulative Meta Ads data for today...")
    all_data = []
    for account_id in AD_ACCOUNT_IDS:
        insights_url = f"https://graph.facebook.com/{API_VERSION}/{account_id}/insights"
        params = {
            "access_token": ACCESS_TOKEN,
            "fields": "date_start,date_stop,impressions,clicks,spend,actions,action_values,cpm,cpc,ctr",
            "date_preset": "today",
            "level": "account",
            "limit": 50
        }
        try:
            time.sleep(2)
            resp = requests.get(insights_url, params=params, timeout=30)
            resp_json = resp.json()
            if resp.status_code != 200 or 'error' in resp_json:
                error_msg = f"API request failed for {account_id}: {resp_json.get('error', {}).get('message', 'Unknown error')}"
                log(f"❌ {error_msg}")
                write_error_to_sheet(error_msg)
                continue
            data = resp_json.get("data", [])
            if not data:
                log(f"⚠️ No data for account {account_id} today")
                continue
            for item in data:
                item['account_id'] = account_id
            log(f"✅ {account_id}: Fetched {len(data)} records")
            all_data.extend(data)
        except Exception as e:
            error_msg = f"Fetch error for {account_id}: {str(e)}"
            log(f"❌ {error_msg}")
            write_error_to_sheet(error_msg)
            continue
    if not all_data:
        log("⚠️ No data available for any account today")
    return all_data

def process_combined_data(all_data, timestamp):
    link_clicks = 0
    landing_page_views = 0
    add_to_cart = 0
    initiate_checkout = 0
    purchases = 0
    purchases_value = 0
    spend = 0
    impressions = 0
    clicks = 0

    for item in all_data:
        spend += float(item.get("spend", 0) or 0)
        impressions += int(float(item.get("impressions", 0)))
        clicks += int(float(item.get("clicks", 0)))

        if "actions" in item and item["actions"]:
            for action in item["actions"]:
                atype = action.get("action_type")
                aval = int(float(action.get("value", 0)))
                if atype == "link_click":
                    link_clicks += aval
                elif atype == "landing_page_view":
                    landing_page_views += aval
                elif atype == "add_to_cart":
                    add_to_cart += aval
                elif atype == "initiate_checkout":
                    initiate_checkout += aval
                elif atype == "purchase" or atype == "offsite_conversion.fb_pixel_purchase":
                    purchases += aval

        if "action_values" in item and item["action_values"]:
            for action in item["action_values"]:
                atype = action.get("action_type")
                aval = float(action.get("value", 0))
                if atype == "offsite_conversion.fb_pixel_purchase":
                    purchases_value += aval

    roas = purchases_value / spend if spend > 0 else 0
    cpc = spend / clicks if clicks > 0 else 0
    cpm = (spend / impressions * 1000) if impressions > 0 else 0
    ctr = (clicks / impressions * 100) if impressions > 0 else 0
    lc_to_lpv = (landing_page_views / link_clicks * 100) if link_clicks > 0 else 0
    lpv_to_atc = (add_to_cart / landing_page_views * 100) if landing_page_views > 0 else 0
    atc_to_ci = (initiate_checkout / add_to_cart * 100) if add_to_cart > 0 else 0
    ci_to_ordered = (purchases / initiate_checkout * 100) if initiate_checkout > 0 else 0
    cvr = (purchases / link_clicks * 100) if link_clicks > 0 else 0

    date = datetime.now(IST).strftime('%Y-%m-%d')
    row = {
        "Date": date,
        "Timestamp": timestamp,
        "Spend": round(spend, 2),
        "Purchases Value": round(purchases_value, 2),
        "Purchases": purchases,
        "Impressions": impressions,
        "Link Clicks": link_clicks,
        "Landing Page Views": landing_page_views,
        "Add to Cart": add_to_cart,
        "Initiate Checkout": initiate_checkout,
        "ROAS": round(roas, 2),
        "CPC": round(cpc, 2),
        "CTR": f"{ctr:.2f}%",
        "LC TO LPV": f"{lc_to_lpv:.2f}%",
        "LPV TO ATC": f"{lpv_to_atc:.2f}%",
        "ATC TO CI": f"{atc_to_ci:.2f}%",
        "CI TO ORDERED": f"{ci_to_ordered:.2f}%",
        "CVR": f"{cvr:.2f}%",
        "CPM": round(cpm, 2),
        "Drop Alert": ""
    }
    df = pd.DataFrame([row])
    log(f"✅ Processed aggregated DataFrame with combined accounts")
    return df

def update_google_sheet(df):
    try:
        worksheet = sheet.worksheet(WORKSHEET_NAME)
        existing = worksheet.get_all_values()
        start_row = len(existing) + 1
        if start_row == 1:
            set_with_dataframe(worksheet, df, include_column_header=True)
            log(f"✅ Appended {len(df)} row to sheet at A1")
        else:
            set_with_dataframe(worksheet, df, include_column_header=False, row=start_row)
            log(f"✅ Appended {len(df)} row to sheet at row {start_row}")
        return True
    except Exception as e:
        error_msg = f"Failed to update Google Sheet: {str(e)}"
        log(f"❌ {error_msg}")
        write_error_to_sheet(error_msg)
        return False

def run_report(timestamp=None):
    if timestamp is None:
        timestamp = datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')
    log(f"🚀 Starting report run for cumulative data up to {timestamp}...")
    
    if not validate_token():
        log("❌ Report failed: Invalid Meta Ads access token")
        return False
    
    for account_id in AD_ACCOUNT_IDS:
        if not test_ad_account(account_id):
            log(f"❌ Report failed: Invalid ad account {account_id}")
            return False
    
    all_data = fetch_meta_data()
    if not all_data:
        log("⚠️ No data to process")
        return False
    
    df = process_combined_data(all_data, timestamp)
    if df is None or df.empty:
        log("❌ Data processing failed or empty DataFrame")
        return False
    
    if update_google_sheet(df):
        log(f"✅ Report completed successfully - captured cumulative data up to {timestamp}")
        return True
    return False

def main():
    log("=" * 60)
    log("🎯 STARTING META ADS HOURLY REPORTER")
    log("=" * 60)
    log(f"📅 Current time: {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')}")
    log(f"📍 Environment: {'Google Colab' if IN_COLAB else 'GitHub Actions'}")
    log("=" * 60)
    
    if setup_google_sheets():
        current_time = datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')
        success = run_report(current_time)
        
        log("=" * 60)
        if success:
            log("✅ REPORT COMPLETED SUCCESSFULLY!")
        else:
            log("⚠️ REPORT COMPLETED WITH WARNINGS")
        log("=" * 60)
    else:
        log("❌ Failed to start: Google Sheets setup failed")
        sys.exit(1)

if __name__ == "__main__":
    main()

