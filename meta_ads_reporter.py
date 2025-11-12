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
        
        def heartbeat():
            while True:
                time.sleep(300)
                print(f"💚 Heartbeat: Session active at {datetime.now().strftime('%H:%M:%S')}")

        Thread(target=heartbeat, daemon=True).start()
        print("✅ Keep-alive enabled for Google Colab")

    keep_colab_alive()
else:
    from google.oauth2 import service_account

# CONFIGURATION
ACCESS_TOKEN = os.environ.get('META_ACCESS_TOKEN', "EAAHeR1E5PKUBP19I9GXYVw8kWusULp7l7ZBbyHf1qZCzBdPZA7enpZAbLZBQGajtASZCJWbesZCthHzV0K8xd2KfDKYZBRZAGjbMDtOZCmlX3jlRpMQUlAp8OedkqBD12rr35FnL4InZCrqhfV3fPTVACozb5YWZC7KmXZBgRabEbE1rwuKnZBJwsHYn0oOPtyZBm504dFJgE1ZA3KTw")
AD_ACCOUNT_IDS = ["act_1820431671907314", "act_24539675529051798"]
API_VERSION = "v21.0"
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID', "1Ka_DkNGCVi2h_plNN55-ZETW7M9MmFpTHocE7LZcYEM")
HOURLY_WORKSHEET_NAME = "Hourly Data"
DAILY_WORKSHEET_NAME = "Daily Sales Report"
IST = timezone(timedelta(hours=5, minutes=30))

sheets_client = None
sheet = None

def log(message):
    timestamp = datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')
    print(f"[{timestamp}] {message}")

def setup_google_sheets():
    global sheets_client, sheet
    try:
        if IN_COLAB:
            auth.authenticate_user()
            creds, _ = default()
            sheets_client = gspread.authorize(creds)
        else:
            creds_file = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', 'service-account.json')
            scopes = ['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive']
            creds = service_account.Credentials.from_service_account_file(creds_file, scopes=scopes)
            sheets_client = gspread.authorize(creds)
        
        sheet = sheets_client.open_by_key(SPREADSHEET_ID)
        for ws_name in [HOURLY_WORKSHEET_NAME, DAILY_WORKSHEET_NAME]:
            try:
                sheet.worksheet(ws_name)
            except:
                sheet.add_worksheet(title=ws_name, rows=1000, cols=20)
        
        log("✅ Google Sheets setup completed")
        return True
    except Exception as e:
        log(f"❌ Google Sheets setup failed: {e}")
        return False

def write_error_to_sheet(error_message):
    try:
        worksheet = sheet.worksheet(HOURLY_WORKSHEET_NAME)
        existing = worksheet.get_all_values()
        start_row = len(existing) + 1
        ts = datetime.now(IST).strftime('%d-%m-%Y %H:%M:%S')
        worksheet.update(range_name=f"A{start_row}", values=[[f"Error at {ts}: {error_message}"]])
    except Exception as e:
        log(f"❌ Failed to write error: {e}")

def validate_token():
    test_url = f"https://graph.facebook.com/{API_VERSION}/me"
    try:
        resp = requests.get(test_url, params={"access_token": ACCESS_TOKEN}, timeout=10).json()
        if "error" in resp:
            write_error_to_sheet(resp["error"]["message"])
            return False
        return True
    except Exception as e:
        write_error_to_sheet(str(e))
        return False

def fetch_meta_data():
    all_data = []
    for account_id in AD_ACCOUNT_IDS:
        url = f"https://graph.facebook.com/{API_VERSION}/{account_id}/insights"
        params = {
            "access_token": ACCESS_TOKEN,
            "fields": "date_start,date_stop,impressions,clicks,spend,actions,action_values,cpm,cpc,ctr",
            "date_preset": "today",
            "level": "account"
        }
        try:
            resp = requests.get(url, params=params, timeout=30)
            data = resp.json().get("data", [])
            for d in data:
                d["account_id"] = account_id
            all_data.extend(data)
        except Exception as e:
            write_error_to_sheet(str(e))
    return all_data

def process_combined_data(all_data, timestamp):
    link_clicks = landing_page_views = add_to_cart = initiate_checkout = purchases = impressions = clicks = 0
    purchases_value = spend = 0.0

    for item in all_data:
        spend += float(item.get("spend", 0))
        impressions += int(float(item.get("impressions", 0)))
        clicks += int(float(item.get("clicks", 0)))
        for act in item.get("actions", []) or []:
            atype = act.get("action_type")
            val = int(float(act.get("value", 0)))
            if atype == "link_click": link_clicks += val
            elif atype == "landing_page_view": landing_page_views += val
            elif atype == "add_to_cart": add_to_cart += val
            elif atype == "initiate_checkout": initiate_checkout += val
            elif atype == "offsite_conversion.fb_pixel_purchase": purchases += val
        for valact in item.get("action_values", []) or []:
            if valact.get("action_type") == "offsite_conversion.fb_pixel_purchase":
                purchases_value += float(valact.get("value", 0))

    roas = purchases_value / spend if spend else 0
    cpc = spend / clicks if clicks else 0
    cpm = spend / impressions * 1000 if impressions else 0
    ctr = clicks / impressions * 100 if impressions else 0

    lc_to_lpv = landing_page_views / link_clicks * 100 if link_clicks else 0
    lpv_to_atc = add_to_cart / landing_page_views * 100 if landing_page_views else 0
    atc_to_ci = initiate_checkout / add_to_cart * 100 if add_to_cart else 0
    ci_to_ordered = purchases / initiate_checkout * 100 if initiate_checkout else 0
    cvr = purchases / link_clicks * 100 if link_clicks else 0

    # 🗓 Change date format here
    date = datetime.now(IST).strftime('%d-%m-%Y')
    
    hourly_row = {
        "Date": date,
        "Timestamp": timestamp,
        "Spend": f"₹{spend:,.0f}",
        "Purchases Value": f"₹{purchases_value:,.0f}",
        "Purchases": purchases,
        "Impressions": impressions,
        "Link Clicks": link_clicks,
        "Landing Page Views": landing_page_views,
        "Add to Cart": add_to_cart,
        "Initiate Checkout": initiate_checkout,
        "ROAS": round(roas, 2),
        "CPC": f"₹{cpc:.2f}",
        "CTR": f"{ctr:.2f}%",
        "LC TO LPV": f"{lc_to_lpv:.2f}%",
        "LPV TO ATC": f"{lpv_to_atc:.2f}%",
        "ATC TO CI": f"{atc_to_ci:.2f}%",
        "CI TO ORDERED": f"{ci_to_ordered:.2f}%",
        "CVR": f"{cvr:.2f}%",
        "CPM": f"₹{cpm:.2f}"
    }

    daily_row = {k: v for k, v in hourly_row.items() if k != "Timestamp"}
    return pd.DataFrame([hourly_row]), pd.DataFrame([daily_row])

def update_hourly_sheet(df):
    try:
        ws = sheet.worksheet(HOURLY_WORKSHEET_NAME)
        existing = ws.get_all_values()
        row = len(existing) + 1
        set_with_dataframe(ws, df, include_column_header=(row == 1), row=row)
        log("✅ Hourly sheet updated")
        return True
    except Exception as e:
        write_error_to_sheet(str(e))
        return False

def update_daily_sheet(df):
    try:
        ws = sheet.worksheet(DAILY_WORKSHEET_NAME)
        existing = ws.get_all_values()
        current_date = datetime.now(IST).strftime('%d-%m-%Y')
        update_row = None

        for idx, row in enumerate(existing[1:], start=2):
            if row and row[0] == current_date:
                update_row = idx
                break

        if update_row:
            set_with_dataframe(ws, df, include_column_header=False, row=update_row, col=1)
            log(f"✅ Updated daily data for {current_date}")
        else:
            row = len(existing) + 1
            set_with_dataframe(ws, df, include_column_header=(row == 1), row=row)
            log(f"✅ Added new daily entry for {current_date}")
        return True
    except Exception as e:
        write_error_to_sheet(str(e))
        return False

def run_report():
    timestamp = datetime.now(IST).strftime('%d-%m-%Y %H:%M:%S')
    if not validate_token(): return False
    data = fetch_meta_data()
    if not data: return False
    hourly_df, daily_df = process_combined_data(data, timestamp)
    update_hourly_sheet(hourly_df)
    update_daily_sheet(daily_df)
    log("✅ Both sheets updated successfully")
    return True

def main():
    log("🎯 META ADS DAILY TRACKER STARTED")
    if setup_google_sheets():
        run_report()
    else:
        log("❌ Setup failed")

if __name__ == "__main__":
    main()
