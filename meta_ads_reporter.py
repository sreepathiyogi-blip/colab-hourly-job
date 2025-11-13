import sys
import os
import requests
import pandas as pd
import numpy as np
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

# ======================
# CONFIGURATION
# ======================
ACCESS_TOKEN = os.environ.get('META_ACCESS_TOKEN', "EAAHeR1E5PKUBP19I9GXYVw8kWusULp7l7ZBbyHf1qZCzBdPZA7enpZAbLZBQGajtASZCJWbesZCthHzV0K8xd2KfDKYZBRZAGjbMDtOZCmlX3jlRpMQUlAp8OedkqBD12rr35FnL4InZCrqhfV3fPTVACozb5YWZC7KmXZBgRabEbE1rwuKnZBJwsHYn0oOPtyZBm504dFJgE1ZA3KTw")
AD_ACCOUNT_IDS = ["act_1820431671907314", "act_24539675529051798"]
API_VERSION = "v21.0"
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID', "1Ka_DkNGCVi2h_plNN55-ZETW7M9MmFpTHocE7LZcYEM")
HOURLY_WORKSHEET_NAME = "Hourly Data"
DAILY_WORKSHEET_NAME = "Daily Sales Report"
AD_LEVEL_SHEET_NAME = "Ad Level Daily Sales"  # Only TODAY's data updates; historical dates FROZEN

IST = timezone(timedelta(hours=5, minutes=30))
sheets_client = None
sheet = None

# ======================
# UTIL
# ======================
def log(message):
    timestamp = datetime.now(IST).strftime('%m/%d/%Y %H:%M:%S IST')
    print(f"[{timestamp}] {message}")

def setup_google_sheets():
    """Authorize and ensure all worksheets exist."""
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
        
        for ws_name in [HOURLY_WORKSHEET_NAME, DAILY_WORKSHEET_NAME, AD_LEVEL_SHEET_NAME]:
            try:
                sheet.worksheet(ws_name)
            except gspread.WorksheetNotFound:
                # Plenty of rows/cols for growth
                sheet.add_worksheet(title=ws_name, rows=20000, cols=50)
        
        log("✅ Google Sheets setup completed")
        return True
    except Exception as e:
        log(f"❌ Google Sheets setup failed: {e}")
        return False

def write_error_to_sheet(error_message):
    """Append an error line in the Hourly sheet for visibility."""
    try:
        ws = sheet.worksheet(HOURLY_WORKSHEET_NAME)
        existing = ws.get_all_values()
        start_row = len(existing) + 1
        ts = datetime.now(IST).strftime('%m/%d/%Y %H:%M:%S')
        ws.update(range_name=f"A{start_row}", values=[[f"Error at {ts}: {error_message}"]]) 
    except Exception as e:
        log(f"❌ Failed to write error: {e}")

def validate_token():
    test_url = f"https://graph.facebook.com/{API_VERSION}/me"
    try:
        resp = requests.get(test_url, params={"access_token": ACCESS_TOKEN}, timeout=15).json()
        if "error" in resp:
            write_error_to_sheet(resp["error"]["message"])
            return False
        return True
    except Exception as e:
        write_error_to_sheet(str(e))
        return False

# ======================
# META API FETCHERS
# ======================
COMMON_FIELDS = "date_start,date_stop,impressions,clicks,spend,actions,action_values,cpm,cpc,ctr"

def _paginate(url, params):
    """Simple paging helper for Graph API GET."""
    results = []
    while True:
        r = requests.get(url, params=params, timeout=60)
        j = r.json()
        data = j.get("data", [])
        if data:
            results.extend(data)
        paging = j.get("paging", {})
        next_url = paging.get("next")
        if not next_url:
            break
        # After first call, follow absolute 'next' without params
        url, params = next_url, {}
    return results

def fetch_meta_data_account_today(account_id):
    url = f"https://graph.facebook.com/{API_VERSION}/{account_id}/insights"
    params = {
        "access_token": ACCESS_TOKEN,
        "fields": COMMON_FIELDS,
        "date_preset": "today",
        "level": "account"
    }
    try:
        return _paginate(url, params)
    except Exception as e:
        write_error_to_sheet(f"Account fetch error ({account_id}): {e}")
        return []

def fetch_meta_data_ads_today(account_id):
    # Include ad identifiers & names
    fields = COMMON_FIELDS + ",ad_id,ad_name"
    url = f"https://graph.facebook.com/{API_VERSION}/{account_id}/insights"
    params = {
        "access_token": ACCESS_TOKEN,
        "fields": fields,
        "date_preset": "today",
        "level": "ad"
    }
    try:
        return _paginate(url, params)
    except Exception as e:
        write_error_to_sheet(f"Ad-level fetch error ({account_id}): {e}")
        return []

# ======================
# PROCESSORS
# ======================
def _extract_metrics(items):
    """Aggregate account-level metrics across all rows."""
    link_clicks = landing_page_views = add_to_cart = initiate_checkout = purchases = impressions = clicks = 0
    purchases_value = spend = 0.0
    
    for item in items:
        spend += float(item.get("spend", 0) or 0)
        impressions += int(float(item.get("impressions", 0) or 0))
        clicks += int(float(item.get("clicks", 0) or 0))
        
        for act in item.get("actions", []) or []:
            atype = act.get("action_type")
            val = int(float(act.get("value", 0) or 0))
            if atype == "link_click":
                link_clicks += val
            elif atype == "landing_page_view":
                landing_page_views += val
            elif atype == "add_to_cart":
                add_to_cart += val
            elif atype == "initiate_checkout":
                initiate_checkout += val
            elif atype == "offsite_conversion.fb_pixel_purchase":
                purchases += val
        
        for valact in item.get("action_values", []) or []:
            if valact.get("action_type") == "offsite_conversion.fb_pixel_purchase":
                purchases_value += float(valact.get("value", 0) or 0)
    
    roas = purchases_value / spend if spend else 0
    cpc = spend / clicks if clicks else 0
    cpm = spend / impressions * 1000 if impressions else 0
    ctr = clicks / impressions * 100 if impressions else 0
    lc_to_lpv = (landing_page_views / link_clicks * 100) if link_clicks else 0
    lpv_to_atc = (add_to_cart / landing_page_views * 100) if landing_page_views else 0
    atc_to_ci = (initiate_checkout / add_to_cart * 100) if add_to_cart else 0
    ci_to_ordered = (purchases / initiate_checkout * 100) if initiate_checkout else 0
    cvr = (purchases / link_clicks * 100) if link_clicks else 0
    
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
        "LC TO LPV": lc_to_lpv,
        "LPV TO ATC": lpv_to_atc,
        "ATC TO CI": atc_to_ci,
        "CI TO ORDERED": ci_to_ordered,
        "CVR": cvr,
        "CPM": cpm
    }

def process_account_summary(all_account_data, timestamp):
    """Build the Hourly + Daily summary rows."""
    m = _extract_metrics(all_account_data)
    
    hourly_row = {
        "Date": datetime.now(IST).strftime('%m/%d/%Y'),
        "Timestamp": timestamp,
        "Spend": f"₹{m['Spend']:,.0f}",
        "Purchases Value": f"₹{m['Purchases Value']:,.0f}",
        "Purchases": m["Purchases"],
        "Impressions": m["Impressions"],
        "Link Clicks": m["Link Clicks"],
        "Landing Page Views": m["Landing Page Views"],
        "Add to Cart": m["Add to Cart"],
        "Initiate Checkout": m["Initiate Checkout"],
        "ROAS": round(m["ROAS"], 2),
        "CPC": f"₹{m['CPC']:.2f}",
        "CTR": f"{m['CTR']:.2f}%",
        "LC TO LPV": f"{m['LC TO LPV']:.2f}%",
        "LPV TO ATC": f"{m['LPV TO ATC']:.2f}%",
        "ATC TO CI": f"{m['ATC TO CI']:.2f}%",
        "CI TO ORDERED": f"{m['CI TO ORDERED']:.2f}%",
        "CVR": f"{m['CVR']:.2f}%",
        "CPM": f"₹{m['CPM']:.2f}"
    }
    
    daily_row = {k: v for k, v in hourly_row.items() if k != "Timestamp"}
    
    return pd.DataFrame([hourly_row]), pd.DataFrame([daily_row])

def process_ad_level(all_ad_rows, today_str):
    """
    Build a per-ad table for TODAY (no Timestamp).
    Aggregates across accounts per ad_id/ad_name for the current run.
    Shows 0 for missing values instead of NaN.
    """
    recs = []
    for r in all_ad_rows:
        spend = float(r.get("spend", 0) or 0)
        impressions = int(float(r.get("impressions", 0) or 0))
        clicks = int(float(r.get("clicks", 0) or 0))
        ad_id = r.get("ad_id", "")
        ad_name = r.get("ad_name", "")
        
        link_clicks = landing_page_views = add_to_cart = initiate_checkout = purchases = 0
        purchase_value = 0.0
        
        for act in (r.get("actions") or []):
            at = act.get("action_type")
            val = int(float(act.get("value", 0) or 0))
            if at == "link_click":
                link_clicks += val
            elif at == "landing_page_view":
                landing_page_views += val
            elif at == "add_to_cart":
                add_to_cart += val
            elif at == "initiate_checkout":
                initiate_checkout += val
            elif at == "offsite_conversion.fb_pixel_purchase":
                purchases += val
        
        for valact in (r.get("action_values") or []):
            if valact.get("action_type") == "offsite_conversion.fb_pixel_purchase":
                purchase_value += float(valact.get("value", 0) or 0)
        
        recs.append({
            "ad_id": ad_id,
            "ad_name": ad_name,
            "spend": spend,
            "impressions": impressions,
            "clicks": clicks,
            "link_clicks": link_clicks,
            "landing_page_views": landing_page_views,
            "add_to_cart": add_to_cart,
            "initiate_checkout": initiate_checkout,
            "purchases": purchases,
            "purchases_value": purchase_value
        })
    
    if not recs:
        return pd.DataFrame(columns=[
            "Date","Ad ID","Ad Name","Spend","Purchases Value","Purchases",
            "Impressions","Clicks","ROAS","CPC","CTR",
            "LC→LPV %","LPV→ATC %","ATC→CI %","CI→Order %","CVR %","CPM"
        ])
    
    df = pd.DataFrame(recs)
    
    # Aggregate across accounts by ad for THIS RUN (so one row per ad for today)
    g = df.groupby(["ad_id","ad_name"], as_index=False).sum(numeric_only=True)
    
    # ---- Derived metrics (use 0 for division by zero instead of NaN) ----
    g["ROAS"] = g.apply(lambda x: x["purchases_value"] / x["spend"] if x["spend"] > 0 else 0, axis=1)
    g["CPC"] = g.apply(lambda x: x["spend"] / x["clicks"] if x["clicks"] > 0 else 0, axis=1)
    g["CPM"] = g.apply(lambda x: (x["spend"] / x["impressions"]) * 1000 if x["impressions"] > 0 else 0, axis=1)
    g["CTR"] = g.apply(lambda x: (x["clicks"] / x["impressions"]) * 100 if x["impressions"] > 0 else 0, axis=1)
    g["LC→LPV %"] = g.apply(lambda x: (x["landing_page_views"] / x["link_clicks"]) * 100 if x["link_clicks"] > 0 else 0, axis=1)
    g["LPV→ATC %"] = g.apply(lambda x: (x["add_to_cart"] / x["landing_page_views"]) * 100 if x["landing_page_views"] > 0 else 0, axis=1)
    g["ATC→CI %"] = g.apply(lambda x: (x["initiate_checkout"] / x["add_to_cart"]) * 100 if x["add_to_cart"] > 0 else 0, axis=1)
    g["CI→Order %"] = g.apply(lambda x: (x["purchases"] / x["initiate_checkout"]) * 100 if x["initiate_checkout"] > 0 else 0, axis=1)
    g["CVR %"] = g.apply(lambda x: (x["purchases"] / x["link_clicks"]) * 100 if x["link_clicks"] > 0 else 0, axis=1)
    
    out = pd.DataFrame({
        "Date": today_str,
        "Ad ID": g["ad_id"],
        "Ad Name": g["ad_name"],
        "Spend": g["spend"].round(0),
        "Purchases Value": g["purchases_value"].round(0),
        "Purchases": g["purchases"].astype(int),
        "Impressions": g["impressions"].astype(int),
        "Clicks": g["clicks"].astype(int),
        "ROAS": g["ROAS"].round(2),
        "CPC": g["CPC"].round(2),
        "CTR": g["CTR"].round(2),
        "LC→LPV %": g["LC→LPV %"].round(2),
        "LPV→ATC %": g["LPV→ATC %"].round(2),
        "ATC→CI %": g["ATC→CI %"].round(2),
        "CI→Order %": g["CI→Order %"].round(2),
        "CVR %": g["CVR %"].round(2),
        "CPM": g["CPM"].round(2),
    }).sort_values(["Spend","Purchases Value"], ascending=False).reset_index(drop=True)
    
    # Fill any remaining NaN values with 0
    out = out.fillna(0)
    
    return out

# ======================
# SHEET WRITERS
# ======================
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

def update_daily_summary_row(df_daily):
    """Upsert one daily summary row per date in 'Daily Sales Report'."""
    try:
        ws = sheet.worksheet(DAILY_WORKSHEET_NAME)
        existing = ws.get_all_values()
        current_date = datetime.now(IST).strftime('%m/%d/%Y')
        
        update_row = None
        for idx, row in enumerate(existing[1:], start=2):
            if row and row[0] == current_date:
                update_row = idx
                break
        
        if update_row:
            set_with_dataframe(ws, df_daily, include_column_header=False, row=update_row, col=1)
            log(f"✅ Updated daily summary for {current_date}")
        else:
            row = len(existing) + 1
            set_with_dataframe(ws, df_daily, include_column_header=(row == 1), row=row)
            log(f"✅ Added new daily summary for {current_date}")
        
        return True
    except Exception as e:
        write_error_to_sheet(str(e))
        return False

def upsert_ad_level_daily(ad_df, today_str):
    """
    ✨ NEW LOGIC: Keep historical data FROZEN ✨
    - Previous dates: NEVER touched
    - TODAY only: Remove existing rows with today's date and replace with fresh data
    """
    try:
        try:
            ws = sheet.worksheet(AD_LEVEL_SHEET_NAME)
        except gspread.WorksheetNotFound:
            ws = sheet.add_worksheet(title=AD_LEVEL_SHEET_NAME, rows=20000, cols=50)
            set_with_dataframe(ws, ad_df, include_column_header=True, row=1, col=1)
            log("✅ Ad Level Daily Sales created & written")
            return True

        existing = ws.get_all_values()
        if not existing:
            set_with_dataframe(ws, ad_df, include_column_header=True, row=1, col=1)
            log("✅ Ad Level Daily Sales initialized (was empty)")
            return True

        headers = existing[0]
        rows = existing[1:]

        # Make headers unique to avoid pandas errors
        def make_unique(cols):
            seen = {}
            out = []
            for c in cols:
                base = c if c is not None else ""
                if base in seen:
                    seen[base] += 1
                    new = f"{base}_{seen[base]}"
                else:
                    seen[base] = 0
                    new = base
                out.append(new)
            return out

        unique_headers = make_unique(headers)

        if rows:
            df_exist = pd.DataFrame(rows, columns=unique_headers)
        else:
            df_exist = pd.DataFrame(columns=unique_headers)

        # Find the Date column
        date_col = None
        for col in df_exist.columns:
            if col == "Date" or col.lower().startswith("date"):
                date_col = col
                break

        if date_col is None or date_col not in df_exist.columns:
            df_exist.insert(0, "Date", "")
            date_col = "Date"

        # ✨ KEY CHANGE: Keep ALL rows EXCEPT today's date ✨
        df_historical = df_exist[df_exist[date_col] != today_str].copy()
        
        log(f"📌 Preserving {len(df_historical)} rows from previous dates (frozen)")
        log(f"🔄 Replacing {len(df_exist[df_exist[date_col] == today_str])} rows for {today_str}")

        # Ensure ad_df has Date column
        if "Date" not in ad_df.columns:
            ad_df.insert(0, "Date", today_str)
        else:
            ad_df["Date"] = today_str

        # Align columns
        all_cols = list(dict.fromkeys(list(df_historical.columns) + list(ad_df.columns)))
        df_historical = df_historical.reindex(columns=all_cols, fill_value=0)
        ad_prepped = ad_df.reindex(columns=all_cols, fill_value=0)

        # Combine: Historical (frozen) + Today's fresh data
        df_new = pd.concat([df_historical, ad_prepped], ignore_index=True, sort=False)
        df_new = df_new.fillna(0)

        # Write back
        ws.clear()
        set_with_dataframe(ws, df_new, include_column_header=True, row=1, col=1)
        log(f"✅ Historical data preserved | Today ({today_str}): {len(ad_df)} fresh rows")
        return True
        
    except Exception as e:
        write_error_to_sheet(f"Ad-level daily upsert failed: {e}")
        return False

# ======================
# RUNNER
# ======================
def run_report():
    timestamp = datetime.now(IST).strftime('%m/%d/%Y %H:%M:%S')
    today_str = datetime.now(IST).strftime('%m/%d/%Y')
    
    if not validate_token():
        return False
    
    # Fetch data
    all_account_data = []
    all_ad_data = []
    for account_id in AD_ACCOUNT_IDS:
        all_account_data.extend(fetch_meta_data_account_today(account_id))
        all_ad_data.extend(fetch_meta_data_ads_today(account_id))
    
    if not all_account_data:
        write_error_to_sheet("No account-level data returned.")
        return False
    
    # 1) Hourly summary row (append)
    hourly_df, daily_df = process_account_summary(all_account_data, timestamp)
    update_hourly_sheet(hourly_df)
    
    # 2) Daily summary row (upsert by date)
    update_daily_summary_row(daily_df)
    
    # 3) Ad Level Daily Sales (Historical FROZEN, only TODAY updates)
    ad_level_df = process_ad_level(all_ad_data, today_str)
    upsert_ad_level_daily(ad_level_df, today_str)
    
    log("✅ Hourly summary, Daily summary, and Ad Level Daily Sales updated")
    return True

def main():
    log("🎯 META ADS DAILY TRACKER STARTED")
    if setup_google_sheets():
        run_report()
    else:
        log("❌ Setup failed")

if __name__ == "__main__":
    main()
