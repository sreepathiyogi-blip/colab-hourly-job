name: Run meta_ads_reporter hourly

on:
  schedule:
    # Runs every hour at minute 0 (UTC time)
    - cron: "0 * * * *"
  
  workflow_dispatch:  # Manual trigger option

# Ensure only one workflow runs at a time
concurrency:
  group: meta-ads-reporter
  cancel-in-progress: false

jobs:
  run:
    runs-on: ubuntu-latest
    timeout-minutes: 10  # Prevent hanging workflows
    
    steps:
      - name: Checkout repository
        uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.10"
          cache: 'pip'  # Cache pip dependencies for faster runs
      
      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          if [ -f requirements.txt ]; then 
            pip install -r requirements.txt
          else
            pip install pandas numpy requests gspread gspread-dataframe google-auth google-auth-oauthlib google-auth-httplib2
          fi
      
      - name: Setup Google credentials
        run: |
          echo '${{ secrets.GOOGLE_CREDENTIALS }}' > service-account.json
          chmod 600 service-account.json  # Secure file permissions
      
      - name: Run meta_ads_reporter.py
        env:
          GOOGLE_APPLICATION_CREDENTIALS: service-account.json
          META_ACCESS_TOKEN: ${{ secrets.META_ACCESS_TOKEN }}
          SPREADSHEET_ID: ${{ secrets.SPREADSHEET_ID }}
          TZ: Asia/Kolkata  # Set timezone for logs
        run: |
          echo "=============================================="
          echo "🚀 HOURLY UPDATE STARTING"
          echo "=============================================="
          echo "📅 Current Date: $(date '+%Y-%m-%d')"
          echo "🕐 Current Time (IST): $(TZ=Asia/Kolkata date '+%H:%M:%S %Z')"
          echo "🕐 Current Time (UTC): $(date -u '+%H:%M:%S %Z')"
          echo "📊 Workflow Run: #${{ github.run_number }}"
          echo "=============================================="
          echo ""
          
          python meta_ads_reporter.py
          
          echo ""
          echo "=============================================="
          echo "✅ HOURLY UPDATE COMPLETED"
          echo "🕐 Finished at (IST): $(TZ=Asia/Kolkata date '+%H:%M:%S %Z')"
          echo "=============================================="
      
      - name: Cleanup credentials
        if: always()
        run: rm -f service-account.json
      
      # Optional: Upload logs on failure for debugging
      - name: Upload logs on failure
        if: failure()
        uses: actions/upload-artifact@v4
        with:
          name: error-logs-${{ github.run_number }}
          path: |
            *.log
            *.txt
          retention-days: 7
