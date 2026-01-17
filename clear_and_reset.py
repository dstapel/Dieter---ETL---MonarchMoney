# %%
"""
Clear Transactions Sheet and Reset Control Script
=================================================
This script clears the Transactions sheet and resets the last_run_utc control value
so you can do a full reload of all transaction data.

Run this when you want to:
- Test the script with fresh data
- Reload all transactions with new field extractions
- Fix any data issues by starting fresh
"""
import os
from pathlib import Path
from google.oauth2.service_account import Credentials
import gspread

# Same configuration as main script
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

BASE_DIR = Path(__file__).parent
CREDS_PATH = BASE_DIR / ".secrets" / "GSheet-Monarch-Key.json"

ACCOUNTS_WS = "Accounts"
TXNS_WS = "Transactions"
CONTROL_WS = "Control"

# Google Sheet: set your spreadsheet ID (from URL) or via env var GSPREAD_SHEET_ID
SPREADSHEET_ID = os.getenv("GSPREAD_SHEET_ID", "18KMvcg-z8r77Csth7zbOf_VKmyWzkhZzCT8Btdbkpag")

def _ensure_ws(gc: gspread.Client, sheet_id: str, title: str) -> gspread.Worksheet:
    sh = gc.open_by_key(sheet_id)
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=1000, cols=26)

def main():
    """Clear transactions and reset control for fresh start."""
    print("🧹 Starting clear and reset process...")
    
    # Authorize Google Sheets
    creds = Credentials.from_service_account_file(str(CREDS_PATH), scopes=SCOPES)
    gc = gspread.authorize(creds)
    
    try:
        # Clear Transactions sheet
        print("📋 Clearing Transactions sheet...")
        ws_tx = _ensure_ws(gc, SPREADSHEET_ID, TXNS_WS)
        ws_tx.clear()
        print("✅ Transactions sheet cleared!")
        
        # Reset Control sheet
        print("🎛️  Resetting Control sheet...")
        ws_ctl = _ensure_ws(gc, SPREADSHEET_ID, CONTROL_WS)
        ws_ctl.clear()
        # Set up fresh control structure with no last_run_utc value
        ws_ctl.update([["key", "value"]], "A1:B1")
        print("✅ Control sheet reset (last_run_utc cleared)!")
        
        print("\n🎉 All done! Your sheets are ready for a fresh data load.")
        print("💡 Run your main script now to reload all transaction data with the new field extractions.")
        
    except Exception as e:
        print(f"❌ Error during clear and reset: {e}")
        raise

if __name__ == "__main__":
    main()
