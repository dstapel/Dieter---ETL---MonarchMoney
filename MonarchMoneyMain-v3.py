# filepath: /c:/Users/dstap/Dropbox/Dieter Files/Dieter Codes/_DataFlow/Finance/MonarchMoney/MonarchMoneyMain-v2.py
# %%
import asyncio
import os
import json
import argparse
from datetime import datetime, timedelta, timezone, date
from pathlib import Path
from monarchmoney import MonarchMoney, RequireMFAException
from google.oauth2.service_account import Credentials
from gql.transport.exceptions import TransportServerError
import gspread
import dataclasses
from typing import Any, Dict, List, Optional

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

BASE_DIR = Path(__file__).parent
CREDS_PATH = BASE_DIR / ".secrets" / "GSheet-Monarch-Key.json"
SESSION_DIR = BASE_DIR / ".mm"
SESSION_PATH = SESSION_DIR / "mm_session.pickle"

ACCOUNTS_WS = "Accounts"
TXNS_WS = "Transactions"
BUDGETS_WS = "Budgets"
CONTROL_WS = "Control"

# Google Sheet: set your spreadsheet ID (from URL) or via env var GSPREAD_SHEET_ID
SPREADSHEET_ID = os.getenv("GSPREAD_SHEET_ID", "18KMvcg-z8r77Csth7zbOf_VKmyWzkhZzCT8Btdbkpag")

# ---- Runtime config (tweak here; no env required) ----
DEBUG = False                 # If True, write debug JSONs to .mm (e.g., tx_first_page.json)
ADVANCE_ON_EMPTY = True       # If True, update Control!B2 even when no new transactions were returned
BACKFILL_DAYS = 365*10        # If Control!B2 is empty, backfill this many days by default (10 years to capture all history)
FORCE_FULL_REFRESH = False    # If True, ignore Control!B2 last_run_utc and reload all data from BACKFILL_DAYS ago
FORCE_START_DATE: Optional[str] = None
#   Optional ISO date string "YYYY-MM-DD". If set, the load window always starts at this date,
#   ignoring Control!B2 for the first day of the window. Set to None to disable.
TXN_PAGE_LIMIT = 500          # Page size used for get_transactions(limit=..., offset=...)
REQUEST_TIMEOUT = 30         # MonarchMoney client timeout (seconds)
ENABLE_BUDGETS = True         # If True, fetch and sync budget data to Google Sheets 
BUDGET_MONTHS = 6             # Number of months of budget data to fetch (past/future)
# -----------------------------------------------

# Ensure the .mm directory exists
SESSION_DIR.mkdir(parents=True, exist_ok=True)

creds = Credentials.from_service_account_file(str(CREDS_PATH), scopes=SCOPES)

def _scalar(v):
    if isinstance(v, (str, int, float, bool)) or v is None:
        return v
    try:
        return json.dumps(v, default=str)
    except Exception:
        return str(v)

def _to_dict(x):
    if isinstance(x, dict):
        return {k: _scalar(v) for k, v in x.items()}
    if hasattr(x, "to_dict"):
        return {k: _scalar(v) for k, v in x.to_dict().items()}
    if hasattr(x, "dict"):
        return {k: _scalar(v) for k, v in x.dict().items()}
    if hasattr(x, "model_dump"):
        return {k: _scalar(v) for k, v in x.model_dump().items()}
    if dataclasses.is_dataclass(x):
        return {k: _scalar(v) for k, v in dataclasses.asdict(x).items()}
    if hasattr(x, "__dict__"):
        return {k: _scalar(v) for k, v in vars(x).items() if not k.startswith("_")}
    return {"value": _scalar(x)}

def _ensure_ws(gc: gspread.Client, sheet_id: str, title: str) -> gspread.Worksheet:
    sh = gc.open_by_key(sheet_id)
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows="1000", cols="26")

def _account_headers_rows(records: list[dict]):
    """
    Generate headers and rows for accounts with AccountType in column 2.
    """
    if not records:
        return [], []
    
    # Get all unique keys from records  
    all_keys = {k for r in records for k in r.keys()}
    
    # Start with AccountType in position 2, then add remaining columns
    headers = []
    
    # Add columns in specific order for accounts
    priority_columns = ["id", "TypeDisplay", "AccountType", "displayName", "InstitutionName", "currentBalance", "displayBalance"]  # InstitutionName after displayName
    
    for col in priority_columns:
        if col in all_keys:
            headers.append(col)
            all_keys.discard(col)
    
    # Add remaining columns with custom sort to put type before subtype
    remaining_keys = sorted(all_keys)
    
    # Handle type/subtype ordering - put type before subtype
    if "type" in remaining_keys and "subtype" in remaining_keys:
        remaining_keys.remove("type")
        remaining_keys.remove("subtype")
        # Re-sort without type/subtype, then add them in correct order
        other_keys = sorted([k for k in remaining_keys])
        # Find where to insert type/subtype alphabetically
        insert_pos = 0
        for i, key in enumerate(other_keys):
            if key > "type":
                insert_pos = i
                break
        else:
            insert_pos = len(other_keys)
        
        # Insert type and subtype in that order
        other_keys.insert(insert_pos, "type")
        other_keys.insert(insert_pos + 1, "subtype")
        headers.extend(other_keys)
    else:
        headers.extend(remaining_keys)
    
    # Sort records by TypeDisplay, AccountType, then displayName
    records_sorted = sorted(records, key=lambda x: (
        x.get('TypeDisplay', ''), 
        x.get('AccountType', ''), 
        x.get('displayName', '')
    ))
    
    rows = [[r.get(h, "") for h in headers] for r in records_sorted]
    return headers, rows

def _headers_rows(records: list[dict]):
    if not records:
        return [], []
    
    # Define the desired column order, with timestamps at the end
    # Exclude redundant empty columns: accountDisplayName, accountId
    base_columns = [
        "__typename", 
        # Replace "account" with its breakout (skip redundant accountDisplayName, accountId)
        "AccID", "AccDispName", "AccType",
        "amount", "attachments",
        # Replace "category" with its breakout 
        "CatID", "CatDispName", "CatType",
        "date", "hideFromReports", "id", "isRecurring", "isSplitTransaction",
        # Replace "merchant" with its breakout
        "MrchntID", "MrchntDispName", "MrchntTranCount", "MrchntType", 
        "needsReview", "notes", "pending", "plaidName", "reviewStatus",
        # Keep original "tags" AND add "TagsCSL" right after
        "tags", "TagsCSL",
        # Timestamps at the end, before our metadata
        "createdAt", "updatedAt",
        # Our metadata last
        "loadedAtUtc"
    ]
    
    # Get all unique keys from records
    all_keys = {k for r in records for k in r.keys()}
    
    # Remove the redundant empty columns
    all_keys.discard("accountDisplayName")
    all_keys.discard("accountId")
    
    # Start with base columns that exist in the data
    headers = [col for col in base_columns if col in all_keys]
    
    # Add any remaining columns that aren't in our base list (for flexibility)
    remaining = all_keys - set(headers)
    headers.extend(sorted(remaining))
    
    rows = [[r.get(h, "") for h in headers] for r in records]
    return headers, rows

def _parse_iso(s: str) -> datetime | None:
    try:
        # Accept both with/without timezone
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def _find_txn_date_key(sample: dict) -> str | None:
    candidates = [
        "date", "transDate", "transactionDate", "postedDate", "datePosted",
        "madeOn", "createdAt", "activityDate"
    ]
    for k in candidates:
        if k in sample:
            return k
    # Try to find something that looks like a date
    for k, v in sample.items():
        if isinstance(v, str) and (v[:4].isdigit() and "-" in v):
            return k
    return None

def _as_dict(obj):
    if isinstance(obj, dict):
        return obj
    if obj is None:
        return None
    if hasattr(obj, "model_dump"):
        try: return obj.model_dump()
        except Exception: pass
    if hasattr(obj, "dict"):
        try: return obj.dict()
        except Exception: pass
    if dataclasses.is_dataclass(obj):
        try: return dataclasses.asdict(obj)
        except Exception: pass
    if hasattr(obj, "__dict__"):
        try: return vars(obj)
        except Exception: pass
    return None

def _get_field(obj, key):
    d = _as_dict(obj)
    if d is not None and key in d:
        return d[key]
    if hasattr(obj, key):
        try: return getattr(obj, key)
        except Exception: pass
    return None

def _save_debug(name: str, obj):
    # Write only when DEBUG is enabled
    if not DEBUG:
        return
    try:
        p = SESSION_DIR / f"{name}.json"
        text = json.dumps(obj, default=str)
        p.write_text(text, encoding="utf-8")
        print(f"Saved debug -> {p}")
    except Exception:
        pass

def _unwrap_transactions(obj):
    # Returns a list of transaction-like items or [].
    if obj is None:
        return []

    if isinstance(obj, list):
        return obj

    d = _as_dict(obj)
    if d is None:
        return []

    # Common wrappers
    data = _get_field(d, "data")
    if data is not None and data is not d:
        return _unwrap_transactions(data)

    for k in ("transactions", "allTransactions", "transactionsForAccount",
              "transactionsByAccount", "getTransactions"):
        part = _get_field(d, k)
        if part is not None:
            return _unwrap_transactions(part)

    # NEW: GraphQL list container using "results"
    results = _get_field(d, "results")
    if isinstance(results, list):
        return results

    # Connection patterns
    nodes = _get_field(d, "nodes")
    if nodes is not None:
        return _unwrap_transactions(nodes)

    edges = _get_field(d, "edges")
    if edges is not None:
        out = []
        for e in (edges or []):
            node = _get_field(e, "node")
            out.append(node if node is not None else e)
        return out

    items = _get_field(d, "items")
    if items is not None:
        return _unwrap_transactions(items)

    return []

def _txn_account_id(t: dict) -> str | None:
    for k in ("accountId", "account_id", "accountUuid"):
        v = t.get(k)
        if isinstance(v, str) and v:
            return v
        if isinstance(v, dict):
            for kk in ("id", "accountId", "entityId", "uid"):
                if v.get(kk):
                    return v[kk]
    v = t.get("account")
    if isinstance(v, dict):
        for kk in ("id", "accountId", "entityId", "uid"):
            if v.get(kk):
                return v[kk]
    return None

def _extract_connection(d: dict):
    """
    Find a GraphQL connection dict and return (items, has_next, end_cursor).
    Looks for keys like allTransactions/transactions/... then edges/nodes + pageInfo.
    """
    if not isinstance(d, dict):
        return None, False, None
    # candidates that typically hold the connection
    for top_key in ("allTransactions", "transactions", "transactionsForAccount",
                    "transactionsByAccount", "getTransactions"):
        conn = d.get(top_key)
        if isinstance(conn, dict):
            items = []
            if isinstance(conn.get("edges"), list):
                items = [(e.get("node", e)) for e in conn["edges"]]
            elif isinstance(conn.get("nodes"), list):
                items = conn["nodes"]
            elif isinstance(conn.get("items"), list):
                items = conn["items"]
            page = conn.get("pageInfo") or conn.get("page_info") or {}
            has_next = bool(page.get("hasNextPage") or page.get("has_next_page"))
            end_cursor = page.get("endCursor") or page.get("end_cursor") or page.get("cursor")
            return items, has_next, end_cursor
    # If the top-level IS the connection
    if any(k in d for k in ("edges", "nodes", "items", "pageInfo", "page_info")):
        items = []
        if isinstance(d.get("edges"), list):
            items = [(e.get("node", e)) for e in d["edges"]]
        elif isinstance(d.get("nodes"), list):
            items = d["nodes"]
        elif isinstance(d.get("items"), list):
            items = d["items"]
        page = d.get("pageInfo") or d.get("page_info") or {}
        has_next = bool(page.get("hasNextPage") or page.get("has_next_page"))
        end_cursor = page.get("endCursor") or page.get("end_cursor") or page.get("cursor")
        return items, has_next, end_cursor
    return None, False, None

async def _fetch_all_transactions(mm: MonarchMoney, accounts_list: list[dict], start_dt: datetime, end_dt: datetime):
    """
    Production: call the concrete method available in your client:
    get_transactions(limit, offset, start_date, end_date, ...), paginate by offset.
    """
    start_s = start_dt.date().isoformat()
    end_s = end_dt.date().isoformat()

    all_items: list = []
    limit = TXN_PAGE_LIMIT
    offset = 0
    page = 0

    while True:
        page += 1
        try:
            res = await mm.get_transactions(limit=limit, offset=offset, start_date=start_s, end_date=end_s)
        except TypeError:
            # Some versions may not accept offset when 0; retry without offset only on first page.
            if offset == 0:
                res = await mm.get_transactions(limit=limit, start_date=start_s, end_date=end_s)
            else:
                raise

        # Save the first page (opt-in) for troubleshooting
        if page == 1:
            _save_debug("tx_first_page", _as_dict(res) or res)

        items = _unwrap_transactions(res)
        # Fallback for { transactions: [...] } or { data: { transactions: [...] } }
        if not items and isinstance(res, dict):
            maybe = res.get("transactions") or (res.get("data") or {}).get("transactions")
            if isinstance(maybe, list):
                items = maybe

        count = len(items or [])
        if count:
            all_items.extend(items)
            print(f"Fetched page {page}: {count} transactions (offset {offset}).")
        else:
            print(f"Fetched page {page}: 0 transactions; stopping.")
            break

        if count < limit:
            break
        offset += limit

    return all_items

def _format_timestamp(ts_str: str) -> str:
    """Convert ISO timestamp to Google Sheets friendly format."""
    if not ts_str:
        return ""
    try:
        # Parse ISO format and convert to simpler format for Google Sheets
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        # Format as YYYY-MM-DD HH:MM:SS (Google Sheets can handle this easily)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ts_str  # Return original if parsing fails

def _format_date(date_str: str) -> str:
    """Convert date string to format that Google Sheets will recognize as a date."""
    if not date_str:
        return ""
    try:
        # Handle various date formats and convert to a format Google Sheets recognizes as date
        if 'T' in date_str:  # ISO datetime format
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            # Return as DATE() function for Google Sheets
            return f"=DATE({dt.year},{dt.month},{dt.day})"
        else:  # Assume it's already a date string
            # Try parsing as date
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            # Return as DATE() function for Google Sheets
            return f"=DATE({dt.year},{dt.month},{dt.day})"
    except Exception:
        try:
            # Fallback: try different common date formats
            for fmt in ["%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d", "%m-%d-%Y", "%d-%m-%Y"]:
                dt = datetime.strptime(date_str, fmt)
                # Return as DATE() function for Google Sheets
                return f"=DATE({dt.year},{dt.month},{dt.day})"
        except Exception:
            pass
        return date_str  # Return original if all parsing fails

def _extract_nested_fields(td: dict) -> dict:
    """
    Extract nested JSON structures into separate columns and remove original complex columns:
    - Replace "account" with: AccID, AccDispName, AccType
    - Replace "category" with: CatID, CatDispName, CatType  
    - Replace "merchant" with: MrchntID, MrchntDispName, MrchntTranCount, MrchntType
    - Keep "tags" and add: TagsCSL (comma-separated list of tag names)
    """
    # Extract Account fields and remove original
    account = td.get("account", {})
    if isinstance(account, str):
        try:
            account = json.loads(account)
        except:
            account = {}
    
    td["AccID"] = account.get("id", "")
    td["AccDispName"] = account.get("displayName", "")
    td["AccType"] = account.get("__typename", "")
    # Remove original complex column
    td.pop("account", None)
    
    # Extract Category fields and remove original
    category = td.get("category", {})
    if isinstance(category, str):
        try:
            category = json.loads(category)
        except:
            category = {}
    
    td["CatID"] = category.get("id", "")
    td["CatDispName"] = category.get("name", "")
    td["CatType"] = category.get("__typename", "")
    # Remove original complex column
    td.pop("category", None)
    
    # Extract Merchant fields and remove original
    merchant = td.get("merchant", {})
    if isinstance(merchant, str):
        try:
            merchant = json.loads(merchant)
        except:
            merchant = {}
    
    td["MrchntID"] = merchant.get("id", "")
    td["MrchntDispName"] = merchant.get("name", "")
    td["MrchntTranCount"] = merchant.get("transactionsCount", "")
    td["MrchntType"] = merchant.get("__typename", "")
    # Remove original complex column
    td.pop("merchant", None)
    
    # Extract Tags as comma-separated list (KEEP original tags column)
    tags = td.get("tags", [])
    if isinstance(tags, str):
        try:
            tags = json.loads(tags)
        except:
            tags = []
    
    tag_names = []
    if isinstance(tags, list):
        for tag in tags:
            if isinstance(tag, dict):
                name = tag.get("name", "")
                if name:
                    tag_names.append(name)
    
    td["TagsCSL"] = ", ".join(tag_names)
    # Keep original "tags" column as requested
    
    # Format timestamp columns for Google Sheets
    if "createdAt" in td:
        td["createdAt"] = _format_timestamp(td["createdAt"])
    if "updatedAt" in td:
        td["updatedAt"] = _format_timestamp(td["updatedAt"])
    if "loadedAtUtc" in td:
        td["loadedAtUtc"] = _format_timestamp(td["loadedAtUtc"])
    
    # Format date column for Google Sheets (ISO format YYYY-MM-DD)
    if "date" in td:
        td["date"] = _format_date(td["date"])
    
    # Process all potential dollar amount fields - strip $ symbol and convert to decimal
    dollar_fields = [
        "amount", "balance", "availableBalance", "currentBalance", "clearedBalance",
        "value", "price", "cost", "fee", "total", "subtotal", "tax", 
        "interestAmount", "principalAmount", "minimumPayment", "creditLimit",
        "availableCredit", "accountBalance", "runningBalance"
    ]
    
    for field in dollar_fields:
        if field in td:
            amount_str = str(td[field])
            # Remove $ symbol, commas, and any other currency formatting
            cleaned_amount = amount_str.replace("$", "").replace(",", "").replace("(", "-").replace(")", "").strip()
            try:
                # Convert to float for proper numeric handling in spreadsheet
                td[field] = float(cleaned_amount) if cleaned_amount else 0.0
            except (ValueError, TypeError):
                # If conversion fails, keep original but log it
                print(f"Warning: Could not convert {field} '{amount_str}' to number")
                td[field] = cleaned_amount
    
    return td

def _process_budget_data(budget_response: dict) -> list[dict]:
    """
    Process budget data from Monarch Money API and flatten for Google Sheets.
    Returns a list of budget records with one row per category per month.
    """
    budget_records = []
    
    # Handle case where API returns an error string instead of dict
    if not isinstance(budget_response, dict):
        print(f"Budget API returned unexpected data type: {type(budget_response)}")
        print(f"Response content: {budget_response}")
        return budget_records
    
    if 'budgetData' not in budget_response:
        print("No budgetData in response:")
        print(f"Available keys: {list(budget_response.keys()) if isinstance(budget_response, dict) else 'Not a dict'}")
        return budget_records
    
    budget_data = budget_response['budgetData']
    
    # Get category and category group mappings
    try:
        category_groups = {cg['id']: cg for cg in budget_response.get('categoryGroups', [])}
        print(f"Found {len(category_groups)} category groups")
    except Exception as e:
        print(f"Error processing category groups: {e}")
        print(f"categoryGroups type: {type(budget_response.get('categoryGroups'))}")
        print(f"categoryGroups content: {budget_response.get('categoryGroups')}")
        return budget_records
        
    categories = {}
    try:
        for cg_id, cg in category_groups.items():
            for cat in cg.get('categories', []):
                categories[cat['id']] = cat
                cat['categoryGroup'] = cg  # Add parent group reference
        print(f"Found {len(categories)} categories")
    except Exception as e:
        print(f"Error processing categories: {e}")
        return budget_records
    
    # Process category-level budgets
    try:
        monthly_by_category = budget_data.get('monthlyAmountsByCategory', [])
        print(f"Processing {len(monthly_by_category)} category budgets")
        for i, cat_budget in enumerate(monthly_by_category):
            try:
                category_id = cat_budget.get('category', {}).get('id')
                category = categories.get(category_id, {})
                category_group = category.get('categoryGroup', {})
                
                # Determine debit/credit based on category group type
                group_type = category_group.get('type', '')
                is_income = group_type == 'income'
                debit_credit_flag = 'Credit' if is_income else 'Debit'
                
                # Get rollover period details
                rollover_period = category.get('rolloverPeriod') or {}
                
                for monthly in cat_budget.get('monthlyAmounts', []):
                    record = {
                        'RecordType': 'Category',
                        'CategoryGroupId': category_group.get('id', ''),
                        'CategoryGroupName': category_group.get('name', ''),
                        'CategoryGroupType': group_type,
                        'DebitCreditFlag': debit_credit_flag,
                        'IsIncome': is_income,
                        'CategoryId': category_id or '',
                        'CategoryName': category.get('name', ''),
                        'CategoryIcon': category.get('icon', ''),
                        'IsSystemCategory': category.get('isSystemCategory', False),
                        'ExcludeFromBudget': category.get('excludeFromBudget', False),
                        'CategoryUpdatedAt': category.get('updatedAt', ''),
                        'Month': monthly.get('month', ''),
                        'PlannedCashFlow': float(monthly.get('plannedCashFlowAmount', 0) or 0),
                        'PlannedSetAside': float(monthly.get('plannedSetAsideAmount', 0) or 0),
                        'ActualAmount': float(monthly.get('actualAmount', 0) or 0),
                        'CumulativeActualAmount': float(monthly.get('cumulativeActualAmount', 0) or 0),
                        'RemainingAmount': float(monthly.get('remainingAmount', 0) or 0),
                        'RolloverAmount': float(monthly.get('previousMonthRolloverAmount', 0) or 0),
                        'RolloverType': monthly.get('rolloverType', ''),
                        'RolloverPeriodId': rollover_period.get('id', ''),
                        'RolloverStartMonth': rollover_period.get('startMonth', ''),
                        'RolloverEndMonth': rollover_period.get('endMonth', ''),
                        'RolloverStartingBalance': float(rollover_period.get('startingBalance', 0) or 0),
                        'RolloverTargetAmount': float(rollover_period.get('targetAmount', 0) or 0),
                        'RolloverFrequency': rollover_period.get('frequency', ''),
                        'BudgetVariability': category.get('budgetVariability', ''),
                        'CategoryOrder': category.get('order', 0),
                        'GroupOrder': category_group.get('order', 0)
                    }
                    budget_records.append(record)
            except Exception as e:
                print(f"Error processing category budget {i}: {e}")
                continue
        print(f"Processed category budgets, total records so far: {len(budget_records)}")
    except Exception as e:
        print(f"Error in category budget processing: {e}")
        return budget_records
    
    # Process category group-level budgets
    try:
        monthly_by_group = budget_data.get('monthlyAmountsByCategoryGroup', [])
        print(f"Processing {len(monthly_by_group)} category group budgets")
        for i, group_budget in enumerate(monthly_by_group):
            try:
                group_id = group_budget.get('categoryGroup', {}).get('id')
                category_group = category_groups.get(group_id, {})
                
                # Determine debit/credit based on category group type
                group_type = category_group.get('type', '')
                is_income = group_type == 'income'
                debit_credit_flag = 'Credit' if is_income else 'Debit'
                
                # Get rollover period details for group
                group_rollover_period = category_group.get('rolloverPeriod') or {}
                
                for monthly in group_budget.get('monthlyAmounts', []):
                    record = {
                        'RecordType': 'CategoryGroup',
                        'CategoryGroupId': group_id or '',
                        'CategoryGroupName': category_group.get('name', ''),
                        'CategoryGroupType': group_type,
                        'DebitCreditFlag': debit_credit_flag,
                        'IsIncome': is_income,
                        'CategoryId': '',
                        'CategoryName': '',
                        'CategoryIcon': '',
                        'IsSystemCategory': False,
                        'ExcludeFromBudget': False,
                        'CategoryUpdatedAt': category_group.get('updatedAt', ''),
                        'GroupLevelBudgetingEnabled': category_group.get('groupLevelBudgetingEnabled', False),
                        'Month': monthly.get('month', ''),
                        'PlannedCashFlow': float(monthly.get('plannedCashFlowAmount', 0) or 0),
                        'PlannedSetAside': 0.0,  # Group level doesn't have set aside
                        'ActualAmount': float(monthly.get('actualAmount', 0) or 0),
                        'CumulativeActualAmount': float(monthly.get('cumulativeActualAmount', 0) or 0),
                        'RemainingAmount': float(monthly.get('remainingAmount', 0) or 0),
                        'RolloverAmount': float(monthly.get('previousMonthRolloverAmount', 0) or 0),
                        'RolloverType': monthly.get('rolloverType', ''),
                        'RolloverPeriodId': group_rollover_period.get('id', ''),
                        'RolloverStartMonth': group_rollover_period.get('startMonth', ''),
                        'RolloverEndMonth': group_rollover_period.get('endMonth', ''),
                        'RolloverStartingBalance': float(group_rollover_period.get('startingBalance', 0) or 0),
                        'RolloverTargetAmount': float(group_rollover_period.get('targetAmount', 0) or 0),
                        'RolloverFrequency': group_rollover_period.get('frequency', ''),
                        'BudgetVariability': category_group.get('budgetVariability', ''),
                        'CategoryOrder': 0,
                        'GroupOrder': category_group.get('order', 0)
                    }
                    budget_records.append(record)
            except Exception as e:
                print(f"Error processing category group budget {i}: {e}")
                continue
        print(f"Processed group budgets, total records so far: {len(budget_records)}")
    except Exception as e:
        print(f"Error in category group budget processing: {e}")
        return budget_records
    
    # Process flexible expense budgets
    try:
        flex_expenses = budget_data.get('monthlyAmountsForFlexExpense', [])
        print(f"Processing flexible expense budgets")
        if isinstance(flex_expenses, list):
            print(f"Processing {len(flex_expenses)} flexible expense budgets")
            for i, flex_expense in enumerate(flex_expenses):
                try:
                    for monthly in flex_expense.get('monthlyAmounts', []):
                        record = {
                            'RecordType': 'FlexibleExpense',
                            'CategoryGroupId': '',
                            'CategoryGroupName': 'Flexible Expenses',
                            'CategoryGroupType': 'expense',
                            'DebitCreditFlag': 'Debit',
                            'IsIncome': False,
                            'CategoryId': '',
                            'CategoryName': '',
                            'CategoryIcon': '',
                            'IsSystemCategory': False,
                            'ExcludeFromBudget': False,
                            'CategoryUpdatedAt': '',
                            'GroupLevelBudgetingEnabled': False,
                            'Month': monthly.get('month', ''),
                            'PlannedCashFlow': float(monthly.get('plannedCashFlowAmount', 0) or 0),
                            'PlannedSetAside': 0.0,
                            'ActualAmount': float(monthly.get('actualAmount', 0) or 0),
                            'CumulativeActualAmount': float(monthly.get('cumulativeActualAmount', 0) or 0),
                            'RemainingAmount': float(monthly.get('remainingAmount', 0) or 0),
                            'RolloverAmount': float(monthly.get('previousMonthRolloverAmount', 0) or 0),
                            'RolloverType': monthly.get('rolloverType', ''),
                            'RolloverPeriodId': '',
                            'RolloverStartMonth': '',
                            'RolloverEndMonth': '',
                            'RolloverStartingBalance': 0.0,
                            'RolloverTargetAmount': 0.0,
                            'RolloverFrequency': '',
                            'BudgetVariability': flex_expense.get('budgetVariability', ''),
                            'CategoryOrder': 0,
                            'GroupOrder': 999  # Put at end
                        }
                        budget_records.append(record)
                except Exception as e:
                    print(f"Error processing flexible expense {i}: {e}")
                    continue
        elif isinstance(flex_expenses, dict):
            # Handle single flex expense object
            print("Processing single flexible expense budget")
            try:
                for monthly in flex_expenses.get('monthlyAmounts', []):
                    record = {
                        'RecordType': 'FlexibleExpense',
                        'CategoryGroupId': '',
                        'CategoryGroupName': 'Flexible Expenses',
                        'CategoryGroupType': 'expense',
                        'DebitCreditFlag': 'Debit',
                        'IsIncome': False,
                        'CategoryId': '',
                        'CategoryName': '',
                        'CategoryIcon': '',
                        'IsSystemCategory': False,
                        'ExcludeFromBudget': False,
                        'CategoryUpdatedAt': '',
                        'GroupLevelBudgetingEnabled': False,
                        'Month': monthly.get('month', ''),
                        'PlannedCashFlow': float(monthly.get('plannedCashFlowAmount', 0) or 0),
                        'PlannedSetAside': 0.0,
                        'ActualAmount': float(monthly.get('actualAmount', 0) or 0),
                        'CumulativeActualAmount': float(monthly.get('cumulativeActualAmount', 0) or 0),
                        'RemainingAmount': float(monthly.get('remainingAmount', 0) or 0),
                        'RolloverAmount': float(monthly.get('previousMonthRolloverAmount', 0) or 0),
                        'RolloverType': monthly.get('rolloverType', ''),
                        'RolloverPeriodId': '',
                        'RolloverStartMonth': '',
                        'RolloverEndMonth': '',
                        'RolloverStartingBalance': 0.0,
                        'RolloverTargetAmount': 0.0,
                        'RolloverFrequency': '',
                        'BudgetVariability': flex_expenses.get('budgetVariability', ''),
                        'CategoryOrder': 0,
                        'GroupOrder': 999  # Put at end
                    }
                    budget_records.append(record)
            except Exception as e:
                print(f"Error processing single flexible expense: {e}")
        else:
            print(f"Unexpected flexible expense type: {type(flex_expenses)}")
        print(f"Processed flexible expenses, total records so far: {len(budget_records)}")
    except Exception as e:
        print(f"Error in flexible expense processing: {e}")
        return budget_records
    
    # Process monthly totals
    for monthly_total in budget_data.get('totalsByMonth', []):
        month = monthly_total.get('month', '')
        
        # Add income total
        income = monthly_total.get('totalIncome', {})
        record = {
            'RecordType': 'TotalIncome',
            'CategoryGroupId': '',
            'CategoryGroupName': 'Income',
            'CategoryGroupType': 'income',
            'DebitCreditFlag': 'Credit',
            'IsIncome': True,
            'CategoryId': '',
            'CategoryName': 'Total Income',
            'CategoryIcon': '',
            'IsSystemCategory': True,
            'ExcludeFromBudget': False,
            'CategoryUpdatedAt': '',
            'GroupLevelBudgetingEnabled': False,
            'Month': month,
            'PlannedCashFlow': float(income.get('plannedAmount', 0) or 0),
            'PlannedSetAside': 0.0,
            'ActualAmount': float(income.get('actualAmount', 0) or 0),
            'CumulativeActualAmount': 0.0,  # Totals don't have cumulative
            'RemainingAmount': float(income.get('remainingAmount', 0) or 0),
            'RolloverAmount': float(income.get('previousMonthRolloverAmount', 0) or 0),
            'RolloverType': '',
            'RolloverPeriodId': '',
            'RolloverStartMonth': '',
            'RolloverEndMonth': '',
            'RolloverStartingBalance': 0.0,
            'RolloverTargetAmount': 0.0,
            'RolloverFrequency': '',
            'BudgetVariability': '',
            'CategoryOrder': 0,
            'GroupOrder': -1  # Put at top
        }
        budget_records.append(record)
        
        # Add expense totals
        expense_types = [
            ('TotalExpenses', 'totalExpenses', 'Total Expenses'),
            ('FixedExpenses', 'totalFixedExpenses', 'Fixed Expenses'),
            ('FlexibleExpenses', 'totalFlexibleExpenses', 'Flexible Expenses'),
            ('NonMonthlyExpenses', 'totalNonMonthlyExpenses', 'Non-Monthly Expenses')
        ]
        
        for record_type, data_key, display_name in expense_types:
            expense_data = monthly_total.get(data_key, {})
            record = {
                'RecordType': record_type,
                'CategoryGroupId': '',
                'CategoryGroupName': 'Expenses',
                'CategoryGroupType': 'expense',
                'DebitCreditFlag': 'Debit',
                'IsIncome': False,
                'CategoryId': '',
                'CategoryName': display_name,
                'CategoryIcon': '',
                'IsSystemCategory': True,
                'ExcludeFromBudget': False,
                'CategoryUpdatedAt': '',
                'GroupLevelBudgetingEnabled': False,
                'Month': month,
                'PlannedCashFlow': float(expense_data.get('plannedAmount', 0) or 0),
                'PlannedSetAside': 0.0,
                'ActualAmount': float(expense_data.get('actualAmount', 0) or 0),
                'CumulativeActualAmount': 0.0,  # Totals don't have cumulative
                'RemainingAmount': float(expense_data.get('remainingAmount', 0) or 0),
                'RolloverAmount': float(expense_data.get('previousMonthRolloverAmount', 0) or 0),
                'RolloverType': '',
                'RolloverPeriodId': '',
                'RolloverStartMonth': '',
                'RolloverEndMonth': '',
                'RolloverStartingBalance': 0.0,
                'RolloverTargetAmount': 0.0,
                'RolloverFrequency': '',
                'BudgetVariability': '',
                'CategoryOrder': 0,
                'GroupOrder': 998  # Put near end
            }
            budget_records.append(record)
    
    # Add metadata timestamp
    run_ts = datetime.now(timezone.utc).isoformat()
    for record in budget_records:
        record['LoadedAtUtc'] = run_ts
    
    return budget_records

def _budget_headers_rows(records: list[dict]):
    """
    Generate headers and rows for budget data with logical column ordering.
    """
    if not records:
        return [], []
    
    # Define the desired column order for budget data
    priority_columns = [
        "RecordType", "CategoryGroupName", "CategoryGroupType", "DebitCreditFlag", "IsIncome",
        "CategoryName", "CategoryIcon", "IsSystemCategory", "ExcludeFromBudget", 
        "Month", "PlannedCashFlow", "ActualAmount", "CumulativeActualAmount", "RemainingAmount", 
        "PlannedSetAside", "RolloverAmount", "RolloverType", "RolloverPeriodId",
        "RolloverStartMonth", "RolloverEndMonth", "RolloverStartingBalance", "RolloverTargetAmount", "RolloverFrequency",
        "BudgetVariability", "GroupLevelBudgetingEnabled", "CategoryUpdatedAt",
        "CategoryGroupId", "CategoryId", "GroupOrder", "CategoryOrder", "LoadedAtUtc"
    ]
    
    # Get all unique keys from records
    all_keys = {k for r in records for k in r.keys()}
    
    # Start with priority columns that exist in the data
    headers = [col for col in priority_columns if col in all_keys]
    
    # Add any remaining columns that aren't in our priority list
    remaining = all_keys - set(headers)
    headers.extend(sorted(remaining))
    
    # Sort records by CategoryGroupName, CategoryName, then Month
    records_sorted = sorted(records, key=lambda x: (
        x.get('CategoryGroupName', ''), 
        x.get('CategoryName', ''), 
        x.get('Month', '')
    ))
    
    rows = [[r.get(h, "") for h in headers] for r in records_sorted]
    return headers, rows

def parse_arguments():
    """Parse command line arguments to override default configuration."""
    parser = argparse.ArgumentParser(description="Monarch Money to Google Sheets sync")
    
    parser.add_argument("--debug", action="store_true", 
                       help="Enable debug mode (saves debug JSONs)")
    parser.add_argument("--force-full-refresh", action="store_true",
                       help="Force full refresh, ignore last_run_utc in Control sheet")
    parser.add_argument("--force-start-date", type=str, metavar="YYYY-MM-DD",
                       help="Force start date (YYYY-MM-DD format)")
    parser.add_argument("--backfill-days", type=int, metavar="N",
                       help="Number of days to backfill (default: 1095)")
    parser.add_argument("--page-limit", type=int, metavar="N",
                       help="Transaction page limit (default: 500)")
    parser.add_argument("--no-advance-empty", action="store_true",
                       help="Don't update Control sheet when no transactions found")
    parser.add_argument("--timeout", type=int, metavar="MS",
                       help="Request timeout in milliseconds (default: 3000)")
    parser.add_argument("--spreadsheet-id", type=str,
                       help="Google Sheets spreadsheet ID (overrides env var)")
    parser.add_argument("--enable-budgets", action="store_true",
                       help="Enable budget data sync (default: enabled)")
    parser.add_argument("--disable-budgets", action="store_true",
                       help="Disable budget data sync")
    parser.add_argument("--budget-months", type=int, metavar="N",
                       help="Number of months of budget data to fetch (default: 3)")
    
    return parser.parse_args()

def apply_arguments(args):
    """Apply command line arguments to global configuration variables."""
    global DEBUG, FORCE_FULL_REFRESH, FORCE_START_DATE, BACKFILL_DAYS
    global TXN_PAGE_LIMIT, ADVANCE_ON_EMPTY, REQUEST_TIMEOUT, SPREADSHEET_ID
    global ENABLE_BUDGETS, BUDGET_MONTHS
    
    if args.debug:
        DEBUG = True
        print("Debug mode enabled")
    
    if args.force_full_refresh:
        FORCE_FULL_REFRESH = True
        print("Force full refresh enabled")
    
    if args.force_start_date:
        FORCE_START_DATE = args.force_start_date
        print(f"Force start date set to: {FORCE_START_DATE}")
    
    if args.backfill_days:
        BACKFILL_DAYS = args.backfill_days
        print(f"Backfill days set to: {BACKFILL_DAYS}")
    
    if args.page_limit:
        TXN_PAGE_LIMIT = args.page_limit
        print(f"Page limit set to: {TXN_PAGE_LIMIT}")
    
    if args.no_advance_empty:
        ADVANCE_ON_EMPTY = False
        print("Will not advance Control sheet on empty results")
    
    if args.timeout:
        REQUEST_TIMEOUT = args.timeout
        print(f"Request timeout set to: {REQUEST_TIMEOUT}ms")
    
    if args.spreadsheet_id:
        SPREADSHEET_ID = args.spreadsheet_id
        print(f"Spreadsheet ID set to: {SPREADSHEET_ID}")
    
    if args.enable_budgets:
        ENABLE_BUDGETS = True
        print("Budget sync enabled")
    
    if args.disable_budgets:
        ENABLE_BUDGETS = False
        print("Budget sync disabled")
    
    if args.budget_months:
        BUDGET_MONTHS = args.budget_months
        print(f"Budget months set to: {BUDGET_MONTHS}")

def _process_accounts(accounts_list: list) -> list:
    """
    Process accounts to extract subtype display value as AccountType column.
    Inserts AccountType as column 2, shifting other columns right.
    """
    processed = []
    for account in accounts_list:
        acc_dict = _to_dict(account)
        
        # Extract AccountType from subtype display value
        subtype = acc_dict.get("subtype", {})
        if isinstance(subtype, str):
            try:
                subtype = json.loads(subtype)
            except:
                subtype = {}
        
        account_type = ""
        if isinstance(subtype, dict):
            account_type = subtype.get("display", "")
        
        # Insert AccountType as new column - this will be column 3 now
        acc_dict["AccountType"] = account_type
        
        # Extract TypeDisplay from type display value  
        type_obj = acc_dict.get("type", {})
        if isinstance(type_obj, str):
            try:
                type_obj = json.loads(type_obj)
            except:
                type_obj = {}
        
        type_display = ""
        if isinstance(type_obj, dict):
            type_display = type_obj.get("display", "")
        
        # Insert TypeDisplay as new column 2
        acc_dict["TypeDisplay"] = type_display
        
        # Extract InstitutionName from institution name value
        institution = acc_dict.get("institution", {})
        if isinstance(institution, str):
            try:
                institution = json.loads(institution)
            except:
                institution = {}
        
        institution_name = ""
        if isinstance(institution, dict):
            institution_name = institution.get("name", "")
        
        # Insert InstitutionName as new column 4
        acc_dict["InstitutionName"] = institution_name
        
        processed.append(acc_dict)
    
    return processed

async def main():
    gc = gspread.authorize(creds)
    mm = MonarchMoney(timeout=REQUEST_TIMEOUT)
    
    try:
        # Retry logic for Transport Error 525 (CloudFlare SSL issues)
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Session
                if SESSION_PATH.exists():
                    print(f"Loading saved session from {SESSION_PATH} ...")
                    mm.load_session()
                else:
                    print("No saved session. Starting interactive login...")
                    await mm.interactive_login()
                    mm.save_session()
                break  # Success, exit retry loop
                
            except (TransportServerError, Exception) as e:
                if "525" in str(e) and attempt < max_retries - 1:
                    print(f"Transport Error 525 (attempt {attempt + 1}/{max_retries}). Retrying in 5 seconds...")
                    if SESSION_PATH.exists():
                        SESSION_PATH.unlink()  # Remove stale session
                    await asyncio.sleep(5)
                    continue
                else:
                    if attempt == max_retries - 1:
                        print(f"Transport Error after {max_retries} attempts: {e}")
                    raise

        # Accounts -> console + sheet
        accounts = await mm.get_accounts()
        accounts_list = accounts["accounts"] if isinstance(accounts, dict) and "accounts" in accounts else accounts
        print(f"Fetched {len(accounts_list)} accounts.")
        try:
            print(json.dumps(accounts_list[:3], indent=2, default=str))
        except Exception:
            pass

        acc_norm = _process_accounts(accounts_list or [])
        acc_headers, acc_rows = _account_headers_rows(acc_norm)
        ws_acc = _ensure_ws(gc, SPREADSHEET_ID, ACCOUNTS_WS)
        if acc_rows:
            ws_acc.clear()
            # gspread v6+: pass values first, then range
            ws_acc.update([acc_headers] + acc_rows, "A1", value_input_option='USER_ENTERED')
            print(f"Wrote {len(acc_rows)} rows to '{ACCOUNTS_WS}'.")
        else:
            print("No accounts returned; Accounts sheet left unchanged.")

        # Build accountId -> displayName map for joining onto transactions
        acct_name_by_id = {}
        for a in acc_norm:
            aid = a.get("id") or a.get("accountId") or a.get("entityId") or a.get("uid")
            aname = a.get("displayName") or a.get("name") or ""
            if aid:
                acct_name_by_id[aid] = aname

        # Budgets -> console + sheet
        if ENABLE_BUDGETS:
            print("Fetching budget data...")
            try:
                # Calculate date range for budget data - use first day of months
                from calendar import monthrange
                
                today = date.today()
                current_year = today.year
                current_month = today.month
                
                # Calculate start month (go back BUDGET_MONTHS-1 months from current)
                start_month_offset = current_month - (BUDGET_MONTHS - 1)
                start_year = current_year
                while start_month_offset <= 0:
                    start_month_offset += 12
                    start_year -= 1
                
                # Calculate end month (go forward BUDGET_MONTHS months from current)
                end_month_offset = current_month + BUDGET_MONTHS
                end_year = current_year
                while end_month_offset > 12:
                    end_month_offset -= 12
                    end_year += 1
                
                # Format as first day of start month and last day of end month
                start_date_str = f"{start_year}-{start_month_offset:02d}-01"
                end_day = monthrange(end_year, end_month_offset)[1]  # Last day of month
                end_date_str = f"{end_year}-{end_month_offset:02d}-{end_day:02d}"
                
                print(f"Requesting budget data from {start_date_str} to {end_date_str}")
                
                budget_response = await mm.get_budgets(
                    start_date=start_date_str,
                    end_date=end_date_str
                    # No longer passing use_legacy_goals or use_v2_goals parameters
                    # as they were removed in the fix
                )
                
                _save_debug("budget_response", budget_response)
                print(f"Budget response type: {type(budget_response)}")
                print(f"Budget response keys: {list(budget_response.keys()) if isinstance(budget_response, dict) else 'Not a dict'}")
                
                # Check if response is valid
                if isinstance(budget_response, str):
                    print(f"Budget API returned error string: {budget_response}")
                    raise Exception(f"Budget API error: {budget_response}")
                
                budget_records = _process_budget_data(budget_response)
                print(f"Processed {len(budget_records)} budget records.")
                
                if budget_records:
                    try:
                        print("Sample budget records:")
                        print(json.dumps(budget_records[:3], indent=2, default=str))
                    except Exception:
                        pass
                    
                    budget_headers, budget_rows = _budget_headers_rows(budget_records)
                    ws_budget = _ensure_ws(gc, SPREADSHEET_ID, BUDGETS_WS)
                    ws_budget.clear()
                    ws_budget.update([budget_headers] + budget_rows, "A1", value_input_option='USER_ENTERED')
                    print(f"Wrote {len(budget_rows)} budget rows to '{BUDGETS_WS}'.")
                else:
                    print("No budget records returned; Budgets sheet left unchanged.")
                    
            except Exception as e:
                print(f"Error fetching/processing budget data: {e}")
                # Try with default dates (let API choose)
                try:
                    print("Retrying with API defaults...")
                    budget_response = await mm.get_budgets()
                    # No parameters - let API use defaults
                    _save_debug("budget_response_retry", budget_response)
                    print(f"Retry budget response type: {type(budget_response)}")
                    print(f"Retry budget response keys: {list(budget_response.keys()) if isinstance(budget_response, dict) else 'Not a dict'}")
                    
                    # Check if response is valid
                    if isinstance(budget_response, str):
                        print(f"Budget API retry returned error string: {budget_response}")
                        raise Exception(f"Budget API retry error: {budget_response}")
                    
                    budget_records = _process_budget_data(budget_response)
                    print(f"Retry processed {len(budget_records)} budget records.")
                    
                    if budget_records:
                        budget_headers, budget_rows = _budget_headers_rows(budget_records)
                        ws_budget = _ensure_ws(gc, SPREADSHEET_ID, BUDGETS_WS)
                        ws_budget.clear()
                        ws_budget.update([budget_headers] + budget_rows, "A1", value_input_option='USER_ENTERED')
                        print(f"Wrote {len(budget_rows)} budget rows to '{BUDGETS_WS}' (retry successful).")
                except Exception as retry_e:
                    print(f"Budget retry also failed: {retry_e}")
                    print("\n⚠️  BUDGET SYNC FAILED ⚠️")
                    print("Possible reasons:")
                    print("  • Your Monarch Money account doesn't have budgets set up")
                    print("  • Budget access requires Monarch Premium subscription")
                    print("  • Budget feature may not be available for your account type")
                    print("")
                    print("💡 To try budget sync anyway, use: --enable-budgets")
                    print("   Your accounts and transactions sync normally regardless.")
                # Don't fail the entire script if budget processing fails
        else:
            print("Budget sync disabled via configuration.")

        # Control sheet -> get last run
        ws_ctl = _ensure_ws(gc, SPREADSHEET_ID, CONTROL_WS)
        ctl_vals = ws_ctl.get_values("A1:B2")
        if not ctl_vals:
            ws_ctl.update([["key", "value"]], "A1:B1")
            ctl_vals = [["key", "value"]]
        last_run_utc = None
        if len(ctl_vals) >= 2 and len(ctl_vals[1]) >= 2 and ctl_vals[1][0].lower() == "last_run_utc":
            last_run_utc = _parse_iso(ctl_vals[1][1])

        # Optional forced start date from config (overrides first-day start)
        if FORCE_START_DATE:
            try:
                forced = datetime.fromisoformat(FORCE_START_DATE).date()
                last_run_utc = datetime.combine(forced, datetime.min.time(), tzinfo=timezone.utc)
                print(f"Overriding start from FORCE_START_DATE={FORCE_START_DATE}")
            except Exception:
                print(f"Warning: could not parse FORCE_START_DATE={FORCE_START_DATE}")

        # Force full refresh option - ignore Control!B2 and reload everything
        if FORCE_FULL_REFRESH:
            last_run_utc = datetime.now(timezone.utc) - timedelta(days=BACKFILL_DAYS)
            print(f"FORCE_FULL_REFRESH enabled: Loading all data from last {BACKFILL_DAYS} days")

        ws_tx = _ensure_ws(gc, SPREADSHEET_ID, TXNS_WS)
        existing_txn_values = ws_tx.get_all_values()
        existing_txn_count = max(0, (len(existing_txn_values) - 1)) if existing_txn_values else 0

        # If Control is empty, default backfill
        if not last_run_utc:
            last_run_utc = datetime.now(timezone.utc) - timedelta(days=BACKFILL_DAYS)

        # Window start at start of day (UTC)
        start_dt = datetime.combine(last_run_utc.date(), datetime.min.time(), tzinfo=timezone.utc)

        # First run: widen window if sheet is empty and start is today (unless forced)
        if existing_txn_count == 0 and start_dt.date() == datetime.now(timezone.utc).date() and not FORCE_START_DATE:
            bf_start_date = (datetime.now(timezone.utc) - timedelta(days=BACKFILL_DAYS)).date()
            start_dt = datetime.combine(bf_start_date, datetime.min.time(), tzinfo=timezone.utc)
            print(f"Initial backfill: Transactions sheet empty. Expanding window to last {BACKFILL_DAYS} days.")

        end_dt = datetime.now(timezone.utc)
        print(f"Loading transactions from {start_dt.isoformat()} to {end_dt.isoformat()}")

        # Fetch transactions
        transactions = await _fetch_all_transactions(mm, accounts_list, start_dt, end_dt)
        transactions_list = _unwrap_transactions(transactions)

        if not transactions_list:
            print("No transactions returned for the window; keeping existing rows.")
        # Normalize and enrich
        txn_norm = []
        run_ts = datetime.now(timezone.utc).isoformat()
        for t in transactions_list or []:
            td = _to_dict(t)
            aid = _txn_account_id(td) or td.get("accountId") or td.get("account_id")
            td["accountId"] = aid or ""
            td["accountDisplayName"] = acct_name_by_id.get(aid, "")
            td["loadedAtUtc"] = run_ts
            
            # Extract nested structures into separate columns
            td = _extract_nested_fields(td)
            
            txn_norm.append(td)

        # Respect global toggle for advancing Control on empty result
        if not txn_norm:
            if ADVANCE_ON_EMPTY:
                ws_ctl.update([["key", "value"], ["last_run_utc", end_dt.isoformat()]], "A1:B2")
                print(f"No transactions for window. Updated {CONTROL_WS}!B2 last_run_utc = {end_dt.isoformat()}")
            else:
                print("No transactions for window. Control last_run_utc left unchanged.")
            return

        print(f"Unwrapped {len(txn_norm)} transactions. Sample:")
        try:
            print(json.dumps(txn_norm[:3], indent=2, default=str))
        except Exception:
            pass

        # Detect date column in transactions
        date_key = _find_txn_date_key(txn_norm[0])  # safe now (txn_norm not empty)

        # Existing Transactions sheet
        ws_tx = _ensure_ws(gc, SPREADSHEET_ID, TXNS_WS)

        # Load existing TXNs as list[dict]
        existing = []
        values = ws_tx.get_all_values()
        if values:
            headers = values[0]
            for row in values[1:]:
                d = {headers[i]: row[i] if i < len(row) else "" for i in range(len(headers))}
                existing.append(d)

        # Partition: keep rows strictly before start_dt date, replace the rest
        kept = []
        if existing and date_key and date_key in existing[0]:
            for r in existing:
                v = r.get(date_key, "")
                dt = _parse_iso(v) or _parse_iso(v + "T00:00:00Z")
                if dt and dt.date() < start_dt.date():
                    # Extract nested fields from existing rows if they haven't been processed yet
                    if "AccID" not in r:  # Check if already processed
                        r = _extract_nested_fields(r)
                    kept.append(r)
        else:
            kept = []

        merged = kept + txn_norm

        # Write merged to sheet with proper date formatting
        headers, rows = _headers_rows(merged)
        ws_tx.clear()
        if headers:
            ws_tx.update([headers] + rows, "A1", value_input_option='USER_ENTERED')
        print(f"Wrote {len(rows)} transaction rows to '{TXNS_WS}' (kept {len(kept)} prior rows).")

        # Update control timestamp after successful write
        ws_ctl.update([["key", "value"], ["last_run_utc", end_dt.isoformat()]], "A1:B2")
        print(f"Updated {CONTROL_WS}!B2 last_run_utc = {end_dt.isoformat()}")

    except TransportServerError as e:
        if getattr(e, "code", None) == 401:
            print("Saved session is invalid/expired (401). Re-authenticating...")
            try:
                if SESSION_PATH.exists():
                    SESSION_PATH.unlink(missing_ok=True)
                await mm.interactive_login()
                mm.save_session()
                print("Re-login complete. Re-run the script.")
            except Exception as inner:
                print(f"Re-login failed: {inner}")
        else:
            print(f"Transport error: {e}")
    except RequireMFAException as e:
        print("MFA required:", e)
    except Exception as e:
        print("Error:", e)

try:
    loop = asyncio.get_running_loop()
except RuntimeError:
    loop = None

if __name__ == "__main__":
    # Parse and apply command line arguments
    args = parse_arguments()
    apply_arguments(args)

if loop and loop.is_running():
    task = loop.create_task(main())
else:
    asyncio.run(main())