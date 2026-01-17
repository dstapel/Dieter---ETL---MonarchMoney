# filepath: /c:/Users/dstap/Dropbox/Dieter Files/Dieter Codes/_DataFlow/Finance/MonarchMoney/MonarchMoneyMain-v2.py
# %%
import asyncio
import os
import json
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
CONTROL_WS = "Control"

# Google Sheet: set your spreadsheet ID (from URL) or via env var GSPREAD_SHEET_ID
SPREADSHEET_ID = os.getenv("GSPREAD_SHEET_ID", "18KMvcg-z8r77Csth7zbOf_VKmyWzkhZzCT8Btdbkpag")

# ---- Runtime config (tweak here; no env required) ----
DEBUG = False                 # If True, write debug JSONs to .mm (e.g., tx_first_page.json)
ADVANCE_ON_EMPTY = True       # If True, update Control!B2 even when no new transactions were returned
BACKFILL_DAYS = 365*3           # If Control!B2 is empty, backfill this many days by default
FORCE_START_DATE: Optional[str] = None
#   Optional ISO date string "YYYY-MM-DD". If set, the load window always starts at this date,
#   ignoring Control!B2 for the first day of the window. Set to None to disable.
TXN_PAGE_LIMIT = 500          # Page size used for get_transactions(limit=..., offset=...)
REQUEST_TIMEOUT = 3000        # MonarchMoney client timeout (milliseconds)
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
    
    return td

async def main():
    gc = gspread.authorize(creds)
    mm = MonarchMoney(timeout=REQUEST_TIMEOUT)
    try:
        # Session
        if SESSION_PATH.exists():
            print(f"Loading saved session from {SESSION_PATH} ...")
            mm.load_session()
        else:
            print("No saved session. Starting interactive login...")
            await mm.interactive_login()
            mm.save_session()

        # Accounts -> console + sheet
        accounts = await mm.get_accounts()
        accounts_list = accounts["accounts"] if isinstance(accounts, dict) and "accounts" in accounts else accounts
        print(f"Fetched {len(accounts_list)} accounts.")
        try:
            print(json.dumps(accounts_list[:3], indent=2, default=str))
        except Exception:
            pass

        acc_norm = [_to_dict(a) for a in (accounts_list or [])]
        acc_headers, acc_rows = _headers_rows(acc_norm)
        ws_acc = _ensure_ws(gc, SPREADSHEET_ID, ACCOUNTS_WS)
        if acc_rows:
            ws_acc.clear()
            # gspread v6+: pass values first, then range
            ws_acc.update([acc_headers] + acc_rows, "A1")
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

        # Write merged to sheet
        headers, rows = _headers_rows(merged)
        ws_tx.clear()
        if headers:
            ws_tx.update([headers] + rows, "A1")
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

if loop and loop.is_running():
    task = loop.create_task(main())
else:
    asyncio.run(main())