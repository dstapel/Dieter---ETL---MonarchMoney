# Column Structure Change Log

## Purpose
Track all changes to the Google Sheets column structure to maintain data integrity.

---

## December 24, 2025

### Initial Documentation
- Created this change log to track column modifications
- Documented current 31-column structure in CRITICAL-START-HERE.md

### Recent Changes Made Today
1. **Amount Field Processing** - Added code to strip `$` symbols and convert to decimal
   - **Impact**: Data format change, but same column position (column 6)
   - **Status**: ✅ Approved by user

2. **Nested Field Extraction** - Added extraction of account/category/merchant sub-fields
   - **Impact**: Added new columns (AccID, AccDispName, etc.) while preserving originals  
   - **Status**: ⚠️ User questioned if columns were dropped

3. **Column Order Restoration** - Restored original complex columns to maintain positions
   - **Impact**: Ensured `amount` stays in column 6 position
   - **Status**: ✅ Fixed to maintain expected structure

4. **Accounts Sheet: Added AccountType Column** - APPROVED BY USER
   - **Proposed Change**: Extract "display" value from "subtype" object and insert as new "AccountType" column
   - **Reason**: User requested AccountType in column B (position 2) for better account categorization
   - **Impact**: 
     * New "AccountType" column inserted at position 2
     * All other account columns shift right by one position
     * Original "subtype" column remains in its position
   - **User Approval**: ✅ Approved ("let's go")
   - **Implementation Status**: ✅ Complete
   - **Files Modified**: MonarchMoneyMain-v3.py
   - **Functions Added**: `_process_accounts()`, `_account_headers_rows()`

### Action Items
- [ ] User needs to verify no other columns were accidentally dropped
- [ ] Confirm current column structure matches expectations in next run
- [ ] Document any additional changes user approves

---

## Template for Future Changes

### [Date] - [Change Description]
**Proposed Change**: [What needs to be modified]
**Reason**: [Why this change is needed]
**Impact**: [Which columns affected, position changes]
**User Approval**: [ ] Requested [ ] Approved [ ] Rejected
**Implementation Status**: [ ] Pending [ ] Complete [ ] Rolled Back

---

*Always update this log BEFORE making any column structure changes*