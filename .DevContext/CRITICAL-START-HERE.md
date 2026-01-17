# 🚨 CRITICAL PROJECT CONTEXT - READ FIRST 🚨

## MANDATORY RULES FOR ANY AI ASSISTANT WORKING ON THIS PROJECT

### ⚠️ COLUMN STRUCTURE IS SACRED ⚠️

**NEVER, EVER, UNDER ANY CIRCUMSTANCES:**
- Remove columns without explicit user confirmation
- Change column order without explicit user confirmation  
- Add columns without explicit user confirmation
- Rename columns without explicit user confirmation

### WHY THIS MATTERS
This MonarchMoney scraper feeds data into Google Spreadsheets that serve as the foundation for:
- Financial calculations
- Reports and analytics
- Derivative financial data
- Automated processes that depend on specific column positions

**Breaking column structure = Breaking financial infrastructure**

### REQUIRED PROTOCOL BEFORE ANY COLUMN CHANGES

1. **STOP** - Do not proceed with any column modifications
2. **ASK** - Explicitly ask the user: "This change will affect column structure. Should I proceed?"
3. **WAIT** - Wait for explicit confirmation before making changes
4. **DOCUMENT** - Update this documentation with any approved changes

### CURRENT CRITICAL COLUMN POSITIONS (As of Dec 24, 2025)
Based on `_headers_rows()` function in MonarchMoneyMain-v3.py:

1. `__typename`
2. `account` (original complex JSON column)
3. `AccID` (extracted account ID)
4. `AccDispName` (extracted account display name)
5. `AccType` (extracted account type)
6. `amount` ← **CRITICAL: User expects this in position 6**
7. `attachments`
8. `category` (original complex JSON column)
9. `CatID` (extracted category ID)
10. `CatDispName` (extracted category display name)
11. `CatType` (extracted category type)
12. `date`
13. `hideFromReports`
14. `id`
15. `isRecurring`
16. `isSplitTransaction`
17. `merchant` (original complex JSON column)
18. `MrchntID` (extracted merchant ID)
19. `MrchntDispName` (extracted merchant display name)
20. `MrchntTranCount` (extracted merchant transaction count)
21. `MrchntType` (extracted merchant type)
22. `needsReview`
23. `notes`
24. `pending`
25. `plaidName`
26. `reviewStatus`
27. `tags` (original complex JSON array)
28. `TagsCSL` (comma-separated tag names)
29. `createdAt` (formatted timestamp)
30. `updatedAt` (formatted timestamp)
31. `loadedAtUtc` (our metadata timestamp)

### WHAT'S BEEN MODIFIED RECENTLY
- Added `amount` field processing to strip `$` symbols and convert to decimal numbers
- Added command line argument support for configuration overrides
- Added nested field extraction while preserving original complex columns
- Increased BACKFILL_DAYS to 10 years (3650 days) for comprehensive historical data

### IF YOU NEED TO MODIFY COLUMNS
1. Check this document first
2. Ask user explicitly: "I need to [add/remove/change] column [X]. This will affect your spreadsheet structure. Do you want me to proceed?"
3. Get explicit YES/NO confirmation
4. Update this document with changes
5. Never assume silence means consent

### CONTACT USER IMMEDIATELY IF:
- Any code change might affect column order
- You discover missing/dropped columns  
- API changes require column structure updates
- You need to add new extracted fields

## REMEMBER: FINANCIAL DATA INTEGRITY > CODE ELEGANCE

**When in doubt, ASK. When unsure, ASK. When confident, STILL ASK.**

---
*This document was created December 24, 2025, after discovering potential column structure changes that could have broken financial calculations.*