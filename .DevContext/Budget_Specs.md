# Monarch Money Budget Integration Specifications

## Overview
Budget extraction functionality integrated into MonarchMoneyMain-v3.py to pull budget data from Monarch Money API and sync to Google Sheets alongside existing accounts and transactions.

## Technical Requirements

### Dependencies
- **monarchmoney library**: MUST use latest version from GitHub (not PyPI)
  - PyPI version (v0.1.15) lacks flexible budget support
  - Install: `pip install git+https://github.com/hammem/monarchmoney.git`
  - Issue: GraphQL errors occur with flexible budgets on older versions

### Monarch Money Prerequisites  
- **Monarch Premium subscription** required for budget API access
- **Flexible budgets feature** must be enabled in account settings
- Account must have budget data configured for target months

## API Implementationhttps://www.owox.com/blog/articles/date-filtering-with-query-google-sheets

### GraphQL Budget Query
```python
mm.get_budgets()  # Returns comprehensive budget data structure
```

### Data Structure Returned
```
budgets: {
  monthlyTotals: [
    {
      month: "2024-12-01"
      totalIncome: { plannedAmount, actualAmount, remainingAmount }
      totalExpenses: { plannedAmount, actualAmount, remainingAmount }
      totalFixedExpenses: { ... }
      totalFlexibleExpenses: { ... }
      totalNonMonthlyExpenses: { ... }
    }
  ]
  categoryGroups: [
    {
      id, name, type, order
      categories: [
        {
          id, name, order
          budgets: [
            {
              month, plannedAmount, actualAmount, remainingAmount
              rolloverSettings: { type, previousMonthRolloverAmount }
            }
          ]
        }
      ]
    }
  ]
  flexibleExpenseBudgets: [
    {
      id, name, month, budgetVariability
      plannedCashFlow, plannedSetAside, actualAmount, remainingAmount
    }
  ]
}
```

## Data Processing Logic

### Record Types Generated
1. **CategoryBudget**: Individual category budget entries
2. **CategoryGroupBudget**: Category group totals  
3. **FlexibleExpenseBudget**: Flexible expense entries
4. **TotalIncome**: Monthly income totals
5. **TotalExpenses**: Monthly expense totals (fixed, flexible, non-monthly)

### Amount Handling - CRITICAL
- **Budget API returns amounts in DOLLARS** (not cents like transactions)
- **Do NOT divide by 100** - amounts are already in correct units
- Previous bug: Divided by 100 causing amounts to be 100x smaller than UI

### Google Sheets Structure
Target worksheet: `BUDGETS_WS = "Budgets"`

Headers:
```
RecordType, CategoryGroupName, CategoryGroupType, DebitCreditFlag, IsIncome,
CategoryName, CategoryIcon, IsSystemCategory, ExcludeFromBudget, 
Month, PlannedCashFlow, ActualAmount, CumulativeActualAmount, RemainingAmount, 
PlannedSetAside, RolloverAmount, RolloverType, RolloverPeriodId,
RolloverStartMonth, RolloverEndMonth, RolloverStartingBalance, RolloverTargetAmount, RolloverFrequency,
BudgetVariability, GroupLevelBudgetingEnabled, CategoryUpdatedAt,
CategoryGroupId, CategoryId, GroupOrder, CategoryOrder, LoadedAtUtc
```

### Debit/Credit Classification - NEW
- **CategoryGroupType**: Raw group type from Monarch ("income" or "expense")
- **DebitCreditFlag**: Explicit accounting flag ("Credit" for income, "Debit" for expenses)
- **IsIncome**: Boolean flag (true for income categories, false for expense categories)

### High-Impact Enhancement Fields - NEW
- **CategoryIcon**: Unicode/emoji icon for visual identification in spreadsheets
- **CumulativeActualAmount**: Running total of actual spending for trend analysis
- **IsSystemCategory**: Distinguishes Monarch system categories from user-created ones
- **ExcludeFromBudget**: Flags categories excluded from budget calculations
- **CategoryUpdatedAt**: Last modification timestamp for change tracking
- **GroupLevelBudgetingEnabled**: Indicates if group-level budgeting is active
- **Rollover Period Fields**: Detailed rollover configuration (ID, start/end months, balances, targets, frequency)

Classification Rules:
- Income categories: `DebitCreditFlag = "Credit"`, `IsIncome = true`
- Expense categories: `DebitCreditFlag = "Debit"`, `IsIncome = false`
- Flexible expenses: Always treated as `DebitCreditFlag = "Debit"`, `IsIncome = false`

## Command Line Interface

### Budget Controls
```bash
--enable-budgets     # Enable budget processing (default: disabled)
--disable-budgets    # Explicitly disable budget processing  
--budget-months N    # Number of months to retrieve (default: 6)
```

### Usage Examples
```bash
# Extract budgets for last 6 months
python MonarchMoneyMain-v3.py --enable-budgets

# Extract budgets for last 12 months  
python MonarchMoneyMain-v3.py --enable-budgets --budget-months 12

# Skip budget extraction
python MonarchMoneyMain-v3.py --disable-budgets
```

## Implementation Details

### Key Functions
- `_process_budget_data(budget_data, months)`: Main budget processing logic
- `_budget_headers_rows()`: Returns headers and formatted data for sheets
- Enhanced argument parsing for budget controls
- Comprehensive error handling with debug output

### Error Handling
- GraphQL API compatibility checks
- Flexible budget feature validation
- Empty data handling for missing budget entries
- Library version compatibility warnings

## Troubleshooting Guide

### Common Issues

1. **GraphQL Errors with Flexible Budgets**
   - Symptom: API errors when flexible budgets enabled
   - Solution: Update to latest GitHub version of monarchmoney library

2. **Amount Reconciliation Issues**  
   - Symptom: Amounts 100x smaller than Monarch UI
   - Solution: Remove `/100` division - budget API returns dollars not cents

3. **Missing Budget Data**
   - Symptom: Empty budget records or limited data
   - Solution: Verify Monarch Premium subscription and flexible budget settings

4. **Library Version Issues**
   - Symptom: Import errors or missing methods
   - Solution: `pip install git+https://github.com/hammem/monarchmoney.git`

### Debug Information
Script provides detailed logging for:
- Budget data structure examination
- Record processing progress  
- Google Sheets write operations
- Error context and API responses

## Data Validation

### Expected Outputs
- **Volume**: 600+ budget records typical for 6 months of data
- **Categories**: All configured budget categories across timeframes
- **Totals**: Income and expense totals with breakdowns
- **Flexible**: Flexible expense budgets with variability settings

### Debit/Credit Validation
- **Income categories**: Should have `DebitCreditFlag = "Credit"` and `IsIncome = true`
- **Expense categories**: Should have `DebitCreditFlag = "Debit"` and `IsIncome = false`
- **Amount signs**: Income amounts are typically positive, expense amounts are typically positive (representing budget allocations)
- **Consistency check**: All records should have non-empty `CategoryGroupType` and `DebitCreditFlag` fields

### High-Impact Field Validation - NEW
- **CategoryIcon**: Should contain valid Unicode/emoji characters for user categories
- **CumulativeActualAmount**: Should be running total ≥ individual `ActualAmount`
- **IsSystemCategory**: System categories should have `true`, user categories should have `false`
- **ExcludeFromBudget**: Should be boolean values only
- **CategoryUpdatedAt**: Should be valid ISO timestamps for categories with recent changes
- **Rollover validation**: Period IDs should be consistent across related records

### Reconciliation Checks
- Compare Google Sheets amounts with Monarch Money UI
- Verify planned vs actual amounts alignment
- Check rollover calculations for accuracy
- Validate month-by-month progression
- **NEW**: Verify debit/credit flags match category types in Monarch UI
- **NEW**: Validate cumulative amounts show proper running totals
- **NEW**: Check that category icons display properly in Google Sheets
- **NEW**: Verify system vs user category classifications are accurate
- **NEW**: Confirm rollover period details match Monarch configuration

## Integration Points

### Google Sheets API
- Uses existing `gspread` integration
- Writes to dedicated "Budgets" worksheet
- Maintains consistent formatting with accounts/transactions

### Monarch Money Session
- Leverages existing authentication flow
- Uses same session management as accounts/transactions
- Respects rate limiting and error handling patterns

## Performance Considerations

### API Limits
- Budget data retrieval is single API call
- Processing time scales with number of categories and months
- Memory usage reasonable for typical budget configurations

### Optimization Opportunities  
- Batch processing of multiple months
- Incremental updates for new data only
- Caching of stable budget configurations

## Future Enhancements

### Potential Features
- Budget variance analysis and alerting
- Trend analysis across time periods
- Integration with transaction categorization
- Automated budget vs actual reconciliation

### Data Export Options
- CSV export capability
- Direct database integration
- API endpoint for external consumption
- Real-time budget monitoring

## Version History

### v3.0 - Budget Integration
- Added comprehensive budget extraction
- Implemented flexible budget support
- Fixed amount unit handling issues
- Enhanced error handling and debugging

## Dependencies & Versions

```
monarchmoney: Latest from GitHub
gspread: Existing version  
google-auth: Existing version
argparse: Standard library
logging: Standard library  
```

## Testing Checklist

- [ ] Budget extraction with flexible budgets enabled
- [ ] Amount reconciliation with Monarch UI
- [ ] Multi-month data retrieval  
- [ ] Google Sheets integration
- [ ] Error handling for missing data
- [ ] Command line argument processing
- [ ] Library version compatibility

---

*Last Updated: December 2024*  
*Context: MonarchMoney Budget Integration Project*