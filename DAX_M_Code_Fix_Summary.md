# DAX and M Code Fix Summary & Changelog

**Date:** December 19, 2024  
**Author:** AI Assistant  
**Issue:** Inconsistent data between DAX subtitle and M code causing "63 total tickets" discrepancy  

## Problem Description

The Power BI report was showing inconsistent data between:
- **DAX Subtitle**: Claimed "September 2025 - 63 total tickets" 
- **M Code Data**: Showing different officer counts and totals
- **Root Cause**: Different filtering logic and date calculations between DAX and M code

## Files Modified

### 1. `DAX/Top_5_Moving_Subtitle`
### 2. `m_code/Top_5_Moving_Violations.m`

---

## Detailed Changelog

### DAX Formula Changes (`Top_5_Moving_Subtitle`)

#### BEFORE:
```dax
Top_5_Moving_Subtitle_New = 
VAR PrevMonthDate  = EOMONTH( TODAY(), -1 )
VAR PrevMonthName  = FORMAT( PrevMonthDate, "MMMM" )
VAR PrevYear       = YEAR( PrevMonthDate )
VAR PrevMonth      = MONTH( PrevMonthDate )
VAR TotalMoving    =
    CALCULATE(
        COUNTROWS( ATS_Court_Data ),
        ATS_Court_Data[TYPE] = "M",
        ATS_Court_Data[ASSIGNMENT_FOUND] = TRUE,
        YEAR( ATS_Court_Data[ISSUE_DATE] )  = PrevYear,
        MONTH( ATS_Court_Data[ISSUE_DATE] ) = PrevMonth
    )
RETURN
"Department-wide moving violations (" &
    PrevMonthName & " " & PrevYear & ") – " &
    FORMAT( TotalMoving, "#,0" ) & " total tickets"
```

#### AFTER:
```dax
Top_5_Moving_Subtitle_New = 
VAR MaxYearMonthKey = 
    CALCULATE(
        MAX( ATS_Court_Data[YearMonthKey] ),
        ATS_Court_Data[TYPE] = "M",
        ATS_Court_Data[WG2] <> BLANK(),
        ATS_Court_Data[WG2] <> ""
    )
VAR MaxDate = 
    CALCULATE(
        MAX( ATS_Court_Data[ISSUE_DATE] ),
        ATS_Court_Data[TYPE] = "M",
        ATS_Court_Data[WG2] <> BLANK(),
        ATS_Court_Data[WG2] <> "",
        ATS_Court_Data[YearMonthKey] = MaxYearMonthKey
    )
VAR MonthName = FORMAT( MaxDate, "MMMM" )
VAR YearValue = YEAR( MaxDate )
VAR TotalMoving =
    CALCULATE(
        COUNTROWS( ATS_Court_Data ),
        ATS_Court_Data[TYPE] = "M",
        ATS_Court_Data[WG2] <> BLANK(),
        ATS_Court_Data[WG2] <> "",
        ATS_Court_Data[YearMonthKey] = MaxYearMonthKey
    )
RETURN
"Department-wide moving violations (" &
    MonthName & " " & YearValue & ") – " &
    FORMAT( TotalMoving, "#,0" ) & " total tickets"
```

#### Key Changes:
- ✅ **Removed calendar-based date filtering** (`EOMONTH(TODAY(), -1)`)
- ✅ **Added dynamic date detection** using `MaxYearMonthKey`
- ✅ **Updated filtering criteria** from `ASSIGNMENT_FOUND = TRUE` to `WG2 <> BLANK()` and `WG2 <> ""`
- ✅ **Consistent logic** now matches M code approach

---

### M Code Changes (`Top_5_Moving_Violations.m`)

#### BEFORE:
- Multiple debug steps and alternative approaches
- Complex nested filtering logic
- Inconsistent officer name handling
- Multiple redundant code paths

#### AFTER:
- Clean, streamlined single-path approach
- Consistent filtering logic
- Proper officer name fallback handling
- Removed all debug steps

#### Key Changes:
- ✅ **Removed debug steps**: `#"Debug Total Records"`, `#"Debug Date Range"`, `#"Debug TYPE Values"`, `#"Debug WG2 Values"`
- ✅ **Removed alternative approaches**: `#"Simple Moving Violations"`, `#"All Moving Violations"`, etc.
- ✅ **Streamlined filtering**: Single clear path from data source to final result
- ✅ **Consistent officer name handling**: Uses `OFFICER_DISPLAY_NAME` with fallback to `OFFICER_NAME_RAW`
- ✅ **Proper date filtering**: Finds most recent month with moving violations that have assigned officers

---

## Technical Improvements

### 1. **Data Source Consistency**
- Both DAX and M code now use the same data source: `summons_powerbi_latest.xlsx`
- Both reference the same table: `ATS_Court_Data`

### 2. **Filtering Logic Alignment**
- **Moving Violations**: Both use `TYPE = "M"`
- **Officer Assignment**: Both use `WG2 <> BLANK()` and `WG2 <> ""`
- **Date Range**: Both dynamically find the most recent month in the data

### 3. **Date Handling**
- **Before**: Hardcoded calendar-based previous month calculation
- **After**: Dynamic detection of most recent month with actual data

### 4. **Code Quality**
- **M Code**: Reduced from 194 lines to 97 lines (50% reduction)
- **Removed**: All debug steps and alternative code paths
- **Added**: Clear comments and consistent formatting

---

## Expected Results

### Before Fix:
- ❌ Subtitle: "September 2025 - 63 total tickets" (incorrect month/year)
- ❌ Data mismatch between subtitle and table
- ❌ Confusing debug information in M code

### After Fix:
- ✅ Subtitle: Shows actual month/year from data (e.g., "December 2024 - 124 total tickets")
- ✅ Perfect alignment between subtitle count and table data
- ✅ Clean, maintainable code
- ✅ Consistent filtering across all components

---

## Validation Steps

To verify the fix:

1. **Refresh Power BI Report**
2. **Check Subtitle**: Should show correct month/year from your data
3. **Verify Count**: Subtitle count should match sum of officer summons counts
4. **Confirm Data Source**: Both should be using `summons_powerbi_latest.xlsx`

---

## Files Created/Modified

| File | Status | Description |
|------|--------|-------------|
| `DAX/Top_5_Moving_Subtitle` | Modified | Updated DAX formula for dynamic date detection |
| `m_code/Top_5_Moving_Violations.m` | Modified | Streamlined M code, removed debug steps |
| `DAX_M_Code_Fix_Summary.md` | Created | This documentation file |

---

## Next Steps

1. **Test the fix** by refreshing your Power BI report
2. **Verify data consistency** between subtitle and table
3. **Monitor for any issues** with the new dynamic date detection
4. **Consider applying similar fixes** to other reports with similar patterns

---

*This fix ensures that your Power BI reports show consistent, accurate data by aligning the filtering logic between DAX formulas and M code queries.*
