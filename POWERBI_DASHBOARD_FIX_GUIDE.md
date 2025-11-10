# Power BI Dashboard Fix Guide - "2" Value Issue

## Problem Identified

Your dashboard is showing "**2**" for all months instead of actual ticket counts (hundreds/thousands).

**Root Cause:** Power BI is **counting the number of records** instead of **summing the TICKET_COUNT field**.

### Why This Happens

Each month has **multiple aggregate records**:
- 1 record for Moving (M) from HISTORICAL_SUMMARY
- 1 record for Parking (P) from HISTORICAL_SUMMARY
- 1 record for Moving (M) from COURT_CURRENT
- 1 record for Parking (P) from COURT_CURRENT

When you use `COUNT(TICKET_NUMBER)` or similar, Power BI counts these records (usually 2 per type) instead of summing the actual ticket counts.

### Example for September 2024:
- **What Power BI shows:** M=2, P=2
- **What should show:** M=926, P=4,669

## Solution: Use TICKET_COUNT Field

### Step 1: Update Your Matrix Visual

1. Open your Power BI report
2. Select the matrix visual showing the monthly breakdown
3. In the **Values** section, remove any measure using COUNT, COUNTA, or COUNTROWS
4. Add this field instead:
   - Drag `TICKET_COUNT` to the Values area
   - Ensure aggregation is set to **Sum** (not Count)

### Step 2: Create DAX Measures (Recommended)

Open Power BI Desktop and create these measures in the ATS_Court_Data table:

```dax
Total Tickets = SUM(ATS_Court_Data[TICKET_COUNT])

Moving Violations =
CALCULATE(
    SUM(ATS_Court_Data[TICKET_COUNT]),
    ATS_Court_Data[TYPE] = "M"
)

Parking Violations =
CALCULATE(
    SUM(ATS_Court_Data[TICKET_COUNT]),
    ATS_Court_Data[TYPE] = "P"
)

Special Complaints =
CALCULATE(
    SUM(ATS_Court_Data[TICKET_COUNT]),
    ATS_Court_Data[TYPE] = "C"
)
```

### Step 3: Update Your Visuals

Replace all instances where you're currently using:
- ❌ `COUNT(TICKET_NUMBER)`
- ❌ `COUNTA(TICKET_NUMBER)`
- ❌ `COUNTROWS(ATS_Court_Data)`

With:
- ✅ `[Total Tickets]` measure
- ✅ `SUM(TICKET_COUNT)`
- ✅ `[Moving Violations]` or `[Parking Violations]` measures

## Expected Results After Fix

### Monthly Breakdown (from screenshots):

**September 2024:**
- Moving: 926 (not 2)
- Parking: 4,669 (not 2)

**October 2024:**
- Moving: 758 (not 2)
- Parking: 4,853 (not 2)

**August 2025:**
- Moving: 1,358 (not 2)
- Parking: 6,119 (not 2)

**September 2025:**
- Moving: 565
- Parking: 4,025
- Special Complaints: 9

## Important Notes

### About Aggregate Records

The data contains **aggregate summary records** (not individual tickets) for historical months:

- **IS_AGGREGATE = TRUE:** Summary record representing multiple tickets
  - Example: One record with TICKET_COUNT=463 means 463 individual tickets

- **IS_AGGREGATE = FALSE:** Individual e-ticket record (September 2025 only)
  - Example: One record with TICKET_COUNT=1 means 1 individual ticket

**Always sum TICKET_COUNT** to get accurate totals across both aggregate and individual records.

### Data Sources Explanation

Your data now comes from **three sources**:

1. **HISTORICAL_SUMMARY** (ETL_VERSION)
   - August dashboard data
   - Sep 2024 - Aug 2025
   - Aggregate records

2. **COURT_CURRENT** (ETL_VERSION)
   - September dashboard data
   - Sep 2024 - Aug 2025 (excludes Sep 2025 to avoid duplication)
   - Aggregate records

3. **ETICKET_CURRENT** (ETL_VERSION)
   - E-ticket export
   - Sep 2025 only
   - Individual ticket records

For any month Sep 2024 - Aug 2025: Data from both HISTORICAL_SUMMARY and COURT_CURRENT (2 sources)
For September 2025: Data from ETICKET_CURRENT only (1 source)

## Verification Steps

After making the changes:

1. **Refresh your data** in Power BI (Home → Refresh)
2. **Check September 2024:**
   - Should show M ≈ 926, P ≈ 4,669 (not 2, 2)
3. **Check September 2025:**
   - Should show M = 565, P = 4,025, C = 9
4. **Verify totals column:**
   - Should show thousands of tickets, not single digits

## Quick Fix Checklist

- [ ] Open Power BI Desktop
- [ ] Create the DAX measures (Total Tickets, Moving Violations, Parking Violations)
- [ ] Update matrix visual to use `SUM(TICKET_COUNT)` or new measures
- [ ] Remove any COUNT-based measures
- [ ] Refresh data
- [ ] Verify September 2024 shows correct values (M=926, P=4,669)
- [ ] Verify September 2025 shows correct values (M=565, P=4,025)
- [ ] Save and publish

## Files Reference

- **DAX Measures:** `DAX/TICKET_COUNT_MEASURES.dax`
- **M Code:** `m_code/ATS_Court_Data.m`
- **Python ETL:** `SummonsMaster_Simple.py`
- **Output Data:** `C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons\summons_powerbi_latest.xlsx`

## Need Help?

The TICKET_COUNT field is the **key to accurate reporting**. It stores:
- For aggregate records: The actual count of tickets that record represents
- For individual records: Always 1 (one ticket per record)

When you SUM this field, you get the true total number of tickets, regardless of how many records are in the data.
