# Power BI M Code - Update Guide

## Date: October 14, 2025
## New Data Structure from SummonsMaster_Simple.py

---

## Key Changes in Data Structure

### New Columns Added:
1. **`TICKET_COUNT`** (Int64) - Number of tickets represented (1 for individual, count for aggregates)
2. **`IS_AGGREGATE`** (Boolean) - TRUE for historical summary records, FALSE for individual tickets
3. **`OFFICER_NAME_RAW`** (Text) - Raw officer name from source data

### Critical Filters Required:
**All officer-level queries MUST filter out aggregate records:**

```m
// Filter out aggregate historical records
#"Filtered Individual Tickets" = Table.SelectRows(
    #"Changed Type",
    each [IS_AGGREGATE] = false or [IS_AGGREGATE] = null
)
```

**Also exclude non-officer records:**

```m
// Exclude aggregate bureaus and multiple officers
each [WG2] <> null
    and [WG2] <> ""
    and [WG2] <> "AGGREGATE"
    and [OFFICER_DISPLAY_NAME] <> null
    and [OFFICER_DISPLAY_NAME] <> ""
    and [OFFICER_DISPLAY_NAME] <> "MULTIPLE OFFICERS"
    and [OFFICER_DISPLAY_NAME] <> "MULTIPLE OFFICERS (Historical)"
```

---

## Updated M Code Files

### 1. Top_5_Moving_Violations_UPDATED.m
**Location:** `m_code\Top_5_Moving_Violations_UPDATED.m`

**Key Updates:**
- ✅ Added IS_AGGREGATE filter
- ✅ Added TICKET_COUNT column type
- ✅ Filters out AGGREGATE bureau records
- ✅ Filters out MULTIPLE OFFICERS records
- ✅ Fixed incomplete line 17 from original
- ✅ Includes badge number and team in output

**Output Columns:**
- Rank (1-5)
- Officer (display name with badge)
- Badge (4-digit padded)
- Bureau (WG2)
- Team
- Moving Summons (count)

---

### 2. Top_5_Parking_Violations_UPDATED.m
**Location:** `m_code\Top_5_Parking_Violations_UPDATED.m`

**Key Updates:**
- ✅ Added IS_AGGREGATE filter
- ✅ Added TICKET_COUNT column type
- ✅ Filters out AGGREGATE bureau records
- ✅ Filters out MULTIPLE OFFICERS records
- ✅ Includes badge number and team in output

**Output Columns:**
- Rank (1-5)
- Officer (display name with badge)
- Badge (4-digit padded)
- Bureau (WG2)
- Team
- Parking Summons (count)

---

## Data Type Reference

### Complete Column Type Specification:

```m
#"Changed Type" = Table.TransformColumnTypes(
    #"Promoted Headers",
    {
        {"TICKET_NUMBER", type text},
        {"TICKET_COUNT", Int64.Type},              // NEW
        {"IS_AGGREGATE", type logical},            // NEW
        {"PADDED_BADGE_NUMBER", type text},        // ALWAYS TEXT (4-digit padded)
        {"OFFICER_DISPLAY_NAME", type text},
        {"OFFICER_NAME_RAW", type text},           // NEW
        {"WG1", type text},
        {"WG2", type text},
        {"WG3", type text},
        {"WG4", type text},
        {"WG5", type text},
        {"TEAM", type text},
        {"ISSUE_DATE", type datetime},
        {"VIOLATION_NUMBER", type text},
        {"VIOLATION_DESCRIPTION", type text},
        {"VIOLATION_TYPE", type text},
        {"TYPE", type text},
        {"STATUS", type text},
        {"TOTAL_PAID_AMOUNT", type number},
        {"FINE_AMOUNT", type number},
        {"COST_AMOUNT", type number},
        {"MISC_AMOUNT", type number},
        {"Year", Int64.Type},
        {"Month", Int64.Type},
        {"YearMonthKey", Int64.Type},
        {"Month_Year", type text},
        {"ASSIGNMENT_FOUND", type logical},
        {"DATA_QUALITY_SCORE", type number},
        {"DATA_QUALITY_TIER", type text},
        {"SOURCE_FILE", type text},
        {"PROCESSING_TIMESTAMP", type datetime},
        {"ETL_VERSION", type text}
    }
)
```

---

## Common Query Patterns

### 1. Get All Individual Tickets (Exclude Aggregates)

```m
#"Individual Tickets" = Table.SelectRows(
    #"Changed Type",
    each [IS_AGGREGATE] = false or [IS_AGGREGATE] = null
)
```

### 2. Get Only Patrol Bureau Officers

```m
#"Patrol Bureau" = Table.SelectRows(
    #"Individual Tickets",
    each Text.Upper([WG2]) = "PATROL BUREAU"
)
```

### 3. Get Most Recent Month

```m
MaxYearMonthKey = List.Max(#"Individual Tickets"[YearMonthKey])

#"Recent Month" = Table.SelectRows(
    #"Individual Tickets",
    each [YearMonthKey] = MaxYearMonthKey
)
```

### 4. Count Tickets by Officer (September 2025 Only)

```m
#"Grouped by Officer" = Table.Group(
    #"Recent Month Individual",
    {"OFFICER_DISPLAY_NAME", "PADDED_BADGE_NUMBER", "WG2", "TYPE"},
    {{"Ticket_Count", each Table.RowCount(_), type number}}
)
```

### 5. Include Historical Aggregate Counts

If you want to include aggregate historical data:

```m
// Get aggregate records
#"Aggregate Historical" = Table.SelectRows(
    #"Changed Type",
    each [IS_AGGREGATE] = true
)

// Sum the TICKET_COUNT column instead of counting rows
#"Grouped Aggregates" = Table.Group(
    #"Aggregate Historical",
    {"Year", "Month", "TYPE"},
    {{"Total_Tickets", each List.Sum([TICKET_COUNT]), type number}}
)
```

---

## Migration Steps

### If Using Old M Code Files:

1. **Backup current queries** in Power BI Desktop (right-click query → Duplicate)

2. **Update data type transformations** to include new columns:
   - Add `TICKET_COUNT` as Int64
   - Add `IS_AGGREGATE` as logical
   - Add `OFFICER_NAME_RAW` as text

3. **Add aggregate filter** immediately after type conversion:
   ```m
   #"Filtered Individual Tickets" = Table.SelectRows(
       #"Changed Type",
       each [IS_AGGREGATE] = false or [IS_AGGREGATE] = null
   )
   ```

4. **Update officer filters** to exclude aggregate records:
   - Add `[WG2] <> "AGGREGATE"`
   - Add `[OFFICER_DISPLAY_NAME] <> "MULTIPLE OFFICERS"`
   - Add `[OFFICER_DISPLAY_NAME] <> "MULTIPLE OFFICERS (Historical)"`

5. **Test queries** with new data file

6. **Replace old queries** once verified

---

## Verification Checklist

After updating M code, verify:

- ✅ Top 5 Moving Violations shows actual officers (not "MULTIPLE OFFICERS")
- ✅ Top 5 Parking Violations shows actual officers
- ✅ Badge numbers display as 4-digit text (e.g., "0223", "1234")
- ✅ September 2025 is detected as the most recent month
- ✅ Counts look reasonable (577 moving, 4,037 parking for Sept 2025)
- ✅ Officer names include badge numbers in parentheses
- ✅ No aggregate records appear in officer-level reports

---

## Troubleshooting

### Problem: "0 records" or empty results

**Solution:** Check IS_AGGREGATE filter - make sure you're filtering it out:
```m
each [IS_AGGREGATE] = false or [IS_AGGREGATE] = null
```

### Problem: Badge numbers showing as decimals

**Solution:** Ensure PADDED_BADGE_NUMBER is set to `type text`:
```m
{"PADDED_BADGE_NUMBER", type text}
```

### Problem: Seeing "MULTIPLE OFFICERS" in top 5

**Solution:** Add filter to exclude these:
```m
each [OFFICER_DISPLAY_NAME] <> "MULTIPLE OFFICERS"
    and [OFFICER_DISPLAY_NAME] <> "MULTIPLE OFFICERS (Historical)"
```

### Problem: Wrong month being selected

**Solution:** Make sure you filter individual tickets BEFORE finding max month:
```m
// Filter first
#"Individual Tickets" = Table.SelectRows(...)

// Then find max
MaxYearMonthKey = List.Max(#"Individual Tickets"[YearMonthKey])
```

---

## Example: Complete Working Query

See `Top_5_Moving_Violations_UPDATED.m` or `Top_5_Parking_Violations_UPDATED.m` for complete working examples.

---

**Questions?** Check the `SOLUTION_SUMMARY.md` file for details on the data structure and Python processing script.
