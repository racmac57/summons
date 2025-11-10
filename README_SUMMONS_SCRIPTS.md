# Summons Data Merge Diagnostic & Fix Scripts

## Overview
Two Python scripts to diagnose and fix format mismatches between gold standard historical data and new September 2025 summons data.  
**Update (November 2025):** The primary production pipeline now runs through `SummonsMaster_Simple.py`, which consumes the consolidated backfill located in `C:\Dev\PowerBI_Date\Backfill\2025_09\...` and the latest monthly e-ticket export (e.g. `05_EXPORTS\_Summons\E_Ticket\25_10_e_ticketexport.csv`). See the _Current Workflow_ section below for details.

---

## Current Workflow (November 2025)

1. **Run `SummonsMaster_Simple.py`**
   - Calculates a rolling 13‑month window (October 2024 → October 2025 as of 2025‑11‑10).
   - Loads the new backfill CSV (aggregate counts) and the prior month’s e‑ticket export.
   - Writes `summons_powerbi_latest.xlsx` to `03_Staging\Summons`.

2. **Power BI Refresh Tips**
   - In Power Query force `TICKET_COUNT` to **Whole Number** before refreshing visuals.
   - Create a simple measure:  
     ```DAX
     Total Tickets = SUM('___Summons'[TICKET_COUNT])
     ```
     Use this measure in the matrix Values bucket to display monthly totals (Oct‑25: M = 480, P = 3,336, Total = 3,816).
   - `SummonsMaster_Simple.py` now pads/normalizes badges, applies diagnostics, and includes a manual override for badge `0388` (Officer Liggio) so the Patrol bureau is always present. See log output for match counts (look for `Assignment enrichment: 3818/3818`).

3. **Diagnostics for unmatched badges**
   - Run `diagnose_unmatched_badges.py` (included in the repo) after the ETL to export any remaining unmatched records to `unmatched_badges_diagnostic.csv`.
   - If badges are missing in `Assignment_Master_V2.csv`, add them or extend `ASSIGNMENT_OVERRIDES` in the script; log output now surfaces sample badge numbers automatically.

4. **Legacy merge scripts** (below) remain available for validating individual CSV drops, but the automated pipeline supersedes them for monthly refreshes.

---

## Files Created
1. **summons_merge_diagnostic.py** - Analyzes both files and identifies issues
2. **summons_merge_fix.py** - Automatically corrects September 2025 format

## Usage Instructions

### Step 1: Run Diagnostic
```bash
python summons_merge_diagnostic.py
```

This will:
- ✅ Compare gold standard vs September 2025 schemas
- ✅ Validate column names, data types, and order
- ✅ Check September 2025 totals (M=565, P=4,025, C=9)
- ✅ Test merge compatibility
- ✅ Generate detailed diagnostic report

**Expected Output:**
- Green checkmarks (✅) for matching elements
- Red X marks (❌) for mismatches
- Summary of issues found

### Step 2: Run Fix (if issues found)
```bash
python summons_merge_fix.py
```

This will:
- ✅ Create timestamped backup of original file
- ✅ Reorder columns to match gold standard
- ✅ Convert data types (especially Count of TICKET_NUMBER to int64)
- ✅ Validate totals after fixes
- ✅ Test merge compatibility
- ✅ Save corrected file

**Safety Features:**
- Automatic backup created before any changes
- Original file preserved with timestamp
- Validation performed before saving

## File Locations (Configured in Scripts)

**Gold Standard (DO NOT MODIFY):**
```
C:\Users\carucci_r\OneDrive - City of Hackensack\02_ETL_Scripts\Summons\backfill_data\25_08_Hackensack Police Department - Summons Dashboard.csv
```

**September 2025 (Will be corrected):**
```
C:\Users\carucci_r\OneDrive - City of Hackensack\02_ETL_Scripts\Summons\visual_export\25_09_Hackensack Police Department - Summons Dashboard.csv
```

## Expected Results

### If Format Matches:
```
✅ NO ISSUES DETECTED
   - Schema matches perfectly
   - Totals validate correctly
   - Merge compatible
```

### If Issues Found:
The fix script will automatically correct:
- Column ordering
- Data type mismatches (text → integer for counts)
- Merge compatibility issues

## Common Issues & Solutions

### Issue: Count of TICKET_NUMBER stored as text
**Symptom:** Data type shows 'object' instead of 'int64'
**Fix:** Automatic conversion to int64 in fix script

### Issue: Column order doesn't match
**Symptom:** Columns present but in different order
**Fix:** Automatic reordering to match gold standard

### Issue: Totals don't match expected values
**Symptom:** M≠565, P≠4,025, or C≠9
**Solution:** Review Python script that generates September 2025 data
- Check e-ticket classification logic (P/M/C assignment)
- Verify no duplicate records in source data
- Confirm date filtering is correct (Sept 2025 only)

## Validation Checklist

After running both scripts, verify:
- [ ] September 2025 file has backup created
- [ ] Column names match exactly
- [ ] Data types match (especially Count = int64)
- [ ] Totals: M=565, P=4,025, C=9
- [ ] Merge test passes
- [ ] Power BI can refresh without errors

## M Code Integration Notes

If issues persist in Power BI after running these scripts, check M Code:
1. Verify file path points to corrected visual_export file
2. Ensure data types are recognized correctly on import
3. Check for any manual type conversions overriding correct types

## Troubleshooting

### Scripts won't run
- Verify file paths are correct
- Check OneDrive sync status
- Ensure Python has pandas installed: `pip install pandas`

### Fix doesn't resolve all issues
- Review diagnostic output for clues
- Check if source e-ticket data has quality issues
- Validate Python script that creates September 2025 file

## Contact
R. A. Carucci - Principal Analyst
Hackensack Police Department
