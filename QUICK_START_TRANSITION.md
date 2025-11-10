# Quick Start Guide - SummonsMaster_Transition.py

## Pre-Flight Checklist

### 1. Close Excel Files
- [ ] Close `summons_powerbi_latest.xlsx` if open
- [ ] Close any monthly archive files

### 2. Verify File Paths Exist

**Historical Court Files**:
```
C:\Users\carucci_r\OneDrive - City of Hackensack\05_EXPORTS\_Summons\Court\25_08_ATS.csv
C:\Users\carucci_r\OneDrive - City of Hackensack\05_EXPORTS\_Summons\Court\25_08_Hackensack Police Department Summons Dashboard.csv
```

**E-Ticket Folder**:
```
C:\Users\carucci_r\OneDrive - City of Hackensack\05_EXPORTS\_Summons\E_Ticket\
```

**Assignment File**:
```
C:\Users\carucci_r\OneDrive - City of Hackensack\09_Reference\Personnel\Assignment_Master_V2.csv
```

### 3. Check Python Environment
Ensure required packages are installed:
```bash
pip install pandas numpy openpyxl python-dateutil
```

## Running the Script

### Option 1: Direct Execution (Recommended)
```bash
cd "C:\Users\carucci_r\OneDrive - City of Hackensack\02_ETL_Scripts\Summons"
python SummonsMaster_Transition.py
```

### Option 2: From Python
```python
import SummonsMaster_Transition
SummonsMaster_Transition.main()
```

## What to Expect

### Console Output
You'll see phased execution with clear progress indicators:

```
===========================================================
SUMMONS MASTER TRANSITION SCRIPT
===========================================================
Rolling 13-Month Window: 2024-09-01 to 2025-09-30
Assignment entries loaded: 150

PHASE 1: Loading Historical Data (pre-Sept 2025)
-----------------------------------------------------------
Loading historical court data for 2024-09-01 to 2025-09-30
Processing summary format from 25_08_Hackensack Police Department Summons Dashboard.csv
  Expanded 50 summary rows into 1,234 individual records
  Filtered: 1,234 -> 1,234 records (pre-2025-09-01)
Processing individual court records from 25_08_ATS.csv
  Filtered: 500 -> 500 records (pre-2025-09-01)
Total historical records loaded: 1,734
Historical backfill records: 1,734

PHASE 2: Loading E-ticket Data (Sept 2025+)
-----------------------------------------------------------
Loading e-ticket data for 2024-09-01 to 2025-09-30
  Filtered 25_09_e_ticketexport.csv: 300 -> 300 records (2025-09-01+)
Total e-ticket records loaded: 300
E-ticket current records: 300

PHASE 3: Combining and Processing Data
-----------------------------------------------------------
Combined records before deduplication: 2,034
Combined records after deduplication: 2,034
Assignment merge completed. Rows without officer display after merge: 1,234
Classification reasons: {'CODE:TITLE39': 800, 'DESC:PARKING': 1,100, ...}
PEO/Class I pre-check — potential M→P candidates: 5
PEO/Class I rule — eligible to flip from M→P: 5; flipped: 5

PHASE 4: Data Validation
-----------------------------------------------------------
===========================================================
VALIDATION: Date Range Check
===========================================================
Historical data range: 2024-09-01 to 2025-08-31
E-ticket data range: 2025-09-01 to 2025-09-30
Transition cutoff: 2025-09-01
✓ Date ranges validated - no overlaps detected
===========================================================

===========================================================
VALIDATION: Record Counts
===========================================================
Total records: 2,034
Records by source:
  HISTORICAL_BACKFILL: 1,234 (60.7%)
  COURT_BACKFILL: 500 (24.6%)
  ETICKET_CURRENT: 300 (14.7%)
Records by TYPE:
  M: 600 (29.5%)
  P: 1,400 (68.8%)
  C: 34 (1.7%)
===========================================================

===========================================================
VALIDATION: Data Quality
===========================================================
✓ No duplicate ticket numbers
✓ All dates parsed successfully
Records with unknown officer: 1,234 (60.7%)
✓ All required columns present
===========================================================

PHASE 5: Creating Summary Reports
-----------------------------------------------------------

PHASE 6: Writing Output Files
-----------------------------------------------------------
Removed existing file: C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons\summons_powerbi_latest.xlsx
✓ Main output file written: summons_powerbi_latest.xlsx
✓ Archive file written: summons_powerbi_transition_2025_10.xlsx
Summary written: summons_transition_summary_202510.md
Changelog written: summons_transition_changelog_20251014_143022.txt

===========================================================
EXECUTION COMPLETE
===========================================================
Historical backfill records: 1,734
E-ticket current records: 300
Total final records: 2,034
Output file: C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons\summons_powerbi_latest.xlsx
===========================================================
```

### Generated Files

1. **Excel Output** (in `C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons\`):
   - `summons_powerbi_latest.xlsx` - Main output
   - `summons_powerbi_transition_2025_10.xlsx` - Monthly archive

2. **Summary & Logs** (in script directory):
   - `summons_transition_processing.log` - Detailed execution log
   - `summons_transition_summary_202510.md` - Data summary
   - `summons_transition_changelog_20251014_143022.txt` - Change log

## Reviewing Results

### 1. Check the Log File
```bash
# View last 50 lines
tail -n 50 summons_transition_processing.log

# Or open in text editor
notepad summons_transition_processing.log
```

**Look For**:
- ✓ No ERROR messages
- ✓ Validation checks passed
- ✓ Date ranges correct
- ✓ Record counts reasonable

### 2. Inspect the Excel Output

Open `summons_powerbi_latest.xlsx` and verify:

**Sheet: Summons_Data**
- Check `ETL_VERSION` column distribution
- Verify `ISSUE_DATE` ranges align with sources
- Confirm `TYPE` values (M/P/C) are reasonable
- Check `OFFICER_DISPLAY_NAME` populated for e-ticket data

**Sheet: PB_Overall_MvsP**
- Patrol Bureau totals by type

**Sheet: PB_ByOfficer_MvsP**
- Officer-level breakdown

**Sheet: PEO_Flips_Audit**
- Shows any M→P conversions due to PEO rule

### 3. Review the Summary Markdown

```bash
notepad summons_transition_summary_202510.md
```

Check:
- Data source percentages
- TYPE distribution
- Classification reasons
- Top officers lists

## Validation Checklist

After first run, verify these key metrics:

### Date Ranges
- [ ] Historical data: All dates < 2025-09-01
- [ ] E-ticket data: All dates >= 2025-09-01
- [ ] No overlap warnings in validation section

### Record Counts
- [ ] Total matches expected volume
- [ ] ETL_VERSION distribution makes sense:
  - HISTORICAL_BACKFILL: Summary data (expanded records)
  - COURT_BACKFILL: Individual court records
  - ETICKET_CURRENT: E-ticket exports

### Data Quality
- [ ] Zero duplicate ticket numbers
- [ ] All dates parsed (no NaT values)
- [ ] Officer assignments present for e-ticket data
- [ ] TYPE distribution reasonable (more P than M typically)

### Output Files
- [ ] Excel file opens without errors
- [ ] All 5 sheets present
- [ ] Data looks correct in each sheet
- [ ] Summary markdown is readable

## Testing with Power BI

1. **Open your existing Power BI report**
2. **Refresh the data source**:
   - Right-click on data source → Refresh
   - Or use Home → Refresh
3. **Verify visuals update correctly**
4. **Check for any errors in Query Editor**

**If errors occur**:
- Check column names match exactly
- Verify file path is correct
- Review ETL_VERSION field (new column, may need to add to queries)

## Troubleshooting

### Issue: "No historical data loaded"

**Check**:
1. Do the files exist at the paths specified?
2. Are the files readable (not corrupted)?
3. Do they have the expected columns?

**Fix**:
```python
# Test file loading manually
from pathlib import Path
p = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\05_EXPORTS\_Summons\Court\25_08_ATS.csv")
print(p.exists())  # Should be True
```

### Issue: "No e-ticket data loaded"

**Check**:
1. Does the E_TICKET_FOLDER exist?
2. Are there .csv files in it?
3. Are they in the correct format?

**Fix**:
```python
# List files in folder
from pathlib import Path
folder = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\05_EXPORTS\_Summons\E_Ticket")
files = list(folder.glob("*.csv"))
print(f"Found {len(files)} CSV files")
for f in files:
    print(f.name)
```

### Issue: "Permission denied" when writing Excel

**Cause**: Excel file is open

**Fix**:
1. Close the Excel file
2. Re-run the script
3. Or let script auto-generate timestamped filename

### Issue: Date range warnings

**Example**: "WARNING: Historical data extends into e-ticket period"

**Cause**: Data in historical files has dates >= 2025-09-01

**Fix**:
- Review the source files
- Adjust TRANSITION_DATE if needed
- Or filter the source data manually

### Issue: High count of "UNKNOWN OFFICER"

**Expected for**:
- HISTORICAL_BACKFILL records (summary data has no officer info)
- COURT_BACKFILL records (if badge numbers missing)

**Normal for**:
- E-ticket data to have full officer assignments

**Check**: Review by ETL_VERSION to see which source has the issue

## Next Steps After Successful Run

1. **Archive the original script**:
   ```bash
   # Make a backup
   copy SummonsMaster.py SummonsMaster_ORIGINAL.py
   ```

2. **Update your batch files** (if any):
   - Replace `SummonsMaster.py` with `SummonsMaster_Transition.py`

3. **Update Power BI documentation**:
   - Note the new ETL_VERSION field
   - Document data source split at Sept 1, 2025

4. **Schedule regular runs**:
   - Run monthly or as needed
   - Monitor log files for issues

5. **Monitor transition period**:
   - Sept 2025: Watch for clean cutover
   - Verify no gaps in data
   - Check both sources contributing correctly

## Need Help?

Review these files for diagnostics:
1. `summons_transition_processing.log` - Full execution details
2. `summons_transition_summary_202510.md` - High-level statistics
3. Excel output - Raw data inspection

---

**Script**: SummonsMaster_Transition.py  
**Documentation**: TRANSITION_SCRIPT_SUMMARY.md  
**Support**: Check logs and validation output

