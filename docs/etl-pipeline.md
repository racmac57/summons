# ETL Pipeline Reference

## Pipeline Overview

```
Backfill CSV --------+
                     +---> SummonsMaster_Simple.py ---> summons_powerbi_latest.xlsx ---> Power BI
E-Ticket CSVs -------+                                   (Summons_Data sheet)
                     |
Assignment_Master ---+
```

### Alternate Pipeline (DFR)

```
E-Ticket CSVs -------+
                     +---> summons_etl_enhanced.py ---> summons_powerbi_latest.xlsx ---> Power BI
Assignment_Master ---+                             |
                                                   +---> dfr_directed_patrol_enforcement.xlsx (DFR Summons Log)
LegalCodes JSON -----+
                     +---> update_dfr_violation_lookup.py ---> dfr_directed_patrol_enforcement.xlsx (ViolationData)
```

## Data Sources

| Source | Path | Format | Period |
|---|---|---|---|
| Backfill aggregates | `PowerBI_Data/Backfill/2025_12/summons/2025_12_department_wide_summons.csv` | CSV | Sep 2024 - Aug 2025 |
| E-ticket exports | `05_EXPORTS/_Summons/E_Ticket/{year}/month/{YYYY}_{MM}_eticket_export.csv` | CSV (semicolon 2025, comma 2026+) | Sep 2025+ |
| Assignment Master | `09_Reference/Personnel/Assignment_Master_V2.csv` | CSV | Current |
| Legal codes (DFR only) | `09_Reference/LegalCodes/data/Title39/` and `CityOrdinances/` | JSON | Reference |
| DFR workbook (DFR only) | `Shared Folder/Compstat/Contributions/Drone/dfr_directed_patrol_enforcement.xlsx` | XLSX | Ongoing |

## Processing Steps (SummonsMaster_Simple.py)

1. **Load backfill** -- Read aggregated CSV, tag `ETL_VERSION=HISTORICAL_SUMMARY`, set `IS_AGGREGATE=true`
2. **Scan e-ticket folder** -- Discover monthly CSVs matching `{YYYY}_{MM}_eticket_export.csv` pattern, auto-detect delimiter and encoding
3. **Load e-tickets** -- Read each CSV, tag `ETL_VERSION=ETICKET_CURRENT`, set `TICKET_COUNT=1`, `IS_AGGREGATE=false`
4. **Badge padding** -- Zero-pad all badge numbers to 4 digits (`38` -> `0038`)
5. **Assignment join** -- Left join on `PADDED_BADGE_NUMBER` to get display names and org hierarchy (WG1-WG5)
6. **Conditional overrides** -- Apply `ASSIGNMENT_OVERRIDES` dict for temporary assignments (badge-level, optionally conditional on violation description)
7. **PEO rule** -- Reclassify parking enforcement officer violations
8. **TYPE classification** -- Categorize violations: M (moving), P (parking), C (criminal)
9. **Rolling window** -- Filter to 13 complete months ending prior month
10. **Write output** -- Single Excel file (`summons_powerbi_latest.xlsx`), single sheet (`Summons_Data`)

## Processing Steps (summons_etl_enhanced.py) -- Differences

- Uses `fuzzywuzzy` for officer name matching (fallback when badge lookup fails)
- Imports `path_config.get_onedrive_root()` from `06_Workspace_Management/scripts`
- Exports DFR-relevant records to `dfr_directed_patrol_enforcement.xlsx` (DFR Summons Log sheet)
- Supports `DFR_CONFIG` dict with badge list, column mappings, status code translations

## Output Schema

| Column | Type | Notes |
|---|---|---|
| TICKET_NUMBER | text | Unique summons ID |
| PADDED_BADGE_NUMBER | text(4) | Zero-padded badge |
| OFFICER_DISPLAY_NAME | text | From Assignment Master |
| TYPE | text | M, P, or C |
| VIOLATION_DESCRIPTION | text | Statute/violation |
| ISSUE_DATE | date | Violation date |
| YearMonthKey | int | YYYYMM |
| Month_Year | text | MM-YY display |
| ETL_VERSION | text | HISTORICAL_SUMMARY or ETICKET_CURRENT |
| IS_AGGREGATE | bool | true for backfill |
| TICKET_COUNT | int | >1 for aggregates |
| WG1-WG5 | text | Org hierarchy |
| STATUS | text | ACTI, VOID, PAID, etc. |

## Power BI Queries (M Code)

| Query Name | File | Purpose | Key Filter |
|---|---|---|---|
| summons_13month_trend | `summons_13month_trend.m` | 13-month rolling trend chart | Groups by Month_Year, TYPE |
| summons_top5_moving | `summons_top5_moving.m` | Top 5 officers (moving) | TYPE="M", max YearMonthKey, IS_AGGREGATE=false |
| summons_top5_parking | `summons_top5_parking.m` | Top 5 officers (parking) | TYPE="P", max YearMonthKey, IS_AGGREGATE=false |
| summons_main_data | (compiled in all_summons_queries_m_code.txt) | Base data table | All records |
| summons_all_bureaus | (compiled in all_summons_queries_m_code.txt) | Bureau breakdown | Prior complete month |

## DAX Measures

| Measure | Formula | Purpose |
|---|---|---|
| Total Tickets | `SUM(ATS_Court_Data[TICKET_COUNT])` | All ticket types |
| Moving Violations | `CALCULATE(SUM(...), TYPE="M")` | Moving only |
| Parking Violations | `CALCULATE(SUM(...), TYPE="P")` | Parking only |
| Special Complaints | `CALCULATE(SUM(...), TYPE="C")` | Criminal only |
| Current Month Tickets | `CALCULATE(SUM(...), DATESMTD(...))` | MTD |
| Previous Month Tickets | `CALCULATE(SUM(...), DATEADD(...,-1,MONTH))` | Prior month |

## Fragile Logic / Hardcoded Values

| Location | Issue | Risk |
|---|---|---|
| `SummonsMaster_Simple.py` line 24 | Hardcoded `2025_12_department_wide_summons.csv` | Breaks when new backfill is generated |
| `SummonsMaster_Simple.py` lines 43-68 | `ASSIGNMENT_OVERRIDES` dict | Personnel changes require code edits |
| `summons_etl_enhanced.py` line 19 | `from fuzzywuzzy import fuzz, process` | Package may not be installed |
| `summons_backfill_13month.py` line 22 | Assignment Master path at `06_Workspace_Management/` | Different from primary script's `09_Reference/` path |
| `summons_backfill_13month.py` lines 29-42 | `HISTORICAL_FILES` list is fully hardcoded | Must be manually updated for new months |
| `run_eticket_export.py` line 5 | Uses `RobertCarucci` instead of `carucci_r` | Breaks junction convention |
| All M code files | Hardcoded path to `summons_powerbi_latest.xlsx` | Path change requires editing all queries |
| `DAX/TICKET_COUNT_MEASURES.dax` | References `ATS_Court_Data` table name | Table must match in Power BI model |
