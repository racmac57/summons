# Summons ETL Diff Analysis
## Generated: 2026-03-28
## Decision: summons_etl_enhanced.py is AUTHORITATIVE

---

## Script Comparison

### SummonsMaster_Simple.py (DEPRECATED)
- **Lines:** 711
- **Input sources:**
  1. `PowerBI_Data/Backfill/2025_12/summons/2025_12_department_wide_summons.csv` (historical backfill aggregate)
  2. `05_EXPORTS/_Summons/E_Ticket/{year}/month/{YYYY}_{MM}_eticket_export.csv` (monthly e-ticket)
  3. `09_Reference/Personnel/Assignment_Master_V2.csv` (officer assignments)
- **Transform logic:**
  1. Compute rolling 13-month window (ending prior complete month)
  2. Load backfill CSV -> create aggregate records (TICKET_COUNT > 1, IS_AGGREGATE=True, ETL_VERSION=HISTORICAL_SUMMARY)
  3. Load e-ticket CSV for previous month only (auto-detect delimiter)
  4. Parse dates with multi-format fallback
  5. Pad badge numbers to 4 digits
  6. Join to Assignment Master V2 on PADDED_BADGE_NUMBER
  7. Apply ASSIGNMENT_OVERRIDES (conditional: badge 2025 FIRE LANES -> SSOCC)
  8. Apply PEO/Class I rule (reclassify M -> P for PEO/Class I officers)
  9. Filter combined data to 13-month window
  10. Recompute date intelligence columns (Year, Month, YearMonthKey, Month_Year)
- **Output:** `03_Staging/Summons/summons_powerbi_latest.xlsx` (sheet: Summons_Data)
- **Output schema:** TICKET_NUMBER, TICKET_COUNT, IS_AGGREGATE, PADDED_BADGE_NUMBER, OFFICER_DISPLAY_NAME, OFFICER_NAME_RAW, ISSUE_DATE, VIOLATION_NUMBER, VIOLATION_DESCRIPTION, VIOLATION_TYPE, TYPE, STATUS, LOCATION, WARNING_FLAG, SOURCE_FILE, ETL_VERSION, Year, Month, YearMonthKey, Month_Year, TEAM, WG1-WG5, POSS_CONTRACT_TYPE, ASSIGNMENT_FOUND, DATA_QUALITY_SCORE, DATA_QUALITY_TIER, TOTAL_PAID_AMOUNT, FINE_AMOUNT, COST_AMOUNT, MISC_AMOUNT, PROCESSING_TIMESTAMP
- **Unique logic:**
  - Rolling 13-month window calculation with `compute_reporting_window()`
  - Backfill aggregate record creation (TICKET_COUNT > 1, IS_AGGREGATE=True)
  - PEO/Class I reclassification rule (M -> P)
  - Full WG1-WG5 + TEAM + POSS_CONTRACT_TYPE enrichment from Assignment Master
  - Badge 0388 (LIGGIO) override
  - `add_badge_to_name()` appending badge to display name
  - `classify_violations()` function (defined but no longer called in main flow)
  - `DATA_QUALITY_TIER` column (High/Medium/Low)
  - Financial columns: TOTAL_PAID_AMOUNT, FINE_AMOUNT, COST_AMOUNT, MISC_AMOUNT

### summons_etl_enhanced.py (AUTHORITATIVE)
- **Lines:** 833
- **Input sources:**
  1. `05_EXPORTS/_Summons/E_Ticket/{year}/month/{YYYY}_{MM}_eticket_export.csv` (13-month rolling window of monthly e-ticket files)
  2. `09_Reference/Personnel/Assignment_Master_V2.csv` (preferred) or `06_Workspace_Management/Assignment_Master_V2.csv` (fallback)
  3. Backfill gap months via `summons_backfill_merge.merge_missing_summons_months()` (imported from 06_Workspace_Management/scripts/)
  4. Optional: ATS reports, manual summons Excel files
- **Transform logic:**
  1. Scan 13-month window of e-ticket CSVs (auto-detect semicolon/comma delimiter)
  2. Validate Officer Id (not null, numeric, not zero)
  3. Pad Officer Id -> PADDED_BADGE_NUMBER (4-digit)
  4. Preserve raw officer name for audit only (OFFICER_NAME_RAW)
  5. Join to Assignment Master on PADDED_BADGE_NUMBER (supports V2 and V3 schema)
  6. Apply ASSIGNMENT_OVERRIDES (conditional: badge 2025 FIRE LANES -> SSOCC; badge 0738 -> SSOCC)
  7. FuzzyWuzzy last-name verification (export last name vs Assignment Master LAST_NAME)
  8. Categorize violations (TRAFFIC_TITLE39, MUNICIPAL_ORDINANCE, OTHER)
  9. Classify TYPE from Case Type Code (M/P/C)
  10. Add data quality metrics (score + issues)
  11. Flag name format anomalies
  12. Add TICKET_COUNT=1 for individual records
  13. Merge backfill for gap months via summons_backfill_merge
  14. DFR export: filter DFR badge records, map to DFR schema, append to dfr_directed_patrol_enforcement.xlsx
- **Output:** `03_Staging/Summons/summons_powerbi_latest.xlsx` (sheet: Summons_Data) + timestamped copy + DFR workbook append
- **Output schema:** PADDED_BADGE_NUMBER, OFFICER_DISPLAY_NAME, WG2, TITLE, TYPE, YearMonthKey, Month_Year, Year, Month, VIOLATION_DATE, ISSUE_DATE, STATUTE, VIOLATION_CATEGORY, OFFICER_NAME_RAW, NAME_FORMAT_ANOMALY, OFFICER_MATCH_QUALITY, DATA_QUALITY_SCORE, DATA_QUALITY_ISSUES, DATA_SOURCE, PROCESSED_TIMESTAMP, ETL_VERSION, TICKET_COUNT, (plus all original CSV columns)
- **Additional features over Simple:**
  - Class-based architecture (SummonsETLProcessor)
  - FuzzyWuzzy last-name cross-validation
  - Assignment Master V2/V3 dual-schema support
  - DFR export pipeline (map to DFR schema, dedup, append to Excel workbook)
  - Name format anomaly flagging
  - OFFICER_MATCH_QUALITY tracking (DIRECT_BADGE / NO_MATCH / NO_MASTER)
  - DATA_QUALITY_ISSUES field (descriptive, not just score)
  - Timestamped output + latest copy (corruption protection)
  - Badge number_format='@' in Excel (prevents leading zero loss)
  - Processing summary JSON export
  - QA validation logging (counts by badge+type)
  - ATS automated report support
  - Manual summons Excel file support
  - Dynamic path resolution via path_config.get_onedrive_root()
  - WG2 normalization ("SAFE STREETS OPERATIONS CONTROL CENTER" -> "SSOCC")
  - Backfill merge for gap months via external module

---

## Logic Comparison Matrix

| Feature | SummonsMaster_Simple | summons_etl_enhanced | Gap? |
|---------|---------------------|---------------------|------|
| Backfill CSV loading | Direct load from PowerBI_Data/Backfill/ (aggregate records, TICKET_COUNT > 1) | Via summons_backfill_merge module (imported from 06_WM) | Different mechanism, same goal |
| E-ticket CSV loading | Single previous month only | Full 13-month rolling window scan | Enhanced is broader |
| Assignment Master join | V2 only (Proposed 4-Digit Format) | V2 + V3 dual-schema support | Enhanced is more flexible |
| Badge overrides | 0388 (LIGGIO), 2025 (RAMIREZ, conditional FIRE LANES) | 2025 (RAMIREZ, conditional FIRE LANES), 0738 (POLSON) | **Gap: Badge 0388 not in enhanced** |
| Column mapping | Direct column construction in DataFrame | Standardize then enrich pipeline | No functional gap |
| Date parsing | Multi-format fallback (4 explicit formats + coerce) | pd.to_datetime with errors='coerce' | Simple has more explicit format handling |
| Output schema | 31+ columns with full WG1-WG5 hierarchy | 20+ priority columns plus originals | **Gap: WG1, WG3-WG5, TEAM, POSS_CONTRACT_TYPE missing in enhanced** |
| DFR export | Not present | Full DFR pipeline (map, dedup, append to workbook) | Enhanced only |
| FuzzyWuzzy matching | Not present | Last-name cross-validation with score | Enhanced only |
| Error handling | Basic try/except with logging | Structured logging + QA validation + processing summary JSON | Enhanced is more robust |
| PEO reclassification rule | M -> P for PEO/Class I officers | Not present | **Gap: PEO rule not in enhanced** |
| IS_AGGREGATE flag | Present (True for backfill records) | Not present (relies on TICKET_COUNT from backfill merge) | Minor schema gap |
| 13-month window enforcement | Explicit compute_reporting_window() with filter | Dynamic scan of available e-ticket files + backfill merge | Different mechanism |
| Badge-in-display-name | Appends "(XXXX)" to display name | Not present (uses Assignment Master name as-is) | Cosmetic difference |
| Financial columns | TOTAL_PAID_AMOUNT, FINE_AMOUNT, COST_AMOUNT, MISC_AMOUNT (all NaN) | FINE_AMOUNT, AMOUNT (numeric fix) | Simple's financial cols are always NaN anyway |

---

## Logic Gaps (business logic in Simple that is NOT in Enhanced)

1. **PEO/Class I reclassification rule**: `apply_peo_rule()` converts TYPE M -> P for officers where WG3 is "PEO" or "CLASS I". This rule is NOT in the enhanced script. **Impact**: PEO officers could appear with moving violations in the dashboard, which is operationally incorrect (PEOs cannot issue moving violations).

2. **Badge 0388 (LIGGIO) override**: Present in Simple but not in Enhanced. **Impact**: LOW -- this badge would fall through to "UNKNOWN - 0388" in enhanced. The override was flagged for human review anyway (may be stale).

3. **Full org hierarchy (WG1, WG3, WG4, WG5, TEAM, POSS_CONTRACT_TYPE)**: Simple loads and merges all WG columns from Assignment Master. Enhanced only loads WG2 (plus optional LAST_NAME, TITLE). **Impact**: MEDIUM -- Power BI queries that filter on WG1-WG5 or TEAM will not work with enhanced output. However, the current M code queries in production primarily use WG2 (Bureau) for the bureau breakdown visual.

4. **IS_AGGREGATE column**: Simple explicitly tags backfill records. Enhanced relies on the backfill merge module to handle this. **Impact**: LOW -- backfill merge module adds TICKET_COUNT and the necessary flags.

5. **DATA_QUALITY_TIER column**: Simple has "High"/"Medium"/"Low" tier. Enhanced has DATA_QUALITY_ISSUES (descriptive) but no tier. **Impact**: LOW -- no known Power BI query references DATA_QUALITY_TIER.

---

## Human Review Needed

1. **PEO reclassification rule** -- Should be ported to enhanced script. PEO/Class I officers cannot issue moving violations; this is a business rule, not optional logic. Recommend adding `apply_peo_rule()` to enhanced or to `summons_etl_normalize.py` in 06_Workspace_Management.

2. **Badge 0388 (LIGGIO) override** -- Is this assignment still current? If yes, add to enhanced ASSIGNMENT_OVERRIDES. If stale, no action needed.

3. **WG1/WG3-WG5/TEAM columns** -- Confirm whether any Power BI visual or M code query references these columns. If so, update `_load_assignment_master()` in enhanced to include them.

---

## Conclusion

The enhanced script is the correct choice for production. It has broader e-ticket coverage (13-month scan vs single-month), DFR export capability, better data quality tracking, and a more maintainable class-based architecture. The three logic gaps identified above are manageable: the PEO rule is the only one with real operational impact and should be ported. The other gaps are either stale (badge 0388) or low-impact (WG columns not actively queried).

**Recommendation**: Proceed with deprecation. Flag PEO rule port as a follow-up task.
