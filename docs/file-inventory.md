# File Inventory

*Generated: 2026-03-28*

## Active Production Files

| File | Lines | Role | Status |
|---|---|---|---|
| `SummonsMaster_Simple.py` | 711 | Primary production ETL | Active |
| `summons_etl_enhanced.py` | 833 | E-ticket ETL with DFR export | Active |
| `summons_backfill_13month.py` | 213 | Historical backfill builder | Active (rare use) |
| `update_dfr_violation_lookup.py` | 206 | DFR violation reference updater | Active |
| `run_eticket_export.py` | 16 | Test wrapper for summons_etl_enhanced | Active |

## M Code / Power Query -- Production (Root)

| File | Query | Updated |
|---|---|---|
| `summons_13month_trend.m` | summons_13month_trend | 2026-01-11 |
| `summons_top5_moving.m` | summons_top5_moving | 2026-01-11 |
| `summons_top5_parking.m` | summons_top5_parking | 2026-01-11 |
| `all_summons_queries_m_code.txt` | All queries compiled | Reference |

## M Code -- Legacy (Root)

| File | Status |
|---|---|
| `Backfill_Query_Fix.m` | Superseded |
| `Backfill_Query_Fixed_Complete.m` | Superseded |
| `powerbi_query_corrected.m` | Superseded |

## M Code -- Legacy (`m_code/` directory, 18 files)

| File | Notes |
|---|---|
| `ATS_Court_Data.m` | Base data loader (Oct 2025) |
| `ATS_Court_Data_ALL_SOURCES.m` | Multi-source variant |
| `ATS_Court_Data_CLEAN.m` | Cleaned variant |
| `ATS_Court_Data_FIXED.m` | Fixed variant |
| `ATS_Court_Data_UPDATED.m` | Updated variant |
| `Data_Source_Verification.m` | Verification helper |
| `FINAL_13_Month_Moving_Trend.m` | Superseded by root summons_13month_trend.m |
| `FINAL_13_Month_Parking_Trend.m` | Superseded by root summons_13month_trend.m |
| `FINAL_Top_5_Moving_Sept_2025.m` | Superseded by root summons_top5_moving.m |
| `FINAL_Top_5_Parking_Sept_2025.m` | Superseded by root summons_top5_parking.m |
| `Patrol_Summons.m` | Patrol bureau filter |
| `Previous_Month_ATS_Court_Data_DYNAMIC.m` | Dynamic month filter |
| `Top_5_Moving_Violations.m` | Pre-rename version |
| `Top_5_Moving_Violations_UPDATED.m` | Updated pre-rename |
| `Top_5_Parking_Violations.m` | Pre-rename version |
| `Top_5_Parking_Violations_UPDATED.m` | Updated pre-rename |
| `2025_11_10_14_45_25____summons.m` | Timestamp-named artifact |
| `FINAL_M_CODE_GUIDE.md` | Guide doc in M code dir |
| `M_CODE_UPDATE_GUIDE.md` | Update guide in M code dir |

## DAX Measures

| File | Measures |
|---|---|
| `DAX/TICKET_COUNT_MEASURES.dax` | Total, Moving, Parking, Complaints, MTD, Prior Month |
| `DAX/Top_5_Moving_Subtitle` | Dynamic subtitle for Top 5 card |

## Configuration Files

| File | Status |
|---|---|
| `config.yaml` | Stale -- not loaded by any active script |
| `emergency_config.yaml` | Stale -- July 2025 emergency artifact |
| `Summons.code-workspace` | VS Code workspace (minimal) |

## Batch Files

| File | Status |
|---|---|
| `SummonsMaster_ETL_Batch_Executor.bat` | Dead -- references nonexistent scripts |
| `Focused_ETL_Batch_Executor.bat` | Dead -- references `focused_summons_etl_current_month.py` (doesn't exist) |

## Dead Python Scripts (Root) -- 75 files

These scripts are superseded by `SummonsMaster_Simple.py` and `summons_etl_enhanced.py`. All have copies in `archive/scripts/` already. They should be removed from root.

### ETL Variants (superseded)
`SummonsMaster.py`, `SummonsMaster_Drop_In.py`, `SummonsMaster_Transition.py`, `enhanced_monthly_summons_etl.py`, `enhanced_police_analytics_final.py`, `etl_correct_paths.py`, `etl_data_type_fix.py`, `etl_final_working.py`, `etl_with_june_2024.py`, `fixed_badge_matching_etl.py`, `fixed_historical_summons_etl.py`, `historical_summons_etl.py`, `main_orchestrator.py`, `perfect_100_percent_etl.py`, `police_analytics_structure.py`, `process_monthly_summons.py`, `rolling_12_month_etl_fix.py`, `rolling_13_month_etl.py`, `rolling_13_month_etl_clean.py`, `rolling_13_month_etl_final.py`, `rolling_13_month_etl_fixed.py`, `simple_perfect_etl.py`, `summons_etl_ats.py`, `summons_etl_ats_fixed.py`, `summons_etl_final.py`, `summons_etl_final (3).py`, `summons_etl_fixed.py`, `summons_etl_fixed_paths.py`, `summons_etl_script.py`, `summons_utils.py`, `summons_utils_final.py`

### Badge/Assignment Fix Scripts (one-off)
`assignment_fix_script.py`, `assignment_merger.py`, `assignment_v2_diagnostic.py`, `badge_assignment_fix.py`, `badge_assignment_fix_FINAL.py`, `badge_assignment_fix_JUNE_2025.py`, `badge_assignment_fix_JUNE_2025_CORRECTED.py`, `badge_assignment_fix_UPDATED.py`, `badge_diagnostic.py`, `badge_format_fix_etl.py`, `final_badge_fix.py`, `quick_fix_assignment_issue.py`, `run_badge_fix_script.py`, `updated_assignment_columns.py`, `updated_badge_fix.py`

### Date/Data Fix Scripts (one-off)
`June_2024_Column_Alignment_Fix.py`, `June_2024_Inclusion_Fix.py`, `Rolling_12_Month_Previous_Month_Fix.py`, `Simple_June_2024_Fix.py`, `extract_june_2024_simple.py`, `find_june_data.py`, `fix_date_range_june_2024.py`, `fix_ticket_numbers.py`, `june_2024_date_range_fix.py`, `summons_merge_fix.py`

### Debug/Diagnostic Scripts (one-off)
`analyze_discrepancy.py`, `check_summons_sheets.py`, `create_clean_output.py`, `debug_clearance_dax.py`, `debug_clearance_value.py`, `debug_previous_month_data.py`, `debug_previous_month_data_fixed.py`, `detailed_comparison.py`, `emergency_file_finder.py`, `etl_diagnostic.py`, `file_finder_script.py`, `investigate_court_data.py`, `june_data_diagnostic.py`, `powerbi_file_fix.py`, `force_powerbi_file_fix.py`, `reconciliation_report.py`, `summons_merge_diagnostic.py`, `test_analytics.py`, `test_summons_utils.py`, `test_transition_config.py`, `verify_output.py`, `file_migration_executor.py`

### Duplicate (OneDrive sync conflicts)
`SummonsMaster_Simple (1).py`, `summons_etl_final (3).py`

## Empty Junk Files (shell artifacts)

| File | Size | Notes |
|---|---|---|
| `#` | 0 bytes | Likely `#` comment pasted as filename |
| `cd` | 0 bytes | Misdirected `cd` command |
| `echo` | 0 bytes | Misdirected `echo` command |
| `python` | 0 bytes | Misdirected `python` command |
| `_ul` | 0 bytes | Unknown artifact |

## Stale Documentation (Root)

| File | Status |
|---|---|
| `CLEANUP_COMPLETED.md` | Archive candidate |
| `CLEANUP_PLAN.md` | Archive candidate |
| `Claude-Data column missing in police department query.md` | AI chat log -- archive |
| `Claude-E-ticket summons migration validation.md` | AI chat log -- archive |
| `DATA_DUPLICATION_FIX_SUMMARY.md` | Reference -- archive or keep |
| `DATA_FIX_SUMMARY.md` | Archive candidate |
| `DATA_SOURCE_VERIFICATION_GUIDE.md` | Reference -- archive or keep |
| `DAX_M_Code_Fix_Summary.md` | Archive candidate |
| `Email_Summary_for_Supervisor.txt` | Archive candidate |
| `Executive_Summary_Data_Reconciliation.md` | Archive candidate |
| `POWERBI_DASHBOARD_FIX_GUIDE.md` | Archive candidate |
| `POWERBI_M_CODE_COMPATIBILITY.md` | Archive candidate |
| `PYTHON_WORKSPACE_AI_GUIDE.md` | Archive candidate |
| `PYTHON_WORKSPACE_TEMPLATE.md` | Archive candidate |
| `QUERY_NAME_UPDATE_SUMMARY.md` | Reference -- archive or keep |
| `QUICK_START_TRANSITION.md` | Archive candidate |
| `Query_Naming_Recommendations.md` | Archive candidate |
| `README (1).md` | Duplicate -- delete |
| `README_SUMMONS_SCRIPTS.md` | Superseded by docs/ |
| `README_SUMMONS_SCRIPTS (1).md` | Duplicate -- delete |
| `SOLUTION_SUMMARY.md` | Reference -- archive or keep |
| `SOLUTION_SUMMARY (1).md` | Duplicate -- delete |
| `TRANSITION_SCRIPT_SUMMARY.md` | Archive candidate |
| `cursor_script_directory_cleanup_and_org.md` | Archive candidate |
| `summons_schema_report.md` | Archive candidate |

## Subdirectories

| Directory | Contents | Files |
|---|---|---|
| `archive/` | Archived legacy scripts, tests, docs, data | ~130 files |
| `archive/batch/` | Archived batch files | 2 |
| `archive/data/` | Archived data/logs | 20 |
| `archive/docs/` | Archived documentation | 9 |
| `archive/scripts/` | Archived Python scripts | ~95 |
| `archive/tests/` | Empty | 0 |
| `backfill_data/` | Historical summary CSV | 1 |
| `data_samples/` | Test/reference data exports | 5 |
| `DAX/` | Power BI DAX measures | 2 |
| `docs/` | Technical documentation | 3 |
| `documents/` | Chat logs, development notes, old logs | 21 |
| `logs/` | Empty (reserved) | 0 |
| `m_code/` | Legacy Power Query M code | 18 |
| `nppBackup/` | Notepad++ editor backups | 8 |
| `visual_export/` | Power BI visual test export | 1 |
| `__pycache__/` | Python bytecode cache | 6 |
