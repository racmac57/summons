# Changelog

All notable changes to the Summons ETL pipeline are documented in this file.

## 2026-03-28 -- Documentation Audit

- Complete repository audit and documentation refresh
- Updated CLAUDE.md with full ETL pipeline map, tech debt inventory, and known issues
- Updated README.md with current repository structure
- Created CHANGELOG.md, SUMMARY.md, CONTRIBUTING.md
- Updated docs/etl-pipeline.md, docs/file-inventory.md, docs/config-reference.md
- Created reorganization_proposal.md for pending cleanup
- Updated .gitignore with project-specific exclusions
- Identified 75+ dead scripts, 5 empty junk files, and stale batch files

## 2026-02-17 -- summons_etl_enhanced.py Update

- Updated DFR configuration and ASSIGNMENT_OVERRIDES
- Badge 2025 conditional override for FIRE LANES violations
- Badge 0738 (Polson, CIV) added as DFR operator

## 2026-01-11 -- M Code Standardization

- Rewrote root-level M code queries (summons_13month_trend.m, summons_top5_moving.m, summons_top5_parking.m)
- Standardized query naming from `___Top_5_*` to `summons_top5_*`
- All M code now reads from `summons_powerbi_latest.xlsx` / `Summons_Data` sheet
- Added SortKey column to 13-month trend for proper date ordering

## 2025-12 -- SummonsMaster_Simple.py Created

- New simplified ETL replacing the multi-script approach
- Backfill + e-ticket + Assignment Master in a single script
- Rolling 13-month window with automatic date math
- ASSIGNMENT_OVERRIDES dict for temporary personnel

## 2025-11-10 -- Data Duplication Fix

- Fixed Power BI showing incorrect counts due to COUNTROWS vs SUM(TICKET_COUNT)
- Created DAX/TICKET_COUNT_MEASURES.dax with correct SUM-based measures
- Query naming standardization documented in QUERY_NAME_UPDATE_SUMMARY.md

## 2025-10-14 -- M Code Overhaul (m_code/ directory)

- Created FINAL_* M code versions for all Power BI queries
- ATS_Court_Data series of M code files for data source testing
- M_CODE_UPDATE_GUIDE.md and FINAL_M_CODE_GUIDE.md created

## 2025-09 -- E-Ticket Transition

- Moved from court-based SUMMONS_MASTER.xlsx to e-ticket CSV exports
- Created summons_etl_enhanced.py for the new data format
- DFR export capability added for drone enforcement tracking

## 2025-07 -- Badge Assignment Fixes

- Multiple badge matching iterations (badge_assignment_fix_*.py series)
- Assignment Master V2 introduced (CSV format, replacing XLSM)
- Batch executors created (SummonsMaster_ETL_Batch_Executor.bat, Focused_ETL_Batch_Executor.bat)

## 2025-06-28 -- Initial Project

- Initial commit with config.yaml, process_monthly_summons.py
- Historical court data processing from SUMMONS_MASTER.xlsx
- ATS report integration
