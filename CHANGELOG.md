# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

## [2026-03-28] — swarm-run
### Added
- `CHANGELOG.md` (this file) -- Root-level changelog
- `SUMMARY.md` -- Status summary
- `CONTRIBUTING.md` -- Contributor guidelines stub
- `reorganization_proposal.md` -- Proposed cleanup plan (75+ dead scripts)
- `findings.json` -- Swarm audit findings
- `docs/etl-diff-analysis.md` -- Comparison of enhanced vs simple ETL scripts

### Changed
- `CLAUDE.md` -- Updated with full ETL pipeline map, tech debt inventory, and known issues
- `README.md` -- Updated with current repository structure
- `.gitignore` -- Updated with project-specific exclusions
- `docs/etl-pipeline.md` -- Updated ETL pipeline documentation
- `docs/file-inventory.md` -- Updated file inventory
- `docs/config-reference.md` -- Updated configuration reference
- `SummonsMaster_Simple.py` moved to `archive/deprecated/SummonsMaster_Simple_DEPRECATED_2026-03-28.py`

### Deprecated
- `SummonsMaster_Simple.py` -- deprecated in favor of `summons_etl_enhanced.py` (authoritative)
- 75+ dead Python scripts in root identified for archival
- 5 empty junk files (#, cd, echo, python, _ul) identified for deletion
- 2 dead batch files (SummonsMaster_ETL_Batch_Executor.bat, Focused_ETL_Batch_Executor.bat) identified for archival

## [2026-02-17]
### Changed
- `summons_etl_enhanced.py` -- Updated DFR configuration and ASSIGNMENT_OVERRIDES
- Badge 2025 conditional override for FIRE LANES violations
- Badge 0738 (Polson, CIV) added as DFR operator

## [2026-01-11]
### Changed
- Rewrote root-level M code queries (summons_13month_trend.m, summons_top5_moving.m, summons_top5_parking.m)
- Standardized query naming from `___Top_5_*` to `summons_top5_*`
- All M code now reads from `summons_powerbi_latest.xlsx` / `Summons_Data` sheet
- Added SortKey column to 13-month trend for proper date ordering

## [2025-12]
### Added
- `SummonsMaster_Simple.py` -- New simplified ETL replacing the multi-script approach
- Backfill + e-ticket + Assignment Master in a single script
- Rolling 13-month window with automatic date math
- ASSIGNMENT_OVERRIDES dict for temporary personnel

## [2025-11-10]
### Fixed
- Power BI showing incorrect counts due to COUNTROWS vs SUM(TICKET_COUNT)
- Created `DAX/TICKET_COUNT_MEASURES.dax` with correct SUM-based measures
- Query naming standardization documented in QUERY_NAME_UPDATE_SUMMARY.md

## [2025-10-14]
### Added
- Created FINAL_* M code versions for all Power BI queries in `m_code/` directory
- ATS_Court_Data series of M code files for data source testing
- M_CODE_UPDATE_GUIDE.md and FINAL_M_CODE_GUIDE.md

## [2025-09]
### Changed
- Moved from court-based SUMMONS_MASTER.xlsx to e-ticket CSV exports
- Created `summons_etl_enhanced.py` for the new data format
- DFR export capability added for drone enforcement tracking

## [2025-07]
### Fixed
- Multiple badge matching iterations (badge_assignment_fix_*.py series)
- Assignment Master V2 introduced (CSV format, replacing XLSM)
- Batch executors created

## [2025-06-28]
### Added
- Initial commit with config.yaml, process_monthly_summons.py
- Historical court data processing from SUMMONS_MASTER.xlsx
- ATS report integration
