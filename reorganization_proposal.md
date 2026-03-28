# Reorganization Proposal

*Generated: 2026-03-28*
*Status: PROPOSAL ONLY -- no files moved or deleted*

## Overview

The Summons repository has 5 active Python scripts and ~75 dead scripts in root, plus ~25 stale markdown files, empty junk files, and broken batch files. The `archive/` directory already exists and contains copies of most dead scripts, but the originals remain in root.

## Phase 1: Delete Empty Junk Files

These are 0-byte files created by misdirected terminal commands. Safe to delete.

| File | Size | Origin |
|---|---|---|
| `#` | 0 bytes | Shell comment character |
| `cd` | 0 bytes | Misdirected `cd` command |
| `echo` | 0 bytes | Misdirected `echo` command |
| `python` | 0 bytes | Misdirected `python` command |
| `_ul` | 0 bytes | Unknown artifact |

## Phase 2: Delete OneDrive Sync Duplicates

These `(1)` and `(3)` files are OneDrive sync conflicts. The original versions exist.

| File | Original |
|---|---|
| `README (1).md` | `README.md` |
| `README_SUMMONS_SCRIPTS (1).md` | `README_SUMMONS_SCRIPTS.md` |
| `SOLUTION_SUMMARY (1).md` | `SOLUTION_SUMMARY.md` |
| `SummonsMaster_Simple (1).py` | `SummonsMaster_Simple.py` |
| `summons_etl_final (3).py` | `summons_etl_final.py` |

## Phase 3: Archive Dead Python Scripts

75 Python scripts in root are superseded by `SummonsMaster_Simple.py` and `summons_etl_enhanced.py`. Most already have copies in `archive/scripts/`. Move originals to `archive/scripts/` (overwrite existing copies since they're identical).

See `docs/file-inventory.md` for the complete list organized by category:
- ETL variants (31 files)
- Badge/assignment fix scripts (15 files)
- Date/data fix scripts (10 files)
- Debug/diagnostic scripts (22 files)

## Phase 4: Archive Stale Markdown Documentation

Move to `archive/docs/`:

| File | Reason |
|---|---|
| `CLEANUP_COMPLETED.md` | Historical reference only |
| `CLEANUP_PLAN.md` | Historical reference only |
| `Claude-Data column missing in police department query.md` | AI chat log |
| `Claude-E-ticket summons migration validation.md` | AI chat log |
| `DATA_DUPLICATION_FIX_SUMMARY.md` | Oct 2025 fix notes |
| `DATA_FIX_SUMMARY.md` | Historical fix notes |
| `DATA_SOURCE_VERIFICATION_GUIDE.md` | Superseded by docs/ |
| `DAX_M_Code_Fix_Summary.md` | Historical fix notes |
| `Email_Summary_for_Supervisor.txt` | One-off communication |
| `Executive_Summary_Data_Reconciliation.md` | Historical reference |
| `POWERBI_DASHBOARD_FIX_GUIDE.md` | Historical fix guide |
| `POWERBI_M_CODE_COMPATIBILITY.md` | Historical reference |
| `PYTHON_WORKSPACE_AI_GUIDE.md` | Historical reference |
| `PYTHON_WORKSPACE_TEMPLATE.md` | Historical reference |
| `QUERY_NAME_UPDATE_SUMMARY.md` | Jan 2026 naming changes |
| `QUICK_START_TRANSITION.md` | Transition guide (complete) |
| `Query_Naming_Recommendations.md` | Completed recommendations |
| `README_SUMMONS_SCRIPTS.md` | Superseded by README.md + docs/ |
| `SOLUTION_SUMMARY.md` | Historical architecture notes |
| `TRANSITION_SCRIPT_SUMMARY.md` | Transition guide (complete) |
| `cursor_script_directory_cleanup_and_org.md` | Prior cleanup plan |
| `summons_schema_report.md` | Schema reference (in CLAUDE.md now) |

## Phase 5: Archive Stale Data/Text Files in Root

| File | Reason |
|---|---|
| `ats_query_sample.txt` | Copy in `documents/` |
| `chatlog_fix_assisgnemnt_master.txt` | Copy in `documents/` |
| `claude_chatlog.txt` | Copy in `documents/` |
| `final_assignment.csv` | Old data output |
| `final_assignment_csv.txt` | Old data output |
| `historica_ats_table_preview.txt` | Copy in `documents/` |
| `orchestrator.log` | Old log |
| `previous_month_dax.txt` | Copy in `documents/` |
| `python_message.txt` | Copy in `documents/` |
| `reconciliation_summary.txt` | Old report |
| `requirements_txt.txt` | Copy in `documents/` |
| `summons_changelog_20251110_130900.txt` | Old changelog |
| `summons_etl_enhanced.txt` | Text copy of script |
| `summons_etl_enhanced.py.backup_20260217_212244` | Backup file |
| `traffic_dax1csv.csv` | Old data export |
| `traffic_dax2.csv` | Old data export |
| `traffic_m_code.txt` | Copy in `documents/` |
| `updated_script_summons.txt` | Copy in `documents/` |
| `preview_table_ATS_Court_Data_Post_Update.csv` | Old test data |
| `summons_powerbi_latest_summons_data.csv` | Old data export |
| `summons_13month_trend_preview_table.csv` | Old test data |
| `2025_10_summon_preview_table.csv` | Old test data |
| `summons_diagnostic_package.tar.gz` | Diagnostic archive |

## Phase 6: Archive Batch Files

Both batch files reference scripts that no longer exist.

| File | Reason |
|---|---|
| `SummonsMaster_ETL_Batch_Executor.bat` | References `simple_perfect_etl.py` (doesn't exist) |
| `Focused_ETL_Batch_Executor.bat` | References `focused_summons_etl_current_month.py` (doesn't exist) |

## Phase 7: Archive Legacy M Code and Config

| File | Reason |
|---|---|
| `Backfill_Query_Fix.m` | Superseded by root M code |
| `Backfill_Query_Fixed_Complete.m` | Superseded by root M code |
| `powerbi_query_corrected.m` | Superseded by root M code |
| `config.yaml` | Not loaded by any active script |
| `emergency_config.yaml` | July 2025 emergency artifact |

The entire `m_code/` directory could be archived since root `.m` files (Jan 2026) supersede all contents.

## Phase 8: Investigate `#` Directory

The `#` entry appears as both a 0-byte file and a directory in the filesystem listing. This is a shell artifact. It should be removed.

## Phase 9: Clean Up Other Directories

- `nppBackup/` -- Notepad++ backup files. Archive or add to .gitignore.
- `__pycache__/` -- Already in .gitignore. Can be deleted locally.
- `documents/` -- Contains valuable DAX references and historical context. Keep but consider renaming to `reference/` for clarity.
- `logs/` -- Empty directory. Keep for future use.
- `archive/tests/` -- Empty directory. Remove or populate.

## Proposed Final Structure

```
Summons/
  SummonsMaster_Simple.py        # Primary ETL
  summons_etl_enhanced.py        # DFR ETL
  summons_backfill_13month.py    # Backfill builder
  update_dfr_violation_lookup.py # DFR lookup updater
  run_eticket_export.py          # Test wrapper
  summons_13month_trend.m        # M Code: trend
  summons_top5_moving.m          # M Code: top 5 moving
  summons_top5_parking.m         # M Code: top 5 parking
  all_summons_queries_m_code.txt # M Code: reference
  CLAUDE.md                      # AI handoff
  README.md                      # Project overview
  CHANGELOG.md                   # Version history
  SUMMARY.md                     # Project summary
  CONTRIBUTING.md                # Contribution guidelines
  .gitignore
  Summons.code-workspace
  DAX/                           # DAX measures (2 files)
  docs/                          # Technical docs (3 files)
  backfill_data/                 # Essential data (1 file)
  documents/                     # Reference material
  archive/                       # All archived material
```

This reduces root from ~150 files to ~18 files.
