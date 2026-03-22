#!/usr/bin/env python3
# summons_backfill_13month.py
# Purpose: Build a 13-month staging file for the summons_13month_trend Power BI visual.
#          Processes historical e-ticket CSVs (Jan 2025 - Dec 2025) using the same
#          Assignment Master join as summons_etl_enhanced.py, then merges with the
#          existing Jan 2026 staging data.
# Run: python summons_backfill_13month.py

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

BASE = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack")

ASSIGNMENT_MASTER  = BASE / "06_Workspace_Management" / "Assignment_Master_V2.csv"
STAGING_CURRENT    = BASE / "03_Staging" / "Summons" / "summons_powerbi_latest.xlsx"
STAGING_OUT        = BASE / "03_Staging" / "Summons" / "summons_powerbi_latest.xlsx"
STAGING_BACKUP     = BASE / "03_Staging" / "Summons" / f"summons_powerbi_pre_backfill_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

# Historical CSVs in chronological order (Jan 2025 - Dec 2025)
# 2025_07 landed in the root instead of 2025/month
HISTORICAL_FILES = [
    BASE / "05_EXPORTS" / "_Summons" / "E_Ticket" / "2025" / "month" / "2025_01_eticket_export.csv",
    BASE / "05_EXPORTS" / "_Summons" / "E_Ticket" / "2025" / "month" / "2025_02_eticket_export.csv",
    BASE / "05_EXPORTS" / "_Summons" / "E_Ticket" / "2025" / "month" / "2025_03_eticket_export.csv",
    BASE / "05_EXPORTS" / "_Summons" / "E_Ticket" / "2025" / "month" / "2025_04_eticket_export.csv",
    BASE / "05_EXPORTS" / "_Summons" / "E_Ticket" / "2025" / "month" / "2025_05_eticket_export.csv",
    BASE / "05_EXPORTS" / "_Summons" / "E_Ticket" / "2025" / "month" / "2025_06_eticket_export.csv",
    BASE / "05_EXPORTS" / "_Summons" / "E_Ticket" / "2025_07_eticket_export.csv",   # root location
    BASE / "05_EXPORTS" / "_Summons" / "E_Ticket" / "2025" / "month" / "2025_08_eticket_export.csv",
    BASE / "05_EXPORTS" / "_Summons" / "E_Ticket" / "2025" / "month" / "2025_09_eticket_export.csv",
    BASE / "05_EXPORTS" / "_Summons" / "E_Ticket" / "2025" / "month" / "2025_10_eticket_export.csv",
    BASE / "05_EXPORTS" / "_Summons" / "E_Ticket" / "2025" / "month" / "2025_11_eticket_export.csv",
    BASE / "05_EXPORTS" / "_Summons" / "E_Ticket" / "2025" / "month" / "2025_12_eticket_export.csv",
]


def load_assignment_master():
    df = pd.read_csv(ASSIGNMENT_MASTER, dtype=str)
    logging.info(f"Assignment Master: {len(df)} rows, schema={'V3' if 'STANDARD_NAME' in df.columns else 'V2'}")

    if 'STANDARD_NAME' in df.columns:
        display_col = 'STANDARD_NAME'
    elif 'Proposed 4-Digit Format' in df.columns:
        display_col = 'Proposed 4-Digit Format'
    else:
        raise ValueError("Assignment Master missing display name column")

    keep = [display_col, 'PADDED_BADGE_NUMBER'] + [c for c in ('WG2', 'TITLE', 'LAST_NAME') if c in df.columns]
    df = df[keep].copy()
    df = df.rename(columns={display_col: 'OFFICER_DISPLAY_NAME'})
    df['PADDED_BADGE_NUMBER'] = df['PADDED_BADGE_NUMBER'].astype(str).str.strip().str.zfill(4)
    df = df.drop_duplicates(subset='PADDED_BADGE_NUMBER').reset_index(drop=True)
    return df


def process_eticket_csv(csv_path: Path, assignment_master: pd.DataFrame) -> pd.DataFrame:
    """Read one e-ticket CSV, join to assignment master, return standardised DataFrame."""
    logging.info(f"Processing: {csv_path.name}")
    # Auto-detect delimiter (2025 files use semicolons, 2026 files use commas)
    raw = None
    for enc in ('utf-8', 'latin-1', 'cp1252'):
        for sep in (';', ','):
            try:
                df_test = pd.read_csv(
                    csv_path, dtype=str, low_memory=False,
                    encoding=enc, sep=sep, on_bad_lines='skip', nrows=2
                )
                if len(df_test.columns) > 5:
                    raw = pd.read_csv(
                        csv_path, dtype=str, low_memory=False,
                        encoding=enc, sep=sep, on_bad_lines='skip'
                    )
                    logging.info(f"  Read OK: encoding={enc} sep='{sep}' cols={len(raw.columns)} rows={len(raw)}")
                    break
            except Exception:
                pass
        if raw is not None:
            break

    if raw is None:
        logging.error(f"  Could not read {csv_path.name}")
        return pd.DataFrame()

    if 'Officer Id' not in raw.columns or 'Issue Date' not in raw.columns:
        logging.error(f"  Missing required columns in {csv_path.name}")
        return pd.DataFrame()

    raw['PADDED_BADGE_NUMBER'] = raw['Officer Id'].astype(str).str.strip().str.zfill(4)

    # Determine TYPE: 'Case Type Code' P=parking M=moving C=criminal
    if 'Case Type Code' in raw.columns:
        raw['TYPE'] = raw['Case Type Code'].astype(str).str.strip().str.upper()
    else:
        raw['TYPE'] = 'U'

    raw['OFFICER_NAME_RAW'] = (
        raw.get('Officer Last Name', pd.Series([''] * len(raw))).fillna('') + ', ' +
        raw.get('Officer First Name', pd.Series([''] * len(raw))).fillna('')
    ).str.strip(', ')

    # Parse issue date for month/year keys
    raw['ISSUE_DATE'] = pd.to_datetime(raw['Issue Date'], errors='coerce')
    valid = raw[raw['ISSUE_DATE'].notna()].copy()

    valid['Year']         = valid['ISSUE_DATE'].dt.year
    valid['Month']        = valid['ISSUE_DATE'].dt.month
    valid['YearMonthKey'] = valid['Year'] * 100 + valid['Month']
    valid['Month_Year']   = valid['Month'].astype(str).str.zfill(2) + '-' + valid['Year'].astype(str).str[-2:]

    # Join assignment master
    merged = valid.merge(assignment_master, on='PADDED_BADGE_NUMBER', how='left')
    merged['WG2'] = merged['WG2'].fillna('UNKNOWN')
    merged['OFFICER_DISPLAY_NAME'] = merged['OFFICER_DISPLAY_NAME'].fillna('UNKNOWN')
    merged['DATA_QUALITY_SCORE'] = 100
    merged['ETL_VERSION']        = 'backfill_v1.0'
    merged['PROCESSED_TIMESTAMP'] = datetime.now()

    matched   = merged['WG2'].ne('UNKNOWN').sum()
    unmatched = merged['WG2'].eq('UNKNOWN').sum()
    logging.info(f"  {len(merged)} rows | matched={matched} unmatched={unmatched}")

    # Keep same column set as the ETL staging output
    keep_cols = [
        'PADDED_BADGE_NUMBER', 'OFFICER_DISPLAY_NAME', 'WG2',
        'TYPE', 'YearMonthKey', 'Month_Year', 'Year', 'Month',
        'ISSUE_DATE', 'OFFICER_NAME_RAW', 'DATA_QUALITY_SCORE',
        'ETL_VERSION', 'PROCESSED_TIMESTAMP'
    ]
    if 'TITLE' in merged.columns:
        keep_cols.insert(3, 'TITLE')

    return merged[[c for c in keep_cols if c in merged.columns]].copy()


def main():
    logging.info("=== Summons 13-Month Backfill ===")

    # 1. Load assignment master
    am = load_assignment_master()

    # 2. Load existing Jan 2026 staging data (already processed by the ETL)
    logging.info(f"Loading existing staging: {STAGING_CURRENT.name}")
    existing = pd.read_excel(STAGING_CURRENT, sheet_name='Summons_Data', dtype=str)
    logging.info(f"  Existing rows: {len(existing)}")

    # Back up the current staging file before overwriting
    import shutil
    shutil.copy2(STAGING_CURRENT, STAGING_BACKUP)
    logging.info(f"  Backup saved: {STAGING_BACKUP.name}")

    # 3. Process each historical CSV
    backfill_frames = []
    for csv_path in HISTORICAL_FILES:
        if not csv_path.exists():
            logging.warning(f"  FILE NOT FOUND — skipping: {csv_path}")
            continue
        df = process_eticket_csv(csv_path, am)
        if not df.empty:
            backfill_frames.append(df)

    if not backfill_frames:
        logging.error("No historical data processed — aborting.")
        return

    backfill = pd.concat(backfill_frames, ignore_index=True)
    logging.info(f"Historical records total: {len(backfill)}")

    # 4. Align columns — add any missing cols to backfill so concat works cleanly
    for col in existing.columns:
        if col not in backfill.columns:
            backfill[col] = np.nan
    backfill = backfill[[c for c in existing.columns if c in backfill.columns]]

    # 5. Filter existing to Jan 2026 only (YearMonthKey=202601) — exclude partial Feb spill
    existing['YearMonthKey'] = pd.to_numeric(existing['YearMonthKey'], errors='coerce')
    jan2026 = existing[existing['YearMonthKey'] == 202601].copy()
    logging.info(f"Jan 2026 rows from existing staging: {len(jan2026)}")

    # 6. Combine: 12 months backfill + Jan 2026
    combined = pd.concat([backfill, jan2026], ignore_index=True)
    combined = combined.sort_values(['YearMonthKey', 'PADDED_BADGE_NUMBER']).reset_index(drop=True)

    # 7. Month distribution summary
    logging.info("=== Combined month distribution ===")
    dist = combined.groupby('YearMonthKey').size().sort_index()
    for k, v in dist.items():
        logging.info(f"  {k}: {v} rows")
    logging.info(f"  TOTAL: {len(combined)} rows across {len(dist)} months")

    # 8. Save
    logging.info(f"Saving to: {STAGING_OUT}")
    with pd.ExcelWriter(STAGING_OUT, engine='openpyxl') as writer:
        combined.to_excel(writer, sheet_name='Summons_Data', index=False)

    # Also save a timestamped copy
    ts_path = STAGING_OUT.parent / f"summons_powerbi_{datetime.now().strftime('%Y%m%d_%H%M%S')}_13month.xlsx"
    import shutil
    shutil.copy2(STAGING_OUT, ts_path)

    logging.info(f"Done. Staging file updated with {len(combined)} rows across {len(dist)} months.")
    logging.info(f"Timestamped copy: {ts_path.name}")


if __name__ == "__main__":
    main()
