#!/usr/bin/env python3
"""
SummonsMaster_Simple.py - Simplified Summons Processing
Processes:
  - Historical backfill: Sep 2024 - Aug 2025 (from August dashboard CSV)
  - Current court data: Sep 2024 - Sep 2025 (from September dashboard CSV)
  - Current e-ticket: Sep 2025 only (from e-ticket export)
Outputs: Clean data for Power BI with proper badge number formatting
"""

import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from pathlib import Path
import logging
import sys

# =========================
# CONFIGURATION
# =========================

# Latest consolidated backfill (aggregate) data
BACKFILL_FILE_OVERRIDE = Path(
    r"C:\Users\carucci_r\OneDrive - City of Hackensack\PowerBI_Data\Backfill\2025_12\summons\2025_12_department_wide_summons.csv"
)
BACKFILL_BASE_DIR = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\PowerBI_Data\Backfill")
BACKFILL_FILENAME = "2025_12_department_wide_summons.csv"

# Folder containing monthly e-ticket exports (one CSV per month)
ETICKET_FOLDER = Path(
    r"C:\Users\carucci_r\OneDrive - City of Hackensack\05_EXPORTS\_Summons\E_Ticket"
)

# Optional override for testing specific dates
TODAY_OVERRIDE = None

# Assignment Master
ASSIGNMENT_FILE = Path(
    r"C:\Users\carucci_r\OneDrive - City of Hackensack\09_Reference\Personnel\Assignment_Master_V2.csv"
)

# Manual overrides for badges missing from assignment file or temp assignments
ASSIGNMENT_OVERRIDES = {
    # Example: Badge 0388 currently missing from assignment master but needed for bureau reporting
    "0388": {
        "OFFICER_DISPLAY_NAME": "LIGGIO, PO",
        "TEAM": "PATROL",
        "WG1": "OPERATIONS DIVISION",
        "WG2": "PATROL BUREAU",
        "WG3": "PLATOON A",
        "WG4": "A3",
        "WG5": "",
        "POSS_CONTRACT_TYPE": "",
    },
    # Feb 2026: PEO Mariah Ramirez (badge 2025) on temp assignment to SSOCC for drone/firezone work.
    # Only FIRE LANES violations from badge 2025 show as SSOCC; other violations stay Traffic Bureau.
    "2025": {
        "_condition": {"column": "VIOLATION_DESCRIPTION", "contains": "FIRE LANES"},
        "OFFICER_DISPLAY_NAME": "M. RAMIREZ #2025",
        "TEAM": "Safe Streets Operations",
        "WG1": "OPERATIONS DIVISION",
        "WG2": "SAFE STREETS OPERATIONS CONTROL CENTER",
        "WG3": "PEO",
        "WG4": "",
        "WG5": "",
        "POSS_CONTRACT_TYPE": "PARKING ENFORCEMENT OFF",
    },
}

# Output
OUTPUT_FILE = Path(
    r"C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons\summons_powerbi_latest.xlsx"
)

LOG_FILE = "summons_simple_processing.log"

# =========================
# LOGGING SETUP
# =========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ],
)
log = logging.getLogger("summons_simple")

# =========================
# HELPER FUNCTIONS
# =========================

def pad_badge_number(badge_value):
    """
    Convert badge number to 4-digit zero-padded TEXT format.
    Handles integers, floats, and strings.
    Returns string with leading zeros (e.g., "0123", "1234").
    """
    if pd.isna(badge_value) or badge_value == "":
        return ""

    # Convert to string and remove any decimal points and trailing zeros
    badge_str = str(badge_value).strip()

    # If it's a float-like string (e.g., "123.0"), take only the integer part
    if "." in badge_str:
        badge_str = badge_str.split(".")[0]

    # Remove any non-digit characters
    badge_str = "".join(c for c in badge_str if c.isdigit())

    # Pad to 4 digits with leading zeros
    return badge_str.zfill(4)

def parse_date(date_value):
    """Parse dates from various formats"""
    if pd.isna(date_value) or str(date_value).strip() == "":
        return pd.NaT

    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%m/%d/%y"):
        try:
            return pd.to_datetime(date_value, format=fmt, errors="raise")
        except:
            pass

    return pd.to_datetime(date_value, errors="coerce")

# =========================
# REPORTING WINDOW HELPERS
# =========================

def add_month(month_start: date) -> date:
    """Return the first day of the next month for a given date."""
    # Always move to day 28 to avoid month length issues, then advance to the first of the next month.
    tentative = month_start.replace(day=28) + timedelta(days=4)
    return tentative.replace(day=1)

def compute_reporting_window(today: date | None = None):
    """
    Determine the rolling 13-month window ending with the last completed month.
    Returns (window_start, prev_month_start, prev_month_end, window_months, window_labels)
    """
    if today is None:
        today = TODAY_OVERRIDE or date.today()

    current_month_start = today.replace(day=1)
    prev_month_end = current_month_start - timedelta(days=1)
    prev_month_start = prev_month_end.replace(day=1)

    # Go back 12 additional months to get a 13-month window
    window_start = prev_month_start
    for _ in range(12):
        window_start = (window_start.replace(day=1) - timedelta(days=1)).replace(day=1)

    window_months: list[date] = []
    cursor = window_start
    while cursor <= prev_month_end:
        window_months.append(cursor)
        cursor = add_month(cursor)

    window_labels = {dt.strftime("%m-%y") for dt in window_months}
    return window_start, prev_month_start, prev_month_end, window_months, window_labels

def find_backfill_file() -> Path | None:
    """Locate the latest backfill CSV."""
    if BACKFILL_FILE_OVERRIDE.exists():
        return BACKFILL_FILE_OVERRIDE

    if not BACKFILL_BASE_DIR.exists():
        log.error(f"Backfill base directory not found: {BACKFILL_BASE_DIR}")
        return None

    candidates = list(BACKFILL_BASE_DIR.glob(f"*/summons/{BACKFILL_FILENAME}"))
    if not candidates:
        log.error("No backfill files found in the expected directory structure.")
        return None

    latest = max(candidates, key=lambda p: p.stat().st_mtime)
    log.info(f"Using backfill file: {latest}")
    return latest

# =========================
# LOAD HISTORICAL BACKFILL
# =========================

def load_backfill_data(window_start: date, prev_month_start: date, window_labels: set[str]):
    """
    Load historical summary data (Sep 2024 - Aug 2025).
    Stores as aggregate records, not individual tickets.
    """
    log.info("=" * 70)
    log.info("LOADING HISTORICAL BACKFILL DATA (Sep 2024 - Aug 2025)")
    log.info("=" * 70)

    backfill_path = find_backfill_file()
    if backfill_path is None or not backfill_path.exists():
        log.error("Backfill file could not be located.")
        return pd.DataFrame()

    df = pd.read_csv(backfill_path, encoding="utf-8")
    log.info(f"Loaded {len(df)} summary rows from backfill file")

    records = []
    for idx, row in df.iterrows():
        try:
            # Handle both column name formats
            violation_type = str(row.get("TYPE", row.get("Time Category", "P"))).strip()
            count = int(float(row.get("Count of TICKET_NUMBER", row.get("Sum of Value", 0))))
            month_year = str(row.get("Month_Year", row.get("PeriodLabel", ""))).strip()

            if count <= 0 or not month_year or "-" not in month_year:
                continue

            # Parse Month_Year (format: MM-YY)
            parts = month_year.split("-")
            month = int(parts[0])
            year = 2000 + int(parts[1])
            issue_date = date(year, month, 1)

            if issue_date < window_start or issue_date >= prev_month_start:
                continue

            month_year_label = issue_date.strftime("%m-%y")
            if month_year_label not in window_labels:
                continue

            # Create aggregate record
            record = {
                "TICKET_NUMBER": f"HIST_AGG_{year}{month:02d}_{violation_type}_{idx}",
                "TICKET_COUNT": count,
                "IS_AGGREGATE": True,
                "PADDED_BADGE_NUMBER": "",
                "OFFICER_DISPLAY_NAME": "MULTIPLE OFFICERS (Historical)",
                "OFFICER_NAME_RAW": "MULTIPLE OFFICERS",
                "ISSUE_DATE": pd.Timestamp(issue_date),
                "VIOLATION_NUMBER": "39:4-85" if violation_type == "M" else "175-10",
                "VIOLATION_DESCRIPTION": f"{violation_type} Violations - Historical ({count} tickets)",
                "VIOLATION_TYPE": f"{violation_type} Violations - Historical",
                "TYPE": violation_type,
                "STATUS": "Historical Summary",
                "LOCATION": "",
                "WARNING_FLAG": "",
                "SOURCE_FILE": backfill_path.name,
                "ETL_VERSION": "HISTORICAL_SUMMARY",
                "Year": year,
                "Month": month,
                "YearMonthKey": year * 100 + month,
                "Month_Year": month_year_label,
                "TEAM": "AGGREGATE",
                "WG1": "",
                "WG2": "AGGREGATE",
                "WG3": "",
                "WG4": "",
                "WG5": "",
                "POSS_CONTRACT_TYPE": "",
                "ASSIGNMENT_FOUND": False,
                "DATA_QUALITY_SCORE": 100,
                "DATA_QUALITY_TIER": "High",
                "TOTAL_PAID_AMOUNT": np.nan,
                "FINE_AMOUNT": np.nan,
                "COST_AMOUNT": np.nan,
                "MISC_AMOUNT": np.nan,
                "PROCESSING_TIMESTAMP": datetime.now(),
            }
            records.append(record)

        except Exception as e:
            log.warning(f"Error processing backfill row {idx}: {e}")
            continue

    result = pd.DataFrame(records)
    total_tickets = result["TICKET_COUNT"].sum() if not result.empty else 0
    log.info(f"Created {len(result)} aggregate records (representing {total_tickets:,} historical tickets)")

    expected_labels = window_labels.copy()
    prev_label = prev_month_start.strftime("%m-%y")
    if prev_label in expected_labels:
        expected_labels.remove(prev_label)
    present_labels = set(result["Month_Year"].unique())
    missing_labels = sorted(expected_labels - present_labels)
    if missing_labels:
        log.warning(f"Missing backfill months (no aggregate data): {', '.join(missing_labels)}")

    return result

# =========================
# LOAD SEPTEMBER 2025 E-TICKET
# =========================

def load_eticket_data(prev_month_start: date, prev_month_end: date):
    """
    Load the e-ticket data for the previous month.
    New directory structure: YYYY/raw/month/YYYY_MM_eticket_export.csv
    """
    log.info("=" * 70)
    log.info(f"LOADING E-TICKET DATA FOR {prev_month_start.strftime('%B %Y')}")
    log.info("=" * 70)

    # Build path: YYYY\month\YYYY_MM_eticket_export.csv
    year = prev_month_start.year
    month = prev_month_start.month
    eticket_filename = f"{year}_{month:02d}_eticket_export.csv"
    eticket_path = ETICKET_FOLDER / str(year) / "month" / eticket_filename

    if not eticket_path.exists():
        log.error(f"E-ticket file not found: {eticket_path}")
        return pd.DataFrame()

    # Detect delimiter (could be comma or semicolon)
    with open(eticket_path, "r", encoding="utf-8") as f:
        first_line = f.readline()
        delimiter = ";" if first_line.count(";") > first_line.count(",") else ","

    log.info(f"Detected delimiter: '{delimiter}'")

    # Load CSV
    try:
        df = pd.read_csv(eticket_path, dtype=str, encoding="utf-8", na_filter=False,
                        delimiter=delimiter, on_bad_lines='skip')
    except Exception as e:
        log.error(f"Error loading e-ticket file: {e}")
        return pd.DataFrame()

    log.info(f"Loaded {len(df)} records from e-ticket file")

    # Parse dates
    issue_dates = df.get("Issue Date", pd.Series([""] * len(df))).apply(parse_date)

    # Build standardized dataframe
    # Use Case Type Code directly from export (do not reclassify)
    case_type_raw = df.get("Case Type Code", pd.Series([""] * len(df))).astype(str).str.strip().str.upper()
    
    result = pd.DataFrame({
        "TICKET_NUMBER": df.get("Ticket Number", ""),
        "TICKET_COUNT": 1,  # Each e-ticket is individual
        "IS_AGGREGATE": False,
        "PADDED_BADGE_NUMBER": df.get("Officer Id", "").apply(pad_badge_number),
        "OFFICER_DISPLAY_NAME": "",  # Will be filled from assignment
        "OFFICER_NAME_RAW": df.get("Officer Last Name", "") + ", " + df.get("Officer First Name", ""),
        "ISSUE_DATE": issue_dates,
        "VIOLATION_NUMBER": df.get("Statute", ""),
        "VIOLATION_DESCRIPTION": df.get("Violation Description", ""),
        "VIOLATION_TYPE": df.get("Violation Description", ""),
        "TYPE": case_type_raw,  # Use Case Type Code directly from export (M, P, C)
        "STATUS": df.get("Case Status Code", ""),
        "LOCATION": df.get("Offense Street Name", ""),
        "WARNING_FLAG": df.get("Written Warning", ""),
        "SOURCE_FILE": eticket_path.name,
        "ETL_VERSION": "ETICKET_CURRENT",
        "Year": issue_dates.dt.year,
        "Month": issue_dates.dt.month,
        "YearMonthKey": issue_dates.dt.year * 100 + issue_dates.dt.month,
        "Month_Year": issue_dates.dt.strftime("%m-%y"),
        "TEAM": "",
        "WG1": "",
        "WG2": "",
        "WG3": "",
        "WG4": "",
        "WG5": "",
        "POSS_CONTRACT_TYPE": "",
        "ASSIGNMENT_FOUND": False,
        "DATA_QUALITY_SCORE": 100,
        "DATA_QUALITY_TIER": "High",
        "TOTAL_PAID_AMOUNT": np.nan,
        "FINE_AMOUNT": np.nan,
        "COST_AMOUNT": np.nan,
        "MISC_AMOUNT": np.nan,
        "PROCESSING_TIMESTAMP": datetime.now(),
    })

    # Restrict to the prior month window
    result = result[result["ISSUE_DATE"].notna()]
    if result.empty:
        log.warning("No valid issue dates found in e-ticket data.")
        return result

    mask = (result["ISSUE_DATE"] >= pd.Timestamp(prev_month_start)) & (
        result["ISSUE_DATE"] <= pd.Timestamp(prev_month_end)
    )
    before_count = len(result)
    result = result.loc[mask].copy()
    if before_count != len(result):
        log.info(f"Filtered e-ticket records to previous month: {before_count} -> {len(result)}")

    # NOTE: Removed classify_violations() call - now using Case Type Code directly from export
    # This ensures visual counts match raw export counts exactly

    log.info(f"Processed {len(result)} e-ticket records")
    type_counts = result["TYPE"].value_counts().to_dict()
    log.info(f"  TYPE breakdown: {type_counts}")

    return result

# =========================
# CLASSIFICATION
# =========================

def classify_violations(df):
    """Classify violations as Moving (M), Parking (P), or Special Complaint (C)"""

    def classify_row(row):
        statute = str(row.get("VIOLATION_NUMBER", "")).upper().strip()
        desc = str(row.get("VIOLATION_DESCRIPTION", "")).upper().strip()

        # Title 39 = Moving
        if statute.startswith("39:"):
            return "M"

        # Parking keywords
        parking_keywords = ["PARK", "METER", "HANDICAP", "LOADING", "FIRE LANE",
                           "STOPPING", "STANDING", "RESIDENT PERMIT"]
        if any(kw in desc for kw in parking_keywords):
            return "P"

        # Special complaint keywords
        special_keywords = ["GARBAGE", "DOG", "HOUSING", "NOISE", "LITTER"]
        if any(kw in desc for kw in special_keywords):
            return "C"

        # Default to parking for municipal ordinances
        return "P"

    df["TYPE"] = df.apply(classify_row, axis=1)
    return df

# =========================
# ASSIGNMENT ENRICHMENT
# =========================

def load_assignment_data():
    """Load assignment master data"""
    if not ASSIGNMENT_FILE.exists():
        log.warning(f"Assignment file not found: {ASSIGNMENT_FILE}")
        return pd.DataFrame()

    usecols = ["PADDED_BADGE_NUMBER", "Proposed 4-Digit Format",
               "TEAM", "WG1", "WG2", "WG3", "WG4", "WG5", "POSS_CONTRACT_TYPE"]

    df = pd.read_csv(ASSIGNMENT_FILE, dtype=str, usecols=usecols, encoding="utf-8", na_filter=False)

    # Ensure badge numbers are properly padded
    df["PADDED_BADGE_NUMBER"] = df["PADDED_BADGE_NUMBER"].apply(pad_badge_number)

    # Standardize officer display name casing
    df["Proposed 4-Digit Format"] = df["Proposed 4-Digit Format"].astype(str).str.strip()
    for col in ["TEAM", "WG1", "WG2", "WG3", "WG4", "WG5", "POSS_CONTRACT_TYPE"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # Remove duplicates
    df = df.drop_duplicates(subset=["PADDED_BADGE_NUMBER"], keep="first")

    log.info(f"Loaded {len(df)} assignment records")

    return df

def enrich_with_assignments(df, assign_df):
    """Merge assignment data with summons data"""
    if assign_df.empty:
        log.warning("No assignment data available - skipping enrichment")
        return df

    # Ensure PADDED_BADGE_NUMBER column exists and is padded
    df["PADDED_BADGE_NUMBER"] = df["PADDED_BADGE_NUMBER"].apply(pad_badge_number)

    # Diagnostics before merge
    total_badges = df["PADDED_BADGE_NUMBER"].notna().sum()
    unique_badges = df.loc[df["PADDED_BADGE_NUMBER"] != "", "PADDED_BADGE_NUMBER"].nunique()
    log.info(f"Assignment merge prep: {total_badges} records contain badge numbers ({unique_badges} unique)")

    starting_rows = len(df)

    # Rename assignment columns for merge
    assign_df = assign_df.rename(columns={"Proposed 4-Digit Format": "OFFICER_DISPLAY_NAME_ASSIGN"})

    # Merge
    merged = df.merge(
        assign_df,
        on="PADDED_BADGE_NUMBER",
        how="left",
        suffixes=("", "_ASSIGN"),
        validate="m:1"
    )

    # Validate no row explosion
    if len(merged) != starting_rows:
        raise ValueError(f"Assignment merge created extra rows: {starting_rows} -> {len(merged)}")

    # Update columns from assignment data
    for col in ["OFFICER_DISPLAY_NAME", "TEAM", "WG1", "WG2", "WG3", "WG4", "WG5", "POSS_CONTRACT_TYPE"]:
        assign_col = col + "_ASSIGN" if col != "OFFICER_DISPLAY_NAME" else "OFFICER_DISPLAY_NAME_ASSIGN"
        if assign_col in merged.columns:
            # Fill blanks with assignment data
            mask = (merged[col].astype(str).str.strip() == "")
            merged.loc[mask, col] = merged.loc[mask, assign_col]
            merged.drop(columns=[assign_col], inplace=True)

    # Apply manual overrides for known badges (optionally conditional on violation type)
    for badge, overrides in ASSIGNMENT_OVERRIDES.items():
        mask = merged["PADDED_BADGE_NUMBER"] == badge
        # Support conditional override: only apply when column contains substring (e.g. FIRE LANES)
        cond = overrides.get("_condition")
        if cond and isinstance(cond, dict):
            col_name = cond.get("column")
            substring = cond.get("contains", "")
            if col_name and substring:
                # Try common column names for violation description
                for c in [col_name, "VIOLATION_DESCRIPTION", "Violation Description"]:
                    if c in merged.columns:
                        mask = mask & merged[c].astype(str).str.upper().str.contains(substring.upper(), na=False)
                        break
            overrides = {k: v for k, v in overrides.items() if k != "_condition"}
        if mask.any():
            log.info(f"Applying assignment override for badge {badge} ({mask.sum()} records)")
            for col, value in overrides.items():
                if col in merged.columns:
                    merged.loc[mask, col] = value

    # Mark records with assignments found
    merged["ASSIGNMENT_FOUND"] = (merged["OFFICER_DISPLAY_NAME"].astype(str).str.strip() != "")

    # Default unknown bureau/team for unmatched badge records (excluding historical aggregates)
    unmatched_mask = (~merged["ASSIGNMENT_FOUND"]) & (merged["PADDED_BADGE_NUMBER"].astype(str).str.strip() != "")
    if unmatched_mask.any():
        merged.loc[unmatched_mask, ["TEAM", "WG1", "WG2", "WG3", "WG4", "WG5"]] = merged.loc[
            unmatched_mask, ["TEAM", "WG1", "WG2", "WG3", "WG4", "WG5"]
        ].fillna("").replace("", "UNKNOWN")
        merged.loc[unmatched_mask, "WG2"] = "UNKNOWN"

    # Add badge to display name if not already there
    def add_badge_to_name(row):
        name = str(row.get("OFFICER_DISPLAY_NAME", "")).strip()
        badge = str(row.get("PADDED_BADGE_NUMBER", "")).strip()

        if not name or name == "MULTIPLE OFFICERS (Historical)":
            return name

        if not badge:
            return name

        # Check if badge already in name
        if f"({badge})" in name or f"#{badge}" in name:
            return name

        return f"{name} ({badge})"

    merged["OFFICER_DISPLAY_NAME"] = merged.apply(add_badge_to_name, axis=1)

    matched = merged["ASSIGNMENT_FOUND"].sum()
    log.info(f"Assignment enrichment: {matched}/{len(merged)} records matched ({matched/len(merged)*100:.1f}%)")

    # Diagnostics for unmatched badges
    unmatched_badges = merged.loc[~merged["ASSIGNMENT_FOUND"], "PADDED_BADGE_NUMBER"].astype(str).str.strip()
    unmatched_badges = [b for b in unmatched_badges if b]
    if unmatched_badges:
        sample_unmatched = unmatched_badges[:10]
        log.warning(f"Unmatched badge records: {len(unmatched_badges)} (showing up to 10): {sample_unmatched}")

        summons_badges = set(merged.loc[merged["PADDED_BADGE_NUMBER"] != "", "PADDED_BADGE_NUMBER"])
        assignment_badges = set(assign_df["PADDED_BADGE_NUMBER"])
        missing_in_assignment = sorted(list(summons_badges - assignment_badges))[:10]
        if missing_in_assignment:
            log.warning(f"Badges present in summons but not in Assignment_Master (sample): {missing_in_assignment}")

    else:
        log.info("All badge-bearing records matched an assignment entry.")

    return merged

# =========================
# PEO RULE
# =========================

def apply_peo_rule(df):
    """
    PEO and Class I officers cannot issue moving violations.
    Convert any M to P for these officers.
    """
    wg3_values = df["WG3"].astype(str).str.strip().str.upper()
    mask = wg3_values.isin(["PEO", "CLASS I"]) & (df["TYPE"] == "M")

    flipped = mask.sum()
    df.loc[mask, "TYPE"] = "P"

    if flipped > 0:
        log.info(f"PEO/Class I rule: Converted {flipped} moving violations to parking")

    return df

# =========================
# MAIN PROCESSING
# =========================

def main():
    log.info("")
    log.info("=" * 70)
    log.info("SUMMONS MASTER - SIMPLIFIED PROCESSING")
    log.info("=" * 70)
    log.info("")

    window_start, prev_month_start, prev_month_end, window_months, window_labels = compute_reporting_window()
    log.info(f"Reporting window start: {window_start.strftime('%Y-%m-%d')}")
    log.info(f"Previous month start:  {prev_month_start.strftime('%Y-%m-%d')}")
    log.info(f"Previous month end:    {prev_month_end.strftime('%Y-%m-%d')}")

    # Load data
    backfill_df = load_backfill_data(window_start, prev_month_start, window_labels)
    # REMOVED: court_current_df = load_court_current_data()  
    # ❌ This was causing duplication by loading Sept 2024 - Aug 2025 twice!
    # ✅ We only need backfill_df for historical data (Sept 2024 - Aug 2025)
    eticket_df = load_eticket_data(prev_month_start, prev_month_end)

    if backfill_df.empty and eticket_df.empty:
        log.error("No data loaded - exiting")
        return False

    # Combine
    log.info("")
    log.info("=" * 70)
    log.info("COMBINING DATA")
    log.info("=" * 70)

    frames = [f for f in [backfill_df, eticket_df] if not f.empty]
    all_df = pd.concat(frames, ignore_index=True, sort=False)
    log.info(f"Combined total: {len(all_df)} records")

    if all_df.empty:
        log.error("Combined dataset is empty after loading sources.")
        return False

    # Ensure we only retain the 13-month reporting window
    all_df = all_df[all_df["ISSUE_DATE"].notna()].copy()
    mask = (all_df["ISSUE_DATE"] >= pd.Timestamp(window_start)) & (
        all_df["ISSUE_DATE"] <= pd.Timestamp(prev_month_end)
    )
    before_window_count = len(all_df)
    all_df = all_df.loc[mask].copy()
    if before_window_count != len(all_df):
        log.info(f"Filtered records to reporting window: {before_window_count} -> {len(all_df)}")

    # Recompute date intelligence columns for consistency
    all_df["Year"] = all_df["ISSUE_DATE"].dt.year.astype(int)
    all_df["Month"] = all_df["ISSUE_DATE"].dt.month.astype(int)
    all_df["YearMonthKey"] = all_df["Year"] * 100 + all_df["Month"]
    all_df["Month_Year"] = all_df["ISSUE_DATE"].dt.strftime("%m-%y")

    # Log breakdown by source
    if not all_df.empty:
        log.info("")
        log.info("Data source breakdown:")
        etl_counts = all_df["ETL_VERSION"].value_counts().to_dict()
        for source, count in etl_counts.items():
            ticket_count = all_df[all_df["ETL_VERSION"] == source]["TICKET_COUNT"].sum()
            log.info(f"  {source}: {count:,} records (representing {ticket_count:,} tickets)")
        log.info("")

    # Load and apply assignments
    log.info("")
    log.info("=" * 70)
    log.info("ENRICHING WITH ASSIGNMENT DATA")
    log.info("=" * 70)

    assign_df = load_assignment_data()
    all_df = enrich_with_assignments(all_df, assign_df)

    # Apply PEO rule
    all_df = apply_peo_rule(all_df)

    # Final type breakdown
    log.info("")
    log.info("FINAL TYPE BREAKDOWN:")
    type_counts = all_df["TYPE"].value_counts().to_dict()
    for typ, count in type_counts.items():
        log.info(f"  {typ}: {count:,}")

    # Write output
    log.info("")
    log.info("=" * 70)
    log.info("WRITING OUTPUT")
    log.info("=" * 70)

    output_file = OUTPUT_FILE  # Use local variable
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Remove existing file if it exists
    if output_file.exists():
        try:
            output_file.unlink()
            log.info(f"Removed existing file: {output_file}")
        except PermissionError:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = output_file.parent / f"summons_powerbi_latest_{timestamp}.xlsx"
            log.warning(f"File is open - writing to: {output_file}")

    # Write Excel
    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        all_df.to_excel(writer, sheet_name="Summons_Data", index=False)

    log.info(f"Output written: {output_file}")
    log.info(f"Total records: {len(all_df):,}")
    log.info("")
    log.info("=" * 70)
    log.info("PROCESSING COMPLETE")
    log.info("=" * 70)

    return True

if __name__ == "__main__":
    sys.exit(0 if main() else 1)
