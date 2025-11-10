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
from datetime import datetime
from pathlib import Path
import logging
import sys

# =========================
# CONFIGURATION
# =========================

# Historical backfill data (Sep 2024 - Aug 2025)
BACKFILL_FILE = Path(
    r"C:\Users\carucci_r\OneDrive - City of Hackensack\02_ETL_Scripts\Summons\backfill_data\25_08_Hackensack Police Department - Summons Dashboard.csv"
)

# Current court data (Sep 2024 - Sep 2025)
COURT_CURRENT_FILE = Path(
    r"C:\Users\carucci_r\OneDrive - City of Hackensack\02_ETL_Scripts\Summons\backfill_data\25_09_Hackensack Police Department - Summons Dashboard.csv"
)

# Current month e-ticket data (Sep 2025)
ETICKET_FILE = Path(
    r"C:\Users\carucci_r\OneDrive - City of Hackensack\05_EXPORTS\_Summons\E_Ticket\25_09_e_ticketexport.csv"
)

# Assignment Master
ASSIGNMENT_FILE = Path(
    r"C:\Users\carucci_r\OneDrive - City of Hackensack\09_Reference\Personnel\Assignment_Master_V2.csv"
)

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
# LOAD HISTORICAL BACKFILL
# =========================

def load_backfill_data():
    """
    Load historical summary data (Sep 2024 - Aug 2025).
    Stores as aggregate records, not individual tickets.
    """
    log.info("=" * 70)
    log.info("LOADING HISTORICAL BACKFILL DATA (Sep 2024 - Aug 2025)")
    log.info("=" * 70)

    if not BACKFILL_FILE.exists():
        log.error(f"Backfill file not found: {BACKFILL_FILE}")
        return pd.DataFrame()

    df = pd.read_csv(BACKFILL_FILE, encoding="utf-8")
    log.info(f"Loaded {len(df)} summary rows from backfill file")

    records = []
    for idx, row in df.iterrows():
        try:
            violation_type = str(row.get("TYPE", "P")).strip()
            count = int(float(row.get("Count of TICKET_NUMBER", 0)))
            month_year = str(row.get("Month_Year", "")).strip()

            if count <= 0 or not month_year or "-" not in month_year:
                continue

            # Parse Month_Year (format: MM-YY)
            parts = month_year.split("-")
            month = int(parts[0])
            year = 2000 + int(parts[1])

            # Create aggregate record
            record = {
                "TICKET_NUMBER": f"HIST_AGG_{year}{month:02d}_{violation_type}_{idx}",
                "TICKET_COUNT": count,
                "IS_AGGREGATE": True,
                "PADDED_BADGE_NUMBER": "",
                "OFFICER_DISPLAY_NAME": "MULTIPLE OFFICERS (Historical)",
                "OFFICER_NAME_RAW": "MULTIPLE OFFICERS",
                "ISSUE_DATE": pd.Timestamp(year, month, 1),
                "VIOLATION_NUMBER": "39:4-85" if violation_type == "M" else "175-10",
                "VIOLATION_DESCRIPTION": f"{violation_type} Violations - Historical ({count} tickets)",
                "VIOLATION_TYPE": f"{violation_type} Violations - Historical",
                "TYPE": violation_type,
                "STATUS": "Historical Summary",
                "LOCATION": "",
                "WARNING_FLAG": "",
                "SOURCE_FILE": BACKFILL_FILE.name,
                "ETL_VERSION": "HISTORICAL_SUMMARY",
                "Year": year,
                "Month": month,
                "YearMonthKey": year * 100 + month,
                "Month_Year": f"{month:02d}-{year-2000:02d}",
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

    return result

# =========================
# LOAD COURT CURRENT DATA
# =========================

def load_court_current_data():
    """
    Load current court data (Sep 2024 - Aug 2025).
    EXCLUDES Sep 2025 to avoid duplication with e-ticket data.
    Stores as aggregate records, not individual tickets.
    """
    log.info("=" * 70)
    log.info("LOADING COURT CURRENT DATA (Sep 2024 - Aug 2025, excluding Sep 2025)")
    log.info("=" * 70)

    if not COURT_CURRENT_FILE.exists():
        log.error(f"Court current file not found: {COURT_CURRENT_FILE}")
        return pd.DataFrame()

    df = pd.read_csv(COURT_CURRENT_FILE, encoding="utf-8")
    log.info(f"Loaded {len(df)} summary rows from court current file")

    records = []
    for idx, row in df.iterrows():
        try:
            violation_type = str(row.get("TYPE", "P")).strip()
            count = int(float(row.get("Count of TICKET_NUMBER", 0)))
            month_year = str(row.get("Month_Year", "")).strip()

            if count <= 0 or not month_year or "-" not in month_year:
                continue

            # Parse Month_Year (format: MM-YY)
            parts = month_year.split("-")
            month = int(parts[0])
            year = 2000 + int(parts[1])

            # CRITICAL: Skip September 2025 to avoid duplication with e-ticket data
            if year == 2025 and month == 9:
                log.info(f"Skipping Sep 2025 {violation_type} ({count} tickets) - will use e-ticket data instead")
                continue

            # Create aggregate record
            record = {
                "TICKET_NUMBER": f"COURT_AGG_{year}{month:02d}_{violation_type}_{idx}",
                "TICKET_COUNT": count,
                "IS_AGGREGATE": True,
                "PADDED_BADGE_NUMBER": "",
                "OFFICER_DISPLAY_NAME": "MULTIPLE OFFICERS (Court Current)",
                "OFFICER_NAME_RAW": "MULTIPLE OFFICERS",
                "ISSUE_DATE": pd.Timestamp(year, month, 1),
                "VIOLATION_NUMBER": "39:4-85" if violation_type == "M" else "175-10",
                "VIOLATION_DESCRIPTION": f"{violation_type} Violations - Court Current ({count} tickets)",
                "VIOLATION_TYPE": f"{violation_type} Violations - Court Current",
                "TYPE": violation_type,
                "STATUS": "Court Current",
                "LOCATION": "",
                "WARNING_FLAG": "",
                "SOURCE_FILE": COURT_CURRENT_FILE.name,
                "ETL_VERSION": "COURT_CURRENT",
                "Year": year,
                "Month": month,
                "YearMonthKey": year * 100 + month,
                "Month_Year": f"{month:02d}-{year-2000:02d}",
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
            log.warning(f"Error processing court current row {idx}: {e}")
            continue

    result = pd.DataFrame(records)
    total_tickets = result["TICKET_COUNT"].sum() if not result.empty else 0
    log.info(f"Created {len(result)} aggregate records (representing {total_tickets:,} court current tickets)")

    return result

# =========================
# LOAD SEPTEMBER 2025 E-TICKET
# =========================

def load_eticket_data():
    """
    Load September 2025 e-ticket data.
    """
    log.info("=" * 70)
    log.info("LOADING SEPTEMBER 2025 E-TICKET DATA")
    log.info("=" * 70)

    if not ETICKET_FILE.exists():
        log.error(f"E-ticket file not found: {ETICKET_FILE}")
        return pd.DataFrame()

    # Detect delimiter (could be comma or semicolon)
    with open(ETICKET_FILE, "r", encoding="utf-8") as f:
        first_line = f.readline()
        delimiter = ";" if first_line.count(";") > first_line.count(",") else ","

    log.info(f"Detected delimiter: '{delimiter}'")

    # Load CSV
    try:
        df = pd.read_csv(ETICKET_FILE, dtype=str, encoding="utf-8", na_filter=False,
                        delimiter=delimiter, on_bad_lines='skip')
    except Exception as e:
        log.error(f"Error loading e-ticket file: {e}")
        return pd.DataFrame()

    log.info(f"Loaded {len(df)} records from e-ticket file")

    # Parse dates
    issue_dates = df.get("Issue Date", pd.Series([""] * len(df))).apply(parse_date)

    # Build standardized dataframe
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
        "TYPE": "",  # Will be classified
        "STATUS": df.get("Case Status Code", ""),
        "LOCATION": df.get("Offense Street Name", ""),
        "WARNING_FLAG": df.get("Written Warning", ""),
        "SOURCE_FILE": ETICKET_FILE.name,
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

    # Classify violation types
    result = classify_violations(result)

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

    # Remove duplicates
    df = df.drop_duplicates(subset=["PADDED_BADGE_NUMBER"], keep="first")

    log.info(f"Loaded {len(df)} assignment records")

    return df

def enrich_with_assignments(df, assign_df):
    """Merge assignment data with summons data"""
    if assign_df.empty:
        log.warning("No assignment data available - skipping enrichment")
        return df

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

    # Mark records with assignments found
    merged["ASSIGNMENT_FOUND"] = (merged["OFFICER_DISPLAY_NAME"].astype(str).str.strip() != "")

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

    # Load data
    backfill_df = load_backfill_data()
    # REMOVED: court_current_df = load_court_current_data()  
    # ❌ This was causing duplication by loading Sept 2024 - Aug 2025 twice!
    # ✅ We only need backfill_df for historical data (Sept 2024 - Aug 2025)
    eticket_df = load_eticket_data()

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
