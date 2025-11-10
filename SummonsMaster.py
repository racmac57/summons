#!/usr/bin/env python3
# SummonsMaster — Final Drop-In Version (Laptop Ready)
# Generates ATS-compatible monthly output for Power BI

import pandas as pd, numpy as np, re, calendar, sys, warnings, logging
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from pathlib import Path
from textwrap import dedent

# Regex patterns for badge detection
BADGE_PAREN_RE = re.compile(r"\(\s*\d{4}\s*\)")
BADGE_HASH_RE  = re.compile(r"#\s*\d{4}\b")

# --- Vectorized classification lookups/regex ---

# Explicit city ordinances that must be PARKING (normalize by removing spaces)
PARKING_ORDS = {"175-10G", "175-10.2(G)", "175-13.1(C)(1)"}
PARKING_ORDS_NORM = {re.sub(r"\s+", "", s.upper()) for s in PARKING_ORDS}

# Regex for statute families
RE_TITLE39 = re.compile(r"^\s*39:\s*\d", re.IGNORECASE)

# Parking-ish description keywords (keep broad; we resolve ties by priority)
RE_PARKING_DESC = re.compile(
    r"\b(PARK|METER|HANDICAP|LOADING|FIRE\s*LANE|NO\s*PARKING|STOPPING|STANDING|RESIDENT(?:IAL)?\s*PERMIT|COMMERCIAL\s*VEHICLE)\b",
    re.IGNORECASE,
)

# Special complaint keywords (non-traffic municipal issues)
RE_SPECIAL_DESC = re.compile(
    r"\b(GARBAGE|TRASH|DOG|ANIMAL|HOUSING|URINAT|NOISE|PROPERTY\s*MAINT|LITTER|CODE\s*ENFORCEMENT|ZONING)\b",
    re.IGNORECASE,
)

# Moving-ish description cues (used only if not Title 39; gives M)
RE_MOVING_DESC = re.compile(
    r"\b(SPEED|SIGNAL|STOP\s*SIGN|LANE\s*USE|CELL\s*PHONE|SEAT\s*BELT|FAIL(?:URE)?\s*TO\s*YIELD|RED\s*LIGHT)\b",
    re.IGNORECASE,
)

warnings.filterwarnings("ignore", category=UserWarning)

# =========================
# CONFIG (verified paths)
# =========================
SOURCE_FILES = [
    # Current month e-ticket data
    r"C:\Users\carucci_r\OneDrive - City of Hackensack\05_EXPORTS\_Summons\E_Ticket\25_09_e_ticketexport.csv",
    # Historical data backfill
    r"C:\Users\carucci_r\OneDrive - City of Hackensack\02_ETL_Scripts\Summons\data_samples\2025_10_09_21_01_02_Hackensack Police Department - Summons Dashboard.csv",
    r"C:\Users\carucci_r\OneDrive - City of Hackensack\05_EXPORTS\_Summons\Court\25_08_ATS.csv",
]

ASSIGNMENT_FILE = Path(
    r"C:\Users\carucci_r\OneDrive - City of Hackensack\09_Reference\Personnel\Assignment_Master_V2.csv"
)

OUTPUT_FILE = Path(
    r"C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons\summons_powerbi_latest.xlsx"
)
ARCHIVE_MONTHLY = True

LOG_FILE = "summons_processing.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("summons_etl")

# =========================
# 13-Month Window
# =========================
def rolling_window():
    today = date.today()
    prev_y = today.year if today.month > 1 else today.year - 1
    prev_m = today.month - 1 if today.month > 1 else 12
    end_day = calendar.monthrange(prev_y, prev_m)[1]
    end_d = date(prev_y, prev_m, end_day)
    start_d = (end_d - relativedelta(months=12)).replace(day=1)  # 12 months back from end = 13 total months
    log.info(f"Rolling 13-Month Window: {start_d} to {end_d}")
    return pd.Timestamp(start_d), pd.Timestamp(end_d)

# =========================
# Helpers
# =========================
def detect_encoding(p: Path):
    try:
        with open(p, "r", encoding="utf-8") as f:
            f.read(1024)
        return "utf-8"
    except UnicodeDecodeError:
        return "latin-1"

def detect_delimiter(p: Path, enc):
    with open(p, "r", encoding=enc, errors="ignore") as f:
        s = f.read(4096)
    return ";" if s.count(";") > s.count(",") else ","

def parse_date(s):
    if not s or str(s).strip() == "":
        return pd.NaT
    for f in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%m/%d/%y"):
        try:
            return pd.to_datetime(s, format=f, errors="raise")
        except Exception:
            pass
    return pd.to_datetime(s, errors="coerce")

def classify_type(code: str, desc: str) -> str:
    """
    Return 'P' (Parking), 'M' (Moving), or 'C' (Special Complaint)
    using current rules, with robust matching for known parking ordinances.
    """
    c_raw = (code or "")
    d_raw = (desc or "")

    # Normalize statute code: uppercase and remove all whitespace
    c_norm = re.sub(r"\s+", "", c_raw.upper())

    # Explicit parking ordinances (normalize comparison by stripping whitespace)
    PARKING_ORDS = {"175-10G", "175-10.2(G)", "175-13.1(C)(1)"}
    PARKING_ORDS_NORM = {re.sub(r"\s+", "", s.upper()) for s in PARKING_ORDS}

    if c_norm in PARKING_ORDS_NORM:
        return "P"

    # Existing logic — keep as-is conceptually, but normalize description comparisons
    d_norm = d_raw.upper()
    # Examples of parking-ish keywords (keep whatever you currently check, add here if needed)
    parking_keywords = [
        "PARKING", "METER", "HANDICAP", "LOADING", "FIRE LANE", "NO PARKING",
        "STOPPING", "STANDING", "RESIDENT PERMIT", "COMMERCIAL VEHICLE"
    ]
    if any(k in d_norm for k in parking_keywords):
        return "P"

    # Special complaint keywords (leave consistent with your current rules)
    special_keywords = [
        "GARBAGE", "DOG", "HOUSING", "URINAT", "NOISE", "PROPERTY MAINT",
        "LITTER", "CODE ENFORCEMENT", "ZONING"
    ]
    if any(k in d_norm for k in special_keywords):
        return "C"

    # Title 39 defaults to moving unless caught by a rule above
    if c_norm.startswith("39:") or c_raw.strip().startswith("39:"):
        return "M"

    # Fallbacks: if description strongly suggests moving (speeding, signal, stop, etc.)
    moving_keywords = ["SPEED", "SIGNAL", "STOP SIGN", "LANE USE", "CELL PHONE", "SEAT BELT"]
    if any(k in d_norm for k in moving_keywords):
        return "M"

    # Final fallback: prefer Parking for municipal/unknown unless evidence of moving
    return "P"

def classify_types_vectorized(df: pd.DataFrame) -> pd.DataFrame:
    """
    Vectorized classifier. Produces:
      - TYPE: 'P' (Parking), 'M' (Moving), 'C' (Special Complaint)
      - CLASSIFY_REASON: short tag for audit
    Priority order:
      1) Explicit Parking ordinances list  -> P
      2) Parking description keywords      -> P
      3) Special complaint keywords        -> C
      4) Title 39 statutes                 -> M
      5) Moving description keywords       -> M
      6) Fallback                          -> P
    """
    # Safe views
    statute = df["VIOLATION_NUMBER_NORM"]
    statute_ns = df["VIOLATION_NUMBER_NOSPACE"]
    desc_up = df["VIOLATION_DESCRIPTION_UP"]

    n = len(df)
    out_type = np.full(n, "", dtype=object)
    out_reason = np.full(n, "", dtype=object)

    # 1) Explicit Parking ordinances
    mask_parking_code = statute_ns.isin(PARKING_ORDS_NORM)
    out_type[mask_parking_code.values] = "P"
    out_reason[mask_parking_code.values] = "ORD:PARKING_LIST"

    # 2) Parking by description
    mask_parking_desc = desc_up.str.contains(RE_PARKING_DESC, na=False)
    # Only set where not set yet
    m2 = ~mask_parking_code & mask_parking_desc
    out_type[m2.values] = "P"
    out_reason[m2.values] = "DESC:PARKING"

    # 3) Special complaints by description
    mask_special = desc_up.str.contains(RE_SPECIAL_DESC, na=False)
    m3 = (out_type == "") & mask_special.values
    out_type[m3] = "C"
    out_reason[m3] = "DESC:SPECIAL"

    # 4) Title 39 -> Moving
    mask_title39 = statute.str.match(RE_TITLE39, na=False)
    m4 = (out_type == "") & mask_title39.values
    out_type[m4] = "M"
    out_reason[m4] = "CODE:TITLE39"

    # 5) Moving by description (only if not set)
    mask_moving_desc = desc_up.str.contains(RE_MOVING_DESC, na=False)
    m5 = (out_type == "") & mask_moving_desc.values
    out_type[m5] = "M"
    out_reason[m5] = "DESC:MOVING"

    # 6) Fallback -> Parking
    m6 = (out_type == "")
    out_type[m6] = "P"
    out_reason[m6] = "FALLBACK:PARKING"

    df["TYPE"] = out_type
    df["CLASSIFY_REASON"] = out_reason
    return df

# =========================
# Assignment Lookup
# =========================
def load_assignment(path: Path = None, logger=None) -> pd.DataFrame:
    """
    Load Assignment_Master_V2 as a tidy DataFrame keyed on PADDED_BADGE_NUMBER,
    with only the columns we need for fast vectorized enrichment.
    """
    if path is None:
        path = ASSIGNMENT_FILE
    
    if not path.exists():
        if logger:
            logger.warning("Assignment Master missing – continuing without it")
        return pd.DataFrame()
    
    usecols = [
        "PADDED_BADGE_NUMBER", "Proposed 4-Digit Format",
        "TEAM","WG1","WG2","WG3","WG4","WG5","POSS_CONTRACT_TYPE"
    ]
    
    try:
        df = pd.read_csv(path, dtype=str, usecols=usecols, encoding="utf-8", na_filter=False)
    except Exception as e:
        if logger:
            logger.warning(f"Could not load assignment file: {e}")
        return pd.DataFrame()
    
    # Normalize key
    df["PADDED_BADGE_NUMBER"] = df["PADDED_BADGE_NUMBER"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(4)

    # Light trim
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()

    if logger:
        logger.info(f"Assignment entries loaded: {len(df):,}")

    return df

def enrich_assign_vectorized(df: pd.DataFrame, assign_df: pd.DataFrame, logger=None) -> pd.DataFrame:
    """
    Vectorized enrichment:
      - Normalizes PADDED_BADGE_NUMBER on the fact DF
      - Single left-merge to bring officer/team fields
      - Adds clean fallbacks; no row loops
    """
    if assign_df.empty:
        # No assignment data available, just ensure columns exist
        for c in ["PADDED_BADGE_NUMBER", "OFFICER_DISPLAY_NAME", "TEAM","WG1","WG2","WG3","WG4","WG5","POSS_CONTRACT_TYPE"]:
            if c not in df.columns:
                df[c] = ""
        if "PEO_RULE_APPLIED" not in df.columns:
            df["PEO_RULE_APPLIED"] = False
        return df
    
    # Ensure columns exist
    for c in ["PADDED_BADGE_NUMBER", "OFFICER_DISPLAY_NAME", "TEAM","WG1","WG2","WG3","WG4","WG5","POSS_CONTRACT_TYPE"]:
        if c not in df.columns:
            df[c] = ""

    # Normalize the key on fact DF
    df["PADDED_BADGE_NUMBER"] = (
        df["PADDED_BADGE_NUMBER"]
        .astype(str)
        .str.replace(r"\D", "", regex=True)
        .str.zfill(4)
    )

    # Do the merge
    right = assign_df.rename(columns={"Proposed 4-Digit Format": "OFFICER_DISPLAY_NAME_ASSIGN"})
    merged = df.merge(
        right,
        how="left",
        on="PADDED_BADGE_NUMBER",
        suffixes=("", "_ASSIGN"),
        copy=False
    )

    # Coalesce final columns from assign where fact missing
    def _coalesce(dst: str, src: str):
        dstv = merged[dst].astype(str)
        srcv = merged[src].astype(str)
        merged[dst] = np.where(dstv.str.strip().eq(""), srcv, dstv)

    _coalesce("OFFICER_DISPLAY_NAME", "OFFICER_DISPLAY_NAME_ASSIGN")
    for col in ["TEAM","WG1","WG2","WG3","WG4","WG5","POSS_CONTRACT_TYPE"]:
        _coalesce(col, f"{col}")

    # Drop temp assign-only column
    if "OFFICER_DISPLAY_NAME_ASSIGN" in merged.columns:
        merged.drop(columns=["OFFICER_DISPLAY_NAME_ASSIGN"], inplace=True)

    # Ensure audit flag exists
    if "PEO_RULE_APPLIED" not in merged.columns:
        merged["PEO_RULE_APPLIED"] = False

    # Minor de-fragmentation before returning
    merged = merged.copy()

    if logger:
        no_match = int(merged["OFFICER_DISPLAY_NAME"].astype(str).str.strip().eq("").sum())
        logger.info(f"Assignment merge completed. Rows without officer display after merge: {no_match:,}")

    return merged

# =========================
# Loader
# =========================
def load_csv(path: Path):
    enc = detect_encoding(path)
    delim = detect_delimiter(path, enc)
    df = pd.read_csv(path, dtype=str, keep_default_na=False, encoding=enc, delimiter=delim)
    df.columns = [c.strip() for c in df.columns]
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()
    return df

def create_historical_records(df_historical):
    """Create individual records from historical aggregated data"""
    records = []
    for _, row in df_historical.iterrows():
        count = int(row["Count of TICKET_NUMBER"])
        violation_type = row["TYPE"]
        month_year = row["Month_Year"]
        
        # Parse month-year to get year and month
        month, year = month_year.split("-")
        year = 2000 + int(year)  # Convert YY to YYYY
        month = int(month)
        
        # Create YearMonthKey
        year_month_key = year * 100 + month
        
        # Create individual records
        for i in range(count):
            records.append({
                "TICKET_NUMBER": f"HIST_{year}{month:02d}_{i+1:06d}",
                "OFFICER_NAME_RAW": "Historical Data",
                "BADGE_NUMBER_RAW": "",
                "PADDED_BADGE_NUMBER": "",
                "ISSUE_DATE": pd.Timestamp(year, month, 15),  # Mid-month date
                "VIOLATION_NUMBER": "39:4-1" if violation_type == "M" else "39:4-138" if violation_type == "P" else "OTHER",
                "VIOLATION_DESCRIPTION": "Moving Violation" if violation_type == "M" else "Parking Violation" if violation_type == "P" else "Other Violation",
                "VIOLATION_TYPE": "Moving Violation" if violation_type == "M" else "Parking Violation" if violation_type == "P" else "Other Violation",
                "TYPE": violation_type,
                "STATUS": "Historical",
                "LOCATION": "Historical Data",
                "WARNING_FLAG": "",
                "SOURCE_FILE": "Historical_Backfill.csv",
                "ETL_VERSION": "HISTORICAL_BACKFILL",
                "Year": year,
                "Month": month,
                "YearMonthKey": year_month_key,
                "Month_Year": month_year,
                "TOTAL_PAID_AMOUNT": np.nan,
                "FINE_AMOUNT": np.nan,
                "COST_AMOUNT": np.nan,
                "MISC_AMOUNT": np.nan,
                "OFFICER_DISPLAY_NAME": "",
                "WG1": "",
                "WG2": "",
                "WG3": "",
                "WG4": "",
                "WG5": "",
            })
    
    return pd.DataFrame(records)

def load_file(path: Path):
    if path.suffix.lower() == ".csv":
        df = load_csv(path)
        df["SOURCE_FILE"] = path.name
        
        # Check if this is the historical backfill file
        if "Count of TICKET_NUMBER" in df.columns and "Month_Year" in df.columns:
            log.info(f"Processing historical backfill data from {path.name}")
            return create_historical_records(df)
        
        if "Ticket Number" in df.columns:
            # e-ticket schema - build data first, then assign in bulk
            badge_raw = df.get("Officer Id", "").str.replace(r"\D","",regex=True).str.zfill(4)
            issue_date = df.get("Issue Date").apply(parse_date)
            
            # Build all columns at once
            out = pd.DataFrame({
                "TICKET_NUMBER": df.get("Ticket Number", ""),
                "OFFICER_NAME_RAW": (
                    df.get("Officer Last Name", "")
                    + ", "
                    + df.get("Officer First Name", "")
                ),
                "BADGE_NUMBER_RAW": df.get("Officer Id", ""),
                "PADDED_BADGE_NUMBER": badge_raw,
                "ISSUE_DATE": issue_date,
                "VIOLATION_NUMBER": df.get("Statute", ""),
                "VIOLATION_DESCRIPTION": df.get("Violation Description", ""),
                "VIOLATION_TYPE": df.get("Violation Description", ""),  # Alias for M code compatibility
                "STATUS": df.get("Case Status Code", ""),
                "LOCATION": df.get("Offense Street Name", ""),
                "WARNING_FLAG": df.get("Written Warning", ""),
                "SOURCE_FILE": path.name,
                "ETL_VERSION": "ETICKET_TO_ATS_FINAL",
                "Year": issue_date.dt.year,
                "Month": issue_date.dt.month,
                "YearMonthKey": issue_date.dt.year * 100 + issue_date.dt.month,
                "Month_Year": issue_date.dt.strftime("%m-%y"),
                # Financial data placeholders (not in e-ticket export)
                "TOTAL_PAID_AMOUNT": np.nan,
                "FINE_AMOUNT": np.nan,
                "COST_AMOUNT": np.nan,
                "MISC_AMOUNT": np.nan,
                # Assignment columns (will be filled later)
                "OFFICER_DISPLAY_NAME": "",
                "WG1": "",
                "WG2": "",
                "WG3": "",
                "WG4": "",
                "WG5": "",
            })
            
            # Classification will be done centrally after concatenation
            
            return out
        else:
            # ATS Court CSV schema - map to standard columns
            # Helper function to get column with fallback names
            def get_col(primary, fallback="", default_val=""):
                if primary in df.columns:
                    return df[primary].copy()
                elif fallback and fallback in df.columns:
                    return df[fallback].copy()
                else:
                    return pd.Series([default_val] * len(df), dtype=str)
            
            out = pd.DataFrame()
            out["TICKET_NUMBER"] = get_col("TICKET_NUMBER", "Ticket Number")
            out["OFFICER_NAME_RAW"] = get_col("OFFICER_NAME")
            out["BADGE_NUMBER_RAW"] = get_col("BADGE_NUMBER")
            out["PADDED_BADGE_NUMBER"] = out["BADGE_NUMBER_RAW"].astype(str).str.replace(r"\D","",regex=True).str.zfill(4)
            
            # Get issue date column and parse
            if "ISSUE_DATE" in df.columns:
                out["ISSUE_DATE"] = df["ISSUE_DATE"].apply(parse_date)
            elif "Issue Date" in df.columns:
                out["ISSUE_DATE"] = df["Issue Date"].apply(parse_date)
            else:
                out["ISSUE_DATE"] = pd.NaT
            
            out["VIOLATION_NUMBER"] = get_col("VIOLATION_NUMBER", "Statute")
            out["VIOLATION_DESCRIPTION"] = get_col("VIOLATION_DESCRIPTION", "Violation Description")
            out["VIOLATION_TYPE"] = get_col("VIOLATION_TYPE") if "VIOLATION_TYPE" in df.columns else out["VIOLATION_DESCRIPTION"].copy()
            # Classification will be done centrally after concatenation
            out["STATUS"] = get_col("STATUS", "Case Status Code")
            out["LOCATION"] = get_col("LOCATION", "Offense Street Name")
            out["WARNING_FLAG"] = get_col("WARNING_FLAG")
            out["SOURCE_FILE"] = path.name
            out["ETL_VERSION"] = "ATS_COURT_CSV"
            out["Year"] = out["ISSUE_DATE"].dt.year
            out["Month"] = out["ISSUE_DATE"].dt.month
            out["YearMonthKey"] = out["Year"] * 100 + out["Month"]
            out["Month_Year"] = out["ISSUE_DATE"].dt.strftime("%m-%y")
            
            # Add financial columns from ATS CSV
            if "TOTAL_PAID_AMOUNT" in df.columns:
                out["TOTAL_PAID_AMOUNT"] = pd.to_numeric(df["TOTAL_PAID_AMOUNT"], errors="coerce")
            else:
                out["TOTAL_PAID_AMOUNT"] = np.nan
            
            if "FINE_AMOUNT" in df.columns:
                out["FINE_AMOUNT"] = pd.to_numeric(df["FINE_AMOUNT"], errors="coerce")
            else:
                out["FINE_AMOUNT"] = np.nan
                
            if "COST_AMOUNT" in df.columns:
                out["COST_AMOUNT"] = pd.to_numeric(df["COST_AMOUNT"], errors="coerce")
            else:
                out["COST_AMOUNT"] = np.nan
                
            if "MISC_AMOUNT" in df.columns:
                out["MISC_AMOUNT"] = pd.to_numeric(df["MISC_AMOUNT"], errors="coerce")
            else:
                out["MISC_AMOUNT"] = np.nan
            return out
    elif path.suffix.lower() in [".xlsx", ".xls"]:
        df = pd.read_excel(path)
        df["SOURCE_FILE"] = path.name
        return df
    else:
        log.warning(f"Unknown file type: {path}")
        return pd.DataFrame()

# =========================
# Enforcement Rules
# =========================
def enforce_peo_rule(df, logger=None):
    """
    If WG3 is PEO or CLASS I, officers never issue moving summons.
    Force TYPE to 'P' and mark PEO_RULE_APPLIED=True where a flip occurs.
    Logs pre- and post-flip counts for transparency.
    """
    # Guards
    if "WG3" not in df.columns or "TYPE" not in df.columns:
        return df

    if "PEO_RULE_APPLIED" not in df.columns:
        df["PEO_RULE_APPLIED"] = False

    wg3_u = df["WG3"].astype(str).str.strip().str.upper()
    # Rows that SHOULD be flipped based on rule (before we touch TYPE)
    preflip_mask = wg3_u.isin(["PEO", "CLASS I"]) & (df["TYPE"].astype(str) == "M")
    preflip_count = int(preflip_mask.sum())

    # Apply flips
    df.loc[preflip_mask, "TYPE"] = "P"
    df.loc[preflip_mask, "PEO_RULE_APPLIED"] = True

    flipped_count = preflip_count  # Since we flip exactly these

    if logger is not None:
        logger.info(f"PEO/Class I rule — eligible to flip from M→P: {preflip_count}; flipped: {flipped_count}")

        # Optional: show a tiny sample for auditing
        if preflip_count > 0:
            sample = df.loc[preflip_mask, ["TICKET_NUMBER","PADDED_BADGE_NUMBER","WG3","TYPE"]].head(5)
            try:
                logger.info("PEO flip sample:\n" + sample.to_string(index=False))
            except Exception:
                pass

    return df

# =========================
# Officer Display Fallback
# =========================
def apply_officer_display_fallback(df):
    """
    Ensure OFFICER_DISPLAY_NAME is never blank and contains at most one badge tag.
    If OFFICER_DISPLAY_NAME is blank, fall back to OFFICER_NAME_RAW; if still blank, 'UNKNOWN OFFICER'.
    Append (####) once if neither (#####) nor (####) already present.
    """
    for col in ["OFFICER_DISPLAY_NAME", "OFFICER_NAME_RAW", "PADDED_BADGE_NUMBER"]:
        if col not in df.columns:
            df[col] = ""

    disp  = df["OFFICER_DISPLAY_NAME"].astype(str).str.strip()
    raw   = df["OFFICER_NAME_RAW"].astype(str).str.strip()
    badge = df["PADDED_BADGE_NUMBER"].astype(str).str.strip()

    # Fill blanks with raw, then UNKNOWN
    use_raw_mask = disp.eq("")
    disp = disp.mask(use_raw_mask, raw)
    disp = disp.mask(disp.astype(str).str.strip().eq(""), "UNKNOWN OFFICER")

    def _append_once(name, b):
        name = str(name)
        b = (b or "").strip()
        if not b:
            return name
        # If name already has (#1234) or (1234) or '#1234', don't add again
        if BADGE_PAREN_RE.search(name) or BADGE_HASH_RE.search(name):
            return name
        # Add a single " (####)"
        return f"{name} ({b})"

    df["OFFICER_DISPLAY_NAME"] = [ _append_once(n, b) for n, b in zip(disp, badge) ]
    return df

# =========================
# Quality / Enrichment
# =========================
# Old enrich_assign function removed - replaced with vectorized version

def add_quality(df):
    df["DATA_QUALITY_SCORE"] = 100
    # Check for missing ticket numbers if column exists
    if "TICKET_NUMBER" in df.columns:
        df.loc[df["TICKET_NUMBER"].astype(str).str.strip()=="", "DATA_QUALITY_SCORE"] -= 30
    # Check for assignment data
    if "ASSIGNMENT_FOUND" in df.columns:
        df.loc[~df["ASSIGNMENT_FOUND"], "DATA_QUALITY_SCORE"] -= 20
    df["DATA_QUALITY_SCORE"] = df["DATA_QUALITY_SCORE"].clip(lower=0)
    
    # Add DATA_QUALITY_TIER for M code compatibility
    df["DATA_QUALITY_TIER"] = "Medium"
    df.loc[df["DATA_QUALITY_SCORE"] >= 80, "DATA_QUALITY_TIER"] = "High"
    df.loc[df["DATA_QUALITY_SCORE"] < 60, "DATA_QUALITY_TIER"] = "Low"
    return df

# =========================
# Summary / Reporting
# =========================
def write_outputs_summary(all_df, overall, by_officer, by_officer_month, output_file, start, end):
    """Write summary markdown and changelog files"""
    base = output_file.parent
    now = datetime.now()
    ym = now.strftime("%Y%m")
    ts = now.strftime("%Y%m%d_%H%M%S")

    # Get Patrol Bureau data using pre-computed column
    patrol = all_df[all_df.get("IS_PATROL", False)].copy()

    # Calculate statistics
    counts_all = all_df["TYPE"].value_counts().to_dict()
    counts_patrol = patrol["TYPE"].value_counts().to_dict()
    peo_swaps = int((all_df.get("PEO_RULE_APPLIED", False) == True).sum())
    reason_counts = all_df["CLASSIFY_REASON"].value_counts().to_dict()

    # Get top officers for Patrol Bureau
    patrol_movers = patrol[patrol["TYPE"] == "M"].groupby(["OFFICER_DISPLAY_NAME", "PADDED_BADGE_NUMBER"]).size().sort_values(ascending=False).head(10)
    patrol_parkers = patrol[patrol["TYPE"] == "P"].groupby(["OFFICER_DISPLAY_NAME", "PADDED_BADGE_NUMBER"]).size().sort_values(ascending=False).head(10)

    # Get source files used
    source_files = sorted(set(all_df.get("SOURCE_FILE","").astype(str).unique()))
    
    # Create summary markdown
    summary_path = base / f"summons_summary_{ym}.md"
    
    summary = f"""# Summons Summary — {ym}

**Window:** {start.date()} → {end.date()}  
**Rows:** {len(all_df)}  |  **Unique Tickets:** {all_df['TICKET_NUMBER'].nunique()}

## Counts (All)
"""
    
    for t, c in counts_all.items():
        summary += f"- **{t}:** {c}\n"
    
    summary += f"""
## Counts (Patrol Bureau only)
"""
    
    for t, c in counts_patrol.items():
        summary += f"- **{t}:** {c}\n"
    
    summary += f"""
## Classification Reasons
"""
    
    for reason, count in reason_counts.items():
        summary += f"- **{reason}:** {count}\n"
    
    summary += f"""
**PEO/Class I rule applied:** {peo_swaps} rows

## Top 10 Officers - Moving Violations (Patrol Bureau)
"""
    
    for (name, badge), count in patrol_movers.items():
        summary += f"- **{name} ({badge}):** {count}\n"
    
    summary += f"""
## Top 10 Officers - Parking Violations (Patrol Bureau)
"""
    
    for (name, badge), count in patrol_parkers.items():
        summary += f"- **{name} ({badge}):** {count}\n"
    
    summary += f"""
**Outputs:** {output_file.name}  
**Sources used:** {", ".join(source_files)}
"""

    # Create changelog
    changelog_path = base / f"summons_changelog_{ts}.txt"
    
    changes = f"""{ts} — Script updates: badge padding + assignment mapping with POSS_CONTRACT_TYPE; fixed parking statutes
(175-10G, 175-10.2(G), 175-13.1(C)(1)); WG3 PEO/CLASS I rule (force to P); added summary/changelog files.
Adjusted rows due to PEO/CLASS I rule: {peo_swaps}
"""

    # Write files
    try:
        with open(summary_path, "w", encoding="utf-8") as f:
            f.write(summary)
        log.info(f"Summary written: {summary_path}")
        
        with open(changelog_path, "w", encoding="utf-8") as f:
            f.write(changes)
        log.info(f"Changelog written: {changelog_path}")
            
    except Exception as e:
        log.warning(f"Could not write summary/changelog files: {e}")

# =========================
# Main
# =========================
def main():
    start, end = rolling_window()
    
    # Load assignment data once
    assign_df = load_assignment(logger=log)
    
    frames = []
    for f in SOURCE_FILES:
        p = Path(f)
        if not p.exists():
            log.error(f"Missing: {p}")
            continue
        df = load_file(p)
        if df.empty:
            continue
        if "ISSUE_DATE" in df.columns:
            df = df[(df["ISSUE_DATE"] >= start) & (df["ISSUE_DATE"] <= end)]
        
        df = add_quality(df)
        df["PROCESSING_TIMESTAMP"] = datetime.now()
        # Filter out "U" type records if desired
        # df = df[df["TYPE"] != "U"]  # Uncomment this line to exclude "U" records
        frames.append(df)
    if not frames:
        log.error("No data loaded.")
        return False
    
    # Concatenate all source data
    all_df = pd.concat(frames, ignore_index=True, sort=False)
    all_df = all_df.drop_duplicates(subset=["TICKET_NUMBER"], keep="first")
    
    # Vectorized enrichment (fast)
    all_df = enrich_assign_vectorized(all_df, assign_df, log)
    
    # Normalize heavy string ops ONCE (reuse later)
    all_df["WG2_u"] = all_df["WG2"].astype(str).str.strip().str.upper()
    all_df["WG3_u"] = all_df["WG3"].astype(str).str.strip().str.upper()
    all_df["VIOLATION_NUMBER_NORM"] = all_df["VIOLATION_NUMBER"].astype(str).str.strip().str.upper()
    all_df["VIOLATION_DESCRIPTION_UP"] = all_df["VIOLATION_DESCRIPTION"].astype(str).str.upper()
    
    # For fast set-membership on explicit parking ordinances (remove whitespace)
    all_df["VIOLATION_NUMBER_NOSPACE"] = all_df["VIOLATION_NUMBER_NORM"].str.replace(r"\s+", "", regex=True)
    
    # Vectorized classification (fast, produces TYPE and CLASSIFY_REASON)
    all_df = classify_types_vectorized(all_df)
    
    # Log classification reason counts for audit
    reason_counts = all_df["CLASSIFY_REASON"].value_counts().to_dict()
    log.info(f"Classification reasons: {reason_counts}")
    
    # Display fallback (single pass)
    all_df = apply_officer_display_fallback(all_df)
    
    # Pre-check: how many rows would qualify BEFORE flipping?
    preflip_candidates = (all_df["WG3_u"].isin(["PEO","CLASS I"])) & (all_df["TYPE"].astype(str) == "M")
    log.info(f"PEO/Class I pre-check — potential M→P candidates: {int(preflip_candidates.sum())}")
    if preflip_candidates.any():
        sample = all_df.loc[preflip_candidates, ["TICKET_NUMBER","PADDED_BADGE_NUMBER","WG3","TYPE","VIOLATION_NUMBER"]].head(5)
        log.info("Sample candidates (preflip):\n" + sample.to_string(index=False))
    
    # Enforce PEO rule (uses WG3_u behind the scenes)
    all_df = enforce_peo_rule(all_df, logger=log)
    
    # If you group by WG2 == "PATROL BUREAU" often, keep a boolean mask column:
    all_df["IS_PATROL"] = all_df["WG2_u"].eq("PATROL BUREAU")
    
    # Cast common low-cardinality text columns to category to reduce memory + speed groupby
    for col in ["TYPE","WG2_u","WG3_u","OFFICER_DISPLAY_NAME","PADDED_BADGE_NUMBER"]:
        if col in all_df.columns:
            all_df[col] = all_df[col].astype("category")
    
    # Defragment before heavy operations
    all_df = all_df.copy()
    
    # Create PEO audit sheet for any flips that occurred
    peo_audit = all_df.loc[
        (all_df["PEO_RULE_APPLIED"] == True),
        ["TICKET_NUMBER","ISSUE_DATE","PADDED_BADGE_NUMBER","OFFICER_DISPLAY_NAME","WG3","TYPE","VIOLATION_NUMBER","VIOLATION_DESCRIPTION"]
    ].sort_values(["ISSUE_DATE","OFFICER_DISPLAY_NAME","TICKET_NUMBER"])
    
    # ===== Patrol Bureau movers/parkers summary =====
    # Use the pre-computed IS_PATROL boolean column for efficiency
    patrol = all_df[all_df["IS_PATROL"]].copy()

    # Overall counts (using observed=True for category optimization)
    overall = (
        patrol.groupby("TYPE", observed=True)
              .size()
              .reindex(["M","P"], fill_value=0)
              .rename_axis("TYPE")
              .reset_index(name="COUNT")
    )

    # Per-officer breakdown (OFFICER_DISPLAY_NAME + badge) for Movers (M) and Parkers (P)
    by_officer = (
        patrol.pivot_table(index=["OFFICER_DISPLAY_NAME","PADDED_BADGE_NUMBER","TEAM","WG2","WG3","WG4"],
                           columns="TYPE", values="TICKET_NUMBER", aggfunc="count", fill_value=0, observed=True)
              .reindex(columns=["M","P"], fill_value=0)
              .reset_index()
    )

    # Optional: also include Month_Year for trend by officer
    by_officer_month = (
        patrol.pivot_table(index=["Month_Year","OFFICER_DISPLAY_NAME","PADDED_BADGE_NUMBER","TEAM","WG2","WG3","WG4"],
                           columns="TYPE", values="TICKET_NUMBER", aggfunc="count", fill_value=0, observed=True)
              .reindex(columns=["M","P"], fill_value=0)
              .reset_index()
              .sort_values(["Month_Year","OFFICER_DISPLAY_NAME"])
    )
    
    # Create local reference to the global OUTPUT_FILE
    output_file = OUTPUT_FILE
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Handle file access issues
    try:
        # Try to remove existing file if it exists and is locked
        if output_file.exists():
            try:
                output_file.unlink()  # Remove existing file
                log.info(f"Removed existing file: {output_file}")
            except PermissionError:
                log.warning(f"Cannot remove existing file - it may be open in Excel. Trying to write with new name...")
                # Use timestamp to create unique filename
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_file = output_file.parent / f"summons_powerbi_latest_{timestamp}.xlsx"
                log.info(f"Using new filename: {output_file}")
        
        with pd.ExcelWriter(output_file, engine="openpyxl") as w:
            all_df.to_excel(w, sheet_name="Summons_Data", index=False)
            overall.to_excel(w, sheet_name="PB_Overall_MvsP", index=False)
            by_officer.to_excel(w, sheet_name="PB_ByOfficer_MvsP", index=False)
            by_officer_month.to_excel(w, sheet_name="PB_ByOfficer_Month", index=False)
            peo_audit.to_excel(w, sheet_name="PEO_Flips_Audit", index=False)
            
    except PermissionError as e:
        log.error(f"Permission denied writing to {output_file}. Please close Excel and try again.")
        log.error(f"Error details: {e}")
        return False
    except Exception as e:
        log.error(f"Unexpected error writing Excel file: {e}")
        return False

    if ARCHIVE_MONTHLY:
        stamp = datetime.now().strftime("%Y_%m")
        out2 = output_file.with_name(f"summons_powerbi_{stamp}.xlsx")
        try:
            # Remove existing archive file if it exists
            if out2.exists():
                out2.unlink()
                log.info(f"Removed existing archive file: {out2}")
            
            with pd.ExcelWriter(out2, engine="openpyxl") as w:
                all_df.to_excel(w, sheet_name="Summons_Data", index=False)
                overall.to_excel(w, sheet_name="PB_Overall_MvsP", index=False)
                by_officer.to_excel(w, sheet_name="PB_ByOfficer_MvsP", index=False)
                by_officer_month.to_excel(w, sheet_name="PB_ByOfficer_Month", index=False)
                peo_audit.to_excel(w, sheet_name="PEO_Flips_Audit", index=False)
        except PermissionError as e:
            log.warning(f"Could not create archive file {out2}: {e}")
        except Exception as e:
            log.warning(f"Unexpected error creating archive file: {e}")
    
    # Write summary and changelog files
    write_outputs_summary(all_df, overall, by_officer, by_officer_month, output_file, start, end)
    
    log.info(f"Saved: {output_file} | Rows: {len(all_df)}")
    return True

if __name__ == "__main__":
    sys.exit(0 if main() else 1)
