#!/usr/bin/env python3
# SummonsMaster — E-Ticket ETL with Assignment join
# Output: ...\03_Staging\Summons\summons_powerbi_latest.xlsx

import pandas as pd, numpy as np, re, calendar, sys, warnings, logging
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from pathlib import Path
from glob import glob

warnings.filterwarnings("ignore", category=UserWarning)

# ===== CONFIG =====
E_TICKET_FOLDER = r"C:\Users\carucci_r\OneDrive - City of Hackensack\05_EXPORTS\_Summons\E_Ticket"
ASSIGNMENT_FILE = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\09_Reference\Personnel\Assignment_Master_V2.csv")

OUTPUT_FILE = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons\summons_powerbi_latest.xlsx")
ARCHIVE_MONTHLY = True
LOG_FILE = "summons_processing.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("summons_etl")

# ===== WINDOW (13 months ending prior month) =====
def rolling_window():
    today = date.today()
    py = today.year if today.month > 1 else today.year - 1
    pm = today.month - 1 if today.month > 1 else 12
    end_day = calendar.monthrange(py, pm)[1]
    end_d = date(py, pm, end_day)
    start_d = (end_d - relativedelta(months=12)).replace(day=1)
    log.info(f"Window: {start_d} → {end_d}")
    return pd.Timestamp(start_d), pd.Timestamp(end_d)

# ===== HELPERS =====
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
    if s is None or str(s).strip() == "":
        return pd.NaT
    for f in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%m/%d/%y"):
        try:
            return pd.to_datetime(s, format=f, errors="raise")
        except Exception:
            pass
    return pd.to_datetime(s, errors="coerce")

def to_money(x):
    if pd.isna(x): return 0.0
    s = str(x).strip().replace(",", "")
    try: return float(s)
    except Exception:
        m = re.findall(r"[-]?\d+(?:\.\d+)?", s)
        return float(m[0]) if m else 0.0

def sget(df, primary, alt=None, fill=""):
    if primary in df.columns: return df[primary]
    if alt and alt in df.columns: return df[alt]
    return pd.Series([fill]*len(df))

# ===== ASSIGNMENTS (CSV) =====
# Requires PADDED_BADGE_NUMBER and Proposed 4-Digit Format.
# Also TEAM, WG1..WG5, POSS_CONTRACT_TYPE if present.
def load_assignment():
    if not ASSIGNMENT_FILE.exists():
        log.warning(f"Assignment file missing: {ASSIGNMENT_FILE}")
        return pd.DataFrame()
    enc = detect_encoding(ASSIGNMENT_FILE)
    delim = detect_delimiter(ASSIGNMENT_FILE, enc)
    df = pd.read_csv(ASSIGNMENT_FILE, dtype=str, encoding=enc, delimiter=delim, keep_default_na=False)
    df.columns = [c.strip() for c in df.columns]
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()

    def pick(*names):
        for n in names:
            if n in df.columns: return n
        return None

    col_badge = pick("PADDED_BADGE_NUMBER","Padded_Badge","BADGE_PADDED","Badge_Padded","Badge 4-Digit")
    if not col_badge:
        raw = pick("BADGE_NUMBER","Badge","Officer_Badge")
        if raw:
            df["PADDED_BADGE_NUMBER"] = df[raw].str.replace(r"[^\d]","",regex=True).str.zfill(4)
            col_badge = "PADDED_BADGE_NUMBER"
        else:
            df["PADDED_BADGE_NUMBER"] = ""
            col_badge = "PADDED_BADGE_NUMBER"

    col_disp = pick("Proposed 4-Digit Format")
    if not col_disp:
        df["Proposed 4-Digit Format"] = ""
        col_disp = "Proposed 4-Digit Format"

    for col in ["TEAM","WG1","WG2","WG3","WG4","WG5","POSS_CONTRACT_TYPE"]:
        if col not in df.columns:
            df[col] = ""

    df_std = df.rename(columns={col_badge: "PADDED_BADGE_NUMBER", col_disp: "PROPOSED_4_DIGIT_FORMAT"})

    # normalize key
    df_std["PADDED_BADGE_NUMBER"] = (
        df_std["PADDED_BADGE_NUMBER"].astype(str).str.strip().str.replace(r"[^\d]", "", regex=True).str.zfill(4)
    )

    keep = ["PADDED_BADGE_NUMBER","PROPOSED_4_DIGIT_FORMAT","TEAM","WG1","WG2","WG3","WG4","WG5","POSS_CONTRACT_TYPE"]
    return df_std[keep].drop_duplicates()

# ===== TARGET EXPORT SCHEMA =====
ATS_EXPORT_COLS = [
    "PADDED_BADGE_NUMBER", "Proposed 4-Digit Format",  # display as requested
    "TEAM","WG1","WG2","WG3","WG4","WG5","POSS_CONTRACT_TYPE",
    "ORI","TICKET_NUMBER","ISSUE_DATE",
    "VIOLATION_NUMBER","TYPE","STATUS","DISPOSITION_DATE","FIND_CD","PAYMENT_DATE",
    "ASSESSED_AMOUNT","FINE_AMOUNT","COST_AMOUNT","MISC_AMOUNT","TOTAL_PAID_AMOUNT","CITY_COST_AMOUNT",
    "Year","Month","YearMonthKey","Month_Year",
    "ASSIGNMENT_FOUND","DATA_QUALITY_SCORE","DATA_QUALITY_TIER","SOURCE_FILE","ETL_VERSION","PROCESSING_TIMESTAMP",
    "VIOLATION_DESCRIPTION","LOCATION","VEHICLE_MAKE","VEHICLE_MODEL","PLATE","STATE","COURT_DATE","COMMENTS",
    "WARNING_FLAG",
]

# ===== LOAD E-TICKET CSV =====
def load_eticket_csv(path: Path) -> pd.DataFrame:
    enc = detect_encoding(path)
    delim = detect_delimiter(path, enc)
    df = pd.read_csv(path, dtype=str, keep_default_na=False, encoding=enc, delimiter=delim)
    df.columns = [c.strip() for c in df.columns]
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()

    out = pd.DataFrame()
    out["TICKET_NUMBER"]         = sget(df, "Ticket Number")
    out["ISSUE_DATE"]            = sget(df, "Issue Date").apply(parse_date)
    out["BADGE_NUMBER_RAW"]      = sget(df, "Officer Id")
    out["PADDED_BADGE_NUMBER"]   = out["BADGE_NUMBER_RAW"].astype(str).str.replace(r"[^\d]","",regex=True).str.zfill(4)
    out["VIOLATION_NUMBER"]      = sget(df, "Statute")
    out["VIOLATION_DESCRIPTION"] = sget(df, "Violation Description")
    out["TYPE"]                  = sget(df, "Case Type Code")
    out["STATUS"]                = sget(df, "Case Status Code")
    out["DISPOSITION_DATE"]      = sget(df, "Court Date").apply(parse_date)
    out["FIND_CD"]               = ""
    out["PAYMENT_DATE"]          = pd.NaT
    out["ASSESSED_AMOUNT"]       = 0.0
    out["FINE_AMOUNT"]           = sget(df, "Penalty").apply(to_money)
    out["COST_AMOUNT"]           = 0.0
    out["MISC_AMOUNT"]           = 0.0
    out["TOTAL_PAID_AMOUNT"]     = 0.0
    out["CITY_COST_AMOUNT"]      = 0.0
    out["LOCATION"]              = sget(df, "Offense Street Name")
    out["VEHICLE_MAKE"]          = sget(df, "Make Description", alt="Vehicle Make")
    out["VEHICLE_MODEL"]         = sget(df, "Model Year", alt="Vehicle Body Description")
    out["PLATE"]                 = sget(df, "License Plate Number", alt="Plate")
    out["STATE"]                 = sget(df, "License Plate State Code", alt="State").str.upper()
    out["COURT_DATE"]            = sget(df, "Court Date")
    out["COMMENTS"]              = sget(df, "Parking Note", alt="Moving Note")
    out["WARNING_FLAG"]          = sget(df, "Warning", alt="Written Warning")
    out["ORI"]                   = sget(df, "Jurisdiction Code")

    out["Year"]         = out["ISSUE_DATE"].dt.year
    out["Month"]        = out["ISSUE_DATE"].dt.month
    out["YearMonthKey"] = out["Year"]*100 + out["Month"]
    out["Month_Year"]   = out["ISSUE_DATE"].dt.strftime("%m-%y")

    out["SOURCE_FILE"]  = path.name
    out["ETL_VERSION"]  = "ETICKET_TO_ATS_VMAP"
    return out

# ===== ENRICHMENT: merge assignments (no name fallback) =====
def enrich_assign(df: pd.DataFrame, assign_df: pd.DataFrame) -> pd.DataFrame:
    if "PADDED_BADGE_NUMBER" not in df.columns:
        df["PADDED_BADGE_NUMBER"] = (
            df.get("BADGE_NUMBER_RAW","").astype(str).str.replace(r"[^\d]","",regex=True).str.zfill(4)
        )

    if assign_df is None or assign_df.empty:
        for c in ["TEAM","WG1","WG2","WG3","WG4","WG5","POSS_CONTRACT_TYPE"]:
            if c not in df.columns: df[c] = ""
        df["PROPOSED_4_DIGIT_FORMAT"] = ""
        df["ASSIGNMENT_FOUND"] = False
        return df

    # drop any existing org/display columns to avoid overlap
    df = df.drop(columns=[c for c in ["TEAM","WG1","WG2","WG3","WG4","WG5","POSS_CONTRACT_TYPE","PROPOSED_4_DIGIT_FORMAT"] if c in df.columns], errors="ignore")

    # normalize key on both sides
    df["PADDED_BADGE_NUMBER"] = df["PADDED_BADGE_NUMBER"].astype(str).str.strip().str.replace(r"[^\d]","",regex=True).str.zfill(4)
    assign_df["PADDED_BADGE_NUMBER"] = assign_df["PADDED_BADGE_NUMBER"].astype(str).str.strip().str.replace(r"[^\d]","",regex=True).str.zfill(4)

    merged = df.merge(assign_df, on="PADDED_BADGE_NUMBER", how="left")

    merged["PROPOSED_4_DIGIT_FORMAT"] = merged["PROPOSED_4_DIGIT_FORMAT"].fillna("")
    for c in ["TEAM","WG1","WG2","WG3","WG4","WG5","POSS_CONTRACT_TYPE"]:
        merged[c] = merged[c].fillna("")

    merged["ASSIGNMENT_FOUND"] = merged["PROPOSED_4_DIGIT_FORMAT"].ne("")
    return merged

# ===== MAIN =====
def main():
    start, end = rolling_window()

    files = [Path(p) for p in glob(fr"{E_TICKET_FOLDER}\*.csv")]
    if not files:
        log.error(f"No e-ticket CSVs found in: {E_TICKET_FOLDER}")
        return False
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)

    assign_df = load_assignment()

    frames = []
    for p in files:
        try:
            df = load_eticket_csv(p)
        except Exception as e:
            log.error(f"Load failed: {p.name} | {e}")
            continue

        before = len(df)
        df = df[(df["ISSUE_DATE"].notna()) & (df["ISSUE_DATE"] >= start) & (df["ISSUE_DATE"] <= end)]
        log.info(f"{p.name}: window {before} → {len(df)}")
        if df.empty:
            continue

        df = enrich_assign(df, assign_df)

        for col in ["ASSESSED_AMOUNT","FINE_AMOUNT","COST_AMOUNT","MISC_AMOUNT","TOTAL_PAID_AMOUNT","CITY_COST_AMOUNT"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

        df["DATA_QUALITY_SCORE"] = 100.0
        df.loc[df["TICKET_NUMBER"].astype(str).str.strip()=="","DATA_QUALITY_SCORE"] -= 30
        df.loc[~df["ASSIGNMENT_FOUND"],"DATA_QUALITY_SCORE"] -= 20
        df["DATA_QUALITY_SCORE"] = df["DATA_QUALITY_SCORE"].clip(lower=0)
        df["DATA_QUALITY_TIER"] = pd.cut(
            df["DATA_QUALITY_SCORE"], bins=[-0.1,50,75,90,100],
            labels=["Poor","Fair","Good","Excellent"], include_lowest=True
        )

        df["PROCESSING_TIMESTAMP"] = datetime.now()
        frames.append(df)

    if not frames:
        log.error("No rows after filtering/merge.")
        return False

    combined = pd.concat(frames, ignore_index=True, sort=False)
    pre = len(combined)
    combined = combined.sort_values(by=["ISSUE_DATE","TICKET_NUMBER"]).drop_duplicates(subset=["TICKET_NUMBER"], keep="first")
    log.info(f"Dedup: {pre} → {len(combined)}")

    # Build export frame
    export = combined.copy()
    # exact output column name
    export["Proposed 4-Digit Format"] = export.get("PROPOSED_4_DIGIT_FORMAT", "")

    # Do NOT export OFFICER_NAME_RAW or OFFICER_DISPLAY_NAME
    for drop_col in ["OFFICER_NAME_RAW","OFFICER_DISPLAY_NAME","PROPOSED_4_DIGIT_FORMAT"]:
        if drop_col in export.columns and drop_col not in ATS_EXPORT_COLS:
            pass  # keep internal only; not referenced in ATS_EXPORT_COLS

    final_cols = [c for c in ATS_EXPORT_COLS if c in export.columns]
    final_df = export[final_cols].copy()

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    # Primary save with permission handling
    try:
        with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as w:
            final_df.to_excel(w, sheet_name="Summons_Data", index=False)
        log.info(f"Saved: {OUTPUT_FILE}")
    except PermissionError as e:
        # Target file may be open in Excel. Fall back to a timestamped file and warn the user.
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        alt = OUTPUT_FILE.with_name(f"summons_powerbi_latest_{ts}.xlsx")
        try:
            with pd.ExcelWriter(alt, engine="openpyxl") as w:
                final_df.to_excel(w, sheet_name="Summons_Data", index=False)
            log.warning(f"Could not write {OUTPUT_FILE} (permission denied). Saved to {alt} instead. Close {OUTPUT_FILE} to overwrite.")
        except Exception as e2:
            log.error(f"Failed to save to alternate path {alt}: {e2}")
            return False

    if ARCHIVE_MONTHLY:
        stamp = datetime.now().strftime("%Y_%m")
        monthly = OUTPUT_FILE.with_name(f"summons_powerbi_{stamp}.xlsx")
        try:
            with pd.ExcelWriter(monthly, engine="openpyxl") as w:
                final_df.to_excel(w, sheet_name="Summons_Data", index=False)
            log.info(f"Saved: {monthly}")
        except PermissionError:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            altm = OUTPUT_FILE.with_name(f"summons_powerbi_{stamp}_{ts}.xlsx")
            try:
                with pd.ExcelWriter(altm, engine="openpyxl") as w:
                    final_df.to_excel(w, sheet_name="Summons_Data", index=False)
                log.warning(f"Could not write archive {monthly} (permission denied). Saved to {altm} instead.")
            except Exception as e3:
                log.error(f"Failed to save archive to alternate path {altm}: {e3}")
                return False

    # coverage report
    matches = final_df["Proposed 4-Digit Format"].ne("").sum()
    log.info(f"Assignment display populated: {matches}/{len(final_df)} ({matches/len(final_df)*100:.1f}%)")
    log.info(f"Rows: {len(final_df)}")
    return True

if __name__ == "__main__":
    sys.exit(0 if main() else 1)
