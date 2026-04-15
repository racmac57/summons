"""Verify staged Summons output against the raw e-ticket export for a report month.

Usage:
    python scripts/verify_summons_against_raw.py --report-month 2026-03

Reports row counts, distinct badges, remaining UNKNOWN tags, and badges resolved
to INACTIVE personnel via the historical lookup. Exits non-zero if any badge
present in the raw export has STATUS=INACTIVE in the Assignment Master but is
not tagged with a real display name in staging.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack")
STAGING = ROOT / "03_Staging" / "Summons" / "summons_powerbi_latest.xlsx"
ASSIGN = ROOT / "09_Reference" / "Personnel" / "Assignment_Master_V2.csv"


def raw_path(report_month: str) -> Path:
    yyyy, mm = report_month.split("-")
    base = ROOT / "05_EXPORTS" / "_Summons" / "E_Ticket" / yyyy / "month"
    for name in (f"{yyyy}_{mm}_eticket_export.csv", f"{yyyy}_{mm}_eticket_export_FIXED.csv"):
        p = base / name
        if p.exists():
            return p
    raise FileNotFoundError(f"No raw e-ticket export for {report_month} under {base}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report-month", required=True, help="YYYY-MM")
    args = ap.parse_args()

    raw_p = raw_path(args.report_month)
    print(f"Raw     : {raw_p}")
    print(f"Staging : {STAGING}")
    print(f"Master  : {ASSIGN}")

    raw = pd.read_csv(raw_p, sep=";", dtype=str, low_memory=False, on_bad_lines="skip")
    if "Officer Id" not in raw.columns:
        raw = pd.read_csv(raw_p, sep=",", dtype=str, low_memory=False, on_bad_lines="skip")
    raw = raw[raw["Officer Id"].astype(str).str.strip().str.match(r"^\d+$").fillna(False)]
    raw["BADGE"] = raw["Officer Id"].astype(str).str.strip().str.zfill(4)

    staged = pd.read_excel(STAGING, dtype={"PADDED_BADGE_NUMBER": str})
    staged["PADDED_BADGE_NUMBER"] = staged["PADDED_BADGE_NUMBER"].astype(str).str.zfill(4)
    if "VIOLATION_DATE" in staged.columns:
        vd = pd.to_datetime(staged["VIOLATION_DATE"], errors="coerce")
        staged_month = staged[vd.dt.strftime("%Y-%m") == args.report_month].copy()
    else:
        staged_month = staged.copy()

    master = pd.read_csv(ASSIGN, dtype=str)
    master.columns = [c.strip() for c in master.columns]
    master["PADDED_BADGE_NUMBER"] = master["PADDED_BADGE_NUMBER"].astype(str).str.zfill(4)
    inactive_badges = set(
        master.loc[
            master.get("STATUS", pd.Series(dtype=str)).fillna("").str.strip().str.upper() == "INACTIVE",
            "PADDED_BADGE_NUMBER",
        ].tolist()
    )

    print()
    print(f"Raw rows         : {len(raw)}")
    print(f"Staged rows ({args.report_month}): {len(staged_month)}")
    print(f"Raw distinct badges     : {raw['BADGE'].nunique()}")
    print(f"Staged distinct badges  : {staged_month['PADDED_BADGE_NUMBER'].nunique()}")

    unknown = staged_month[staged_month["OFFICER_DISPLAY_NAME"].astype(str).str.startswith("UNKNOWN")]
    print(f"Remaining UNKNOWN rows  : {len(unknown)}")

    raw_badges = set(raw["BADGE"].unique())
    inactive_in_raw = raw_badges & inactive_badges
    print(f"INACTIVE badges in raw  : {len(inactive_in_raw)} -> {sorted(inactive_in_raw)}")

    if inactive_in_raw:
        resolved = staged_month[
            staged_month["PADDED_BADGE_NUMBER"].isin(inactive_in_raw)
            & ~staged_month["OFFICER_DISPLAY_NAME"].astype(str).str.startswith("UNKNOWN")
        ]
        sample_cols = [c for c in ("PADDED_BADGE_NUMBER", "OFFICER_DISPLAY_NAME", "STATUS", "INACTIVE_REASON")
                       if c in resolved.columns]
        print(f"Resolved INACTIVE rows  : {len(resolved)}")
        print("\nSample (up to 10):")
        print(resolved[sample_cols].drop_duplicates().head(10).to_string(index=False))

        unresolved = inactive_in_raw - set(resolved["PADDED_BADGE_NUMBER"].unique())
        if unresolved:
            print(f"\n[FAIL] INACTIVE badges in raw not tagged in staging: {sorted(unresolved)}")
            return 1

    print("\n[OK] Verification passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
