"""Normalization helpers for Summons ETL date windows.

`normalize_date_windows` enforces the one-month-per-row guardrail on
assignment/override windows: any row whose [date_start, date_end] interval
spans more than one calendar month is split into N rows, one per month,
with each sub-range clipped to that month's boundaries. Open-ended end
dates are clipped to a far-future sentinel so they survive splitting.
"""

from __future__ import annotations

import pandas as pd

FAR_FUTURE = pd.Timestamp("2099-12-31")


def normalize_date_windows(
    df: pd.DataFrame,
    start_col: str = "date_start",
    end_col: str = "date_end",
) -> pd.DataFrame:
    """Split rows so no row's [start, end] window spans more than one calendar month.

    - Null/missing end dates are treated as open-ended and clipped to FAR_FUTURE.
    - Rows wholly within a single month pass through unchanged (preserving dtypes
      where possible).
    - Returned frame has the same columns as input; index is reset.
    - Pure function; does not mutate the input.
    """
    if df.empty:
        return df.copy()
    if start_col not in df.columns or end_col not in df.columns:
        raise KeyError(f"normalize_date_windows requires columns {start_col!r} and {end_col!r}")

    work = df.copy()
    work[start_col] = pd.to_datetime(work[start_col], errors="coerce")
    work[end_col] = pd.to_datetime(work[end_col], errors="coerce").fillna(FAR_FUTURE)

    bad = work[start_col].isna()
    if bad.any():
        raise ValueError(f"normalize_date_windows: {int(bad.sum())} row(s) have unparseable {start_col}")

    out_rows: list[dict] = []
    for _, row in work.iterrows():
        start = row[start_col]
        end = row[end_col]
        if end < start:
            raise ValueError(f"normalize_date_windows: {end_col} ({end}) precedes {start_col} ({start})")

        # Iterate per-month: each segment runs from max(start, month_first) to min(end, month_last)
        cursor = start
        while cursor <= end:
            month_last = (cursor + pd.offsets.MonthEnd(0)).normalize()
            seg_end = min(end, month_last)
            new_row = row.to_dict()
            new_row[start_col] = cursor
            new_row[end_col] = seg_end
            out_rows.append(new_row)
            cursor = (month_last + pd.Timedelta(days=1)).normalize()

    result = pd.DataFrame(out_rows, columns=df.columns).reset_index(drop=True)
    return result


if __name__ == "__main__":
    # Smoke test
    sample = pd.DataFrame([
        {"badge": "0115", "date_start": "2026-04-06", "date_end": None, "label": "TTD"},
        {"badge": "2025", "date_start": "2026-03-01", "date_end": "2026-03-04", "label": "MARCH"},
        {"badge": "9999", "date_start": "2026-02-15", "date_end": "2026-05-10", "label": "MULTI"},
    ])
    out = normalize_date_windows(sample)
    print(out.to_string(index=False))
    assert (out["date_end"] - out["date_start"]).max() <= pd.Timedelta(days=31), "guardrail breach"
    multi = out[out["label"] == "MULTI"]
    assert len(multi) == 4, f"expected 4 month-split rows for MULTI, got {len(multi)}"
    print("\nnormalize_date_windows smoke test PASSED")
