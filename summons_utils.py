# summons_utils.py
import pandas as pd
from fuzzywuzzy import process
import re
import logging
import yaml
from importlib import reload
from logging.handlers import RotatingFileHandler

# === Logging Config ===
def init_logger(name='summons', level=logging.INFO, log_to_file=True):
    logger = logging.getLogger(name)
    if not logger.handlers:
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        if log_to_file:
            file_handler = RotatingFileHandler("summons_etl.log", maxBytes=1048576, backupCount=3)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
        logger.setLevel(level)
    return logger

# === Config Loader ===
def load_config(path="config.yaml"):
    with open(path, "r") as file:
        return yaml.safe_load(file)

# === Badge & Name Utilities ===
def clean_badge_numbers(series):
    return series.astype(str).str.extract(r'(\d+)')[0].fillna('0').str.zfill(4)

def parse_officer_name(name):
    if pd.isna(name) or not str(name).strip():
        return None, None
    name_str = str(name).upper().strip()
    for prefix in ['P.O.', 'SGT', 'LT', 'CAPT', 'DET', 'OFFICER', 'SERGEANT']:
        name_str = re.sub(rf'^{re.escape(prefix)}\s*', '', name_str)
    parts = name_str.split()
    if len(parts) >= 2:
        return parts[0][0], parts[1]
    return None, None

def fuzzy_officer_match(target, candidates_df):
    search_name = f"{target['FIRST_INITIAL']} {target['LAST_NAME']}"
    candidates = candidates_df.apply(
        lambda x: f"{x['FIRST_INITIAL']} {x['LAST_NAME']}" if pd.notna(x['FIRST_INITIAL']) else '', axis=1
    ).tolist()
    match = process.extractOne(search_name, candidates, scorer=process.default_scorer)
    if match and match[1] >= 80:
        return candidates_df.iloc[candidates.index(match[0])]
    return pd.Series()

# === Header and Column Handling ===
def detect_header_row(df):
    candidates = ["OFFICER", "TICKET", "ISSUE DATE"]
    for i, row in df.iterrows():
        if any(any(str(cell).upper().strip().startswith(c) for cell in row) for c in candidates):
            return i
    return 0

def remap_columns(df, column_map):
    df = df.copy()
    for old_col, new_col in column_map.items():
        if old_col in df.columns:
            df.rename(columns={old_col: new_col}, inplace=True)
    return df

def clean_wrapped_headers(df):
    header_row = detect_header_row(df)
    trimmed = df.iloc[header_row:].reset_index(drop=True)
    headers = trimmed.iloc[0].astype(str).str.replace(r'\n|\r', ' ', regex=True).str.strip().tolist()
    officer_idx = next((i for i, h in enumerate(headers) if "OFFICER" in h.upper()), None)
    if officer_idx is not None:
        headers.insert(officer_idx, "BADGE_NUMBER")
        headers[officer_idx + 1] = "OFFICER_NAME"
        headers.insert(officer_idx + 2, "ORI")
    data = trimmed.iloc[1:].reset_index(drop=True)
    data.columns = headers[:len(data.columns)]

    required_columns = ["BADGE_NUMBER", "OFFICER_NAME", "TICKET_NUMBER", "ISSUE_DATE"]
    missing = [col for col in required_columns if col not in data.columns]
    if missing:
        logging.warning(f"Missing required columns: {missing}. File may be skipped or incomplete.")

    logging.info(f"Parsed headers: {data.columns.tolist()}")
    logging.info(f"Loaded {len(data)} rows from cleaned DataFrame.")

    return data

# === Assignment Matching ===
def enrich_with_assignment(data_df, assignment_df):
    assignment_df['PADDED_BADGE_NUMBER'] = assignment_df['PADDED_BADGE_NUMBER'].astype(str).str.zfill(4)
    assignment_df['FIRST_INITIAL'] = assignment_df['FIRST_NAME'].str[0].str.upper()
    assignment_df['LAST_NAME'] = assignment_df['LAST_NAME'].str.upper().str.strip()

    merged = pd.merge(
        data_df,
        assignment_df,
        left_on='BADGE_NUMBER',
        right_on='PADDED_BADGE_NUMBER',
        how='left',
        suffixes=('', '_ASSIGN')
    )

    unmatched = merged['FIRST_NAME'].isna()
    total_unmatched = unmatched.sum()
    logging.info(f"Fuzzy matching needed for {total_unmatched} unmatched officers")

    for idx in merged[unmatched].index:
        row = merged.loc[idx]
        if pd.notna(row['FIRST_INITIAL']) and pd.notna(row['LAST_NAME']):
            match = fuzzy_officer_match(row, assignment_df)
            for col in ['FIRST_NAME', 'LAST_NAME', 'Division', 'Unit', 'Squad', 'ASSIGNMENT']:
                if col in assignment_df.columns and col in match:
                    merged.at[idx, col] = match[col]

    logging.info(f"Final matched assignments: {merged['FIRST_NAME'].notna().sum()}/{len(merged)}")
    return merged
