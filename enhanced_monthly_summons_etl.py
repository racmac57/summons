#!/usr/bin/env python3
# 🕒 2025-09-08-18-30-00
# Project: SummonsMaster/process_monthly_summons_with_assignments.py
# Author: R. A. Carucci
# Purpose: Monthly summons processing with full assignment integration for Power BI

import pandas as pd
import numpy as np
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from pathlib import Path
import logging
import sys
import warnings
import calendar
import os

# Standardized ETL writer import
TOOLS_ABS = r"C:\Dev\Power_BI_Data\tools"
TOOLS_REL = Path(__file__).resolve().parent.parent / 'tools'
if TOOLS_ABS not in sys.path and not Path(TOOLS_ABS).exists():
    sys.path.insert(0, str(TOOLS_REL))
else:
    sys.path.insert(0, TOOLS_ABS)

from etl_output_writer import write_current_month, normalize_monthkey

# Suppress openpyxl warnings
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

# Configuration
SOURCE_FOLDER = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\05_EXPORTS\_Summons\Court")
ASSIGNMENT_FILE = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\_Hackensack_Data_Repository\ASSIGNED_SHIFT\Assignment_Master_V2.xlsx")
OUTPUT_FILE = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons\summons_powerbi_latest.xlsx")

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('summons_processing.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def load_assignment_master():
    """Load and prepare Assignment Master V2 with comprehensive badge lookup"""
    try:
        logger.info("Loading Assignment Master V2...")
        
        # Load Assignment Master V2
        assignment_df = pd.read_excel(ASSIGNMENT_FILE, sheet_name='Sheet1')
        logger.info(f"Loaded {len(assignment_df)} assignment records")
        
        # Create comprehensive badge lookup dictionary
        badge_lookup = {}
        
        for _, row in assignment_df.iterrows():
            # Get badge variations
            raw_badge = str(row['BADGE_NUMBER']).strip()
            padded_badge = str(row['PADDED_BADGE_NUMBER']).strip()
            
            # Create multiple badge format variations
            badge_variations = [
                raw_badge,
                raw_badge.zfill(4),
                padded_badge,
                str(int(float(raw_badge))).zfill(4) if raw_badge.replace('.', '').isdigit() else raw_badge.zfill(4)
            ]
            badge_variations = list(dict.fromkeys(badge_variations))  # Remove duplicates
            
            # Create assignment data
            assignment_data = {
                'OFFICER_DISPLAY_NAME': str(row['Proposed 4-Digit Format']).strip(),
                'WG1': str(row['WG1']).strip(),
                'WG2': str(row['WG2']).strip(),  # Bureau (e.g., "Patrol Bureau")
                'WG3': str(row['WG3']).strip(),
                'WG4': str(row['WG4']).strip() if pd.notna(row['WG4']) else '',
                'WG5': str(row['WG5']).strip() if pd.notna(row['WG5']) else '',
                'FIRST_NAME': str(row['FIRST_NAME']).strip() if 'FIRST_NAME' in row else '',
                'LAST_NAME': str(row['LAST_NAME']).strip() if 'LAST_NAME' in row else ''
            }
            
            # Add all badge variations to lookup
            for badge_var in badge_variations:
                if badge_var and badge_var != 'nan':
                    badge_lookup[badge_var] = assignment_data
        
        logger.info(f"Created badge lookup with {len(badge_lookup)} entries")
        return badge_lookup
        
    except Exception as e:
        logger.error(f"Error loading Assignment Master: {str(e)}")
        return {}

def calculate_date_range():
    """Calculate the 13-month rolling window ending with previous month from today"""
    today = date.today()
    
    # End date is the last day of the previous month
    if today.month == 1:
        end_date = date(today.year - 1, 12, 31)
    else:
        # Get last day of previous month
        last_day = calendar.monthrange(today.year, today.month - 1)[1]
        end_date = date(today.year, today.month - 1, last_day)
    
    # Start date is 13 months back from end date (first day of that month)
    start_temp = end_date - relativedelta(months=12)
    start_date = date(start_temp.year, start_temp.month, 1)
    
    logger.info(f"Processing 13-month rolling window: {start_date} to {end_date}")
    logger.info(f"This covers {(end_date.year - start_date.year) * 12 + (end_date.month - start_date.month) + 1} months")
    
    return start_date, end_date

def filter_monthly_files(folder_path):
    """Filter xlsx files to include monthly ATS files and specific summary files"""
    if not folder_path.exists():
        logger.error(f"Source folder does not exist: {folder_path}")
        raise FileNotFoundError(f"Source folder not found: {folder_path}")
    
    xlsx_files = list(folder_path.glob("*.xlsx"))
    logger.info(f"Found {len(xlsx_files)} xlsx files in source folder")
    
    filtered_files = []
    
    for file_path in xlsx_files:
        filename = file_path.name
        include_file = False
        
        # Include monthly ATS files
        if filename.endswith('_ATS.xlsx'):
            include_file = True
            logger.info(f"Including monthly ATS file: {filename}")
        
        # Include specifically 24_ALL_SUMMONS.xlsx (2024 data)
        elif filename == '24_ALL_SUMMONS.xlsx':
            include_file = True
            logger.info(f"Including 2024 summary file: {filename}")
        
        if include_file:
            filtered_files.append(file_path)
    
    logger.info(f"Filtered to {len(filtered_files)} files for processing")
    return filtered_files

def extract_and_process_file(file_path, badge_lookup, start_date, end_date):
    """Extract data from file and apply assignment matching"""
    try:
        logger.info(f"Processing file: {file_path.name}")
        
        # Read Excel file starting from row 5 (skip headers)
        df = pd.read_excel(file_path, skiprows=4)
        
        # Standardize column names (handle line breaks in headers)
        if len(df.columns) >= 17:
            df.columns = [
                'BADGE_NUMBER_RAW', 'OFFICER_NAME_RAW', 'ORI', 'TICKET_NUMBER', 'ISSUE_DATE',
                'VIOLATION_NUMBER', 'TYPE', 'STATUS', 'DISPOSITION_DATE', 'FIND_CD', 'PAYMENT_DATE',
                'ASSESSED_AMOUNT', 'FINE_AMOUNT', 'COST_AMOUNT', 'MISC_AMOUNT', 'TOTAL_PAID_AMOUNT', 'CITY_COST_AMOUNT'
            ]
        
        logger.info(f"  - Loaded {len(df)} rows")
        
        # Add source file tracking
        df['SOURCE_FILE'] = file_path.name
        
        # Clean badge numbers
        df['BADGE_CLEAN'] = df['BADGE_NUMBER_RAW'].astype(str).str.strip()
        
        # Remove footer rows and invalid records
        footer_conditions = (
            df['BADGE_CLEAN'].str.upper().str.contains('TOTAL', na=False) |
            df['OFFICER_NAME_RAW'].astype(str).str.contains('Run Date', na=False) |
            df['BADGE_CLEAN'].str.len() > 10
        )
        df = df[~footer_conditions]
        
        # Remove civilian complaints and invalid badges
        invalid_badges = ['9999', '0000', 'nan', '', '0']
        df = df[~df['BADGE_CLEAN'].isin(invalid_badges)]
        
        # Apply date filtering
        df['ISSUE_DATE'] = pd.to_datetime(df['ISSUE_DATE'], errors='coerce')
        start_dt = pd.Timestamp(start_date)
        end_dt = pd.Timestamp(end_date)
        df = df[(df['ISSUE_DATE'] >= start_dt) & (df['ISSUE_DATE'] <= end_dt)]
        
        logger.info(f"  - After filtering: {len(df)} records")
        
        # Initialize assignment columns
        df['PADDED_BADGE_NUMBER'] = df['BADGE_CLEAN'].str.zfill(4)
        df['OFFICER_DISPLAY_NAME'] = ''
        df['WG1'] = ''
        df['WG2'] = ''  # Bureau column
        df['WG3'] = ''
        df['WG4'] = ''
        df['WG5'] = ''
        df['ASSIGNMENT_FOUND'] = False
        
        # Apply assignment matching
        match_count = 0
        unmatched_badges = set()
        
        for idx, row in df.iterrows():
            badge = row['BADGE_CLEAN']
            matched = False
            
            # Try multiple badge formats
            badge_attempts = [
                badge,
                badge.lstrip('0'),
                badge.zfill(4),
                str(int(badge)) if badge.isdigit() else badge,
                str(int(badge)).zfill(4) if badge.isdigit() else badge
            ]
            badge_attempts = list(dict.fromkeys(badge_attempts))
            
            for attempt_badge in badge_attempts:
                if attempt_badge in badge_lookup:
                    assignment = badge_lookup[attempt_badge]
                    
                    # Update all assignment fields
                    df.at[idx, 'PADDED_BADGE_NUMBER'] = badge.zfill(4)
                    df.at[idx, 'OFFICER_DISPLAY_NAME'] = assignment['OFFICER_DISPLAY_NAME']
                    df.at[idx, 'WG1'] = assignment['WG1']
                    df.at[idx, 'WG2'] = assignment['WG2']  # Bureau assignment
                    df.at[idx, 'WG3'] = assignment['WG3']
                    df.at[idx, 'WG4'] = assignment['WG4']
                    df.at[idx, 'WG5'] = assignment['WG5']
                    df.at[idx, 'ASSIGNMENT_FOUND'] = True
                    
                    match_count += 1
                    matched = True
                    break
            
            if not matched:
                unmatched_badges.add(badge)
        
        match_rate = (match_count / len(df)) * 100 if len(df) > 0 else 0
        logger.info(f"  - Assignment match rate: {match_rate:.1f}% ({match_count}/{len(df)})")
        
        if unmatched_badges:
            logger.info(f"  - Unmatched badges: {sorted(list(unmatched_badges))[:5]}")
        
        # Add violation type mapping
        df['VIOLATION_TYPE'] = df['TYPE'].map({'P': 'Parking', 'M': 'Moving'}).fillna('Unknown')
        
        # Clean numeric columns
        numeric_cols = ['TOTAL_PAID_AMOUNT', 'FINE_AMOUNT', 'COST_AMOUNT', 'MISC_AMOUNT']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Add date intelligence
        df['Year'] = df['ISSUE_DATE'].dt.year
        df['Month'] = df['ISSUE_DATE'].dt.month
        df['YearMonthKey'] = df['Year'] * 100 + df['Month']
        df['Month_Year'] = df['ISSUE_DATE'].dt.strftime('%m-%y')
        
        return df
        
    except Exception as e:
        logger.error(f"Error processing file {file_path.name}: {str(e)}")
        return pd.DataFrame()

def add_data_quality_metrics(df):
    """Add data quality score and tier"""
    df['DATA_QUALITY_SCORE'] = 100.0
    
    # Deduct points for missing/invalid data
    df.loc[df['TICKET_NUMBER'].isna(), 'DATA_QUALITY_SCORE'] -= 30
    df.loc[~df['ASSIGNMENT_FOUND'], 'DATA_QUALITY_SCORE'] -= 20
    df.loc[df['VIOLATION_TYPE'] == 'Unknown', 'DATA_QUALITY_SCORE'] -= 10
    
    # Ensure score doesn't go below 0
    df['DATA_QUALITY_SCORE'] = df['DATA_QUALITY_SCORE'].clip(lower=0)
    
    # Create quality tier
    df['DATA_QUALITY_TIER'] = pd.cut(
        df['DATA_QUALITY_SCORE'],
        bins=[0, 50, 75, 90, 100],
        labels=['Poor', 'Fair', 'Good', 'Excellent'],
        include_lowest=True
    )
    
    return df

def main():
    """Main processing function with full assignment integration"""
    try:
        logger.info("Starting enhanced monthly summons processing with assignments v3.0")
        
        # Load assignment master
        badge_lookup = load_assignment_master()
        if not badge_lookup:
            logger.warning("No assignment data loaded - proceeding without bureau assignments")
        
        # Calculate date range
        start_date, end_date = calculate_date_range()
        
        # Get filtered file list
        monthly_files = filter_monthly_files(SOURCE_FOLDER)
        
        if not monthly_files:
            logger.error("No monthly files found to process")
            return False
        
        # Process each file with assignment matching
        all_data = []
        
        for file_path in monthly_files:
            file_data = extract_and_process_file(file_path, badge_lookup, start_date, end_date)
            if not file_data.empty:
                all_data.append(file_data)
        
        if not all_data:
            logger.error("No data extracted from any files")
            return False
        
        # Combine all data
        logger.info("Combining all processed data")
        combined_df = pd.concat(all_data, ignore_index=True, sort=False)
        logger.info(f"Combined dataset: {len(combined_df)} records")
        
        # Remove duplicates based on ticket number
        before_dedup = len(combined_df)
        combined_df = combined_df.drop_duplicates(subset=['TICKET_NUMBER'], keep='first')
        after_dedup = len(combined_df)
        if before_dedup > after_dedup:
            logger.info(f"Removed {before_dedup - after_dedup} duplicate tickets")
        
        # Add data quality metrics
        combined_df = add_data_quality_metrics(combined_df)
        
        # Add processing metadata
        combined_df['PROCESSING_TIMESTAMP'] = datetime.now()
        combined_df['ETL_VERSION'] = '3.0_WITH_ASSIGNMENTS'
        
        # Validate results
        logger.info("=== FINAL DATA VALIDATION ===")
        logger.info(f"Total records: {len(combined_df):,}")
        
        # Check assignment coverage
        if 'ASSIGNMENT_FOUND' in combined_df.columns:
            assigned = combined_df['ASSIGNMENT_FOUND'].sum()
            assign_rate = (assigned / len(combined_df)) * 100
            logger.info(f"Assignment match rate: {assign_rate:.1f}% ({assigned:,}/{len(combined_df):,})")
            
            # Bureau distribution
            if 'WG2' in combined_df.columns:
                bureau_dist = combined_df['WG2'].value_counts()
                logger.info("Bureau distribution:")
                for bureau, count in bureau_dist.items():
                    if bureau and bureau != '':
                        pct = (count / len(combined_df)) * 100
                        logger.info(f"  {bureau}: {count:,} ({pct:.1f}%)")
        
        # Violation type distribution
        if 'VIOLATION_TYPE' in combined_df.columns:
            violation_dist = combined_df['VIOLATION_TYPE'].value_counts()
            logger.info("Violation types:")
            for vtype, count in violation_dist.items():
                pct = (count / len(combined_df)) * 100
                logger.info(f"  {vtype}: {count:,} ({pct:.1f}%)")
        
        # Month distribution
        if 'Month_Year' in combined_df.columns:
            unique_months = combined_df['Month_Year'].nunique()
            logger.info(f"Unique months: {unique_months}")
        
        # Save output
        logger.info(f"Saving output to: {OUTPUT_FILE}")
        OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
        
        # Select and order columns for Power BI
        powerbi_columns = [
            'PADDED_BADGE_NUMBER', 'OFFICER_DISPLAY_NAME',
            'WG1', 'WG2', 'WG3', 'WG4', 'WG5',  # All assignment columns
            'TICKET_NUMBER', 'ISSUE_DATE', 'VIOLATION_NUMBER', 'VIOLATION_TYPE', 'TYPE', 'STATUS',
            'TOTAL_PAID_AMOUNT', 'FINE_AMOUNT', 'COST_AMOUNT', 'MISC_AMOUNT',
            'Year', 'Month', 'YearMonthKey', 'Month_Year',
            'ASSIGNMENT_FOUND', 'DATA_QUALITY_SCORE', 'DATA_QUALITY_TIER',
            'SOURCE_FILE', 'PROCESSING_TIMESTAMP', 'ETL_VERSION'
        ]
        
        # Only include columns that exist
        export_columns = [col for col in powerbi_columns if col in combined_df.columns]
        final_df = combined_df[export_columns]
        
        # MonthKey for current-month export (normalized to first day of report month)
        # end_date is last day of previous month, so first day of that month is our report_end_month
        report_end_month = pd.Timestamp(date(end_date.year, end_date.month, 1))
        
        # Add MonthKey if not present, or normalize existing one
        if 'MonthKey' not in final_df.columns:
            # Create MonthKey from Year and Month columns if available
            if 'Year' in final_df.columns and 'Month' in final_df.columns:
                final_df['MonthKey'] = pd.to_datetime(final_df[['Year', 'Month']].assign(Day=1))
            else:
                # Fallback: use report_end_month for all rows
                final_df['MonthKey'] = report_end_month
        
        # Normalize MonthKey to first day of month and convert to date type
        final_df['MonthKey'] = final_df['MonthKey'].apply(normalize_monthkey)
        final_df['MonthKey'] = pd.to_datetime(final_df['MonthKey']).dt.date
        
        # Standardized current-month output for PBIX backfill union
        write_current_month(
            df=final_df,
            subject='Summons',
            report_end_month=report_end_month
        )
        
        with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
            final_df.to_excel(writer, sheet_name='Summons_Data', index=False)
        
        logger.info("Processing completed successfully")
        logger.info(f"Final dataset: {len(final_df):,} records with full assignment data")
        
        # Print summary for Power BI
        print("\n" + "="*60)
        print("SUMMONS ETL COMPLETE - READY FOR POWER BI")
        print("="*60)
        print(f"Output file: {OUTPUT_FILE}")
        print(f"Total records: {len(final_df):,}")
        print(f"Assignment coverage: {assign_rate:.1f}%")
        print(f"Columns included: WG1, WG2 (Bureau), WG3, WG4, WG5")
        print("="*60)
        
        return True
        
    except Exception as e:
        logger.error(f"Fatal error in main processing: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        success = main()
        if success:
            print("\n✅ Monthly summons processing with assignments completed successfully")
            print("📊 Power BI can now filter by WG2 (Bureau) column")
            sys.exit(0)
        else:
            print("\n❌ Monthly summons processing failed")
            sys.exit(1)
    except Exception as e:
        print(f"\n❌ Fatal error: {str(e)}")
        sys.exit(1)