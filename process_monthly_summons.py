#!/usr/bin/env python3
"""
Monthly Summons Data Processing Script
Processes all monthly summons files and combines them into a rolling 13-month dataset.
Enhanced version with comprehensive date intelligence and data quality features.
"""

import pandas as pd
import numpy as np
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from pathlib import Path
import logging
import sys
import warnings
import calendar

# Suppress openpyxl warnings
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

# Configuration
SOURCE_FOLDER = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\05_EXPORTS\_Summons\Court")
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
    """
    Filter xlsx files to include:
    1. Monthly ATS files ending with '_ATS.xlsx'
    2. Specifically '24_ALL_SUMMONS.xlsx' (2024 data)
    3. Exclude other 'ALL', 'summary', 'master', 'historical' files
    """
    if not folder_path.exists():
        logger.error(f"Source folder does not exist: {folder_path}")
        raise FileNotFoundError(f"Source folder not found: {folder_path}")
    
    xlsx_files = list(folder_path.glob("*.xlsx"))
    logger.info(f"Found {len(xlsx_files)} xlsx files in source folder")
    
    filtered_files = []
    
    for file_path in xlsx_files:
        filename = file_path.name
        include_file = False
        
        # Include monthly ATS files (e.g., 25_01_ATS.xlsx, 24_12_ATS.xlsx)
        if filename.endswith('_ATS.xlsx'):
            include_file = True
            logger.info(f"Including monthly ATS file: {filename}")
        
        # Include specifically 24_ALL_SUMMONS.xlsx (2024 data)
        elif filename == '24_ALL_SUMMONS.xlsx':
            include_file = True
            logger.info(f"Including 2024 summary file: {filename}")
        
        # Exclude other files with exclusion keywords
        elif any(keyword.lower() in filename.lower() for keyword in 
                ['all', 'summary', 'master', 'historical']):
            logger.info(f"Excluding file: {filename} (contains exclusion keyword)")
        
        # Include other xlsx files that don't match exclusion patterns
        else:
            logger.info(f"Skipping unknown file pattern: {filename}")
        
        if include_file:
            filtered_files.append(file_path)
    
    logger.info(f"Filtered to {len(filtered_files)} files for processing")
    return filtered_files

def extract_data_from_file(file_path):
    """
    Extract data from xlsx file starting from row 5 (skip header rows).
    """
    try:
        logger.info(f"Processing file: {file_path.name}")
        
        # Read Excel file starting from row 4 (0-indexed, so row 5)
        df = pd.read_excel(file_path, skiprows=4)
        
        # Log basic info about the file
        logger.info(f"  - Loaded {len(df)} rows, {len(df.columns)} columns")
        logger.info(f"  - Column names: {list(df.columns)}")
        
        # Add source file column for tracking
        df['SOURCE_FILE'] = file_path.name
        
        return df
        
    except Exception as e:
        logger.error(f"Error processing file {file_path.name}: {str(e)}")
        return pd.DataFrame()

def add_date_intelligence_columns(df, date_col):
    """Add comprehensive date intelligence columns for Power BI reporting"""
    if df.empty or date_col not in df.columns:
        return df
    
    try:
        # Ensure date column is datetime
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        
        # Remove records with invalid dates
        initial_count = len(df)
        df = df[df[date_col].notna()]
        if len(df) < initial_count:
            logger.info(f"Removed {initial_count - len(df)} records with invalid dates")
        
        if df.empty:
            return df
        
        # Basic date components
        df['Year'] = df[date_col].dt.year
        df['Month'] = df[date_col].dt.month
        df['Day'] = df[date_col].dt.day
        df['Quarter'] = df[date_col].dt.quarter
        
        # YearMonthKey for sorting (integer format: 202408)
        df['YearMonthKey'] = df[date_col].dt.year * 100 + df[date_col].dt.month
        
        # Month_Year for display (string format: "08-24")
        df['Month_Year'] = df[date_col].dt.strftime('%m-%y')
        
        # Fiscal year and quarter (starts July)
        df['FiscalYear'] = df.apply(lambda row: row['Year'] if row['Month'] >= 7 else row['Year'] - 1, axis=1)
        df['FiscalQuarter'] = df.apply(lambda row: 
            1 if row['Month'] in [7, 8, 9] else
            2 if row['Month'] in [10, 11, 12] else
            3 if row['Month'] in [1, 2, 3] else 4, axis=1)
        
        # Month and day names
        df['MonthName'] = df[date_col].dt.month_name()
        df['DayOfWeekName'] = df[date_col].dt.day_name()
        df['DayOfWeek'] = df[date_col].dt.dayofweek + 1  # Monday = 1, Sunday = 7
        
        # Additional useful date intelligence
        df['WeekOfYear'] = df[date_col].dt.isocalendar().week
        df['IsWeekend'] = df['DayOfWeek'].isin([6, 7])
        
        logger.info("Added comprehensive date intelligence columns")
        
    except Exception as e:
        logger.error(f"Error adding date intelligence: {str(e)}")
    
    return df

def apply_date_filtering(df, start_date, end_date):
    """Apply strict rolling 13-month window filtering with validation"""
    if df.empty:
        return df
    
    # Look for common date column names
    date_columns = [col for col in df.columns if any(keyword in str(col).upper() for keyword in 
                   ['DATE', 'ISSUED', 'ISSUE'])]
    
    if not date_columns:
        logger.warning("No date columns found. Returning all data.")
        return df
    
    # Use the first date column found
    date_col = date_columns[0]
    logger.info(f"Using date column: {date_col}")
    
    try:
        # Convert to datetime
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        
        # Apply strict date filtering: >= start_date AND <= end_date
        initial_count = len(df)
        start_dt = pd.Timestamp(start_date)
        end_dt = pd.Timestamp(end_date)
        df = df[(df[date_col] >= start_dt) & (df[date_col] <= end_dt)]
        filtered_count = len(df)
        
        logger.info(f"Strict date filtering: {initial_count} -> {filtered_count} records")
        
        # Validate we have exactly 13 months of data
        if not df.empty:
            unique_months = df[date_col].dt.to_period('M').nunique()
            logger.info(f"Dataset contains {unique_months} unique months")
            
            if unique_months != 13:
                logger.warning(f"Expected 13 months of data, but found {unique_months} months")
            
            # Add date intelligence columns
            df = add_date_intelligence_columns(df, date_col)
        
    except Exception as e:
        logger.error(f"Error in date filtering: {str(e)}")
        logger.warning("Proceeding without date filtering")
    
    return df

def add_violation_type_mapping(df):
    """Add VIOLATION_TYPE column mapping P=Parking, M=Moving"""
    # Look for violation code or type columns
    violation_cols = [col for col in df.columns if any(keyword in str(col).upper() for keyword in 
                     ['VIOLATION', 'CODE', 'TYPE', 'OFFENSE'])]
    
    if violation_cols:
        violation_col = violation_cols[0]
        logger.info(f"Adding violation type mapping using column: {violation_col}")
        
        # Convert to string for consistent processing
        df[violation_col] = df[violation_col].astype(str)
        
        # Create VIOLATION_TYPE column
        df['VIOLATION_TYPE'] = df[violation_col].apply(lambda x: 
            'Parking' if str(x).upper().startswith('P') else
            'Moving' if str(x).upper().startswith('M') else
            'Unknown')
        
        # Log distribution
        type_counts = df['VIOLATION_TYPE'].value_counts()
        logger.info(f"Violation type distribution: {type_counts.to_dict()}")
    else:
        logger.warning("No violation code column found. Skipping violation type mapping.")
        df['VIOLATION_TYPE'] = 'Unknown'
    
    return df

def calculate_data_quality_score(df):
    """Calculate data quality score for each record"""
    if df.empty:
        return df
    
    # Initialize quality score
    df['DATA_QUALITY_SCORE'] = 100.0
    
    # Define quality checks and their weights
    quality_checks = []
    
    # Check for ticket number
    ticket_cols = [col for col in df.columns if any(keyword in str(col).upper() for keyword in 
                  ['TICKET', 'SUMMONS', 'CITATION'])]
    if ticket_cols:
        ticket_col = ticket_cols[0]
        missing_ticket = df[ticket_col].isna() | (df[ticket_col] == '') | (df[ticket_col] == 0)
        df.loc[missing_ticket, 'DATA_QUALITY_SCORE'] -= 30  # Heavy penalty for missing ticket
        quality_checks.append(('Missing Ticket', missing_ticket.sum()))
    
    # Check for badge number
    badge_cols = [col for col in df.columns if any(keyword in str(col).upper() for keyword in 
                 ['BADGE', 'OFFICER', 'ISSUER'])]
    if badge_cols:
        badge_col = badge_cols[0]
        invalid_badges = df[badge_col].astype(str).isin(['9999', '0000', 'nan', '', '0'])
        df.loc[invalid_badges, 'DATA_QUALITY_SCORE'] -= 20
        quality_checks.append(('Invalid Badge', invalid_badges.sum()))
    
    # Check for date issues
    date_cols = [col for col in df.columns if any(keyword in str(col).upper() for keyword in 
                ['DATE', 'ISSUED'])]
    if date_cols:
        date_col = date_cols[0]
        invalid_dates = df[date_col].isna()
        df.loc[invalid_dates, 'DATA_QUALITY_SCORE'] -= 25
        quality_checks.append(('Invalid Date', invalid_dates.sum()))
    
    # Check for violation type
    if 'VIOLATION_TYPE' in df.columns:
        unknown_violation = df['VIOLATION_TYPE'] == 'Unknown'
        df.loc[unknown_violation, 'DATA_QUALITY_SCORE'] -= 10
        quality_checks.append(('Unknown Violation Type', unknown_violation.sum()))
    
    # Ensure score doesn't go below 0
    df['DATA_QUALITY_SCORE'] = df['DATA_QUALITY_SCORE'].clip(lower=0)
    
    # Create quality tier
    df['DATA_QUALITY_TIER'] = pd.cut(df['DATA_QUALITY_SCORE'], 
                                   bins=[0, 50, 75, 90, 100], 
                                   labels=['Poor', 'Fair', 'Good', 'Excellent'],
                                   include_lowest=True)
    
    # Log quality summary
    avg_score = df['DATA_QUALITY_SCORE'].mean()
    logger.info(f"Average data quality score: {avg_score:.1f}")
    
    tier_counts = df['DATA_QUALITY_TIER'].value_counts()
    logger.info(f"Quality tier distribution: {tier_counts.to_dict()}")
    
    for check_name, count in quality_checks:
        if count > 0:
            logger.info(f"Quality issue - {check_name}: {count} records")
    
    return df

def clean_data(df):
    """Enhanced data cleaning with strict validation"""
    if df.empty:
        return df
    
    initial_count = len(df)
    
    # Look for ticket number columns
    ticket_cols = [col for col in df.columns if any(keyword in str(col).upper() for keyword in 
                  ['TICKET', 'SUMMONS', 'CITATION'])]
    
    # Look for badge number columns  
    badge_cols = [col for col in df.columns if any(keyword in str(col).upper() for keyword in 
                 ['BADGE', 'OFFICER', 'ISSUER'])]
    
    # STRICT: Ensure TICKET_NUMBER is never null/empty
    if ticket_cols:
        ticket_col = ticket_cols[0]
        logger.info(f"Strict ticket number validation using column: {ticket_col}")
        
        # Remove any record with missing ticket number
        before_count = len(df)
        # Convert to string first to handle various data types
        df[ticket_col] = df[ticket_col].astype(str)
        df = df[df[ticket_col].notna() & (df[ticket_col] != '') & (df[ticket_col] != 'nan') & (df[ticket_col] != '0')]
        after_count = len(df)
        
        if before_count > after_count:
            logger.info(f"Removed {before_count - after_count} records with invalid ticket numbers")
    else:
        logger.error("No ticket number column found! This is a critical data quality issue.")
    
    # Clean badge numbers
    if badge_cols:
        badge_col = badge_cols[0]
        logger.info(f"Cleaning badge numbers using column: {badge_col}")
        
        # Convert to string to handle different data types
        df[badge_col] = df[badge_col].astype(str)
        
        # Remove invalid badge numbers
        invalid_badges = ['9999', '0000', 'nan', '', '0']
        before_count = len(df)
        df = df[~df[badge_col].isin(invalid_badges)]
        after_count = len(df)
        
        if before_count > after_count:
            logger.info(f"Removed {before_count - after_count} records with invalid badge numbers")
    
    # Add violation type mapping
    df = add_violation_type_mapping(df)
    
    # Calculate data quality scores
    df = calculate_data_quality_score(df)
    
    cleaned_count = len(df)
    logger.info(f"Overall data cleaning: {initial_count} -> {cleaned_count} records (removed: {initial_count - cleaned_count})")
    
    return df

def add_processing_metadata(df):
    """Add processing metadata columns"""
    if df.empty:
        return df
    
    # Processing timestamp
    processing_time = datetime.now()
    df['PROCESSING_TIMESTAMP'] = processing_time
    df['PROCESSING_DATE'] = processing_time.date()
    
    # Data source tracking (already have SOURCE_FILE from extract_data_from_file)
    df['ETL_VERSION'] = '2.0'  # Version of this ETL process
    
    # Record sequence for tracking
    df['RECORD_SEQUENCE'] = range(1, len(df) + 1)
    
    logger.info(f"Added processing metadata with timestamp: {processing_time}")
    return df

def apply_officer_assignment_matching(df):
    """
    Apply existing officer assignment matching logic.
    This would integrate with existing assignment data if available.
    """
    # Check if Assignment_Master.xlsm exists in the current directory
    assignment_file = Path("Assignment_Master.xlsm")
    
    if assignment_file.exists():
        try:
            logger.info("Loading officer assignment data")
            # Read assignment data
            assignments_df = pd.read_excel(assignment_file)
            logger.info(f"Loaded {len(assignments_df)} assignment records")
            
            # Look for badge/officer columns to merge on
            badge_cols_summons = [col for col in df.columns if any(keyword in str(col).upper() for keyword in 
                                ['BADGE', 'OFFICER', 'ISSUER'])]
            badge_cols_assign = [col for col in assignments_df.columns if any(keyword in str(col).upper() for keyword in 
                               ['BADGE', 'OFFICER', 'ID'])]
            
            if badge_cols_summons and badge_cols_assign:
                badge_col_summons = badge_cols_summons[0]
                badge_col_assign = badge_cols_assign[0]
                
                logger.info(f"Merging on {badge_col_summons} = {badge_col_assign}")
                
                # Merge assignment data
                df = df.merge(assignments_df, 
                            left_on=badge_col_summons, 
                            right_on=badge_col_assign, 
                            how='left',
                            suffixes=('', '_ASSIGN'))
                
                logger.info(f"After assignment merge: {len(df)} records")
            
        except Exception as e:
            logger.error(f"Error applying officer assignments: {str(e)}")
            logger.warning("Proceeding without assignment matching")
    else:
        logger.info("No assignment file found. Skipping officer assignment matching.")
    
    return df

def validate_data_quality(df):
    """Enhanced data quality validation with comprehensive checks"""
    if df.empty:
        logger.error("Dataset is empty!")
        return False
    
    logger.info("=== COMPREHENSIVE DATA QUALITY REPORT ===")
    logger.info(f"Total records: {len(df):,}")
    logger.info(f"Total columns: {len(df.columns)}")
    
    # Validate 13-month requirement
    if 'YearMonthKey' in df.columns:
        unique_months = df['YearMonthKey'].nunique()
        logger.info(f"Unique months in dataset: {unique_months}")
        if unique_months == 13:
            logger.info("Exactly 13 months of data confirmed")
        else:
            logger.warning(f"Expected 13 months, found {unique_months}")
        
        # Show month distribution
        month_counts = df['Month_Year'].value_counts().sort_index()
        logger.info("Month distribution:")
        for month, count in month_counts.items():
            logger.info(f"  {month}: {count:,} records")
    
    # Data quality score summary
    if 'DATA_QUALITY_SCORE' in df.columns:
        avg_score = df['DATA_QUALITY_SCORE'].mean()
        min_score = df['DATA_QUALITY_SCORE'].min()
        max_score = df['DATA_QUALITY_SCORE'].max()
        
        logger.info(f"Data Quality Scores - Avg: {avg_score:.1f}, Min: {min_score:.1f}, Max: {max_score:.1f}")
        
        # Quality tier summary
        if 'DATA_QUALITY_TIER' in df.columns:
            tier_summary = df['DATA_QUALITY_TIER'].value_counts().to_dict()
            logger.info(f"Quality tiers: {tier_summary}")
    
    # Violation type distribution
    if 'VIOLATION_TYPE' in df.columns:
        violation_dist = df['VIOLATION_TYPE'].value_counts().to_dict()
        logger.info(f"Violation types: {violation_dist}")
    
    # Critical validations
    critical_issues = []
    
    # Check ticket numbers
    ticket_cols = [col for col in df.columns if any(keyword in str(col).upper() for keyword in 
                  ['TICKET', 'SUMMONS', 'CITATION'])]
    if ticket_cols:
        ticket_col = ticket_cols[0]
        null_tickets = df[ticket_col].isna().sum()
        if null_tickets > 0:
            critical_issues.append(f"{null_tickets} records with null ticket numbers")
    
    # Check for duplicates
    if ticket_cols:
        duplicates = df[ticket_cols[0]].duplicated().sum()
        if duplicates > 0:
            critical_issues.append(f"{duplicates} duplicate ticket numbers")
    
    # Date range validation
    date_columns = [col for col in df.columns if any(keyword in str(col).upper() for keyword in 
                   ['DATE', 'ISSUED', 'VIOLATION'])]
    
    if date_columns:
        date_col = date_columns[0]
        try:
            min_date = df[date_col].min()
            max_date = df[date_col].max()
            logger.info(f"Date range: {min_date.date()} to {max_date.date()}")
            
            # Check if dates are within expected range
            today = date.today()
            if max_date.date() > today:
                critical_issues.append(f"Future dates found (max: {max_date.date()})")
        except Exception as e:
            critical_issues.append(f"Date validation failed: {str(e)}")
    
    # Report critical issues
    if critical_issues:
        logger.error("CRITICAL DATA QUALITY ISSUES:")
        for issue in critical_issues:
            logger.error(f"  ✗ {issue}")
        return False
    else:
        logger.info("All critical data quality checks passed")
    
    logger.info("=== END DATA QUALITY REPORT ===")
    return True

def main():
    """
    Main processing function.
    """
    try:
        logger.info("Starting enhanced monthly summons processing v2.0")
        
        # Calculate date range
        start_date, end_date = calculate_date_range()
        
        # Get filtered file list
        monthly_files = filter_monthly_files(SOURCE_FOLDER)
        
        if not monthly_files:
            logger.error("No monthly files found to process")
            return False
        
        # Process each file and combine data
        all_data = []
        
        for file_path in monthly_files:
            file_data = extract_data_from_file(file_path)
            if not file_data.empty:
                # Apply date filtering
                file_data = apply_date_filtering(file_data, start_date, end_date)
                if not file_data.empty:
                    all_data.append(file_data)
        
        if not all_data:
            logger.error("No data extracted from any files")
            return False
        
        # Combine all data
        logger.info("Combining all data")
        combined_df = pd.concat(all_data, ignore_index=True, sort=False)
        logger.info(f"Combined dataset: {len(combined_df)} records")
        
        # Clean data (includes violation type mapping and quality scoring)
        combined_df = clean_data(combined_df)
        
        # Apply officer assignment matching
        combined_df = apply_officer_assignment_matching(combined_df)
        
        # Add processing metadata
        combined_df = add_processing_metadata(combined_df)
        
        # Validate data quality
        if not validate_data_quality(combined_df):
            logger.error("Data quality validation failed")
            return False
        
        # Save output
        logger.info(f"Saving output to: {OUTPUT_FILE}")
        OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
        
        with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
            combined_df.to_excel(writer, sheet_name='Summons_Data', index=False)
        
        logger.info("Processing completed successfully")
        logger.info(f"Final dataset: {len(combined_df):,} records, {len(combined_df.columns)} columns")
        
        # Final summary of key columns added
        key_columns = ['YearMonthKey', 'Month_Year', 'VIOLATION_TYPE', 'DATA_QUALITY_SCORE', 
                      'DATA_QUALITY_TIER', 'FiscalYear', 'FiscalQuarter']
        existing_key_cols = [col for col in key_columns if col in combined_df.columns]
        logger.info(f"Key intelligence columns added: {existing_key_cols}")
        
        return True
        
    except Exception as e:
        logger.error(f"Fatal error in main processing: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        success = main()
        if success:
            print("Enhanced monthly summons processing completed successfully")
            sys.exit(0)
        else:
            print("Monthly summons processing failed")
            sys.exit(1)
    except Exception as e:
        print(f"Fatal error: {str(e)}")
        sys.exit(1)