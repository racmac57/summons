# 🕒 2025-06-28-21-05-00
# Project: Police_Analytics_Dashboard/summons_utils_final
# Author: R. A. Carucci
# Purpose: Consolidated utility functions for summons ETL pipeline

import pandas as pd
import numpy as np
import re
from pathlib import Path
from datetime import datetime
import logging

def setup_logging(log_file="summons_etl.log"):
    """Configure logging for ETL processes"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def fix_data_types_for_excel(df):
    """Comprehensive data type fixing for Excel compatibility"""
    logger = logging.getLogger(__name__)
    logger.info(f"Fixing data types for {len(df)} records...")
    
    df = df.copy()
    
    # Numeric columns - common patterns
    numeric_patterns = ['COUNT', 'NUMBER', 'AMOUNT', 'SCORE', 'BADGE']
    for col in df.columns:
        if any(pattern in str(col).upper() for pattern in numeric_patterns):
            try:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(r'[^\d.-]', '', regex=True),
                    errors='coerce'
                ).fillna(0)
                logger.info(f"Fixed numeric column: {col}")
            except Exception as e:
                logger.warning(f"Could not fix numeric column {col}: {e}")
    
    # Date columns - common patterns
    date_patterns = ['DATE', 'TIME', 'TIMESTAMP']
    for col in df.columns:
        if any(pattern in str(col).upper() for pattern in date_patterns):
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                logger.info(f"Fixed date column: {col}")
            except Exception as e:
                logger.warning(f"Could not fix date column {col}: {e}")
    
    # String columns - ensure proper string format
    string_patterns = ['NAME', 'SOURCE', 'DIVISION', 'BUREAU', 'TYPE', 'CATEGORY']
    for col in df.columns:
        if any(pattern in str(col).upper() for pattern in string_patterns):
            df[col] = df[col].astype(str).replace('nan', '').replace('None', '')
    
    # Replace infinite values
    df = df.replace([np.inf, -np.inf], 0)
    
    logger.info("Data type fixes complete")
    return df

def extract_badge_number_advanced(officer_name):
    """Advanced badge number extraction with multiple patterns"""
    if pd.isna(officer_name):
        return None
    
    name_str = str(officer_name).strip()
    
    # Multiple extraction patterns in order of preference
    patterns = [
        r'(\d{4})$',                    # 4 digits at end
        r'(\d{3})$',                    # 3 digits at end  
        r'#(\d{3,4})',                  # #123 or #1234
        r'\s(\d{3,4})\s*$',            # space + digits at end
        r'(\d{4})\s*$',                # 4 digits with optional space
        r'BADGE[:\s]*(\d{3,4})',       # BADGE: 1234
        r'ID[:\s]*(\d{3,4})',          # ID: 1234
        r'(\d{3,4})\s*-\s*\w+$',       # 1234-SMITH pattern
    ]
    
    for pattern in patterns:
        match = re.search(pattern, name_str, re.IGNORECASE)
        if match:
            badge = match.group(1)
            # Pad to 4 digits
            return badge.zfill(4)
    
    return None

def clean_officer_name(officer_name):
    """Clean and standardize officer names"""
    if pd.isna(officer_name):
        return ""
    
    name = str(officer_name).strip().upper()
    
    # Remove common prefixes
    prefixes = [r'^P\.O\.\s*', r'^DET\.\s*', r'^SGT\.\s*', r'^LT\.\s*', 
               r'^CAPT\.\s*', r'^OFFICER\s*', r'^DETECTIVE\s*']
    
    for prefix in prefixes:
        name = re.sub(prefix, '', name)
    
    # Remove badge numbers from name
    name = re.sub(r'\s*#?\d{3,4}\s*$', '', name)
    name = re.sub(r'\s*-\s*\d{3,4}\s*$', '', name)
    
    return name.strip()

def validate_assignment_master(df):
    """Validate Assignment Master data structure"""
    logger = logging.getLogger(__name__)
    
    required_cols = ['BADGE_NUMBER', 'FULL_NAME']
    missing_cols = [col for col in required_cols if col not in df.columns]
    
    if missing_cols:
        logger.error(f"Assignment Master missing columns: {missing_cols}")
        return False
    
    # Check for reasonable data
    if len(df) < 50:
        logger.warning(f"Assignment Master has only {len(df)} officers - seems low")
    
    badge_nulls = df['BADGE_NUMBER'].isna().sum()
    if badge_nulls > len(df) * 0.1:  # More than 10% missing badges
        logger.warning(f"Assignment Master has {badge_nulls} missing badge numbers")
    
    logger.info(f"Assignment Master validation: {len(df)} officers loaded")
    return True

def calculate_match_quality(merged_df):
    """Calculate and report data matching quality metrics"""
    logger = logging.getLogger(__name__)
    
    total_records = len(merged_df)
    
    # Officer matching
    if 'FULL_NAME' in merged_df.columns:
        matched_officers = merged_df['FULL_NAME'].notna().sum()
        officer_match_rate = (matched_officers / total_records) * 100
    else:
        matched_officers = 0
        officer_match_rate = 0
    
    # Badge extraction success
    if 'PADDED_BADGE_NUMBER' in merged_df.columns:
        extracted_badges = merged_df['PADDED_BADGE_NUMBER'].notna().sum()
        badge_extraction_rate = (extracted_badges / total_records) * 100
    else:
        extracted_badges = 0
        badge_extraction_rate = 0
    
    # Data completeness
    completeness_scores = []
    key_fields = ['OFFICER_NAME', 'ISSUE_DATE', 'BADGE_NUMBER']
    for field in key_fields:
        if field in merged_df.columns:
            complete_pct = (merged_df[field].notna().sum() / total_records) * 100
            completeness_scores.append(complete_pct)
    
    avg_completeness = np.mean(completeness_scores) if completeness_scores else 0
    
    # Log quality metrics
    logger.info("=" * 50)
    logger.info("DATA QUALITY METRICS")
    logger.info("=" * 50)
    logger.info(f"Total Records: {total_records:,}")
    logger.info(f"Officer Match Rate: {officer_match_rate:.1f}% ({matched_officers:,} matched)")
    logger.info(f"Badge Extraction Rate: {badge_extraction_rate:.1f}% ({extracted_badges:,} extracted)")
    logger.info(f"Average Data Completeness: {avg_completeness:.1f}%")
    
    # Quality assessment
    if officer_match_rate >= 80 and avg_completeness >= 70:
        quality_status = "EXCELLENT"
    elif officer_match_rate >= 60 and avg_completeness >= 50:
        quality_status = "GOOD"
    elif officer_match_rate >= 40 and avg_completeness >= 30:
        quality_status = "FAIR"
    else:
        quality_status = "POOR"
    
    logger.info(f"Overall Quality: {quality_status}")
    logger.info("=" * 50)
    
    return {
        'total_records': total_records,
        'officer_match_rate': officer_match_rate,
        'badge_extraction_rate': badge_extraction_rate,
        'avg_completeness': avg_completeness,
        'quality_status': quality_status
    }

def create_processing_summary(df, quality_metrics, output_path):
    """Create a processing summary report"""
    logger = logging.getLogger(__name__)
    
    summary = {
        'processing_date': datetime.now().isoformat(),
        'total_records': quality_metrics['total_records'],
        'officer_match_rate': quality_metrics['officer_match_rate'],
        'badge_extraction_rate': quality_metrics['badge_extraction_rate'],
        'data_completeness': quality_metrics['avg_completeness'],
        'quality_status': quality_metrics['quality_status'],
        'unique_officers': df['FULL_NAME'].nunique() if 'FULL_NAME' in df.columns else 0,
        'date_range': {
            'min': df.select_dtypes(include=['datetime']).min().min() if not df.select_dtypes(include=['datetime']).empty else None,
            'max': df.select_dtypes(include=['datetime']).max().max() if not df.select_dtypes(include=['datetime']).empty else None
        },
        'data_sources': df['DATA_SOURCE'].value_counts().to_dict() if 'DATA_SOURCE' in df.columns else {}
    }
    
    # Save summary as JSON
    import json
    summary_file = Path(output_path) / f"processing_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2, default=str)
    
    logger.info(f"Processing summary saved: {summary_file}")
    return summary_file

def safe_excel_save(df, output_path, sheet_name='Data'):
    """Safely save DataFrame to Excel with error handling"""
    logger = logging.getLogger(__name__)
    
    try:
        # Apply data type fixes
        df_clean = fix_data_types_for_excel(df)
        
        # Save to Excel
        df_clean.to_excel(output_path, index=False, sheet_name=sheet_name)
        
        # Verify file was created
        if Path(output_path).exists():
            file_size = Path(output_path).stat().st_size
            logger.info(f"Excel file saved successfully: {output_path} ({file_size/1024:.1f} KB)")
            return True
        else:
            logger.error("Excel file was not created")
            return False
            
    except Exception as e:
        logger.error(f"Excel save failed: {e}")
        
        # Try CSV fallback
        try:
            csv_path = str(output_path).replace('.xlsx', '.csv').replace('.xls', '.csv')
            df_clean.to_csv(csv_path, index=False)
            logger.info(f"Saved as CSV fallback: {csv_path}")
            return csv_path
        except Exception as e2:
            logger.error(f"CSV fallback also failed: {e2}")
            return False

def get_file_info(file_path):
    """Get comprehensive file information"""
    path = Path(file_path)
    
    if not path.exists():
        return None
    
    stat = path.stat()
    
    return {
        'path': str(path),
        'name': path.name,
        'size_bytes': stat.st_size,
        'size_kb': stat.st_size / 1024,
        'size_mb': stat.st_size / (1024 * 1024),
        'modified': datetime.fromtimestamp(stat.st_mtime),
        'exists': True
    }

# Main utility functions for specific use cases
def load_and_validate_assignment_master(file_path, required_columns=None):
    """Load Assignment Master with validation"""
    logger = logging.getLogger(__name__)
    
    if required_columns is None:
        required_columns = [1, 2, 3, 4, 6, 7, 15]  # Default column positions
    
    try:
        df = pd.read_excel(file_path)
        logger.info(f"Assignment Master loaded: {len(df)} rows, {len(df.columns)} columns")
        
        # Extract required columns
        if len(df.columns) >= max(required_columns) + 1:
            df_clean = df.iloc[:, required_columns].copy()
            df_clean.columns = ['LAST_NAME', 'FIRST_NAME', 'BADGE_NUMBER', 
                               'PADDED_BADGE_NUMBER', 'DIVISION', 'BUREAU', 'FULL_NAME']
            
            # Clean data
            df_clean = df_clean[df_clean['LAST_NAME'] != 'LAST_NAME']
            df_clean = df_clean.dropna(subset=['BADGE_NUMBER'])
            df_clean['PADDED_BADGE_NUMBER'] = df_clean['PADDED_BADGE_NUMBER'].astype(str).str.zfill(4)
            
            if validate_assignment_master(df_clean):
                return df_clean
            else:
                return None
        else:
            logger.error(f"Assignment Master has insufficient columns: {len(df.columns)}")
            return None
            
    except Exception as e:
        logger.error(f"Error loading Assignment Master: {e}")
        return None

def find_best_sheet(excel_file, target_sheets=None):
    """Find the best sheet to use from Excel file"""
    logger = logging.getLogger(__name__)
    
    if target_sheets is None:
        target_sheets = ['ENHANCED_BADGE', 'Enhanced_Badge', 'ENHANCED BADGE', 'SUMMONS_MASTER']
    
    try:
        excel_obj = pd.ExcelFile(excel_file)
        available_sheets = excel_obj.sheet_names
        logger.info(f"Available sheets: {available_sheets}")
        
        # Try preferred sheets first
        for sheet in target_sheets:
            if sheet in available_sheets:
                try:
                    test_df = pd.read_excel(excel_file, sheet_name=sheet)
                    if len(test_df) > 10:  # Must have reasonable amount of data
                        logger.info(f"Selected sheet: {sheet} ({len(test_df)} rows)")
                        return sheet, test_df
                except Exception as e:
                    logger.warning(f"Could not load sheet {sheet}: {e}")
                    continue
        
        # Fallback to any sheet with substantial data
        for sheet in available_sheets:
            try:
                test_df = pd.read_excel(excel_file, sheet_name=sheet)
                if len(test_df) > 100:  # At least 100 rows
                    logger.info(f"Using fallback sheet: {sheet} ({len(test_df)} rows)")
                    return sheet, test_df
            except Exception as e:
                continue
        
        logger.error("No suitable sheet found with substantial data")
        return None, None
        
    except Exception as e:
        logger.error(f"Error reading Excel file: {e}")
        return None, None
