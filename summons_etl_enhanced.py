#!/usr/bin/env python3
# 🕒 2025-06-28-23-00-00
# Project: Police_Analytics_Dashboard/summons_etl_enhanced_FINAL
# Author: R. A. Carucci
# Purpose: FINAL ETL pipeline for July 1st deployment with data type fixes

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime, timedelta
import re
from fuzzywuzzy import fuzz, process

def fix_data_types_quick(df):
    """Quick data type fix for save issues"""
    print(f"🔧 Fixing data types for {len(df)} records...")
    df = df.copy()

    numeric_cols = ['FINE_AMOUNT', 'AMOUNT', 'DATA_QUALITY_SCORE', 'BADGE_NUMBER', 'PADDED_BADGE_NUMBER']
    for col in numeric_cols:
        if col in df.columns:
            try:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(r'[^\d.-]', '', regex=True),
                    errors='coerce'
                ).fillna(0)
                print(f"  ✅ Fixed numeric: {col}")
            except Exception as e:
                print(f"  ⚠️ Could not fix {col}: {e}")

    date_cols = ['VIOLATION_DATE', 'ISSUE_DATE', 'PROCESSED_TIMESTAMP']
    for col in date_cols:
        if col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                print(f"  ✅ Fixed date: {col}")
            except Exception as e:
                print(f"  ⚠️ Could not fix {col}: {e}")

    string_cols = ['OFFICER_NAME_RAW', 'DATA_SOURCE', 'DIVISION', 'BUREAU', 'FULL_NAME']
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).replace('nan', '')

    df = df.replace([np.inf, -np.inf], 0)
    print(f"✅ Data type fixes complete")
    return df

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('summons_etl.log'),
        logging.StreamHandler()
    ]
)

class SummonsETLProcessor:
    def __init__(self, config_path="config.json"):
        """Initialize ETL processor with configuration"""
        self.base_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack")
        self.source_data_path = self.base_path / "01_SourceData" / "Summons"
        self.staging_path = self.base_path / "03_Staging" / "Summons"
        self.assignment_master_path = self.base_path / "01_SourceData" / "Assignment_Master.xlsx"
        
        # Ensure staging directory exists
        self.staging_path.mkdir(parents=True, exist_ok=True)
        
        # Load assignment master for officer matching
        self.assignment_master = self._load_assignment_master()
        
    def _load_assignment_master(self):
        """Load and prepare assignment master data"""
        try:
            df = pd.read_excel(self.assignment_master_path, sheet_name='Assignment_Master')
            
            # Based on file analysis: columns 2,3,4,5,7,8,16
            df_clean = df.iloc[:, [2, 3, 4, 5, 7, 8, 16]].copy()
            df_clean.columns = ['LAST_NAME', 'FIRST_NAME', 'BADGE_NUMBER', 'PADDED_BADGE_NUMBER', 
                               'DIVISION', 'BUREAU', 'FULL_NAME']
            
            # Remove header row if it exists
            df_clean = df_clean[df_clean['LAST_NAME'] != 'LAST_NAME'].copy()
            
            # Ensure padded badge numbers are strings
            df_clean['PADDED_BADGE_NUMBER'] = df_clean['PADDED_BADGE_NUMBER'].astype(str).str.zfill(4)
            
            logging.info(f"Loaded {len(df_clean)} officer records from Assignment Master")
            return df_clean
            
        except Exception as e:
            logging.error(f"Error loading Assignment Master: {e}")
            return pd.DataFrame()
    
    def process_summons_master_file(self, file_path, sheet_name='ENHANCED_BADGE'):
        """Process the SUMMONS_MASTER.xlsx file with ENHANCED_BADGE sheet"""
        try:
            # Load the specific sheet
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            
            logging.info(f"Loaded {len(df)} records from {sheet_name} sheet")
            logging.info(f"Columns found: {list(df.columns)}")
            
            # The current structure appears to have date values in first columns
            # Need to identify actual data vs header issues
            
            # Skip rows with malformed headers and find actual data
            actual_data_start = self._find_data_start_row(df)
            
            if actual_data_start > 0:
                # Re-read with proper header row
                df = pd.read_excel(file_path, sheet_name=sheet_name, 
                                 header=actual_data_start, skiprows=range(actual_data_start))
                
            # Standardize column names
            df = self._standardize_summons_columns(df)
            
            # Extract and pad badge numbers
            df = self._extract_badge_numbers(df)
            
            # Join with assignment master
            df_enriched = self._enrich_with_officer_data(df)
            
            # Add violation categorization
            df_final = self._categorize_violations(df_enriched)
            
            # Add data quality metrics
            df_final = self._add_data_quality_metrics(df_final)
            
            return df_final
            
        except Exception as e:
            logging.error(f"Error processing summons file: {e}")
            return pd.DataFrame()
    
    def _find_data_start_row(self, df):
        """Find the actual start of data by looking for proper column structure"""
        for i in range(min(10, len(df))):
            row = df.iloc[i]
            # Look for typical summons data patterns
            if any(isinstance(val, str) and any(pattern in str(val).upper() for pattern in 
                  ['BADGE', 'OFFICER', 'VIOLATION', 'STATUTE', 'DATE']) for val in row):
                return i
        return 0
    
    def _standardize_summons_columns(self, df):
        """Standardize column names for consistent processing"""
        # Common column mappings based on municipal court exports
        column_mapping = {
            'Badge': 'BADGE_RAW',
            'Badge Number': 'BADGE_RAW', 
            'Officer': 'OFFICER_NAME_RAW',
            'Officer Name': 'OFFICER_NAME_RAW',
            'Violation': 'VIOLATION_CODE',
            'Statute': 'STATUTE',
            'Date': 'VIOLATION_DATE',
            'Issue Date': 'VIOLATION_DATE',
            'Amount': 'FINE_AMOUNT',
            'Fine': 'FINE_AMOUNT',
            'Location': 'VIOLATION_LOCATION'
        }
        
        # Apply mapping where columns exist
        for old_col, new_col in column_mapping.items():
            if old_col in df.columns:
                df = df.rename(columns={old_col: new_col})
        
        return df
    
    def _extract_badge_numbers(self, df):
        """Extract and standardize badge numbers from various formats"""
        badge_columns = [col for col in df.columns if 'BADGE' in col.upper()]
        
        if not badge_columns:
            # Try to extract from officer name field
            if 'OFFICER_NAME_RAW' in df.columns:
                df['BADGE_EXTRACTED'] = df['OFFICER_NAME_RAW'].apply(self._extract_badge_from_name)
            else:
                df['BADGE_EXTRACTED'] = None
        else:
            df['BADGE_EXTRACTED'] = df[badge_columns[0]]
        
        # Pad badge numbers to 4 digits
        df['PADDED_BADGE_NUMBER'] = df['BADGE_EXTRACTED'].apply(
            lambda x: str(x).zfill(4) if pd.notna(x) and str(x).isdigit() else None
        )
        
        return df
    
    def _extract_badge_from_name(self, officer_name):
        """Extract badge number from officer name string"""
        if pd.isna(officer_name):
            return None
            
        # Look for patterns like "P.O. A SMITH #123" or "DET. JONES 456"
        patterns = [
            r'#(\d{3,4})',  # #123 or #1234
            r'\s(\d{3,4})$',  # ending with 123 or 1234
            r'(\d{4})',  # any 4-digit number
        ]
        
        for pattern in patterns:
            match = re.search(pattern, str(officer_name))
            if match:
                return match.group(1)
        
        return None
    
    def _enrich_with_officer_data(self, df):
        """Enrich summons data with officer assignment information"""
        if self.assignment_master.empty:
            logging.warning("Assignment Master not available for enrichment")
            return df
        
        # Direct badge number matching
        merged = df.merge(
            self.assignment_master,
            on='PADDED_BADGE_NUMBER',
            how='left',
            suffixes=('', '_ASSIGNMENT')
        )
        
        # For unmatched records, try fuzzy name matching
        unmatched = merged[merged['FULL_NAME'].isna()].copy()
        matched = merged[merged['FULL_NAME'].notna()].copy()
        
        if not unmatched.empty and 'OFFICER_NAME_RAW' in unmatched.columns:
            unmatched_fuzzy = self._fuzzy_match_officers(unmatched)
            final_df = pd.concat([matched, unmatched_fuzzy], ignore_index=True)
        else:
            final_df = merged
        
        # Calculate match quality
        final_df['OFFICER_MATCH_QUALITY'] = final_df.apply(
            lambda row: 'DIRECT_BADGE' if pd.notna(row['FULL_NAME']) and pd.notna(row['PADDED_BADGE_NUMBER'])
            else 'FUZZY_NAME' if pd.notna(row['FULL_NAME'])
            else 'NO_MATCH', axis=1
        )
        
        return final_df
    
    def _fuzzy_match_officers(self, unmatched_df):
        """Perform fuzzy matching for officers without direct badge matches"""
        officer_names = self.assignment_master['FULL_NAME'].tolist()
        
        def find_best_match(officer_name):
            if pd.isna(officer_name):
                return None, 0
            
            # Clean the name for better matching
            clean_name = re.sub(r'^(P\.O\.|DET\.|SGT\.|LT\.|CAPT\.)\s*', '', str(officer_name).upper())
            
            match = process.extractOne(clean_name, officer_names, scorer=fuzz.ratio)
            if match and match[1] >= 75:  # 75% confidence threshold
                return match[0], match[1]
            return None, 0
        
        unmatched_df[['BEST_MATCH', 'MATCH_SCORE']] = unmatched_df['OFFICER_NAME_RAW'].apply(
            lambda x: pd.Series(find_best_match(x))
        )
        
        # Merge with assignment data for good matches
        good_matches = unmatched_df[unmatched_df['MATCH_SCORE'] >= 75].copy()
        good_matches = good_matches.merge(
            self.assignment_master,
            left_on='BEST_MATCH',
            right_on='FULL_NAME',
            how='left',
            suffixes=('', '_ASSIGNMENT')
        )
        
        # Keep poor matches as-is
        poor_matches = unmatched_df[unmatched_df['MATCH_SCORE'] < 75].copy()
        
        return pd.concat([good_matches, poor_matches], ignore_index=True)
    
    def _categorize_violations(self, df):
        """Add violation type categorization"""
        # Title 39 (Traffic) vs Municipal Ordinances
        def categorize_violation(statute):
            if pd.isna(statute):
                return 'UNKNOWN'
            
            statute_str = str(statute).upper()
            if '39:' in statute_str or 'TITLE 39' in statute_str:
                return 'TRAFFIC_TITLE39'
            elif any(ordinance in statute_str for ordinance in ['ORD', 'ORDINANCE', 'MUNICIPAL']):
                return 'MUNICIPAL_ORDINANCE'
            else:
                return 'OTHER'
        
        df['VIOLATION_CATEGORY'] = df.get('STATUTE', pd.Series()).apply(categorize_violation)
        
        return df
    
    def _add_data_quality_metrics(self, df):
        """Add data quality assessment columns"""
        df['DATA_QUALITY_SCORE'] = 0
        df['DATA_QUALITY_ISSUES'] = ''
        
        # Score components
        if 'PADDED_BADGE_NUMBER' in df.columns:
            df.loc[df['PADDED_BADGE_NUMBER'].notna(), 'DATA_QUALITY_SCORE'] += 25
            df.loc[df['PADDED_BADGE_NUMBER'].isna(), 'DATA_QUALITY_ISSUES'] += 'MISSING_BADGE; '
        
        if 'OFFICER_MATCH_QUALITY' in df.columns:
            df.loc[df['OFFICER_MATCH_QUALITY'] == 'DIRECT_BADGE', 'DATA_QUALITY_SCORE'] += 25
            df.loc[df['OFFICER_MATCH_QUALITY'] == 'FUZZY_NAME', 'DATA_QUALITY_SCORE'] += 15
            df.loc[df['OFFICER_MATCH_QUALITY'] == 'NO_MATCH', 'DATA_QUALITY_ISSUES'] += 'NO_OFFICER_MATCH; '
        
        if 'VIOLATION_DATE' in df.columns:
            df.loc[df['VIOLATION_DATE'].notna(), 'DATA_QUALITY_SCORE'] += 25
            df.loc[df['VIOLATION_DATE'].isna(), 'DATA_QUALITY_ISSUES'] += 'MISSING_DATE; '
        
        if 'STATUTE' in df.columns:
            df.loc[df['STATUTE'].notna(), 'DATA_QUALITY_SCORE'] += 25
            df.loc[df['STATUTE'].isna(), 'DATA_QUALITY_ISSUES'] += 'MISSING_STATUTE; '
        
        # Clean up quality issues
        df['DATA_QUALITY_ISSUES'] = df['DATA_QUALITY_ISSUES'].str.rstrip('; ')
        
        return df
    
    def process_ats_report(self, file_path):
        """Process ATS (Automated Traffic System) report"""
        try:
            df = pd.read_excel(file_path, sheet_name=0)  # First sheet
            
            logging.info(f"Loaded {len(df)} ATS records")
            
            # Based on analysis: Badge, Officer Name, Citation Number, Date, Violation
            if len(df.columns) >= 6:
                df.columns = ['BADGE_RAW', 'OFFICER_NAME_RAW', 'CITATION_REF', 'CITATION_NUMBER', 
                             'VIOLATION_DATE', 'STATUTE'] + list(df.columns[6:])
            
            # Extract badge numbers (already clean 4-digit format)
            df['PADDED_BADGE_NUMBER'] = df['BADGE_RAW'].astype(str).str.zfill(4)
            
            # Clean officer names
            df['OFFICER_NAME_CLEAN'] = df['OFFICER_NAME_RAW'].str.replace(r'^P\.O\.\s*', '', regex=True)
            
            # Add source identifier
            df['DATA_SOURCE'] = 'ATS_AUTOMATED'
            df['VIOLATION_CATEGORY'] = 'TRAFFIC_AUTOMATED'
            
            # Enrich with assignment data
            df_enriched = self._enrich_with_officer_data(df)
            
            return df_enriched
            
        except Exception as e:
            logging.error(f"Error processing ATS report: {e}")
            return pd.DataFrame()
    
    def create_unified_summons_dataset(self, summons_files, ats_files=None):
        """Create unified dataset from multiple summons sources"""
        all_datasets = []
        
        # Process manual summons files
        for file_path in summons_files:
            df = self.process_summons_master_file(file_path)
            if not df.empty:
                df['DATA_SOURCE'] = f'MANUAL_SUMMONS_{Path(file_path).stem}'
                all_datasets.append(df)
        
        # Process ATS files
        if ats_files:
            for file_path in ats_files:
                df = self.process_ats_report(file_path)
                if not df.empty:
                    all_datasets.append(df)
        
        if not all_datasets:
            logging.warning("No valid datasets found")
            return pd.DataFrame()
        
        # Combine all datasets
        unified_df = pd.concat(all_datasets, ignore_index=True, sort=False)
        
        # Add processing metadata
        unified_df['PROCESSED_TIMESTAMP'] = datetime.now()
        unified_df['ETL_VERSION'] = '2.0'
        
        # Create summary statistics
        self._generate_processing_summary(unified_df)
        
        return unified_df
    
    def _generate_processing_summary(self, df):
        """Generate and log processing summary statistics"""
        summary = {
            'total_records': len(df),
            'date_range': f"{df['VIOLATION_DATE'].min()} to {df['VIOLATION_DATE'].max()}" if 'VIOLATION_DATE' in df.columns else 'Unknown',
            'unique_officers': df['PADDED_BADGE_NUMBER'].nunique() if 'PADDED_BADGE_NUMBER' in df.columns else 0,
            'match_quality_distribution': df['OFFICER_MATCH_QUALITY'].value_counts().to_dict() if 'OFFICER_MATCH_QUALITY' in df.columns else {},
            'violation_categories': df['VIOLATION_CATEGORY'].value_counts().to_dict() if 'VIOLATION_CATEGORY' in df.columns else {},
            'data_quality_avg': df['DATA_QUALITY_SCORE'].mean() if 'DATA_QUALITY_SCORE' in df.columns else 0
        }
        
        logging.info("=== PROCESSING SUMMARY ===")
        for key, value in summary.items():
            logging.info(f"{key}: {value}")
        
        # Save summary to staging area
        summary_path = self.staging_path / f"processing_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        import json
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
    
    def save_to_staging(self, df, filename_prefix="summons_unified"):
        """Save processed data to staging area for Power BI consumption"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Main dataset
        main_file = self.staging_path / f"{filename_prefix}_{timestamp}.xlsx"
        df.to_excel(main_file, index=False, sheet_name='Summons_Data')
        
        # Create separate fact and dimension tables for Power BI
        self._create_star_schema_tables(df, timestamp)
        
        logging.info(f"Data saved to staging: {main_file}")
        
        return main_file
    
    def _create_star_schema_tables(self, df, timestamp):
        """Create fact and dimension tables for optimal Power BI performance"""
        
        # Fact table - core summons transactions
        fact_columns = ['PADDED_BADGE_NUMBER', 'VIOLATION_DATE', 'STATUTE', 'VIOLATION_CATEGORY',
                       'FINE_AMOUNT', 'DATA_SOURCE', 'DATA_QUALITY_SCORE', 'PROCESSED_TIMESTAMP']
        
        fact_table = df[[col for col in fact_columns if col in df.columns]].copy()
        fact_table['SUMMONS_ID'] = range(1, len(fact_table) + 1)
        
        # Officer dimension
        officer_dim = df[['PADDED_BADGE_NUMBER', 'FULL_NAME', 'DIVISION', 'BUREAU', 
                         'OFFICER_MATCH_QUALITY']].drop_duplicates().reset_index(drop=True)
        officer_dim = officer_dim[officer_dim['PADDED_BADGE_NUMBER'].notna()]
        
        # Date dimension (for dashboard date filtering)
        if 'VIOLATION_DATE' in df.columns:
            date_dim = pd.DataFrame({
                'DATE_KEY': pd.date_range(
                    start=df['VIOLATION_DATE'].min(),
                    end=df['VIOLATION_DATE'].max(),
                    freq='D'
                )
            })
            date_dim['YEAR'] = date_dim['DATE_KEY'].dt.year
            date_dim['MONTH'] = date_dim['DATE_KEY'].dt.month
            date_dim['QUARTER'] = date_dim['DATE_KEY'].dt.quarter
            date_dim['DAY_OF_WEEK'] = date_dim['DATE_KEY'].dt.day_name()
        else:
            date_dim = pd.DataFrame()
        
        # Save star schema tables
        star_schema_file = self.staging_path / f"summons_star_schema_{timestamp}.xlsx"
        
        with pd.ExcelWriter(star_schema_file, engine='openpyxl') as writer:
            fact_table.to_excel(writer, sheet_name='Fact_Summons', index=False)
            officer_dim.to_excel(writer, sheet_name='Dim_Officers', index=False)
            if not date_dim.empty:
                date_dim.to_excel(writer, sheet_name='Dim_Dates', index=False)
        
        logging.info(f"Star schema tables saved: {star_schema_file}")

def main():
    """Main execution function"""
    logging.info("Starting Summons ETL Process v2.0")
    
    # Initialize processor
    processor = SummonsETLProcessor()
    
    # Example usage with your uploaded files
    summons_file = r"C:\Users\carucci_r\OneDrive - City of Hackensack\01_SourceData\Summons\SUMMONS_MASTER.xlsx"
    ats_file = r"C:\Users\carucci_r\OneDrive - City of Hackensack\01_SourceData\ATS\2025_04_ATS_Report.xlsx"
    
    # Process files
    unified_data = processor.create_unified_summons_dataset(
        summons_files=[summons_file],
        ats_files=[ats_file]
    )
    
    if not unified_data.empty:
        # Save to staging for Power BI
        output_file = processor.save_to_staging(unified_data)
        logging.info(f"ETL process completed successfully. Output: {output_file}")
    else:
        logging.error("ETL process failed - no data processed")

if __name__ == "__main__":
    main()