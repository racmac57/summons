# 🕒 2025-07-09-19-30-00
# Project: SummonsMaster/summons_etl_final.py
# Author: R. A. Carucci
# Purpose: Production ETL pipeline for municipal court summons data with assignment integration

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'summons_etl_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)

def process_summons_master_production():
    """
    Complete SummonsMaster ETL workflow for production deployment
    Processes ATS court export and integrates with Assignment Master
    """
    
    logging.info("🚀 Starting SummonsMaster Production ETL")
    
    # File paths - update these to match your environment
    base_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack")
    court_file = base_path / "02_Staging" / "Summons" / "2025_04_ATS_Report.xlsx"
    assignment_file = base_path / "02_Staging" / "Summons" / "Assignment_Master.xlsm"
    output_dir = base_path / "03_Staging" / "Summons"
    
    try:
        # Step 1: Process Court Export Data
        logging.info("📁 Loading ATS court export data...")
        
        # Read court data starting from row 5 (skip headers)
        court_df = pd.read_excel(court_file, skiprows=4, header=None)
        
        # Assign proper column names based on ATS export structure
        column_names = [
            'BADGE_NUMBER',      # Column 0 - Officer badge (padded 4-digit)
            'OFFICER_NAME',      # Column 1 - "P.O. LAST FIRST" format
            'ORI',              # Column 2 - "0223" (Hackensack ORI)
            'TICKET_NUMBER',    # Column 3 - "E25010257"
            'ISSUE_DATE',       # Column 4 - "04/07/2025"
            'VIOLATION_NUMBER', # Column 5 - "170-7" or "39:4-138D"
            'TYPE',             # Column 6 - "P"=Parking, "M"=Moving
            'STATUS',           # Column 7 - "ACTI"=Active
            'DISPOSITION_DATE', # Column 8
            'FIND_CD',          # Column 9
            'PAYMENT_DATE',     # Column 10
            'ASSESSED_AMOUNT',  # Column 11
            'FINE_AMOUNT',      # Column 12
            'COST_AMOUNT',      # Column 13
            'MISC_AMOUNT',      # Column 14
            'TOTAL_PAID',       # Column 15 - Final revenue amount
            'CITY_COST'         # Column 16
        ]
        
        # Apply column names (only for available columns)
        court_df.columns = column_names[:len(court_df.columns)]
        
        # Data cleaning and standardization
        logging.info("🧹 Cleaning court export data...")
        
        # Clean badge numbers - ensure 4-digit padding
        court_df['PADDED_BADGE_NUMBER'] = (court_df['BADGE_NUMBER']
                                          .astype(str)
                                          .str.strip()
                                          .str.zfill(4))
        
        # Clean officer names - remove rank prefixes
        court_df['OFFICER_NAME_CLEAN'] = (court_df['OFFICER_NAME']
                                         .astype(str)
                                         .str.replace(r'^(P\.O\.|SPO|SGT|LT|CAPT)\s+', '', regex=True)
                                         .str.strip()
                                         .str.upper())
        
        # Date standardization
        court_df['ISSUE_DATE'] = pd.to_datetime(court_df['ISSUE_DATE'], errors='coerce')
        court_df['MONTH_YEAR'] = court_df['ISSUE_DATE'].dt.strftime('%Y-%m')
        
        # Revenue calculations
        court_df['TOTAL_PAID_AMOUNT'] = pd.to_numeric(court_df['TOTAL_PAID'], errors='coerce').fillna(0)
        
        # Violation type mapping
        court_df['VIOLATION_TYPE'] = court_df['TYPE'].map({
            'P': 'Parking',
            'M': 'Moving'
        }).fillna('Unknown')
        
        logging.info(f"✅ Court data loaded: {len(court_df):,} enforcement records")
        
        # Step 2: Load Assignment Master Data
        logging.info("📋 Loading Assignment Master data...")
        
        assignment_df = pd.read_excel(assignment_file, sheet_name='Assignment_Master')
        
        # Clean assignment data
        assignment_df['PADDED_BADGE_NUMBER'] = (assignment_df['BADGE_NUMBER']
                                               .astype(str)
                                               .str.strip()
                                               .str.zfill(4))
        
        # Create full name for matching
        assignment_df['FULL_NAME'] = (assignment_df['FIRST_NAME'].astype(str) + ' ' + 
                                     assignment_df['LAST_NAME'].astype(str)).str.upper()
        
        logging.info(f"✅ Assignment data loaded: {len(assignment_df)} officer records")
        
        # Step 3: Join Court and Assignment Data
        logging.info("🔗 Joining enforcement and assignment data...")
        
        # Primary join on padded badge numbers
        merged_df = court_df.merge(
            assignment_df[['PADDED_BADGE_NUMBER', 'FIRST_NAME', 'LAST_NAME', 'FULL_NAME',
                          'TITLE', 'TEAM', 'WG1', 'WG2', 'WG3']],
            on='PADDED_BADGE_NUMBER',
            how='left'
        )
        
        # Assignment hierarchy mapping
        merged_df['DIVISION'] = merged_df['WG1']     # Operations Division
        merged_df['BUREAU'] = merged_df['WG2']       # Patrol Bureau, Traffic Bureau
        merged_df['UNIT'] = merged_df['WG3']         # Platoon A, Platoon B, Class I
        merged_df['ASSIGNMENT_FOUND'] = merged_df['TEAM'].notna()
        
        # Match rate calculation
        match_rate = (merged_df['ASSIGNMENT_FOUND'].sum() / len(merged_df)) * 100
        
        logging.info(f"✅ Data joined successfully - Assignment match rate: {match_rate:.1f}%")
        
        # Step 4: Generate Officer Performance Summary
        logging.info("📊 Calculating officer performance metrics...")
        
        officer_summary = (merged_df[merged_df['ASSIGNMENT_FOUND'] == True]
                          .groupby(['PADDED_BADGE_NUMBER', 'FULL_NAME', 'DIVISION', 'BUREAU', 'UNIT'])
                          .agg({
                              'TICKET_NUMBER': 'count',
                              'TOTAL_PAID_AMOUNT': 'sum',
                              'TYPE': [
                                  lambda x: (x == 'M').sum(),  # Moving violations
                                  lambda x: (x == 'P').sum()   # Parking violations
                              ]
                          })
                          .round(2))
        
        # Flatten column names
        officer_summary.columns = ['TOTAL_TICKETS', 'TOTAL_REVENUE', 'MOVING_TICKETS', 'PARKING_TICKETS']
        officer_summary = officer_summary.reset_index()
        
        # Calculate additional metrics
        officer_summary['REVENUE_PER_TICKET'] = (officer_summary['TOTAL_REVENUE'] / 
                                                officer_summary['TOTAL_TICKETS']).round(2)
        officer_summary['MOVING_PERCENTAGE'] = ((officer_summary['MOVING_TICKETS'] / 
                                               officer_summary['TOTAL_TICKETS']) * 100).round(1)
        
        # Step 5: Prepare Power BI Dataset
        logging.info("📈 Preparing Power BI dataset...")
        
        powerbi_columns = [
            'PADDED_BADGE_NUMBER', 'OFFICER_NAME_CLEAN', 'FULL_NAME',
            'DIVISION', 'BUREAU', 'UNIT', 'TITLE',
            'TICKET_NUMBER', 'ISSUE_DATE', 'MONTH_YEAR',
            'VIOLATION_NUMBER', 'VIOLATION_TYPE', 'TOTAL_PAID_AMOUNT',
            'ASSIGNMENT_FOUND'
        ]
        
        powerbi_df = merged_df[powerbi_columns].copy()
        
        # Ensure all data types are appropriate for Excel export
        powerbi_df['PADDED_BADGE_NUMBER'] = powerbi_df['PADDED_BADGE_NUMBER'].astype(str)
        powerbi_df['TOTAL_PAID_AMOUNT'] = powerbi_df['TOTAL_PAID_AMOUNT'].astype(float)
        powerbi_df['ASSIGNMENT_FOUND'] = powerbi_df['ASSIGNMENT_FOUND'].astype(bool)
        
        # Step 6: Export Results
        logging.info("💾 Exporting processed datasets...")
        
        # Create output directory if it doesn't exist
        output_dir.mkdir(exist_ok=True)
        
        # Export main Power BI dataset
        powerbi_output = output_dir / "summons_powerbi_latest.xlsx"
        powerbi_df.to_excel(powerbi_output, index=False, sheet_name='SummonsMaster')
        
        # Export officer summary
        officer_output = output_dir / "officer_performance_summary.xlsx"
        officer_summary.to_excel(officer_output, index=False, sheet_name='OfficerSummary')
        
        # Export complete dataset for analysis
        complete_output = output_dir / "complete_enforcement_dataset.xlsx"
        merged_df.to_excel(complete_output, index=False, sheet_name='CompleteData')
        
        # Step 7: Generate Summary Report
        total_revenue = merged_df['TOTAL_PAID_AMOUNT'].sum()
        date_range_start = merged_df['ISSUE_DATE'].min().strftime('%Y-%m-%d')
        date_range_end = merged_df['ISSUE_DATE'].max().strftime('%Y-%m-%d')
        active_officers = merged_df[merged_df['ASSIGNMENT_FOUND'] == True]['PADDED_BADGE_NUMBER'].nunique()
        
        summary_report = f"""
        ✅ HACKENSACK PD SUMMONS MASTER ETL COMPLETE!
        
        📈 Processing Summary:
        ├── Total Enforcement Records: {len(merged_df):,}
        ├── Assignment Match Rate: {match_rate:.1f}%
        ├── Active Officers: {active_officers}
        ├── Total Revenue: ${total_revenue:,.2f}
        └── Date Range: {date_range_start} to {date_range_end}
        
        📁 Output Files Created:
        ├── summons_powerbi_latest.xlsx ({len(powerbi_df):,} records)
        ├── officer_performance_summary.xlsx ({len(officer_summary)} officers)
        └── complete_enforcement_dataset.xlsx (full analysis)
        
        🎯 Ready for Power BI Dashboard Deployment!