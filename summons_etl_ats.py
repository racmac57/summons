# 🕒 2025-07-09-22-30-00
# Project: SummonsMaster/summons_etl_ats.py
# Author: R. A. Carucci
# Purpose: Process ATS court data with correct paths

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

def process_ats_court_data():
    """Process the latest ATS court export data"""
    
    print("🚀 ATS COURT DATA ETL")
    print("=" * 50)
    
    # Correct file paths
    base_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack")
    
    # Use the latest ATS file
    court_file = base_path / "05_EXPORTS" / "_Summons" / "Court" / "25_06_ATS.xlsx"
    assignment_file = base_path / "_Hackensack_Data_Repository" / "ASSIGNED_SHIFT" / "Assignment_Master_V2.xlsx"
    output_dir = base_path / "03_Staging" / "Summons"
    
    print(f"📁 Court File: {court_file}")
    print(f"📁 Assignment File: {assignment_file}")
    print(f"📁 Output Directory: {output_dir}")
    
    # Check files exist
    if not court_file.exists():
        print(f"❌ Court file not found: {court_file}")
        return None
        
    if not assignment_file.exists():
        print(f"❌ Assignment file not found: {assignment_file}")
        return None
    
    print("✅ All files found")
    
    try:
        # Load assignment data
        print("📋 Loading Assignment Master V2...")
        assignment_df = pd.read_excel(assignment_file, sheet_name='Sheet1')
        print(f"✅ Assignment data loaded: {len(assignment_df)} officers")
        
        # Load court data (skip header rows)
        print("📊 Loading ATS court data...")
        court_df = pd.read_excel(court_file, skiprows=4, header=None)
        print(f"✅ Court data loaded: {len(court_df)} rows")
        
        # Assign proper column names
        court_columns = [
            'BADGE_NUMBER_RAW',     # Column 0
            'OFFICER_NAME_RAW',     # Column 1  
            'ORI',                  # Column 2
            'TICKET_NUMBER',        # Column 3
            'ISSUE_DATE',           # Column 4
            'VIOLATION_NUMBER',     # Column 5
            'TYPE',                 # Column 6
            'STATUS',               # Column 7
            'DISPOSITION_DATE',     # Column 8
            'FIND_CD',              # Column 9
            'PAYMENT_DATE',         # Column 10
            'ASSESSED_AMOUNT',      # Column 11
            'FINE_AMOUNT',          # Column 12
            'COST_AMOUNT',          # Column 13
            'MISC_AMOUNT',          # Column 14
            'TOTAL_PAID_AMOUNT',    # Column 15
            'CITY_COST_AMOUNT'      # Column 16
        ]
        
        # Apply column names
        num_cols = min(len(court_columns), len(court_df.columns))
        court_df.columns = court_columns[:num_cols] + [f'Extra_Col_{i}' for i in range(num_cols, len(court_df.columns))]
        
        # Create padded badge numbers
        court_df['PADDED_BADGE_NUMBER'] = (court_df['BADGE_NUMBER_RAW']
                                          .astype(str)
                                          .str.strip()
                                          .str.zfill(4))
        
        print(f"📋 Sample badge numbers: {court_df['PADDED_BADGE_NUMBER'].head().tolist()}")
        print(f"📋 Unique badges: {court_df['PADDED_BADGE_NUMBER'].nunique()}")
        
        # Join with assignment data
        print("🔗 Joining with assignment data...")
        merged_df = court_df.merge(
            assignment_df[['PADDED_BADGE_NUMBER', 'FULL_NAME', 'FIRST_NAME', 'LAST_NAME', 
                          'TITLE', 'TEAM', 'WG1', 'WG2', 'WG3']],
            on='PADDED_BADGE_NUMBER',
            how='left'
        )
        
        # Calculate match rate
        matched_records = merged_df['FULL_NAME'].notna().sum()
        match_rate = (matched_records / len(merged_df)) * 100
        
        print(f"✅ Join completed:")
        print(f"   Total records: {len(merged_df):,}")
        print(f"   Matched records: {matched_records:,}")
        print(f"   Match rate: {match_rate:.1f}%")
        
        # Add hierarchy and metadata
        merged_df['DIVISION'] = merged_df['WG1']
        merged_df['BUREAU'] = merged_df['WG2']
        merged_df['UNIT'] = merged_df['WG3']
        merged_df['ASSIGNMENT_FOUND'] = merged_df['FULL_NAME'].notna()
        merged_df['PROCESSED_TIMESTAMP'] = datetime.now()
        merged_df['DATA_SOURCE'] = court_file.stem
        
        # Clean data types
        merged_df['TOTAL_PAID_AMOUNT'] = pd.to_numeric(merged_df['TOTAL_PAID_AMOUNT'], errors='coerce').fillna(0)
        merged_df['ISSUE_DATE'] = pd.to_datetime(merged_df['ISSUE_DATE'], errors='coerce')
        
        # Export to Power BI format
        print("💾 Exporting to Power BI format...")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        output_file = output_dir / "summons_powerbi_latest.xlsx"
        merged_df.to_excel(output_file, index=False, sheet_name='ATS_Court_Data')
        
        # Summary
        total_revenue = merged_df['TOTAL_PAID_AMOUNT'].sum()
        unique_officers = merged_df[merged_df['ASSIGNMENT_FOUND'] == True]['FULL_NAME'].nunique()
        
        print(f"✅ SUCCESS! Output created: {output_file}")
        print(f"📊 Final Results:")
        print(f"   Total Records: {len(merged_df):,}")
        print(f"   Assignment Match Rate: {match_rate:.1f}%")
        print(f"   Officers with Data: {unique_officers}")
        print(f"   Total Revenue: ${total_revenue:,.2f}")
        
        return merged_df
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return None

if __name__ == "__main__":
    result = process_ats_court_data()
    if result is not None:
        print("\n🎯 READY FOR POWER BI!")