# 🕒 2025-07-09-22-45-00
# Project: SummonsMaster/summons_etl_ats_fixed.py
# Author: R. A. Carucci
# Purpose: Process ATS court data with data type fix for badge matching

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

def process_ats_court_data_fixed():
    """Process the latest ATS court export data with data type fixes"""
    
    print("🚀 ATS COURT DATA ETL - FIXED VERSION")
    print("=" * 60)
    
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
        
        # Ensure assignment badges are strings
        assignment_df['PADDED_BADGE_NUMBER'] = assignment_df['PADDED_BADGE_NUMBER'].astype(str).str.strip()
        print(f"📋 Assignment badge samples: {assignment_df['PADDED_BADGE_NUMBER'].head().tolist()}")
        
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
        
        # Create padded badge numbers - FIXED DATA TYPE CONVERSION
        print("🔧 Fixing badge number data types...")
        
        # Convert to string first, handle any non-numeric values
        court_df['BADGE_NUMBER_CLEAN'] = court_df['BADGE_NUMBER_RAW'].astype(str).str.strip()
        
        # Remove any non-numeric characters and pad to 4 digits
        court_df['PADDED_BADGE_NUMBER'] = (court_df['BADGE_NUMBER_CLEAN']
                                          .str.replace(r'[^\d]', '', regex=True)  # Remove non-digits
                                          .str.zfill(4))                          # Pad to 4 digits
        
        # Remove any badges that are all zeros or empty
        court_df = court_df[~court_df['PADDED_BADGE_NUMBER'].isin(['0000', ''])]
        
        print(f"📋 Court badge samples: {court_df['PADDED_BADGE_NUMBER'].head().tolist()}")
        print(f"📋 Unique court badges: {court_df['PADDED_BADGE_NUMBER'].nunique()}")
        print(f"📋 Records after cleanup: {len(court_df)}")
        
        # Double-check data types match
        print(f"🔍 Assignment badge type: {assignment_df['PADDED_BADGE_NUMBER'].dtype}")
        print(f"🔍 Court badge type: {court_df['PADDED_BADGE_NUMBER'].dtype}")
        
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
        
        # Show some successful matches
        if matched_records > 0:
            print("📋 Sample successful matches:")
            matches = merged_df[merged_df['FULL_NAME'].notna()][['PADDED_BADGE_NUMBER', 'FULL_NAME', 'WG1', 'WG2']].head()
            for _, row in matches.iterrows():
                print(f"   Badge {row['PADDED_BADGE_NUMBER']} → {row['FULL_NAME']} ({row['WG1']}, {row['WG2']})")
        
        # Add hierarchy and metadata
        merged_df['DIVISION'] = merged_df['WG1']
        merged_df['BUREAU'] = merged_df['WG2']
        merged_df['UNIT'] = merged_df['WG3']
        merged_df['ASSIGNMENT_FOUND'] = merged_df['FULL_NAME'].notna()
        merged_df['PROCESSED_TIMESTAMP'] = datetime.now()
        merged_df['DATA_SOURCE'] = court_file.stem
        merged_df['ETL_VERSION'] = 'v7.0_ATS_FIXED'
        
        # Clean data types for financial fields
        financial_columns = ['TOTAL_PAID_AMOUNT', 'FINE_AMOUNT', 'COST_AMOUNT', 'MISC_AMOUNT', 'ASSESSED_AMOUNT']
        for col in financial_columns:
            if col in merged_df.columns:
                merged_df[col] = pd.to_numeric(merged_df[col], errors='coerce').fillna(0)
        
        # Clean date fields
        merged_df['ISSUE_DATE'] = pd.to_datetime(merged_df['ISSUE_DATE'], errors='coerce')
        merged_df['MONTH_YEAR'] = merged_df['ISSUE_DATE'].dt.strftime('%Y-%m')
        
        # Violation type mapping
        merged_df['VIOLATION_TYPE'] = merged_df['TYPE'].map({
            'P': 'Parking',
            'M': 'Moving'
        }).fillna('Unknown')
        
        # Export to Power BI format
        print("💾 Exporting to Power BI format...")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Select key columns for Power BI
        powerbi_columns = [
            'PADDED_BADGE_NUMBER', 'FULL_NAME', 'FIRST_NAME', 'LAST_NAME', 'TITLE',
            'DIVISION', 'BUREAU', 'UNIT', 'TEAM',
            'TICKET_NUMBER', 'ISSUE_DATE', 'MONTH_YEAR', 'VIOLATION_NUMBER', 'VIOLATION_TYPE', 'TYPE', 'STATUS',
            'TOTAL_PAID_AMOUNT', 'FINE_AMOUNT', 'COST_AMOUNT', 'MISC_AMOUNT',
            'OFFICER_NAME_RAW', 'ASSIGNMENT_FOUND', 'PROCESSED_TIMESTAMP', 'DATA_SOURCE', 'ETL_VERSION'
        ]
        
        # Filter to only include columns that exist
        available_columns = [col for col in powerbi_columns if col in merged_df.columns]
        export_df = merged_df[available_columns].copy()
        
        # Final data type fixes for Excel export
        export_df['PADDED_BADGE_NUMBER'] = export_df['PADDED_BADGE_NUMBER'].astype(str)
        if 'ASSIGNMENT_FOUND' in export_df.columns:
            export_df['ASSIGNMENT_FOUND'] = export_df['ASSIGNMENT_FOUND'].astype(bool)
        
        output_file = output_dir / "summons_powerbi_latest.xlsx"
        export_df.to_excel(output_file, index=False, sheet_name='ATS_Court_Data')
        
        # Create backup with timestamp
        backup_file = output_dir / f"summons_powerbi_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        export_df.to_excel(backup_file, index=False, sheet_name='ATS_Court_Data')
        
        # Summary
        total_revenue = export_df['TOTAL_PAID_AMOUNT'].sum() if 'TOTAL_PAID_AMOUNT' in export_df.columns else 0
        unique_officers = export_df[export_df['ASSIGNMENT_FOUND'] == True]['FULL_NAME'].nunique() if 'ASSIGNMENT_FOUND' in export_df.columns else 0
        moving_violations = (export_df['TYPE'] == 'M').sum() if 'TYPE' in export_df.columns else 0
        parking_violations = (export_df['TYPE'] == 'P').sum() if 'TYPE' in export_df.columns else 0
        
        print(f"✅ SUCCESS! Output created: {output_file}")
        print(f"💾 Backup created: {backup_file}")
        print(f"")
        print(f"🎯 FINAL RESULTS:")
        print(f"├── Total Enforcement Records: {len(export_df):,}")
        print(f"├── Assignment Match Rate: {match_rate:.1f}%")
        print(f"├── Officers with Data: {unique_officers}")
        print(f"├── Total Revenue: ${total_revenue:,.2f}")
        print(f"├── Moving Violations: {moving_violations:,}")
        print(f"├── Parking Violations: {parking_violations:,}")
        print(f"└── Data Source: {court_file.name}")
        
        print(f"\n🚀 READY FOR POWER BI DASHBOARD!")
        print(f"📂 Connect to: {output_file}")
        
        return export_df
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    result = process_ats_court_data_fixed()
    if result is not None:
        print("\n" + "="*60)
        print("🎉 ETL COMPLETE - YOUR DASHBOARD IS READY!")
        print("="*60)
    else:
        print("\n❌ ETL failed. Check error messages above.")