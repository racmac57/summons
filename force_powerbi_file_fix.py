# 🕒 2025-07-09-23-30-00
# Project: SummonsMaster/force_powerbi_file_fix.py
# Author: R. A. Carucci
# Purpose: Force create the correct Power BI file with assignment data

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

def force_create_powerbi_file():
    """Force create Power BI file with assignment data"""
    
    print("🔧 FORCE CREATING CORRECT POWER BI FILE")
    print("=" * 60)
    
    # File paths
    base_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack")
    court_file = base_path / "05_EXPORTS" / "_Summons" / "Court" / "25_06_ATS.xlsx"
    assignment_file = base_path / "_Hackensack_Data_Repository" / "ASSIGNED_SHIFT" / "Assignment_Master_V2.xlsx"
    output_dir = base_path / "03_Staging" / "Summons"
    
    try:
        # Load assignment data first
        print("📋 Loading Assignment Master V2...")
        assignment_df = pd.read_excel(assignment_file, sheet_name='Sheet1')
        
        # Create clean assignment lookup
        assignment_lookup = {}
        for _, row in assignment_df.iterrows():
            badge_4digit = str(row['PADDED_BADGE_NUMBER']).strip()
            assignment_lookup[badge_4digit] = {
                'FULL_NAME': str(row['FULL_NAME']).strip(),
                'FIRST_NAME': str(row['FIRST_NAME']).strip(),
                'LAST_NAME': str(row['LAST_NAME']).strip(),
                'TITLE': str(row['TITLE']).strip(),
                'TEAM': str(row['TEAM']).strip(),
                'DIVISION': str(row['WG1']).strip(),
                'BUREAU': str(row['WG2']).strip(),
                'UNIT': str(row['WG3']).strip()
            }
        
        print(f"✅ Assignment lookup created: {len(assignment_lookup)} officers")
        print(f"📋 Sample assignments: {list(assignment_lookup.keys())[:5]}")
        
        # Load court data
        print("📊 Loading court data...")
        court_df = pd.read_excel(court_file, skiprows=4, header=None)
        
        # Assign column names
        court_columns = [
            'BADGE_NUMBER_RAW', 'OFFICER_NAME_RAW', 'ORI', 'TICKET_NUMBER', 'ISSUE_DATE', 
            'VIOLATION_NUMBER', 'TYPE', 'STATUS', 'DISPOSITION_DATE', 'FIND_CD', 'PAYMENT_DATE',
            'ASSESSED_AMOUNT', 'FINE_AMOUNT', 'COST_AMOUNT', 'MISC_AMOUNT', 'TOTAL_PAID_AMOUNT', 'CITY_COST_AMOUNT'
        ]
        
        num_cols = min(len(court_columns), len(court_df.columns))
        court_df.columns = court_columns[:num_cols] + [f'Extra_Col_{i}' for i in range(num_cols, len(court_df.columns))]
        
        # Clean and pad badge numbers
        court_df['BADGE_CLEAN'] = court_df['BADGE_NUMBER_RAW'].astype(str).str.strip()
        court_df = court_df[court_df['BADGE_CLEAN'] != '0']  # Remove invalid badges
        court_df['PADDED_BADGE_NUMBER'] = court_df['BADGE_CLEAN'].str.zfill(4)
        
        print(f"✅ Court data processed: {len(court_df)} records")
        print(f"📋 Court badge samples: {court_df['PADDED_BADGE_NUMBER'].head().tolist()}")
        
        # Manual assignment matching
        print("🔗 Manually matching assignments...")
        
        # Initialize assignment columns
        court_df['FULL_NAME'] = ''
        court_df['FIRST_NAME'] = ''
        court_df['LAST_NAME'] = ''
        court_df['TITLE'] = ''
        court_df['TEAM'] = ''
        court_df['DIVISION'] = ''
        court_df['BUREAU'] = ''
        court_df['UNIT'] = ''
        court_df['ASSIGNMENT_FOUND'] = False
        
        # Apply assignments manually
        match_count = 0
        for idx, row in court_df.iterrows():
            badge = row['PADDED_BADGE_NUMBER']
            if badge in assignment_lookup:
                assignment = assignment_lookup[badge]
                court_df.at[idx, 'FULL_NAME'] = assignment['FULL_NAME']
                court_df.at[idx, 'FIRST_NAME'] = assignment['FIRST_NAME']
                court_df.at[idx, 'LAST_NAME'] = assignment['LAST_NAME']
                court_df.at[idx, 'TITLE'] = assignment['TITLE']
                court_df.at[idx, 'TEAM'] = assignment['TEAM']
                court_df.at[idx, 'DIVISION'] = assignment['DIVISION']
                court_df.at[idx, 'BUREAU'] = assignment['BUREAU']
                court_df.at[idx, 'UNIT'] = assignment['UNIT']
                court_df.at[idx, 'ASSIGNMENT_FOUND'] = True
                match_count += 1
        
        match_rate = (match_count / len(court_df)) * 100
        print(f"✅ Manual matching complete: {match_count} matches ({match_rate:.1f}%)")
        
        # Show successful matches
        matched_officers = court_df[court_df['ASSIGNMENT_FOUND'] == True][['PADDED_BADGE_NUMBER', 'FULL_NAME', 'DIVISION', 'BUREAU']].drop_duplicates()
        print(f"📋 Successfully matched officers:")
        for _, officer in matched_officers.iterrows():
            print(f"   Badge {officer['PADDED_BADGE_NUMBER']} → {officer['FULL_NAME']} ({officer['DIVISION']}, {officer['BUREAU']})")
        
        # Clean data types
        court_df['TOTAL_PAID_AMOUNT'] = pd.to_numeric(court_df['TOTAL_PAID_AMOUNT'], errors='coerce').fillna(0)
        court_df['FINE_AMOUNT'] = pd.to_numeric(court_df['FINE_AMOUNT'], errors='coerce').fillna(0)
        court_df['COST_AMOUNT'] = pd.to_numeric(court_df['COST_AMOUNT'], errors='coerce').fillna(0)
        court_df['MISC_AMOUNT'] = pd.to_numeric(court_df['MISC_AMOUNT'], errors='coerce').fillna(0)
        court_df['ISSUE_DATE'] = pd.to_datetime(court_df['ISSUE_DATE'], errors='coerce')
        court_df['VIOLATION_TYPE'] = court_df['TYPE'].map({'P': 'Parking', 'M': 'Moving'}).fillna('Unknown')
        
        # Add metadata
        court_df['PROCESSED_TIMESTAMP'] = datetime.now()
        court_df['DATA_SOURCE'] = '25_06_ATS.xlsx'
        court_df['ETL_VERSION'] = 'v9.0_MANUAL_MATCH'
        
        # Select columns for Power BI
        powerbi_columns = [
            'PADDED_BADGE_NUMBER', 'FULL_NAME', 'FIRST_NAME', 'LAST_NAME', 'TITLE',
            'DIVISION', 'BUREAU', 'UNIT', 'TEAM',
            'TICKET_NUMBER', 'ISSUE_DATE', 'VIOLATION_NUMBER', 'VIOLATION_TYPE', 'TYPE', 'STATUS',
            'TOTAL_PAID_AMOUNT', 'FINE_AMOUNT', 'COST_AMOUNT', 'MISC_AMOUNT',
            'OFFICER_NAME_RAW', 'ASSIGNMENT_FOUND', 'PROCESSED_TIMESTAMP', 'DATA_SOURCE', 'ETL_VERSION'
        ]
        
        final_df = court_df[powerbi_columns].copy()
        
        # Ensure proper data types for Power BI
        final_df['PADDED_BADGE_NUMBER'] = final_df['PADDED_BADGE_NUMBER'].astype(str)
        final_df['ASSIGNMENT_FOUND'] = final_df['ASSIGNMENT_FOUND'].astype(bool)
        
        # Force replace any remaining blanks with proper values
        text_columns = ['FULL_NAME', 'FIRST_NAME', 'LAST_NAME', 'TITLE', 'DIVISION', 'BUREAU', 'UNIT', 'TEAM']
        for col in text_columns:
            final_df[col] = final_df[col].fillna('').astype(str)
            final_df[col] = final_df[col].replace(['nan', 'None', ''], None)
        
        # Export with the EXACT sheet name Power BI expects
        print("💾 Creating Power BI file...")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        output_file = output_dir / "summons_powerbi_latest.xlsx"
        
        # Delete existing file to ensure clean write
        if output_file.exists():
            output_file.unlink()
        
        # Write with exact sheet name
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            final_df.to_excel(writer, sheet_name='ATS_Court_Data', index=False)
        
        # Verify file was created correctly
        print("🔍 Verifying created file...")
        verification_df = pd.read_excel(output_file, sheet_name='ATS_Court_Data')
        verified_matches = verification_df['ASSIGNMENT_FOUND'].sum()
        verified_names = verification_df['FULL_NAME'].notna().sum()
        
        print(f"✅ File verification:")
        print(f"   Total records: {len(verification_df)}")
        print(f"   Assignment matches: {verified_matches}")
        print(f"   Names populated: {verified_names}")
        
        if verified_names > 0:
            sample_names = verification_df[verification_df['FULL_NAME'].notna()]['FULL_NAME'].unique()[:5]
            print(f"   Sample names: {sample_names.tolist()}")
        
        # Calculate final statistics
        total_revenue = final_df['TOTAL_PAID_AMOUNT'].sum()
        unique_officers = final_df[final_df['ASSIGNMENT_FOUND'] == True]['FULL_NAME'].nunique()
        moving_count = (final_df['TYPE'] == 'M').sum()
        parking_count = (final_df['TYPE'] == 'P').sum()
        
        print(f"\n🎯 FINAL POWER BI FILE RESULTS:")
        print(f"├── Total Records: {len(final_df):,}")
        print(f"├── Assignment Match Rate: {match_rate:.1f}%")
        print(f"├── Officers with Data: {unique_officers}")
        print(f"├── Total Revenue: ${total_revenue:,.2f}")
        print(f"├── Moving Violations: {moving_count:,}")
        print(f"├── Parking Violations: {parking_count:,}")
        print(f"└── File: {output_file}")
        
        return final_df
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    result = force_create_powerbi_file()
    if result is not None:
        print(f"\n🚀 POWER BI FILE FORCE CREATED!")
        print(f"🔄 Now refresh Power BI - assignment data should populate!")
        print(f"📊 Sheet name: 'ATS_Court_Data'")
        print(f"🎯 Your dashboard is ready with complete officer assignment data!")
    else:
        print(f"\n❌ Force creation failed. Check errors above.")