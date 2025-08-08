# 🕒 2025-07-09-23-45-00
# Project: SummonsMaster/updated_assignment_columns.py
# Author: R. A. Carucci
# Purpose: ETL with updated Assignment Master columns - Proposed 4-Digit Format + WG1-WG5

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

def updated_assignment_etl():
    """ETL with updated Assignment Master column selection"""
    
    print("🔧 UPDATED ASSIGNMENT COLUMNS ETL")
    print("=" * 60)
    
    # File paths
    base_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack")
    court_file = base_path / "05_EXPORTS" / "_Summons" / "Court" / "25_06_ATS.xlsx"
    assignment_file = base_path / "_Hackensack_Data_Repository" / "ASSIGNED_SHIFT" / "Assignment_Master_V2.xlsx"
    output_dir = base_path / "03_Staging" / "Summons"
    
    try:
        # Load assignment data with selected columns only
        print("📋 Loading Assignment Master V2 with selected columns...")
        assignment_df = pd.read_excel(assignment_file, sheet_name='Sheet1')
        
        print(f"✅ Assignment data loaded: {len(assignment_df)} officers")
        print(f"📋 Available columns: {assignment_df.columns.tolist()}")
        
        # Select only the requested columns
        selected_assignment_columns = [
            'PADDED_BADGE_NUMBER',      # For matching
            'Proposed 4-Digit Format',  # Officer display name
            'WG1',                      # Work Group 1
            'WG2',                      # Work Group 2  
            'WG3',                      # Work Group 3
            'WG4',                      # Work Group 4
            'WG5'                       # Work Group 5
        ]
        
        # Verify columns exist
        missing_columns = [col for col in selected_assignment_columns if col not in assignment_df.columns]
        if missing_columns:
            print(f"❌ Missing columns: {missing_columns}")
            print(f"📋 Available columns containing 'Proposed': {[col for col in assignment_df.columns if 'Proposed' in col]}")
            print(f"📋 Available columns containing 'WG': {[col for col in assignment_df.columns if 'WG' in col]}")
            return None
        
        # Filter to selected columns
        assignment_clean = assignment_df[selected_assignment_columns].copy()
        
        # Clean the data
        assignment_clean['PADDED_BADGE_NUMBER'] = assignment_clean['PADDED_BADGE_NUMBER'].astype(str).str.strip()
        assignment_clean['OFFICER_DISPLAY_NAME'] = assignment_clean['Proposed 4-Digit Format'].astype(str).str.strip()
        
        print(f"📋 Sample assignment data:")
        for _, row in assignment_clean.head().iterrows():
            print(f"   Badge {row['PADDED_BADGE_NUMBER']} → {row['OFFICER_DISPLAY_NAME']}")
            print(f"      WG1: {row['WG1']} | WG2: {row['WG2']} | WG3: {row['WG3']} | WG4: {row['WG4']} | WG5: {row['WG5']}")
        
        # Create assignment lookup with new structure
        assignment_lookup = {}
        for _, row in assignment_clean.iterrows():
            badge = str(row['PADDED_BADGE_NUMBER']).strip()
            assignment_lookup[badge] = {
                'OFFICER_DISPLAY_NAME': str(row['OFFICER_DISPLAY_NAME']).strip(),
                'WG1': str(row['WG1']).strip(),
                'WG2': str(row['WG2']).strip(), 
                'WG3': str(row['WG3']).strip(),
                'WG4': str(row['WG4']).strip(),
                'WG5': str(row['WG5']).strip()
            }
        
        print(f"✅ Assignment lookup created: {len(assignment_lookup)} officers")
        
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
        
        # Clean court badge numbers
        court_df['BADGE_CLEAN'] = court_df['BADGE_NUMBER_RAW'].astype(str).str.strip()
        court_df = court_df[court_df['BADGE_CLEAN'] != '0']
        court_df['PADDED_BADGE_NUMBER'] = court_df['BADGE_CLEAN'].str.zfill(4)
        
        print(f"✅ Court data processed: {len(court_df)} records")
        
        # Apply assignments with new column structure
        print("🔗 Applying updated assignment structure...")
        
        # Initialize new assignment columns
        court_df['OFFICER_DISPLAY_NAME'] = ''
        court_df['WG1'] = ''
        court_df['WG2'] = ''
        court_df['WG3'] = ''
        court_df['WG4'] = ''
        court_df['WG5'] = ''
        court_df['ASSIGNMENT_FOUND'] = False
        
        # Apply assignments
        match_count = 0
        for idx, row in court_df.iterrows():
            badge = row['PADDED_BADGE_NUMBER']
            if badge in assignment_lookup:
                assignment = assignment_lookup[badge]
                court_df.at[idx, 'OFFICER_DISPLAY_NAME'] = assignment['OFFICER_DISPLAY_NAME']
                court_df.at[idx, 'WG1'] = assignment['WG1']
                court_df.at[idx, 'WG2'] = assignment['WG2']
                court_df.at[idx, 'WG3'] = assignment['WG3']
                court_df.at[idx, 'WG4'] = assignment['WG4']
                court_df.at[idx, 'WG5'] = assignment['WG5']
                court_df.at[idx, 'ASSIGNMENT_FOUND'] = True
                match_count += 1
        
        match_rate = (match_count / len(court_df)) * 100
        print(f"✅ Assignment matching complete: {match_count} matches ({match_rate:.1f}%)")
        
        # Show successful matches
        matched_officers = court_df[court_df['ASSIGNMENT_FOUND'] == True][['PADDED_BADGE_NUMBER', 'OFFICER_DISPLAY_NAME', 'WG1', 'WG2', 'WG3']].drop_duplicates()
        print(f"📋 Successfully matched officers:")
        for _, officer in matched_officers.iterrows():
            print(f"   Badge {officer['PADDED_BADGE_NUMBER']} → {officer['OFFICER_DISPLAY_NAME']}")
            print(f"      {officer['WG1']} → {officer['WG2']} → {officer['WG3']}")
        
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
        court_df['ETL_VERSION'] = 'v10.0_UPDATED_ASSIGNMENTS'
        
        # Select columns for Power BI with new structure
        powerbi_columns = [
            'PADDED_BADGE_NUMBER', 'OFFICER_DISPLAY_NAME', 
            'WG1', 'WG2', 'WG3', 'WG4', 'WG5',
            'TICKET_NUMBER', 'ISSUE_DATE', 'VIOLATION_NUMBER', 'VIOLATION_TYPE', 'TYPE', 'STATUS',
            'TOTAL_PAID_AMOUNT', 'FINE_AMOUNT', 'COST_AMOUNT', 'MISC_AMOUNT',
            'OFFICER_NAME_RAW', 'ASSIGNMENT_FOUND', 'PROCESSED_TIMESTAMP', 'DATA_SOURCE', 'ETL_VERSION'
        ]
        
        final_df = court_df[powerbi_columns].copy()
        
        # Ensure proper data types
        final_df['PADDED_BADGE_NUMBER'] = final_df['PADDED_BADGE_NUMBER'].astype(str)
        final_df['ASSIGNMENT_FOUND'] = final_df['ASSIGNMENT_FOUND'].astype(bool)
        
        # Clean text columns
        text_columns = ['OFFICER_DISPLAY_NAME', 'WG1', 'WG2', 'WG3', 'WG4', 'WG5']
        for col in text_columns:
            final_df[col] = final_df[col].fillna('').astype(str)
            final_df[col] = final_df[col].replace(['nan', 'None', ''], None)
        
        # Export updated file
        print("💾 Creating updated Power BI file...")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        output_file = output_dir / "summons_powerbi_latest.xlsx"
        
        # Delete existing file
        if output_file.exists():
            output_file.unlink()
        
        # Write with exact sheet name
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            final_df.to_excel(writer, sheet_name='ATS_Court_Data', index=False)
        
        # Create backup
        backup_file = output_dir / f"summons_powerbi_UPDATED_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        with pd.ExcelWriter(backup_file, engine='openpyxl') as writer:
            final_df.to_excel(writer, sheet_name='ATS_Court_Data', index=False)
        
        # Verify file
        verification_df = pd.read_excel(output_file, sheet_name='ATS_Court_Data')
        verified_matches = verification_df['ASSIGNMENT_FOUND'].sum()
        verified_names = verification_df['OFFICER_DISPLAY_NAME'].notna().sum()
        
        print(f"✅ File verification:")
        print(f"   Total records: {len(verification_df)}")
        print(f"   Assignment matches: {verified_matches}")
        print(f"   Officer names populated: {verified_names}")
        
        if verified_names > 0:
            sample_names = verification_df[verification_df['OFFICER_DISPLAY_NAME'].notna()]['OFFICER_DISPLAY_NAME'].unique()[:5]
            print(f"   Sample officer names: {sample_names.tolist()}")
        
        # Calculate final statistics
        total_revenue = final_df['TOTAL_PAID_AMOUNT'].sum()
        unique_officers = final_df[final_df['ASSIGNMENT_FOUND'] == True]['OFFICER_DISPLAY_NAME'].nunique()
        moving_count = (final_df['TYPE'] == 'M').sum()
        parking_count = (final_df['TYPE'] == 'P').sum()
        
        print(f"\n🎯 UPDATED ASSIGNMENT STRUCTURE RESULTS:")
        print(f"├── Total Records: {len(final_df):,}")
        print(f"├── Assignment Match Rate: {match_rate:.1f}%")
        print(f"├── Officers with Data: {unique_officers}")
        print(f"├── Total Revenue: ${total_revenue:,.2f}")
        print(f"├── Moving Violations: {moving_count:,}")
        print(f"├── Parking Violations: {parking_count:,}")
        print(f"└── File: {output_file}")
        
        print(f"\n📊 NEW COLUMN STRUCTURE:")
        print(f"├── OFFICER_DISPLAY_NAME: Proposed 4-Digit Format")
        print(f"├── WG1: {verification_df[verification_df['ASSIGNMENT_FOUND']==True]['WG1'].iloc[0] if verified_matches > 0 else 'N/A'}")
        print(f"├── WG2: {verification_df[verification_df['ASSIGNMENT_FOUND']==True]['WG2'].iloc[0] if verified_matches > 0 else 'N/A'}")
        print(f"├── WG3: {verification_df[verification_df['ASSIGNMENT_FOUND']==True]['WG3'].iloc[0] if verified_matches > 0 else 'N/A'}")
        print(f"├── WG4: {verification_df[verification_df['ASSIGNMENT_FOUND']==True]['WG4'].iloc[0] if verified_matches > 0 else 'N/A'}")
        print(f"└── WG5: {verification_df[verification_df['ASSIGNMENT_FOUND']==True]['WG5'].iloc[0] if verified_matches > 0 else 'N/A'}")
        
        return final_df
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    result = updated_assignment_etl()
    if result is not None:
        print(f"\n🚀 UPDATED ASSIGNMENT STRUCTURE COMPLETE!")
        print(f"🔄 Refresh Power BI to see new column structure:")
        print(f"   • OFFICER_DISPLAY_NAME (Proposed 4-Digit Format)")
        print(f"   • WG1, WG2, WG3, WG4, WG5 (Work Group Hierarchy)")
        print(f"🎯 Your dashboard now uses the updated assignment structure!")
    else:
        print(f"\n❌ Update failed. Check errors above.")