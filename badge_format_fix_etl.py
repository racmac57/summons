# 🕒 2025-07-09-24-00-00
# Project: SummonsMaster/badge_format_fix_etl.py
# Author: R. A. Carucci
# Purpose: Fix badge number format mismatch causing assignment join failures

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

def fix_badge_format_matching():
    """Fix badge number format mismatch in assignment matching"""
    
    print("🔧 FIXING BADGE NUMBER FORMAT MISMATCH")
    print("=" * 60)
    
    # File paths
    base_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack")
    court_file = base_path / "05_EXPORTS" / "_Summons" / "Court" / "25_06_ATS.xlsx"
    assignment_file = base_path / "_Hackensack_Data_Repository" / "ASSIGNED_SHIFT" / "Assignment_Master_V2.xlsx"
    output_dir = base_path / "03_Staging" / "Summons"
    
    try:
        # Load assignment data and analyze badge formats
        print("📋 Loading Assignment Master V2...")
        assignment_df = pd.read_excel(assignment_file, sheet_name='Sheet1')
        
        print(f"✅ Assignment data loaded: {len(assignment_df)} officers")
        print(f"📋 Assignment badge formats analysis:")
        print(f"   BADGE_NUMBER samples: {assignment_df['BADGE_NUMBER'].head(10).tolist()}")
        print(f"   PADDED_BADGE_NUMBER samples: {assignment_df['PADDED_BADGE_NUMBER'].head(10).tolist()}")
        
        # Create comprehensive badge lookup with multiple formats
        badge_lookup = {}
        
        for _, row in assignment_df.iterrows():
            # Get all possible badge formats
            raw_badge = str(row['BADGE_NUMBER']).strip()
            padded_badge = str(row['PADDED_BADGE_NUMBER']).strip()
            
            # Create variations for matching
            badge_variations = [
                raw_badge,                          # Original: 83, 386, 2008
                raw_badge.zfill(4),                # Padded: 0083, 0386, 2008
                padded_badge,                       # From PADDED column
                str(int(float(raw_badge))).zfill(4) if raw_badge.replace('.', '').isdigit() else raw_badge.zfill(4)
            ]
            
            # Remove duplicates while preserving order
            badge_variations = list(dict.fromkeys(badge_variations))
            
            # Assignment data
            assignment_data = {
                'OFFICER_DISPLAY_NAME': str(row['Proposed 4-Digit Format']).strip(),
                'WG1': str(row['WG1']).strip(),
                'WG2': str(row['WG2']).strip(),
                'WG3': str(row['WG3']).strip(),
                'WG4': str(row['WG4']).strip() if pd.notna(row['WG4']) else '',
                'WG5': str(row['WG5']).strip() if pd.notna(row['WG5']) else ''
            }
            
            # Add all variations to lookup
            for badge_var in badge_variations:
                if badge_var and badge_var != 'nan':
                    badge_lookup[badge_var] = assignment_data
        
        print(f"✅ Badge lookup created with {len(badge_lookup)} badge variations")
        print(f"📋 Sample badge variations: {list(badge_lookup.keys())[:10]}")
        
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
        
        # Clean court badge numbers and create variations
        court_df['BADGE_CLEAN'] = court_df['BADGE_NUMBER_RAW'].astype(str).str.strip()
        court_df = court_df[~court_df['BADGE_CLEAN'].isin(['0', '0000', '', 'nan'])]
        
        print(f"✅ Court data processed: {len(court_df)} records")
        print(f"📋 Court badge samples: {court_df['BADGE_CLEAN'].head(10).tolist()}")
        print(f"📋 Unique court badges: {court_df['BADGE_CLEAN'].nunique()}")
        
        # Initialize assignment columns
        court_df['PADDED_BADGE_NUMBER'] = court_df['BADGE_CLEAN']
        court_df['OFFICER_DISPLAY_NAME'] = ''
        court_df['WG1'] = ''
        court_df['WG2'] = ''
        court_df['WG3'] = ''
        court_df['WG4'] = ''
        court_df['WG5'] = ''
        court_df['ASSIGNMENT_FOUND'] = False
        
        # Perform assignment matching with multiple badge format attempts
        print("🔗 Performing enhanced badge matching...")
        
        match_count = 0
        match_details = {}
        
        for idx, row in court_df.iterrows():
            badge = row['BADGE_CLEAN']
            matched = False
            
            # Try multiple badge formats for matching
            badge_attempts = [
                badge,                    # Original: 0083
                badge.lstrip('0'),       # Remove leading zeros: 83
                badge.zfill(4),          # Ensure 4 digits: 0083
                str(int(badge)) if badge.isdigit() else badge,  # Clean integer: 83
                str(int(badge)).zfill(4) if badge.isdigit() else badge  # Clean and pad: 0083
            ]
            
            # Remove duplicates while preserving order
            badge_attempts = list(dict.fromkeys(badge_attempts))
            
            for attempt_badge in badge_attempts:
                if attempt_badge in badge_lookup:
                    assignment = badge_lookup[attempt_badge]
                    
                    # Apply assignment data
                    court_df.at[idx, 'PADDED_BADGE_NUMBER'] = badge.zfill(4)
                    court_df.at[idx, 'OFFICER_DISPLAY_NAME'] = assignment['OFFICER_DISPLAY_NAME']
                    court_df.at[idx, 'WG1'] = assignment['WG1']
                    court_df.at[idx, 'WG2'] = assignment['WG2']
                    court_df.at[idx, 'WG3'] = assignment['WG3']
                    court_df.at[idx, 'WG4'] = assignment['WG4']
                    court_df.at[idx, 'WG5'] = assignment['WG5']
                    court_df.at[idx, 'ASSIGNMENT_FOUND'] = True
                    
                    match_count += 1
                    
                    # Track match details
                    if attempt_badge not in match_details:
                        match_details[attempt_badge] = {
                            'original_badge': badge,
                            'matched_format': attempt_badge,
                            'officer_name': assignment['OFFICER_DISPLAY_NAME'],
                            'count': 0
                        }
                    match_details[attempt_badge]['count'] += 1
                    
                    matched = True
                    break
            
            if not matched:
                # Ensure padded badge number for unmatched records
                court_df.at[idx, 'PADDED_BADGE_NUMBER'] = badge.zfill(4)
        
        match_rate = (match_count / len(court_df)) * 100
        print(f"✅ Enhanced matching complete: {match_count} matches ({match_rate:.1f}%)")
        
        # Show successful match details
        print(f"📋 Successful match breakdown:")
        for badge, details in sorted(match_details.items(), key=lambda x: x[1]['count'], reverse=True)[:10]:
            print(f"   Badge {details['original_badge']} → {details['matched_format']} → {details['officer_name']} ({details['count']} records)")
        
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
        court_df['ETL_VERSION'] = 'v11.0_BADGE_FORMAT_FIXED'
        
        # Select columns for Power BI
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
        
        # Clean text columns - replace empty strings with None for proper null handling
        text_columns = ['OFFICER_DISPLAY_NAME', 'WG1', 'WG2', 'WG3', 'WG4', 'WG5']
        for col in text_columns:
            final_df[col] = final_df[col].replace(['', 'nan', 'None'], None)
        
        # Export updated file
        print("💾 Creating badge-format-fixed Power BI file...")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        output_file = output_dir / "summons_powerbi_latest.xlsx"
        
        # Delete existing file
        if output_file.exists():
            output_file.unlink()
        
        # Write with exact sheet name
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            final_df.to_excel(writer, sheet_name='ATS_Court_Data', index=False)
        
        # Create backup
        backup_file = output_dir / f"summons_powerbi_BADGE_FIXED_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        with pd.ExcelWriter(backup_file, engine='openpyxl') as writer:
            final_df.to_excel(writer, sheet_name='ATS_Court_Data', index=False)
        
        # Calculate final statistics
        total_revenue = final_df['TOTAL_PAID_AMOUNT'].sum()
        unique_officers = final_df[final_df['ASSIGNMENT_FOUND'] == True]['OFFICER_DISPLAY_NAME'].nunique()
        moving_count = (final_df['TYPE'] == 'M').sum()
        parking_count = (final_df['TYPE'] == 'P').sum()
        
        print(f"✅ Files exported:")
        print(f"   - {output_file}")
        print(f"   - {backup_file}")
        
        print(f"\n🎯 BADGE FORMAT FIX RESULTS:")
        print(f"├── Total Records: {len(final_df):,}")
        print(f"├── Assignment Match Rate: {match_rate:.1f}%")
        print(f"├── Officers with Data: {unique_officers}")
        print(f"├── Total Revenue: ${total_revenue:,.2f}")
        print(f"├── Moving Violations: {moving_count:,}")
        print(f"├── Parking Violations: {parking_count:,}")
        print(f"└── File: {output_file}")
        
        # Show unique officers found
        if unique_officers > 0:
            officer_list = final_df[final_df['ASSIGNMENT_FOUND'] == True]['OFFICER_DISPLAY_NAME'].unique()
            print(f"\n👮 Officers Successfully Matched:")
            for officer in officer_list:
                officer_records = final_df[final_df['OFFICER_DISPLAY_NAME'] == officer]
                print(f"   {officer}: {len(officer_records)} records")
        
        return final_df
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    result = fix_badge_format_matching()
    if result is not None:
        print(f"\n🚀 BADGE FORMAT FIX COMPLETE!")
        print(f"🔄 Refresh Power BI to see officers with assignment data!")
        print(f"📊 Your DAX measures should now work with populated officer names!")
    else:
        print(f"\n❌ Badge format fix failed. Check errors above.")