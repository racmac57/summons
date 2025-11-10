# 🕒 2025-07-09-24-30-00
# Project: SummonsMaster/simple_perfect_etl.py
# Author: R. A. Carucci
# Purpose: Simple approach to achieve 100% match rate without regex issues

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

def simple_perfect_match():
    """Simple approach to achieve 100% match rate"""
    
    print("🎯 SIMPLE PERFECT 100% MATCH RATE")
    print("=" * 60)
    
    # File paths
    base_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack")
    court_file = base_path / "05_EXPORTS" / "_Summons" / "Court" / "25_06_ATS.xlsx"
    assignment_file = base_path / "_Hackensack_Data_Repository" / "ASSIGNED_SHIFT" / "Assignment_Master_V2.xlsx"
    output_dir = base_path / "03_Staging" / "Summons"
    
    try:
        # Load assignment data
        print("📋 Loading Assignment Master V2...")
        assignment_df = pd.read_excel(assignment_file, sheet_name='Sheet1')
        
        # Create badge lookup
        badge_lookup = {}
        for _, row in assignment_df.iterrows():
            raw_badge = str(row['BADGE_NUMBER']).strip()
            padded_badge = str(row['PADDED_BADGE_NUMBER']).strip()
            
            badge_variations = [
                raw_badge,
                raw_badge.zfill(4),
                padded_badge,
                str(int(float(raw_badge))).zfill(4) if raw_badge.replace('.', '').isdigit() else raw_badge.zfill(4)
            ]
            badge_variations = list(dict.fromkeys(badge_variations))
            
            assignment_data = {
                'OFFICER_DISPLAY_NAME': str(row['Proposed 4-Digit Format']).strip(),
                'WG1': str(row['WG1']).strip(),
                'WG2': str(row['WG2']).strip(),
                'WG3': str(row['WG3']).strip(),
                'WG4': str(row['WG4']).strip() if pd.notna(row['WG4']) else '',
                'WG5': str(row['WG5']).strip() if pd.notna(row['WG5']) else ''
            }
            
            for badge_var in badge_variations:
                if badge_var and badge_var != 'nan':
                    badge_lookup[badge_var] = assignment_data
        
        print(f"✅ Assignment lookup created: {len(badge_lookup)} badge variations")
        
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
        
        print(f"📋 Raw court data loaded: {len(court_df)} rows")
        
        # STEP 1: Simple footer removal
        print("🗑️ Filtering out footer rows...")
        
        # Convert to string and check for footer content
        court_df['BADGE_STR'] = court_df['BADGE_NUMBER_RAW'].astype(str)
        court_df['OFFICER_STR'] = court_df['OFFICER_NAME_RAW'].astype(str)
        court_df['TICKET_STR'] = court_df['TICKET_NUMBER'].astype(str)
        
        # Simple string matching for footers (no regex)
        footer_conditions = (
            court_df['BADGE_STR'].str.upper().str.contains('TOTAL', na=False) |
            court_df['OFFICER_STR'].str.contains('Run Date', na=False) |
            court_df['OFFICER_STR'].str.contains('July 7', na=False) |
            court_df['OFFICER_STR'].str.contains('PROG', na=False) |
            court_df['TICKET_STR'].str.contains('acs', na=False) |
            court_df['BADGE_STR'].str.len() > 10  # Footer rows tend to have long text
        )
        
        footer_rows = footer_conditions.sum()
        court_df = court_df[~footer_conditions].reset_index(drop=True)
        
        print(f"✅ Footer filter: {footer_rows} footer rows removed")
        print(f"   Remaining records: {len(court_df)}")
        
        # STEP 2: Filter civilian complaints and invalid badges
        print("🗑️ Filtering out civilian complaints and invalid badges...")
        
        court_df['BADGE_CLEAN'] = court_df['BADGE_NUMBER_RAW'].astype(str).str.strip()
        
        # Simple filtering
        valid_records = (
            (court_df['BADGE_CLEAN'] != '9999') &  # No civilian complaints
            (court_df['BADGE_CLEAN'] != '0') &     # No zero badges
            (court_df['BADGE_CLEAN'] != '0000') &  # No padded zero badges
            (court_df['BADGE_CLEAN'] != '') &      # No empty badges
            (court_df['BADGE_CLEAN'] != 'nan') &   # No nan values
            (court_df['BADGE_CLEAN'].str.len() >= 1) &  # At least 1 character
            (court_df['BADGE_CLEAN'].str.len() <= 6)    # Reasonable length
        )
        
        filtered_count = len(court_df) - valid_records.sum()
        court_df = court_df[valid_records].reset_index(drop=True)
        
        print(f"✅ Invalid records filter: {filtered_count} records removed")
        print(f"   Clean enforcement records: {len(court_df)}")
        
        # STEP 3: Assignment matching
        print("🔗 Performing assignment matching...")
        
        # Initialize assignment columns
        court_df['PADDED_BADGE_NUMBER'] = court_df['BADGE_CLEAN']
        court_df['OFFICER_DISPLAY_NAME'] = ''
        court_df['WG1'] = ''
        court_df['WG2'] = ''
        court_df['WG3'] = ''
        court_df['WG4'] = ''
        court_df['WG5'] = ''
        court_df['ASSIGNMENT_FOUND'] = False
        
        # Perform matching
        match_count = 0
        match_details = {}
        
        for idx, row in court_df.iterrows():
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
                    
                    court_df.at[idx, 'PADDED_BADGE_NUMBER'] = badge.zfill(4)
                    court_df.at[idx, 'OFFICER_DISPLAY_NAME'] = assignment['OFFICER_DISPLAY_NAME']
                    court_df.at[idx, 'WG1'] = assignment['WG1']
                    court_df.at[idx, 'WG2'] = assignment['WG2']
                    court_df.at[idx, 'WG3'] = assignment['WG3']
                    court_df.at[idx, 'WG4'] = assignment['WG4']
                    court_df.at[idx, 'WG5'] = assignment['WG5']
                    court_df.at[idx, 'ASSIGNMENT_FOUND'] = True
                    
                    match_count += 1
                    
                    if attempt_badge not in match_details:
                        match_details[attempt_badge] = {
                            'officer_name': assignment['OFFICER_DISPLAY_NAME'],
                            'count': 0
                        }
                    match_details[attempt_badge]['count'] += 1
                    
                    matched = True
                    break
            
            if not matched:
                court_df.at[idx, 'PADDED_BADGE_NUMBER'] = badge.zfill(4)
        
        match_rate = (match_count / len(court_df)) * 100
        print(f"✅ Assignment matching complete: {match_count} matches ({match_rate:.1f}%)")
        
        # Show unmatched badges if any
        unmatched_df = court_df[court_df['ASSIGNMENT_FOUND'] == False]
        if len(unmatched_df) > 0:
            unmatched_badges = unmatched_df['BADGE_CLEAN'].unique()
            print(f"🔍 Remaining unmatched badges: {sorted(unmatched_badges)}")
            
            # Show a few examples
            for badge in sorted(unmatched_badges)[:3]:
                sample_record = unmatched_df[unmatched_df['BADGE_CLEAN'] == badge].iloc[0]
                print(f"   Badge {badge}: {sample_record['OFFICER_NAME_RAW']}")
        
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
        court_df['ETL_VERSION'] = 'v13.0_SIMPLE_PERFECT'
        
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
        
        # Clean text columns
        text_columns = ['OFFICER_DISPLAY_NAME', 'WG1', 'WG2', 'WG3', 'WG4', 'WG5']
        for col in text_columns:
            final_df[col] = final_df[col].replace(['', 'nan', 'None'], None)
        
        # Export perfect file
        print("💾 Creating SIMPLE PERFECT Power BI file...")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        output_file = output_dir / "summons_powerbi_latest.xlsx"
        
        if output_file.exists():
            output_file.unlink()
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            final_df.to_excel(writer, sheet_name='ATS_Court_Data', index=False)
        
        # Create backup
        backup_file = output_dir / f"summons_powerbi_SIMPLE_PERFECT_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
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
        
        print(f"\n🎯 SIMPLE PERFECT ETL RESULTS:")
        print(f"├── Total Clean Enforcement Records: {len(final_df):,}")
        print(f"├── Assignment Match Rate: {match_rate:.1f}%")
        print(f"├── Officers with Data: {unique_officers}")
        print(f"├── Total Revenue: ${total_revenue:,.2f}")
        print(f"├── Moving Violations: {moving_count:,}")
        print(f"├── Parking Violations: {parking_count:,}")
        print(f"├── Footer Rows Removed: {footer_rows}")
        print(f"├── Invalid Records Removed: {filtered_count}")
        print(f"└── Data Quality: {match_rate:.1f}%")
        
        # Success message
        if match_rate >= 99.9:
            print(f"\n🏆 PERFECTION ACHIEVED!")
            print(f"🎯 Essentially 100% assignment match rate!")
        elif match_rate >= 99.0:
            print(f"\n🏆 NEAR PERFECTION! ({match_rate:.1f}%)")
            print(f"🎯 Excellent data quality achieved!")
        elif match_rate >= 98.0:
            print(f"\n✅ EXCELLENT! ({match_rate:.1f}%)")
            print(f"🎯 Very high data quality achieved!")
        
        return final_df
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    result = simple_perfect_match()
    if result is not None:
        print(f"\n🚀 SIMPLE PERFECT ETL COMPLETE!")
        print(f"🔄 Refresh Power BI to see the perfected dataset!")
        print(f"📊 Your dashboard now has maximum possible data quality!")
    else:
        print(f"\n❌ Simple perfect ETL failed. Check errors above.")