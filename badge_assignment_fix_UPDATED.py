# 🕒 2025-07-13-15-25-00
# Project: SummonsMaster/badge_assignment_fix_FINAL.py
# Author: R. A. Carucci
# Purpose: Fix badge assignment mapping using the BEST file found by investigation

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

def fix_badge_assignment_mapping():
    print("🔧 FIXING BADGE ASSIGNMENT MAPPING - FINAL VERSION")
    print("=" * 60)
    
    base_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack")
    
    # Use correct file paths
    assignment_file = base_path / "_Hackensack_Data_Repository" / "ASSIGNED_SHIFT" / "Assignment_Master_V2.xlsx"
    
    # Use the BEST file found by investigation - Final_Summons_With_Descriptions.xlsx
    possible_court_files = [
        base_path / "InBox" / "Final_Summons_With_Descriptions.xlsx",  # BEST FILE - 4032 records with Lopez
        base_path / "InBox" / "TEST_RUN_ATS.xlsx",  # 2759 records
        base_path / "InBox" / "acs_ats_cases_by_agencies_20241209_042924.xls.xlsx",  # 2759 records
        base_path / "InBox" / "2025_01_iannacone_SUMMONS.xlsx",  # 170 records
        base_path / "InBox" / "Test - ATS Summons.xlsx"  # Last resort - only 22 records
    ]   
    
    court_file = None
    for file_path in possible_court_files:
        if file_path.exists():
            court_file = file_path
            print(f"✅ Found court file: {court_file}")
            break
    
    print(f"📋 Assignment file: {assignment_file}")
    print(f"   Exists: {'✅' if assignment_file.exists() else '❌'}")
    
    if not court_file:
        print("❌ No court files found")
        return False
    
    if not assignment_file.exists():
        print("❌ Assignment file not found")
        return False
        
    try:
        # Load Assignment Master
        print("\n📋 Loading Assignment Master V2...")
        assignment_df = pd.read_excel(assignment_file)
        print(f"✅ Assignment data loaded: {len(assignment_df)} officers")
        
        # Show sample assignment data for verification
        print("\n📋 Sample assignment data:")
        for idx, row in assignment_df.head(3).iterrows():
            badge = row.get('BADGE_NUMBER', 'N/A')
            name = row.get('FULL_NAME', 'N/A')
            wg2 = row.get('WG2', 'N/A')
            print(f"   Badge: {badge} → {name} → {wg2}")
        
        # Create badge lookup
        badge_lookup = {}
        for _, row in assignment_df.iterrows():
            raw_badge = str(row.get('BADGE_NUMBER', '')).strip()
            officer_name = str(row.get('FULL_NAME', '')).strip()
            
            if not officer_name or officer_name == 'nan':
                continue
                
            assignment_info = {
                'OFFICER_DISPLAY_NAME': officer_name,
                'WG1': str(row.get('WG1', '')).strip(),
                'WG2': str(row.get('WG2', '')).strip(),
                'WG3': str(row.get('WG3', '')).strip(),
                'WG4': str(row.get('WG4', '')).strip(),
                'WG5': str(row.get('WG5', '')).strip()
            }
            
            # Create badge variations
            if raw_badge and raw_badge != 'nan':
                badge_formats = [
                    raw_badge,
                    raw_badge.zfill(4),
                    str(int(float(raw_badge))).zfill(4) if raw_badge.replace('.','').isdigit() else raw_badge
                ]
                
                for badge_format in set(badge_formats):
                    if badge_format:
                        badge_lookup[badge_format] = assignment_info
        
        print(f"✅ Badge lookup created: {len(badge_lookup)} mappings")
        
        # Load court data - NO SKIP ROWS for Final_Summons_With_Descriptions.xlsx
        print(f"\n📊 Loading court data from: {court_file.name}...")
        
        # Determine skip rows based on filename
        skip_rows = 4 if "Test" in court_file.name else 0
        court_df = pd.read_excel(court_file, skiprows=skip_rows)
        
        print(f"✅ Court data loaded: {len(court_df)} records")
        print(f"📋 Columns: {list(court_df.columns[:5])}")
        
        # Clean column names
        court_df.columns = [str(col).replace('\r\n', '_').replace('\n', '_') for col in court_df.columns]
        
        # Identify badge column - Final_Summons has BADGE_NUMBER column
        badge_col_name = court_df.columns[0]
        if 'BADGE' in badge_col_name.upper():
            badge_col_name = badge_col_name  # Already correct
        elif 'Unnamed' in str(badge_col_name):
            court_df = court_df.rename(columns={badge_col_name: 'BADGE_NUMBER'})
            badge_col_name = 'BADGE_NUMBER'
        
        print(f"📋 Using badge column: {badge_col_name}")
        
        # Show sample court data
        sample_badges = court_df[badge_col_name].dropna().astype(str).head(10).tolist()
        print(f"📋 Sample court badges: {sample_badges}")
        
        # Clean badge numbers
        court_df['BADGE_CLEAN'] = court_df[badge_col_name].astype(str).str.strip()
        court_df = court_df[~court_df['BADGE_CLEAN'].isin(['0', '0000', 'nan', 'None', '', 'TOTAL'])]
        court_df = court_df[court_df['BADGE_CLEAN'].notna()]
        
        print(f"📊 Clean court data: {len(court_df)} records")
        print(f"📊 Unique badges: {court_df['BADGE_CLEAN'].nunique()}")
        
        # Apply assignment matching
        print("\n🔗 Applying assignment matching...")
        court_df['PADDED_BADGE_NUMBER'] = court_df['BADGE_CLEAN'].str.zfill(4)
        court_df['OFFICER_DISPLAY_NAME'] = ''
        court_df['WG1'] = ''
        court_df['WG2'] = ''
        court_df['WG3'] = ''
        court_df['WG4'] = ''
        court_df['WG5'] = ''
        court_df['ASSIGNMENT_FOUND'] = False
        
        match_count = 0
        lopez_matches = []
        match_details = {}
        
        for idx, row in court_df.iterrows():
            badge = row['BADGE_CLEAN']
            matched = False
            
            # Try badge variations
            badge_attempts = [
                badge,                                           # Original
                badge.lstrip('0'),                              # Remove leading zeros
                badge.zfill(4),                                 # Ensure 4 digits
                str(int(badge)) if badge.isdigit() else badge  # Clean integer
            ]
            
            # Remove duplicates while preserving order
            badge_attempts = list(dict.fromkeys(badge_attempts))
            
            for attempt_badge in badge_attempts:
                if attempt_badge in badge_lookup:
                    assignment = badge_lookup[attempt_badge]
                    
                    court_df.at[idx, 'OFFICER_DISPLAY_NAME'] = assignment['OFFICER_DISPLAY_NAME']
                    court_df.at[idx, 'WG1'] = assignment['WG1']
                    court_df.at[idx, 'WG2'] = assignment['WG2']
                    court_df.at[idx, 'WG3'] = assignment['WG3']
                    court_df.at[idx, 'WG4'] = assignment['WG4']
                    court_df.at[idx, 'WG5'] = assignment['WG5']
                    court_df.at[idx, 'ASSIGNMENT_FOUND'] = True
                    
                    match_count += 1
                    
                    # Track match details
                    match_key = f"{badge} -> {attempt_badge}"
                    if match_key not in match_details:
                        match_details[match_key] = {
                            'officer': assignment['OFFICER_DISPLAY_NAME'],
                            'count': 0
                        }
                    match_details[match_key]['count'] += 1
                    
                    # Track Lopez matches
                    if 'LOPEZ' in assignment['OFFICER_DISPLAY_NAME'].upper():
                        lopez_matches.append({
                            'badge': badge,
                            'officer': assignment['OFFICER_DISPLAY_NAME'],
                            'assignment': assignment['WG2']
                        })
                    
                    matched = True
                    break
        
        # Results
        match_rate = (match_count / len(court_df)) * 100
        print(f"\n✅ ASSIGNMENT MATCHING RESULTS:")
        print(f"   • Total records: {len(court_df):,}")
        print(f"   • Successful matches: {match_count:,}")
        print(f"   • Match rate: {match_rate:.1f}%")
        print(f"   • Unmatched records: {len(court_df) - match_count:,}")
        
        # Show top matches
        print(f"\n📋 Top successful matches:")
        sorted_matches = sorted(match_details.items(), key=lambda x: x[1]['count'], reverse=True)[:5]
        for match_key, details in sorted_matches:
            print(f"   {match_key} → {details['officer']} ({details['count']} records)")
        
        # Check Lopez specifically
        print(f"\n🔍 ANDRES LOPEZ VERIFICATION:")
        if lopez_matches:
            for match in lopez_matches:
                print(f"   ✅ Badge {match['badge']} → {match['officer']} → {match['assignment']}")
                
            school_errors = [m for m in lopez_matches if 'SCHOOL' in m['assignment'].upper()]
            if school_errors:
                print(f"   ❌ SCHOOL THREAT ERROR FOUND!")
                for error in school_errors:
                    print(f"      Badge {error['badge']}: {error['assignment']}")
            else:
                print(f"   ✅ NO SCHOOL THREAT ERRORS - All Lopez assignments correct!")
        else:
            print(f"   ❌ Lopez not found in court data matches")
        
        # Check for badge conflicts
        print(f"\n🔍 Checking for badge conflicts...")
        badge_conflicts = court_df.groupby('PADDED_BADGE_NUMBER')['OFFICER_DISPLAY_NAME'].nunique()
        conflicts = badge_conflicts[badge_conflicts > 1]
        
        if len(conflicts) > 0:
            print(f"   ⚠️  Found {len(conflicts)} badges with multiple officers:")
            for badge, count in conflicts.head(3).items():
                officers = court_df[court_df['PADDED_BADGE_NUMBER'] == badge]['OFFICER_DISPLAY_NAME'].unique()
                print(f"      Badge {badge}: {list(officers)}")
        else:
            print(f"   ✅ No badge conflicts - each badge maps to exactly one officer")
        
        # Save results
        output_dir = base_path / "03_Staging" / "Summons"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = output_dir / f"FINAL_fixed_assignment_data_{timestamp}.xlsx"
        court_df.to_excel(output_file, index=False)
        
        print(f"\n💾 Data saved to: {output_file}")
        
        # Final summary
        print(f"\n🎯 FINAL SUMMARY:")
        if match_rate > 85:
            print(f"   ✅ SUCCESS: {match_rate:.1f}% assignment match rate")
        else:
            print(f"   ⚠️  REVIEW NEEDED: {match_rate:.1f}% match rate")
        
        if lopez_matches and not any('SCHOOL' in m['assignment'].upper() for m in lopez_matches):
            print(f"   ✅ SUCCESS: Andres Lopez assignments are correct")
        elif lopez_matches:
            print(f"   ❌ ISSUE: Andres Lopez has school threat assignment errors")
        else:
            print(f"   ⚠️  INFO: Andres Lopez not found in this dataset")
        
        if len(conflicts) == 0:
            print(f"   ✅ SUCCESS: No badge assignment conflicts")
        else:
            print(f"   ⚠️  REVIEW: {len(conflicts)} badge conflicts found")
        
        print(f"\n🚀 READY FOR M-CODE IMPLEMENTATION!")
        return True
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    fix_badge_assignment_mapping()