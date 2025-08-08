# 🕒 2025-07-13-15-05-00
# Project: SummonsMaster/badge_assignment_fix_UPDATED.py
# Author: R. A. Carucci
# Purpose: Fix badge assignment mapping with correct file paths from search results

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

def fix_badge_assignment_mapping():
    """
    Fix badge assignment mapping with corrected file paths
    """
    
    print("🔧 FIXING BADGE ASSIGNMENT MAPPING - UPDATED VERSION")
    print("=" * 60)
    
    base_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack")
    
    # Use the CORRECT file paths from your search results
    assignment_file = base_path / "_Hackensack_Data_Repository" / "ASSIGNED_SHIFT" / "Assignment_Master_V2.xlsx"
    
    # Try court files in order of preference
    possible_court_files = [
        base_path / "InBox" / "2025_04_ATS_Report.xlsx",  # Check if this exists
        base_path / "_MONTHLY_DATA" / "_EXPORTS" / "Summons" / "Court" / "2025_04_ATS_Report.xlsx",
        base_path / "05_EXPORTS" / "25_06_ATS.xlsx"
    ]
    
    court_file = None
    for file_path in possible_court_files:
        if file_path.exists():
            court_file = file_path
            break
    
    # Verify files exist
    print(f"📋 Assignment file: {assignment_file}")
    print(f"   Exists: {'✅' if assignment_file.exists() else '❌'}")
    
    if court_file:
        print(f"📊 Court file: {court_file}")
        print(f"   Exists: {'✅' if court_file.exists() else '❌'}")
    else:
        print("❌ No court files found. Available files:")
        for file_path in possible_court_files:
            print(f"   {file_path} - {'Found' if file_path.exists() else 'Not found'}")
        return False
    
    if not assignment_file.exists():
        print(f"❌ Assignment file not found at: {assignment_file}")
        return False
        
    try:
        # 1. Load Assignment Master V2
        print("\n📋 Loading Assignment Master V2...")
        assignment_df = pd.read_excel(assignment_file)
        
        print(f"✅ Assignment data loaded: {len(assignment_df)} officers")
        print(f"📋 Columns: {list(assignment_df.columns)}")
        
        # Show first few records to verify data
        print("\n📋 Sample assignment data:")
        for idx, row in assignment_df.head(3).iterrows():
            badge = row.get('BADGE_NUMBER', 'N/A')
            padded = row.get('PADDED_BADGE_NUMBER', 'N/A')
            name = row.get('FULL_NAME', 'N/A')
            wg2 = row.get('WG2', 'N/A')
            print(f"   Badge: {badge} → Padded: {padded} → {name} ({wg2})")
        
        # 2. Create badge lookup dictionary
        print("\n🔗 Creating badge lookup...")
        badge_lookup = {}
        
        for _, row in assignment_df.iterrows():
            # Get badge info
            raw_badge = str(row.get('BADGE_NUMBER', '')).strip()
            padded_badge = str(row.get('PADDED_BADGE_NUMBER', '')).strip()
            officer_name = str(row.get('FULL_NAME', '')).strip()
            
            if not officer_name or officer_name == 'nan':
                continue
                
            # Officer assignment info
            assignment_info = {
                'OFFICER_DISPLAY_NAME': officer_name,
                'WG1': str(row.get('WG1', '')).strip(),
                'WG2': str(row.get('WG2', '')).strip(),
                'WG3': str(row.get('WG3', '')).strip(),
                'WG4': str(row.get('WG4', '')).strip(),
                'WG5': str(row.get('WG5', '')).strip()
            }
            
            # Create multiple badge format entries
            badge_formats = []
            
            if raw_badge and raw_badge != 'nan':
                badge_formats.extend([
                    raw_badge,
                    raw_badge.zfill(4),
                    str(int(float(raw_badge))).zfill(4) if raw_badge.replace('.','').isdigit() else raw_badge
                ])
            
            if padded_badge and padded_badge != 'nan' and padded_badge != raw_badge:
                badge_formats.append(padded_badge)
            
            # Remove duplicates
            badge_formats = list(set([b for b in badge_formats if b and b != 'nan']))
            
            # Add to lookup
            for badge_format in badge_formats:
                badge_lookup[badge_format] = assignment_info
        
        print(f"✅ Badge lookup created: {len(badge_lookup)} badge mappings")
        
        # Check for Andres Lopez specifically
        lopez_found = False
        for badge, info in badge_lookup.items():
            if 'LOPEZ' in info['OFFICER_DISPLAY_NAME'].upper() or 'ANDRES' in info['OFFICER_DISPLAY_NAME'].upper():
                print(f"📋 Found Lopez: Badge {badge} → {info['OFFICER_DISPLAY_NAME']} ({info['WG2']})")
                lopez_found = True
        
        if not lopez_found:
            print("⚠️  Andres Lopez not found in assignment master!")
        
        # 3. Load court data
        print(f"\n📊 Loading court data...")
        court_df = pd.read_excel(court_file, skiprows=4)
        
        # Clean column names
        court_df.columns = [str(col).replace('\r\n', '_').replace('\n', '_') for col in court_df.columns]
        
        # Identify badge column
        badge_col_name = court_df.columns[0]
        if 'Unnamed' in str(badge_col_name) or badge_col_name == 0:
            court_df = court_df.rename(columns={badge_col_name: 'BADGE_NUMBER'})
            badge_col_name = 'BADGE_NUMBER'
        
        print(f"✅ Court data loaded: {len(court_df)} records")
        print(f"📋 Badge column: {badge_col_name}")
        
        # 4. Clean court badge numbers
        court_df['BADGE_CLEAN'] = court_df[badge_col_name].astype(str).str.strip()
        
        # Remove invalid badges
        invalid_badges = ['0', '0000', 'nan', 'None', '', 'TOTAL', 'Total']
        court_df = court_df[~court_df['BADGE_CLEAN'].isin(invalid_badges)]
        court_df = court_df[court_df['BADGE_CLEAN'].notna()]
        
        print(f"📊 Clean court data: {len(court_df)} records")
        print(f"📋 Unique badges: {court_df['BADGE_CLEAN'].nunique()}")
        
        # Show sample court badges
        sample_badges = sorted(court_df['BADGE_CLEAN'].unique())[:10]
        print(f"📋 Sample court badges: {sample_badges}")
        
        # 5. Apply assignment matching
        print("\n🔗 Applying assignment matching...")
        
        # Initialize columns
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
        
        for idx, row in court_df.iterrows():
            badge = row['BADGE_CLEAN']
            matched = False
            
            # Try multiple badge formats
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
                    
                    # Apply assignment
                    court_df.at[idx, 'OFFICER_DISPLAY_NAME'] = assignment['OFFICER_DISPLAY_NAME']
                    court_df.at[idx, 'WG1'] = assignment['WG1']
                    court_df.at[idx, 'WG2'] = assignment['WG2']
                    court_df.at[idx, 'WG3'] = assignment['WG3']
                    court_df.at[idx, 'WG4'] = assignment['WG4']
                    court_df.at[idx, 'WG5'] = assignment['WG5']
                    court_df.at[idx, 'ASSIGNMENT_FOUND'] = True
                    
                    match_count += 1
                    
                    # Track Lopez matches
                    if 'LOPEZ' in assignment['OFFICER_DISPLAY_NAME'].upper():
                        lopez_matches.append({
                            'badge': badge,
                            'officer': assignment['OFFICER_DISPLAY_NAME'],
                            'assignment': assignment['WG2']
                        })
                    
                    matched = True
                    break
        
        # 6. Results
        match_rate = (match_count / len(court_df)) * 100
        print(f"\n✅ ASSIGNMENT MATCHING COMPLETE!")
        print(f"📊 Results:")
        print(f"   • Total records: {len(court_df):,}")
        print(f"   • Successful matches: {match_count:,}")
        print(f"   • Match rate: {match_rate:.1f}%")
        
        # 7. Check Lopez specifically
        print(f"\n🔍 ANDRES LOPEZ VERIFICATION:")
        if lopez_matches:
            for match in lopez_matches:
                print(f"   ✅ Badge {match['badge']} → {match['officer']} → {match['assignment']}")
            
            # Check if any show incorrect "School threat" assignment
            school_threat_matches = [m for m in lopez_matches if 'SCHOOL' in m['assignment'].upper()]
            if school_threat_matches:
                print(f"   ❌ Found incorrect School threat assignments:")
                for match in school_threat_matches:
                    print(f"      Badge {match['badge']} incorrectly shows: {match['assignment']}")
            else:
                print(f"   ✅ No incorrect School threat assignments found")
        else:
            print(f"   ❌ Andres Lopez not found in court data matches")
        
        # 8. Check for badge conflicts
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
        
        # 9. Save results
        output_dir = base_path / "03_Staging" / "Summons"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = output_dir / f"fixed_assignment_data_{timestamp}.xlsx"
        
        court_df.to_excel(output_file, index=False)
        print(f"\n💾 Fixed data saved to: {output_file}")
        
        # Summary
        print(f"\n🎯 SUMMARY:")
        if match_rate > 85:
            print(f"   ✅ SUCCESS: {match_rate:.1f}% assignment match rate")
        else:
            print(f"   ⚠️  LOW MATCH RATE: {match_rate:.1f}% - needs investigation")
        
        if lopez_matches and not any('SCHOOL' in m['assignment'].upper() for m in lopez_matches):
            print(f"   ✅ SUCCESS: Andres Lopez assignments appear correct")
        else:
            print(f"   ❌ ISSUE: Andres Lopez assignments need review")
        
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = fix_badge_assignment_mapping()
    
    if success:
        print(f"\n🚀 NEXT STEPS:")
        print(f"1. Review the fixed data file")
        print(f"2. Update your M-Code queries with new assignment logic")
        print(f"3. Test your DAX measure with the corrected data")
    else:
        print(f"\n💡 Check file paths and data format, then retry")