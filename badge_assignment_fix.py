# 🕒 2025-07-13-14-30-00
# Project: SummonsMaster/badge_assignment_fix.py
# Author: R. A. Carucci
# Purpose: Fix badge number format mismatch causing incorrect officer assignments

import pandas as pd
import numpy as np
from pathlib import Path

def fix_badge_assignment_mapping():
    """
    Fix the badge number assignment mapping issue
    
    The problem: Multiple badge numbers are being mapped to the same officer
    (e.g., badges 83, 135, 138 all show "P.O. ANTHONY MATTALIAN 83")
    
    This happens when the badge format in court data doesn't match assignment master
    """
    
    print("🔧 FIXING BADGE ASSIGNMENT MAPPING")
    print("=" * 50)
    
    # File paths
    base_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack")
    assignment_file = base_path / "_ASSIGNMENTS" / "Assignment_Master_V2.xlsx"
    court_file = base_path / "_MONTHLY_DATA" / "_EXPORTS" / "Summons" / "Court" / "2025_04_ATS_Report.xlsx"
    
    try:
        # 1. Load Assignment Master V2 (the corrected version)
        print("📋 Loading Assignment Master V2...")
        assignment_df = pd.read_excel(assignment_file)
        
        # Clean and standardize assignment data
        assignment_df['BADGE_NUMBER'] = assignment_df['BADGE_NUMBER'].astype(str).str.strip()
        assignment_df['PADDED_BADGE_NUMBER'] = assignment_df['PADDED_BADGE_NUMBER'].astype(str).str.strip()
        
        print(f"✅ Assignment data loaded: {len(assignment_df)} officers")
        
        # 2. Create comprehensive badge lookup dictionary
        print("🔗 Creating comprehensive badge lookup...")
        badge_lookup = {}
        
        for _, row in assignment_df.iterrows():
            # Get badge variations
            raw_badge = str(row['BADGE_NUMBER']).strip()
            padded_badge = str(row['PADDED_BADGE_NUMBER']).strip()
            
            # Officer info
            officer_info = {
                'OFFICER_DISPLAY_NAME': row['FULL_NAME'],
                'WG1': row['WG1'],
                'WG2': row['WG2'], 
                'WG3': row['WG3'],
                'WG4': row['WG4'],
                'WG5': row['WG5']
            }
            
            # Create multiple badge format entries for robust matching
            badge_formats = [
                raw_badge,                           # Original: 83
                raw_badge.zfill(4),                 # 4-digit padded: 0083
                padded_badge,                       # From PADDED column
                str(int(float(raw_badge))).zfill(4) if raw_badge.replace('.','').isdigit() else raw_badge
            ]
            
            # Remove duplicates
            badge_formats = list(set(badge_formats))
            
            # Add to lookup with all variations
            for badge_format in badge_formats:
                if badge_format and badge_format != 'nan':
                    badge_lookup[badge_format] = officer_info
        
        print(f"✅ Badge lookup created with {len(badge_lookup)} entries")
        
        # 3. Load court data
        print("📊 Loading court data...")
        
        # Read court export (skip first 4 rows, they contain metadata)
        court_df = pd.read_excel(court_file, skiprows=4)
        
        # Clean column names (remove line breaks)
        court_df.columns = [col.replace('\r\n', '_').replace('\n', '_') if isinstance(col, str) else col for col in court_df.columns]
        
        # The first column should be badge numbers
        if court_df.columns[0] == 0 or 'Unnamed' in str(court_df.columns[0]):
            court_df = court_df.rename(columns={court_df.columns[0]: 'BADGE_NUMBER'})
        
        print(f"✅ Court data loaded: {len(court_df)} records")
        
        # 4. Clean badge numbers from court data
        court_df['BADGE_CLEAN'] = court_df['BADGE_NUMBER'].astype(str).str.strip()
        
        # Remove invalid entries
        invalid_badges = ['0', '0000', 'nan', 'None', '', 'TOTAL', 'Total']
        court_df = court_df[~court_df['BADGE_CLEAN'].isin(invalid_badges)]
        court_df = court_df[court_df['BADGE_CLEAN'].notna()]
        
        print(f"📋 Unique badges in court data: {court_df['BADGE_CLEAN'].nunique()}")
        print(f"📋 Sample court badges: {sorted(court_df['BADGE_CLEAN'].unique())[:10]}")
        
        # 5. Apply assignment matching with FIXED logic
        print("🔗 Applying corrected assignment matching...")
        
        # Initialize assignment columns
        court_df['PADDED_BADGE_NUMBER'] = ''
        court_df['OFFICER_DISPLAY_NAME'] = ''
        court_df['WG1'] = ''
        court_df['WG2'] = ''
        court_df['WG3'] = ''
        court_df['WG4'] = ''
        court_df['WG5'] = ''
        court_df['ASSIGNMENT_FOUND'] = False
        
        match_count = 0
        match_details = {}
        
        for idx, row in court_df.iterrows():
            badge = row['BADGE_CLEAN']
            matched = False
            
            # Try multiple badge formats for matching
            badge_attempts = [
                badge,                                    # Original format
                badge.lstrip('0'),                       # Remove leading zeros
                badge.zfill(4),                         # Ensure 4 digits
                str(int(badge)) if badge.isdigit() else badge  # Clean integer conversion
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
                    
                    # Track successful matches
                    match_key = f"{badge} -> {attempt_badge}"
                    if match_key not in match_details:
                        match_details[match_key] = {
                            'officer': assignment['OFFICER_DISPLAY_NAME'],
                            'count': 0
                        }
                    match_details[match_key]['count'] += 1
                    
                    matched = True
                    break
            
            if not matched:
                # Ensure padded badge number even for unmatched
                court_df.at[idx, 'PADDED_BADGE_NUMBER'] = badge.zfill(4)
        
        match_rate = (match_count / len(court_df)) * 100
        print(f"✅ Assignment matching complete!")
        print(f"📊 Match Results:")
        print(f"   • Total records: {len(court_df):,}")
        print(f"   • Successful matches: {match_count:,}")
        print(f"   • Match rate: {match_rate:.1f}%")
        print(f"   • Unmatched records: {len(court_df) - match_count:,}")
        
        # 6. Show top successful matches
        print(f"\n📋 Top successful matches:")
        sorted_matches = sorted(match_details.items(), key=lambda x: x[1]['count'], reverse=True)[:10]
        for match_key, details in sorted_matches:
            print(f"   {match_key} -> {details['officer']} ({details['count']} records)")
        
        # 7. Verify specific officers mentioned in the issue
        print(f"\n🔍 Checking specific officers...")
        
        # Check for Andres Lopez
        lopez_records = court_df[court_df['OFFICER_DISPLAY_NAME'].str.contains('LOPEZ', na=False, case=False)]
        if len(lopez_records) > 0:
            print(f"   Andres Lopez: {len(lopez_records)} records")
            unique_assignments = lopez_records[['WG2', 'WG3']].drop_duplicates()
            print(f"   Assignments: {unique_assignments.to_dict('records')}")
        
        # Check for multiple officers per badge (the original problem)
        print(f"\n🔍 Checking for badge conflicts...")
        badge_officer_check = court_df.groupby('PADDED_BADGE_NUMBER')['OFFICER_DISPLAY_NAME'].nunique()
        conflicts = badge_officer_check[badge_officer_check > 1]
        
        if len(conflicts) > 0:
            print(f"⚠️  Found {len(conflicts)} badges with multiple officers:")
            for badge, officer_count in conflicts.head(5).items():
                officers = court_df[court_df['PADDED_BADGE_NUMBER'] == badge]['OFFICER_DISPLAY_NAME'].unique()
                print(f"   Badge {badge}: {officer_count} officers -> {list(officers)}")
        else:
            print("✅ No badge conflicts found - each badge maps to exactly one officer")
        
        # 8. Save corrected data
        output_path = base_path / "03_Staging" / "Summons"
        output_path.mkdir(parents=True, exist_ok=True)
        
        output_file = output_path / "corrected_assignment_data.xlsx"
        court_df.to_excel(output_file, index=False)
        
        print(f"\n💾 Corrected data saved to: {output_file}")
        
        # 9. Generate summary report
        summary_stats = {
            'total_records': len(court_df),
            'successful_matches': match_count,
            'match_rate': match_rate,
            'unique_officers': court_df['OFFICER_DISPLAY_NAME'].nunique(),
            'unique_badges': court_df['PADDED_BADGE_NUMBER'].nunique()
        }
        
        print(f"\n📊 FINAL SUMMARY:")
        for key, value in summary_stats.items():
            print(f"   {key}: {value}")
        
        return court_df, badge_lookup
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return None, None

if __name__ == "__main__":
    corrected_data, lookup = fix_badge_assignment_mapping()