# 🕒 2025-07-13-20-10-00
# Project: SummonsMaster/badge_assignment_fix_JUNE_2025.py
# Author: R. A. Carucci
# Purpose: Fix badge assignment mapping for June 2025 data

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

def fix_june_2025_badge_assignments():
    print("🔧 FIXING BADGE ASSIGNMENT MAPPING - JUNE 2025 DATA")
    print("=" * 60)
    
    base_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack")
    
    # Use the CORRECT files for June 2025
    assignment_file = base_path / "_Hackensack_Data_Repository" / "ASSIGNED_SHIFT" / "Assignment_Master_V2.xlsx"
    court_file = base_path / "03_Staging" / "Summons" / "summons_powerbi_final_with_real_june.xlsx"  # JUNE DATA!
    
    print(f"📋 Assignment file: {assignment_file}")
    print(f"   Exists: {'✅' if assignment_file.exists() else '❌'}")
    
    print(f"📊 Court file: {court_file}")
    print(f"   Exists: {'✅' if court_file.exists() else '❌'}")
    
    if not assignment_file.exists() or not court_file.exists():
        print("❌ Required files not found!")
        return False
        
    try:
        # 1. Load Assignment Master V2
        print("\n📋 Loading Assignment Master V2...")
        assignment_df = pd.read_excel(assignment_file)
        print(f"✅ Assignment data loaded: {len(assignment_df)} officers")
        
        # 2. Create badge lookup dictionary
        print("🔗 Creating badge lookup...")
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
        
        # 3. Load June court data (NO skip rows - data starts at row 1)
        print(f"\n📊 Loading June 2025 court data...")
        court_df = pd.read_excel(court_file, skiprows=0)  # No skip rows for this file
        
        print(f"✅ Court data loaded: {len(court_df)} records")
        print(f"📋 Columns: {list(court_df.columns[:5])}")
        
        # 4. Filter for June 2025 data specifically
        print("\n🗓️ Filtering for June 2025...")
        court_df['ISSUE_DATE'] = pd.to_datetime(court_df['ISSUE_DATE'], errors='coerce')
        
        june_2025_data = court_df[
            (court_df['ISSUE_DATE'].dt.month == 6) & 
            (court_df['ISSUE_DATE'].dt.year == 2025)
        ]
        
        print(f"📊 June 2025 records: {len(june_2025_data)}")
        
        if len(june_2025_data) == 0:
            print("❌ No June 2025 data found after filtering!")
            return False
        
        # 5. Check current badge column and assignments
        badge_col = None
        for col in june_2025_data.columns:
            if 'BADGE' in col.upper() and 'PADDED' not in col.upper():
                badge_col = col
                break
        
        if not badge_col:
            print("❌ No badge column found!")
            return False
        
        print(f"📋 Using badge column: {badge_col}")
        
        # Show sample current assignments
        print(f"\n📋 Current assignment sample:")
        sample_data = june_2025_data[[badge_col, 'OFFICER_DISPLAY_NAME', 'WG2']].head(3)
        for _, row in sample_data.iterrows():
            print(f"   Badge {row[badge_col]} → {row['OFFICER_DISPLAY_NAME']} → {row['WG2']}")
        
        # 6. Check for Andres Lopez BEFORE correction
        lopez_before = june_2025_data[
            june_2025_data['OFFICER_DISPLAY_NAME'].str.contains('LOPEZ', na=False, case=False) &
            june_2025_data['OFFICER_DISPLAY_NAME'].str.contains('ANDRES', na=False, case=False)
        ]
        
        print(f"\n🔍 ANDRES LOPEZ BEFORE CORRECTION:")
        if len(lopez_before) > 0:
            lopez_assignment = lopez_before['WG2'].iloc[0]
            print(f"   ✅ Found: {lopez_before['OFFICER_DISPLAY_NAME'].iloc[0]}")
            print(f"   📋 Current assignment: {lopez_assignment}")
            if 'SCHOOL' in lopez_assignment.upper():
                print(f"   ❌ PROBLEM: Shows School threat assignment!")
            else:
                print(f"   ✅ Assignment looks correct")
        else:
            print(f"   ❌ Andres Lopez not found in June 2025 data")
        
        # 7. Apply corrected assignments
        print(f"\n🔗 Applying corrected assignments...")
        
        # Clean badge numbers
        june_2025_data['BADGE_CLEAN'] = june_2025_data[badge_col].astype(str).str.strip()
        june_2025_data = june_2025_data[~june_2025_data['BADGE_CLEAN'].isin(['0', '0000', 'nan', 'None', '', 'TOTAL'])]
        
        # Initialize new assignment columns
        june_2025_data['CORRECTED_OFFICER_DISPLAY_NAME'] = ''
        june_2025_data['CORRECTED_WG1'] = ''
        june_2025_data['CORRECTED_WG2'] = ''
        june_2025_data['CORRECTED_WG3'] = ''
        june_2025_data['CORRECTED_WG4'] = ''
        june_2025_data['CORRECTED_WG5'] = ''
        june_2025_data['ASSIGNMENT_CORRECTED'] = False
        
        match_count = 0
        lopez_matches = []
        
        for idx, row in june_2025_data.iterrows():
            badge = row['BADGE_CLEAN']
            
            # Try badge variations
            badge_attempts = [
                badge,
                badge.lstrip('0'),
                badge.zfill(4),
                str(int(badge)) if badge.isdigit() else badge
            ]
            
            for attempt_badge in badge_attempts:
                if attempt_badge in badge_lookup:
                    assignment = badge_lookup[attempt_badge]
                    
                    june_2025_data.at[idx, 'CORRECTED_OFFICER_DISPLAY_NAME'] = assignment['OFFICER_DISPLAY_NAME']
                    june_2025_data.at[idx, 'CORRECTED_WG1'] = assignment['WG1']
                    june_2025_data.at[idx, 'CORRECTED_WG2'] = assignment['WG2']
                    june_2025_data.at[idx, 'CORRECTED_WG3'] = assignment['WG3']
                    june_2025_data.at[idx, 'CORRECTED_WG4'] = assignment['WG4']
                    june_2025_data.at[idx, 'CORRECTED_WG5'] = assignment['WG5']
                    june_2025_data.at[idx, 'ASSIGNMENT_CORRECTED'] = True
                    
                    match_count += 1
                    
                    if 'LOPEZ' in assignment['OFFICER_DISPLAY_NAME'].upper() and 'ANDRES' in assignment['OFFICER_DISPLAY_NAME'].upper():
                        lopez_matches.append({
                            'badge': badge,
                            'old_assignment': row.get('WG2', 'Unknown'),
                            'new_assignment': assignment['WG2']
                        })
                    
                    break
        
        # 8. Results summary
        match_rate = (match_count / len(june_2025_data)) * 100
        print(f"\n✅ CORRECTION RESULTS:")
        print(f"   • Total June 2025 records: {len(june_2025_data):,}")
        print(f"   • Successfully corrected: {match_count:,}")
        print(f"   • Correction rate: {match_rate:.1f}%")
        
        # 9. Check Andres Lopez AFTER correction
        print(f"\n🔍 ANDRES LOPEZ AFTER CORRECTION:")
        if lopez_matches:
            for match in lopez_matches:
                print(f"   ✅ Badge {match['badge']}")
                print(f"      OLD: {match['old_assignment']}")
                print(f"      NEW: {match['new_assignment']}")
                
                if 'SCHOOL' in match['old_assignment'].upper() and 'PATROL' in match['new_assignment'].upper():
                    print(f"      🎯 FIXED: School threat → Patrol Bureau!")
        else:
            print(f"   ❌ Andres Lopez not found in corrected data")
        
        # 10. Update original columns with corrected data
        print(f"\n🔄 Updating original data with corrections...")
        
        # Replace original assignment columns with corrected ones where available
        june_2025_data['OFFICER_DISPLAY_NAME'] = june_2025_data.apply(
            lambda row: row['CORRECTED_OFFICER_DISPLAY_NAME'] if row['ASSIGNMENT_CORRECTED'] else row['OFFICER_DISPLAY_NAME'], axis=1
        )
        june_2025_data['WG1'] = june_2025_data.apply(
            lambda row: row['CORRECTED_WG1'] if row['ASSIGNMENT_CORRECTED'] else row['WG1'], axis=1
        )
        june_2025_data['WG2'] = june_2025_data.apply(
            lambda row: row['CORRECTED_WG2'] if row['ASSIGNMENT_CORRECTED'] else row['WG2'], axis=1
        )
        june_2025_data['WG3'] = june_2025_data.apply(
            lambda row: row['CORRECTED_WG3'] if row['ASSIGNMENT_CORRECTED'] else row['WG3'], axis=1
        )
        june_2025_data['WG4'] = june_2025_data.apply(
            lambda row: row['CORRECTED_WG4'] if row['ASSIGNMENT_CORRECTED'] else row['WG4'], axis=1
        )
        june_2025_data['WG5'] = june_2025_data.apply(
            lambda row: row['CORRECTED_WG5'] if row['ASSIGNMENT_CORRECTED'] else row['WG5'], axis=1
        )
        
        # Add metadata
        june_2025_data['ASSIGNMENT_FOUND'] = june_2025_data['ASSIGNMENT_CORRECTED']
        june_2025_data['PROCESSED_TIMESTAMP'] = datetime.now()
        june_2025_data['ETL_VERSION'] = 'v7.0_June_2025_Corrected'
        june_2025_data['DATA_SOURCE'] = 'summons_powerbi_final_with_real_june.xlsx'
        
        # 11. Save corrected June 2025 data
        output_dir = base_path / "03_Staging" / "Summons"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = output_dir / f"CORRECTED_June_2025_Assignment_Data_{timestamp}.xlsx"
        
        june_2025_data.to_excel(output_file, index=False)
        print(f"\n💾 Corrected June 2025 data saved to:")
        print(f"   {output_file}")
        
        # 12. Final verification
        final_lopez = june_2025_data[
            june_2025_data['OFFICER_DISPLAY_NAME'].str.contains('LOPEZ', na=False, case=False) &
            june_2025_data['OFFICER_DISPLAY_NAME'].str.contains('ANDRES', na=False, case=False)
        ]
        
        print(f"\n🎯 FINAL VERIFICATION:")
        if len(final_lopez) > 0:
            final_assignment = final_lopez['WG2'].iloc[0]
            print(f"   ✅ Andres Lopez final assignment: {final_assignment}")
            if 'PATROL' in final_assignment.upper():
                print(f"   🏆 SUCCESS: Andres Lopez now shows Patrol Bureau!")
            else:
                print(f"   ⚠️  Check: Assignment may still need review")
        
        print(f"\n🚀 READY FOR M-CODE UPDATE!")
        print(f"   📁 File to use: {output_file.name}")
        
        return output_file
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    result = fix_june_2025_badge_assignments()
    
    if result:
        print(f"\n🎯 NEXT STEPS:")
        print(f"1. Update M-Code file path to: {result.name}")
        print(f"2. Refresh Power BI query")
        print(f"3. Verify DAX shows: 'P.O. ANDRES LOPEZ (Patrol Bureau)'")
        print(f"4. Assignment fix complete for June 2025!")
    else:
        print(f"\n💡 TROUBLESHOOTING NEEDED")