# 🕒 2025-07-13-20-15-00
# Project: SummonsMaster/badge_assignment_fix_JUNE_2025_CORRECTED.py
# Author: R. A. Carucci
# Purpose: Fix Andres Lopez assignment in June 2025 data

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

def fix_june_2025_assignments():
    print("🔧 FIXING JUNE 2025 ASSIGNMENT MAPPING")
    print("=" * 50)
    
    base_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack")
    
    # File paths
    assignment_file = base_path / "_Hackensack_Data_Repository" / "ASSIGNED_SHIFT" / "Assignment_Master_V2.xlsx"
    court_file = base_path / "03_Staging" / "Summons" / "summons_powerbi_final_with_real_june.xlsx"
    
    print(f"📋 Assignment Master: {assignment_file.exists()}")
    print(f"📊 June Court Data: {court_file.exists()}")
    
    try:
        # 1. Load Assignment Master V2 (corrected assignments)
        print("\n📋 Loading Assignment Master V2...")
        assignment_df = pd.read_excel(assignment_file)
        print(f"✅ Assignment data loaded: {len(assignment_df)} officers")
        
        # 2. Create badge lookup dictionary
        print("🔗 Creating corrected badge lookup...")
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
            
            # Create badge variations for matching
            if raw_badge and raw_badge != 'nan':
                badge_formats = [
                    raw_badge,                                           # 375
                    raw_badge.zfill(4),                                 # 0375
                    str(int(float(raw_badge))).zfill(4) if raw_badge.replace('.','').isdigit() else raw_badge
                ]
                
                for badge_format in set(badge_formats):
                    if badge_format:
                        badge_lookup[badge_format] = assignment_info
        
        print(f"✅ Badge lookup created: {len(badge_lookup)} mappings")
        
        # Check Andres Lopez in Assignment Master
        print(f"\n🔍 ANDRES LOPEZ IN ASSIGNMENT MASTER:")
        for badge, info in badge_lookup.items():
            if 'LOPEZ' in info['OFFICER_DISPLAY_NAME'].upper() and 'ANDRES' in info['OFFICER_DISPLAY_NAME'].upper():
                print(f"   Badge {badge} → {info['OFFICER_DISPLAY_NAME']} → {info['WG2']}")
        
        # 3. Load June 2025 court data
        print(f"\n📊 Loading June 2025 court data...")
        court_df = pd.read_excel(court_file, skiprows=0)
        
        # Filter for June 2025
        court_df['ISSUE_DATE'] = pd.to_datetime(court_df['ISSUE_DATE'], errors='coerce')
        june_2025_data = court_df[
            (court_df['ISSUE_DATE'].dt.month == 6) & 
            (court_df['ISSUE_DATE'].dt.year == 2025)
        ].copy()
        
        print(f"✅ June 2025 data: {len(june_2025_data)} records")
        
        # 4. Show current Andres Lopez assignment (BEFORE correction)
        lopez_before = june_2025_data[
            june_2025_data['OFFICER_DISPLAY_NAME'].str.contains('LOPEZ', na=False, case=False) &
            june_2025_data['OFFICER_DISPLAY_NAME'].str.contains('ANDRES', na=False, case=False)
        ]
        
        print(f"\n🔍 ANDRES LOPEZ BEFORE CORRECTION:")
        if len(lopez_before) > 0:
            current_assignment = lopez_before['WG2'].iloc[0]
            badge_num = lopez_before['PADDED_BADGE_NUMBER'].iloc[0]
            print(f"   Badge: {badge_num}")
            print(f"   Name: {lopez_before['OFFICER_DISPLAY_NAME'].iloc[0]}")
            print(f"   Current Assignment: {current_assignment}")
            
            if 'SCHOOL' in current_assignment.upper():
                print(f"   ❌ PROBLEM CONFIRMED: Shows School threat assignment!")
            else:
                print(f"   ✅ Assignment already correct")
        
        # 5. Apply assignment corrections
        print(f"\n🔧 Applying assignment corrections...")
        
        corrections_made = 0
        lopez_corrections = []
        
        for idx, row in june_2025_data.iterrows():
            badge = str(row['PADDED_BADGE_NUMBER']).strip()
            
            # Try different badge formats for lookup
            badge_attempts = [
                badge,
                badge.lstrip('0'),
                badge.zfill(4),
                str(int(badge)) if badge.isdigit() else badge
            ]
            
            for attempt_badge in badge_attempts:
                if attempt_badge in badge_lookup:
                    correct_assignment = badge_lookup[attempt_badge]
                    
                    # Check if assignment needs correction
                    current_wg2 = row['WG2']
                    correct_wg2 = correct_assignment['WG2']
                    
                    if current_wg2 != correct_wg2:
                        # Apply correction
                        june_2025_data.at[idx, 'OFFICER_DISPLAY_NAME'] = correct_assignment['OFFICER_DISPLAY_NAME']
                        june_2025_data.at[idx, 'WG1'] = correct_assignment['WG1']
                        june_2025_data.at[idx, 'WG2'] = correct_assignment['WG2']
                        june_2025_data.at[idx, 'WG3'] = correct_assignment['WG3']
                        june_2025_data.at[idx, 'WG4'] = correct_assignment['WG4']
                        june_2025_data.at[idx, 'WG5'] = correct_assignment['WG5']
                        
                        corrections_made += 1
                        
                        # Track Lopez corrections specifically
                        if 'LOPEZ' in correct_assignment['OFFICER_DISPLAY_NAME'].upper() and 'ANDRES' in correct_assignment['OFFICER_DISPLAY_NAME'].upper():
                            lopez_corrections.append({
                                'badge': badge,
                                'old_assignment': current_wg2,
                                'new_assignment': correct_wg2
                            })
                    
                    break
        
        print(f"✅ Corrections applied: {corrections_made} records updated")
        
        # 6. Show Andres Lopez AFTER correction
        print(f"\n🔍 ANDRES LOPEZ AFTER CORRECTION:")
        if lopez_corrections:
            for correction in lopez_corrections:
                print(f"   Badge {correction['badge']}:")
                print(f"      OLD: {correction['old_assignment']}")
                print(f"      NEW: {correction['new_assignment']}")
                
                if 'SCHOOL' in correction['old_assignment'].upper() and 'PATROL' in correction['new_assignment'].upper():
                    print(f"      🎯 SUCCESS: School threat → Patrol Bureau!")
        
        # Verify final result
        lopez_after = june_2025_data[
            june_2025_data['OFFICER_DISPLAY_NAME'].str.contains('LOPEZ', na=False, case=False) &
            june_2025_data['OFFICER_DISPLAY_NAME'].str.contains('ANDRES', na=False, case=False)
        ]
        
        if len(lopez_after) > 0:
            final_assignment = lopez_after['WG2'].iloc[0]
            print(f"\n✅ FINAL VERIFICATION:")
            print(f"   Andres Lopez final assignment: {final_assignment}")
            
            if 'PATROL' in final_assignment.upper():
                print(f"   🏆 SUCCESS: Andres Lopez now shows Patrol Bureau!")
            else:
                print(f"   ⚠️  Still needs review: {final_assignment}")
        
        # 7. Update metadata
        june_2025_data['ASSIGNMENT_FOUND'] = True
        june_2025_data['PROCESSED_TIMESTAMP'] = datetime.now()
        june_2025_data['ETL_VERSION'] = 'v7.0_June_2025_Assignment_Corrected'
        june_2025_data['DATA_SOURCE'] = 'summons_powerbi_final_with_real_june.xlsx'
        
        # 8. Save corrected June 2025 data
        output_dir = base_path / "03_Staging" / "Summons"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = output_dir / f"CORRECTED_June_2025_Assignments_{timestamp}.xlsx"
        
        june_2025_data.to_excel(output_file, index=False)
        
        print(f"\n💾 CORRECTED JUNE 2025 DATA SAVED:")
        print(f"   📁 File: {output_file}")
        print(f"   📊 Records: {len(june_2025_data):,}")
        print(f"   🔧 Corrections: {corrections_made}")
        
        # 9. Show summary of assignments
        print(f"\n📋 FINAL ASSIGNMENT SUMMARY:")
        assignment_counts = june_2025_data['WG2'].value_counts()
        for assignment, count in assignment_counts.head(5).items():
            print(f"   {assignment}: {count} records")
        
        print(f"\n🚀 READY FOR M-CODE UPDATE!")
        print(f"   Use file: {output_file.name}")
        
        return output_file
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    result = fix_june_2025_assignments()
    
    if result:
        print(f"\n🎯 NEXT STEPS:")
        print(f"1. Update your M-Code file path to use: {result.name}")
        print(f"2. Refresh your Power BI query")
        print(f"3. Check DAX measure - should show 'P.O. ANDRES LOPEZ (Patrol Bureau)'")
        print(f"4. June 2025 assignment fix complete!")
    else:
        print(f"\n💡 Check error messages above for troubleshooting")