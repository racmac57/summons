# 🕒 2025-07-13-14-45-00
# Project: SummonsMaster/badge_assignment_fix_COMPLETE.py
# Author: R. A. Carucci
# Purpose: Complete ETL fix for badge assignment mapping with execution instructions

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import sys

def main():
    """
    EXECUTION INSTRUCTIONS:
    
    1. Open Command Prompt or PowerShell
    2. Navigate to script directory:
       cd "C:\Users\carucci_r\OneDrive - City of Hackensack\02_ETL_Scripts\Summons"
    
    3. Run the script:
       python badge_assignment_fix.py
    
    4. Or run from Python/VS Code:
       exec(open('badge_assignment_fix.py').read())
    """
    
    print("🚀 STARTING BADGE ASSIGNMENT FIX")
    print("=" * 60)
    
    # File paths - auto-detect your environment
    base_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack")
    
    # Try multiple assignment file locations
    assignment_files = [
        base_path / "_ASSIGNMENTS" / "Assignment_Master_V2.xlsx",
        base_path / "_Hackensack_Data_Repository" / "ASSIGNED_SHIFT" / "Assignment_Master_V2.xlsx",
        base_path / "Assignment_Master_V2.xlsx"  # Current directory
    ]
    
    assignment_file = None
    for file_path in assignment_files:
        if file_path.exists():
            assignment_file = file_path
            break
    
    if not assignment_file:
        print("❌ Assignment_Master_V2.xlsx not found!")
        print("📁 Checked locations:")
        for file_path in assignment_files:
            print(f"   {file_path}")
        print("\n💡 Please ensure Assignment_Master_V2.xlsx is in one of these locations")
        return False
    
    # Try multiple court file locations  
    court_files = [
        base_path / "_MONTHLY_DATA" / "_EXPORTS" / "Summons" / "Court" / "2025_04_ATS_Report.xlsx",
        base_path / "05_EXPORTS" / "_Summons" / "Court" / "25_06_ATS.xlsx",
        base_path / "2025_04_ATS_Report.xlsx"  # Current directory
    ]
    
    court_file = None
    for file_path in court_files:
        if file_path.exists():
            court_file = file_path
            break
    
    if not court_file:
        print("❌ Court data file not found!")
        print("📁 Checked locations:")
        for file_path in court_files:
            print(f"   {file_path}")
        print("\n💡 Please ensure a court export file is available")
        return False
    
    print(f"✅ Assignment file found: {assignment_file}")
    print(f"✅ Court file found: {court_file}")
    
    try:
        # 1. Load Assignment Master V2
        print("\n📋 Loading Assignment Master V2...")
        assignment_df = pd.read_excel(assignment_file)
        
        print(f"✅ Assignment data loaded: {len(assignment_df)} officers")
        print(f"📋 Columns available: {list(assignment_df.columns)}")
        
        # Identify correct column names (handle variations)
        badge_col = None
        padded_badge_col = None
        name_col = None
        
        for col in assignment_df.columns:
            col_lower = str(col).lower()
            if 'badge' in col_lower and 'padded' not in col_lower:
                badge_col = col
            elif 'padded' in col_lower and 'badge' in col_lower:
                padded_badge_col = col
            elif any(x in col_lower for x in ['full_name', 'name', 'officer']):
                name_col = col
        
        print(f"📋 Using columns: Badge={badge_col}, Padded={padded_badge_col}, Name={name_col}")
        
        # 2. Create comprehensive badge lookup
        print("🔗 Creating badge lookup dictionary...")
        badge_lookup = {}
        
        for _, row in assignment_df.iterrows():
            # Get badge number in multiple formats
            raw_badge = str(row.get(badge_col, '')).strip() if badge_col else ''
            padded_badge = str(row.get(padded_badge_col, '')).strip() if padded_badge_col else ''
            officer_name = str(row.get(name_col, '')).strip() if name_col else ''
            
            # Skip invalid entries
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
            
            if padded_badge and padded_badge != 'nan':
                badge_formats.append(padded_badge)
            
            # Remove duplicates
            badge_formats = list(set([b for b in badge_formats if b and b != 'nan']))
            
            # Add to lookup
            for badge_format in badge_formats:
                badge_lookup[badge_format] = assignment_info
        
        print(f"✅ Badge lookup created: {len(badge_lookup)} badge entries for {len(assignment_df)} officers")
        
        # Show sample lookups
        print("📋 Sample badge lookups:")
        for i, (badge, info) in enumerate(list(badge_lookup.items())[:5]):
            print(f"   {badge} → {info['OFFICER_DISPLAY_NAME']} ({info['WG2']})")
        
        # 3. Load court data
        print(f"\n📊 Loading court data from: {court_file.name}")
        
        # Read court export (skip metadata rows)
        court_df = pd.read_excel(court_file, skiprows=4)
        
        # Clean column names
        court_df.columns = [str(col).replace('\r\n', '_').replace('\n', '_') for col in court_df.columns]
        
        # Identify badge column (usually first column or unnamed)
        badge_col_name = court_df.columns[0]
        if 'Unnamed' in str(badge_col_name) or badge_col_name == 0:
            court_df = court_df.rename(columns={badge_col_name: 'BADGE_NUMBER'})
            badge_col_name = 'BADGE_NUMBER'
        
        print(f"✅ Court data loaded: {len(court_df)} records")
        print(f"📋 Badge column: {badge_col_name}")
        
        # 4. Clean court badge numbers
        court_df['BADGE_CLEAN'] = court_df[badge_col_name].astype(str).str.strip()
        
        # Remove invalid badges
        invalid_badges = ['0', '0000', 'nan', 'None', '', 'TOTAL', 'Total', 'SUMMARY']
        initial_count = len(court_df)
        court_df = court_df[~court_df['BADGE_CLEAN'].isin(invalid_badges)]
        court_df = court_df[court_df['BADGE_CLEAN'].notna()]
        
        print(f"📊 Cleaned data: {len(court_df)} records (removed {initial_count - len(court_df)} invalid)")
        print(f"📋 Unique badges: {court_df['BADGE_CLEAN'].nunique()}")
        print(f"📋 Sample badges: {sorted(court_df['BADGE_CLEAN'].unique())[:10]}")
        
        # 5. Apply assignment matching
        print("\n🔗 Applying assignment matching...")
        
        # Initialize assignment columns
        court_df['PADDED_BADGE_NUMBER'] = court_df['BADGE_CLEAN'].str.zfill(4)
        court_df['OFFICER_DISPLAY_NAME'] = ''
        court_df['WG1'] = ''
        court_df['WG2'] = ''
        court_df['WG3'] = ''
        court_df['WG4'] = ''
        court_df['WG5'] = ''
        court_df['ASSIGNMENT_FOUND'] = False
        
        # Apply assignments
        match_count = 0
        officer_stats = {}
        
        for idx, row in court_df.iterrows():
            badge = row['BADGE_CLEAN']
            matched = False
            
            # Try multiple badge formats
            badge_attempts = [
                badge,                                           # Original: 0083
                badge.lstrip('0'),                              # No leading zeros: 83
                badge.zfill(4),                                 # 4-digit: 0083
                str(int(badge)) if badge.isdigit() else badge, # Clean int: 83
                str(int(badge)).zfill(4) if badge.isdigit() else badge  # Clean + pad: 0083
            ]
            
            # Remove duplicates
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
                    
                    # Track officer stats
                    officer_name = assignment['OFFICER_DISPLAY_NAME']
                    if officer_name not in officer_stats:
                        officer_stats[officer_name] = {'count': 0, 'assignment': assignment['WG2']}
                    officer_stats[officer_name]['count'] += 1
                    
                    matched = True
                    break
        
        # 6. Results summary
        match_rate = (match_count / len(court_df)) * 100
        print(f"\n✅ ASSIGNMENT MATCHING COMPLETE!")
        print(f"📊 Results:")
        print(f"   • Total records: {len(court_df):,}")
        print(f"   • Successful matches: {match_count:,}")
        print(f"   • Match rate: {match_rate:.1f}%")
        print(f"   • Unique officers matched: {len(officer_stats)}")
        
        # Show top officers by activity
        print(f"\n📋 Top 10 most active officers:")
        sorted_officers = sorted(officer_stats.items(), key=lambda x: x[1]['count'], reverse=True)[:10]
        for officer, stats in sorted_officers:
            print(f"   {officer}: {stats['count']} tickets ({stats['assignment']})")
        
        # 7. Check for specific officers mentioned in issue
        print(f"\n🔍 Checking specific officers...")
        
        # Check for Andres Lopez
        lopez_check = court_df[court_df['OFFICER_DISPLAY_NAME'].str.contains('LOPEZ|ANDRES', na=False, case=False)]
        if len(lopez_check) > 0:
            lopez_assignments = lopez_check[['WG2', 'WG3']].drop_duplicates()
            print(f"   ✅ Andres Lopez found: {len(lopez_check)} records")
            print(f"   📋 Assignments: {lopez_assignments.to_dict('records')}")
        else:
            print(f"   ❌ Andres Lopez not found in matched data")
        
        # Check for badge conflicts (original issue)
        print(f"\n🔍 Checking for badge mapping conflicts...")
        badge_conflicts = court_df.groupby('PADDED_BADGE_NUMBER')['OFFICER_DISPLAY_NAME'].nunique()
        conflicts = badge_conflicts[badge_conflicts > 1]
        
        if len(conflicts) > 0:
            print(f"   ⚠️  Found {len(conflicts)} badges with multiple officers:")
            for badge, count in conflicts.head().items():
                officers = court_df[court_df['PADDED_BADGE_NUMBER'] == badge]['OFFICER_DISPLAY_NAME'].unique()
                print(f"      Badge {badge}: {list(officers)}")
        else:
            print(f"   ✅ No badge conflicts - each badge maps to exactly one officer")
        
        # 8. Save corrected data
        output_dir = base_path / "03_Staging" / "Summons"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = output_dir / f"corrected_assignment_data_{timestamp}.xlsx"
        
        # Add metadata
        court_df['PROCESSED_TIMESTAMP'] = datetime.now()
        court_df['ETL_VERSION'] = 'badge_fix_v1.0'
        court_df['DATA_SOURCE'] = court_file.name
        
        # Save to Excel
        court_df.to_excel(output_file, index=False)
        print(f"\n💾 Corrected data saved to:")
        print(f"   {output_file}")
        
        # 9. Create Power BI ready file
        powerbi_columns = [
            'PADDED_BADGE_NUMBER', 'OFFICER_DISPLAY_NAME',
            'WG1', 'WG2', 'WG3', 'WG4', 'WG5',
            'ASSIGNMENT_FOUND', 'PROCESSED_TIMESTAMP', 'ETL_VERSION'
        ]
        
        # Add court data columns that exist
        additional_cols = []
        for col in court_df.columns:
            if any(keyword in col.upper() for keyword in ['TICKET', 'ISSUE', 'DATE', 'TYPE', 'AMOUNT', 'VIOLATION']):
                additional_cols.append(col)
        
        final_columns = powerbi_columns + additional_cols
        final_columns = [col for col in final_columns if col in court_df.columns]
        
        powerbi_df = court_df[final_columns].copy()
        powerbi_file = output_dir / f"summons_powerbi_ready_{timestamp}.xlsx"
        powerbi_df.to_excel(powerbi_file, index=False)
        
        print(f"💾 Power BI file saved to:")
        print(f"   {powerbi_file}")
        
        print(f"\n🎯 SUMMARY:")
        print(f"   ✅ Fixed badge assignment mapping")
        print(f"   ✅ {match_rate:.1f}% assignment success rate")
        print(f"   ✅ {len(officer_stats)} unique officers identified")
        print(f"   ✅ Data ready for Power BI import")
        
        if match_rate > 85:
            print(f"\n🏆 SUCCESS: High assignment match rate achieved!")
        else:
            print(f"\n⚠️  WARNING: Low match rate - check assignment master data")
        
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    
    if success:
        print(f"\n🚀 NEXT STEPS:")
        print(f"   1. Import Power BI file to update dashboard")
        print(f"   2. Update M-Code queries with corrected data")
        print(f"   3. Verify Andres Lopez shows correct assignment")
        print(f"   4. Test DAX measures with new data")
    else:
        print(f"\n💡 TROUBLESHOOTING:")
        print(f"   1. Check file paths exist")
        print(f"   2. Ensure Assignment_Master_V2.xlsx is accessible") 
        print(f"   3. Verify court export file format")
        print(f"   4. Run script with admin privileges if needed")
