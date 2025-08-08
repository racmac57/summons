# 🕒 2025-07-10-16-20-00
# Project: SummonsMaster/Badge_Diagnostic
# Author: R. A. Carucci
# Purpose: Debug badge number matching between court exports and Assignment Master

import pandas as pd
from pathlib import Path

def debug_badge_matching():
    """Debug why badge numbers aren't matching"""
    
    print("🔍 BADGE MATCHING DIAGNOSTIC")
    print("=" * 50)
    
    try:
        # Load Assignment Master
        assignment_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\_Hackensack_Data_Repository\ASSIGNED_SHIFT\Assignment_Master_V2.xlsx")
        
        print(f"📋 Loading Assignment Master from: {assignment_path}")
        assignment_df = pd.read_excel(assignment_path, sheet_name=0)
        
        print(f"✅ Assignment Master loaded: {len(assignment_df)} rows")
        print(f"📊 Assignment Master columns: {list(assignment_df.columns)}")
        
        # Show sample assignment data
        print("\n📋 ASSIGNMENT MASTER SAMPLE:")
        print("Badge columns in Assignment Master:")
        for col in assignment_df.columns:
            if 'badge' in col.lower() or 'number' in col.lower():
                print(f"   {col}: {assignment_df[col].head(5).tolist()}")
        
        # Load one court file to compare
        court_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\05_EXPORTS\_Summons\Court\25_06_ATS.xlsx")
        
        print(f"\n📋 Loading Court Export from: {court_path}")
        court_df = pd.read_excel(court_path, skiprows=4, header=None)
        
        # Assign basic column names
        court_df.columns = ['BADGE_NUMBER_RAW', 'OFFICER_NAME_RAW'] + [f'Col_{i}' for i in range(2, len(court_df.columns))]
        
        print(f"✅ Court Export loaded: {len(court_df)} rows")
        
        # Show sample court badges
        print("\n📋 COURT EXPORT SAMPLE:")
        print("Raw badges from court export:")
        unique_badges = court_df['BADGE_NUMBER_RAW'].dropna().astype(str).unique()[:20]
        print(f"   Sample badges: {list(unique_badges)}")
        
        # Clean court badges
        court_df['BADGE_CLEAN'] = court_df['BADGE_NUMBER_RAW'].astype(str).str.strip()
        court_df['PADDED_BADGE'] = court_df['BADGE_CLEAN'].str.zfill(4)
        
        print(f"\n🔧 BADGE PROCESSING:")
        badge_examples = court_df[['BADGE_NUMBER_RAW', 'BADGE_CLEAN', 'PADDED_BADGE']].head(10)
        print(badge_examples.to_string(index=False))
        
        # Create assignment lookup variations
        print(f"\n🔗 TESTING ASSIGNMENT LOOKUP:")
        
        # Get potential badge columns from assignment master
        badge_columns = []
        for col in assignment_df.columns:
            if 'badge' in col.lower() or 'number' in col.lower():
                badge_columns.append(col)
        
        print(f"Badge-related columns in Assignment Master: {badge_columns}")
        
        # Test different matching strategies
        test_badges = ['83', '0083', '138', '0138', '386', '0386']
        
        for badge_col in badge_columns:
            print(f"\n   Testing column '{badge_col}':")
            assignment_badges = assignment_df[badge_col].dropna().astype(str).str.strip()
            
            for test_badge in test_badges:
                # Test exact match
                exact_match = assignment_badges[assignment_badges == test_badge]
                if len(exact_match) > 0:
                    print(f"      ✅ Found exact match for '{test_badge}' in {badge_col}")
                    
                # Test padded match
                padded_match = assignment_badges[assignment_badges == test_badge.zfill(4)]
                if len(padded_match) > 0:
                    print(f"      ✅ Found padded match for '{test_badge}' -> '{test_badge.zfill(4)}' in {badge_col}")
                    
                # Test unpadded match
                unpadded_match = assignment_badges[assignment_badges == str(int(test_badge))]
                if len(unpadded_match) > 0:
                    print(f"      ✅ Found unpadded match for '{test_badge}' -> '{str(int(test_badge))}' in {badge_col}")
        
        # Show assignment master badge values
        print(f"\n📊 ASSIGNMENT MASTER BADGE ANALYSIS:")
        for badge_col in badge_columns:
            if badge_col in assignment_df.columns:
                badge_values = assignment_df[badge_col].dropna().astype(str).str.strip().unique()[:15]
                print(f"   {badge_col} sample values: {list(badge_values)}")
        
        # Count potential matches
        print(f"\n🎯 MATCH ANALYSIS:")
        court_badges = set(court_df['PADDED_BADGE'].dropna().unique())
        
        for badge_col in badge_columns:
            if badge_col in assignment_df.columns:
                assignment_badges = set(assignment_df[badge_col].dropna().astype(str).str.strip().str.zfill(4))
                matches = court_badges.intersection(assignment_badges)
                print(f"   Using {badge_col}: {len(matches)} matches out of {len(court_badges)} court badges")
                if len(matches) > 0:
                    print(f"      Sample matches: {list(matches)[:10]}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error in diagnostic: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    debug_badge_matching()