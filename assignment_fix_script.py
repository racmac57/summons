# 🕒 2025-07-09-19-50-00
# Project: SummonsMaster/assignment_integration_fix.py
# Author: R. A. Carucci
# Purpose: Emergency fix for assignment data integration in existing summons file

import pandas as pd
import numpy as np
from pathlib import Path

def fix_assignment_integration():
    """
    Quick fix to add assignment data to existing summons_powerbi_latest.xlsx
    """
    
    print("🚨 EMERGENCY ASSIGNMENT INTEGRATION FIX")
    print("=" * 60)
    
    # File paths
    base_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack")
    summons_file = base_path / "03_Staging" / "Summons" / "summons_powerbi_latest.xlsx"
    assignment_file = base_path / "02_Staging" / "Summons" / "Assignment_Master.xlsm"
    
    try:
        # Step 1: Load existing summons data
        print("📊 Loading existing summons data...")
        summons_df = pd.read_excel(summons_file, sheet_name='Police_Analytics_Data')
        print(f"✅ Loaded {len(summons_df):,} summons records")
        
        # Check current badge format
        print(f"Sample badges: {summons_df['Badge Number'].head().tolist()}")
        print(f"Sample padded badges: {summons_df['PADDED_BADGE_NUMBER'].head().tolist()}")
        
        # Step 2: Load Assignment Master
        print("\n📋 Loading Assignment Master...")
        assign_df = pd.read_excel(assignment_file, sheet_name='Assignment_Master')
        print(f"✅ Loaded {len(assign_df)} officer records")
        
        # Clean assignment data
        assign_df['BADGE_CLEAN'] = assign_df['BADGE_NUMBER'].astype(str).str.strip()
        
        # Show sample assignment data
        print("Sample assignment data:")
        sample_assigns = assign_df[['BADGE_NUMBER', 'FIRST_NAME', 'LAST_NAME', 'TEAM', 'WG1', 'WG2']].head()
        print(sample_assigns.to_string())
        
        # Step 3: Clean badge numbers for matching
        print("\n🔧 Cleaning badge numbers for matching...")
        
        # Clean summons badges - convert to string and remove padding
        summons_df['BADGE_CLEAN'] = summons_df['Badge Number'].astype(str).str.strip()
        
        # Test different badge formats
        print("\nTesting badge formats:")
        test_badges = summons_df['BADGE_CLEAN'].unique()[:5]
        for badge in test_badges:
            # Try different formats
            formats = [
                badge,                    # Original: "83"
                badge.zfill(4),          # Padded: "0083"
                str(int(badge)) if badge.isdigit() else badge  # Cleaned int: "83"
            ]
            print(f"Badge {badge} formats: {formats}")
            
            # Check if any format exists in assignment data
            for fmt in formats:
                matches = assign_df[assign_df['BADGE_CLEAN'] == fmt]
                if not matches.empty:
                    print(f"  ✅ Match found for format '{fmt}': {matches['FIRST_NAME'].iloc[0]} {matches['LAST_NAME'].iloc[0]}")
                    break
        
        # Step 4: Perform the join using multiple badge format attempts
        print("\n🔗 Attempting assignment join...")
        
        # Method 1: Direct match on badge number
        merged_df = summons_df.merge(
            assign_df[['BADGE_CLEAN', 'FIRST_NAME', 'LAST_NAME', 'TEAM', 'WG1', 'WG2', 'WG3', 'TITLE']],
            left_on='BADGE_CLEAN',
            right_on='BADGE_CLEAN',
            how='left',
            suffixes=('', '_ASSIGN')
        )
        
        # Check match rate for Method 1
        method1_matches = merged_df['FIRST_NAME'].notna().sum()
        print(f"Method 1 (direct): {method1_matches} matches ({(method1_matches/len(merged_df)*100):.1f}%)")
        
        # Method 2: Try with integer conversion
        if method1_matches < len(merged_df) * 0.5:
            print("Trying Method 2: Integer badge matching...")
            
            # Create integer badge columns
            summons_df['BADGE_INT'] = pd.to_numeric(summons_df['BADGE_CLEAN'], errors='coerce')
            assign_df['BADGE_INT'] = pd.to_numeric(assign_df['BADGE_CLEAN'], errors='coerce')
            
            merged_df = summons_df.merge(
                assign_df[['BADGE_INT', 'FIRST_NAME', 'LAST_NAME', 'TEAM', 'WG1', 'WG2', 'WG3', 'TITLE']],
                on='BADGE_INT',
                how='left',
                suffixes=('', '_ASSIGN')
            )
            
            method2_matches = merged_df['FIRST_NAME'].notna().sum()
            print(f"Method 2 (integer): {method2_matches} matches ({(method2_matches/len(merged_df)*100):.1f}%)")
        
        # Method 3: Try with padded badges
        if method2_matches < len(merged_df) * 0.5:
            print("Trying Method 3: Padded badge matching...")
            
            # Create 4-digit padded badges
            assign_df['BADGE_PADDED'] = assign_df['BADGE_CLEAN'].astype(str).str.zfill(4)
            summons_df['BADGE_PADDED'] = summons_df['BADGE_CLEAN'].astype(str).str.zfill(4)
            
            merged_df = summons_df.merge(
                assign_df[['BADGE_PADDED', 'FIRST_NAME', 'LAST_NAME', 'TEAM', 'WG1', 'WG2', 'WG3', 'TITLE']],
                on='BADGE_PADDED',
                how='left',
                suffixes=('', '_ASSIGN')
            )
            
            method3_matches = merged_df['FIRST_NAME'].notna().sum()
            print(f"Method 3 (padded): {method3_matches} matches ({(method3_matches/len(merged_df)*100):.1f}%)")
        
        # Step 5: Update the dataframe with assignment data
        print("\n📊 Updating dataframe with assignment data...")
        
        # Create full name and hierarchy
        merged_df['FULL_NAME_FIXED'] = (
            merged_df['FIRST_NAME'].fillna('') + ' ' + 
            merged_df['LAST_NAME'].fillna('')
        ).str.strip()
        
        # Replace empty full names with original officer name data
        merged_df['FULL_NAME_FIXED'] = merged_df['FULL_NAME_FIXED'].replace('', pd.NA)
        
        # Create assignment hierarchy
        merged_df['DIVISION_FIXED'] = merged_df['WG1']
        merged_df['BUREAU_FIXED'] = merged_df['WG2'] 
        merged_df['UNIT_FIXED'] = merged_df['WG3']
        merged_df['ASSIGNMENT_FOUND'] = merged_df['FIRST_NAME'].notna()
        
        # Update the original columns
        merged_df['FULL_NAME'] = merged_df['FULL_NAME_FIXED']
        merged_df['DIVISION'] = merged_df['DIVISION_FIXED']
        merged_df['BUREAU'] = merged_df['BUREAU_FIXED']
        
        # Calculate final match rate
        final_matches = merged_df['ASSIGNMENT_FOUND'].sum()
        match_rate = (final_matches / len(merged_df)) * 100
        
        print(f"✅ Final assignment match rate: {match_rate:.1f}% ({final_matches:,} records)")
        
        # Step 6: Export the fixed file
        print("\n💾 Exporting fixed file...")
        
        # Clean up temporary columns
        columns_to_keep = [col for col in merged_df.columns if not col.endswith('_ASSIGN') and not col.endswith('_FIXED')]
        export_df = merged_df[columns_to_keep]
        
        # Export updated file
        output_file = base_path / "03_Staging" / "Summons" / "summons_powerbi_FIXED.xlsx"
        export_df.to_excel(output_file, index=False, sheet_name='Police_Analytics_Data')
        
        # Also update the original file
        export_df.to_excel(summons_file, index=False, sheet_name='Police_Analytics_Data')
        
        print(f"✅ Files exported:")
        print(f"  - {output_file}")
        print(f"  - {summons_file} (updated)")
        
        # Step 7: Summary report
        unique_officers = export_df[export_df['ASSIGNMENT_FOUND'] == True]['FULL_NAME'].nunique()
        total_revenue = export_df['TOTAL PAID\r\nAMOUNT'].sum()
        
        print(f"\n🎯 ASSIGNMENT FIX COMPLETE!")
        print(f"📈 Results:")
        print(f"├── Total Records: {len(export_df):,}")
        print(f"├── Assignment Match Rate: {match_rate:.1f}%")
        print(f"├── Officers with Data: {unique_officers}")
        print(f"├── Total Revenue: ${total_revenue:,.2f}")
        print(f"└── Ready for Power BI!")
        
        # Show sample of successfully matched records
        print(f"\n✅ Sample Matched Records:")
        matched_sample = export_df[export_df['ASSIGNMENT_FOUND'] == True][
            ['Badge Number', 'FULL_NAME', 'DIVISION', 'BUREAU', 'TOTAL PAID\r\nAMOUNT']
        ].head()
        