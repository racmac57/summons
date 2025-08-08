# 🕒 2025-07-03-22-35-00
# Project: Police_Analytics_Dashboard/assignment_v2_diagnostic
# Author: R. A. Carucci
# Purpose: Diagnose Assignment Master V2 structure to fix badge column access

import pandas as pd
from pathlib import Path

def diagnose_assignment_master_v2():
    """Diagnose Assignment Master V2 structure"""
    
    base_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack")
    assignment_file = base_path / "_Hackensack_Data_Repository" / "ASSIGNED_SHIFT" / "Assignment_Master_V2.xlsx"
    
    print("🔍 ASSIGNMENT MASTER V2 STRUCTURE ANALYSIS")
    print("=" * 60)
    
    try:
        # Load file and check sheets
        xl = pd.ExcelFile(assignment_file)
        print(f"📋 Available sheets: {xl.sheet_names}")
        
        # Try default sheet first
        df = pd.read_excel(assignment_file)
        print(f"\n📊 Sheet: Default (first sheet)")
        print(f"   Rows: {len(df)}")
        print(f"   Columns: {len(df.columns)}")
        
        # Show all column names
        print(f"\n📝 ALL COLUMN NAMES:")
        for i, col in enumerate(df.columns):
            print(f"   {i:2d}: '{col}'")
        
        # Check for PADDED_BADGE_NUMBER specifically
        if 'PADDED_BADGE_NUMBER' in df.columns:
            print(f"\n✅ Found 'PADDED_BADGE_NUMBER' column!")
            padded_col = df['PADDED_BADGE_NUMBER']
            print(f"   Non-null values: {padded_col.notna().sum()}")
            print(f"   Sample values: {padded_col.dropna().head(5).tolist()}")
        else:
            print(f"\n❌ 'PADDED_BADGE_NUMBER' column NOT found")
        
        # Look for any badge-related columns
        badge_related = []
        for col in df.columns:
            if any(keyword in str(col).upper() for keyword in ['BADGE', 'NUMBER', 'ID']):
                badge_related.append(col)
        
        if badge_related:
            print(f"\n🔢 Badge-related columns found:")
            for col in badge_related:
                sample_data = df[col].dropna().head(3).tolist()
                print(f"   '{col}': {sample_data}")
        
        # Check first few rows of data
        print(f"\n📋 FIRST 5 ROWS OF DATA:")
        print(df.head().to_string())
        
        # Try other sheets if multiple exist
        if len(xl.sheet_names) > 1:
            print(f"\n🔍 Checking other sheets...")
            for sheet_name in xl.sheet_names[1:]:
                try:
                    sheet_df = pd.read_excel(assignment_file, sheet_name=sheet_name)
                    print(f"\n📋 Sheet: {sheet_name}")
                    print(f"   Rows: {len(sheet_df)}")
                    print(f"   Columns: {list(sheet_df.columns)}")
                    
                    if 'PADDED_BADGE_NUMBER' in sheet_df.columns:
                        print(f"   ✅ Found PADDED_BADGE_NUMBER in {sheet_name}!")
                        sample_data = sheet_df['PADDED_BADGE_NUMBER'].dropna().head(3).tolist()
                        print(f"   Sample: {sample_data}")
                        
                except Exception as e:
                    print(f"   ❌ Error reading {sheet_name}: {e}")
    
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    diagnose_assignment_master_v2()