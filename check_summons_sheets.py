# 🕒 2025-07-03-22-55-00
# Project: Police_Analytics_Dashboard/check_summons_sheets
# Author: R. A. Carucci
# Purpose: Check if other sheets have different badge numbers

import pandas as pd
from pathlib import Path

def check_all_summons_sheets():
    """Check all sheets in summons file for different badge data"""
    
    base_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack")
    summons_file = base_path / "_MONTHLY_DATA" / "EXCEL_MASTER_FOR_MONTHLY" / "SUMMON_MASTER_V2.xlsm"
    
    print("🔍 CHECKING ALL SUMMONS SHEETS FOR BADGE MATCHES")
    print("=" * 60)
    
    try:
        excel_file = pd.ExcelFile(summons_file)
        print(f"📋 Available sheets: {excel_file.sheet_names}")
        
        # Assignment badges for comparison
        assignment_badges = ['0083', '0105', '0108', '0110', '0127']  # Based on your assignment data
        
        for sheet_name in excel_file.sheet_names:
            try:
                print(f"\n📊 Checking sheet: {sheet_name}")
                df = pd.read_excel(summons_file, sheet_name=sheet_name)
                print(f"   Rows: {len(df)}, Columns: {len(df.columns)}")
                
                # Look for badge columns
                badge_cols = [col for col in df.columns if 'BADGE' in str(col).upper()]
                
                if badge_cols:
                    for badge_col in badge_cols:
                        print(f"   🔢 Badge column found: {badge_col}")
                        
                        # Extract and pad badge numbers
                        badges = (df[badge_col]
                                .astype(str)
                                .str.extract(r'(\d+)')[0]
                                .str.zfill(4)
                                .dropna()
                                .unique()[:10])  # First 10 unique
                        
                        print(f"      Sample badges: {list(badges)}")
                        
                        # Check for matches with assignment
                        matches = [badge for badge in badges if badge in assignment_badges]
                        if matches:
                            print(f"      ✅ MATCHES FOUND: {matches}")
                        else:
                            print(f"      ❌ No matches with assignment badges")
                else:
                    print(f"   ❌ No badge columns found")
                    
            except Exception as e:
                print(f"   ❌ Error reading sheet {sheet_name}: {e}")
    
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    check_all_summons_sheets()