# 🕒 2025-06-28-20-30-00
# Project: Police_Analytics_Dashboard/etl_correct_paths
# Author: R. A. Carucci
# Purpose: ETL script using correct file locations from staging directory

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

def main():
    """Run ETL with correct file paths"""
    print("🚀 ETL with Correct File Paths")
    print("=" * 50)
    
    # Correct file paths based on directory listing
    base_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons")
    
    assignment_file = base_path / "Assignment_Master.xlsm"
    summons_file = base_path / "SUMMONS_MASTER.xlsx"
    
    print(f"📁 Working directory: {base_path}")
    print(f"📄 Assignment Master: {assignment_file}")
    print(f"📄 Summons Master: {summons_file}")
    
    # Load Assignment Master
    try:
        assignment_df = pd.read_excel(assignment_file)
        print(f"✅ Assignment Master loaded: {len(assignment_df)} rows")
        
        # Extract columns 2,3,4,5,7,8,16 (0-indexed: 1,2,3,4,6,7,15)
        if len(assignment_df.columns) >= 16:
            cols_to_extract = [1, 2, 3, 4, 6, 7, 15]  # 0-indexed
            assignment_clean = assignment_df.iloc[:, cols_to_extract].copy()
            assignment_clean.columns = ['LAST_NAME', 'FIRST_NAME', 'BADGE_NUMBER', 
                                       'PADDED_BADGE_NUMBER', 'DIVISION', 'BUREAU', 'FULL_NAME']
            
            # Remove header rows and empty records
            assignment_clean = assignment_clean[assignment_clean['LAST_NAME'] != 'LAST_NAME']
            assignment_clean = assignment_clean.dropna(subset=['BADGE_NUMBER'])
            assignment_clean['PADDED_BADGE_NUMBER'] = assignment_clean['PADDED_BADGE_NUMBER'].astype(str).str.zfill(4)
            
            print(f"✅ Assignment data cleaned: {len(assignment_clean)} officers")
        else:
            print(f"❌ Not enough columns in Assignment Master")
            return
            
    except Exception as e:
        print(f"❌ Error loading Assignment Master: {e}")
        return
    
    # Load Summons Master
    try:
        # Try different sheet names
        summons_df = None
        sheet_names = ['ENHANCED_BADGE', 'Sheet1', 0]  # Try these in order
        
        for sheet in sheet_names:
            try:
                summons_df = pd.read_excel(summons_file, sheet_name=sheet)
                print(f"✅ Summons Master loaded from sheet '{sheet}': {len(summons_df)} rows")
                break
            except:
                continue
        
        if summons_df is None:
            print("❌ Could not load any sheet from Summons Master")
            return
            
    except Exception as e:
        print(f"❌ Error loading Summons Master: {e}")
        return
    
    # Simple processing - find badge columns and merge
    print(f"📊 Summons columns: {list(summons_df.columns[:10])}...")
    
    # Look for badge-related columns
    badge_col = None
    for col in summons_df.columns:
        if 'badge' in str(col).lower() or 'officer' in str(col).lower():
            badge_col = col
            break
    
    if badge_col:
        print(f"🔍 Found badge column: {badge_col}")
        
        # Extract badge numbers and pad them
        summons_df['PADDED_BADGE_NUMBER'] = summons_df[badge_col].astype(str).str.extract(r'(\d{3,4})')[0]
        summons_df['PADDED_BADGE_NUMBER'] = summons_df['PADDED_BADGE_NUMBER'].str.zfill(4)
        
        # Merge with assignment data
        merged_df = summons_df.merge(assignment_clean, on='PADDED_BADGE_NUMBER', how='left')
        
        # Add processing metadata
        merged_df['PROCESSED_TIMESTAMP'] = datetime.now()
        merged_df['DATA_SOURCE'] = 'SUMMONS_MASTER'
        
        print(f"✅ Merge complete: {len(merged_df)} records")
        
        # Calculate match rate
        match_rate = (merged_df['FULL_NAME'].notna().sum() / len(merged_df)) * 100
        print(f"📈 Officer match rate: {match_rate:.1f}%")
        
        # Save final output
        output_file = base_path / "summons_powerbi_latest.xlsx"
        
        try:
            merged_df.to_excel(output_file, index=False)
            print(f"🎯 SUCCESS! Final output: {output_file}")
            print(f"📊 Records ready for Power BI: {len(merged_df)}")
            
            # Show summary
            print("\n📋 SUMMARY:")
            print(f"   Total Records: {len(merged_df):,}")
            print(f"   Matched Officers: {merged_df['FULL_NAME'].notna().sum():,}")
            print(f"   Unique Officers: {merged_df['FULL_NAME'].nunique()}")
            print(f"   Date Range: {merged_df.select_dtypes(include=['datetime']).min().min() if not merged_df.select_dtypes(include=['datetime']).empty else 'N/A'}")
            
        except Exception as e:
            print(f"❌ Save error: {e}")
            # Try CSV fallback
            csv_file = base_path / "summons_powerbi_latest.csv"
            merged_df.to_csv(csv_file, index=False)
            print(f"💾 Saved as CSV: {csv_file}")
            
    else:
        print("❌ No badge column found in Summons Master")

if __name__ == "__main__":
    main()
