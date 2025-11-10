# 🕒 2025-06-28-20-15-00
# Project: Police_Analytics_Dashboard/etl_diagnostic
# Author: R. A. Carucci
# Purpose: Diagnostic script to identify and fix ETL issues

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def test_file_access():
    """Test if we can access all required files"""
    base_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack")
    
    files_to_check = {
        "Assignment Master": base_path / "Assignment_Master.xlsm",
        "Summons Master": base_path / "SUMMONS_MASTER.xlsx", 
        "ATS Report": base_path / "2025_04_ATS_Report.xlsx"
    }
    
    print("🔍 FILE ACCESS TEST")
    print("=" * 50)
    
    all_found = True
    for name, file_path in files_to_check.items():
        if file_path.exists():
            size = file_path.stat().st_size
            print(f"✅ {name}: {file_path} ({size:,} bytes)")
        else:
            print(f"❌ {name}: {file_path} NOT FOUND")
            all_found = False
    
    return all_found, files_to_check

def quick_load_test(files_dict):
    """Quick test to load each file and check basic structure"""
    print("\n📊 FILE LOADING TEST")
    print("=" * 50)
    
    results = {}
    
    # Test Assignment Master
    try:
        assignment_df = pd.read_excel(files_dict["Assignment Master"])
        print(f"✅ Assignment Master: {len(assignment_df)} rows, {len(assignment_df.columns)} columns")
        print(f"   Columns: {list(assignment_df.columns[:5])}...")
        results["assignment"] = assignment_df
    except Exception as e:
        print(f"❌ Assignment Master failed: {e}")
        results["assignment"] = None
    
    # Test Summons Master
    try:
        summons_df = pd.read_excel(files_dict["Summons Master"])
        print(f"✅ Summons Master: {len(summons_df)} rows, {len(summons_df.columns)} columns")
        print(f"   Columns: {list(summons_df.columns[:5])}...")
        results["summons"] = summons_df
    except Exception as e:
        print(f"❌ Summons Master failed: {e}")
        results["summons"] = None
    
    # Test ATS Report
    try:
        ats_df = pd.read_excel(files_dict["ATS Report"])
        print(f"✅ ATS Report: {len(ats_df)} rows, {len(ats_df.columns)} columns")
        print(f"   Columns: {list(ats_df.columns[:5])}...")
        results["ats"] = ats_df
    except Exception as e:
        print(f"❌ ATS Report failed: {e}")
        results["ats"] = None
    
    return results

def create_staging_directory():
    """Ensure staging directory exists"""
    staging_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons")
    staging_path.mkdir(parents=True, exist_ok=True)
    print(f"📁 Staging directory ready: {staging_path}")
    return staging_path

def simple_merge_test(data_results, staging_path):
    """Perform a simple merge test with minimal processing"""
    print("\n🔧 SIMPLE MERGE TEST")
    print("=" * 50)
    
    if not data_results["assignment"] is not None:
        print("❌ Cannot proceed without Assignment Master")
        return None
    
    # Process Assignment Master (columns 2,3,4,5,7,8,16 based on your spec)
    assignment_df = data_results["assignment"]
    
    if len(assignment_df.columns) >= 16:
        # Extract the specific columns you need
        cols_needed = [2, 3, 4, 5, 7, 8, 16]  # 0-indexed
        assignment_clean = assignment_df.iloc[:, cols_needed].copy()
        assignment_clean.columns = ['LAST_NAME', 'FIRST_NAME', 'BADGE_NUMBER', 'PADDED_BADGE_NUMBER', 
                                   'DIVISION', 'BUREAU', 'FULL_NAME']
        
        # Remove any header rows
        assignment_clean = assignment_clean[assignment_clean['LAST_NAME'] != 'LAST_NAME']
        assignment_clean = assignment_clean.dropna(subset=['BADGE_NUMBER'])
        
        print(f"✅ Assignment data cleaned: {len(assignment_clean)} officers")
        print(f"   Sample: {assignment_clean[['BADGE_NUMBER', 'FULL_NAME']].head().to_dict('records')}")
    else:
        print(f"❌ Assignment Master has only {len(assignment_df.columns)} columns, need at least 16")
        return None
    
    # Process ATS data if available
    combined_data = []
    
    if data_results["ats"] is not None:
        ats_df = data_results["ats"]
        print(f"🔄 Processing ATS data: {len(ats_df)} records")
        
        # Simple column mapping - adjust first 6 columns
        if len(ats_df.columns) >= 6:
            ats_clean = ats_df.iloc[:, :6].copy()
            ats_clean.columns = ['BADGE_RAW', 'OFFICER_NAME', 'CITATION_REF', 'CITATION_NUMBER', 
                                'VIOLATION_DATE', 'STATUTE']
            
            # Clean badge numbers to 4 digits
            ats_clean['PADDED_BADGE_NUMBER'] = ats_clean['BADGE_RAW'].astype(str).str.zfill(4)
            ats_clean['DATA_SOURCE'] = 'ATS_AUTOMATED'
            
            # Merge with assignment data
            ats_merged = ats_clean.merge(assignment_clean, on='PADDED_BADGE_NUMBER', how='left')
            combined_data.append(ats_merged)
            
            print(f"✅ ATS merge complete: {len(ats_merged)} records")
            match_rate = (ats_merged['FULL_NAME'].notna().sum() / len(ats_merged)) * 100
            print(f"   Officer match rate: {match_rate:.1f}%")
    
    if combined_data:
        final_df = pd.concat(combined_data, ignore_index=True)
        
        # Add processing timestamp
        final_df['PROCESSED_TIMESTAMP'] = datetime.now()
        
        # Save to staging
        output_file = staging_path / "summons_powerbi_latest.xlsx"
        
        try:
            final_df.to_excel(output_file, index=False)
            print(f"✅ SUCCESS: Data saved to {output_file}")
            print(f"📊 Final dataset: {len(final_df)} records")
            return output_file
        except Exception as e:
            print(f"❌ Excel save failed: {e}")
            # Try CSV
            csv_file = staging_path / "summons_powerbi_latest.csv"
            final_df.to_csv(csv_file, index=False)
            print(f"💾 Saved as CSV: {csv_file}")
            return csv_file
    else:
        print("❌ No data to process")
        return None

def main():
    """Run full diagnostic"""
    print("🚀 ETL DIAGNOSTIC STARTING")
    print("=" * 50)
    
    # Test file access
    files_found, files_dict = test_file_access()
    if not files_found:
        print("❌ CRITICAL: Missing files. Cannot proceed.")
        return
    
    # Test file loading
    data_results = quick_load_test(files_dict)
    
    # Create staging directory
    staging_path = create_staging_directory()
    
    # Attempt simple merge
    output_file = simple_merge_test(data_results, staging_path)
    
    if output_file:
        print(f"\n🎯 SUCCESS! Output ready for Power BI: {output_file}")
    else:
        print(f"\n❌ FAILED! Check errors above.")

if __name__ == "__main__":
    main()
