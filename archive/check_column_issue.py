#!/usr/bin/env python3
# 🕒 2025-08-13-15-20-00
# Project: Column Issue Check
# Author: R. A. Carucci
# Purpose: Check why extra columns are being added

import pandas as pd
from pathlib import Path

def check_column_structure():
    """Check the actual column structure of data files"""
    print("🔍 CHECKING COLUMN STRUCTURE")
    print("=" * 50)
    
    # Check July 2025 file
    july_file = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\05_EXPORTS\_Summons\Court\25_07_ATS.xlsx")
    
    if july_file.exists():
        print(f"📊 Checking {july_file.name}...")
        
        # Read without skipping rows first to see raw structure
        df_raw = pd.read_excel(july_file)
        print(f"   Raw columns: {len(df_raw.columns)}")
        print(f"   Raw column names: {list(df_raw.columns)}")
        
        # Read with skiprows=4 as per ETL script
        df_processed = pd.read_excel(july_file, skiprows=4, header=None)
        print(f"   Processed columns: {len(df_processed.columns)}")
        print(f"   First few column values:")
        for i in range(min(5, len(df_processed.columns))):
            sample_values = df_processed[i].dropna().head(3).tolist()
            print(f"     Column {i}: {sample_values}")
    
    # Check 2024 file
    all_summons_file = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\05_EXPORTS\_Summons\Court\24_ALL_SUMMONS.xlsx")
    
    if all_summons_file.exists():
        print(f"\n📊 Checking {all_summons_file.name}...")
        
        # Read without skipping rows first
        df_raw = pd.read_excel(all_summons_file)
        print(f"   Raw columns: {len(df_raw.columns)}")
        print(f"   Raw column names: {list(df_raw.columns)}")
        
        # Read with skiprows=4
        df_processed = pd.read_excel(all_summons_file, skiprows=4, header=None)
        print(f"   Processed columns: {len(df_processed.columns)}")
        print(f"   First few column values:")
        for i in range(min(5, len(df_processed.columns))):
            sample_values = df_processed[i].dropna().head(3).tolist()
            print(f"     Column {i}: {sample_values}")
    
    # Check the latest output file
    latest_file = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons\summons_powerbi_latest.xlsx")
    
    if latest_file.exists():
        print(f"\n📊 Checking latest output file...")
        
        df_output = pd.read_excel(latest_file, sheet_name='ATS_Court_Data')
        print(f"   Output columns: {len(df_output.columns)}")
        print(f"   Output column names:")
        for i, col in enumerate(df_output.columns):
            print(f"     {i+1:2d}. {col}")

if __name__ == "__main__":
    check_column_structure()
