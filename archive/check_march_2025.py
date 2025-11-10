#!/usr/bin/env python3
# 🕒 2025-08-13-15-30-00
# Project: March 2025 Check
# Author: R. A. Carucci
# Purpose: Check March 2025 file structure

import pandas as pd
from pathlib import Path

def check_march_2025():
    """Check March 2025 file structure"""
    print("🔍 CHECKING MARCH 2025 FILE STRUCTURE")
    print("=" * 50)
    
    march_file = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\05_EXPORTS\_Summons\Court\25_03_ATS.xlsx")
    
    if march_file.exists():
        print(f"📊 Checking {march_file.name}...")
        
        # Read without skipping rows first
        df_raw = pd.read_excel(march_file)
        print(f"   Raw columns: {len(df_raw.columns)}")
        print(f"   Raw column names: {list(df_raw.columns)}")
        
        # Read with skiprows=4
        df_processed = pd.read_excel(march_file, skiprows=4, header=None)
        print(f"   Processed columns: {len(df_processed.columns)}")
        print(f"   First few column values:")
        for i in range(min(10, len(df_processed.columns))):
            sample_values = df_processed[i].dropna().head(3).tolist()
            print(f"     Column {i}: {sample_values}")
        
        # Check if this is a different format
        print(f"\n📋 Column count analysis:")
        print(f"   Expected: 17 columns")
        print(f"   Actual: {len(df_processed.columns)} columns")
        
        if len(df_processed.columns) > 17:
            print(f"   ⚠️ This file has {len(df_processed.columns) - 17} extra columns")
            print(f"   Extra columns: {list(range(17, len(df_processed.columns)))}")
    
    else:
        print(f"❌ March 2025 file not found: {march_file}")

if __name__ == "__main__":
    check_march_2025()
