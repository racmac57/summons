#!/usr/bin/env python3
# 🕒 2025-08-13-14-45-00
# Project: Summons Columns Check
# Author: R. A. Carucci
# Purpose: Check available columns in latest summons data

import pandas as pd
from pathlib import Path

def check_summons_columns():
    """Check what columns are available in the latest summons data"""
    print("🔍 CHECKING SUMMONS DATA COLUMNS")
    print("=" * 50)
    
    file_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons\summons_powerbi_latest.xlsx")
    
    try:
        # Read the ATS_Court_Data sheet
        df = pd.read_excel(file_path, sheet_name='ATS_Court_Data')
        print(f"✅ File loaded: {len(df)} rows")
        print(f"📊 Columns found: {len(df.columns)}")
        
        print("\n📋 ALL COLUMNS:")
        for i, col in enumerate(df.columns):
            print(f"  {i+1:2d}. {col}")
        
        # Check for specific columns mentioned in M code
        expected_cols = [
            'PADDED_BADGE_NUMBER', 'OFFICER_DISPLAY_NAME', 'WG1', 'WG2', 'WG3', 'WG4', 'WG5',
            'TICKET_NUMBER', 'ISSUE_DATE', 'VIOLATION_NUMBER', 'TYPE', 'STATUS',
            'TOTAL_PAID_AMOUNT', 'FINE_AMOUNT', 'COST_AMOUNT', 'MISC_AMOUNT',
            'OFFICER_NAME_RAW', 'ASSIGNMENT_FOUND', 'PROCESSED_TIMESTAMP',
            'DATA_SOURCE', 'ETL_VERSION', 'Month', 'Year'
        ]
        
        print("\n🔍 CHECKING EXPECTED COLUMNS:")
        missing_cols = []
        for col in expected_cols:
            if col in df.columns:
                print(f"  ✅ {col}")
            else:
                print(f"  ❌ {col} - MISSING")
                missing_cols.append(col)
        
        if missing_cols:
            print(f"\n⚠️  MISSING COLUMNS: {missing_cols}")
        else:
            print("\n✅ ALL EXPECTED COLUMNS FOUND!")
        
        # Check for date-related columns
        date_cols = [col for col in df.columns if 'date' in col.lower() or 'Date' in col]
        print(f"\n📅 DATE-RELATED COLUMNS: {date_cols}")
        
        # Check for month/year columns
        month_cols = [col for col in df.columns if 'month' in col.lower() or 'Month' in col]
        year_cols = [col for col in df.columns if 'year' in col.lower() or 'Year' in col]
        print(f"📅 MONTH COLUMNS: {month_cols}")
        print(f"📅 YEAR COLUMNS: {year_cols}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_summons_columns()
