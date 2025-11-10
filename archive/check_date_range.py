#!/usr/bin/env python3
# 🕒 2025-08-13-14-50-00
# Project: Summons Date Range Check
# Author: R. A. Carucci
# Purpose: Check the actual date range in summons data

import pandas as pd
from pathlib import Path
from datetime import datetime

def check_date_range():
    """Check the date range in the latest summons data"""
    print("🔍 CHECKING SUMMONS DATE RANGE")
    print("=" * 50)
    
    file_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons\summons_powerbi_latest.xlsx")
    
    try:
        # Read the ATS_Court_Data sheet
        df = pd.read_excel(file_path, sheet_name='ATS_Court_Data')
        print(f"✅ File loaded: {len(df)} rows")
        
        # Convert ISSUE_DATE to datetime
        df['ISSUE_DATE'] = pd.to_datetime(df['ISSUE_DATE'], errors='coerce')
        
        # Remove null dates
        valid_dates = df[df['ISSUE_DATE'].notna()]
        print(f"📅 Records with valid dates: {len(valid_dates)}")
        
        if len(valid_dates) > 0:
            # Get date range
            min_date = valid_dates['ISSUE_DATE'].min()
            max_date = valid_dates['ISSUE_DATE'].max()
            print(f"📅 Date range: {min_date.date()} to {max_date.date()}")
            
            # Check for July 25th specifically
            july_25_data = valid_dates[valid_dates['ISSUE_DATE'].dt.date == pd.to_datetime('2025-07-25').date()]
            print(f"📊 July 25th records: {len(july_25_data)}")
            
            if len(july_25_data) > 0:
                print("✅ JULY 25TH DATA FOUND!")
                print(july_25_data[['TICKET_NUMBER', 'ISSUE_DATE', 'TYPE', 'OFFICER_DISPLAY_NAME']].head())
            else:
                print("❌ No July 25th data found")
            
            # Check by month
            print("\n📅 DATA BY MONTH:")
            monthly_counts = valid_dates.groupby(valid_dates['ISSUE_DATE'].dt.to_period('M')).size()
            for month, count in monthly_counts.items():
                print(f"  {month}: {count} records")
            
            # Check for July data specifically
            july_data = valid_dates[valid_dates['ISSUE_DATE'].dt.month == 7]
            print(f"\n📊 July 2025 records: {len(july_data)}")
            
            if len(july_data) > 0:
                print("📅 July dates found:")
                for date in sorted(july_data['ISSUE_DATE'].dt.date.unique()):
                    count = len(july_data[july_data['ISSUE_DATE'].dt.date == date])
                    print(f"  - {date}: {count} records")
            
            # Check what TYPE values exist
            print(f"\n📋 TYPE values:")
            type_counts = valid_dates['TYPE'].value_counts()
            for type_val, count in type_counts.items():
                print(f"  {type_val}: {count} records")
                
        else:
            print("❌ No valid dates found in data")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_date_range()
