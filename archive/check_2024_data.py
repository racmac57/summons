#!/usr/bin/env python3
# 🕒 2025-08-13-15-10-00
# Project: 2024 Data Check
# Author: R. A. Carucci
# Purpose: Check what 2024 data is available

import pandas as pd
from pathlib import Path
from datetime import datetime

def check_2024_data():
    """Check what 2024 data is available"""
    print("🔍 CHECKING 2024 DATA")
    print("=" * 50)
    
    court_dir = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\05_EXPORTS\_Summons\Court")
    
    # Check for 2024 files
    files_2024 = list(court_dir.glob("24_*.xlsx"))
    files_all = list(court_dir.glob("24_ALL_SUMMONS.xlsx"))
    
    print(f"📁 2024 files found: {len(files_2024)}")
    for file in files_2024:
        print(f"  📄 {file.name}")
    
    print(f"📁 ALL_SUMMONS files found: {len(files_all)}")
    for file in files_all:
        print(f"  📄 {file.name}")
    
    # Check the ALL_SUMMONS file
    if files_all:
        all_summons_file = files_all[0]
        print(f"\n📊 Checking {all_summons_file.name}...")
        
        try:
            # Try to read the file
            df = pd.read_excel(all_summons_file, skiprows=4, header=None)
            print(f"✅ File loaded: {len(df)} rows")
            
            # Check if we have enough columns for date data
            if len(df.columns) >= 5:
                # Convert potential date column to datetime
                df[4] = pd.to_datetime(df[4], errors='coerce')
                
                # Remove null dates
                valid_dates = df[df[4].notna()]
                print(f"📅 Records with valid dates: {len(valid_dates)}")
                
                if len(valid_dates) > 0:
                    # Get date range
                    min_date = valid_dates[4].min()
                    max_date = valid_dates[4].max()
                    print(f"📅 Date range: {min_date.date()} to {max_date.date()}")
                    
                    # Check for July 2024 specifically
                    july_2024_data = valid_dates[
                        (valid_dates[4].dt.year == 2024) & 
                        (valid_dates[4].dt.month == 7)
                    ]
                    print(f"📊 July 2024 records: {len(july_2024_data)}")
                    
                    if len(july_2024_data) > 0:
                        print("✅ JULY 2024 DATA FOUND!")
                        print("Sample July 2024 records:")
                        print(july_2024_data.head())
                        
                        # Show July 2024 dates
                        july_dates = sorted(july_2024_data[4].dt.date.unique())
                        print(f"📅 July 2024 dates found:")
                        for date in july_dates:
                            count = len(july_2024_data[july_2024_data[4].dt.date == date])
                            print(f"  - {date}: {count} records")
                    else:
                        print("❌ No July 2024 data found")
                    
                    # Check all 2024 data
                    data_2024 = valid_dates[valid_dates[4].dt.year == 2024]
                    print(f"\n📊 Total 2024 records: {len(data_2024)}")
                    
                    if len(data_2024) > 0:
                        print("📅 2024 months found:")
                        monthly_counts = data_2024.groupby(data_2024[4].dt.to_period('M')).size()
                        for month, count in monthly_counts.items():
                            print(f"  {month}: {count} records")
                
        except Exception as e:
            print(f"❌ Error reading {all_summons_file.name}: {e}")
            import traceback
            traceback.print_exc()
    
    else:
        print("❌ No 2024 data files found")

if __name__ == "__main__":
    check_2024_data()
