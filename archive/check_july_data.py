#!/usr/bin/env python3
# 🕒 2025-08-13-15-00-00
# Project: July Data Check
# Author: R. A. Carucci
# Purpose: Check July 2025 ATS data for July 25th records

import pandas as pd
from pathlib import Path
from datetime import datetime

def check_july_data():
    """Check July 2025 ATS data for July 25th records"""
    print("🔍 CHECKING JULY 2025 ATS DATA")
    print("=" * 50)
    
    file_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\05_EXPORTS\_Summons\Court\25_07_ATS.xlsx")
    
    try:
        if file_path.exists():
            print(f"✅ July 2025 file found: {file_path.name}")
            
            # Read the file (skip first 4 rows as per ETL script)
            df = pd.read_excel(file_path, skiprows=4, header=None)
            print(f"📊 Raw data loaded: {len(df)} rows")
            
            # Check if we have enough columns for date data
            if len(df.columns) >= 5:  # Assuming date is in column 4 (0-indexed)
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
                    
                    # Check for July 25th specifically
                    july_25_data = valid_dates[valid_dates[4].dt.date == pd.to_datetime('2025-07-25').date()]
                    print(f"📊 July 25th records: {len(july_25_data)}")
                    
                    if len(july_25_data) > 0:
                        print("✅ JULY 25TH DATA FOUND!")
                        print("Sample records:")
                        print(july_25_data.head())
                    else:
                        print("❌ No July 25th data found")
                        
                        # Show July dates that do exist
                        july_data = valid_dates[valid_dates[4].dt.month == 7]
                        print(f"\n📊 July 2025 records: {len(july_data)}")
                        
                        if len(july_data) > 0:
                            print("📅 July dates found:")
                            for date in sorted(july_data[4].dt.date.unique()):
                                count = len(july_data[july_data[4].dt.date == date])
                                print(f"  - {date}: {count} records")
                else:
                    print("❌ No valid dates found in July file")
            else:
                print(f"⚠️ File has only {len(df.columns)} columns, may not contain date data")
                
        else:
            print(f"❌ July 2025 file not found: {file_path}")
            
            # List all available files
            court_dir = file_path.parent
            files = list(court_dir.glob("25_07*.xlsx"))
            if files:
                print(f"📁 Found July files: {[f.name for f in files]}")
            else:
                print("📁 No July files found")
                
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_july_data()
