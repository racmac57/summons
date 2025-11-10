#!/usr/bin/env python3
# 🕒 2025-08-13-14-30-00
# Project: Outreach Data Diagnostic
# Author: R. A. Carucci
# Purpose: Check for July 25th data in outreach sources

import pandas as pd
from pathlib import Path
from datetime import datetime

def check_community_engagement():
    """Check Community Engagement data for July 25th"""
    print("🔍 CHECKING COMMUNITY ENGAGEMENT DATA")
    print("=" * 50)
    
    file_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\02_ETL_Scripts\Overtime_TimeOff\backup_files\Community_Engagement_Monthly_Backup.xlsx")
    
    try:
        # Read the 2025_Master sheet
        df = pd.read_excel(file_path, sheet_name='2025_Master')
        print(f"✅ File loaded: {len(df)} rows")
        
        # Check for date column
        date_cols = [col for col in df.columns if 'date' in col.lower() or 'Date' in col]
        print(f"📅 Date columns found: {date_cols}")
        
        if date_cols:
            date_col = date_cols[0]
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            
            # Check for July 25th data
            july_25_data = df[df[date_col].dt.date == pd.to_datetime('2025-07-25').date()]
            print(f"📊 July 25th records found: {len(july_25_data)}")
            
            if len(july_25_data) > 0:
                print("✅ JULY 25TH DATA FOUND!")
                print(july_25_data.head())
            else:
                print("❌ No July 25th data found")
                
                # Show date range
                valid_dates = df[df[date_col].notna()][date_col]
                if len(valid_dates) > 0:
                    print(f"📅 Date range: {valid_dates.min()} to {valid_dates.max()}")
                    
                    # Check for July data
                    july_data = df[df[date_col].dt.month == 7]
                    print(f"📊 July records: {len(july_data)}")
                    
                    if len(july_data) > 0:
                        print("📅 July dates found:")
                        for date in july_data[date_col].dt.date.unique():
                            print(f"   - {date}")
        
    except Exception as e:
        print(f"❌ Error: {e}")

def check_stacp():
    """Check STACP data for July 25th"""
    print("\n🔍 CHECKING STACP DATA")
    print("=" * 50)
    
    file_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\Shared Folder\Compstat\Contributions\STACP\STACP.xlsm")
    
    try:
        # Read the 25_School_Outreach sheet
        df = pd.read_excel(file_path, sheet_name='25_School_Outreach')
        print(f"✅ File loaded: {len(df)} rows")
        
        # Check for date column
        date_cols = [col for col in df.columns if 'date' in col.lower() or 'Date' in col]
        print(f"📅 Date columns found: {date_cols}")
        
        if date_cols:
            date_col = date_cols[0]
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            
            # Check for July 25th data
            july_25_data = df[df[date_col].dt.date == pd.to_datetime('2025-07-25').date()]
            print(f"📊 July 25th records found: {len(july_25_data)}")
            
            if len(july_25_data) > 0:
                print("✅ JULY 25TH DATA FOUND!")
                print(july_25_data.head())
            else:
                print("❌ No July 25th data found")
                
                # Show date range
                valid_dates = df[df[date_col].notna()][date_col]
                if len(valid_dates) > 0:
                    print(f"📅 Date range: {valid_dates.min()} to {valid_dates.max()}")
                    
                    # Check for July data
                    july_data = df[df[date_col].dt.month == 7]
                    print(f"📊 July records: {len(july_data)}")
                    
                    if len(july_data) > 0:
                        print("📅 July dates found:")
                        for date in july_data[date_col].dt.date.unique():
                            print(f"   - {date}")
        
    except Exception as e:
        print(f"❌ Error: {e}")

def check_csb():
    """Check CSB data for July 25th"""
    print("\n🔍 CHECKING CSB DATA")
    print("=" * 50)
    
    file_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\Shared Folder\Compstat\Contributions\CSB\csb_monthly.xlsm")
    
    try:
        # Read the CSB_CommOut sheet
        df = pd.read_excel(file_path, sheet_name='CSB_CommOut ')
        print(f"✅ File loaded: {len(df)} rows")
        
        # Check for date column
        date_cols = [col for col in df.columns if 'date' in col.lower() or 'Date' in col]
        print(f"📅 Date columns found: {date_cols}")
        
        if date_cols:
            date_col = date_cols[0]
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            
            # Check for July 25th data
            july_25_data = df[df[date_col].dt.date == pd.to_datetime('2025-07-25').date()]
            print(f"📊 July 25th records found: {len(july_25_data)}")
            
            if len(july_25_data) > 0:
                print("✅ JULY 25TH DATA FOUND!")
                print(july_25_data.head())
            else:
                print("❌ No July 25th data found")
                
                # Show date range
                valid_dates = df[df[date_col].notna()][date_col]
                if len(valid_dates) > 0:
                    print(f"📅 Date range: {valid_dates.min()} to {valid_dates.max()}")
                    
                    # Check for July data
                    july_data = df[df[date_col].dt.month == 7]
                    print(f"📊 July records: {len(july_data)}")
                    
                    if len(july_data) > 0:
                        print("📅 July dates found:")
                        for date in july_data[date_col].dt.date.unique():
                            print(f"   - {date}")
        
    except Exception as e:
        print(f"❌ Error: {e}")

def check_patrol():
    """Check Patrol data for July 25th"""
    print("\n🔍 CHECKING PATROL DATA")
    print("=" * 50)
    
    file_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\Shared Folder\Compstat\Contributions\Patrol\patrol_monthly.xlsm")
    
    try:
        # Read the 25_Comm_Outreach sheet
        df = pd.read_excel(file_path, sheet_name='25_Comm_Outreach')
        print(f"✅ File loaded: {len(df)} rows")
        
        # Check for date column - handle both string and integer column names
        date_cols = []
        for col in df.columns:
            if isinstance(col, str) and ('date' in col.lower() or 'Date' in col):
                date_cols.append(col)
            elif isinstance(col, int):
                # Check if this column contains date-like data
                sample_values = df[col].dropna().head(5).astype(str)
                if any('2025' in str(val) or '2024' in str(val) for val in sample_values):
                    date_cols.append(col)
        
        print(f"📅 Date columns found: {date_cols}")
        
        if date_cols:
            date_col = date_cols[0]
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            
            # Check for July 25th data
            july_25_data = df[df[date_col].dt.date == pd.to_datetime('2025-07-25').date()]
            print(f"📊 July 25th records found: {len(july_25_data)}")
            
            if len(july_25_data) > 0:
                print("✅ JULY 25TH DATA FOUND!")
                print(july_25_data.head())
            else:
                print("❌ No July 25th data found")
                
                # Show date range
                valid_dates = df[df[date_col].notna()][date_col]
                if len(valid_dates) > 0:
                    print(f"📅 Date range: {valid_dates.min()} to {valid_dates.max()}")
                    
                    # Check for July data
                    july_data = df[df[date_col].dt.month == 7]
                    print(f"📊 July records: {len(july_data)}")
                    
                    if len(july_data) > 0:
                        print("📅 July dates found:")
                        for date in july_data[date_col].dt.date.unique():
                            print(f"   - {date}")
        else:
            print("❌ No date columns found")
            print(f"Available columns: {list(df.columns)}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main diagnostic function"""
    print("🔍 JULY 25TH OUTREACH DATA DIAGNOSTIC")
    print("=" * 60)
    print(f"🕒 Check time: {datetime.now()}")
    print()
    
    check_community_engagement()
    check_stacp()
    check_csb()
    check_patrol()
    
    print("\n" + "=" * 60)
    print("📋 DIAGNOSTIC COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    main()
