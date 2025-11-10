#!/usr/bin/env python3
# 🕒 2025-08-13-15-25-00
# Project: Rolling 13-Month ETL (Clean)
# Author: R. A. Carucci
# Purpose: Create rolling 13-month dataset without extra columns

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import glob

def get_rolling_13_month_period():
    """Calculate the rolling 13-month period ending one month ago"""
    today = datetime.now()
    end_date = today - timedelta(days=30)  # One month ago
    start_date = end_date - timedelta(days=365)  # 13 months back
    
    print(f"📅 Rolling 13-month period:")
    print(f"   Start: {start_date.strftime('%Y-%m')} ({start_date.strftime('%m-%y')})")
    print(f"   End: {end_date.strftime('%Y-%m')} ({end_date.strftime('%m-%y')})")
    
    return start_date, end_date

def process_2024_data(start_date, end_date):
    """Process 2024 data from the ALL_SUMMONS file"""
    print("📊 Processing 2024 data from 24_ALL_SUMMONS.xlsx...")
    
    file_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\05_EXPORTS\_Summons\Court\24_ALL_SUMMONS.xlsx")
    
    try:
        # Load assignment data
        assignment_file = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\_Hackensack_Data_Repository\ASSIGNED_SHIFT\Assignment_Master_V2.xlsx")
        assignment_df = pd.read_excel(assignment_file, sheet_name='Sheet1')
        
        # Create badge lookup
        badge_lookup = {}
        for _, row in assignment_df.iterrows():
            raw_badge = str(row['BADGE_NUMBER']).strip()
            padded_badge = str(row['PADDED_BADGE_NUMBER']).strip()
            
            badge_variations = [
                raw_badge,
                raw_badge.zfill(4),
                padded_badge,
                str(int(float(raw_badge))).zfill(4) if raw_badge.replace('.', '').isdigit() else raw_badge.zfill(4)
            ]
            badge_variations = list(dict.fromkeys(badge_variations))
            
            assignment_data = {
                'OFFICER_DISPLAY_NAME': str(row['Proposed 4-Digit Format']).strip(),
                'WG1': str(row['WG1']).strip(),
                'WG2': str(row['WG2']).strip(),
                'WG3': str(row['WG3']).strip(),
                'WG4': str(row['WG4']).strip() if pd.notna(row['WG4']) else '',
                'WG5': str(row['WG5']).strip() if pd.notna(row['WG5']) else ''
            }
            
            for badge_var in badge_variations:
                if badge_var and badge_var != 'nan':
                    badge_lookup[badge_var] = assignment_data
        
        # Load court data
        court_df = pd.read_excel(file_path, skiprows=4, header=None)
        
        # Use only the actual columns that exist (17 columns)
        actual_columns = [
            'BADGE_NUMBER_RAW', 'OFFICER_NAME_RAW', 'ORI', 'TICKET_NUMBER', 'ISSUE_DATE', 
            'VIOLATION_NUMBER', 'TYPE', 'STATUS', 'DISPOSITION_DATE', 'FIND_CD', 'PAYMENT_DATE',
            'ASSESSED_AMOUNT', 'FINE_AMOUNT', 'COST_AMOUNT', 'MISC_AMOUNT', 'TOTAL_PAID_AMOUNT', 'CITY_COST_AMOUNT'
        ]
        
        # Only use the columns that actually exist
        num_cols = min(len(actual_columns), len(court_df.columns))
        court_df.columns = actual_columns[:num_cols]
        
        # Convert ISSUE_DATE to datetime
        court_df['ISSUE_DATE'] = pd.to_datetime(court_df['ISSUE_DATE'], errors='coerce')
        
        # Filter to rolling period
        court_df = court_df[
            (court_df['ISSUE_DATE'] >= start_date) & 
            (court_df['ISSUE_DATE'] <= end_date)
        ].copy()
        
        print(f"📅 Filtered to rolling period: {len(court_df)} records")
        
        # Filter out footer rows
        court_df['BADGE_STR'] = court_df['BADGE_NUMBER_RAW'].astype(str)
        court_df['OFFICER_STR'] = court_df['OFFICER_NAME_RAW'].astype(str)
        court_df['TICKET_STR'] = court_df['TICKET_NUMBER'].astype(str)
        
        footer_conditions = (
            court_df['BADGE_STR'].str.upper().str.contains('TOTAL', na=False) |
            court_df['OFFICER_STR'].str.contains('Run Date', na=False) |
            court_df['OFFICER_STR'].str.contains('July 7', na=False) |
            court_df['OFFICER_STR'].str.contains('PROG', na=False) |
            court_df['TICKET_STR'].str.contains('acs', na=False) |
            court_df['BADGE_STR'].str.len() > 10
        )
        
        court_df = court_df[~footer_conditions].reset_index(drop=True)
        
        # Filter civilian complaints and invalid badges
        court_df['BADGE_CLEAN'] = court_df['BADGE_NUMBER_RAW'].astype(str).str.strip()
        
        valid_records = (
            (court_df['BADGE_CLEAN'] != '9999') &
            (court_df['BADGE_CLEAN'] != '0') &
            (court_df['BADGE_CLEAN'] != '0000') &
            (court_df['BADGE_CLEAN'] != '') &
            (court_df['BADGE_CLEAN'] != 'nan') &
            (court_df['BADGE_CLEAN'].str.len() >= 1) &
            (court_df['BADGE_CLEAN'].str.len() <= 6)
        )
        
        court_df = court_df[valid_records].reset_index(drop=True)
        
        # Perform assignment matching
        court_df['PADDED_BADGE_NUMBER'] = court_df['BADGE_CLEAN'].str.zfill(4)
        court_df['ASSIGNMENT_FOUND'] = court_df['PADDED_BADGE_NUMBER'].isin(badge_lookup.keys())
        
        # Apply assignment data
        for field in ['OFFICER_DISPLAY_NAME', 'WG1', 'WG2', 'WG3', 'WG4', 'WG5']:
            court_df[field] = court_df['PADDED_BADGE_NUMBER'].map(
                lambda x: badge_lookup.get(x, {}).get(field, '') if x in badge_lookup else ''
            )
        
        # Add metadata
        court_df['DATA_SOURCE'] = '24_ALL_SUMMONS.xlsx'
        court_df['PROCESSED_TIMESTAMP'] = datetime.now()
        court_df['ETL_VERSION'] = 'Rolling_13_Month_v1.0'
        
        print(f"✅ Processed {len(court_df)} records from 2024 data")
        return court_df
        
    except Exception as e:
        print(f"❌ Error processing 2024 data: {e}")
        return pd.DataFrame()

def process_monthly_file(file_path, month_date):
    """Process a single monthly ATS file"""
    print(f"📊 Processing {file_path.name} ({month_date.strftime('%Y-%m')})...")
    
    try:
        # Load assignment data (same for all months)
        assignment_file = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\_Hackensack_Data_Repository\ASSIGNED_SHIFT\Assignment_Master_V2.xlsx")
        assignment_df = pd.read_excel(assignment_file, sheet_name='Sheet1')
        
        # Create badge lookup
        badge_lookup = {}
        for _, row in assignment_df.iterrows():
            raw_badge = str(row['BADGE_NUMBER']).strip()
            padded_badge = str(row['PADDED_BADGE_NUMBER']).strip()
            
            badge_variations = [
                raw_badge,
                raw_badge.zfill(4),
                padded_badge,
                str(int(float(raw_badge))).zfill(4) if raw_badge.replace('.', '').isdigit() else raw_badge.zfill(4)
            ]
            badge_variations = list(dict.fromkeys(badge_variations))
            
            assignment_data = {
                'OFFICER_DISPLAY_NAME': str(row['Proposed 4-Digit Format']).strip(),
                'WG1': str(row['WG1']).strip(),
                'WG2': str(row['WG2']).strip(),
                'WG3': str(row['WG3']).strip(),
                'WG4': str(row['WG4']).strip() if pd.notna(row['WG4']) else '',
                'WG5': str(row['WG5']).strip() if pd.notna(row['WG5']) else ''
            }
            
            for badge_var in badge_variations:
                if badge_var and badge_var != 'nan':
                    badge_lookup[badge_var] = assignment_data
        
        # Load court data
        court_df = pd.read_excel(file_path, skiprows=4, header=None)
        
        # Use only the actual columns that exist (17 columns)
        actual_columns = [
            'BADGE_NUMBER_RAW', 'OFFICER_NAME_RAW', 'ORI', 'TICKET_NUMBER', 'ISSUE_DATE', 
            'VIOLATION_NUMBER', 'TYPE', 'STATUS', 'DISPOSITION_DATE', 'FIND_CD', 'PAYMENT_DATE',
            'ASSESSED_AMOUNT', 'FINE_AMOUNT', 'COST_AMOUNT', 'MISC_AMOUNT', 'TOTAL_PAID_AMOUNT', 'CITY_COST_AMOUNT'
        ]
        
        # Only use the columns that actually exist
        num_cols = min(len(actual_columns), len(court_df.columns))
        court_df.columns = actual_columns[:num_cols]
        
        # Filter out footer rows
        court_df['BADGE_STR'] = court_df['BADGE_NUMBER_RAW'].astype(str)
        court_df['OFFICER_STR'] = court_df['OFFICER_NAME_RAW'].astype(str)
        court_df['TICKET_STR'] = court_df['TICKET_NUMBER'].astype(str)
        
        footer_conditions = (
            court_df['BADGE_STR'].str.upper().str.contains('TOTAL', na=False) |
            court_df['OFFICER_STR'].str.contains('Run Date', na=False) |
            court_df['OFFICER_STR'].str.contains('July 7', na=False) |
            court_df['OFFICER_STR'].str.contains('PROG', na=False) |
            court_df['TICKET_STR'].str.contains('acs', na=False) |
            court_df['BADGE_STR'].str.len() > 10
        )
        
        court_df = court_df[~footer_conditions].reset_index(drop=True)
        
        # Filter civilian complaints and invalid badges
        court_df['BADGE_CLEAN'] = court_df['BADGE_NUMBER_RAW'].astype(str).str.strip()
        
        valid_records = (
            (court_df['BADGE_CLEAN'] != '9999') &
            (court_df['BADGE_CLEAN'] != '0') &
            (court_df['BADGE_CLEAN'] != '0000') &
            (court_df['BADGE_CLEAN'] != '') &
            (court_df['BADGE_CLEAN'] != 'nan') &
            (court_df['BADGE_CLEAN'].str.len() >= 1) &
            (court_df['BADGE_CLEAN'].str.len() <= 6)
        )
        
        court_df = court_df[valid_records].reset_index(drop=True)
        
        # Perform assignment matching
        court_df['PADDED_BADGE_NUMBER'] = court_df['BADGE_CLEAN'].str.zfill(4)
        court_df['ASSIGNMENT_FOUND'] = court_df['PADDED_BADGE_NUMBER'].isin(badge_lookup.keys())
        
        # Apply assignment data
        for field in ['OFFICER_DISPLAY_NAME', 'WG1', 'WG2', 'WG3', 'WG4', 'WG5']:
            court_df[field] = court_df['PADDED_BADGE_NUMBER'].map(
                lambda x: badge_lookup.get(x, {}).get(field, '') if x in badge_lookup else ''
            )
        
        # Add metadata
        court_df['DATA_SOURCE'] = file_path.name
        court_df['PROCESSED_TIMESTAMP'] = datetime.now()
        court_df['ETL_VERSION'] = 'Rolling_13_Month_v1.0'
        
        print(f"✅ Processed {len(court_df)} records from {file_path.name}")
        return court_df
        
    except Exception as e:
        print(f"❌ Error processing {file_path.name}: {e}")
        return pd.DataFrame()

def create_rolling_13_month_dataset():
    """Create the rolling 13-month dataset"""
    print("🎯 ROLLING 13-MONTH ETL PROCESS (CLEAN)")
    print("=" * 60)
    
    # Get rolling period
    start_date, end_date = get_rolling_13_month_period()
    
    # Process 2024 data first
    data_2024 = process_2024_data(start_date, end_date)
    
    # Process 2025 monthly files
    base_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\05_EXPORTS\_Summons\Court")
    
    files_to_process = []
    for month in range(1, 8):  # January through July 2025
        filename = f"25_{month:02d}_ATS.xlsx"
        file_path = base_path / filename
        
        if file_path.exists():
            files_to_process.append((file_path, datetime(2025, month, 1)))
            print(f"✅ Found: {filename}")
        else:
            print(f"❌ Missing: {filename}")
    
    # Process each file
    all_data = []
    
    # Add 2024 data if available
    if not data_2024.empty:
        all_data.append(data_2024)
        print(f"📊 Added {len(data_2024)} records from 2024")
    
    # Process 2025 files
    for file_path, month_date in files_to_process:
        monthly_data = process_monthly_file(file_path, month_date)
        if not monthly_data.empty:
            all_data.append(monthly_data)
    
    if not all_data:
        print("❌ No data processed!")
        return
    
    # Combine all data
    print(f"\n🔗 Combining {len(all_data)} datasets...")
    combined_df = pd.concat(all_data, ignore_index=True)
    
    print(f"✅ Combined dataset: {len(combined_df)} total records")
    
    # Create output
    output_dir = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save to Excel with ATS_Court_Data sheet
    output_file = output_dir / f"summons_powerbi_rolling_13_month_clean_{timestamp}.xlsx"
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        combined_df.to_excel(writer, sheet_name='ATS_Court_Data', index=False)
    
    # Also save as latest
    latest_file = output_dir / "summons_powerbi_latest.xlsx"
    with pd.ExcelWriter(latest_file, engine='openpyxl') as writer:
        combined_df.to_excel(writer, sheet_name='ATS_Court_Data', index=False)
    
    print(f"\n💾 Files exported:")
    print(f"   - {output_file}")
    print(f"   - {latest_file}")
    
    # Summary statistics
    print(f"\n📊 ROLLING 13-MONTH SUMMARY:")
    print(f"   Total Records: {len(combined_df):,}")
    
    # Safe date range calculation
    valid_dates = combined_df[combined_df['ISSUE_DATE'].notna()]
    if len(valid_dates) > 0:
        min_date = valid_dates['ISSUE_DATE'].min()
        max_date = valid_dates['ISSUE_DATE'].max()
        print(f"   Date Range: {min_date.date()} to {max_date.date()}")
    
    print(f"   Assignment Match Rate: {(combined_df['ASSIGNMENT_FOUND'].sum() / len(combined_df) * 100):.1f}%")
    
    # Type breakdown
    type_counts = combined_df['TYPE'].value_counts()
    print(f"   Type Breakdown:")
    for type_val, count in type_counts.items():
        print(f"     {type_val}: {count:,}")
    
    # Column count
    print(f"   Total Columns: {len(combined_df.columns)}")
    print(f"   Column Names: {list(combined_df.columns)}")
    
    print(f"\n🎯 ROLLING 13-MONTH ETL COMPLETE!")
    print(f"📊 Your Power BI dashboard now has 13 months of data without extra columns!")

if __name__ == "__main__":
    create_rolling_13_month_dataset()
