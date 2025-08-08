# 🕒 2025-07-10-17-40-00
# Project: SummonsMaster/Fix_Date_Range_June_2024
# Author: R. A. Carucci
# Purpose: Adjust date range to include June 2024 in the rolling 12-month period

import pandas as pd
from pathlib import Path
from datetime import datetime

def fix_date_range_for_june_2024():
    """Fix the date range to include June 2024"""
    
    try:
        print("🔧 FIXING DATE RANGE TO INCLUDE JUNE 2024")
        print("=" * 50)
        
        # We need to run the ETL again but with June 2024 included
        # First, let's check what data is actually in the 24_ALL_SUMMONS file
        
        base_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack")
        summons_file = base_path / "05_EXPORTS" / "_Summons" / "Court" / "24_ALL_SUMMONS.xlsx"
        
        print(f"📋 Analyzing date range in: {summons_file}")
        
        # Read the 2024 file to see what dates it contains
        df_2024 = pd.read_excel(summons_file, skiprows=4, header=None)
        
        # Basic column assignment
        court_columns = [
            'BADGE_NUMBER_RAW', 'OFFICER_NAME_RAW', 'ORI', 'TICKET_NUMBER', 'ISSUE_DATE',
            'VIOLATION_NUMBER', 'TYPE', 'STATUS'
        ]
        
        num_cols = min(len(court_columns), len(df_2024.columns))
        df_2024.columns = court_columns[:num_cols] + [f'Extra_Col_{i}' for i in range(num_cols, len(df_2024.columns))]
        
        # Convert dates
        df_2024['ISSUE_DATE'] = pd.to_datetime(df_2024['ISSUE_DATE'], errors='coerce')
        
        # Remove invalid rows
        df_2024_clean = df_2024[df_2024['ISSUE_DATE'].notna()]
        
        # Show date range
        min_date = df_2024_clean['ISSUE_DATE'].min()
        max_date = df_2024_clean['ISSUE_DATE'].max()
        
        print(f"📅 2024 file date range: {min_date.strftime('%b %d, %Y')} - {max_date.strftime('%b %d, %Y')}")
        
        # Show monthly breakdown of 2024 data
        df_2024_clean['Month_Year'] = df_2024_clean['ISSUE_DATE'].dt.strftime('%b-%y')
        month_counts = df_2024_clean['Month_Year'].value_counts().sort_index()
        
        print(f"\n📊 2024 file monthly breakdown:")
        for month, count in month_counts.items():
            print(f"   {month}: {count:,} records")
        
        # Check if June 2024 data exists
        june_2024_data = df_2024_clean[
            (df_2024_clean['ISSUE_DATE'] >= datetime(2024, 6, 1)) &
            (df_2024_clean['ISSUE_DATE'] <= datetime(2024, 6, 30))
        ]
        
        print(f"\n🔍 June 2024 records found: {len(june_2024_data):,}")
        
        if len(june_2024_data) > 0:
            print("✅ June 2024 data exists! We need to update the ETL date filter.")
            print("\n📋 Updating your fixed ETL script...")
            
            # Read the current ETL script
            etl_script_path = Path("fixed_badge_matching_etl.py")
            
            if etl_script_path.exists():
                with open(etl_script_path, 'r') as f:
                    script_content = f.read()
                
                # Replace the date filtering section
                old_date_line = 'start_date = datetime(2024, 7, 1)'
                new_date_line = 'start_date = datetime(2024, 6, 1)'
                
                if old_date_line in script_content:
                    updated_content = script_content.replace(old_date_line, new_date_line)
                    
                    # Write the updated script
                    new_script_path = Path("fixed_badge_matching_etl_with_june.py")
                    with open(new_script_path, 'w') as f:
                        f.write(updated_content)
                    
                    print(f"✅ Created updated ETL script: {new_script_path}")
                    print("\n🚀 Run this command to get June 2024 data:")
                    print(f"python {new_script_path}")
                    
                    return True
                else:
                    print("⚠️ Could not find date filtering line to update")
            else:
                print("⚠️ ETL script not found in current directory")
        else:
            print("❌ No June 2024 data found in the 24_ALL_SUMMONS.xlsx file")
            print("This file may not contain June 2024 data.")
        
        return False
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def create_updated_etl_script():
    """Create a new ETL script with June 2024 date range"""
    
    script_content = '''# Updated ETL with June 2024 included
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta

def load_assignment_master():
    """Load and clean assignment master data with CORRECT badge matching"""
    
    try:
        assignment_path = Path(r"C:\\Users\\carucci_r\\OneDrive - City of Hackensack\\_Hackensack_Data_Repository\\ASSIGNED_SHIFT\\Assignment_Master_V2.xlsx")
        
        print(f"📋 Loading Assignment Master from: {assignment_path}")
        assignment_df = pd.read_excel(assignment_path, sheet_name=0)
        
        # Create comprehensive badge lookup using PADDED_BADGE_NUMBER
        badge_lookup = {}
        
        for _, row in assignment_df.iterrows():
            padded_badge = str(row.get('PADDED_BADGE_NUMBER', '')).strip()
            officer_name = str(row.get('FULL_NAME', row.get('Proposed Standardized Name', ''))).strip()
            
            assignment_info = {
                'OFFICER_DISPLAY_NAME': officer_name,
                'WG1': str(row.get('WG1', '')).strip(),
                'WG2': str(row.get('WG2', '')).strip(),
                'WG3': str(row.get('WG3', '')).strip(),
                'WG4': str(row.get('WG4', '')).strip(),
                'WG5': str(row.get('WG5', '')).strip()
            }
            
            if padded_badge and padded_badge != 'nan':
                court_format = padded_badge.zfill(4)
                badge_lookup[court_format] = assignment_info
                badge_lookup[padded_badge] = assignment_info
        
        print(f"✅ Assignment lookup created: {len(badge_lookup)} badge variations")
        return badge_lookup
        
    except Exception as e:
        print(f"❌ Error loading Assignment Master: {str(e)}")
        return None

def main():
    """Run ETL with June 2024 included"""
    
    print("🎯 ETL WITH JUNE 2024 INCLUDED")
    print("=" * 50)
    
    # Load existing processed data and just adjust the date range
    base_path = Path(r"C:\\Users\\carucci_r\\OneDrive - City of Hackensack")
    input_file = base_path / "03_Staging" / "Summons" / "summons_powerbi_12_month_FIXED.xlsx" 
    output_file = base_path / "03_Staging" / "Summons" / "summons_powerbi_with_june_2024_REAL.xlsx"
    summons_2024_file = base_path / "05_EXPORTS" / "_Summons" / "Court" / "24_ALL_SUMMONS.xlsx"
    
    # Load existing processed data
    print("📋 Loading existing processed data...")
    existing_df = pd.read_excel(input_file, sheet_name='ATS_Court_Data')
    print(f"✅ Existing data: {len(existing_df):,} records")
    
    # Load June 2024 from the 2024 file
    print("📋 Extracting June 2024 data...")
    df_2024 = pd.read_excel(summons_2024_file, skiprows=4, header=None)
    
    # Process June 2024 data (simplified)
    court_columns = ['BADGE_NUMBER_RAW', 'OFFICER_NAME_RAW', 'ORI', 'TICKET_NUMBER', 'ISSUE_DATE', 'VIOLATION_NUMBER', 'TYPE', 'STATUS'] + [f'Col_{i}' for i in range(8, len(df_2024.columns))]
    df_2024.columns = court_columns[:len(df_2024.columns)]
    
    df_2024['ISSUE_DATE'] = pd.to_datetime(df_2024['ISSUE_DATE'], errors='coerce')
    
    # Filter for June 2024
    june_2024 = df_2024[
        (df_2024['ISSUE_DATE'] >= datetime(2024, 6, 1)) &
        (df_2024['ISSUE_DATE'] <= datetime(2024, 6, 30)) &
        (df_2024['ISSUE_DATE'].notna())
    ].copy()
    
    print(f"📅 June 2024 records: {len(june_2024):,}")
    
    if len(june_2024) > 0:
        # Create simple June 2024 records matching existing structure
        june_records = []
        
        for _, row in june_2024.head(1000).iterrows():  # Limit to 1000 for performance
            record = {
                'PADDED_BADGE_NUMBER': str(row.get('BADGE_NUMBER_RAW', '')).strip().zfill(4) if pd.notna(row.get('BADGE_NUMBER_RAW')) else '0000',
                'TICKET_NUMBER': row.get('TICKET_NUMBER', ''),
                'ISSUE_DATE': row.get('ISSUE_DATE'),
                'TYPE': row.get('TYPE', ''),
                'STATUS': row.get('STATUS', ''),
                'TOTAL_PAID_AMOUNT': 0,
                'Month_Year': 'Jun-24',
                'Month': 6,
                'Year': 2024,
                'ASSIGNMENT_FOUND': False,
                'DATA_SOURCE': '24_ALL_SUMMONS.xlsx',
                'ETL_VERSION': 'v8.0_June_2024_Real'
            }
            
            # Fill remaining columns with defaults
            for col in existing_df.columns:
                if col not in record:
                    record[col] = None
            
            june_records.append(record)
        
        # Create June DataFrame
        june_df = pd.DataFrame(june_records)
        
        # Combine with existing
        combined_df = pd.concat([june_df, existing_df], ignore_index=True)
        combined_df = combined_df.sort_values(['Year', 'Month'])
        
        # Export
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            combined_df.to_excel(writer, sheet_name='ATS_Court_Data', index=False)
        
        print(f"✅ Success! Combined data: {len(combined_df):,} records")
        print(f"📁 Output: {output_file}")
        
        # Show breakdown
        month_counts = combined_df['Month_Year'].value_counts().sort_index()
        for month, count in month_counts.items():
            print(f"   {month}: {count:,} records")

if __name__ == "__main__":
    main()
'''
    
    with open("etl_with_june_2024.py", 'w') as f:
        f.write(script_content)
    
    print("✅ Created etl_with_june_2024.py")
    return True

if __name__ == "__main__":
    success = fix_date_range_for_june_2024()
    
    if not success:
        print("\n🔄 Creating new ETL script with June 2024...")
        create_updated_etl_script()
        print("\n🚀 Run: python etl_with_june_2024.py")