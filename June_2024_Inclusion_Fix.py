# 🕒 2025-07-10-17-15-00
# Project: SummonsMaster/June_2024_Inclusion_Fix
# Author: R. A. Carucci
# Purpose: Extract June 2024 data from 24_ALL_ATS.xlsx and combine for true rolling 12 months

import pandas as pd
from pathlib import Path
from datetime import datetime

def add_june_2024_data():
    """Add June 2024 data to get true rolling 12 months"""
    
    try:
        print("🔄 ADDING JUNE 2024 DATA FOR TRUE ROLLING 12 MONTHS")
        print("=" * 60)
        
        # Paths
        base_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack")
        full_2024_file = base_path / "05_EXPORTS" / "_Summons" / "Court" / "24_ALL_ATS.xlsx"
        existing_file = base_path / "03_Staging" / "Summons" / "summons_powerbi_rolling_12_month.xlsx"
        assignment_file = base_path / "_Hackensack_Data_Repository" / "ASSIGNED_SHIFT" / "Assignment_Master_V2.xlsx"
        output_file = base_path / "03_Staging" / "Summons" / "summons_powerbi_complete_rolling_12_month.xlsx"
        
        print(f"📋 Loading existing rolling data from: {existing_file}")
        existing_df = pd.read_excel(existing_file, sheet_name='ATS_Court_Data')
        print(f"✅ Loaded {len(existing_df):,} existing records")
        
        print(f"\n📋 Loading full 2024 data from: {full_2024_file}")
        # Read 2024 data with same structure as other files
        full_2024_df = pd.read_excel(full_2024_file, skiprows=4, header=None)
        
        # Apply same column structure
        court_columns = [
            'BADGE_NUMBER_RAW', 'OFFICER_NAME_RAW', 'ORI', 'TICKET_NUMBER', 'ISSUE_DATE',
            'VIOLATION_NUMBER', 'TYPE', 'STATUS', 'DISPOSITION_DATE', 'FIND_CD', 'PAYMENT_DATE',
            'ASSESSED_AMOUNT', 'FINE_AMOUNT', 'COST_AMOUNT', 'MISC_AMOUNT', 'TOTAL_PAID_AMOUNT', 'CITY_COST_AMOUNT'
        ]
        
        num_cols = min(len(court_columns), len(full_2024_df.columns))
        full_2024_df.columns = court_columns[:num_cols] + [f'Extra_Col_{i}' for i in range(num_cols, len(full_2024_df.columns))]
        
        print(f"✅ Loaded {len(full_2024_df):,} total 2024 records")
        
        # Clean and filter for June 2024 only
        full_2024_df['BADGE_CLEAN'] = full_2024_df['BADGE_NUMBER_RAW'].astype(str).str.strip()
        invalid_badges = ['0', '0000', 'nan', 'TOTAL', 'Total', 'SUMMARY', 'Summary', '\n']
        full_2024_df = full_2024_df[~full_2024_df['BADGE_CLEAN'].isin(invalid_badges)]
        full_2024_df['PADDED_BADGE_NUMBER'] = full_2024_df['BADGE_CLEAN']
        
        # Convert dates
        full_2024_df['ISSUE_DATE'] = pd.to_datetime(full_2024_df['ISSUE_DATE'], errors='coerce')
        
        # Filter for June 2024 only
        june_2024_start = datetime(2024, 6, 1)
        june_2024_end = datetime(2024, 6, 30, 23, 59, 59)
        
        june_2024_df = full_2024_df[
            (full_2024_df['ISSUE_DATE'] >= june_2024_start) & 
            (full_2024_df['ISSUE_DATE'] <= june_2024_end) &
            (full_2024_df['ISSUE_DATE'].notna())
        ].copy()
        
        print(f"📅 Filtered to June 2024: {len(june_2024_df):,} records")
        
        if len(june_2024_df) == 0:
            print("⚠️ No June 2024 data found in 24_ALL_ATS.xlsx")
            print("This might mean June 2024 data isn't in the file or has different date format")
            return False
        
        # Load assignment master for June 2024 data
        print("📋 Loading Assignment Master for June 2024 matching...")
        assignment_df = pd.read_excel(assignment_file, sheet_name=0)
        
        # Create assignment lookup
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
        
        # Apply assignments to June 2024 data
        june_2024_df['ASSIGNMENT_FOUND'] = False
        june_2024_df['OFFICER_DISPLAY_NAME'] = None
        june_2024_df['WG1'] = None
        june_2024_df['WG2'] = None
        june_2024_df['WG3'] = None
        june_2024_df['WG4'] = None
        june_2024_df['WG5'] = None
        
        matches = 0
        for idx, row in june_2024_df.iterrows():
            badge = row['PADDED_BADGE_NUMBER']
            
            found_match = False
            if badge in badge_lookup:
                assignment = badge_lookup[badge]
                found_match = True
            elif badge.lstrip('0') in badge_lookup and badge.lstrip('0') != '':
                assignment = badge_lookup[badge.lstrip('0')]
                found_match = True
            
            if found_match:
                june_2024_df.at[idx, 'ASSIGNMENT_FOUND'] = True
                june_2024_df.at[idx, 'OFFICER_DISPLAY_NAME'] = assignment['OFFICER_DISPLAY_NAME']
                june_2024_df.at[idx, 'WG1'] = assignment['WG1']
                june_2024_df.at[idx, 'WG2'] = assignment['WG2']
                june_2024_df.at[idx, 'WG3'] = assignment['WG3']
                june_2024_df.at[idx, 'WG4'] = assignment['WG4']
                june_2024_df.at[idx, 'WG5'] = assignment['WG5']
                matches += 1
        
        # Add metadata to June 2024 data
        june_2024_df['PROCESSED_TIMESTAMP'] = datetime.now()
        june_2024_df['DATA_SOURCE'] = '24_ALL_ATS.xlsx'
        june_2024_df['ETL_VERSION'] = 'v6.0_June_2024_Addition'
        june_2024_df['Month_Year'] = june_2024_df['ISSUE_DATE'].dt.strftime('%b-%y')
        june_2024_df['Month'] = june_2024_df['ISSUE_DATE'].dt.month
        june_2024_df['Year'] = june_2024_df['ISSUE_DATE'].dt.year
        
        match_rate = (matches / len(june_2024_df)) * 100 if len(june_2024_df) > 0 else 0
        print(f"✅ June 2024 assignment matching: {match_rate:.1f}%")
        
        # Prepare June 2024 data to match existing structure
        powerbi_columns = [
            'PADDED_BADGE_NUMBER', 'OFFICER_DISPLAY_NAME', 
            'WG1', 'WG2', 'WG3', 'WG4', 'WG5',
            'TICKET_NUMBER', 'ISSUE_DATE', 'VIOLATION_NUMBER', 'TYPE', 'STATUS',
            'TOTAL_PAID_AMOUNT', 'FINE_AMOUNT', 'COST_AMOUNT', 'MISC_AMOUNT',
            'OFFICER_NAME_RAW', 'ASSIGNMENT_FOUND', 'PROCESSED_TIMESTAMP', 'DATA_SOURCE', 'ETL_VERSION',
            'Month_Year', 'Month', 'Year'
        ]
        
        # Ensure all columns exist
        for col in powerbi_columns:
            if col not in june_2024_df.columns:
                june_2024_df[col] = None
        
        june_2024_final = june_2024_df[powerbi_columns].copy()
        
        # Combine with existing data
        print(f"\n🔗 Combining June 2024 data with existing rolling data...")
        combined_df = pd.concat([june_2024_final, existing_df], ignore_index=True)
        
        # Sort by date to ensure proper order
        combined_df = combined_df.sort_values('ISSUE_DATE')
        
        print(f"✅ Combined dataset: {len(combined_df):,} total records")
        
        # Export complete 13-month dataset
        print(f"\n💾 Exporting complete rolling 12-month data...")
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            combined_df.to_excel(writer, sheet_name='ATS_Court_Data', index=False)
        
        # Show month breakdown
        month_summary = combined_df.groupby('Month_Year').size().reset_index(name='Count')
        print(f"\n📈 Complete month-by-month breakdown:")
        for _, row in month_summary.iterrows():
            print(f"   {row['Month_Year']}: {row['Count']:,} records")
        
        # Generate summary statistics
        match_rate = (combined_df['ASSIGNMENT_FOUND'].sum() / len(combined_df)) * 100
        unique_officers = combined_df['PADDED_BADGE_NUMBER'].nunique()
        total_revenue = combined_df['TOTAL_PAID_AMOUNT'].sum()
        moving_count = len(combined_df[combined_df['TYPE'] == 'M'])
        parking_count = len(combined_df[combined_df['TYPE'] == 'P'])
        
        print("\n" + "=" * 60)
        print("🎯 COMPLETE ROLLING 12-MONTH RESULTS:")
        print("=" * 60)
        print(f"📅 Period: Jun 2024 - Jun 2025 (TRUE 12 months)")
        print(f"📊 Total Records: {len(combined_df):,}")
        print(f"🎯 Assignment Match Rate: {match_rate:.1f}%")
        print(f"👮 Unique Officers: {unique_officers}")
        print(f"💰 Total Revenue: ${total_revenue:,.2f}")
        print(f"🚗 Moving Violations: {moving_count:,}")
        print(f"🅿️ Parking Violations: {parking_count:,}")
        print(f"📁 Output File: {output_file}")
        print("=" * 60)
        print("🚀 NOW YOU HAVE TRUE ROLLING 12 MONTHS!")
        print("✅ Connect Power BI to: summons_powerbi_complete_rolling_12_month.xlsx")
        print("✅ Matrix should show Jun-24 through Jun-25 (13 months total)")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    add_june_2024_data()