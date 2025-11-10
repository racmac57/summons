#!/usr/bin/env python3
"""
Quick fix for the ticket number duplication issue in the transition script output.
This script will regenerate unique ticket numbers for the historical backfill data.
"""

import pandas as pd
import numpy as np

def fix_ticket_numbers():
    """Fix duplicate ticket numbers in the Excel output"""
    
    # Load the Excel file
    excel_path = r"C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons\summons_powerbi_latest.xlsx"
    df = pd.read_excel(excel_path, sheet_name='Summons_Data')
    
    print(f"Loaded {len(df):,} records")
    print(f"Unique ticket numbers before fix: {df['TICKET_NUMBER'].nunique():,}")
    
    # Identify historical backfill records
    historical_mask = df['ETL_VERSION'] == 'HISTORICAL_BACKFILL'
    historical_df = df[historical_mask].copy()
    
    print(f"Historical records to fix: {len(historical_df):,}")
    
    # Group by Month_Year and TYPE to regenerate ticket numbers
    historical_df = historical_df.sort_values(['Month_Year', 'TYPE', 'ISSUE_DATE'])
    
    # Create unique ticket numbers within each group
    historical_df['GROUP_ID'] = historical_df.groupby(['Month_Year', 'TYPE']).ngroup()
    
    ticket_counter = 1
    new_ticket_numbers = []
    
    for group_id in sorted(historical_df['GROUP_ID'].unique()):
        group_data = historical_df[historical_df['GROUP_ID'] == group_id]
        
        # Get month/year info from first record
        first_record = group_data.iloc[0]
        month_year = first_record['Month_Year']
        violation_type = first_record['TYPE']
        
        # Parse month-year
        if '-' in month_year:
            month, year = month_year.split('-')
            year = 2000 + int(year)
            month = int(month)
        else:
            continue
        
        # Generate unique ticket numbers for this group
        for i in range(len(group_data)):
            new_ticket = f"HIST_{year}{month:02d}_{violation_type}_{ticket_counter:06d}"
            new_ticket_numbers.append(new_ticket)
            ticket_counter += 1
    
    # Update the ticket numbers
    historical_df['TICKET_NUMBER'] = new_ticket_numbers
    
    # Remove the temporary column
    historical_df = historical_df.drop(columns=['GROUP_ID'])
    
    # Combine with non-historical data
    other_df = df[~historical_mask]
    fixed_df = pd.concat([other_df, historical_df], ignore_index=True)
    
    print(f"Unique ticket numbers after fix: {fixed_df['TICKET_NUMBER'].nunique():,}")
    
    # Save the fixed file
    backup_path = excel_path.replace('.xlsx', '_backup.xlsx')
    print(f"Creating backup: {backup_path}")
    
    # Create backup
    df.to_excel(backup_path, index=False)
    
    # Save fixed version
    print(f"Saving fixed file: {excel_path}")
    fixed_df.to_excel(excel_path, index=False)
    
    print("✅ Fix complete!")
    print(f"   Original records: {len(df):,}")
    print(f"   Fixed records: {len(fixed_df):,}")
    print(f"   Unique tickets before: {df['TICKET_NUMBER'].nunique():,}")
    print(f"   Unique tickets after: {fixed_df['TICKET_NUMBER'].nunique():,}")

if __name__ == "__main__":
    fix_ticket_numbers()
