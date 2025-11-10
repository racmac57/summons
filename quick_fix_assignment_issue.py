#!/usr/bin/env python3
"""
Quick fix for assignment enrichment issues in the transition script.
The problem: Historical records (no badge numbers) are being matched against Assignment Master,
creating massive duplication and null values.
"""

import pandas as pd
import numpy as np

def quick_fix():
    """Fix the assignment enrichment issues"""
    
    print("QUICK FIX for Assignment Issues")
    print("=" * 50)
    
    # Load the Excel file
    excel_path = r"C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons\summons_powerbi_latest.xlsx"
    
    print(f"Loading: {excel_path}")
    df = pd.read_excel(excel_path, sheet_name='Summons_Data')
    
    print(f"Total records: {len(df):,}")
    print(f"Unique tickets: {df['TICKET_NUMBER'].nunique():,}")
    
    # Check ETL_VERSION distribution
    print("\nETL_VERSION distribution:")
    print(df['ETL_VERSION'].value_counts())
    
    # The main issue: Historical records should NOT be enriched with assignment data
    # because they have no real badge numbers
    
    # Separate historical from e-ticket data
    historical_mask = df['ETL_VERSION'] == 'HISTORICAL_BACKFILL'
    eticket_mask = df['ETL_VERSION'] == 'ETICKET_CURRENT'
    
    historical_df = df[historical_mask].copy()
    eticket_df = df[eticket_mask].copy()
    
    print(f"\nHistorical records: {len(historical_df):,}")
    print(f"E-ticket records: {len(eticket_df):,}")
    
    # For historical data, clean up the assignment fields
    print("\nCleaning historical data...")
    
    # Reset assignment fields for historical data (they shouldn't have real assignments)
    assignment_columns = ['OFFICER_DISPLAY_NAME', 'TEAM', 'WG1', 'WG2', 'WG3', 'WG4', 'WG5', 'POSS_CONTRACT_TYPE']
    
    for col in assignment_columns:
        if col in historical_df.columns:
            historical_df[col] = ""
    
    # Set a consistent officer display name for historical data
    historical_df['OFFICER_DISPLAY_NAME'] = "Historical Data (No Officer Info)"
    
    # For e-ticket data, keep the assignment enrichment but clean up any issues
    print("Cleaning e-ticket data...")
    
    # Clean up any null assignment data in e-ticket records
    for col in assignment_columns:
        if col in eticket_df.columns:
            eticket_df[col] = eticket_df[col].fillna("")
    
    # Combine the cleaned data
    print("\nCombining cleaned data...")
    cleaned_df = pd.concat([historical_df, eticket_df], ignore_index=True)
    
    # Remove any duplicate ticket numbers (keep first occurrence)
    print("Removing duplicate ticket numbers...")
    before_dedup = len(cleaned_df)
    cleaned_df = cleaned_df.drop_duplicates(subset=['TICKET_NUMBER'], keep='first')
    after_dedup = len(cleaned_df)
    
    print(f"   Before deduplication: {before_dedup:,}")
    print(f"   After deduplication: {after_dedup:,}")
    print(f"   Removed: {before_dedup - after_dedup:,}")
    
    # Create backup
    backup_path = excel_path.replace('.xlsx', '_before_fix.xlsx')
    print(f"\nCreating backup: {backup_path}")
    df.to_excel(backup_path, index=False)
    
    # Save the cleaned file
    print(f"Saving cleaned file: {excel_path}")
    cleaned_df.to_excel(excel_path, index=False)
    
    # Summary
    print("\nQUICK FIX COMPLETE!")
    print("=" * 50)
    print(f"Final records: {len(cleaned_df):,}")
    print(f"Unique tickets: {cleaned_df['TICKET_NUMBER'].nunique():,}")
    
    print("\nFinal ETL_VERSION distribution:")
    print(cleaned_df['ETL_VERSION'].value_counts())
    
    print("\nOfficer assignment coverage:")
    historical_count = len(cleaned_df[cleaned_df['ETL_VERSION'] == 'HISTORICAL_BACKFILL'])
    eticket_count = len(cleaned_df[cleaned_df['ETL_VERSION'] == 'ETICKET_CURRENT'])
    eticket_with_officer = len(cleaned_df[(cleaned_df['ETL_VERSION'] == 'ETICKET_CURRENT') & 
                                        (cleaned_df['OFFICER_DISPLAY_NAME'] != "")])
    
    print(f"   Historical: {historical_count:,} (no officer info expected)")
    print(f"   E-ticket: {eticket_count:,} total")
    print(f"   E-ticket with officer: {eticket_with_officer:,} ({eticket_with_officer/eticket_count*100:.1f}%)")
    
    print(f"\nBackup saved: {backup_path}")
    print(f"Clean file: {excel_path}")
    print("\nReady for Power BI!")

if __name__ == "__main__":
    quick_fix()
