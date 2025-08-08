#!/usr/bin/env python3
# 🕒 2025-06-28-22-30-00
# Project: Police_Analytics_Dashboard/etl_data_type_fix
# Author: R. A. Carucci
# Purpose: Quick fix for str/float comparison error in save stage

import pandas as pd
import numpy as np
from datetime import datetime

def fix_data_types(df):
    """Fix data type issues before saving"""
    
    print(f"🔧 Fixing data types for {len(df)} records...")
    
    # Make a copy to avoid modifying original
    df_fixed = df.copy()
    
    # Common numeric columns that might have mixed types
    numeric_columns = [
        'FINE_AMOUNT', 'AMOUNT', 'ASSESSED_AMOUNT', 'FINE', 'PENALTY',
        'DATA_QUALITY_SCORE', 'BADGE_NUMBER', 'PADDED_BADGE_NUMBER'
    ]
    
    # Date columns that might have mixed types
    date_columns = [
        'VIOLATION_DATE', 'ISSUE_DATE', 'DATE', 'PROCESSED_TIMESTAMP'
    ]
    
    # Fix numeric columns
    for col in numeric_columns:
        if col in df_fixed.columns:
            try:
                # Convert to string first, then extract numbers, then to float
                df_fixed[col] = pd.to_numeric(
                    df_fixed[col].astype(str).str.replace(r'[^\d.-]', '', regex=True),
                    errors='coerce'
                ).fillna(0)
                print(f"  ✅ Fixed numeric column: {col}")
            except Exception as e:
                print(f"  ⚠️ Could not fix {col}: {e}")
    
    # Fix date columns
    for col in date_columns:
        if col in df_fixed.columns:
            try:
                df_fixed[col] = pd.to_datetime(df_fixed[col], errors='coerce')
                print(f"  ✅ Fixed date column: {col}")
            except Exception as e:
                print(f"  ⚠️ Could not fix {col}: {e}")
    
    # Fix string columns (ensure they're actually strings)
    string_columns = [
        'OFFICER_NAME', 'VIOLATION_CODE', 'STATUTE', 'DATA_SOURCE',
        'DIVISION', 'BUREAU', 'FULL_NAME', 'LAST_NAME', 'FIRST_NAME'
    ]
    
    for col in string_columns:
        if col in df_fixed.columns:
            try:
                df_fixed[col] = df_fixed[col].astype(str).replace('nan', '')
                print(f"  ✅ Fixed string column: {col}")
            except Exception as e:
                print(f"  ⚠️ Could not fix {col}: {e}")
    
    # Remove any completely empty rows
    before_count = len(df_fixed)
    df_fixed = df_fixed.dropna(how='all')
    after_count = len(df_fixed)
    
    if before_count != after_count:
        print(f"  🧹 Removed {before_count - after_count} completely empty rows")
    
    # Replace infinite values with 0
    df_fixed = df_fixed.replace([np.inf, -np.inf], 0)
    
    print(f"✅ Data type fixes complete: {len(df_fixed)} records ready for save")
    
    return df_fixed

def safe_save_to_excel(df, filepath, sheet_name='Data'):
    """Safely save DataFrame to Excel with error handling"""
    
    try:
        print(f"💾 Attempting to save {len(df)} records to {filepath}")
        
        # Fix data types first
        df_clean = fix_data_types(df)
        
        # Ensure no column names are None or empty
        df_clean.columns = [str(col) if col is not None else f'Column_{i}' 
                           for i, col in enumerate(df_clean.columns)]
        
        # Limit column width for Excel compatibility
        for col in df_clean.columns:
            if df_clean[col].dtype == 'object':
                df_clean[col] = df_clean[col].astype(str).str[:32767]  # Excel cell limit
        
        # Save to Excel
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            df_clean.to_excel(writer, sheet_name=sheet_name, index=False)
        
        print(f"✅ Successfully saved to {filepath}")
        return True
        
    except Exception as e:
        print(f"❌ Excel save failed: {e}")
        
        # Try CSV fallback
        try:
            csv_path = str(filepath).replace('.xlsx', '.csv')
            df_clean = fix_data_types(df)
            df_clean.to_csv(csv_path, index=False)
            print(f"✅ Fallback: Saved as CSV to {csv_path}")
            return True
        except Exception as csv_error:
            print(f"❌ CSV fallback also failed: {csv_error}")
            return False

def emergency_save_fix():
    """Emergency function to fix and save the processed data"""
    
    print("🚑 EMERGENCY SAVE FIX")
    print("=" * 40)
    
    # Try to load the processed data from memory or temp location
    staging_path = r"C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons"
    
    # Check if there's a temporary file or if we can recover the data
    temp_files = [
        f"{staging_path}\\temp_processed_data.xlsx",
        f"{staging_path}\\summons_temp.xlsx"
    ]
    
    for temp_file in temp_files:
        try:
            if pd.io.common.file_exists(temp_file):
                print(f"📁 Found temp file: {temp_file}")
                df = pd.read_excel(temp_file)
                
                # Apply fixes and save properly
                success = safe_save_to_excel(
                    df, 
                    f"{staging_path}\\summons_powerbi_latest.xlsx",
                    'Fact_Summons'
                )
                
                if success:
                    print("✅ Emergency save successful!")
                    return True
                    
        except Exception as e:
            print(f"⚠️ Could not recover from {temp_file}: {e}")
    
    print("❌ No recoverable data found")
    return False

# Quick patch function to add to existing ETL
def patch_existing_etl():
    """Generate a patch to add to your existing summons_etl_enhanced.py"""
    
    patch_code = '''
# ADD THIS BEFORE THE SAVE SECTION IN summons_etl_enhanced.py

def fix_data_types_quick(df):
    """Quick data type fix for save issues"""
    df = df.copy()
    
    # Fix numeric columns
    numeric_cols = ['FINE_AMOUNT', 'AMOUNT', 'DATA_QUALITY_SCORE', 'BADGE_NUMBER']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # Fix date columns  
    date_cols = ['VIOLATION_DATE', 'ISSUE_DATE', 'PROCESSED_TIMESTAMP']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Replace infinite values
    df = df.replace([np.inf, -np.inf], 0)
    
    return df

# REPLACE YOUR SAVE SECTION WITH:
try:
    # Fix data types before saving
    unified_data_fixed = fix_data_types_quick(unified_data)
    
    # Save with error handling
    output_file = etl.save_to_staging(unified_data_fixed)
    logging.info(f"✅ ETL process completed successfully. Output: {output_file}")
    
except Exception as save_error:
    logging.error(f"Save error: {save_error}")
    
    # Emergency CSV save
    csv_path = staging_path / "summons_emergency_output.csv"
    unified_data.to_csv(csv_path, index=False)
    logging.info(f"✅ Emergency CSV saved: {csv_path}")
'''
    
    return patch_code

if __name__ == "__main__":
    # Try emergency recovery first
    emergency_save_fix()
    
    # Print the patch code
    print("\n🔧 PATCH CODE FOR YOUR ETL:")
    print("=" * 50)
    print(patch_code)
