# 🕒 2025-07-03-22-50-00
# Project: Police_Analytics_Dashboard/summons_etl_fixed
# Author: R. A. Carucci
# Purpose: Fixed ETL script using corrected PADDED_BADGE_NUMBER column

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import re
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fix_data_types_quick(df):
    """Quick data type fix for Excel save compatibility"""
    print(f"🔧 Fixing data types for {len(df)} records...")
    df = df.copy()

    # Fix numeric columns
    numeric_cols = ['TICKET_COUNT', 'MOVING_COUNT', 'PARKING_COUNT', 'COMPLAINT_COUNT', 
                   'BADGE_NUMBER', 'PADDED_BADGE_NUMBER']
    for col in numeric_cols:
        if col in df.columns:
            try:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(r'[^\d.-]', '', regex=True),
                    errors='coerce'
                ).fillna(0)
                print(f"  ✅ Fixed numeric: {col}")
            except Exception as e:
                print(f"  ⚠️ Could not fix {col}: {e}")

    # Fix date columns
    date_cols = ['ISSUE_DATE', 'PROCESSED_TIMESTAMP']
    for col in date_cols:
        if col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                print(f"  ✅ Fixed date: {col}")
            except Exception as e:
                print(f"  ⚠️ Could not fix {col}: {e}")

    # Fix string columns
    string_cols = ['OFFICER_NAME', 'DATA_SOURCE', 'DIVISION', 'BUREAU', 'FULL_NAME', 'SUMMONS_TYPE']
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).replace('nan', '').replace('None', '')

    # Replace infinite values
    df = df.replace([np.inf, -np.inf], 0)
    print(f"✅ Data type fixes complete")
    return df

def load_assignment_master_fixed(assignment_file):
    """Load Assignment Master with corrected PADDED_BADGE_NUMBER column"""
    try:
        df = pd.read_excel(assignment_file)
        print(f"✅ Assignment Master loaded: {len(df)} rows")
        
        # Use the corrected PADDED_BADGE_NUMBER column directly
        if 'PADDED_BADGE_NUMBER' in df.columns:
            print("✅ Using corrected PADDED_BADGE_NUMBER column")
            
            # Select key columns
            required_cols = ['PADDED_BADGE_NUMBER']
            assignment_clean = df[required_cols].copy()
            
            # Add optional columns
            optional_mapping = {
                'FULL_NAME': ['Proposed Standardized Name', 'Current Display Format', 'FULL_NAME'],
                'DIVISION': ['TEAM', 'WG1', 'DIVISION'], 
                'BUREAU': ['WG2', 'BUREAU', 'UNIT'],
                'TITLE': ['TITLE', 'RANK'],
                'FIRST_NAME': ['FIRST_NAME', 'First Name'],
                'LAST_NAME': ['LAST_NAME', 'Last Name']
            }
            
            for target_col, possible_cols in optional_mapping.items():
                found_col = None
                for col in possible_cols:
                    if col in df.columns and df[col].notna().any():
                        found_col = col
                        break
                
                if found_col:
                    assignment_clean[target_col] = df[found_col]
                    print(f"   ✅ Using {found_col} for {target_col}")
                else:
                    assignment_clean[target_col] = ''
            
            # Create FULL_NAME if needed
            if (assignment_clean['FULL_NAME'].isna().all() or 
                assignment_clean['FULL_NAME'].eq('').all()):
                if ('FIRST_NAME' in assignment_clean.columns and 
                    'LAST_NAME' in assignment_clean.columns):
                    assignment_clean['FULL_NAME'] = (
                        assignment_clean['FIRST_NAME'].astype(str).str[0] + ". " + 
                        assignment_clean['LAST_NAME'].astype(str)
                    ).replace('nan. nan', '')
                    print("   ✅ Created FULL_NAME from FIRST_NAME + LAST_NAME")
            
        else:
            print("❌ PADDED_BADGE_NUMBER column not found")
            return None
        
        # Clean assignment data
        assignment_clean = assignment_clean[assignment_clean['PADDED_BADGE_NUMBER'].notna()]
        assignment_clean = assignment_clean[assignment_clean['PADDED_BADGE_NUMBER'] != 'PADDED_BADGE_NUMBER']
        
        # Ensure PADDED_BADGE_NUMBER is string format
        assignment_clean['PADDED_BADGE_NUMBER'] = assignment_clean['PADDED_BADGE_NUMBER'].astype(str)
        
        print(f"✅ Assignment data cleaned: {len(assignment_clean)} officers")
        
        # Show sample data
        print(f"📋 Sample assignment data:")
        sample_data = assignment_clean[['PADDED_BADGE_NUMBER', 'FULL_NAME']].head(5)
        for _, row in sample_data.iterrows():
            print(f"   Badge: {row['PADDED_BADGE_NUMBER']} → {row['FULL_NAME']}")
        
        return assignment_clean
        
    except Exception as e:
        print(f"❌ Error loading Assignment Master: {e}")
        return None

def extract_badge_number(officer_name):
    """Extract badge number from officer name string"""
    if pd.isna(officer_name):
        return None
    
    patterns = [
        r'(\d{4})$',           # 4 digits at end
        r'(\d{3})$',           # 3 digits at end  
        r'#(\d{3,4})',         # #123 or #1234
        r'\s(\d{3,4})\s*$',    # ending with space + digits
    ]
    
    for pattern in patterns:
        match = re.search(pattern, str(officer_name))
        if match:
            return match.group(1).zfill(4)
    
    return None

def main():
    """Main ETL execution function"""
    print("🚀 SUMMONS ETL FIXED VERSION")
    print("=" * 60)
    
    # File paths
    base_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack")
    assignment_file = base_path / "_Hackensack_Data_Repository" / "ASSIGNED_SHIFT" / "Assignment_Master_V2.xlsx"
    summons_file = base_path / "_MONTHLY_DATA" / "EXCEL_MASTER_FOR_MONTHLY" / "SUMMON_MASTER_V2.xlsm"
    staging_path = base_path / "03_Staging" / "Summons"
    staging_path.mkdir(parents=True, exist_ok=True)
    
    print(f"📁 Assignment Master: {assignment_file}")
    print(f"📁 Summons Master: {summons_file}")
    print(f"📁 Output Directory: {staging_path}")
    
    # Verify files exist
    if not assignment_file.exists():
        print(f"❌ Assignment Master not found: {assignment_file}")
        return None
    
    if not summons_file.exists():
        print(f"❌ Summons Master not found: {summons_file}")
        return None
    
    print("✅ All source files found")
    
    # Load Assignment Master with fixed processing
    assignment_clean = load_assignment_master_fixed(assignment_file)
    if assignment_clean is None:
        print("❌ Failed to load Assignment Master")
        return None
    
    # Load Summons Master
    try:
        excel_file = pd.ExcelFile(summons_file)
        available_sheets = excel_file.sheet_names
        print(f"📋 Available sheets: {available_sheets}")
        
        # Use SUMMONS_ASSIGNED_SHIFT sheet
        target_sheet = 'SUMMONS_ASSIGNED_SHIFT'
        summons_df = pd.read_excel(summons_file, sheet_name=target_sheet)
        print(f"✅ Using sheet '{target_sheet}' with {len(summons_df)} rows")
        print(f"📊 Columns: {list(summons_df.columns)}")
        
    except Exception as e:
        print(f"❌ Error loading Summons Master: {e}")
        return None
    
    # Process summons data
    try:
        # Find badge column
        badge_col = None
        for col in summons_df.columns:
            if any(keyword in str(col).upper() for keyword in ['BADGE', 'BADGE#']):
                badge_col = col
                break
        
        if badge_col:
            print(f"🔍 Found badge column: {badge_col}")
            
            # Extract and pad badge numbers
            summons_df['PADDED_BADGE_NUMBER'] = (
                summons_df[badge_col]
                .astype(str)
                .str.extract(r'(\d+)')[0]
                .str.zfill(4)
            )
            
            extracted_count = summons_df['PADDED_BADGE_NUMBER'].notna().sum()
            print(f"🔢 Badge numbers processed: {extracted_count} out of {len(summons_df)} records")
            
            # Show sample data
            officer_col = 'PATROL_MEMBER'
            if officer_col in summons_df.columns:
                sample_data = summons_df[[officer_col, 'PADDED_BADGE_NUMBER']].dropna().head(3)
                print("📝 Sample data:")
                for _, row in sample_data.iterrows():
                    print(f"   {row[officer_col]} → {row['PADDED_BADGE_NUMBER']}")
        else:
            print("❌ No badge column found")
            return None
            
    except Exception as e:
        print(f"❌ Error processing summons data: {e}")
        return None
    
    # Merge with assignment data
    try:
        merged_df = summons_df.merge(assignment_clean, on='PADDED_BADGE_NUMBER', how='left')
        
        # Calculate statistics
        total_records = len(merged_df)
        matched_records = merged_df['FULL_NAME'].notna().sum()
        match_rate = (matched_records / total_records) * 100
        
        print(f"✅ Merge completed:")
        print(f"   📊 Total records: {total_records}")
        print(f"   👮 Matched officers: {matched_records}")
        print(f"   📈 Match rate: {match_rate:.1f}%")
        print(f"   🎯 Unique officers: {merged_df['FULL_NAME'].nunique()}")
        
        # Add processing metadata
        merged_df['PROCESSED_TIMESTAMP'] = datetime.now()
        merged_df['DATA_SOURCE'] = f'SUMMONS_MASTER_{target_sheet}'
        merged_df['ETL_VERSION'] = 'v5.0_FIXED'
        
    except Exception as e:
        print(f"❌ Error merging data: {e}")
        return None
    
    # Apply data type fixes and save
    try:
        print(f"\n🔧 Preparing data for Excel export...")
        
        # Apply data type fixes
        final_df = fix_data_types_quick(merged_df)
        
        # Save main output file
        output_file = staging_path / "summons_powerbi_latest.xlsx"
        final_df.to_excel(output_file, index=False, sheet_name='Police_Analytics_Data')
        
        # Verify file was created
        if output_file.exists():
            file_size = output_file.stat().st_size
            print(f"🎯 SUCCESS! Output file created: {output_file}")
            print(f"📁 File size: {file_size / 1024:.1f} KB")
            
            # Create backup with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_file = staging_path / f"summons_powerbi_{timestamp}.xlsx"
            final_df.to_excel(backup_file, index=False, sheet_name='Police_Analytics_Data')
            print(f"💾 Backup created: {backup_file.name}")
            
            # Final summary
            print(f"\n🎉 ETL PIPELINE COMPLETE!")
            print(f"=" * 60)
            print(f"📂 Ready for Power BI: {output_file}")
            print(f"📊 Records Processed: {len(final_df)}")
            print(f"👮 Officer Matches: {matched_records} ({match_rate:.1f}%)")
            print(f"⏰ Processing Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"🚀 Status: READY FOR JULY 1ST DEPLOYMENT")
            
            return output_file
            
        else:
            print("❌ Output file was not created")
            return None
            
    except Exception as e:
        print(f"❌ Error saving data: {e}")
        return None

if __name__ == "__main__":
    result = main()
    if result:
        print(f"\n✅ SUCCESS - Connect Power BI to: {result}")
    else:
        print(f"\n❌ FAILED - Check error messages above")