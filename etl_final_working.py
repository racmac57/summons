# 🕒 2025-06-28-20-45-00
# Project: Police_Analytics_Dashboard/etl_final_working
# Author: R. A. Carucci
# Purpose: Working ETL script using ENHANCED_BADGE sheet with 2,506 records

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import re

def main():
    """Run ETL with correct sheet and processing"""
    print("🚀 ETL Final Working Version")
    print("=" * 50)
    
    # File paths
    base_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons")
    assignment_file = base_path / "Assignment_Master.xlsm"
    summons_file = base_path / "SUMMONS_MASTER.xlsx"
    
    print(f"📁 Working directory: {base_path}")
    
    # Load Assignment Master
    try:
        assignment_df = pd.read_excel(assignment_file)
        print(f"✅ Assignment Master loaded: {len(assignment_df)} rows")
        
        # Extract columns 2,3,4,5,7,8,16 (0-indexed: 1,2,3,4,6,7,15)
        cols_to_extract = [1, 2, 3, 4, 6, 7, 15]
        assignment_clean = assignment_df.iloc[:, cols_to_extract].copy()
        assignment_clean.columns = ['LAST_NAME', 'FIRST_NAME', 'BADGE_NUMBER', 
                                   'PADDED_BADGE_NUMBER', 'DIVISION', 'BUREAU', 'FULL_NAME']
        
        # Clean assignment data
        assignment_clean = assignment_clean[assignment_clean['LAST_NAME'] != 'LAST_NAME']
        assignment_clean = assignment_clean.dropna(subset=['BADGE_NUMBER'])
        assignment_clean['PADDED_BADGE_NUMBER'] = assignment_clean['PADDED_BADGE_NUMBER'].astype(str).str.zfill(4)
        
        print(f"✅ Assignment data cleaned: {len(assignment_clean)} officers")
        
    except Exception as e:
        print(f"❌ Error loading Assignment Master: {e}")
        return
    
    # Load Summons Master - ENHANCED_BADGE sheet
    try:
        summons_df = pd.read_excel(summons_file, sheet_name='ENHANCED_BADGE')
        print(f"✅ Summons data loaded from ENHANCED_BADGE: {len(summons_df)} rows")
        print(f"📊 Columns: {list(summons_df.columns)}")
        
        # The ENHANCED_BADGE sheet has OFFICER_NAME column
        if 'OFFICER_NAME' in summons_df.columns:
            print("🔍 Found OFFICER_NAME column")
            
            # Extract badge numbers from officer names
            def extract_badge_number(officer_name):
                if pd.isna(officer_name):
                    return None
                
                # Look for patterns like "P.O. SMITH 1234" or badge numbers
                patterns = [
                    r'(\d{4})$',  # 4 digits at end
                    r'(\d{3})$',  # 3 digits at end  
                    r'#(\d{3,4})',  # #123 or #1234
                    r'\s(\d{3,4})\s*$',  # ending with space + digits
                ]
                
                for pattern in patterns:
                    match = re.search(pattern, str(officer_name))
                    if match:
                        return match.group(1).zfill(4)
                
                return None
            
            # Extract and pad badge numbers
            summons_df['EXTRACTED_BADGE'] = summons_df['OFFICER_NAME'].apply(extract_badge_number)
            summons_df['PADDED_BADGE_NUMBER'] = summons_df['EXTRACTED_BADGE']
            
            # Show extraction results
            extracted_count = summons_df['PADDED_BADGE_NUMBER'].notna().sum()
            print(f"🔢 Badge numbers extracted: {extracted_count} out of {len(summons_df)} records")
            
            # Show some examples
            sample_extractions = summons_df[['OFFICER_NAME', 'PADDED_BADGE_NUMBER']].dropna().head()
            print("📝 Sample extractions:")
            for _, row in sample_extractions.iterrows():
                print(f"   {row['OFFICER_NAME']} → {row['PADDED_BADGE_NUMBER']}")
        
        else:
            print("❌ No OFFICER_NAME column found")
            return
            
    except Exception as e:
        print(f"❌ Error loading Summons Master: {e}")
        return
    
    # Merge with assignment data
    try:
        merged_df = summons_df.merge(assignment_clean, on='PADDED_BADGE_NUMBER', how='left')
        
        # Calculate match statistics
        total_records = len(merged_df)
        matched_records = merged_df['FULL_NAME'].notna().sum()
        match_rate = (matched_records / total_records) * 100
        
        print(f"✅ Merge complete:")
        print(f"   Total records: {total_records:,}")
        print(f"   Matched officers: {matched_records:,}")
        print(f"   Match rate: {match_rate:.1f}%")
        
        # Add processing metadata
        merged_df['PROCESSED_TIMESTAMP'] = datetime.now()
        merged_df['DATA_SOURCE'] = 'ENHANCED_BADGE'
        merged_df['ETL_VERSION'] = 'v3.0'
        
        # Add violation categorization
        if 'SUMMONS_TYPE' in merged_df.columns:
            merged_df['VIOLATION_CATEGORY'] = merged_df['SUMMONS_TYPE'].fillna('UNKNOWN')
        else:
            merged_df['VIOLATION_CATEGORY'] = 'ENFORCEMENT_ACTION'
        
        # Show unique officers
        unique_officers = merged_df['FULL_NAME'].nunique()
        print(f"👮 Unique officers: {unique_officers}")
        
        # Show date range if available
        if 'ISSUE_DATE' in merged_df.columns:
            merged_df['ISSUE_DATE'] = pd.to_datetime(merged_df['ISSUE_DATE'], errors='coerce')
            date_min = merged_df['ISSUE_DATE'].min()
            date_max = merged_df['ISSUE_DATE'].max()
            print(f"📅 Date range: {date_min} to {date_max}")
        
    except Exception as e:
        print(f"❌ Error merging data: {e}")
        return
    
    # Save final output
    try:
        output_file = base_path / "summons_powerbi_latest.xlsx"
        
        # Clean up data types for Excel compatibility
        for col in merged_df.columns:
            if merged_df[col].dtype == 'object':
                merged_df[col] = merged_df[col].astype(str).replace('nan', '')
        
        # Replace inf values
        merged_df = merged_df.replace([np.inf, -np.inf], 0)
        
        # Save to Excel
        merged_df.to_excel(output_file, index=False, sheet_name='Police_Analytics_Data')
        
        print(f"🎯 SUCCESS! Output saved: {output_file}")
        print(f"📊 File size: {output_file.stat().st_size / 1024:.1f} KB")
        
        # Final summary
        print(f"\n📋 FINAL SUMMARY:")
        print(f"   📁 Output File: summons_powerbi_latest.xlsx")
        print(f"   📊 Total Records: {len(merged_df):,}")
        print(f"   👮 Matched Officers: {matched_records:,} ({match_rate:.1f}%)")
        print(f"   🎯 Ready for Power BI: YES")
        print(f"   ⏰ Processed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        return output_file
        
    except Exception as e:
        print(f"❌ Error saving to Excel: {e}")
        
        # Try CSV fallback
        try:
            csv_file = base_path / "summons_powerbi_latest.csv"
            merged_df.to_csv(csv_file, index=False)
            print(f"💾 Saved as CSV instead: {csv_file}")
            return csv_file
        except Exception as e2:
            print(f"❌ CSV save also failed: {e2}")
            return None

if __name__ == "__main__":
    result = main()
    if result:
        print(f"\n🚀 READY FOR POWER BI CONNECTION!")
        print(f"📂 Connect to: {result}")
    else:
        print(f"\n❌ ETL FAILED - Check errors above")
