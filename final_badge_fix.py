# 🕒 2025-07-09-23-00-00
# Project: SummonsMaster/final_badge_fix.py
# Author: R. A. Carucci
# Purpose: Final fix for badge number format mismatch

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

def final_badge_format_fix():
    """Final fix for badge number format issues"""
    
    print("🔧 FINAL BADGE FORMAT FIX")
    print("=" * 60)
    
    # File paths
    base_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack")
    court_file = base_path / "05_EXPORTS" / "_Summons" / "Court" / "25_06_ATS.xlsx"
    assignment_file = base_path / "_Hackensack_Data_Repository" / "ASSIGNED_SHIFT" / "Assignment_Master_V2.xlsx"
    output_dir = base_path / "03_Staging" / "Summons"
    
    try:
        # Load assignment data
        print("📋 Loading Assignment Master V2...")
        assignment_df = pd.read_excel(assignment_file, sheet_name='Sheet1')
        print(f"✅ Assignment data loaded: {len(assignment_df)} officers")
        
        # Show both badge formats in assignment data
        print("📋 Assignment badge formats:")
        print(f"   BADGE_NUMBER samples: {assignment_df['BADGE_NUMBER'].head().tolist()}")
        print(f"   PADDED_BADGE_NUMBER samples: {assignment_df['PADDED_BADGE_NUMBER'].head().tolist()}")
        
        # Create both formats for joining
        assignment_df['BADGE_3DIGIT'] = assignment_df['BADGE_NUMBER'].astype(str).str.strip()  # 83, 135
        assignment_df['BADGE_4DIGIT'] = assignment_df['PADDED_BADGE_NUMBER'].astype(str).str.strip()  # 0083, 0135
        
        # Load court data
        print("📊 Loading ATS court data...")
        court_df = pd.read_excel(court_file, skiprows=4, header=None)
        print(f"✅ Court data loaded: {len(court_df)} rows")
        
        # Assign column names
        court_columns = [
            'BADGE_NUMBER_RAW', 'OFFICER_NAME_RAW', 'ORI', 'TICKET_NUMBER', 'ISSUE_DATE', 
            'VIOLATION_NUMBER', 'TYPE', 'STATUS', 'DISPOSITION_DATE', 'FIND_CD', 'PAYMENT_DATE',
            'ASSESSED_AMOUNT', 'FINE_AMOUNT', 'COST_AMOUNT', 'MISC_AMOUNT', 'TOTAL_PAID_AMOUNT', 'CITY_COST_AMOUNT'
        ]
        
        num_cols = min(len(court_columns), len(court_df.columns))
        court_df.columns = court_columns[:num_cols] + [f'Extra_Col_{i}' for i in range(num_cols, len(court_df.columns))]
        
        # Clean court badge numbers
        court_df['BADGE_CLEAN'] = court_df['BADGE_NUMBER_RAW'].astype(str).str.strip()
        court_df = court_df[court_df['BADGE_CLEAN'] != '0']  # Remove zero badges
        
        print(f"📋 Court badge samples: {court_df['BADGE_CLEAN'].head().tolist()}")
        print(f"📋 Court unique badges: {court_df['BADGE_CLEAN'].nunique()}")
        
        # Try multiple join strategies
        print("🔗 Attempting multiple join strategies...")
        
        # Strategy 1: Direct match on 3-digit format
        merged_df1 = court_df.merge(
            assignment_df[['BADGE_3DIGIT', 'FULL_NAME', 'FIRST_NAME', 'LAST_NAME', 'TITLE', 'TEAM', 'WG1', 'WG2', 'WG3']],
            left_on='BADGE_CLEAN',
            right_on='BADGE_3DIGIT',
            how='left',
            suffixes=('', '_assign1')
        )
        
        matches1 = merged_df1['FULL_NAME'].notna().sum()
        print(f"Strategy 1 (3-digit): {matches1} matches ({matches1/len(merged_df1)*100:.1f}%)")
        
        # Strategy 2: Pad court badges to 4 digits and match
        court_df['BADGE_PADDED'] = court_df['BADGE_CLEAN'].str.zfill(4)
        
        merged_df2 = court_df.merge(
            assignment_df[['BADGE_4DIGIT', 'FULL_NAME', 'FIRST_NAME', 'LAST_NAME', 'TITLE', 'TEAM', 'WG1', 'WG2', 'WG3']],
            left_on='BADGE_PADDED',
            right_on='BADGE_4DIGIT',
            how='left',
            suffixes=('', '_assign2')
        )
        
        matches2 = merged_df2['FULL_NAME'].notna().sum()
        print(f"Strategy 2 (4-digit): {matches2} matches ({matches2/len(merged_df2)*100:.1f}%)")
        
        # Use the better strategy
        if matches2 >= matches1:
            print("✅ Using 4-digit padded strategy")
            final_df = merged_df2.copy()
            final_df['PADDED_BADGE_NUMBER'] = final_df['BADGE_PADDED']
            match_count = matches2
        else:
            print("✅ Using 3-digit direct strategy")
            final_df = merged_df1.copy()
            final_df['PADDED_BADGE_NUMBER'] = final_df['BADGE_CLEAN'].str.zfill(4)
            match_count = matches1
        
        # Add metadata
        final_df['DIVISION'] = final_df['WG1']
        final_df['BUREAU'] = final_df['WG2']
        final_df['UNIT'] = final_df['WG3']
        final_df['ASSIGNMENT_FOUND'] = final_df['FULL_NAME'].notna()
        final_df['PROCESSED_TIMESTAMP'] = datetime.now()
        final_df['DATA_SOURCE'] = court_file.stem
        final_df['ETL_VERSION'] = 'v8.0_BADGE_FIXED'
        
        # Show successful matches
        if match_count > 0:
            print("\n📋 Successful matches found:")
            matches = final_df[final_df['ASSIGNMENT_FOUND'] == True][['PADDED_BADGE_NUMBER', 'FULL_NAME', 'DIVISION', 'BUREAU']].drop_duplicates()
            for _, row in matches.head(10).iterrows():
                print(f"   Badge {row['PADDED_BADGE_NUMBER']} → {row['FULL_NAME']} ({row['DIVISION']}, {row['BUREAU']})")
        
        # Clean data types
        financial_columns = ['TOTAL_PAID_AMOUNT', 'FINE_AMOUNT', 'COST_AMOUNT', 'MISC_AMOUNT']
        for col in financial_columns:
            if col in final_df.columns:
                final_df[col] = pd.to_numeric(final_df[col], errors='coerce').fillna(0)
        
        final_df['ISSUE_DATE'] = pd.to_datetime(final_df['ISSUE_DATE'], errors='coerce')
        final_df['VIOLATION_TYPE'] = final_df['TYPE'].map({'P': 'Parking', 'M': 'Moving'}).fillna('Unknown')
        
        # Export final dataset
        print("\n💾 Exporting corrected dataset...")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Select columns for Power BI
        powerbi_columns = [
            'PADDED_BADGE_NUMBER', 'FULL_NAME', 'FIRST_NAME', 'LAST_NAME', 'TITLE',
            'DIVISION', 'BUREAU', 'UNIT', 'TEAM',
            'TICKET_NUMBER', 'ISSUE_DATE', 'VIOLATION_NUMBER', 'VIOLATION_TYPE', 'TYPE', 'STATUS',
            'TOTAL_PAID_AMOUNT', 'FINE_AMOUNT', 'COST_AMOUNT', 'MISC_AMOUNT',
            'OFFICER_NAME_RAW', 'ASSIGNMENT_FOUND', 'PROCESSED_TIMESTAMP', 'DATA_SOURCE', 'ETL_VERSION'
        ]
        
        available_columns = [col for col in powerbi_columns if col in final_df.columns]
        export_df = final_df[available_columns].copy()
        
        # Final cleanup
        export_df['PADDED_BADGE_NUMBER'] = export_df['PADDED_BADGE_NUMBER'].astype(str)
        export_df['ASSIGNMENT_FOUND'] = export_df['ASSIGNMENT_FOUND'].astype(bool)
        
        # Export files
        output_file = output_dir / "summons_powerbi_latest.xlsx"
        backup_file = output_dir / f"summons_powerbi_FIXED_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        export_df.to_excel(output_file, index=False, sheet_name='ATS_Court_Data_Fixed')
        export_df.to_excel(backup_file, index=False, sheet_name='ATS_Court_Data_Fixed')
        
        # Calculate final statistics
        total_records = len(export_df)
        matched_records = export_df['ASSIGNMENT_FOUND'].sum()
        match_rate = (matched_records / total_records) * 100
        total_revenue = export_df['TOTAL_PAID_AMOUNT'].sum()
        unique_officers = export_df[export_df['ASSIGNMENT_FOUND'] == True]['FULL_NAME'].nunique()
        moving_count = (export_df['TYPE'] == 'M').sum()
        parking_count = (export_df['TYPE'] == 'P').sum()
        
        print(f"✅ SUCCESS! Files exported:")
        print(f"   - {output_file}")
        print(f"   - {backup_file}")
        
        print(f"\n🎯 FINAL CORRECTED RESULTS:")
        print(f"├── Total Enforcement Records: {total_records:,}")
        print(f"├── Assignment Match Rate: {match_rate:.1f}%")
        print(f"├── Officers with Data: {unique_officers}")
        print(f"├── Total Revenue: ${total_revenue:,.2f}")
        print(f"├── Moving Violations: {moving_count:,}")
        print(f"├── Parking Violations: {parking_count:,}")
        print(f"└── Data Source: {court_file.name}")
        
        if match_rate > 50:
            print(f"\n🎉 EXCELLENT! Match rate over 50% - Your dashboard will have rich assignment data!")
        elif match_rate > 20:
            print(f"\n✅ GOOD! Match rate over 20% - Solid assignment coverage for dashboard!")
        else:
            print(f"\n⚠️ Match rate low - May need to update Assignment Master with current officers")
        
        return export_df
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    result = final_badge_format_fix()
    if result is not None:
        print(f"\n🚀 BADGE FORMAT FIX COMPLETE!")
        print(f"📂 Refresh Power BI connection to see corrected data!")
        print(f"🎯 Your assignment data should now populate correctly!")
    else:
        print(f"\n❌ Fix failed. Check error details above.")