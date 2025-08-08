# Diagnostic script for June 2025 data structure
# Save as: june_data_diagnostic.py

import pandas as pd
from pathlib import Path

def diagnose_june_data():
    print("🔍 DIAGNOSING JUNE 2025 DATA STRUCTURE")
    print("=" * 50)
    
    base_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack")
    court_file = base_path / "03_Staging" / "Summons" / "summons_powerbi_final_with_real_june.xlsx"
    
    try:
        # Load the data
        print(f"📊 Loading: {court_file.name}")
        court_df = pd.read_excel(court_file, skiprows=0)
        
        print(f"✅ Data loaded: {len(court_df)} total records")
        
        # Show ALL columns
        print(f"\n📋 ALL COLUMNS ({len(court_df.columns)}):")
        for i, col in enumerate(court_df.columns, 1):
            print(f"   {i:2d}. {col}")
        
        # Filter for June 2025
        court_df['ISSUE_DATE'] = pd.to_datetime(court_df['ISSUE_DATE'], errors='coerce')
        june_2025_data = court_df[
            (court_df['ISSUE_DATE'].dt.month == 6) & 
            (court_df['ISSUE_DATE'].dt.year == 2025)
        ]
        
        print(f"\n📊 June 2025 records: {len(june_2025_data)}")
        
        # Show sample data from first few rows
        print(f"\n📋 SAMPLE JUNE 2025 DATA (first 3 rows):")
        sample_cols = ['PADDED_BADGE_NUMBER', 'OFFICER_DISPLAY_NAME', 'WG1', 'WG2', 'WG3', 'ISSUE_DATE']
        available_cols = [col for col in sample_cols if col in june_2025_data.columns]
        
        sample_data = june_2025_data[available_cols].head(3)
        for idx, row in sample_data.iterrows():
            print(f"\nRow {idx}:")
            for col in available_cols:
                print(f"   {col}: {row[col]}")
        
        # Look for badge-related columns
        print(f"\n🔍 BADGE-RELATED COLUMNS:")
        badge_cols = [col for col in court_df.columns if 'BADGE' in col.upper()]
        if badge_cols:
            for col in badge_cols:
                sample_values = june_2025_data[col].dropna().head(5).tolist()
                print(f"   ✅ {col}: {sample_values}")
        else:
            print(f"   ❌ No columns with 'BADGE' in name found")
        
        # Look for columns that might contain badge numbers
        print(f"\n🔍 POTENTIAL BADGE COLUMNS (by content):")
        for col in june_2025_data.columns:
            if june_2025_data[col].dtype in ['object', 'int64', 'float64']:
                # Check if values look like badge numbers (1-4 digits)
                sample_values = june_2025_data[col].dropna().astype(str).head(10)
                numeric_values = []
                for val in sample_values:
                    val_clean = str(val).strip()
                    if val_clean.isdigit() and 1 <= len(val_clean) <= 4:
                        numeric_values.append(val_clean)
                
                if len(numeric_values) >= 3:  # At least 3 values look like badge numbers
                    print(f"   🎯 {col}: {numeric_values[:5]} (potential badge column)")
        
        # Check for Andres Lopez specifically
        print(f"\n🔍 ANDRES LOPEZ CHECK:")
        lopez_data = june_2025_data[
            june_2025_data['OFFICER_DISPLAY_NAME'].str.contains('LOPEZ', na=False, case=False) &
            june_2025_data['OFFICER_DISPLAY_NAME'].str.contains('ANDRES', na=False, case=False)
        ]
        
        if len(lopez_data) > 0:
            print(f"   ✅ Found {len(lopez_data)} Andres Lopez records")
            lopez_sample = lopez_data[['PADDED_BADGE_NUMBER', 'OFFICER_DISPLAY_NAME', 'WG2']].iloc[0]
            print(f"   📋 Sample record:")
            print(f"      Badge: {lopez_sample['PADDED_BADGE_NUMBER']}")
            print(f"      Name: {lopez_sample['OFFICER_DISPLAY_NAME']}")
            print(f"      Assignment: {lopez_sample['WG2']}")
            
            if 'SCHOOL' in str(lopez_sample['WG2']).upper():
                print(f"      ❌ PROBLEM: Still shows School threat assignment!")
            else:
                print(f"      ✅ Assignment looks correct")
        else:
            print(f"   ❌ Andres Lopez not found in June 2025 data")
        
        # Check unique assignments
        print(f"\n📋 UNIQUE WG2 ASSIGNMENTS IN JUNE DATA:")
        unique_assignments = june_2025_data['WG2'].value_counts()
        for assignment, count in unique_assignments.head(10).items():
            print(f"   {assignment}: {count} records")
        
        print(f"\n🎯 DIAGNOSIS COMPLETE!")
        return june_2025_data
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    data = diagnose_june_data()
    
    if data is not None:
        print(f"\n💡 FINDINGS:")
        print(f"1. The data structure shows PADDED_BADGE_NUMBER column exists")
        print(f"2. Check if assignments are already correct or need fixing")
        print(f"3. Badge column should be 'PADDED_BADGE_NUMBER'")
        print(f"4. Ready to create corrected processing script")