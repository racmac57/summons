# Find June 2025 Data Script
# Save as: find_june_data.py

import pandas as pd
from pathlib import Path
from datetime import datetime

def find_june_2025_data():
    print("🔍 SEARCHING FOR JUNE 2025 COURT DATA")
    print("=" * 50)
    
    base_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack")
    
    # Check your uploaded files from earlier
    files_to_check = [
        # From your original documents
        base_path / "SUMMONS_MASTER.xlsx",
        base_path / "2025_04_ATS_Report.xlsx", 
        base_path / "Assignment_Master.xlsm",
        base_path / "24_ALL_ATS.xlsx",
        base_path / "Issued Cases JanDec 2024.xlsx",
        
        # From InBox (from investigation)
        base_path / "InBox" / "25_06_ATS.xlsx",  # This looks promising for June!
        base_path / "InBox" / "TEST_RUN_ATS.xlsx",
        base_path / "InBox" / "acs_ats_cases_by_agencies_20241209_042924.xls.xlsx",
        base_path / "InBox" / "Final_Summons_With_Descriptions.xlsx",
        
        # Check staging area
        base_path / "03_Staging" / "Summons" / "summons_powerbi_final_with_real_june.xlsx"  # This mentions JUNE!
    ]
    
    june_files = []
    
    for file_path in files_to_check:
        if file_path.exists():
            try:
                print(f"\n📊 Checking: {file_path.name}")
                
                # Try different skip rows
                for skip_rows in [0, 4]:
                    try:
                        df = pd.read_excel(file_path, skiprows=skip_rows)
                        
                        if 'ISSUE_DATE' in df.columns or any('DATE' in col.upper() for col in df.columns):
                            # Find date column
                            date_col = None
                            for col in df.columns:
                                if 'DATE' in col.upper() and 'ISSUE' in col.upper():
                                    date_col = col
                                    break
                            
                            if not date_col:
                                for col in df.columns:
                                    if 'DATE' in col.upper():
                                        date_col = col
                                        break
                            
                            if date_col:
                                # Convert to datetime and check for June 2025
                                df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
                                
                                # Check date ranges
                                valid_dates = df[date_col].dropna()
                                if len(valid_dates) > 0:
                                    min_date = valid_dates.min()
                                    max_date = valid_dates.max()
                                    
                                    # Check for June 2025 data
                                    june_2025_data = df[
                                        (df[date_col].dt.month == 6) & 
                                        (df[date_col].dt.year == 2025)
                                    ]
                                    
                                    print(f"   📅 Date range: {min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}")
                                    print(f"   📊 Total records: {len(df)}")
                                    print(f"   🎯 June 2025 records: {len(june_2025_data)}")
                                    
                                    if len(june_2025_data) > 0:
                                        june_files.append({
                                            'file': file_path,
                                            'skip_rows': skip_rows,
                                            'june_records': len(june_2025_data),
                                            'total_records': len(df),
                                            'date_column': date_col
                                        })
                                        print(f"   ✅ FOUND JUNE 2025 DATA!")
                                        
                                        # Show sample June dates
                                        sample_dates = june_2025_data[date_col].head().tolist()
                                        print(f"   📅 Sample June dates: {[d.strftime('%Y-%m-%d') for d in sample_dates]}")
                        
                        break  # Found readable format
                        
                    except Exception as e:
                        continue
                        
            except Exception as e:
                print(f"   ❌ Error reading: {e}")
    
    print(f"\n🎯 JUNE 2025 DATA SUMMARY:")
    print("=" * 40)
    
    if june_files:
        print(f"✅ Found {len(june_files)} files with June 2025 data!")
        
        # Sort by number of June records
        june_files.sort(key=lambda x: x['june_records'], reverse=True)
        
        for i, file_info in enumerate(june_files, 1):
            print(f"\n{i}. {file_info['file'].name}")
            print(f"   📊 June 2025 records: {file_info['june_records']:,}")
            print(f"   📋 Skip rows: {file_info['skip_rows']}")
            print(f"   📅 Date column: {file_info['date_column']}")
            print(f"   📁 Full path: {file_info['file']}")
        
        # Recommend the best file
        best_file = june_files[0]
        print(f"\n🏆 RECOMMENDED FILE:")
        print(f"   📄 File: {best_file['file']}")
        print(f"   📊 June records: {best_file['june_records']:,}")
        print(f"   🔧 Skip rows: {best_file['skip_rows']}")
        
        return best_file
        
    else:
        print("❌ No files found with June 2025 data!")
        print("\n💡 OPTIONS:")
        print("1. Check if you have a June 2025 court export file")
        print("2. Look for files named with '25_06', 'June', '06_2025', etc.")
        print("3. Check your court system exports for June 2025")
        print("4. Use a different month if June data doesn't exist")
        
        return None

if __name__ == "__main__":
    best_file = find_june_2025_data()
    
    if best_file:
        print(f"\n🚀 NEXT STEPS:")
        print(f"1. Run Python script on: {best_file['file'].name}")
        print(f"2. Update M-Code file path to use June corrected data")
        print(f"3. Verify Andres Lopez shows Patrol Bureau assignment")
    else:
        print(f"\n🔍 SEARCH OTHER LOCATIONS:")
        print(f"1. Check court system exports folder")
        print(f"2. Look for recent downloads")
        print(f"3. Ask for June 2025 export from court system")