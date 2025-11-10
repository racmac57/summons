# Save as: investigate_court_data.py
# This will help us find the best court data file to use

import pandas as pd
from pathlib import Path

def investigate_court_files():
    print("🔍 INVESTIGATING COURT DATA FILES")
    print("=" * 50)
    
    base_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack")
    
    # Check all possible court file locations
    possible_locations = [
        base_path / "InBox",
        base_path / "_MONTHLY_DATA" / "_EXPORTS" / "Summons" / "Court",
        base_path / "05_EXPORTS",
        base_path,  # Root directory
    ]
    
    court_files = []
    
    for location in possible_locations:
        if location.exists():
            print(f"\n📁 Checking: {location}")
            
            # Find Excel files
            for pattern in ["*.xlsx", "*.xls"]:
                files = list(location.glob(pattern))
                for file in files:
                    if any(keyword in file.name.upper() for keyword in ['ATS', 'COURT', 'SUMMONS', '2024', '2025']):
                        court_files.append(file)
                        print(f"   📄 {file.name}")
    
    if not court_files:
        print("❌ No court files found!")
        return
    
    print(f"\n🔍 ANALYZING {len(court_files)} COURT FILES:")
    print("=" * 50)
    
    best_file = None
    max_records = 0
    
    for file_path in court_files:
        try:
            print(f"\n📊 Analyzing: {file_path.name}")
            
            # Try reading with different skip rows
            for skip_rows in [0, 1, 2, 3, 4, 5]:
                try:
                    df = pd.read_excel(file_path, skiprows=skip_rows)
                    
                    if len(df) > 10:  # Has substantial data
                        # Check for badge column
                        first_col = df.columns[0]
                        sample_data = df[first_col].dropna().astype(str).head(10)
                        
                        # Count numeric values (likely badge numbers)
                        numeric_count = sum(1 for x in sample_data if x.isdigit() and len(x) <= 4)
                        
                        print(f"   Skip {skip_rows}: {len(df)} rows, {numeric_count} numeric badges")
                        
                        if len(df) > max_records and numeric_count > 5:
                            max_records = len(df)
                            best_file = file_path
                            best_skip = skip_rows
                            
                    break  # Found readable format
                    
                except Exception as e:
                    continue
            
        except Exception as e:
            print(f"   ❌ Error reading: {e}")
    
    if best_file:
        print(f"\n🏆 BEST FILE FOUND:")
        print(f"   📄 File: {best_file.name}")
        print(f"   📊 Records: {max_records}")
        print(f"   🔢 Skip rows: {best_skip}")
        
        # Show sample data
        try:
            df = pd.read_excel(best_file, skiprows=best_skip)
            print(f"\n📋 Sample data from {best_file.name}:")
            print(f"   Columns: {list(df.columns[:5])}")
            
            first_col = df.columns[0]
            sample_badges = df[first_col].dropna().astype(str).head(10).tolist()
            print(f"   Sample badges: {sample_badges}")
            
            # Check for Lopez
            if len(df.columns) > 1:
                text_cols = [col for col in df.columns if df[col].dtype == 'object']
                lopez_found = False
                
                for col in text_cols[:3]:  # Check first 3 text columns
                    lopez_matches = df[df[col].astype(str).str.contains('LOPEZ|ANDRES', case=False, na=False)]
                    if len(lopez_matches) > 0:
                        print(f"   ✅ Found Lopez references in column: {col}")
                        lopez_found = True
                        break
                
                if not lopez_found:
                    print(f"   ⚠️  No Lopez found in this file")
            
            return best_file, best_skip
            
        except Exception as e:
            print(f"   ❌ Error analyzing best file: {e}")
    
    else:
        print("\n❌ No suitable court files found!")
        print("💡 Available files:")
        for file_path in court_files:
            print(f"   📄 {file_path}")
    
    return None, None

if __name__ == "__main__":
    best_file, skip_rows = investigate_court_files()
    
    if best_file:
        print(f"\n🚀 RECOMMENDATION:")
        print(f"Update your main script to use:")
        print(f"   File: {best_file}")
        print(f"   Skip rows: {skip_rows}")
    else:
        print(f"\n💡 NEXT STEPS:")
        print(f"1. Check if you have a larger court export file")
        print(f"2. Look for files with 'ATS', '2024', or '2025' in the name")
        print(f"3. Ensure the file contains badge numbers and officer data")