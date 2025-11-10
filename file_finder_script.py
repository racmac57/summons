# 🕒 2025-07-13-15-00-00
# Project: SummonsMaster/find_files_and_fix_paths.py
# Author: R. A. Carucci
# Purpose: Find Assignment_Master_V2.xlsx and update badge_assignment_fix.py with correct paths

import os
from pathlib import Path
import glob

def find_assignment_files():
    """Find Assignment_Master_V2.xlsx in all possible locations"""
    
    print("🔍 SEARCHING FOR ASSIGNMENT_MASTER_V2.XLSX")
    print("=" * 50)
    
    base_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack")
    
    # Search patterns
    search_patterns = [
        "**/Assignment_Master_V2.xlsx",
        "**/Assignment_Master.xlsx", 
        "**/Assignment*.xlsx"
    ]
    
    found_files = []
    
    for pattern in search_patterns:
        print(f"🔍 Searching for: {pattern}")
        matches = list(base_path.glob(pattern))
        for match in matches:
            if match.exists():
                found_files.append(match)
                print(f"   ✅ Found: {match}")
    
    if not found_files:
        print("❌ No assignment files found!")
        print("\n📁 Let's check what files ARE available...")
        
        # List directories that might contain the file
        possible_dirs = [
            base_path / "_ASSIGNMENTS",
            base_path / "_Hackensack_Data_Repository" / "ASSIGNED_SHIFT",
            base_path / "03_Staging",
            base_path / "02_ETL_Scripts" / "Summons",
            base_path
        ]
        
        for dir_path in possible_dirs:
            if dir_path.exists():
                print(f"\n📂 Contents of {dir_path}:")
                for item in dir_path.iterdir():
                    if item.is_file() and item.suffix in ['.xlsx', '.xls']:
                        print(f"   📄 {item.name}")
    
    return found_files

def find_court_files():
    """Find court export files"""
    
    print("\n🔍 SEARCHING FOR COURT EXPORT FILES")
    print("=" * 40)
    
    base_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack")
    
    search_patterns = [
        "**/2025_04_ATS_Report.xlsx",
        "**/25_06_ATS.xlsx",
        "**/24_ALL_ATS.xlsx",
        "**/*ATS*.xlsx"
    ]
    
    found_files = []
    
    for pattern in search_patterns:
        print(f"🔍 Searching for: {pattern}")
        matches = list(base_path.glob(pattern))
        for match in matches[:5]:  # Limit to first 5 matches
            if match.exists():
                found_files.append(match)
                print(f"   ✅ Found: {match}")
    
    return found_files

def update_script_with_correct_paths(assignment_file, court_file):
    """Update the badge_assignment_fix.py script with correct file paths"""
    
    script_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\02_ETL_Scripts\Summons\badge_assignment_fix.py")
    
    if not script_path.exists():
        print(f"❌ Script not found at: {script_path}")
        return False
    
    print(f"\n🔧 UPDATING SCRIPT WITH CORRECT PATHS")
    print("=" * 45)
    
    # Read current script
    with open(script_path, 'r', encoding='utf-8') as f:
        script_content = f.read()
    
    # Update assignment file path
    if assignment_file:
        old_assignment_pattern = 'assignment_files = ['
        new_assignment_files = f'''assignment_files = [
        Path(r"{assignment_file}"),
        base_path / "_ASSIGNMENTS" / "Assignment_Master_V2.xlsx",
        base_path / "_Hackensack_Data_Repository" / "ASSIGNED_SHIFT" / "Assignment_Master_V2.xlsx",
        base_path / "Assignment_Master_V2.xlsx"  # Current directory'''
        
        script_content = script_content.replace(
            'assignment_files = [\n        base_path / "_ASSIGNMENTS" / "Assignment_Master_V2.xlsx",',
            new_assignment_files
        )
    
    # Update court file path
    if court_file:
        old_court_pattern = 'court_files = ['
        new_court_files = f'''court_files = [
        Path(r"{court_file}"),
        base_path / "_MONTHLY_DATA" / "_EXPORTS" / "Summons" / "Court" / "2025_04_ATS_Report.xlsx",
        base_path / "05_EXPORTS" / "_Summons" / "Court" / "25_06_ATS.xlsx",
        base_path / "2025_04_ATS_Report.xlsx"  # Current directory'''
        
        script_content = script_content.replace(
            'court_files = [\n        base_path / "_MONTHLY_DATA" / "_EXPORTS" / "Summons" / "Court" / "2025_04_ATS_Report.xlsx",',
            new_court_files
        )
    
    # Save updated script
    backup_path = script_path.with_suffix('.py.backup')
    script_path.rename(backup_path)
    print(f"📁 Backup created: {backup_path}")
    
    with open(script_path, 'w', encoding='utf-8') as f:
        f.write(script_content)
    
    print(f"✅ Script updated: {script_path}")
    return True

def create_simple_test_script(assignment_file, court_file):
    """Create a simple test script with the found file paths"""
    
    test_script = f'''# Simple test script with correct paths
import pandas as pd
from pathlib import Path

def test_file_access():
    """Test if we can access the files"""
    
    assignment_file = Path(r"{assignment_file}")
    court_file = Path(r"{court_file}")
    
    print("📋 Testing file access...")
    
    # Test assignment file
    if assignment_file.exists():
        print(f"✅ Assignment file accessible: {{assignment_file}}")
        try:
            df = pd.read_excel(assignment_file)
            print(f"   📊 Rows: {{len(df)}}, Columns: {{len(df.columns)}}")
            print(f"   📋 Columns: {{list(df.columns)}}")
        except Exception as e:
            print(f"   ❌ Error reading: {{e}}")
    else:
        print(f"❌ Assignment file not found: {{assignment_file}}")
    
    # Test court file  
    if court_file.exists():
        print(f"✅ Court file accessible: {{court_file}}")
        try:
            df = pd.read_excel(court_file, skiprows=4)
            print(f"   📊 Rows: {{len(df)}}, Columns: {{len(df.columns)}}")
        except Exception as e:
            print(f"   ❌ Error reading: {{e}}")
    else:
        print(f"❌ Court file not found: {{court_file}}")

if __name__ == "__main__":
    test_file_access()
'''
    
    test_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\02_ETL_Scripts\Summons\test_file_access.py")
    
    with open(test_path, 'w', encoding='utf-8') as f:
        f.write(test_script)
    
    print(f"\n💾 Test script created: {test_path}")
    print("🚀 Run with: python test_file_access.py")
    
    return test_path

def main():
    """Main execution function"""
    
    print("🔍 FILE FINDER AND PATH FIXER")
    print("=" * 50)
    
    # Find assignment files
    assignment_files = find_assignment_files()
    
    # Find court files  
    court_files = find_court_files()
    
    if not assignment_files:
        print("\n❌ CRITICAL: No Assignment_Master_V2.xlsx found!")
        print("💡 Please ensure the file exists in your OneDrive folder")
        return False
    
    if not court_files:
        print("\n❌ CRITICAL: No court export files found!")
        print("💡 Please ensure you have a court export (.xlsx) file")
        return False
    
    # Use the first found files
    assignment_file = assignment_files[0]
    court_file = court_files[0]
    
    print(f"\n✅ SELECTED FILES:")
    print(f"   📋 Assignment: {assignment_file}")
    print(f"   📊 Court Data: {court_file}")
    
    # Create test script
    test_path = create_simple_test_script(assignment_file, court_file)
    
    # Update main script
    success = update_script_with_correct_paths(assignment_file, court_file)
    
    if success:
        print(f"\n🎯 NEXT STEPS:")
        print(f"1. Run test script: python test_file_access.py")
        print(f"2. If test passes, run: python badge_assignment_fix.py")
        print(f"3. Check for >85% assignment match rate")
    
    return success

if __name__ == "__main__":
    main()
