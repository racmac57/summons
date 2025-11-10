#!/usr/bin/env python3
# 🕒 2025-06-28-20-35-00
# Project: Police_Analytics_Dashboard/emergency_file_finder
# Author: R. A. Carucci
# Purpose: Emergency script to locate and copy required files for July 1st deployment

import os
import shutil
from pathlib import Path
import pandas as pd

def find_and_copy_files():
    """Find required files and copy to staging area"""
    
    print("🔍 EMERGENCY FILE FINDER - July 1st Deployment")
    print("=" * 50)
    
    base_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack")
    staging_path = base_path / "03_Staging" / "Summons"
    staging_path.mkdir(parents=True, exist_ok=True)
    
    # Files we need to find
    required_files = {
        'Assignment_Master.xlsm': [
            r"C:\Users\carucci_r\OneDrive - City of Hackensack\_Hackensack_Data_Repository\ASSIGNED_SHIFT\Assignment_Master.xlsm",
            base_path / "_ASSIGNMENTS" / "Assignment_Master.xlsm",
            base_path / "Assignment_Master.xlsm"
        ],
        'SUMMONS_MASTER.xlsx': [
            r"C:\Users\carucci_r\OneDrive - City of Hackensack\_EXPORTS\Summons\Court\SUMMONS_MASTER.xlsx",
            base_path / "_MONTHLY_DATA" / "_EXPORTS" / "Summons" / "SUMMONS_MASTER.xlsx",
            base_path / "SUMMONS_MASTER.xlsx"
        ],
        '2025_04_ATS_Report.xlsx': [
            r"C:\Users\carucci_r\OneDrive - City of Hackensack\_EXPORTS\ATS\2025_04_ATS_Report.xlsx",
            base_path / "_EXPORTS" / "ATS" / "2025_04_ATS_Report.xlsx",
            base_path / "2025_04_ATS_Report.xlsx"
        ]
    }
    
    found_files = {}
    
    # Search for each file
    for filename, possible_paths in required_files.items():
        print(f"\n🔎 Searching for {filename}...")
        
        found = False
        for path in possible_paths:
            path_obj = Path(path)
            if path_obj.exists():
                print(f"  ✅ FOUND: {path}")
                
                # Copy to staging
                destination = staging_path / filename
                try:
                    shutil.copy2(path_obj, destination)
                    print(f"  📁 COPIED to: {destination}")
                    found_files[filename] = str(destination)
                    found = True
                    break
                except Exception as e:
                    print(f"  ❌ Copy failed: {e}")
            else:
                print(f"  ❌ Not found: {path}")
        
        if not found:
            print(f"  🚨 {filename} NOT FOUND ANYWHERE!")
            # Try to find any similar files
            print(f"  🔍 Searching for similar files...")
            for search_dir in [base_path, base_path / "_EXPORTS", base_path / "_ASSIGNMENTS"]:
                if search_dir.exists():
                    similar_files = list(search_dir.rglob(f"*{filename.split('.')[0].lower()}*"))
                    for similar in similar_files[:3]:  # Show first 3 matches
                        print(f"    📄 Similar: {similar}")
    
    # Validate copied files
    print(f"\n📊 VALIDATION:")
    for filename, filepath in found_files.items():
        try:
            if filename.endswith('.xlsm') or filename.endswith('.xlsx'):
                df = pd.read_excel(filepath)
                print(f"  ✅ {filename}: {len(df)} rows loaded successfully")
            else:
                print(f"  ✅ {filename}: File exists")
        except Exception as e:
            print(f"  ❌ {filename}: Validation failed - {e}")
    
    # Generate emergency config
    if found_files:
        print(f"\n🔧 GENERATING EMERGENCY CONFIG...")
        
        emergency_config = f"""# Emergency config for July 1st deployment
data_sources:
  assignment_master: "{found_files.get('Assignment_Master.xlsm', 'NOT_FOUND')}"
  summons_master: "{found_files.get('SUMMONS_MASTER.xlsx', 'NOT_FOUND')}"
  ats_report: "{found_files.get('2025_04_ATS_Report.xlsx', 'NOT_FOUND')}"

outputs:
  summons_enriched: "{staging_path / 'summons_enriched.xlsx'}"
  summons_powerbi: "{staging_path / 'summons_powerbi_latest.xlsx'}"

staging:
  summons: "{staging_path}"

processing:
  fuzzy_match_threshold: 75
  badge_padding_length: 4
"""
        
        config_path = staging_path.parent.parent / "02_ETL_Scripts" / "Summons" / "emergency_config.yaml"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(config_path, 'w') as f:
            f.write(emergency_config)
        
        print(f"  ✅ Emergency config saved: {config_path}")
    
    # Summary
    print(f"\n🎯 SUMMARY:")
    print(f"  Files found: {len(found_files)}/3")
    print(f"  Files copied to: {staging_path}")
    
    if len(found_files) >= 2:  # Need at least Assignment Master + one data file
        print(f"  🟢 STATUS: READY TO PROCEED!")
        print(f"\n📋 NEXT STEPS:")
        print(f"  1. Run: cd '{staging_path.parent.parent / '02_ETL_Scripts' / 'Summons'}'")
        print(f"  2. Run: python summons_etl_enhanced.py")
        print(f"  3. Connect Power BI to: {staging_path / 'summons_powerbi_latest.xlsx'}")
    else:
        print(f"  🔴 STATUS: MISSING CRITICAL FILES!")
        print(f"\n🚨 MANUAL ACTION REQUIRED:")
        print(f"  1. Locate Assignment_Master.xlsm manually")
        print(f"  2. Locate SUMMONS_MASTER.xlsx manually") 
        print(f"  3. Copy them to: {staging_path}")
    
    return found_files

if __name__ == "__main__":
    found_files = find_and_copy_files()
