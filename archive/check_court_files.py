#!/usr/bin/env python3
# 🕒 2025-08-13-14-55-00
# Project: Court Files Check
# Author: R. A. Carucci
# Purpose: Check available court data files

import os
from pathlib import Path

def check_court_files():
    """Check what court data files are available"""
    print("🔍 CHECKING COURT DATA FILES")
    print("=" * 50)
    
    court_dir = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\05_EXPORTS\_Summons\Court")
    
    try:
        if court_dir.exists():
            print(f"✅ Court directory found: {court_dir}")
            
            files = list(court_dir.glob("*.xlsx"))
            print(f"📁 Excel files found: {len(files)}")
            
            for file in files:
                print(f"  📄 {file.name}")
                print(f"     Size: {file.stat().st_size:,} bytes")
                print(f"     Modified: {file.stat().st_mtime}")
                print()
        else:
            print(f"❌ Court directory not found: {court_dir}")
            
            # Check parent directory
            parent_dir = court_dir.parent
            if parent_dir.exists():
                print(f"📁 Parent directory exists: {parent_dir}")
                parent_files = list(parent_dir.glob("*"))
                print("📄 Files in parent directory:")
                for item in parent_files:
                    if item.is_file():
                        print(f"  📄 {item.name}")
                    else:
                        print(f"  📁 {item.name}/")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_court_files()
