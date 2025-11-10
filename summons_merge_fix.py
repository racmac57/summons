#!/usr/bin/env python3
"""
// 🕒 2025-10-14-19-32-00
// Project: Summons_Analytics/summons_merge_fix.py
// Author: R. A. Carucci
// Purpose: Automatically correct September 2025 summons data to match gold standard format
"""

import pandas as pd
import os
from datetime import datetime
import shutil

# File paths
BASE_PATH = r"C:\Users\carucci_r\OneDrive - City of Hackensack"
GOLD_STANDARD = os.path.join(BASE_PATH, r"02_ETL_Scripts\Summons\backfill_data\25_08_Hackensack Police Department - Summons Dashboard.csv")
SEPT_2025 = os.path.join(BASE_PATH, r"02_ETL_Scripts\Summons\visual_export\25_09_Hackensack Police Department - Summons Dashboard.csv")

def print_header(title):
    """Print formatted section header"""
    print("\n" + "=" * 100)
    print(title)
    print("=" * 100)

def backup_file(filepath):
    """Create timestamped backup of file"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    directory = os.path.dirname(filepath)
    filename = os.path.basename(filepath)
    name, ext = os.path.splitext(filename)
    backup_path = os.path.join(directory, f"{name}_BACKUP_{timestamp}{ext}")
    shutil.copy2(filepath, backup_path)
    print(f"   ✅ Backup created: {os.path.basename(backup_path)}")
    return backup_path

def main():
    """Main fix routine"""
    print_header("🔧 SUMMONS DATA FORMAT FIX")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S EST')}\n")
    
    # Load data
    print("📁 Loading files...")
    gold_df = pd.read_csv(GOLD_STANDARD)
    sept_df = pd.read_csv(SEPT_2025)
    print(f"   ✅ Gold standard: {gold_df.shape[0]} rows")
    print(f"   ✅ September 2025: {sept_df.shape[0]} rows")
    
    # Store original for comparison
    original_sept_df = sept_df.copy()
    
    print_header("🔍 STEP 1: IDENTIFY ISSUES")
    
    issues_found = []
    fixes_applied = []
    
    # Check column order
    if list(gold_df.columns) != list(sept_df.columns):
        issues_found.append("Column order mismatch")
        print("   ❌ Column order doesn't match gold standard")
        print(f"      Gold: {list(gold_df.columns)}")
        print(f"      Sept: {list(sept_df.columns)}")
    else:
        print("   ✅ Column order matches")
    
    # Check data types
    print("\n   Checking data types...")
    type_mismatches = []
    for col in gold_df.columns:
        if col in sept_df.columns:
            gold_type = gold_df[col].dtype
            sept_type = sept_df[col].dtype
            if gold_type != sept_type:
                type_mismatches.append((col, gold_type, sept_type))
                print(f"      ❌ {col}: Gold={gold_type}, Sept={sept_type}")
    
    if type_mismatches:
        issues_found.append(f"{len(type_mismatches)} data type mismatches")
    else:
        print("      ✅ All data types match")
    
    # Validate totals
    print("\n   Checking totals...")
    expected = {'M': 565, 'P': 4025, 'C': 9}
    sept_totals = sept_df.groupby('TYPE')['Count of TICKET_NUMBER'].sum().to_dict()
    
    total_mismatches = []
    for ticket_type, exp_count in expected.items():
        act_count = sept_totals.get(ticket_type, 0)
        if act_count != exp_count:
            total_mismatches.append((ticket_type, exp_count, act_count))
            print(f"      ❌ {ticket_type}: Expected {exp_count:,}, Got {act_count:,}")
    
    if total_mismatches:
        issues_found.append(f"{len(total_mismatches)} total count mismatches")
    else:
        print("      ✅ All totals match expected values")
    
    if not issues_found:
        print("\n✅ NO ISSUES FOUND - File already matches gold standard!")
        return
    
    # Apply fixes
    print_header("🔧 STEP 2: APPLY FIXES")
    
    # Fix 1: Reorder columns to match gold standard
    if list(gold_df.columns) != list(sept_df.columns):
        print("\n   🔧 Reordering columns...")
        sept_df = sept_df[gold_df.columns]
        fixes_applied.append("Reordered columns to match gold standard")
        print("      ✅ Columns reordered")
    
    # Fix 2: Convert data types
    if type_mismatches:
        print("\n   🔧 Converting data types...")
        for col, gold_type, sept_type in type_mismatches:
            try:
                if gold_type == 'int64':
                    sept_df[col] = pd.to_numeric(sept_df[col], errors='coerce').astype('int64')
                    print(f"      ✅ {col}: {sept_type} → int64")
                elif gold_type == 'float64':
                    sept_df[col] = pd.to_numeric(sept_df[col], errors='coerce').astype('float64')
                    print(f"      ✅ {col}: {sept_type} → float64")
                elif gold_type == 'object':
                    sept_df[col] = sept_df[col].astype(str)
                    print(f"      ✅ {col}: {sept_type} → object")
                fixes_applied.append(f"Converted {col} from {sept_type} to {gold_type}")
            except Exception as e:
                print(f"      ⚠️  {col}: Conversion failed - {str(e)}")
    
    # Validate after fixes
    print_header("✅ STEP 3: VALIDATE FIXES")
    
    print("\n   Checking totals after fixes...")
    new_sept_totals = sept_df.groupby('TYPE')['Count of TICKET_NUMBER'].sum().to_dict()
    
    print(f"\n   {'TYPE':<6} {'Expected':>10} {'Before Fix':>12} {'After Fix':>12} {'Status':>8}")
    print("   " + "-" * 52)
    
    all_valid = True
    for ticket_type in sorted(expected.keys()):
        exp = expected[ticket_type]
        before = sept_totals.get(ticket_type, 0)
        after = new_sept_totals.get(ticket_type, 0)
        status = "✅ PASS" if after == exp else "❌ FAIL"
        print(f"   {ticket_type:<6} {exp:>10,} {before:>12,} {after:>12,} {status:>8}")
        if after != exp:
            all_valid = False
    
    if not all_valid:
        print("\n   ⚠️  WARNING: Totals still don't match after fixes!")
        print("   This may indicate an issue in the Python script that generates Sept 2025 data.")
        print("   Review the e-ticket processing logic.")
    
    # Test merge
    print("\n   Testing merge compatibility...")
    try:
        merged_df = pd.concat([gold_df, sept_df], ignore_index=True)
        print(f"      ✅ Merge successful: {merged_df.shape[0]} total rows")
        fixes_applied.append("Verified merge compatibility")
    except Exception as e:
        print(f"      ❌ Merge failed: {str(e)}")
        all_valid = False
    
    # Save corrected file
    if fixes_applied:
        print_header("💾 STEP 4: SAVE CORRECTED FILE")
        
        # Create backup
        print("\n   Creating backup...")
        backup_path = backup_file(SEPT_2025)
        
        # Save corrected version
        print("\n   Saving corrected file...")
        sept_df.to_csv(SEPT_2025, index=False)
        print(f"      ✅ Saved: {os.path.basename(SEPT_2025)}")
        
        # Summary
        print_header("📊 FIX SUMMARY")
        print("\n✅ FIXES APPLIED:")
        for i, fix in enumerate(fixes_applied, 1):
            print(f"   {i}. {fix}")
        
        print(f"\n📁 FILES:")
        print(f"   Original (backup): {os.path.basename(backup_path)}")
        print(f"   Corrected: {os.path.basename(SEPT_2025)}")
        
        if all_valid:
            print("\n🎉 SUCCESS! September 2025 file now matches gold standard format.")
            print("   Ready for Power BI integration.")
        else:
            print("\n⚠️  PARTIAL SUCCESS: Some issues remain.")
            print("   Review validation report above for details.")
    else:
        print("\n   No fixes needed to be saved.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ FATAL ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
