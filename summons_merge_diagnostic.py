#!/usr/bin/env python3
"""
// 🕒 2025-10-14-19-30-00
// Project: Summons_Analytics/summons_merge_diagnostic.py
// Author: R. A. Carucci
// Purpose: Diagnose format differences between gold standard and September 2025 summons data
"""

import pandas as pd
import os
from datetime import datetime

# File paths
BASE_PATH = r"C:\Users\carucci_r\OneDrive - City of Hackensack"
GOLD_STANDARD = os.path.join(BASE_PATH, r"02_ETL_Scripts\Summons\backfill_data\25_08_Hackensack Police Department - Summons Dashboard.csv")
SEPT_2025 = os.path.join(BASE_PATH, r"02_ETL_Scripts\Summons\visual_export\25_09_Hackensack Police Department - Summons Dashboard.csv")
RAW_ETICKET = os.path.join(BASE_PATH, r"05_EXPORTS\_Summons\E_Ticket\25_09_e_ticketexport.csv")

def print_header(title):
    """Print formatted section header"""
    print("\n" + "=" * 100)
    print(title)
    print("=" * 100)

def print_subheader(title):
    """Print formatted subsection header"""
    print("\n" + "-" * 100)
    print(title)
    print("-" * 100)

def analyze_dataframe(df, name):
    """Analyze and display DataFrame structure"""
    print(f"\n📊 {name}")
    print(f"   Shape: {df.shape[0]} rows × {df.shape[1]} columns")
    print(f"   Columns: {list(df.columns)}")
    print(f"\n   Data Types:")
    for col, dtype in df.dtypes.items():
        print(f"      {col}: {dtype}")
    
    if 'TYPE' in df.columns:
        print(f"\n   TYPE Distribution:")
        for ticket_type, count in df['TYPE'].value_counts().items():
            print(f"      {ticket_type}: {count:,}")
    
    if 'Month_Year' in df.columns:
        print(f"\n   Month_Year Values:")
        for month_year in sorted(df['Month_Year'].unique()):
            count = len(df[df['Month_Year'] == month_year])
            print(f"      {month_year}: {count} rows")
    
    if 'Count of TICKET_NUMBER' in df.columns:
        print(f"\n   Total Tickets: {df['Count of TICKET_NUMBER'].sum():,}")
    
    return df

def main():
    """Main diagnostic routine"""
    print_header("🔍 SUMMONS DATA MERGE DIAGNOSTIC REPORT")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S EST')}\n")
    
    # Check file existence
    print("📁 Checking file existence...")
    for label, path in [("Gold Standard", GOLD_STANDARD), 
                        ("Sept 2025", SEPT_2025), 
                        ("Raw E-Ticket", RAW_ETICKET)]:
        exists = "✅" if os.path.exists(path) else "❌"
        print(f"   {exists} {label}: {path}")
    
    # STEP 1: Analyze Gold Standard
    print_header("📋 STEP 1: GOLD STANDARD ANALYSIS (Sept 2024 - Aug 2025)")
    gold_df = pd.read_csv(GOLD_STANDARD)
    analyze_dataframe(gold_df, "Gold Standard Format")
    
    print(f"\n   Sample Records (First 3):")
    print(gold_df.head(3).to_string(index=False))
    
    # STEP 2: Analyze September 2025
    print_header("📊 STEP 2: SEPTEMBER 2025 OUTPUT ANALYSIS")
    sept_df = pd.read_csv(SEPT_2025)
    analyze_dataframe(sept_df, "September 2025 Format")
    
    print(f"\n   Sample Records (First 3):")
    print(sept_df.head(3).to_string(index=False))
    
    # STEP 3: Schema Comparison
    print_header("🔍 STEP 3: SCHEMA COMPARISON")
    
    gold_cols = set(gold_df.columns)
    sept_cols = set(sept_df.columns)
    
    print("\n📌 Column Name Comparison:")
    if gold_cols == sept_cols:
        print("   ✅ Column names MATCH perfectly")
    else:
        print("   ❌ Column names DO NOT MATCH")
        if gold_cols - sept_cols:
            print(f"   Missing in Sept 2025: {gold_cols - sept_cols}")
        if sept_cols - gold_cols:
            print(f"   Extra in Sept 2025: {sept_cols - gold_cols}")
    
    print("\n📌 Data Type Comparison:")
    issues = []
    for col in gold_df.columns:
        if col in sept_df.columns:
            gold_type = gold_df[col].dtype
            sept_type = sept_df[col].dtype
            match = "✅" if gold_type == sept_type else "❌"
            print(f"   {match} {col}: Gold={gold_type}, Sept={sept_type}")
            if gold_type != sept_type:
                issues.append(f"{col} type mismatch: {gold_type} vs {sept_type}")
    
    # STEP 4: Validate Totals
    print_header("📈 STEP 4: VALIDATE SEPTEMBER 2025 TOTALS")
    
    expected = {'M': 565, 'P': 4025, 'C': 9}
    sept_totals = sept_df.groupby('TYPE')['Count of TICKET_NUMBER'].sum().to_dict()
    
    print("\n📊 Ticket Count Validation:")
    print(f"{'TYPE':<6} {'Expected':>10} {'Actual':>10} {'Difference':>12} {'Status':>8}")
    print("-" * 50)
    
    total_match = True
    for ticket_type in sorted(expected.keys()):
        exp = expected[ticket_type]
        act = sept_totals.get(ticket_type, 0)
        diff = act - exp
        status = "✅ PASS" if act == exp else "❌ FAIL"
        print(f"{ticket_type:<6} {exp:>10,} {act:>10,} {diff:>+12,} {status:>8}")
        if act != exp:
            total_match = False
            issues.append(f"{ticket_type} count mismatch: expected {exp:,}, got {act:,}")
    
    total_expected = sum(expected.values())
    total_actual = sum(sept_totals.values())
    print("-" * 50)
    print(f"{'TOTAL':<6} {total_expected:>10,} {total_actual:>10,} {total_actual - total_expected:>+12,}")
    
    # STEP 5: Test Merge
    print_header("🔗 STEP 5: MERGE COMPATIBILITY TEST")
    
    try:
        merged_df = pd.concat([gold_df, sept_df], ignore_index=True)
        print(f"\n   ✅ Merge SUCCESSFUL!")
        print(f"   Combined shape: {merged_df.shape[0]} rows × {merged_df.shape[1]} columns")
        
        print(f"\n   📅 Month_Year Coverage in Merged Data:")
        month_counts = merged_df['Month_Year'].value_counts().sort_index()
        for month, count in month_counts.items():
            print(f"      {month}: {count} rows")
        
        print(f"\n   📊 Total Tickets by Month:")
        monthly_tickets = merged_df.groupby('Month_Year')['Count of TICKET_NUMBER'].sum().sort_index()
        for month, tickets in monthly_tickets.items():
            print(f"      {month}: {tickets:,} tickets")
            
    except Exception as e:
        print(f"\n   ❌ Merge FAILED: {str(e)}")
        issues.append(f"Merge error: {str(e)}")
    
    # SUMMARY
    print_header("📋 DIAGNOSTIC SUMMARY")
    
    if not issues:
        print("\n✅ NO ISSUES DETECTED")
        print("   - Schema matches perfectly")
        print("   - Totals validate correctly")
        print("   - Merge compatible")
        print("\n   September 2025 data is ready for use!")
    else:
        print("\n❌ ISSUES DETECTED:")
        for i, issue in enumerate(issues, 1):
            print(f"   {i}. {issue}")
        
        print("\n🔧 RECOMMENDED ACTIONS:")
        if any("type mismatch" in i for i in issues):
            print("   - Fix data type mismatches (likely Count of TICKET_NUMBER should be int)")
        if any("count mismatch" in i for i in issues):
            print("   - Verify Python script totals match e-ticket raw data")
            print("   - Check for classification logic errors (P/M/C assignment)")
        if any("Merge error" in i for i in issues):
            print("   - Resolve schema incompatibilities before attempting merge")
    
    # Additional diagnostics if issues found
    if issues and 'Count of TICKET_NUMBER' in sept_df.columns:
        print_header("🔬 ADDITIONAL DIAGNOSTICS")
        
        print("\n📊 September 2025 - Detailed Breakdown:")
        detail = sept_df.groupby(['Month_Year', 'TYPE'])['Count of TICKET_NUMBER'].sum().reset_index()
        print(detail.to_string(index=False))
        
        print("\n🔍 Sample Values - Count of TICKET_NUMBER:")
        print(f"   First 10 values: {list(sept_df['Count of TICKET_NUMBER'].head(10))}")
        print(f"   Data type: {sept_df['Count of TICKET_NUMBER'].dtype}")
        print(f"   Has NaN: {sept_df['Count of TICKET_NUMBER'].isna().any()}")
        
        if sept_df['Count of TICKET_NUMBER'].dtype == 'object':
            print("\n   ⚠️  Count is stored as text/object - this is likely the issue!")
            print("   Attempting conversion to numeric...")
            sept_df['Count of TICKET_NUMBER'] = pd.to_numeric(sept_df['Count of TICKET_NUMBER'], errors='coerce')
            print(f"   After conversion: {sept_df['Count of TICKET_NUMBER'].dtype}")
            print(f"   Conversion errors (NaN): {sept_df['Count of TICKET_NUMBER'].isna().sum()}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ FATAL ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
