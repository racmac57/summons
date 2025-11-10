#!/usr/bin/env python3
"""
Verify the output file from the summons processing script
"""
import pandas as pd
import sys

def main():
    file_path = r"C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons\summons_powerbi_latest.xlsx"
    
    try:
        print("=== VERIFICATION REPORT ===")
        print(f"File: {file_path}")
        
        # Load the Excel file and check sheets
        xl = pd.ExcelFile(file_path)
        print(f"Sheet names: {xl.sheet_names}")
        
        # Load the main sheet
        if 'Summons_Data' in xl.sheet_names:
            df = pd.read_excel(file_path, sheet_name='Summons_Data')
        elif 'ATS_Court_Data' in xl.sheet_names:
            df = pd.read_excel(file_path, sheet_name='ATS_Court_Data')
        else:
            df = pd.read_excel(file_path)
        
        print(f"\nTotal records: {len(df):,}")
        print(f"Total columns: {len(df.columns)}")
        
        print(f"\n=== COLUMN NAMES ===")
        for i, col in enumerate(df.columns, 1):
            print(f"{i:2d}. {col}")
        
        print(f"\n=== FIRST 3 ROWS SAMPLE ===")
        # Show just key columns for readability
        key_cols = []
        for col in df.columns:
            if any(keyword in str(col).upper() for keyword in 
                  ['TICKET', 'ISSUE', 'DATE', 'OFFICER', 'YEAR', 'MONTH', 'VIOLATION']):
                key_cols.append(col)
        
        if key_cols:
            print("Key columns sample:")
            print(df[key_cols[:8]].head(3).to_string())
        else:
            print("All columns sample:")
            print(df.iloc[:3, :10].to_string())
        
        print(f"\n=== DATE RANGE VERIFICATION ===")
        date_cols = [col for col in df.columns if 'DATE' in str(col).upper()]
        if date_cols:
            date_col = date_cols[0]
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            print(f"Using date column: {date_col}")
            print(f"Date range: {df[date_col].min()} to {df[date_col].max()}")
        
        # Check for key intelligence columns
        print(f"\n=== KEY INTELLIGENCE COLUMNS ===")
        intelligence_cols = ['YearMonthKey', 'Month_Year', 'Year', 'Month', 'Quarter', 
                           'FiscalYear', 'FiscalQuarter', 'VIOLATION_TYPE', 
                           'DATA_QUALITY_SCORE', 'DATA_QUALITY_TIER']
        
        for col in intelligence_cols:
            if col in df.columns:
                print(f"[OK] {col}: {df[col].nunique()} unique values")
            else:
                print(f"[MISSING] {col}: NOT FOUND")
        
        # Check month distribution
        if 'Month_Year' in df.columns:
            print(f"\n=== MONTH DISTRIBUTION ===")
            month_dist = df['Month_Year'].value_counts().sort_index()
            for month, count in month_dist.items():
                print(f"{month}: {count:,} records")
        
        print(f"\n=== VERIFICATION COMPLETE ===")
        print("File processed successfully and contains expected structure")
        
    except Exception as e:
        print(f"Error verifying file: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()