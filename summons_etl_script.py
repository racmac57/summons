# 🕒 2025-07-03-15-47-15
# Project: SummonsMaster/Production_ETL_Script
# Author: R. A. Carucci
# Purpose: Complete ETL workflow for court export processing with assignment integration

import pandas as pd
import numpy as np
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

def process_summons_master_complete():
    """
    Complete SummonsMaster ETL workflow - Production Ready
    Handles court export format inconsistencies and integrates assignment data
    """
    
    # File paths
    court_file = r"C:\Users\carucci_r\OneDrive - City of Hackensack\_MONTHLY_DATA\_EXPORTS\Summons\Court\2025_04_ATS_Report.xlsx"
    assignment_file = r"C:\Users\carucci_r\OneDrive - City of Hackensack\_ASSIGNMENTS\Assignment_Master.xlsx"
    output_dir = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\_MONTHLY_DATA\_PROCESSED\Summons")
    
    print("🚀 SummonsMaster ETL Workflow Starting...")
    print("=" * 60)
    
    # Step 1: Load court export (handle messy headers)
    print("📁 Loading court export data...")
    try:
        # Read from row 5 (skip the broken headers in rows 0-4)
        court_df = pd.read_excel(court_file, skiprows=4, header=None)
        
        # Assign clean column names manually (court export headers are broken)
        expected_columns = [
            'BADGE_NUMBER',      # Col 0: 4-digit padded badge (0083, 0135)
            'OFFICER_NAME',      # Col 1: "P.O. A MATTALIAN" format
            'ORI',              # Col 2: "0223" (agency code)
            'TICKET_NUMBER',    # Col 3: "E25010257"
            'ISSUE_DATE',       # Col 4: "04/07/2025"
            'VIOLATION_NUMBER', # Col 5: "170-7", "39:4-56"
            'TYPE',             # Col 6: "P" (parking) or "M" (moving)
            'STATUS',           # Col 7: "ACTI" or "DISP"
            'DISPOSITION_DATE', # Col 8: Disposition date
            'FIND_CD',          # Col 9: Finding code
            'PAYMENT_DATE',     # Col 10: Payment date
            'ASSESSED_AMOUNT',  # Col 11: Assessed amount
            'FINE_AMOUNT',      # Col 12: Fine amount
            'COST_AMOUNT',      # Col 13: Court costs
            'MISC_AMOUNT',      # Col 14: Miscellaneous fees
            'TOTAL_PAID',       # Col 15: Total amount paid
            'CITY_COST'         # Col 16: City portion
        ]
        
        # Assign column names (handle cases where file has fewer columns)
        court_df.columns = expected_columns[:len(court_df.columns)]
        
        # Clean and standardize badge numbers
        court_df['BADGE_NUMBER'] = (court_df['BADGE_NUMBER']
                                   .astype(str)
                                   .str.strip()
                                   .str.zfill(4))  # Ensure 4-digit padding
        
        # Clean officer names (remove rank prefixes)
        court_df['OFFICER_NAME_CLEAN'] = (court_df['OFFICER_NAME']
                                         .astype(str)
                                         .str.replace(r'^(P\.O\.|SPO|SGT|LT|CAPT)\s+', '', regex=True)
                                         .str.strip()
                                         .str.title())
        
        # Remove any completely empty rows
        court_df = court_df.dropna(subset=['BADGE_NUMBER', 'OFFICER_NAME'])
        
        print(f"✅ Court data loaded successfully: {len(court_df):,} enforcement records")
        
    except Exception as e:
        print(f"❌ Error loading court data: {e}")
        return None
    
    # Step 2: Load assignment master
    print("📁 Loading assignment master...")
    try:
        assign_df = pd.read_excel(assignment_file, sheet_name='Assignment_Master')
        
        # Clean and pad badge numbers for consistent joining
        assign_df['BADGE_NUMBER_PADDED'] = (assign_df['BADGE_NUMBER']
                                           .astype(str)
                                           .str.strip()
                                           .str.zfill(4))
        
        print(f"✅ Assignment data loaded: {len(assign_df)} officer records")
        
    except Exception as e:
        print(f"❌ Error loading assignment data: {e}")
        return None
    
    # Step 3: Join datasets on badge numbers
    print("🔗 Joining enforcement and assignment data...")
    
    # Merge on padded badge numbers
    merged_df = court_df.merge(
        assign_df[['BADGE_NUMBER_PADDED', 'FIRST_NAME', 'LAST_NAME', 'TITLE',
                  'TEAM', 'WG1', 'WG2', 'WG3']],
        left_on='BADGE_NUMBER',
        right_on='BADGE_NUMBER_PADDED',
        how='left'
    )
    
    # Calculate match rate
    match_rate = (merged_df['TEAM'].notna().sum() / len(merged_df)) * 100
    print(f"🎯 Assignment match rate: {match_rate:.1f}%")
    
    # Step 4: Data transformation and enrichment
    print("📊 Processing enforcement metrics...")
    
    # Date standardization
    merged_df['ISSUE_DATE'] = pd.to_datetime(merged_df['ISSUE_DATE'], errors='coerce')
    merged_df['MONTH_YEAR'] = merged_df['ISSUE_DATE'].dt.strftime('%Y-%m')
    
    # Revenue calculation
    merged_df['REVENUE_AMOUNT'] = pd.to_numeric(merged_df['TOTAL_PAID'], errors='coerce').fillna(0)
    
    # Violation type mapping
    merged_df['VIOLATION_TYPE'] = merged_df['TYPE'].map({
        'P': 'Parking',
        'M': 'Moving'
    }).fillna('Unknown')
    
    # Assignment hierarchy
    merged_df['DIVISION'] = merged_df['WG1'].fillna('Unknown Division')
    merged_df['BUREAU'] = merged_df['WG2'].fillna('Unknown Bureau')
    merged_df['UNIT'] = merged_df['WG3'].fillna('Unknown Unit')
    merged_df['ASSIGNMENT_FOUND'] = merged_df['TEAM'].notna()
    
    # Officer full name construction
    merged_df['OFFICER_FULL_NAME'] = (merged_df['FIRST_NAME'].fillna('') + ' ' + 
                                     merged_df['LAST_NAME'].fillna('')).str.strip()
    merged_df['OFFICER_DISPLAY'] = merged_df['OFFICER_FULL_NAME'].where(
        merged_df['OFFICER_FULL_NAME'] != '', 
        merged_df['OFFICER_NAME_CLEAN']
    )
    
    # Step 5: Generate summary statistics
    print("📈 Generating officer performance summary...")
    
    officer_summary = merged_df.groupby([
        'BADGE_NUMBER', 'OFFICER_DISPLAY', 'TITLE', 
        'DIVISION', 'BUREAU', 'UNIT'
    ]).agg({
        'TICKET_NUMBER': 'count',
        'REVENUE_AMOUNT': 'sum',
        'VIOLATION_TYPE': lambda x: (x == 'Moving').sum(),
        'TYPE': lambda x: (x == 'P').sum()  # Parking count
    }).round(2)
    
    officer_summary.columns = ['TOTAL_TICKETS', 'TOTAL_REVENUE', 'MOVING_TICKETS', 'PARKING_TICKETS']
    officer_summary = officer_summary.reset_index()
    
    # Add performance metrics
    officer_summary['REVENUE_PER_TICKET'] = (officer_summary['TOTAL_REVENUE'] / 
                                             officer_summary['TOTAL_TICKETS']).round(2)
    officer_summary['MOVING_PERCENTAGE'] = (officer_summary['MOVING_TICKETS'] / 
                                           officer_summary['TOTAL_TICKETS'] * 100).round(1)
    
    # Step 6: Prepare Power BI dataset
    print("🎨 Preparing Power BI dataset...")
    
    powerbi_df = merged_df[[
        'BADGE_NUMBER',
        'OFFICER_DISPLAY', 
        'TITLE',
        'DIVISION',
        'BUREAU', 
        'UNIT',
        'ISSUE_DATE',
        'MONTH_YEAR',
        'VIOLATION_NUMBER',
        'VIOLATION_TYPE',
        'REVENUE_AMOUNT',
        'TICKET_NUMBER',
        'ASSIGNMENT_FOUND'
    ]].copy()
    
    # Remove any rows with invalid dates or missing critical data
    powerbi_df = powerbi_df.dropna(subset=['ISSUE_DATE', 'BADGE_NUMBER'])
    
    # Step 7: Export processed data
    print("💾 Exporting processed datasets...")
    
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        # Export main datasets
        powerbi_df.to_excel(output_dir / 'SummonsMaster_PowerBI_Ready.xlsx', index=False)
        officer_summary.to_excel(output_dir / 'Officer_Performance_Summary.xlsx', index=False)
        merged_df.to_excel(output_dir / 'Complete_Enforcement_Dataset.xlsx', index=False)
        
        # Export data quality report
        quality_report = generate_quality_report(merged_df, match_rate)
        with open(output_dir / 'Data_Quality_Report.txt', 'w') as f:
            f.write(quality_report)
        
        print("✅ All files exported successfully!")
        
    except Exception as e:
        print(f"❌ Error exporting files: {e}")
        return None
    
    # Step 8: Final summary
    total_revenue = merged_df['REVENUE_AMOUNT'].sum()
    date_range = f"{merged_df['ISSUE_DATE'].min().strftime('%Y-%m-%d')} to {merged_df['ISSUE_DATE'].max().strftime('%Y-%m-%d')}"
    
    print("\n" + "=" * 60)
    print("🎉 SUMMONS MASTER ETL COMPLETE!")
    print("=" * 60)
    print(f"📊 Total Enforcement Records: {len(merged_df):,}")
    print(f"🎯 Assignment Match Rate: {match_rate:.1f}%")
    print(f"💰 Total Revenue Generated: ${total_revenue:,.2f}")
    print(f"📅 Date Range: {date_range}")
    print(f"👮 Officers with Activity: {officer_summary['BADGE_NUMBER'].nunique()}")
    print(f"🚗 Moving Violations: {(merged_df['VIOLATION_TYPE'] == 'Moving').sum():,}")
    print(f"🅿️  Parking Violations: {(merged_df['VIOLATION_TYPE'] == 'Parking').sum():,}")
    print("\n📁 Output Files Created:")
    print(f"  • SummonsMaster_PowerBI_Ready.xlsx ({len(powerbi_df):,} records)")
    print(f"  • Officer_Performance_Summary.xlsx ({len(officer_summary)} officers)")
    print(f"  • Complete_Enforcement_Dataset.xlsx ({len(merged_df):,} records)")
    print(f"  • Data_Quality_Report.txt")
    print("=" * 60)
    
    return powerbi_df, officer_summary, merged_df

def generate_quality_report(df, match_rate):
    """Generate comprehensive data quality report"""
    
    report = f"""
SummonsMaster Data Quality Report
Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}
=====================================

SUMMARY STATISTICS:
- Total Records: {len(df):,}
- Assignment Match Rate: {match_rate:.1f}%
- Date Range: {df['ISSUE_DATE'].min().strftime('%Y-%m-%d')} to {df['ISSUE_DATE'].max().strftime('%Y-%m-%d')}
- Total Revenue: ${df['REVENUE_AMOUNT'].sum():,.2f}

DATA QUALITY CHECKS:
✓ Badge Number Format: {('✅ PASS' if (df['BADGE_NUMBER'].str.len() == 4).all() else '❌ FAIL')}
✓ Assignment Matches: {('✅ PASS' if match_rate > 85 else '❌ FAIL')} ({match_rate:.1f}% > 85%)
✓ Revenue Values: {('✅ PASS' if (df['REVENUE_AMOUNT'] >= 0).all() else '❌ FAIL')}
✓ Valid Dates: {('✅ PASS' if df['ISSUE_DATE'].notna().all() else '❌ FAIL')}
✓ No Duplicate Tickets: {('✅ PASS' if not df['TICKET_NUMBER'].duplicated().any() else '❌ FAIL')}

OFFICER ACTIVITY BREAKDOWN:
- Officers with Enforcement: {df['BADGE_NUMBER'].nunique()}
- Most Active Officer: {df['BADGE_NUMBER'].value_counts().index[0]} ({df['BADGE_NUMBER'].value_counts().iloc[0]} tickets)
- Average Tickets per Officer: {df.groupby('BADGE_NUMBER').size().mean():.1f}

VIOLATION TYPE DISTRIBUTION:
- Moving Violations: {(df['VIOLATION_TYPE'] == 'Moving').sum():,} ({(df['VIOLATION_TYPE'] == 'Moving').sum()/len(df)*100:.1f}%)
- Parking Violations: {(df['VIOLATION_TYPE'] == 'Parking').sum():,} ({(df['VIOLATION_TYPE'] == 'Parking').sum()/len(df)*100:.1f}%)

REVENUE ANALYSIS:
- Average Revenue per Ticket: ${df['REVENUE_AMOUNT'].mean():.2f}
- Highest Revenue Ticket: ${df['REVENUE_AMOUNT'].max():.2f}
- Total Outstanding (Status=ACTI): ${df[df['STATUS'] == 'ACTI']['REVENUE_AMOUNT'].sum():.2f}

MISSING DATA:
- Records without Assignments: {(~df['ASSIGNMENT_FOUND']).sum():,}
- Records with Missing Revenue: {df['REVENUE_AMOUNT'].isna().sum():,}
- Records with Invalid Dates: {df['ISSUE_DATE'].isna().sum():,}
"""
    
    return report

# Main execution
if __name__ == "__main__":
    powerbi_data, officer_summary, full_dataset = process_summons_master_complete()