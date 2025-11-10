# 🕒 2025-07-10-17-00-00
# Project: SummonsMaster/Rolling_12_Month_Previous_Month_Fix
# Author: R. A. Carucci
# Purpose: Fix date range to show Jun 2024 - Jun 2025 (rolling 12 months ending previous month)

import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

def fix_rolling_12_month_dates():
    """Fix the existing file to show proper rolling 12 months"""
    
    try:
        # Load the existing fixed file
        input_file = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons\summons_powerbi_12_month_FIXED.xlsx")
        output_file = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons\summons_powerbi_rolling_12_month.xlsx")
        
        print("🔄 FIXING ROLLING 12-MONTH DATE RANGE")
        print("=" * 50)
        print(f"📋 Loading existing data from: {input_file}")
        
        # Read the existing processed data
        df = pd.read_excel(input_file, sheet_name='ATS_Court_Data')
        
        print(f"✅ Loaded {len(df):,} records")
        
        # Convert ISSUE_DATE to datetime if not already
        df['ISSUE_DATE'] = pd.to_datetime(df['ISSUE_DATE'], errors='coerce')
        
        # Show current date range
        current_min = df['ISSUE_DATE'].min()
        current_max = df['ISSUE_DATE'].max()
        print(f"📅 Current range: {current_min.strftime('%b %Y')} - {current_max.strftime('%b %Y')}")
        
        # Calculate rolling 12 months ending with previous month
        today = datetime.now()
        
        # Previous month end date
        if today.month == 1:
            prev_month_year = today.year - 1
            prev_month = 12
        else:
            prev_month_year = today.year
            prev_month = today.month - 1
        
        # End date: Last day of previous month
        end_date = datetime(prev_month_year, prev_month, 1)
        # Get last day of the month
        if prev_month == 12:
            next_month = datetime(prev_month_year + 1, 1, 1)
        else:
            next_month = datetime(prev_month_year, prev_month + 1, 1)
        end_date = next_month - timedelta(days=1)
        end_date = end_date.replace(hour=23, minute=59, second=59)
        
        # Start date: First day of month 12 months before end date
        start_year = prev_month_year - 1 if prev_month == 12 else prev_month_year - (1 if prev_month <= 6 else 0)
        start_month = prev_month if prev_month > 6 else prev_month + 6 if prev_month <= 6 else prev_month - 6
        
        # Simpler calculation: exactly 12 months back
        start_date = datetime(end_date.year - 1, end_date.month, 1)
        
        print(f"🎯 Target rolling 12 months:")
        print(f"   Start: {start_date.strftime('%B %d, %Y')} (Jun 1, 2024)")
        print(f"   End:   {end_date.strftime('%B %d, %Y')} (Jun 30, 2025)")
        
        # Filter data for rolling 12 months
        filtered_df = df[
            (df['ISSUE_DATE'] >= start_date) & 
            (df['ISSUE_DATE'] <= end_date) &
            (df['ISSUE_DATE'].notna())
        ].copy()
        
        print(f"📊 Filtered to {len(filtered_df):,} records in rolling 12-month period")
        
        # Recalculate Month_Year to ensure proper formatting
        filtered_df['Month_Year'] = filtered_df['ISSUE_DATE'].dt.strftime('%b-%y')
        filtered_df['Month'] = filtered_df['ISSUE_DATE'].dt.month
        filtered_df['Year'] = filtered_df['ISSUE_DATE'].dt.year
        
        # Show month breakdown
        month_summary = filtered_df.groupby('Month_Year').size().reset_index(name='Count')
        print(f"\n📈 Month-by-month breakdown:")
        for _, row in month_summary.iterrows():
            print(f"   {row['Month_Year']}: {row['Count']:,} records")
        
        # Export corrected file
        print(f"\n💾 Exporting corrected rolling 12-month data...")
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            filtered_df.to_excel(writer, sheet_name='ATS_Court_Data', index=False)
        
        # Generate summary statistics
        match_rate = (filtered_df['ASSIGNMENT_FOUND'].sum() / len(filtered_df)) * 100
        unique_officers = filtered_df['PADDED_BADGE_NUMBER'].nunique()
        total_revenue = filtered_df['TOTAL_PAID_AMOUNT'].sum()
        moving_count = len(filtered_df[filtered_df['TYPE'] == 'M'])
        parking_count = len(filtered_df[filtered_df['TYPE'] == 'P'])
        
        print("\n" + "=" * 60)
        print("🎯 ROLLING 12-MONTH RESULTS:")
        print("=" * 60)
        print(f"📅 Period: Jun 2024 - Jun 2025 (12 months)")
        print(f"📊 Total Records: {len(filtered_df):,}")
        print(f"🎯 Assignment Match Rate: {match_rate:.1f}%")
        print(f"👮 Unique Officers: {unique_officers}")
        print(f"💰 Total Revenue: ${total_revenue:,.2f}")
        print(f"🚗 Moving Violations: {moving_count:,}")
        print(f"🅿️ Parking Violations: {parking_count:,}")
        print(f"📁 Output File: {output_file}")
        print("=" * 60)
        print("🚀 READY FOR POWER BI!")
        print("✅ Connect to the rolling 12-month file")
        print("✅ Matrix should show Jun-24 through Jun-25")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    fix_rolling_12_month_dates()