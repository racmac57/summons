#!/usr/bin/env python3
"""
Analyze the record count discrepancy: Current 42,019 vs Expected 35,166 (diff: 6,853)
"""
import pandas as pd
import sys
from datetime import date

def main():
    file_path = r"C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons\summons_powerbi_latest.xlsx"
    
    try:
        print("=== RECORD COUNT DISCREPANCY ANALYSIS ===")
        print(f"Current records: 42,019")
        print(f"Expected (July baseline): 35,166") 
        print(f"Difference: +6,853 records")
        print("")
        
        # Load the data
        df = pd.read_excel(file_path, sheet_name='Summons_Data')
        print(f"Loaded {len(df):,} records for analysis")
        
        # Fix column names for analysis
        ticket_col = 'TICKET\nNUMBER'
        date_col = 'ISSUE\nDATE'
        source_col = 'SOURCE_FILE'
        
        print(f"\n=== 1. DUPLICATE TICKET ANALYSIS ===")
        
        # Check for duplicates
        total_tickets = len(df)
        unique_tickets = df[ticket_col].nunique()
        duplicates = total_tickets - unique_tickets
        
        print(f"Total records: {total_tickets:,}")
        print(f"Unique ticket numbers: {unique_tickets:,}")
        print(f"Duplicate records: {duplicates:,}")
        
        if duplicates > 0:
            # Find duplicate tickets
            duplicate_tickets = df[df[ticket_col].duplicated(keep=False)]
            print(f"\nDuplicate ticket analysis:")
            dup_summary = duplicate_tickets.groupby(ticket_col).agg({
                source_col: lambda x: list(x.unique()),
                date_col: 'nunique'
            }).reset_index()
            
            print(f"Number of ticket numbers with duplicates: {len(dup_summary)}")
            print("Sample duplicate tickets:")
            for idx, row in dup_summary.head(10).iterrows():
                print(f"  {row[ticket_col]}: appears in {row[source_col]}")
        
        print(f"\n=== 2. SOURCE FILE ANALYSIS ===")
        
        # Analyze by source file
        source_analysis = df.groupby(source_col).agg({
            ticket_col: ['count', 'nunique'],
            date_col: ['min', 'max']
        }).round(2)
        
        source_analysis.columns = ['Total_Records', 'Unique_Tickets', 'Min_Date', 'Max_Date']
        source_analysis = source_analysis.reset_index()
        
        print("Records by source file:")
        for idx, row in source_analysis.iterrows():
            duplicates_in_file = row['Total_Records'] - row['Unique_Tickets']
            print(f"  {row[source_col]:<20} {row['Total_Records']:>6,} records, {row['Unique_Tickets']:>6,} unique, {duplicates_in_file:>4} dups")
        
        print(f"\n=== 3. DOUBLE-COUNTING ANALYSIS ===")
        
        # Check overlap between 24_ALL_SUMMONS.xlsx and monthly files
        all_summons_data = df[df[source_col] == '24_ALL_SUMMONS.xlsx']
        monthly_data = df[df[source_col] != '24_ALL_SUMMONS.xlsx']
        
        print(f"24_ALL_SUMMONS.xlsx: {len(all_summons_data):,} records")
        print(f"Monthly ATS files: {len(monthly_data):,} records")
        
        # Find overlapping tickets
        all_summons_tickets = set(all_summons_data[ticket_col])
        monthly_tickets = set(monthly_data[ticket_col])
        overlapping_tickets = all_summons_tickets.intersection(monthly_tickets)
        
        print(f"Unique tickets in 24_ALL_SUMMONS.xlsx: {len(all_summons_tickets):,}")
        print(f"Unique tickets in monthly files: {len(monthly_tickets):,}")
        print(f"OVERLAPPING tickets (double-counted): {len(overlapping_tickets):,}")
        
        if len(overlapping_tickets) > 0:
            # Analyze overlapping tickets by month
            overlap_df = df[df[ticket_col].isin(overlapping_tickets)]
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            overlap_df = overlap_df.copy()
            overlap_df[date_col] = pd.to_datetime(overlap_df[date_col], errors='coerce')
            
            overlap_analysis = overlap_df.groupby([overlap_df[date_col].dt.to_period('M'), source_col]).size().unstack(fill_value=0)
            print(f"\nOverlapping records by month and source:")
            print(overlap_analysis)
        
        print(f"\n=== 4. DATE FILTERING VERIFICATION ===")
        
        # Check date range compliance
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        min_date = df[date_col].min()
        max_date = df[date_col].max()
        
        # Expected range: Aug 2024 - Aug 2025
        expected_start = pd.Timestamp('2024-08-01')
        expected_end = pd.Timestamp('2025-08-31')
        
        print(f"Actual date range: {min_date.date()} to {max_date.date()}")
        print(f"Expected range: {expected_start.date()} to {expected_end.date()}")
        
        # Check for records outside expected range
        outside_range = df[(df[date_col] < expected_start) | (df[date_col] > expected_end)]
        print(f"Records outside expected range: {len(outside_range):,}")
        
        if len(outside_range) > 0:
            outside_analysis = outside_range.groupby(source_col)[ticket_col].count()
            print("Records outside range by source:")
            for source, count in outside_analysis.items():
                print(f"  {source}: {count:,} records")
        
        print(f"\n=== 5. MONTHLY BREAKDOWN ===")
        
        # Monthly distribution
        df['Month_Year'] = df[date_col].dt.strftime('%m-%y')
        monthly_breakdown = df.groupby('Month_Year').agg({
            ticket_col: ['count', 'nunique'],
            source_col: lambda x: list(x.unique())
        })
        
        monthly_breakdown.columns = ['Total_Records', 'Unique_Tickets', 'Source_Files']
        monthly_breakdown = monthly_breakdown.reset_index()
        
        print("Monthly breakdown with source file info:")
        total_records = 0
        total_unique = 0
        for idx, row in monthly_breakdown.iterrows():
            duplicates_in_month = row['Total_Records'] - row['Unique_Tickets']
            total_records += row['Total_Records']
            total_unique += row['Unique_Tickets']
            sources = ', '.join([s.replace('.xlsx', '') for s in row['Source_Files']])
            print(f"  {row['Month_Year']}: {row['Total_Records']:>5,} records, {row['Unique_Tickets']:>5,} unique, {duplicates_in_month:>4} dups | Sources: {sources}")
        
        print(f"\nTOTALS: {total_records:,} records, {total_unique:,} unique")
        
        print(f"\n=== 6. ROOT CAUSE ANALYSIS ===")
        
        # Calculate potential causes
        print("Breakdown of the +6,853 record difference:")
        
        if len(overlapping_tickets) > 0:
            # Double-counting from overlapping tickets
            overlap_records = len(df[df[ticket_col].isin(overlapping_tickets)])
            # Each overlapping ticket appears twice, so extra records = overlap_records / 2
            double_count_extra = overlap_records // 2
            print(f"  1. Double-counting (overlap): +{double_count_extra:,} records")
        else:
            double_count_extra = 0
            print(f"  1. Double-counting: No overlap found")
        
        if len(outside_range) > 0:
            print(f"  2. Records outside date range: +{len(outside_range):,} records")
        else:
            print(f"  2. Records outside date range: 0 records")
        
        remaining_diff = 6853 - double_count_extra - len(outside_range)
        print(f"  3. Other factors: +{remaining_diff:,} records")
        
        if duplicates > 0:
            print(f"  4. Note: Total duplicates in dataset: {duplicates:,}")
        
        print(f"\n=== 7. RECOMMENDATIONS ===")
        
        if len(overlapping_tickets) > 0:
            print(f"1. CRITICAL: Remove double-counted records from overlapping tickets")
            print(f"   - Deduplication needed: {len(overlapping_tickets):,} ticket numbers")
            print(f"   - Strategy: Keep 24_ALL_SUMMONS.xlsx records, remove from monthly files")
        
        if len(outside_range) > 0:
            print(f"2. Remove {len(outside_range):,} records outside the Aug 2024-Aug 2025 range")
        
        if duplicates > 0:
            print(f"3. Review {duplicates:,} duplicate records within the dataset")
        
        print(f"4. Expected result after deduplication: ~35,166 records (July baseline)")
        
    except Exception as e:
        print(f"Error in analysis: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()