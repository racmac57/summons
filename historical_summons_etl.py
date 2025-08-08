# 🕒 2025-07-10-16-00-00
# Project: SummonsMaster/Historical_12_Month_Complete_ETL
# Author: R. A. Carucci
# Purpose: Process ALL historical exports for complete 12-month Power BI dashboard

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import logging
import glob

def setup_logging():
    """Setup logging for ETL process"""
    log_file = f"historical_summons_etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def load_assignment_master():
    """Load and clean assignment master data"""
    logger = logging.getLogger(__name__)
    
    try:
        # Try multiple possible locations
        possible_paths = [
            r"C:\Users\carucci_r\OneDrive - City of Hackensack\_Hackensack_Data_Repository\ASSIGNED_SHIFT\Assignment_Master_V2.xlsx",
            r"C:\Users\carucci_r\OneDrive - City of Hackensack\_ASSIGNMENTS\Assignment_Master.xlsx",
            r"C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Assignment_Master.xlsx"
        ]
        
        assignment_df = None
        for path in possible_paths:
            if Path(path).exists():
                logger.info(f"Loading Assignment Master from: {path}")
                assignment_df = pd.read_excel(path, sheet_name=0)  # First sheet
                break
        
        if assignment_df is None:
            logger.error("❌ Assignment Master not found in any expected location")
            return None
        
        # Create comprehensive badge lookup with variations
        badge_lookup = {}
        
        for _, row in assignment_df.iterrows():
            # Get badge variations
            raw_badge = str(row.get('BADGE_NUMBER', '')).strip()
            padded_badge = str(row.get('PADDED_BADGE_NUMBER', '')).strip()
            
            # Officer info
            officer_name = str(row.get('FULL_NAME', row.get('Proposed 4-Digit Format', ''))).strip()
            
            # Assignment info
            assignment_info = {
                'OFFICER_DISPLAY_NAME': officer_name,
                'WG1': str(row.get('WG1', '')).strip(),
                'WG2': str(row.get('WG2', '')).strip(),
                'WG3': str(row.get('WG3', '')).strip(),
                'WG4': str(row.get('WG4', '')).strip(),
                'WG5': str(row.get('WG5', '')).strip()
            }
            
            # Add all badge variations to lookup
            if raw_badge and raw_badge != 'nan':
                badge_lookup[raw_badge] = assignment_info
                badge_lookup[raw_badge.zfill(3)] = assignment_info
                badge_lookup[raw_badge.zfill(4)] = assignment_info
            
            if padded_badge and padded_badge != 'nan':
                badge_lookup[padded_badge] = assignment_info
        
        logger.info(f"✅ Assignment lookup created: {len(badge_lookup)} badge variations")
        return badge_lookup
        
    except Exception as e:
        logger.error(f"❌ Error loading Assignment Master: {str(e)}")
        return None

def process_single_court_file(file_path, assignment_lookup):
    """Process a single court export file"""
    logger = logging.getLogger(__name__)
    
    try:
        logger.info(f"📋 Processing: {file_path.name}")
        
        # Read court export - court files typically start data at row 5
        court_df = pd.read_excel(file_path, skiprows=4, header=None)
        
        # Standard court export column structure
        court_columns = [
            'BADGE_NUMBER_RAW', 'OFFICER_NAME_RAW', 'ORI', 'TICKET_NUMBER', 'ISSUE_DATE',
            'VIOLATION_NUMBER', 'TYPE', 'STATUS', 'DISPOSITION_DATE', 'FIND_CD', 'PAYMENT_DATE',
            'ASSESSED_AMOUNT', 'FINE_AMOUNT', 'COST_AMOUNT', 'MISC_AMOUNT', 'TOTAL_PAID_AMOUNT', 'CITY_COST_AMOUNT'
        ]
        
        # Handle variable column counts
        num_cols = min(len(court_columns), len(court_df.columns))
        court_df.columns = court_columns[:num_cols] + [f'Extra_Col_{i}' for i in range(num_cols, len(court_df.columns))]
        
        # Clean badge numbers
        court_df['BADGE_CLEAN'] = court_df['BADGE_NUMBER_RAW'].astype(str).str.strip()
        
        # Remove invalid/footer rows
        invalid_badges = ['0', '0000', 'nan', 'TOTAL', 'Total', 'SUMMARY', 'Summary']
        court_df = court_df[~court_df['BADGE_CLEAN'].isin(invalid_badges)]
        court_df = court_df[court_df['BADGE_CLEAN'].notna()]
        
        # Pad badge numbers
        court_df['PADDED_BADGE_NUMBER'] = court_df['BADGE_CLEAN'].str.zfill(4)
        
        # Convert dates with error handling
        court_df['ISSUE_DATE'] = pd.to_datetime(court_df['ISSUE_DATE'], errors='coerce')
        
        # Apply assignment lookup
        court_df['ASSIGNMENT_FOUND'] = False
        court_df['OFFICER_DISPLAY_NAME'] = None
        court_df['WG1'] = None
        court_df['WG2'] = None
        court_df['WG3'] = None
        court_df['WG4'] = None
        court_df['WG5'] = None
        
        matches = 0
        for idx, row in court_df.iterrows():
            badge = row['PADDED_BADGE_NUMBER']
            if badge in assignment_lookup:
                assignment = assignment_lookup[badge]
                court_df.at[idx, 'ASSIGNMENT_FOUND'] = True
                court_df.at[idx, 'OFFICER_DISPLAY_NAME'] = assignment['OFFICER_DISPLAY_NAME']
                court_df.at[idx, 'WG1'] = assignment['WG1']
                court_df.at[idx, 'WG2'] = assignment['WG2']
                court_df.at[idx, 'WG3'] = assignment['WG3']
                court_df.at[idx, 'WG4'] = assignment['WG4']
                court_df.at[idx, 'WG5'] = assignment['WG5']
                matches += 1
        
        # Add file metadata
        court_df['PROCESSED_TIMESTAMP'] = datetime.now()
        court_df['DATA_SOURCE'] = file_path.name
        court_df['ETL_VERSION'] = 'v4.0_Historical_Complete'
        
        match_rate = (matches / len(court_df)) * 100 if len(court_df) > 0 else 0
        logger.info(f"✅ Processed {file_path.name}: {len(court_df)} records, {match_rate:.1f}% match rate")
        
        return court_df
        
    except Exception as e:
        logger.error(f"❌ Error processing {file_path.name}: {str(e)}")
        return None

def main():
    """Main historical ETL process"""
    logger = setup_logging()
    logger.info("🚀 Starting COMPLETE Historical Summons ETL Process")
    
    print("🎯 COMPLETE HISTORICAL SUMMONS ETL")
    print("=" * 60)
    
    try:
        # Define paths
        export_folder = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\05_EXPORTS\_Summons\Court")
        output_folder = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons")
        
        # Create output folder
        output_folder.mkdir(parents=True, exist_ok=True)
        
        # Load assignment master
        print("📋 Loading Assignment Master...")
        assignment_lookup = load_assignment_master()
        if assignment_lookup is None:
            print("❌ Cannot proceed without Assignment Master")
            return
        
        # Find all Excel files
        print(f"📁 Scanning export folder: {export_folder}")
        excel_files = list(export_folder.glob("*.xlsx"))
        
        if not excel_files:
            print(f"❌ No Excel files found in {export_folder}")
            print("Please check the folder path and ensure export files exist.")
            return
        
        print(f"✅ Found {len(excel_files)} Excel files to process:")
        for file_path in excel_files:
            print(f"   - {file_path.name}")
        
        # Process all files
        print("\n🔄 Processing all historical files...")
        all_dataframes = []
        total_records = 0
        
        for file_path in excel_files:
            if file_path.name.startswith('~$'):  # Skip temp files
                continue
                
            processed_df = process_single_court_file(file_path, assignment_lookup)
            if processed_df is not None and len(processed_df) > 0:
                all_dataframes.append(processed_df)
                total_records += len(processed_df)
                print(f"   ✅ {file_path.name}: {len(processed_df)} records")
            else:
                print(f"   ⚠️ {file_path.name}: No valid data")
        
        if not all_dataframes:
            print("❌ No valid data processed from any files")
            return
        
        # Combine all data
        print(f"\n🔗 Combining data from {len(all_dataframes)} files...")
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        
        # Filter for rolling 12 months (July 2024 - June 2025)
        print("📅 Filtering for rolling 12-month period...")
        end_date = datetime(2025, 6, 30)
        start_date = datetime(2024, 7, 1)
        
        # Filter by date range
        date_filtered = combined_df[
            (combined_df['ISSUE_DATE'] >= start_date) & 
            (combined_df['ISSUE_DATE'] <= end_date) &
            (combined_df['ISSUE_DATE'].notna())
        ].copy()
        
        # Prepare final dataset
        powerbi_columns = [
            'PADDED_BADGE_NUMBER', 'OFFICER_DISPLAY_NAME', 
            'WG1', 'WG2', 'WG3', 'WG4', 'WG5',
            'TICKET_NUMBER', 'ISSUE_DATE', 'VIOLATION_NUMBER', 'TYPE', 'STATUS',
            'TOTAL_PAID_AMOUNT', 'FINE_AMOUNT', 'COST_AMOUNT', 'MISC_AMOUNT',
            'OFFICER_NAME_RAW', 'ASSIGNMENT_FOUND', 'PROCESSED_TIMESTAMP', 'DATA_SOURCE', 'ETL_VERSION'
        ]
        
        # Ensure all columns exist
        for col in powerbi_columns:
            if col not in date_filtered.columns:
                date_filtered[col] = None
        
        final_df = date_filtered[powerbi_columns].copy()
        
        # Add calculated columns for Power BI
        final_df['Month_Year'] = final_df['ISSUE_DATE'].dt.strftime('%b-%y')
        final_df['Month'] = final_df['ISSUE_DATE'].dt.month
        final_df['Year'] = final_df['ISSUE_DATE'].dt.year
        
        # Export main file
        output_file = output_folder / "summons_powerbi_12_month_historical.xlsx"
        print(f"\n💾 Exporting to: {output_file}")
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            final_df.to_excel(writer, sheet_name='ATS_Court_Data', index=False)
        
        # Create previous month file for KPIs
        previous_month = datetime.now().replace(day=1) - timedelta(days=1)
        prev_month_data = final_df[
            (final_df['ISSUE_DATE'].dt.year == previous_month.year) &
            (final_df['ISSUE_DATE'].dt.month == previous_month.month)
        ].copy()
        
        prev_month_file = output_folder / f"summons_powerbi_prev_month_{previous_month.strftime('%Y_%m')}.xlsx"
        if len(prev_month_data) > 0:
            with pd.ExcelWriter(prev_month_file, engine='openpyxl') as writer:
                prev_month_data.to_excel(writer, sheet_name='ATS_Court_Data', index=False)
            print(f"💾 Previous month KPI file: {prev_month_file}")
        
        # Generate summary statistics
        match_rate = (final_df['ASSIGNMENT_FOUND'].sum() / len(final_df)) * 100
        unique_officers = final_df['PADDED_BADGE_NUMBER'].nunique()
        total_revenue = final_df['TOTAL_PAID_AMOUNT'].sum()
        date_range = f"{final_df['ISSUE_DATE'].min().strftime('%b %Y')} - {final_df['ISSUE_DATE'].max().strftime('%b %Y')}"
        moving_count = len(final_df[final_df['TYPE'] == 'M'])
        parking_count = len(final_df[final_df['TYPE'] == 'P'])
        
        # Print results
        print("\n" + "=" * 60)
        print("🎯 COMPLETE HISTORICAL ETL RESULTS:")
        print("=" * 60)
        print(f"📊 Total Records Processed: {len(final_df):,}")
        print(f"📅 Date Range: {date_range}")
        print(f"🎯 Assignment Match Rate: {match_rate:.1f}%")
        print(f"👮 Unique Officers: {unique_officers}")
        print(f"💰 Total Revenue: ${total_revenue:,.2f}")
        print(f"🚗 Moving Violations: {moving_count:,}")
        print(f"🅿️ Parking Violations: {parking_count:,}")
        print(f"📁 Output Files:")
        print(f"   - Historical (12-month): {output_file}")
        if len(prev_month_data) > 0:
            print(f"   - Previous Month KPIs: {prev_month_file}")
        print("=" * 60)
        print("🚀 READY FOR POWER BI DASHBOARD!")
        print("✅ Connect Power BI to the historical file for 12-month matrix")
        print("✅ Use previous month file for current KPI calculations")
        
    except Exception as e:
        logger.error(f"❌ Fatal error in ETL process: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()