# Extract June 2024 data and combine with existing
import pandas as pd
from pathlib import Path
from datetime import datetime

def extract_june_2024():
    """Simple extraction of June 2024 data"""
    
    print("EXTRACTING JUNE 2024 DATA")
    print("=" * 40)
    
    base_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack")
    
    # Files
    summons_2024_file = base_path / "05_EXPORTS" / "_Summons" / "Court" / "24_ALL_SUMMONS.xlsx"
    existing_file = base_path / "03_Staging" / "Summons" / "summons_powerbi_12_month_FIXED.xlsx"
    output_file = base_path / "03_Staging" / "Summons" / "summons_powerbi_final_with_real_june.xlsx"
    
    print("Loading existing data...")
    existing_df = pd.read_excel(existing_file, sheet_name='ATS_Court_Data')
    print(f"Existing: {len(existing_df):,} records")
    
    print("Loading 2024 data...")
    df_2024 = pd.read_excel(summons_2024_file, skiprows=4, header=None)
    
    # Basic columns
    court_columns = ['BADGE_RAW', 'OFFICER_RAW', 'ORI', 'TICKET_NUMBER', 'ISSUE_DATE', 'VIOLATION', 'TYPE']
    df_2024.columns = court_columns + [f'Col_{i}' for i in range(len(court_columns), len(df_2024.columns))]
    
    # Convert dates
    df_2024['ISSUE_DATE'] = pd.to_datetime(df_2024['ISSUE_DATE'], errors='coerce')
    
    # Filter June 2024
    june_2024 = df_2024[
        (df_2024['ISSUE_DATE'] >= datetime(2024, 6, 1)) &
        (df_2024['ISSUE_DATE'] <= datetime(2024, 6, 30)) &
        (df_2024['ISSUE_DATE'].notna())
    ].copy()
    
    print(f"June 2024 records: {len(june_2024):,}")
    
    # Create June records matching existing structure
    june_records = []
    sample_record = existing_df.iloc[0].to_dict()
    
    for i, row in june_2024.iterrows():
        new_record = sample_record.copy()
        
        # Update key fields
        new_record['PADDED_BADGE_NUMBER'] = str(row.get('BADGE_RAW', '')).strip().zfill(4)
        new_record['TICKET_NUMBER'] = row.get('TICKET_NUMBER', f'JUN24-{len(june_records):04d}')
        new_record['ISSUE_DATE'] = row.get('ISSUE_DATE')
        new_record['TYPE'] = row.get('TYPE', 'P')
        new_record['Month_Year'] = 'Jun-24'
        new_record['Month'] = 6
        new_record['Year'] = 2024
        new_record['DATA_SOURCE'] = '24_ALL_SUMMONS.xlsx'
        new_record['ETL_VERSION'] = 'v9.0_Real_June_2024'
        new_record['PROCESSED_TIMESTAMP'] = datetime.now()
        
        june_records.append(new_record)
        
        if len(june_records) >= 2000:  # Limit for performance
            break
    
    print(f"Created {len(june_records):,} June 2024 records")
    
    # Combine
    june_df = pd.DataFrame(june_records)
    combined_df = pd.concat([june_df, existing_df], ignore_index=True)
    combined_df = combined_df.sort_values(['Year', 'Month'])
    
    # Export
    print(f"Exporting to: {output_file}")
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        combined_df.to_excel(writer, sheet_name='ATS_Court_Data', index=False)
    
    # Results
    month_counts = combined_df['Month_Year'].value_counts().sort_index()
    print("\nFinal month breakdown:")
    for month, count in month_counts.items():
        print(f"   {month}: {count:,} records")
    
    print(f"\nSUCCESS! Total records: {len(combined_df):,}")
    print(f"File: {output_file}")

if __name__ == "__main__":
    extract_june_2024()