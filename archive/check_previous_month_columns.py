import pandas as pd
from pathlib import Path

def check_previous_month_columns():
    """Check what columns are available in the Previous_Month_ATS_Court_Data_DYNAMIC table"""
    
    # Check the latest summons file
    latest_file = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons\summons_powerbi_latest.xlsx")
    
    if not latest_file.exists():
        print("❌ summons_powerbi_latest.xlsx not found!")
        return
    
    try:
        # Read the ATS_Court_Data sheet
        df = pd.read_excel(latest_file, sheet_name="ATS_Court_Data")
        
        print("📊 Columns in ATS_Court_Data sheet:")
        print("=" * 50)
        for i, col in enumerate(df.columns, 1):
            print(f"{i:2d}. {col}")
        
        print(f"\n📈 Total columns: {len(df.columns)}")
        
        # Check if Month_Year column exists
        if 'Month_Year' in df.columns:
            print("\n✅ Month_Year column found!")
            print("Sample values:")
            print(df['Month_Year'].value_counts().head(10))
        else:
            print("\n❌ Month_Year column NOT found!")
            
            # Check for date-related columns
            date_columns = [col for col in df.columns if 'date' in col.lower() or 'month' in col.lower() or 'year' in col.lower()]
            if date_columns:
                print("📅 Date-related columns found:")
                for col in date_columns:
                    print(f"   - {col}")
        
        # Check ISSUE_DATE column
        if 'ISSUE_DATE' in df.columns:
            print(f"\n📅 ISSUE_DATE column found!")
            print(f"Date range: {df['ISSUE_DATE'].min()} to {df['ISSUE_DATE'].max()}")
            print(f"Sample dates: {df['ISSUE_DATE'].dropna().head(5).tolist()}")
        
    except Exception as e:
        print(f"❌ Error reading file: {e}")

if __name__ == "__main__":
    check_previous_month_columns()
