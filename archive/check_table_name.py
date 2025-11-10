import pandas as pd
from pathlib import Path

def check_table_name():
    """Check what the actual table name should be in Power BI"""
    
    # Check the latest summons file
    latest_file = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons\summons_powerbi_latest.xlsx")
    
    if not latest_file.exists():
        print("❌ summons_powerbi_latest.xlsx not found!")
        return
    
    try:
        # Get all sheet names
        excel_file = pd.ExcelFile(latest_file)
        print("📊 Available sheets in summons_powerbi_latest.xlsx:")
        print("=" * 50)
        for i, sheet_name in enumerate(excel_file.sheet_names, 1):
            print(f"{i}. {sheet_name}")
        
        print(f"\n📈 Total sheets: {len(excel_file.sheet_names)}")
        
        # Check if there's a sheet that might be the "Previous_Month" table
        for sheet_name in excel_file.sheet_names:
            if 'previous' in sheet_name.lower() or 'month' in sheet_name.lower():
                print(f"\n🎯 Potential Previous Month table: {sheet_name}")
                
                # Read a few rows to see the structure
                df = pd.read_excel(latest_file, sheet_name=sheet_name, nrows=5)
                print(f"   Columns: {list(df.columns)}")
                print(f"   Sample data:")
                print(df.head(2))
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_table_name()
