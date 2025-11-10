import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

def debug_previous_month_data():
    """Debug what data is available for previous month filtering"""
    
    # Check the latest summons file
    latest_file = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons\summons_powerbi_latest.xlsx")
    
    if not latest_file.exists():
        print("❌ summons_powerbi_latest.xlsx not found!")
        return
    
    try:
        # Read the ATS_Court_Data sheet
        df = pd.read_excel(latest_file, sheet_name="ATS_Court_Data")
        
        print("🔍 DEBUGGING PREVIOUS MONTH DATA")
        print("=" * 60)
        
        # Calculate previous month dates
        today = datetime.now()
        previous_month_start = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
        previous_month_end = today.replace(day=1) - timedelta(days=1)
        
        print(f"📅 Today: {today.strftime('%Y-%m-%d')}")
        print(f"📅 Previous Month Start: {previous_month_start.strftime('%Y-%m-%d')}")
        print(f"📅 Previous Month End: {previous_month_end.strftime('%Y-%m-%d')}")
        
        # Check ISSUE_DATE data
        if 'ISSUE_DATE' in df.columns:
            print(f"\n📊 ISSUE_DATE Analysis:")
            print(f"   Total records: {len(df)}")
            print(f"   Non-null dates: {df['ISSUE_DATE'].notna().sum()}")
            print(f"   Date range: {df['ISSUE_DATE'].min()} to {df['ISSUE_DATE'].max()}")
            
            # Filter by previous month
            previous_month_data = df[
                (df['ISSUE_DATE'] >= previous_month_start) & 
                (df['ISSUE_DATE'] <= previous_month_end)
            ]
            print(f"   Records in previous month: {len(previous_month_data)}")
            
            if len(previous_month_data) > 0:
                print(f"   Sample dates in previous month:")
                print(f"   {previous_month_data['ISSUE_DATE'].head(10).tolist()}")
        
        # Check WG2 (Bureau) data
        if 'WG2' in df.columns:
            print(f"\n🏢 WG2 (Bureau) Analysis:")
            print(f"   Unique values: {df['WG2'].unique()}")
            print(f"   Value counts:")
            print(df['WG2'].value_counts())
            
            # Check Traffic Bureau specifically
            traffic_data = df[df['WG2'] == "Traffic Bureau"]
            print(f"   Traffic Bureau records: {len(traffic_data)}")
            
            # Check Patrol Bureau specifically
            patrol_data = df[df['WG2'] == "Patrol Bureau"]
            print(f"   Patrol Bureau records: {len(patrol_data)}")
        
        # Check TYPE data
        if 'TYPE' in df.columns:
            print(f"\n🚗 TYPE Analysis:")
            print(f"   Unique values: {df['TYPE'].unique()}")
            print(f"   Value counts:")
            print(df['TYPE'].value_counts())
            
            # Check Parking (P) specifically
            parking_data = df[df['TYPE'] == "P"]
            print(f"   Parking (P) records: {len(parking_data)}")
            
            # Check Moving (M) specifically
            moving_data = df[df['TYPE'] == "M"]
            print(f"   Moving (M) records: {len(moving_data)}")
        
        # Check ASSIGNMENT_FOUND data
        if 'ASSIGNMENT_FOUND' in df.columns:
            print(f"\n✅ ASSIGNMENT_FOUND Analysis:")
            print(f"   Unique values: {df['ASSIGNMENT_FOUND'].unique()}")
            print(f"   Value counts:")
            print(df['ASSIGNMENT_FOUND'].value_counts())
            
            # Check TRUE specifically
            assigned_data = df[df['ASSIGNMENT_FOUND'] == True]
            print(f"   Assigned (TRUE) records: {len(assigned_data)}")
        
        # Combined filter test
        print(f"\n🔍 COMBINED FILTER TEST:")
        
        # Test Traffic Bureau + Parking + Assigned + Previous Month
        traffic_parking_prev_month = df[
            (df['WG2'] == "Traffic Bureau") &
            (df['TYPE'] == "P") &
            (df['ASSIGNMENT_FOUND'] == True) &
            (df['ISSUE_DATE'] >= previous_month_start) &
            (df['ISSUE_DATE'] <= previous_month_end)
        ]
        print(f"   Traffic Bureau + Parking + Assigned + Previous Month: {len(traffic_parking_prev_month)} records")
        
        if len(traffic_parking_prev_month) > 0:
            print(f"   Sample data:")
            print(traffic_parking_prev_month[['WG2', 'TYPE', 'ASSIGNMENT_FOUND', 'ISSUE_DATE', 'TOTAL_PAID_AMOUNT']].head(5))
        
        # Test without date filter
        traffic_parking_all = df[
            (df['WG2'] == "Traffic Bureau") &
            (df['TYPE'] == "P") &
            (df['ASSIGNMENT_FOUND'] == True)
        ]
        print(f"   Traffic Bureau + Parking + Assigned (ALL DATES): {len(traffic_parking_all)} records")
        
        # Test without assignment filter
        traffic_parking_no_assignment = df[
            (df['WG2'] == "Traffic Bureau") &
            (df['TYPE'] == "P") &
            (df['ISSUE_DATE'] >= previous_month_start) &
            (df['ISSUE_DATE'] <= previous_month_end)
        ]
        print(f"   Traffic Bureau + Parking + Previous Month (no assignment filter): {len(traffic_parking_no_assignment)} records")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_previous_month_data()
