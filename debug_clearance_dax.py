import pandas as pd
from pathlib import Path

def debug_clearance_dax():
    """Debug why the DAX measure isn't showing July 2025 data"""
    
    print("🔍 DEBUGGING CLEARANCE DAX - JULY 2025")
    print("=" * 50)
    
    # Check if the table exists and has the right data
    print("📊 POSSIBLE ISSUES:")
    print("1. Table name mismatch")
    print("2. Column name mismatch") 
    print("3. July 2025 data not in the table")
    print("4. Filter context issues")
    print("5. DAX measure not properly connected to visual")
    
    print("\n🔍 TROUBLESHOOTING STEPS:")
    print("1. Check if table name is 'Det_case_dispositions_clearance'")
    print("2. Check if column name is 'Closed Case Dispositions'")
    print("3. Check if 'Value' column exists")
    print("4. Verify July 2025 data exists in the table")
    print("5. Check if visual has proper filters applied")
    
    print("\n💡 DAX MEASURE DEBUGGING:")
    print("Try this simplified version first:")
    print("=" * 40)
    print("Test Measure = ")
    print("VAR ClearanceRate = SELECTEDVALUE('Det_case_dispositions_clearance'[Value])")
    print("VAR DispositionType = SELECTEDVALUE('Det_case_dispositions_clearance'[Closed Case Dispositions])")
    print("VAR MonthFilter = SELECTEDVALUE('Det_case_dispositions_clearance'[Month])")
    print("RETURN")
    print("    \"Rate: \" & FORMAT(ClearanceRate, \"0.0\") & \" | Type: \" & DispositionType & \" | Month: \" & MonthFilter")
    print("=" * 40)
    
    print("\n🎯 SPECIFIC CHECKS:")
    print("1. In Power BI, go to Data view")
    print("2. Find 'Det_case_dispositions_clearance' table")
    print("3. Check if '07-25' exists in Month column")
    print("4. Check if 'Clearance Percentage' exists in 'Closed Case Dispositions' column")
    print("5. Check if the Value for July 2025 Clearance Percentage is 46.0")
    
    print("\n🔧 ALTERNATIVE DAX (if table name is different):")
    print("Try this if the table name might be different:")
    print("=" * 40)
    print("Performance Icon = ")
    print("VAR ClearanceRate = SELECTEDVALUE('Det_case_dispositions_clearance'[Value], 0)")
    print("VAR DispositionType = SELECTEDVALUE('Det_case_dispositions_clearance'[Closed Case Dispositions], \"\")")
    print("VAR MonthFilter = SELECTEDVALUE('Det_case_dispositions_clearance'[Month], \"\")")
    print("RETURN")
    print("IF(")
    print("    DispositionType = \"Clearance Percentage\",")
    print("    SWITCH(")
    print("        TRUE(),")
    print("        ClearanceRate >= 50, \"🟢 \" & FORMAT(ClearanceRate, \"0.0\") & \"%\",")
    print("        ClearanceRate >= 45, \"🟡 \" & FORMAT(ClearanceRate, \"0.0\") & \"%\", ")
    print("        ClearanceRate >= 40, \"🟠 \" & FORMAT(ClearanceRate, \"0.0\") & \"%\",")
    print("        ClearanceRate >= 35, \"🔴 \" & FORMAT(ClearanceRate, \"0.0\") & \"%\",")
    print("        \"🔴 \" & FORMAT(ClearanceRate, \"0.0\") & \"%\"")
    print("    ),")
    print("    FORMAT(ClearanceRate, \"#,0\")")
    print(")")
    print("=" * 40)

if __name__ == "__main__":
    debug_clearance_dax()
