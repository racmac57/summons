import pandas as pd
from pathlib import Path

def debug_clearance_value():
    """Debug what the actual clearance rate value is for July 2025"""
    
    print("🔍 DEBUGGING CLEARANCE RATE VALUE - JULY 2025")
    print("=" * 50)
    
    print("📊 FROM THE IMAGE ANALYSIS:")
    print("   - The DAX measure IS working")
    print("   - July 2025 shows: 🔴 %")
    print("   - This means the clearance rate is below 35%")
    print("   - But we need to see the actual value")
    
    print("\n💡 CREATE THIS DEBUG MEASURE:")
    print("=" * 40)
    print("Debug Clearance Value = ")
    print("VAR PreviousMonthStart = EOMONTH(TODAY(), -2) + 1")
    print("VAR PreviousMonthEnd = EOMONTH(TODAY(), -1)")
    print("VAR PreviousMonthMMYY = FORMAT(PreviousMonthStart, \"MM-yy\")")
    print("")
    print("VAR ClearanceRateForPreviousMonth = ")
    print("    CALCULATE(")
    print("        MAX('Det_case_dispositions_clearance'[Value]),")
    print("        'Det_case_dispositions_clearance'[Closed Case Dispositions] = \"Clearance Percentage\",")
    print("        'Det_case_dispositions_clearance'[Month] = PreviousMonthMMYY,")
    print("        ALL('Det_case_dispositions_clearance'[Month]),")
    print("        ALL('Det_case_dispositions_clearance'[Closed Case Dispositions])")
    print("    )")
    print("RETURN")
    print("    \"Previous Month: \" & PreviousMonthMMYY & \" | Clearance Rate: \" & FORMAT(ClearanceRateForPreviousMonth, \"0.0\") & \"%\"")
    print("=" * 40)
    
    print("\n🎯 ALTERNATIVE SIMPLE DEBUG:")
    print("=" * 40)
    print("Simple Debug = ")
    print("VAR PreviousMonthMMYY = FORMAT(EOMONTH(TODAY(), -2) + 1, \"MM-yy\")")
    print("VAR ClearanceValue = ")
    print("    CALCULATE(")
    print("        MAX('Det_case_dispositions_clearance'[Value]),")
    print("        'Det_case_dispositions_clearance'[Closed Case Dispositions] = \"Clearance Percentage\",")
    print("        'Det_case_dispositions_clearance'[Month] = PreviousMonthMMYY")
    print("    )")
    print("RETURN")
    print("    ClearanceValue")
    print("=" * 40)
    
    print("\n🔧 IF THE VALUE IS VERY LOW:")
    print("   - Check if the data source has been updated")
    print("   - Verify the clearance rate calculation in the M code")
    print("   - The manual clearance rates table might need updating")
    print("   - July 2025 might actually have a very low clearance rate")

if __name__ == "__main__":
    debug_clearance_value()
