import pandas as pd
from pathlib import Path

def check_clearance_thresholds():
    """Check what clearance rate thresholds are being used"""
    
    print("🔍 CLEARANCE RATE THRESHOLD ANALYSIS")
    print("=" * 50)
    
    # Based on the visual, it appears the thresholds are:
    print("📊 VISUAL THRESHOLDS (based on the image):")
    print("   🟢 Excellent (50%+) - Green")
    print("   🟡 Good (45-49%) - Yellow") 
    print("   🟠 Fair (40-44%) - Orange")
    print("   🔴 Below Target (35-39%) - Red")
    print("   🔴 Needs Improvement (<35%) - Red")
    
    print("\n📊 YOUR DAX THRESHOLDS:")
    print("   🟢 60%+ - Green")
    print("   🟡 50-59% - Yellow")
    print("   🟠 40-49% - Orange")
    print("   🔴 <40% - Red")
    
    print("\n🎯 THE MISMATCH:")
    print("   - July 2025: 46.0%")
    print("   - Visual shows: Orange (Fair 40-44%)")
    print("   - Your DAX would show: Orange (40-49%)")
    print("   - But the visual is using different thresholds!")
    
    print("\n💡 SOLUTION:")
    print("   Update your DAX to match the visual thresholds:")
    print("   - 🟢 50%+ (Excellent)")
    print("   - 🟡 45-49% (Good)")
    print("   - 🟠 40-44% (Fair)")
    print("   - 🔴 35-39% (Below Target)")
    print("   - 🔴 <35% (Needs Improvement)")

if __name__ == "__main__":
    check_clearance_thresholds()
