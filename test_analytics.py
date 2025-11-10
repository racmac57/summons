#!/usr/bin/env python3

print("="*50)
print("DEBUG: Starting analytics test...")
print("="*50)

try:
    import argparse
    print("✓ argparse imported successfully")
except Exception as e:
    print(f"✗ argparse import failed: {e}")

try:
    from pathlib import Path
    print("✓ pathlib imported successfully")
except Exception as e:
    print(f"✗ pathlib import failed: {e}")

# Test paths
overtime_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\05_EXPORTS\_Overtime")
timeoff_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\05_EXPORTS\_Time_Off")

print(f"\n📁 Checking data directories...")
print(f"Overtime path: {overtime_path}")
print(f"Overtime exists: {overtime_path.exists()}")

if overtime_path.exists():
    files = list(overtime_path.glob("*.xls*"))
    print(f"Overtime files: {len(files)}")
    for f in files[:3]:
        print(f"  - {f.name}")
else:
    print("⚠️ Overtime directory not found")

print(f"\nTimeoff path: {timeoff_path}")
print(f"Timeoff exists: {timeoff_path.exists()}")

if timeoff_path.exists():
    files = list(timeoff_path.glob("*.xls*"))
    print(f"Timeoff files: {len(files)}")
    for f in files[:3]:
        print(f"  - {f.name}")
else:
    print("⚠️ Timeoff directory not found")

# Test argparse
print(f"\n🔧 Testing argument parsing...")
try:
    parser = argparse.ArgumentParser(description="Test Analytics")
    parser.add_argument("--test", action="store_true", help="Test flag")
    args = parser.parse_args()
    print(f"✓ Arguments parsed successfully")
    print(f"Test flag: {args.test}")
    
    if args.test:
        print("🎯 Test mode activated!")
    else:
        print("📋 Showing help...")
        parser.print_help()
        
except Exception as e:
    print(f"✗ Argument parsing failed: {e}")

print(f"\n" + "="*50)
print("DEBUG: Test completed!")
print("="*50)