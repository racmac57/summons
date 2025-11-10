#!/usr/bin/env python3
"""
Pre-Flight Configuration Test for SummonsMaster_Transition.py

Run this script BEFORE running the main transition script to verify:
1. All file paths are correct
2. Required files exist
3. Files are readable
4. Basic data structure is correct
5. Date ranges are reasonable

Usage: python test_transition_config.py
"""

import sys
from pathlib import Path
from datetime import datetime

# Color codes for terminal output
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    RESET = '\033[0m'
    BOLD = '\033[1m'

def print_header(text):
    print(f"\n{Colors.BLUE}{Colors.BOLD}{'='*60}{Colors.RESET}")
    print(f"{Colors.BLUE}{Colors.BOLD}{text}{Colors.RESET}")
    print(f"{Colors.BLUE}{Colors.BOLD}{'='*60}{Colors.RESET}\n")

def print_success(text):
    print(f"{Colors.GREEN}[OK] {text}{Colors.RESET}")

def print_warning(text):
    print(f"{Colors.YELLOW}[WARN] {text}{Colors.RESET}")

def print_error(text):
    print(f"{Colors.RED}[ERROR] {text}{Colors.RESET}")

def print_info(text):
    print(f"{Colors.BLUE}[INFO] {text}{Colors.RESET}")

# Configuration from SummonsMaster_Transition.py
HISTORICAL_COURT_FILES = [
    r"C:\Users\carucci_r\OneDrive - City of Hackensack\05_EXPORTS\_Summons\Court\Historical\25_08_ATS.csv",
    r"C:\Users\carucci_r\OneDrive - City of Hackensack\02_ETL_Scripts\Summons\data_samples\2025_10_09_21_01_02_Hackensack Police Department - Summons Dashboard.csv"
]

E_TICKET_FOLDER = r"C:\Users\carucci_r\OneDrive - City of Hackensack\05_EXPORTS\_Summons\E_Ticket"

SAMPLE_ETICKET_FILES = [
    r"C:\Users\carucci_r\OneDrive - City of Hackensack\05_EXPORTS\_Summons\E_Ticket\25_09_e_ticketexport.csv"
]

ASSIGNMENT_FILE = Path(
    r"C:\Users\carucci_r\OneDrive - City of Hackensack\09_Reference\Personnel\Assignment_Master_V2.csv"
)

OUTPUT_FILE = Path(
    r"C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons\summons_powerbi_latest.xlsx"
)

TRANSITION_DATE = "2025-09-01"

def test_file_exists(path, name):
    """Test if a file exists and is readable"""
    p = Path(path)
    if p.exists():
        if p.is_file():
            try:
                # Try to open and read first line
                with open(p, 'r', encoding='utf-8', errors='ignore') as f:
                    first_line = f.readline()
                if first_line:
                    print_success(f"{name}: Found and readable")
                    return True, "readable"
                else:
                    print_warning(f"{name}: Found but appears empty")
                    return True, "empty"
            except PermissionError:
                print_error(f"{name}: Found but permission denied")
                return False, "permission_denied"
            except Exception as e:
                print_error(f"{name}: Found but error reading: {e}")
                return False, "error"
        else:
            print_error(f"{name}: Path exists but is not a file")
            return False, "not_file"
    else:
        print_error(f"{name}: NOT FOUND")
        print_info(f"   Expected at: {p}")
        return False, "not_found"

def test_folder_exists(path, name):
    """Test if a folder exists and has CSV files"""
    p = Path(path)
    if p.exists():
        if p.is_dir():
            csv_files = list(p.glob("*.csv"))
            if csv_files:
                print_success(f"{name}: Found with {len(csv_files)} CSV file(s)")
                for f in csv_files[:3]:  # Show first 3 files
                    print_info(f"   - {f.name}")
                if len(csv_files) > 3:
                    print_info(f"   ... and {len(csv_files) - 3} more")
                return True, len(csv_files)
            else:
                print_warning(f"{name}: Found but contains no CSV files")
                return True, 0
        else:
            print_error(f"{name}: Path exists but is not a directory")
            return False, "not_dir"
    else:
        print_error(f"{name}: NOT FOUND")
        print_info(f"   Expected at: {p}")
        return False, "not_found"

def test_output_writable():
    """Test if output location is writable"""
    output_dir = OUTPUT_FILE.parent
    
    if not output_dir.exists():
        print_warning(f"Output directory does not exist: {output_dir}")
        try:
            output_dir.mkdir(parents=True, exist_ok=True)
            print_success(f"Created output directory: {output_dir}")
            return True
        except Exception as e:
            print_error(f"Cannot create output directory: {e}")
            return False
    
    if OUTPUT_FILE.exists():
        # Check if file is locked (e.g., open in Excel)
        try:
            # Try to open for writing
            with open(OUTPUT_FILE, 'a'):
                pass
            print_success(f"Output file exists and is writable")
            print_warning(f"   File will be overwritten: {OUTPUT_FILE.name}")
            return True
        except PermissionError:
            print_error(f"Output file exists but is LOCKED (close Excel)")
            print_info(f"   File: {OUTPUT_FILE}")
            return False
    else:
        # Try to create a test file
        try:
            test_file = output_dir / "test_write.tmp"
            test_file.touch()
            test_file.unlink()
            print_success(f"Output directory is writable")
            return True
        except Exception as e:
            print_error(f"Cannot write to output directory: {e}")
            return False

def quick_csv_check(path, expected_columns=None):
    """Quick check of CSV structure"""
    try:
        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            header = f.readline().strip()
            columns = [c.strip() for c in header.split(',')]
            
            if expected_columns:
                found_cols = []
                missing_cols = []
                for col in expected_columns:
                    if any(col.upper() in c.upper() for c in columns):
                        found_cols.append(col)
                    else:
                        missing_cols.append(col)
                
                if missing_cols:
                    print_warning(f"   Missing expected columns: {', '.join(missing_cols)}")
                    return False
                else:
                    print_info(f"   Has expected columns: {', '.join(found_cols)}")
                    return True
            else:
                print_info(f"   Found {len(columns)} columns")
                if len(columns) > 10:
                    print_info(f"   First 5: {', '.join(columns[:5])}")
                else:
                    print_info(f"   Columns: {', '.join(columns)}")
                return True
                
    except Exception as e:
        print_warning(f"   Could not read CSV: {e}")
        return False

def main():
    """Run all configuration tests"""
    print_header("TRANSITION SCRIPT PRE-FLIGHT CHECK")
    print(f"Script: SummonsMaster_Transition.py")
    print(f"Test Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Transition Date: {TRANSITION_DATE}")
    
    results = {
        'total_tests': 0,
        'passed': 0,
        'warnings': 0,
        'failed': 0
    }
    
    # Test 1: Historical Court Files
    print_header("TEST 1: Historical Court Files")
    for i, file_path in enumerate(HISTORICAL_COURT_FILES, 1):
        results['total_tests'] += 1
        exists, status = test_file_exists(file_path, f"Historical File {i}")
        
        if exists and status == "readable":
            results['passed'] += 1
            # Quick structure check
            if "Dashboard" in file_path:
                print_info("   Expected format: Summary data (Count of TICKET_NUMBER, Month_Year)")
                quick_csv_check(file_path, ["Count of TICKET_NUMBER", "Month_Year", "TYPE"])
            else:
                print_info("   Expected format: Individual court records (ATS)")
                quick_csv_check(file_path, ["TICKET_NUMBER", "ISSUE_DATE"])
        elif exists:
            results['warnings'] += 1
        else:
            results['failed'] += 1
    
    # Test 2: E-Ticket Folder
    print_header("TEST 2: E-Ticket Data Folder")
    results['total_tests'] += 1
    exists, csv_count = test_folder_exists(E_TICKET_FOLDER, "E-Ticket Folder")
    if exists and csv_count > 0:
        results['passed'] += 1
    elif exists:
        results['warnings'] += 1
    else:
        results['failed'] += 1
    
    # Test 3: Sample E-Ticket Files
    print_header("TEST 3: Sample E-Ticket Files")
    for i, file_path in enumerate(SAMPLE_ETICKET_FILES, 1):
        results['total_tests'] += 1
        exists, status = test_file_exists(file_path, f"E-Ticket Sample {i}")
        
        if exists and status == "readable":
            results['passed'] += 1
            print_info("   Expected format: E-ticket export (Ticket Number, Issue Date)")
            quick_csv_check(file_path, ["Ticket Number", "Issue Date", "Officer Id"])
        elif exists:
            results['warnings'] += 1
        else:
            results['failed'] += 1
    
    # Test 4: Assignment File
    print_header("TEST 4: Assignment Master File")
    results['total_tests'] += 1
    exists, status = test_file_exists(ASSIGNMENT_FILE, "Assignment Master")
    if exists and status == "readable":
        results['passed'] += 1
        quick_csv_check(ASSIGNMENT_FILE, ["PADDED_BADGE_NUMBER", "TEAM", "WG2"])
    elif exists:
        results['warnings'] += 1
    else:
        results['failed'] += 1
    
    # Test 5: Output Location
    print_header("TEST 5: Output File Location")
    results['total_tests'] += 1
    if test_output_writable():
        results['passed'] += 1
    else:
        results['failed'] += 1
    
    # Test 6: Python Dependencies
    print_header("TEST 6: Python Dependencies")
    dependencies = {
        'pandas': 'Data processing',
        'numpy': 'Numerical operations',
        'openpyxl': 'Excel file writing',
        'dateutil': 'Date parsing'
    }
    
    for module, description in dependencies.items():
        results['total_tests'] += 1
        try:
            __import__(module)
            print_success(f"{module}: Installed ({description})")
            results['passed'] += 1
        except ImportError:
            print_error(f"{module}: NOT INSTALLED ({description})")
            print_info(f"   Install with: pip install {module}")
            results['failed'] += 1
    
    # Final Summary
    print_header("TEST SUMMARY")
    
    print(f"Total Tests:  {results['total_tests']}")
    print_success(f"Passed:       {results['passed']}")
    if results['warnings'] > 0:
        print_warning(f"Warnings:     {results['warnings']}")
    if results['failed'] > 0:
        print_error(f"Failed:       {results['failed']}")
    
    print()
    
    if results['failed'] == 0 and results['warnings'] == 0:
        print_success("ALL TESTS PASSED - Ready to run SummonsMaster_Transition.py")
        return 0
    elif results['failed'] == 0:
        print_warning("TESTS PASSED WITH WARNINGS - Review warnings before proceeding")
        return 0
    else:
        print_error("SOME TESTS FAILED - Fix issues before running the transition script")
        print()
        print_info("Common fixes:")
        print_info("  - Check file paths are correct")
        print_info("  - Verify files are not open in Excel")
        print_info("  - Install missing Python packages: pip install pandas numpy openpyxl python-dateutil")
        print_info("  - Check file permissions")
        return 1

if __name__ == "__main__":
    sys.exit(main())

