#!/usr/bin/env python3
# 🕒 2025-06-28-18-45-00
# Project: Police_Analytics_Dashboard/rapid_deployment_test
# Author: R. A. Carucci
# Purpose: Quick validation script for July 1st dashboard deployment

import pandas as pd
import logging
from pathlib import Path
from datetime import datetime
import sys

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def validate_deployment_readiness():
    """Validate all components for rapid dashboard deployment"""
    
    print("🚀 RAPID DASHBOARD DEPLOYMENT VALIDATION")
    print("=" * 50)
    
    base_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack")
    validation_results = {}
    
    # 1. Check folder structure
    print("\n📁 FOLDER STRUCTURE CHECK:")
    required_folders = [
        "02_ETL_Scripts/Summons",
        "03_Staging/Summons", 
        "04_PowerBI/Summons_Dashboard"
    ]
    
    for folder in required_folders:
        folder_path = base_path / folder
        exists = folder_path.exists()
        print(f"  {folder}: {'✅' if exists else '❌'}")
        if not exists:
            print(f"    Creating: {folder_path}")
            folder_path.mkdir(parents=True, exist_ok=True)
        validation_results[f"folder_{folder.replace('/', '_')}"] = exists
    
    # 2. Check data files
    print("\n📊 DATA FILES CHECK:")
    data_files = {
        "Assignment_Master": "Assignment_Master.xlsm",
        "Summons_Data": "SUMMONS_MASTER.xlsx", 
        "ATS_Report": "2025_04_ATS_Report.xlsx"
    }
    
    for name, filename in data_files.items():
        # Check multiple possible locations
        possible_paths = [
            base_path / "01_SourceData" / filename,
            base_path / "_ASSIGNMENTS" / filename,
            base_path / "_MONTHLY_DATA" / "_EXPORTS" / "Summons" / filename,
            base_path / filename
        ]
        
        found = False
        for path in possible_paths:
            if path.exists():
                print(f"  {name}: ✅ Found at {path}")
                validation_results[f"data_{name}"] = str(path)
                found = True
                break
        
        if not found:
            print(f"  {name}: ❌ NOT FOUND")
            validation_results[f"data_{name}"] = None
    
    # 3. Check ETL scripts
    print("\n🔧 ETL SCRIPTS CHECK:")
    etl_scripts = [
        "summons_etl_enhanced.py",
        "summons_master_production.py", 
        "file_migration_executor.py",
        "police_analytics_structure.py"
    ]
    
    etl_folder = base_path / "02_ETL_Scripts" / "Summons"
    for script in etl_scripts:
        script_path = etl_folder / script
        exists = script_path.exists()
        print(f"  {script}: {'✅' if exists else '❌'}")
        validation_results[f"script_{script}"] = exists
    
    # 4. Test data loading capability
    print("\n📈 DATA LOADING TEST:")
    try:
        # Test Assignment Master loading
        assignment_path = validation_results.get("data_Assignment_Master")
        if assignment_path:
            df_assignment = pd.read_excel(assignment_path)
            print(f"  Assignment Master: ✅ Loaded {len(df_assignment)} records")
            validation_results["assignment_load_success"] = True
            validation_results["assignment_record_count"] = len(df_assignment)
        else:
            print(f"  Assignment Master: ❌ File not found")
            validation_results["assignment_load_success"] = False
        
        # Test Summons data loading  
        summons_path = validation_results.get("data_Summons_Data")
        if summons_path:
            xl_file = pd.ExcelFile(summons_path)
            print(f"  Summons Master: ✅ Found sheets: {xl_file.sheet_names[:5]}...")
            
            # Try to load ENHANCED_BADGE sheet
            if 'ENHANCED_BADGE' in xl_file.sheet_names:
                df_summons = pd.read_excel(summons_path, sheet_name='ENHANCED_BADGE')
                print(f"  ENHANCED_BADGE sheet: ✅ Loaded {len(df_summons)} records")
                validation_results["summons_load_success"] = True
                validation_results["summons_record_count"] = len(df_summons)
            else:
                print(f"  ENHANCED_BADGE sheet: ❌ Not found")
                validation_results["summons_load_success"] = False
        else:
            print(f"  Summons Master: ❌ File not found")
            validation_results["summons_load_success"] = False
            
    except Exception as e:
        print(f"  Data Loading Error: ❌ {e}")
        validation_results["data_load_error"] = str(e)
    
    # 5. Generate deployment readiness score
    print("\n🎯 DEPLOYMENT READINESS ASSESSMENT:")
    
    # Calculate readiness score
    total_checks = 0
    passed_checks = 0
    
    for key, value in validation_results.items():
        if isinstance(value, bool):
            total_checks += 1
            if value:
                passed_checks += 1
    
    readiness_score = (passed_checks / total_checks) * 100 if total_checks > 0 else 0
    
    print(f"  Overall Readiness: {readiness_score:.1f}% ({passed_checks}/{total_checks} checks passed)")
    
    if readiness_score >= 80:
        print(f"  Status: 🟢 READY FOR DEPLOYMENT")
        deployment_status = "READY"
    elif readiness_score >= 60:
        print(f"  Status: 🟡 NEEDS MINOR FIXES")
        deployment_status = "MINOR_ISSUES"
    else:
        print(f"  Status: 🔴 MAJOR ISSUES - NEEDS ATTENTION")
        deployment_status = "MAJOR_ISSUES"
    
    # 6. Generate action plan
    print(f"\n📋 IMMEDIATE ACTION PLAN:")
    
    if validation_results.get("data_Assignment_Master") is None:
        print(f"  🔥 CRITICAL: Locate Assignment_Master.xlsm file")
    
    if validation_results.get("data_Summons_Data") is None:
        print(f"  🔥 CRITICAL: Locate SUMMONS_MASTER.xlsx file")
    
    if not validation_results.get("script_summons_etl_enhanced.py"):
        print(f"  ⚠️  URGENT: Deploy summons_etl_enhanced.py script")
    
    if not validation_results.get("summons_load_success"):
        print(f"  ⚠️  URGENT: Fix summons data loading issues")
    
    print(f"\n  📍 Next Step: Run ETL pipeline test")
    print(f"  📍 Timeline: Dashboard must be operational by July 1, 2025 (2 days)")
    
    # 7. Save validation report
    report_path = base_path / "06_Documentation" / "Change_Log" / f"deployment_validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(report_path, 'w') as f:
        f.write(f"DASHBOARD DEPLOYMENT VALIDATION REPORT\n")
        f.write(f"Generated: {datetime.now()}\n")
        f.write(f"Readiness Score: {readiness_score:.1f}%\n")
        f.write(f"Status: {deployment_status}\n\n")
        
        f.write("VALIDATION RESULTS:\n")
        for key, value in validation_results.items():
            f.write(f"  {key}: {value}\n")
    
    print(f"\n📄 Validation report saved: {report_path}")
    
    return validation_results, readiness_score, deployment_status

def create_emergency_etl_pipeline():
    """Create simplified ETL pipeline for emergency deployment"""
    
    print(f"\n🚑 CREATING EMERGENCY ETL PIPELINE...")
    
    base_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack")
    
    # Simple ETL script for immediate deployment
    simple_etl = '''
import pandas as pd
from pathlib import Path

# Simple summons data processor for emergency deployment
def process_summons_data():
    base_path = Path(r"C:\\Users\\carucci_r\\OneDrive - City of Hackensack")
    
    # Load assignment master
    assignment_df = pd.read_excel(base_path / "Assignment_Master.xlsm")
    
    # Load summons data (try ENHANCED_BADGE sheet)
    summons_df = pd.read_excel(base_path / "SUMMONS_MASTER.xlsx", sheet_name="ENHANCED_BADGE")
    
    # Basic processing - clean and output
    output_path = base_path / "03_Staging" / "Summons" / "summons_data_simple.xlsx"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    summons_df.to_excel(output_path, index=False)
    print(f"Emergency ETL complete: {output_path}")

if __name__ == "__main__":
    process_summons_data()
'''
    
    emergency_script_path = base_path / "02_ETL_Scripts" / "Summons" / "emergency_etl.py"
    emergency_script_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(emergency_script_path, 'w') as f:
        f.write(simple_etl)
    
    print(f"✅ Emergency ETL created: {emergency_script_path}")
    
    return emergency_script_path

if __name__ == "__main__":
    print("Starting 72-hour dashboard deployment validation...\n")
    
    try:
        results, score, status = validate_deployment_readiness()
        
        if status == "MAJOR_ISSUES":
            print(f"\n🚑 Creating emergency deployment tools...")
            emergency_script = create_emergency_etl_pipeline()
            print(f"✅ Emergency tools ready")
        
        print(f"\n🎯 SUMMARY:")
        print(f"   Deployment readiness: {score:.1f}%")
        print(f"   Status: {status}")
        print(f"   Days remaining: 2")
        print(f"   Target: July 1, 2025")
        
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        print(f"🚑 Creating emergency deployment tools...")
        emergency_script = create_emergency_etl_pipeline()
