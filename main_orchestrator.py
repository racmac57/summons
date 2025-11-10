#!/usr/bin/env python3
# 🕒 2025-06-28-21-15-00
# Project: Police_Analytics_Dashboard/main_orchestrator
# Author: R. A. Carucci
# Purpose: Master orchestrator for July 1st dashboard deployment

import argparse
import logging
import sys
from pathlib import Path
from datetime import datetime

def init_logger():
    """Initialize logging for main orchestrator"""
    logger = logging.getLogger("main_orchestrator")
    
    # Create formatter
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler
    log_file = Path("orchestrator.log")
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    logger.setLevel(logging.INFO)
    return logger

def run_deployment_validation():
    """Run deployment readiness validation"""
    try:
        import subprocess
        result = subprocess.run([sys.executable, "rapid_deployment_test.py"], 
                              capture_output=True, text=True)
        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)
        return result.returncode == 0
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        return False

def run_file_finder():
    """Run emergency file finder"""
    try:
        import subprocess
        result = subprocess.run([sys.executable, "emergency_file_finder.py"], 
                              capture_output=True, text=True)
        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)
        return result.returncode == 0
    except Exception as e:
        print(f"❌ File finder failed: {e}")
        return False

def run_production_etl():
    """Run production ETL pipeline"""
    try:
        import subprocess
        result = subprocess.run([sys.executable, "summons_etl_enhanced.py"], 
                              capture_output=True, text=True)
        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)
        return result.returncode == 0
    except Exception as e:
        print(f"❌ ETL failed: {e}")
        return False

def run_tests():
    """Run utility tests"""
    try:
        import subprocess
        result = subprocess.run([sys.executable, "test_summons_utils.py"], 
                              capture_output=True, text=True)
        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)
        return result.returncode == 0
    except Exception as e:
        print(f"❌ Tests failed: {e}")
        return False

def full_deployment():
    """Run complete deployment sequence"""
    logger = init_logger()
    
    print("🚀 POLICE ANALYTICS - FULL DEPLOYMENT SEQUENCE")
    print("=" * 60)
    print(f"🕒 Started: {datetime.now()}")
    print(f"🎯 Target: July 1, 2025 Dashboard Deployment")
    print()
    
    steps = [
        ("🔍 Step 1: Deployment Validation", run_deployment_validation),
        ("📁 Step 2: File Discovery & Staging", run_file_finder),
        ("⚙️ Step 3: Production ETL Pipeline", run_production_etl),
        ("🧪 Step 4: Utility Tests", run_tests)
    ]
    
    results = {}
    
    for step_name, step_function in steps:
        print(f"\n{step_name}")
        print("-" * 50)
        
        try:
            success = step_function()
            results[step_name] = success
            
            if success:
                print(f"✅ {step_name} - SUCCESS")
            else:
                print(f"❌ {step_name} - FAILED")
                
                # Ask user if they want to continue
                response = input(f"\n⚠️ {step_name} failed. Continue anyway? (y/n): ").lower()
                if response != 'y':
                    print(f"🛑 Deployment stopped by user")
                    break
                    
        except Exception as e:
            print(f"❌ {step_name} - ERROR: {e}")
            results[step_name] = False
    
    # Summary
    print(f"\n📊 DEPLOYMENT SUMMARY")
    print("=" * 40)
    
    total_steps = len(steps)
    successful_steps = sum(1 for success in results.values() if success)
    
    for step_name, success in results.items():
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"  {status} {step_name}")
    
    success_rate = (successful_steps / total_steps) * 100
    print(f"\n🎯 Overall Success Rate: {success_rate:.1f}% ({successful_steps}/{total_steps})")
    
    if success_rate >= 75:
        print(f"🟢 STATUS: DEPLOYMENT READY!")
        print(f"\n📋 NEXT STEPS:")
        print(f"  1. Connect Power BI to: 03_Staging/Summons/summons_powerbi_latest.xlsx")
        print(f"  2. Build dashboard visualizations")
        print(f"  3. Test and deploy by July 1st")
    elif success_rate >= 50:
        print(f"🟡 STATUS: PARTIAL SUCCESS - Manual fixes needed")
    else:
        print(f"🔴 STATUS: DEPLOYMENT ISSUES - Review errors above")
    
    print(f"\n🕒 Completed: {datetime.now()}")
    
    return success_rate >= 75

def main():
    """Main CLI interface"""
    parser = argparse.ArgumentParser(
        description="Police Analytics Dashboard ETL Orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --full-deploy          # Run complete deployment sequence
  python main.py --validate             # Check deployment readiness
  python main.py --find-files           # Locate and stage data files
  python main.py --run-etl              # Run production ETL only
  python main.py --test                 # Run utility tests
  python main.py --status               # Show current status
        """
    )
    
    parser.add_argument("--full-deploy", action="store_true", 
                       help="Run complete deployment sequence")
    parser.add_argument("--validate", action="store_true", 
                       help="Run deployment readiness validation")
    parser.add_argument("--find-files", action="store_true", 
                       help="Run file discovery and staging")
    parser.add_argument("--run-etl", action="store_true", 
                       help="Run production ETL pipeline")
    parser.add_argument("--test", action="store_true", 
                       help="Run utility tests")
    parser.add_argument("--status", action="store_true", 
                       help="Show current deployment status")
    
    args = parser.parse_args()
    
    logger = init_logger()
    
    if args.full_deploy:
        logger.info("Starting full deployment sequence...")
        success = full_deployment()
        sys.exit(0 if success else 1)
        
    elif args.validate:
        logger.info("Running deployment validation...")
        success = run_deployment_validation()
        sys.exit(0 if success else 1)
        
    elif args.find_files:
        logger.info("Running file discovery...")
        success = run_file_finder()
        sys.exit(0 if success else 1)
        
    elif args.run_etl:
        logger.info("Running production ETL...")
        success = run_production_etl()
        sys.exit(0 if success else 1)
        
    elif args.test:
        logger.info("Running utility tests...")
        success = run_tests()
        sys.exit(0 if success else 1)
        
    elif args.status:
        print("🔍 CURRENT DEPLOYMENT STATUS")
        print("=" * 40)
        
        # Check file existence
        staging_path = Path("../03_Staging/Summons")
        required_files = [
            "summons_enriched.xlsx",
            "summons_powerbi_latest.xlsx"
        ]
        
        print(f"📁 Staging Directory: {staging_path.absolute()}")
        print(f"   Exists: {'✅' if staging_path.exists() else '❌'}")
        
        if staging_path.exists():
            for file in required_files:
                file_path = staging_path / file
                exists = file_path.exists()
                if exists:
                    mod_time = datetime.fromtimestamp(file_path.stat().st_mtime)
                    print(f"   {file}: ✅ (Modified: {mod_time})")
                else:
                    print(f"   {file}: ❌ Missing")
        
        # Check scripts
        scripts = ["summons_etl_enhanced.py", "emergency_file_finder.py", "config.yaml"]
        print(f"\n🔧 ETL Scripts:")
        for script in scripts:
            exists = Path(script).exists()
            print(f"   {script}: {'✅' if exists else '❌'}")
        
    else:
        parser.print_help()

if __name__ == "__main__":
    main()