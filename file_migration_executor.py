#!/usr/bin/env python3
# 🕒 2025-06-28-15-35-00
# Project: Police_Analytics_Dashboard/file_migration_executor
# Author: R. A. Carucci
# Purpose: Execute migration from scattered folders to new 7-tier standardized structure

import os
import shutil
import logging
from pathlib import Path
from datetime import datetime
import json
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'file_migration_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)

class FileSystemMigrator:
    def __init__(self):
        """Initialize file migration system"""
        self.base_path = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack")
        
        # Define new 7-tier folder structure
        self.new_structure = {
            "01_SourceData": {
                "Summons": ["Court_Exports", "LawSoft_Exports", "Manual_Entries"],
                "Overtime": ["Timesheets", "Payroll_Reports", "Supervisor_Approvals"],
                "TimeOff": ["Vacation_Requests", "Sick_Leave", "Personal_Time"],
                "Arrests": ["Booking_Reports", "UCR_Data", "Detective_Reports"],
                "ResponseTimes": ["CAD_Exports", "Dispatch_Logs", "Unit_Activity"],
                "Assignment_Master": [],
                "ATS": ["Automated_Traffic", "Camera_Citations"]
            },
            "02_ETL_Scripts": {
                "Summons": [],
                "Overtime": [],
                "TimeOff": [],
                "Arrests": [],
                "ResponseTimes": [],
                "_Utilities": [],
                "_Archive": []
            },
            "03_Staging": {
                "Summons": [],
                "Overtime": [],
                "TimeOff": [],
                "Arrests": [],
                "ResponseTimes": [],
                "_Archive": []
            },
            "04_PowerBI": {
                "Summons_Dashboard": ["Data_Sources", "Reports", "Templates"],
                "Overtime_Dashboard": ["Data_Sources", "Reports", "Templates"],
                "TimeOff_Dashboard": ["Data_Sources", "Reports", "Templates"],
                "Arrests_Dashboard": ["Data_Sources", "Reports", "Templates"],
                "ResponseTimes_Dashboard": ["Data_Sources", "Reports", "Templates"],
                "_Shared_Resources": ["Themes", "Icons", "Common_Measures"]
            },
            "05_Reports": {
                "Monthly": ["Summons", "Overtime", "Arrests", "ResponseTimes"],
                "Quarterly": ["Executive_Summary", "Trend_Analysis"],
                "Annual": ["Performance_Reports", "Statistical_Analysis"],
                "Ad_Hoc": []
            },
            "06_Documentation": {
                "ETL_Procedures": [],
                "Data_Dictionary": [],
                "User_Guides": [],
                "Technical_Specs": [],
                "Change_Log": []
            },
            "07_Archive": {
                "Legacy_Files": [],
                "Backup_Data": [],
                "Deprecated_Reports": []
            }
        }
        
        # Define legacy folder patterns to migrate
        self.legacy_patterns = {
            "summons": ["*summons*", "*court*", "*citation*", "*ticket*"],
            "overtime": ["*overtime*", "*OT*", "*time*"],
            "timeoff": ["*vacation*", "*sick*", "*leave*", "*time_off*"],
            "arrests": ["*arrest*", "*booking*", "*UCR*"],
            "response": ["*response*", "*CAD*", "*dispatch*"],
            "assignment": ["*assignment*", "*roster*", "*personnel*"],
            "powerbi": ["*.pbix", "*.pbit"],
            "reports": ["*report*", "*summary*", "*analysis*"]
        }
    
    def create_new_structure(self):
        """Create the complete new folder structure"""
        logging.info("Creating new 7-tier folder structure...")
        
        created_folders = []
        
        for tier_name, tier_content in self.new_structure.items():
            tier_path = self.base_path / tier_name
            tier_path.mkdir(exist_ok=True)
            created_folders.append(str(tier_path))
            
            if isinstance(tier_content, dict):
                for folder_name, subfolders in tier_content.items():
                    folder_path = tier_path / folder_name
                    folder_path.mkdir(exist_ok=True)
                    created_folders.append(str(folder_path))
                    
                    for subfolder in subfolders:
                        subfolder_path = folder_path / subfolder
                        subfolder_path.mkdir(exist_ok=True)
                        created_folders.append(str(subfolder_path))
        
        logging.info(f"Created {len(created_folders)} folders in new structure")
        return created_folders
    
    def scan_existing_files(self):
        """Scan existing OneDrive structure for files to migrate"""
        logging.info("Scanning existing files for migration...")
        
        existing_files = {}
        exclude_folders = {
            "01_SourceData", "02_ETL_Scripts", "03_Staging", 
            "04_PowerBI", "05_Reports", "06_Documentation", "07_Archive"
        }
        
        for root, dirs, files in os.walk(self.base_path):
            # Skip new structure folders
            root_path = Path(root)
            if any(excluded in root_path.parts for excluded in exclude_folders):
                continue
            
            for file in files:
                file_path = Path(root) / file
                file_info = {
                    'path': str(file_path),
                    'size': file_path.stat().st_size,
                    'modified': datetime.fromtimestamp(file_path.stat().st_mtime),
                    'extension': file_path.suffix.lower(),
                    'category': self._categorize_file(file_path)
                }
                
                if file_info['category'] != 'unknown':
                    existing_files[str(file_path)] = file_info
        
        logging.info(f"Found {len(existing_files)} files to potentially migrate")
        return existing_files
    
    def _categorize_file(self, file_path):
        """Categorize file based on name and path patterns"""
        file_str = str(file_path).lower()
        
        # Check against known patterns
        for category, patterns in self.legacy_patterns.items():
            for pattern in patterns:
                if pattern.replace('*', '') in file_str:
                    return category
        
        # Check by file extension
        extension_mapping = {
            '.pbix': 'powerbi',
            '.pbit': 'powerbi',
            '.xlsx': 'data',
            '.xls': 'data',
            '.csv': 'data',
            '.py': 'script',
            '.m': 'script',
            '.sql': 'script',
            '.pdf': 'reports',
            '.docx': 'documentation',
            '.txt': 'documentation'
        }
        
        return extension_mapping.get(file_path.suffix.lower(), 'unknown')
    
    def create_migration_plan(self, existing_files):
        """Create detailed migration plan with user confirmation"""
        migration_plan = {
            'summons': [],
            'overtime': [],
            'timeoff': [],
            'arrests': [],
            'response': [],
            'assignment': [],
            'powerbi': [],
            'reports': [],
            'scripts': [],
            'documentation': [],
            'archive': []
        }
        
        for file_path, file_info in existing_files.items():
            category = file_info['category']
            
            # Determine destination based on category and file type
            if category == 'summons':
                if file_info['extension'] in ['.xlsx', '.xls', '.csv']:
                    destination = self.base_path / "01_SourceData" / "Summons" / "Court_Exports"
                else:
                    destination = self.base_path / "07_Archive" / "Legacy_Files"
            
            elif category == 'overtime':
                if file_info['extension'] in ['.xlsx', '.xls']:
                    destination = self.base_path / "01_SourceData" / "Overtime" / "Timesheets"
                else:
                    destination = self.base_path / "07_Archive" / "Legacy_Files"
            
            elif category == 'timeoff':
                destination = self.base_path / "01_SourceData" / "TimeOff" / "Vacation_Requests"
            
            elif category == 'arrests':
                destination = self.base_path / "01_SourceData" / "Arrests" / "Booking_Reports"
            
            elif category == 'response':
                destination = self.base_path / "01_SourceData" / "ResponseTimes" / "CAD_Exports"
            
            elif category == 'assignment':
                destination = self.base_path / "01_SourceData" / "Assignment_Master"
            
            elif category == 'powerbi':
                destination = self.base_path / "04_PowerBI" / "_Shared_Resources"
            
            elif category == 'reports':
                destination = self.base_path / "05_Reports" / "Ad_Hoc"
            
            elif file_info['extension'] in ['.py', '.m', '.sql']:
                destination = self.base_path / "02_ETL_Scripts" / "_Archive"
            
            else:
                destination = self.base_path / "07_Archive" / "Legacy_Files"
            
            migration_plan[category].append({
                'source': file_path,
                'destination': destination / Path(file_path).name,
                'file_info': file_info
            })
        
        return migration_plan
    
    def execute_migration(self, migration_plan, dry_run=True):
        """Execute the file migration"""
        logging.info(f"{'DRY RUN: ' if dry_run else ''}Executing file migration...")
        
        migration_results = {
            'moved': [],
            'copied': [],
            'errors': [],
            'skipped': []
        }
        
        for category, files in migration_plan.items():
            logging.info(f"Processing {category}: {len(files)} files")
            
            for file_migration in files:
                source = Path(file_migration['source'])
                destination = Path(file_migration['destination'])
                
                try:
                    if not source.exists():
                        migration_results['errors'].append(f"Source not found: {source}")
                        continue
                    
                    # Create destination directory
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    
                    # Handle duplicate names
                    if destination.exists():
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        destination = destination.parent / f"{destination.stem}_{timestamp}{destination.suffix}"
                    
                    if not dry_run:
                        # Copy instead of move for safety
                        shutil.copy2(source, destination)
                        migration_results['copied'].append(f"{source} -> {destination}")
                    else:
                        migration_results['moved'].append(f"PLAN: {source} -> {destination}")
                    
                except Exception as e:
                    error_msg = f"Error migrating {source}: {str(e)}"
                    migration_results['errors'].append(error_msg)
                    logging.error(error_msg)
        
        # Save migration log
        self._save_migration_log(migration_results, dry_run)
        
        return migration_results
    
    def _save_migration_log(self, results, dry_run):
        """Save migration results to log file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = self.base_path / "06_Documentation" / "Change_Log" / f"migration_log_{timestamp}.json"
        
        log_file.parent.mkdir(parents=True, exist_ok=True)
        
        log_data = {
            'timestamp': timestamp,
            'dry_run': dry_run,
            'summary': {
                'moved': len(results['moved']),
                'copied': len(results['copied']),
                'errors': len(results['errors']),
                'skipped': len(results['skipped'])
            },
            'details': results
        }
        
        with open(log_file, 'w') as f:
            json.dump(log_data, f, indent=2, default=str)
        
        logging.info(f"Migration log saved: {log_file}")
    
    def update_powerbi_connections(self):
        """Generate script to update Power BI data source connections"""
        
        connection_updates = {
            "Summons Dashboard": {
                "old_path": r"C:\Users\carucci_r\OneDrive - City of Hackensack\_MONTHLY_DATA\_EXPORTS\Summons",
                "new_path": r"C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons"
            },
            "Overtime Dashboard": {
                "old_path": r"C:\Users\carucci_r\OneDrive - City of Hackensack\_ASSIGNMENTS",
                "new_path": r"C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Overtime"
            }
        }
        
        # Create PowerQuery M code for connection updates
        m_code_template = '''
// Updated data source paths for {dashboard_name}
let
    Source = Excel.Workbook(File.Contents("{new_path}\\{filename}")),
    Data = Source{{[Item="{sheet_name}",Kind="Sheet"]}}[Data],
    // Continue with existing transformations...
in
    Data
'''
        
        update_script_path = self.base_path / "06_Documentation" / "Technical_Specs" / "powerbi_connection_updates.txt"
        update_script_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(update_script_path, 'w') as f:
            f.write("=== POWER BI CONNECTION UPDATE GUIDE ===\n\n")
            f.write("1. Open each Power BI file\n")
            f.write("2. Go to Transform Data > Data Source Settings\n")
            f.write("3. Update file paths as follows:\n\n")
            
            for dashboard, paths in connection_updates.items():
                f.write(f"{dashboard}:\n")
                f.write(f"  OLD: {paths['old_path']}\n")
                f.write(f"  NEW: {paths['new_path']}\n\n")
        
        logging.info(f"Power BI connection update guide saved: {update_script_path}")

def main():
    """Main migration execution"""
    print("=== POLICE ANALYTICS DASHBOARD FILE MIGRATION ===\n")
    
    migrator = FileSystemMigrator()
    
    # Step 1: Create new structure
    print("Step 1: Creating new folder structure...")
    created_folders = migrator.create_new_structure()
    print(f"✅ Created {len(created_folders)} folders\n")
    
    # Step 2: Scan existing files
    print("Step 2: Scanning existing files...")
    existing_files = migrator.scan_existing_files()
    print(f"✅ Found {len(existing_files)} files to migrate\n")
    
    # Step 3: Create migration plan
    print("Step 3: Creating migration plan...")
    migration_plan = migrator.create_migration_plan(existing_files)
    
    total_files = sum(len(files) for files in migration_plan.values())
    print(f"✅ Migration plan created for {total_files} files\n")
    
    # Show summary
    print("MIGRATION SUMMARY:")
    for category, files in migration_plan.items():
        if files:
            print(f"  {category.upper()}: {len(files)} files")
    
    # Step 4: Confirmation and execution
    print(f"\n🔍 DRY RUN: Execute migration plan? (y/n): ", end="")
    response = input().lower().strip()
    
    if response == 'y':
        print("\nExecuting DRY RUN...")
        results = migrator.execute_migration(migration_plan, dry_run=True)
        
        print(f"\n📊 DRY RUN RESULTS:")
        print(f"  Planned moves: {len(results['moved'])}")
        print(f"  Errors: {len(results['errors'])}")
        
        if results['errors']:
            print("\n❌ ERRORS FOUND:")
            for error in results['errors'][:5]:  # Show first 5 errors
                print(f"    {error}")
        
        print(f"\n🚀 Execute ACTUAL migration? (y/n): ", end="")
        final_response = input().lower().strip()
        
        if final_response == 'y':
            print("\nExecuting ACTUAL migration...")
            actual_results = migrator.execute_migration(migration_plan, dry_run=False)
            print(f"✅ Migration completed! Check logs for details.")
            
            # Update Power BI connection guide
            migrator.update_powerbi_connections()
            print(f"✅ Power BI connection guide created.")
    
    print(f"\n🎯 Next Steps:")
    print(f"  1. Review migration logs in 06_Documentation/Change_Log/")
    print(f"  2. Update Power BI data source connections")
    print(f"  3. Test ETL scripts with new folder paths")
    print(f"  4. Archive old folders after validation")

if __name__ == "__main__":
    main()