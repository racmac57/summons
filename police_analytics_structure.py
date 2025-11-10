# 🕒 2025-06-28-16-45-00
# Project: Police_Analytics/folder_structure_setup.py
# Author: R. A. Carucci
# Purpose: Create standardized folder structure for multiple Power BI analytics dashboards

import os
from pathlib import Path
import shutil
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PoliceAnalyticsStructure:
    """Creates and manages folder structure for police analytics dashboards"""
    
    def __init__(self, base_path: str = r"C:\Users\carucci_r\OneDrive - City of Hackensack"):
        self.base_path = Path(base_path)
        
        # Define the new structure
        self.structure = {
            "01_SourceData": {
                "LawSoft": ["Arrests", "Summons", "Incidents"],
                "CAD_System": ["Response_Times", "Call_Types"],
                "Timekeeping": ["Overtime", "Time_Off", "Schedules"],
                "SCRPA": [],
                "External": ["USZips", "Demographics", "GIS"]
            },
            "02_ETL_Scripts": {
                "Summons": [],
                "Overtime_TimeOff": [],
                "Arrests": [],
                "Response_Times": [],
                "Common": ["utils.py", "config.py", "data_quality.py"]
            },
            "03_Staging": {
                "Summons": [],
                "Overtime_TimeOff": [],
                "Arrests": [],
                "Response_Times": [],
                "Dimensions": []
            },
            "04_PowerBI": {
                "Summons_Dashboard": [],
                "Overtime_TimeOff_Dashboard": [],
                "Arrests_Dashboard": [],
                "Response_Times_Dashboard": [],
                "Executive_Summary": [],
                "Templates": []
            },
            "05_Reports": {
                "Monthly": [],
                "Quarterly": [],
                "Annual": [],
                "Ad_Hoc": []
            },
            "06_Documentation": {
                "Data_Dictionary": [],
                "Process_Flows": [],
                "User_Guides": [],
                "Technical_Specs": []
            },
            "99_Archive": {
                "Old_Exports": [],
                "Backup_Scripts": [],
                "Legacy_Files": []
            }
        }
        
        # Current paths to migrate
        self.migration_map = {
            "_EXPORTS/Summons": "01_SourceData/LawSoft/Summons",
            "_LawSoft_EXPORT": "01_SourceData/LawSoft",
            "_25_SCRPA": "01_SourceData/SCRPA",
            "_REFERENCE/ASSIGNMENT_MASTER": "01_SourceData/External",
            "_Hackensack_Data_Repository": "01_SourceData/External",
            "PowerBI_Staging": "03_Staging"
        }
    
    def create_folder_structure(self):
        """Create the complete folder structure"""
        logger.info("Creating police analytics folder structure...")
        
        for main_folder, subfolders in self.structure.items():
            main_path = self.base_path / main_folder
            main_path.mkdir(exist_ok=True)
            
            if isinstance(subfolders, dict):
                for subfolder, sub_subfolders in subfolders.items():
                    sub_path = main_path / subfolder
                    sub_path.mkdir(exist_ok=True)
                    
                    # Create sub-subfolders if they exist
                    for sub_sub in sub_subfolders:
                        if sub_sub.endswith('.py'):
                            # Create empty Python files
                            (sub_path / sub_sub).touch()
                        else:
                            (sub_path / sub_sub).mkdir(exist_ok=True)
            else:
                for subfolder in subfolders:
                    (main_path / subfolder).mkdir(exist_ok=True)
        
        logger.info("✅ Folder structure created successfully")
    
    def create_readme_files(self):
        """Create README files for each major section"""
        readme_content = {
            "01_SourceData/README.md": """# Source Data
Raw data exports from various systems:
- **LawSoft**: Arrests, summons, incidents
- **CAD_System**: Response times, call data
- **Timekeeping**: Overtime, time off, schedules
- **SCRPA**: State compliance data
- **External**: Reference data (ZIP codes, GIS)

## File Naming Convention
- Format: `SYSTEM_DATATYPE_YYYY_MM.extension`
- Example: `LawSoft_Arrests_2025_06.xlsx`
""",
            
            "02_ETL_Scripts/README.md": """# ETL Scripts
Python scripts for data processing and enrichment.

## Dashboard Scripts
- `summons_etl.py`: Summons data processing
- `overtime_etl.py`: Overtime/time-off processing
- `arrests_etl.py`: Arrest data processing
- `response_times_etl.py`: CAD response time analysis

## Common Utilities
- `utils.py`: Shared functions
- `config.py`: Configuration settings
- `data_quality.py`: Validation routines
""",
            
            "03_Staging/README.md": """# Staging Area
Clean, processed data ready for Power BI consumption.

## Output Files
- Fact tables: `{dashboard}_fact_YYYY_MM.xlsx`
- Dimensions: `{entity}_dimension.xlsx`
- Aggregates: `{dashboard}_summary_YYYY_MM.xlsx`
""",
            
            "04_PowerBI/README.md": """# Power BI Dashboards
Production Power BI files and templates.

## Dashboards
- **Summons**: Traffic enforcement analytics
- **Overtime_TimeOff**: Staff scheduling and costs
- **Arrests**: Crime and arrest patterns
- **Response_Times**: Emergency response performance
- **Executive_Summary**: High-level KPIs

## Naming Convention
- Format: `{Dashboard_Name}_v{Version}.pbix`
- Example: `Summons_Dashboard_v2.1.pbix`
""",
            
            "05_Reports/README.md": """# Reports
Generated reports and exports from Power BI.

## Report Types
- **Monthly**: Standard monthly metrics
- **Quarterly**: Trend analysis
- **Annual**: Year-end summaries
- **Ad_Hoc**: Special requests

## Export Formats
- PDF for distribution
- Excel for detailed analysis
- PowerPoint for presentations
"""
        }
        
        for file_path, content in readme_content.items():
            full_path = self.base_path / file_path
            full_path.write_text(content)
        
        logger.info("✅ README files created")
    
    def create_config_files(self):
        """Create configuration files for the ETL process"""
        
        # Main config file
        config_content = '''# 🕒 2025-06-28-16-45-00
# Project: Police_Analytics/config.py
# Author: R. A. Carucci
# Purpose: Central configuration for all analytics dashboards

from pathlib import Path

# Base paths
BASE_PATH = Path(r"C:\\Users\\carucci_r\\OneDrive - City of Hackensack")
SOURCE_PATH = BASE_PATH / "01_SourceData"
STAGING_PATH = BASE_PATH / "03_Staging"
POWERBI_PATH = BASE_PATH / "04_PowerBI"

# Data sources
PATHS = {
    'lawsoft_arrests': SOURCE_PATH / "LawSoft" / "Arrests",
    'lawsoft_summons': SOURCE_PATH / "LawSoft" / "Summons",
    'cad_responses': SOURCE_PATH / "CAD_System" / "Response_Times",
    'timekeeping_ot': SOURCE_PATH / "Timekeeping" / "Overtime",
    'timekeeping_to': SOURCE_PATH / "Timekeeping" / "Time_Off",
    'external_ref': SOURCE_PATH / "External",
    'assignment_master': SOURCE_PATH / "External" / "Assignment_Master.xlsm",
    'city_ordinances': SOURCE_PATH / "External" / "CityOrdinances.xlsx",
    'title39': SOURCE_PATH / "External" / "Title39_Full.xlsx",
    'date_dimension': SOURCE_PATH / "External" / "date_dimension.xlsx"
}

# Dashboard configurations
DASHBOARDS = {
    'summons': {
        'name': 'Municipal Summons Analytics',
        'refresh_schedule': 'Monthly',
        'key_metrics': ['Total Summons', 'Revenue', 'Officer Performance']
    },
    'overtime': {
        'name': 'Overtime & Time Off Analytics', 
        'refresh_schedule': 'Weekly',
        'key_metrics': ['OT Hours', 'OT Costs', 'Time Off Usage']
    },
    'arrests': {
        'name': 'Arrest Analytics',
        'refresh_schedule': 'Monthly', 
        'key_metrics': ['Arrest Volume', 'Demographics', 'Geographic Patterns']
    },
    'response_times': {
        'name': 'Response Time Analytics',
        'refresh_schedule': 'Daily',
        'key_metrics': ['Avg Response Time', 'Call Volume', 'Peak Hours']
    }
}

# Data quality rules
QUALITY_RULES = {
    'required_fields': ['Date', 'Officer', 'Type'],
    'date_format': '%Y-%m-%d',
    'numeric_fields': ['Amount', 'Hours', 'Count'],
    'max_missing_percent': 5
}
'''
        
        config_path = self.base_path / "02_ETL_Scripts" / "Common" / "config.py"
        config_path.write_text(config_content)
        
        # Utils file
        utils_content = '''# 🕒 2025-06-28-16-45-00
# Project: Police_Analytics/utils.py
# Author: R. A. Carucci
# Purpose: Shared utility functions for all ETL processes

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from typing import Optional, Dict, Any, Tuple

def setup_logging(log_name: str) -> logging.Logger:
    """Setup standardized logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(log_name)

def find_latest_file(directory: Path, pattern: str = "*.xlsx") -> Optional[Path]:
    """Find the most recent file matching pattern"""
    files = list(directory.glob(pattern))
    return max(files, key=lambda f: f.stat().st_mtime) if files else None

def standardize_officer_badge(badge: str) -> str:
    """Standardize badge numbers to 4-digit format"""
    return str(badge).zfill(4) if badge else "0000"

def parse_officer_name(name: str) -> Tuple[Optional[str], Optional[str]]:
    """Parse officer name to first initial and last name"""
    if pd.isna(name) or not str(name).strip():
        return None, None
    
    name_str = str(name).strip().upper()
    prefixes = ['P.O.', 'SGT', 'LT', 'CAPT', 'DET', 'OFFICER']
    
    for prefix in prefixes:
        name_str = name_str.replace(prefix, '').strip()
    
    parts = name_str.split()
    if len(parts) >= 2:
        return parts[0][0], parts[1]
    return None, None

def cast_date_columns(df: pd.DataFrame, date_cols: list) -> pd.DataFrame:
    """Cast specified columns to datetime"""
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    return df

def cast_numeric_columns(df: pd.DataFrame, numeric_cols: list) -> pd.DataFrame:
    """Cast specified columns to numeric"""
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    return df

def add_time_dimensions(df: pd.DataFrame, date_col: str = 'Date') -> pd.DataFrame:
    """Add standard time dimension columns"""
    if date_col not in df.columns:
        return df
    
    df['Year'] = df[date_col].dt.year
    df['Month'] = df[date_col].dt.month
    df['Month_Name'] = df[date_col].dt.strftime('%B')
    df['Quarter'] = df[date_col].dt.quarter
    df['Week_of_Year'] = df[date_col].dt.isocalendar().week
    df['Day_of_Week'] = df[date_col].dt.dayofweek
    df['Day_Name'] = df[date_col].dt.strftime('%A')
    df['Is_Weekend'] = df[date_col].dt.weekday >= 5
    
    return df

def export_to_excel(df: pd.DataFrame, filename: str, output_path: Path) -> None:
    """Export dataframe to Excel with standardized formatting"""
    full_path = output_path / filename
    df.to_excel(full_path, index=False)
    logging.info(f"Exported {len(df)} records to {full_path}")
'''
        
        utils_path = self.base_path / "02_ETL_Scripts" / "Common" / "utils.py"
        utils_path.write_text(utils_content)
        
        logger.info("✅ Configuration files created")
    
    def migrate_existing_files(self, dry_run: bool = True):
        """Migrate existing files to new structure"""
        logger.info(f"{'DRY RUN: ' if dry_run else ''}Migrating existing files...")
        
        migration_log = []
        
        for old_path, new_path in self.migration_map.items():
            source = self.base_path / old_path
            destination = self.base_path / new_path
            
            if source.exists():
                if not dry_run:
                    destination.mkdir(parents=True, exist_ok=True)
                    if source.is_file():
                        shutil.copy2(source, destination / source.name)
                    else:
                        shutil.copytree(source, destination, dirs_exist_ok=True)
                
                migration_log.append(f"{'WOULD MOVE' if dry_run else 'MOVED'}: {old_path} → {new_path}")
            else:
                migration_log.append(f"NOT FOUND: {old_path}")
        
        for log_entry in migration_log:
            logger.info(log_entry)
        
        return migration_log
    
    def create_dashboard_templates(self):
        """Create template scripts for each dashboard"""
        
        templates = {
            "summons_etl_template.py": '''# Template for Summons ETL
from utils import *
from config import PATHS, DASHBOARDS

def run_summons_etl():
    """Process summons data for Power BI"""
    logger = setup_logging('summons_etl')
    
    # Load files
    summons_file = find_latest_file(PATHS['lawsoft_summons'])
    assignment_df = pd.read_excel(PATHS['assignment_master'])
    
    # Process data
    # ... (your existing summons logic here)
    
    # Export to staging
    output_path = STAGING_PATH / "Summons"
    export_to_excel(fact_table, "summons_fact.xlsx", output_path)
    
if __name__ == "__main__":
    run_summons_etl()
''',
            
            "overtime_etl_template.py": '''# Template for Overtime ETL
from utils import *
from config import PATHS, DASHBOARDS

def run_overtime_etl():
    """Process overtime/time-off data for Power BI"""
    logger = setup_logging('overtime_etl')
    
    # Load files
    ot_file = find_latest_file(PATHS['timekeeping_ot'])
    to_file = find_latest_file(PATHS['timekeeping_to'])
    
    # Process data
    # ... (your overtime/time-off logic here)
    
    # Export to staging
    output_path = STAGING_PATH / "Overtime_TimeOff"
    export_to_excel(combined_df, "overtime_timeoff_fact.xlsx", output_path)

if __name__ == "__main__":
    run_overtime_etl()
'''
        }
        
        template_path = self.base_path / "02_ETL_Scripts"
        for filename, content in templates.items():
            (template_path / filename).write_text(content)
        
        logger.info("✅ Dashboard templates created")
    
    def run_complete_setup(self, dry_run: bool = True):
        """Run the complete setup process"""
        logger.info("🚀 Starting Police Analytics folder setup...")
        
        # Create structure
        self.create_folder_structure()
        
        # Create documentation
        self.create_readme_files()
        
        # Create config files
        self.create_config_files()
        
        # Create templates
        self.create_dashboard_templates()
        
        # Show migration plan
        migration_log = self.migrate_existing_files(dry_run=dry_run)
        
        logger.info("✅ Setup complete!")
        
        return {
            'structure_created': True,
            'migration_plan': migration_log,
            'next_steps': [
                "Review migration plan",
                "Run with dry_run=False to execute migration", 
                "Update existing scripts to use new paths",
                "Configure Power BI data sources",
                "Set up refresh schedules"
            ]
        }


# Main execution
if __name__ == "__main__":
    # Initialize setup
    setup = PoliceAnalyticsStructure()
    
    # Run setup (dry run first to see what will happen)
    result = setup.run_complete_setup(dry_run=True)
    
    print("\n📋 Setup Summary:")
    print("=" * 50)
    print("✅ Folder structure created")
    print("✅ Documentation files created") 
    print("✅ Configuration files created")
    print("✅ Template scripts created")
    
    print("\n📁 Migration Plan:")
    for log_entry in result['migration_plan']:
        print(f"  {log_entry}")
    
    print("\n🎯 Next Steps:")
    for step in result['next_steps']:
        print(f"  • {step}")
    
    print("\n💡 To execute migration, run:")
    print("setup.run_complete_setup(dry_run=False)")
