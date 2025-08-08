#!/usr/bin/env python3
"""
Enhanced Police Department Overtime Analytics System - WORKING VERSION
"""

import argparse
import logging
from pathlib import Path
from typing import Dict, List, Union

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Path constants
DEFAULT_BASE_PATH = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\02_ETL_Scripts\Overtime_TimeOff")
OVERTIME_EXPORT_PATH = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\05_EXPORTS\_Overtime")
TIME_OFF_EXPORT_PATH = Path(r"C:\Users\carucci_r\OneDrive - City of Hackensack\05_EXPORTS\_Time_Off")


class EnhancedPoliceAnalytics:
    """Enhanced Police Analytics - Minimal Working Version"""

    def __init__(self, base_path=None):
        """Initialize analytics."""
        self.base_path = Path(base_path or DEFAULT_BASE_PATH)
        self.overtime_folder = OVERTIME_EXPORT_PATH
        self.timeoff_folder = TIME_OFF_EXPORT_PATH
        self.output_dir = self.base_path / "output"
        
        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Analytics initialized - Base: {self.base_path}")

    def verify_data_sources(self) -> Dict[str, Union[bool, int, List[str]]]:
        """Verify data source folders and files."""
        verification = {}
        
        # Check overtime folder
        ot_files = list(self.overtime_folder.glob("*.xls*")) if self.overtime_folder.exists() else []
        verification['overtime_folder_exists'] = self.overtime_folder.exists()
        verification['overtime_files_found'] = len(ot_files)
        verification['overtime_files'] = [f.name for f in ot_files]
        
        # Check time-off folder
        to_files = list(self.timeoff_folder.glob("*.xls*")) if self.timeoff_folder.exists() else []
        verification['timeoff_folder_exists'] = self.timeoff_folder.exists()
        verification['timeoff_files_found'] = len(to_files)
        verification['timeoff_files'] = [f.name for f in to_files]
        
        logger.info(f"Overtime: {verification['overtime_files_found']} files in {self.overtime_folder}")
        logger.info(f"Time-off: {verification['timeoff_files_found']} files in {self.timeoff_folder}")
        
        return verification

    def run_verification(self) -> int:
        """Run verification and display results."""
        verification = self.verify_data_sources()
        
        print("\n" + "="*60)
        print("ENHANCED POLICE ANALYTICS - DATA SOURCE VERIFICATION")
        print("="*60)
        
        print(f"\nBase Path: {self.base_path}")
        print(f"Output Directory: {self.output_dir}")
        
        print(f"\nOvertime Data:")
        print(f"  Folder Exists: {verification['overtime_folder_exists']}")
        print(f"  Files Found: {verification['overtime_files_found']}")
        print(f"  Location: {self.overtime_folder}")
        
        print(f"\nTime-Off Data:")
        print(f"  Folder Exists: {verification['timeoff_folder_exists']}")
        print(f"  Files Found: {verification['timeoff_files_found']}")
        print(f"  Location: {self.timeoff_folder}")
        
        if verification['overtime_files_found'] > 0:
            print(f"\nOvertime Files:")
            for file in verification['overtime_files'][:5]:
                print(f"  - {file}")
            if verification['overtime_files_found'] > 5:
                print(f"  ... and {verification['overtime_files_found'] - 5} more")
        
        if verification['timeoff_files_found'] > 0:
            print(f"\nTime-Off Files:")
            for file in verification['timeoff_files'][:5]:
                print(f"  - {file}")
            if verification['timeoff_files_found'] > 5:
                print(f"  ... and {verification['timeoff_files_found'] - 5} more")
        
        # Summary
        total_files = verification['overtime_files_found'] + verification['timeoff_files_found']
        print(f"\nValidation Summary:")
        print(f"  Total Data Files: {total_files}")
        print(f"  Ready for Analytics: {'✓ YES' if total_files > 0 else '✗ NO'}")
        
        if total_files == 0:
            print(f"\nAction Required:")
            print(f"  1. Verify POSS export files are in expected locations")
            print(f"  2. Check folder permissions and network connectivity")
            print(f"  3. Ensure files contain 'OTActivity' and 'TimeOffActivity' sheets")
            return 1
        
        print(f"\n✓ Verification completed successfully")
        return 0


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Enhanced Police Department Overtime & Time-Off Analytics",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python enhanced_police_analytics_final.py --verify-only
  python enhanced_police_analytics_final.py --help
        """
    )
    
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Only verify data sources and folder structure"
    )
    
    parser.add_argument(
        "--base-path",
        type=str,
        help="Custom base path for analytics"
    )
    
    args = parser.parse_args()
    
    # Initialize analytics
    try:
        analytics = EnhancedPoliceAnalytics(base_path=args.base_path)
        logger.info("Enhanced Police Analytics initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize analytics: {e}")
        return 1
    
    # Handle verification
    if args.verify_only:
        return analytics.run_verification()
    
    # Default: show help
    parser.print_help()
    return 0


if __name__ == "__main__":
    exit(main())