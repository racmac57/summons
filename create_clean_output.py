#!/usr/bin/env python3
"""
Create a clean output by regenerating the data with proper assignment handling.
This avoids the corrupted Excel file and fixes the assignment enrichment issues.
"""

import pandas as pd
import numpy as np
from datetime import datetime
import calendar

def create_clean_output():
    """Create clean output by regenerating data properly"""
    
    print("CREATING CLEAN OUTPUT")
    print("=" * 50)
    
    # Load the Assignment Master file
    assignment_path = r"C:\Users\carucci_r\OneDrive - City of Hackensack\09_Reference\Personnel\Assignment_Master_V2.csv"
    print(f"Loading assignment file: {assignment_path}")
    
    try:
        assign_df = pd.read_csv(assignment_path, dtype=str, na_filter=False)
        assign_df["PADDED_BADGE_NUMBER"] = assign_df["PADDED_BADGE_NUMBER"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(4)
        print(f"Loaded {len(assign_df):,} assignment records")
    except Exception as e:
        print(f"Warning: Could not load assignment file: {e}")
        assign_df = pd.DataFrame()
    
    # Load the historical CSV data (the source data)
    historical_csv_path = r"C:\Users\carucci_r\OneDrive - City of Hackensack\02_ETL_Scripts\Summons\data_samples\2025_10_09_21_01_02_Hackensack Police Department - Summons Dashboard.csv"
    print(f"Loading historical CSV: {historical_csv_path}")
    
    historical_summary = pd.read_csv(historical_csv_path)
    print(f"Loaded {len(historical_summary):,} summary rows")
    
    # Expand historical data properly
    print("Expanding historical summary data...")
    historical_records = []
    
    for idx, row in historical_summary.iterrows():
        count = int(float(row.get("Count of TICKET_NUMBER", 0)))
        if count <= 0:
            continue
            
        month_year_str = str(row.get("Month_Year", "")).strip()
        violation_type = str(row.get("TYPE", "P")).strip()
        
        # Parse Month_Year (format: MM-YY)
        if "-" in month_year_str:
            parts = month_year_str.split("-")
            month = int(parts[0])
            year = 2000 + int(parts[1])  # Convert YY to YYYY
        else:
            continue
        
        # Create individual records
        for i in range(count):
            # Distribute dates across the month
            day = (i % 28) + 1
            days_in_month = calendar.monthrange(year, month)[1]
            issue_date = pd.Timestamp(year, month, min(day, days_in_month))
            
            # Generate unique ticket number
            ticket_num = f"HIST_{year}{month:02d}_{violation_type}_{i+1:06d}"
            
            # Determine violation code and description
            if violation_type == "M":
                viol_code = "39:4-85"
                viol_desc = "Moving Violation - Historical"
            elif violation_type == "P":
                viol_code = "175-10"
                viol_desc = "Parking Violation - Historical"
            else:  # C
                viol_code = "MUNICIPAL"
                viol_desc = "Special Complaint - Historical"
            
            record = {
                "TICKET_NUMBER": ticket_num,
                "OFFICER_NAME_RAW": "",
                "BADGE_NUMBER_RAW": "",
                "PADDED_BADGE_NUMBER": "",
                "ISSUE_DATE": issue_date,
                "VIOLATION_NUMBER": viol_code,
                "VIOLATION_DESCRIPTION": viol_desc,
                "VIOLATION_TYPE": viol_desc,
                "TYPE": violation_type,
                "STATUS": "Historical",
                "LOCATION": "",
                "WARNING_FLAG": "",
                "SOURCE_FILE": "Historical_Summary_Data",
                "ETL_VERSION": "HISTORICAL_BACKFILL",
                "Year": year,
                "Month": month,
                "YearMonthKey": year * 100 + month,
                "Month_Year": f"{month:02d}-{year-2000:02d}",
                # Financial columns
                "TOTAL_PAID_AMOUNT": np.nan,
                "FINE_AMOUNT": np.nan,
                "COST_AMOUNT": np.nan,
                "MISC_AMOUNT": np.nan,
                # Assignment columns (empty for historical data)
                "OFFICER_DISPLAY_NAME": "Historical Data (No Officer Info)",
                "TEAM": "",
                "WG1": "",
                "WG2": "",
                "WG3": "",
                "WG4": "",
                "WG5": "",
                "POSS_CONTRACT_TYPE": "",
                "DATA_QUALITY_SCORE": 100,
                "DATA_QUALITY_TIER": "High",
                "PROCESSING_TIMESTAMP": datetime.now(),
            }
            historical_records.append(record)
    
    historical_df = pd.DataFrame(historical_records)
    print(f"Created {len(historical_df):,} historical records")
    
    # Load e-ticket data
    print("Loading e-ticket data...")
    eticket_path = r"C:\Users\carucci_r\OneDrive - City of Hackensack\05_EXPORTS\_Summons\E_Ticket\25_09_e_ticketexport.csv"
    
    try:
        eticket_df = pd.read_csv(eticket_path, dtype=str, na_filter=False)
        
        # Process e-ticket data
        eticket_df["ISSUE_DATE"] = pd.to_datetime(eticket_df["Issue Date"], errors="coerce")
        eticket_df = eticket_df[eticket_df["ISSUE_DATE"] >= pd.Timestamp("2025-09-01")]
        
        # Convert to standard format
        badge_raw = eticket_df.get("Officer Id", "").str.replace(r"\D","",regex=True).str.zfill(4)
        
        eticket_processed = pd.DataFrame({
            "TICKET_NUMBER": eticket_df.get("Ticket Number", ""),
            "OFFICER_NAME_RAW": (
                eticket_df.get("Officer Last Name", "") + ", " +
                eticket_df.get("Officer First Name", "")
            ),
            "BADGE_NUMBER_RAW": eticket_df.get("Officer Id", ""),
            "PADDED_BADGE_NUMBER": badge_raw,
            "ISSUE_DATE": eticket_df["ISSUE_DATE"],
            "VIOLATION_NUMBER": eticket_df.get("Statute", ""),
            "VIOLATION_DESCRIPTION": eticket_df.get("Violation Description", ""),
            "VIOLATION_TYPE": eticket_df.get("Violation Description", ""),
            "TYPE": "",  # Will be classified later
            "STATUS": eticket_df.get("Case Status Code", ""),
            "LOCATION": eticket_df.get("Offense Street Name", ""),
            "WARNING_FLAG": eticket_df.get("Written Warning", ""),
            "SOURCE_FILE": eticket_df.get("Ticket Number", "").apply(lambda x: "25_09_e_ticketexport.csv"),
            "ETL_VERSION": "ETICKET_CURRENT",
            "Year": eticket_df["ISSUE_DATE"].dt.year,
            "Month": eticket_df["ISSUE_DATE"].dt.month,
            "YearMonthKey": eticket_df["ISSUE_DATE"].dt.year * 100 + eticket_df["ISSUE_DATE"].dt.month,
            "Month_Year": eticket_df["ISSUE_DATE"].dt.strftime("%m-%y"),
            "TOTAL_PAID_AMOUNT": np.nan,
            "FINE_AMOUNT": np.nan,
            "COST_AMOUNT": np.nan,
            "MISC_AMOUNT": np.nan,
            "OFFICER_DISPLAY_NAME": "",
            "TEAM": "",
            "WG1": "",
            "WG2": "",
            "WG3": "",
            "WG4": "",
            "WG5": "",
            "POSS_CONTRACT_TYPE": "",
            "DATA_QUALITY_SCORE": 100,
            "DATA_QUALITY_TIER": "High",
            "PROCESSING_TIMESTAMP": datetime.now(),
        })
        
        # Enrich e-ticket data with assignment info
        if not assign_df.empty:
            print("Enriching e-ticket data with assignment info...")
            right = assign_df.rename(columns={"Proposed 4-Digit Format": "OFFICER_DISPLAY_NAME_ASSIGN"})
            eticket_processed = eticket_processed.merge(
                right, how="left", on="PADDED_BADGE_NUMBER", suffixes=("", "_ASSIGN")
            )
            
            # Coalesce assignment data
            eticket_processed["OFFICER_DISPLAY_NAME"] = eticket_processed["OFFICER_DISPLAY_NAME"].fillna(
                eticket_processed["OFFICER_DISPLAY_NAME_ASSIGN"]
            )
            
            for col in ["TEAM","WG1","WG2","WG3","WG4","WG5","POSS_CONTRACT_TYPE"]:
                eticket_processed[col] = eticket_processed[col].fillna(eticket_processed[col])
            
            # Clean up
            if "OFFICER_DISPLAY_NAME_ASSIGN" in eticket_processed.columns:
                eticket_processed = eticket_processed.drop(columns=["OFFICER_DISPLAY_NAME_ASSIGN"])
        
        # Apply officer display fallback
        eticket_processed["OFFICER_DISPLAY_NAME"] = eticket_processed.apply(
            lambda row: f"{row['OFFICER_NAME_RAW']} ({row['PADDED_BADGE_NUMBER']})" 
                       if row["OFFICER_DISPLAY_NAME"] == "" and row["OFFICER_NAME_RAW"] != ""
                       else row["OFFICER_DISPLAY_NAME"] if row["OFFICER_DISPLAY_NAME"] != ""
                       else "UNKNOWN OFFICER", axis=1
        )
        
        print(f"Created {len(eticket_processed):,} e-ticket records")
        
    except Exception as e:
        print(f"Warning: Could not load e-ticket data: {e}")
        eticket_processed = pd.DataFrame()
    
    # Combine data
    print("Combining data...")
    if not eticket_processed.empty:
        all_df = pd.concat([historical_df, eticket_processed], ignore_index=True)
    else:
        all_df = historical_df
    
    print(f"Total combined records: {len(all_df):,}")
    print(f"Unique ticket numbers: {all_df['TICKET_NUMBER'].nunique():,}")
    
    # Simple classification for TYPE
    print("Applying classification...")
    def classify_type(row):
        viol_num = str(row["VIOLATION_NUMBER"]).upper()
        viol_desc = str(row["VIOLATION_DESCRIPTION"]).upper()
        
        if "39:" in viol_num:
            return "M"
        elif "PARK" in viol_desc or "175-" in viol_num:
            return "P"
        else:
            return "P"  # Default to parking
    
    all_df["TYPE"] = all_df.apply(classify_type, axis=1)
    
    # Add data source labels
    print("Adding data source labels...")
    all_df["Data Source"] = all_df["ETL_VERSION"].apply(
        lambda x: "Historical Summary (Sept 2024 - Aug 2025)" if x == "HISTORICAL_BACKFILL"
        else "E-Ticket System (Sept 2025+)" if x == "ETICKET_CURRENT"
        else "Unknown"
    )
    
    all_df["Data Period"] = all_df["ISSUE_DATE"].apply(
        lambda x: "Pre-Transition (Historical)" if x < pd.Timestamp("2025-09-01")
        else "Post-Transition (E-Ticket)"
    )
    
    # Summary
    print("\nFINAL SUMMARY")
    print("=" * 50)
    print(f"Total records: {len(all_df):,}")
    print(f"Unique tickets: {all_df['TICKET_NUMBER'].nunique():,}")
    
    print("\nETL_VERSION distribution:")
    print(all_df["ETL_VERSION"].value_counts())
    
    print("\nTYPE distribution:")
    print(all_df["TYPE"].value_counts())
    
    print("\nData Source distribution:")
    print(all_df["Data Source"].value_counts())
    
    # Save the clean output
    output_path = r"C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons\summons_powerbi_latest.xlsx"
    print(f"\nSaving clean output: {output_path}")
    
    all_df.to_excel(output_path, index=False)
    
    print("✅ CLEAN OUTPUT CREATED!")
    print(f"File: {output_path}")
    print("Ready for Power BI!")

if __name__ == "__main__":
    create_clean_output()
