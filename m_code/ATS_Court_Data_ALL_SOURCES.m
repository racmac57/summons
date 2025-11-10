// 🕒 2025-10-14 (Updated for All Three Data Sources)
// Summons_Analytics/ATS_Court_Data_All_Sources
// Author: R. A. Carucci
// Purpose: Load the complete summons dataset with all three sources:
//   1. HISTORICAL_SUMMARY (Aug dashboard: Sep 2024 - Aug 2025)
//   2. COURT_CURRENT (Sep dashboard: Sep 2024 - Sep 2025)
//   3. ETICKET_CURRENT (Sep e-ticket: Individual tickets)

let
    // Load the dataset from ETL output
    Source = Excel.Workbook(
        File.Contents(
            "C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons\summons_powerbi_latest.xlsx"),
        null, true),

    // Select the Summons_Data sheet
    Data = Source{[Item="Summons_Data", Kind="Sheet"]}[Data],

    // Promote headers
    #"Promoted Headers" = Table.PromoteHeaders(Data, [PromoteAllScalars = true]),

    // Set data types for all columns
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers", {
        {"TICKET_NUMBER", type text},
        {"TICKET_COUNT", Int64.Type},
        {"IS_AGGREGATE", type logical},
        {"PADDED_BADGE_NUMBER", type text},
        {"OFFICER_DISPLAY_NAME", type text},
        {"OFFICER_NAME_RAW", type text},
        {"ISSUE_DATE", type datetime},
        {"VIOLATION_NUMBER", type text},
        {"VIOLATION_DESCRIPTION", type text},
        {"VIOLATION_TYPE", type text},
        {"TYPE", type text},
        {"STATUS", type text},
        {"LOCATION", type text},
        {"WARNING_FLAG", type text},
        {"SOURCE_FILE", type text},
        {"ETL_VERSION", type text},
        {"Year", Int64.Type},
        {"Month", Int64.Type},
        {"YearMonthKey", Int64.Type},
        {"Month_Year", type text},
        {"TEAM", type text},
        {"WG1", type text},
        {"WG2", type text},
        {"WG3", type text},
        {"WG4", type text},
        {"WG5", type text},
        {"POSS_CONTRACT_TYPE", type text},
        {"ASSIGNMENT_FOUND", type logical},
        {"DATA_QUALITY_SCORE", Int64.Type},
        {"DATA_QUALITY_TIER", type text},
        {"TOTAL_PAID_AMOUNT", type number},
        {"FINE_AMOUNT", type number},
        {"COST_AMOUNT", type number},
        {"MISC_AMOUNT", type number},
        {"PROCESSING_TIMESTAMP", type datetime}
    })

in
    #"Changed Type"
