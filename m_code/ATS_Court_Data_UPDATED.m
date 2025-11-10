// 🕒 2025-10-14 (Transition Script Compatible)
// Summons_Analytics/ATS_Court_Data_Enhanced
// Author: R. A. Carucci
// Purpose: Load the enhanced summons dataset processed by ETL script
// Updated: Added data source labels for HISTORICAL_BACKFILL vs ETICKET_CURRENT

let
    // Load the enhanced dataset from your ETL output
    Source = Excel.Workbook(
        File.Contents(
            "C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons\summons_powerbi_latest.xlsx"),
        null, true),

    // Select the first sheet (Excel workbook format)
    ATS_Court_Data_Sheet = Source{[Item="Sheet1", Kind="Sheet"]}[Data],

    // Promote headers
    #"Promoted Headers" =
        Table.PromoteHeaders(ATS_Court_Data_Sheet, [PromoteAllScalars = true]),

    // Set data types based on your enhanced dataset structure
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers", {
        {"TICKET_NUMBER", type text}, {"OFFICER_NAME_RAW", type text},
        {"BADGE_NUMBER_RAW", type text}, {"PADDED_BADGE_NUMBER", type text},
        {"ISSUE_DATE", type datetime}, {"VIOLATION_NUMBER", type text},
        {"VIOLATION_DESCRIPTION", type text}, {"VIOLATION_TYPE", type text},
        {"TYPE", type text}, {"STATUS", type text}, {"LOCATION", type text},
        {"WARNING_FLAG", type text}, {"SOURCE_FILE", type text},
        {"ETL_VERSION", type text}, {"Year", type number}, {"Month", type number},
        {"YearMonthKey", type number}, {"Month_Year", type text},
        {"TOTAL_PAID_AMOUNT", type number}, {"FINE_AMOUNT", type number},
        {"COST_AMOUNT", type number}, {"MISC_AMOUNT", type number},
        {"OFFICER_DISPLAY_NAME", type text}, {"TEAM", type text},
        {"WG1", type text}, {"WG2", type text}, {"WG3", type text},
        {"WG4", type text}, {"WG5", type text}, {"POSS_CONTRACT_TYPE", type text},
        {"DATA_QUALITY_SCORE", type number}, {"DATA_QUALITY_TIER", type text},
        {"PROCESSING_TIMESTAMP", type datetime}
    }),

    // ===== Add user-friendly data source label (only if column doesn't exist) =====
    #"Added Data Source Label" = if Table.HasColumns(#"Changed Type", "Data Source") then
        #"Changed Type"
    else
        Table.AddColumn(#"Changed Type", "Data Source", each 
            if [ETL_VERSION] = "ETICKET_CURRENT" then "E-Ticket System (Sept 2025+)"
            else if [ETL_VERSION] = "COURT_BACKFILL" then "Court Records (Pre-Sept 2025)"
            else if [ETL_VERSION] = "HISTORICAL_BACKFILL" then "Historical Summary (Sept 2024 - Aug 2025)"
            else "Unknown Source"
        , type text),

    // ===== Add transition period flag (only if column doesn't exist) =====
    #"Added Period Flag" = if Table.HasColumns(#"Added Data Source Label", "Data Period") then
        #"Added Data Source Label"
    else
        Table.AddColumn(#"Added Data Source Label", "Data Period", each 
            if [ISSUE_DATE] < #datetime(2025, 9, 1, 0, 0, 0) then "Pre-Transition (Historical)"
            else "Post-Transition (E-Ticket)"
        , type text)

in
    #"Added Period Flag"

