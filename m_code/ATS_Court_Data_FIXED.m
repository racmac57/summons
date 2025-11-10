// 🕒 2025-10-14 (Transition Script Compatible - FIXED)
// Summons_Analytics/ATS_Court_Data_Enhanced
// Author: R. A. Carucci
// Purpose: Load the enhanced summons dataset processed by ETL script
// FIXED: Removed non-existent columns, matches actual Excel output

let
    // Load the enhanced dataset from your ETL output
    Source = Excel.Workbook(
        File.Contents(
            "C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons\summons_powerbi_latest.xlsx"),
        null, true),

    // Select the Summons_Data sheet
    ATS_Court_Data_Sheet = Source{[Item = "Summons_Data",
                                   Kind = "Sheet"]}[Data],

    // Promote headers
    #"Promoted Headers" =
        Table.PromoteHeaders(ATS_Court_Data_Sheet, [PromoteAllScalars = true]),

    // Set data types for key columns only (let Power BI auto-detect the rest)
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers", {
        {"TICKET_NUMBER", type text},
        {"ISSUE_DATE", type datetime},
        {"VIOLATION_NUMBER", type text},
        {"VIOLATION_DESCRIPTION", type text},
        {"TYPE", type text},
        {"ETL_VERSION", type text},
        {"OFFICER_DISPLAY_NAME", type text},
        {"PADDED_BADGE_NUMBER", type text},
        {"Year", type number},
        {"Month", type number},
        {"Month_Year", type text}
    }),

    // ===== Add user-friendly data source label =====
    #"Added Data Source Label" = Table.AddColumn(#"Changed Type", "Data Source", each 
        if [ETL_VERSION] = "ETICKET_CURRENT" then "E-Ticket System (Sept 2025+)"
        else if [ETL_VERSION] = "COURT_BACKFILL" then "Court Records (Pre-Sept 2025)"
        else if [ETL_VERSION] = "HISTORICAL_BACKFILL" then "Historical Summary (Sept 2024 - Aug 2025)"
        else "Unknown Source"
    , type text),

    // ===== Add transition period flag =====
    #"Added Period Flag" = Table.AddColumn(#"Added Data Source Label", "Data Period", each 
        if [ISSUE_DATE] < #datetime(2025, 9, 1, 0, 0, 0) then "Pre-Transition (Historical)"
        else "Post-Transition (E-Ticket)"
    , type text)

in
    #"Added Period Flag"
