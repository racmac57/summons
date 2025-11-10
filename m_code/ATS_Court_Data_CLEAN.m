// 🕒 2025-10-14 (Clean Output Compatible)
// Summons_Analytics/ATS_Court_Data_Clean
// Author: R. A. Carucci
// Purpose: Load the clean summons dataset (Historical Summary Data Only)
// Compatible with the clean output file

let
    // Load the clean dataset
    Source = Excel.Workbook(
        File.Contents(
            "C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons\summons_powerbi_latest.xlsx"),
        null, true),

    // Select the first sheet (Excel workbook format)
    Data = Source{[Item="Sheet1", Kind="Sheet"]}[Data],

    // Promote headers
    #"Promoted Headers" = Table.PromoteHeaders(Data, [PromoteAllScalars = true]),

    // Set data types for essential columns only
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers", {
        {"TICKET_NUMBER", type text},
        {"ISSUE_DATE", type datetime},
        {"VIOLATION_NUMBER", type text},
        {"VIOLATION_DESCRIPTION", type text},
        {"TYPE", type text},
        {"ETL_VERSION", type text},
        {"OFFICER_DISPLAY_NAME", type text},
        {"Year", type number},
        {"Month", type number},
        {"Month_Year", type text},
        {"Data Source", type text},
        {"Data Period", type text}
    })

in
    #"Changed Type"
