// 🕒 2025-07-16-16-00-00
// Project: SummonsMaster/Previous_Month_ATS_Court_Data_DYNAMIC
// Author: R. A. Carucci
// Purpose: Dynamic M-Code for previous month ATS Court Data

let
    // Load the current ATS Court Data
    Source = Excel.Workbook(
        File.Contents(
            "C:/Users/carucci_r/OneDrive - City of Hackensack/03_Staging/Summons/summons_powerbi_latest.xlsx"
        ),
        null,
        true
    ),

    // Select the main data sheet
    Sheet1_Sheet = Source{[Item = "Summons_Data", Kind = "Sheet"]}[Data],

// Promote headers
#"Promoted Headers" = Table.PromoteHeaders(
        Sheet1_Sheet,
        [PromoteAllScalars = true]
    ),

// Ensure proper data types
#"Changed Type" = Table.TransformColumnTypes(
#"Promoted Headers",
        {
            {"PADDED_BADGE_NUMBER", type text}, {"OFFICER_DISPLAY_NAME", type text},
            {"WG1", type text}, {"WG2", type text}, {"WG3", type text},
            {"WG4", type text}, {"WG5", type text}, {"TICKET_NUMBER", type text},
            {"ISSUE_DATE", type datetime}, {"VIOLATION_NUMBER", type text},
            {"VIOLATION_TYPE", type text}, {"TYPE", type text}, {"STATUS", type text},
            {"TOTAL_PAID_AMOUNT", type number}, {"FINE_AMOUNT", type number},
            {"COST_AMOUNT", type number}, {"MISC_AMOUNT", type number},
            {"Year", Int64.Type}, {"Month", Int64.Type}, {"YearMonthKey", Int64.Type},
            {"Month_Year", type text}, {"ASSIGNMENT_FOUND", type logical},
            {"DATA_QUALITY_SCORE", type number}, {"DATA_QUALITY_TIER", type text},
            {"SOURCE_FILE", type text}, {"PROCESSING_TIMESTAMP", type datetime},
            {"ETL_VERSION", type text}
        }
    ),

// Filter for previous month dynamically
#"Filtered Previous Month" = 
        let
            // Get the start of the current month and the start of the previous month
            Today = DateTime.LocalNow(),
            StartOfCurrentMonth = Date.StartOfMonth(Today),
            StartOfPreviousMonth = Date.AddMonths(StartOfCurrentMonth, -1)
        in
            Table.SelectRows(
#"Changed Type",
                each [ISSUE_DATE] >= StartOfPreviousMonth and [ISSUE_DATE] < StartOfCurrentMonth
            ),

// Filter out any rows where WG2 is null
#"Filtered Rows" = Table.SelectRows(
#"Filtered Previous Month",
        each ([WG2] <> null)
    ),

// Final type change for the date column, if needed
#"Changed Type1" = Table.TransformColumnTypes(
#"Filtered Rows",
        {{"ISSUE_DATE", type date}}
    )

in
#"Changed Type1"