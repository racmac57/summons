// 🕒 2024-12-19-17-30
// Project: SummonsMaster/Top_5_Moving_Violations_Direct
// Author: R. A. Carucci
// Purpose: Generate Top 5 Moving Violations by Officer for Most Recent Month
// (Direct Excel Reference)

let
    // Load the current ATS Court Data directly from Excel
    Source = Excel.Workbook(
        File.Contents(
            "C:/Users/carucci_r/OneDrive - City of Hackensack/03_Staging/Summons/summons_powerbi_latest.xlsx"
        ),
        null,
        true
    ),
    ATS_Court_Data_Sheet = Source{[Item = "Summons_Data", Kind = "Sheet"]}[Data],
#"Promoted Headers" =

// Set proper data types
#"Changed Type" = Table.TransformColumnTypes(
#"Promoted Headers",
        {
            {"PADDED_BADGE_NUMBER", type text},
            {"OFFICER_DISPLAY_NAME", type text},
            {"WG1", type text}, {"WG2", type text}, {"WG3", type text},
            {"WG4", type text}, {"WG5", type text},
            {"TICKET_NUMBER", type text},
            {"ISSUE_DATE", type datetime},
            {"VIOLATION_NUMBER", type text},
            {"VIOLATION_TYPE", type text},
            {"TYPE", type text}, {"STATUS", type text},
            {"TOTAL_PAID_AMOUNT", type number},
            {"FINE_AMOUNT", type number},
            {"COST_AMOUNT", type number},
            {"MISC_AMOUNT", type number},
            {"Year", Int64.Type}, {"Month", Int64.Type},
            {"YearMonthKey", Int64.Type}, {"Month_Year", type text},
            {"ASSIGNMENT_FOUND", type logical},
            {"DATA_QUALITY_SCORE", type number},
            {"DATA_QUALITY_TIER", type text},
            {"SOURCE_FILE", type text},
            {"PROCESSING_TIMESTAMP", type datetime},
            {"ETL_VERSION", type text}
        }
    ),

    // Find the most recent month in the data for moving violations with assigned officers
    MaxYearMonthKey = 
        let
            MovingWithOfficers = Table.SelectRows(#"Changed Type", 
                each [TYPE] = "M" and [WG2] <> null and [WG2] <> "")
        in
            List.Max(MovingWithOfficers[YearMonthKey]),

// Filter for the most recent month's moving violations with assigned officers
#"Filtered Recent Month" = Table.SelectRows(#"Changed Type", 
        each [YearMonthKey] = MaxYearMonthKey and [TYPE] = "M" and [WG2] <> null and [WG2] <> ""),

// Create effective officer name (use OFFICER_DISPLAY_NAME or fallback to
// OFFICER_NAME_RAW)
#"Added Effective Officer Name" = Table.AddColumn(
#"Filtered Recent Month",
        "Effective_Officer_Name",
        each if [OFFICER_DISPLAY_NAME] <> null and [OFFICER_DISPLAY_NAME] <> "" 
             then [OFFICER_DISPLAY_NAME] 
             else [OFFICER_NAME_RAW]
    ),

// Group by effective officer name and count tickets
#"Grouped by Officer" = Table.Group(
#"Added Effective Officer Name",
        {"Effective_Officer_Name"},
        {{"Summons_Count", each Table.RowCount(_), type number}}
    ),

// Sort by summons count descending
#"Sorted by Count" = Table.Sort(
#"Grouped by Officer",
        {{"Summons_Count", Order.Descending}}
    ),

// Take top 5
#"Top 5 Officers" = Table.FirstN(#"Sorted by Count", 5),

// Add rank column
#"Added Rank" = Table.AddIndexColumn(#"Top 5 Officers", "Rank", 1, 1),

// Rename columns for display
#"Renamed Columns" = Table.RenameColumns(
#"Added Rank",
        {
            {"Effective_Officer_Name", "Officer"},
            {"Summons_Count", "Summons Count"}
        }
    )

in
#"Renamed Columns"