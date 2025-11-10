// 🕒 2025-10-14-13-35 - UPDATED FOR NEW DATA STRUCTURE
// Project: SummonsMaster/Top_5_Parking_Violations_Updated
// Author: R. A. Carucci (updated by Claude Code)
// Purpose: Generate Top 5 Parking Violations by Officer for Most Recent Month
// Changes: Added IS_AGGREGATE filter, fixed data types, proper column handling

let
    // Load the current summons data directly from Excel
    Source = Excel.Workbook(
        File.Contents(
            "C:/Users/carucci_r/OneDrive - City of Hackensack/03_Staging/Summons/summons_powerbi_latest.xlsx"
        ),
        null,
        true
    ),

    // Get the Summons_Data sheet
    Summons_Data_Sheet = Source{[Item = "Summons_Data", Kind = "Sheet"]}[Data],

    // Promote headers
    #"Promoted Headers" = Table.PromoteHeaders(Summons_Data_Sheet, [PromoteAllScalars = true]),

    // Set proper data types (updated to include new columns)
    #"Changed Type" = Table.TransformColumnTypes(
        #"Promoted Headers",
        {
            {"TICKET_NUMBER", type text},
            {"TICKET_COUNT", Int64.Type},
            {"IS_AGGREGATE", type logical},
            {"PADDED_BADGE_NUMBER", type text},
            {"OFFICER_DISPLAY_NAME", type text},
            {"OFFICER_NAME_RAW", type text},
            {"WG1", type text},
            {"WG2", type text},
            {"WG3", type text},
            {"WG4", type text},
            {"WG5", type text},
            {"TEAM", type text},
            {"ISSUE_DATE", type datetime},
            {"VIOLATION_NUMBER", type text},
            {"VIOLATION_DESCRIPTION", type text},
            {"VIOLATION_TYPE", type text},
            {"TYPE", type text},
            {"STATUS", type text},
            {"TOTAL_PAID_AMOUNT", type number},
            {"FINE_AMOUNT", type number},
            {"COST_AMOUNT", type number},
            {"MISC_AMOUNT", type number},
            {"Year", Int64.Type},
            {"Month", Int64.Type},
            {"YearMonthKey", Int64.Type},
            {"Month_Year", type text},
            {"ASSIGNMENT_FOUND", type logical},
            {"DATA_QUALITY_SCORE", type number},
            {"DATA_QUALITY_TIER", type text},
            {"SOURCE_FILE", type text},
            {"PROCESSING_TIMESTAMP", type datetime},
            {"ETL_VERSION", type text}
        }
    ),

    // Filter out aggregate historical records (only individual tickets)
    #"Filtered Individual Tickets" = Table.SelectRows(
        #"Changed Type",
        each [IS_AGGREGATE] = false or [IS_AGGREGATE] = null
    ),

    // Find the most recent month in the data
    MaxYearMonthKey = List.Max(#"Filtered Individual Tickets"[YearMonthKey]),

    // Filter for the most recent month
    #"Filtered Recent Month" = Table.SelectRows(
        #"Filtered Individual Tickets",
        each [YearMonthKey] = MaxYearMonthKey
    ),

    // Filter for parking violations only (TYPE = "P")
    #"Filtered Parking Only" = Table.SelectRows(
        #"Filtered Recent Month",
        each [TYPE] = "P"
    ),

    // Filter for records with officer assignment (exclude aggregates)
    #"Filtered Assigned Officers" = Table.SelectRows(
        #"Filtered Parking Only",
        each [WG2] <> null
            and [WG2] <> ""
            and [WG2] <> "AGGREGATE"
            and [OFFICER_DISPLAY_NAME] <> null
            and [OFFICER_DISPLAY_NAME] <> ""
            and [OFFICER_DISPLAY_NAME] <> "MULTIPLE OFFICERS"
            and [OFFICER_DISPLAY_NAME] <> "MULTIPLE OFFICERS (Historical)"
    ),

    // Group by officer and count tickets
    #"Grouped by Officer" = Table.Group(
        #"Filtered Assigned Officers",
        {"OFFICER_DISPLAY_NAME", "PADDED_BADGE_NUMBER", "WG2", "TEAM"},
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

    // Reorder columns for better display
    #"Reordered Columns" = Table.ReorderColumns(
        #"Added Rank",
        {"Rank", "OFFICER_DISPLAY_NAME", "PADDED_BADGE_NUMBER", "WG2", "TEAM", "Summons_Count"}
    ),

    // Rename columns for display
    #"Renamed Columns" = Table.RenameColumns(
        #"Reordered Columns",
        {
            {"OFFICER_DISPLAY_NAME", "Officer"},
            {"PADDED_BADGE_NUMBER", "Badge"},
            {"WG2", "Bureau"},
            {"TEAM", "Team"},
            {"Summons_Count", "Parking Summons"}
        }
    )

in
    #"Renamed Columns"
