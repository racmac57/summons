// 🕒 2025-10-14-14-00 - FINAL VERSION
// Project: Top 5 Parking Violations - September 2025 ONLY
// Purpose: Show top 5 officers for parking violations in September 2025
// Data: Individual e-ticket records from processing script

let
    // Load processed data
    Source = Excel.Workbook(
        File.Contents(
            "C:/Users/carucci_r/OneDrive - City of Hackensack/03_Staging/Summons/summons_powerbi_latest.xlsx"
        ),
        null,
        true
    ),

    Summons_Data = Source{[Item = "Summons_Data", Kind = "Sheet"]}[Data],

    #"Promoted Headers" = Table.PromoteHeaders(Summons_Data, [PromoteAllScalars = true]),

    // Set data types
    #"Changed Type" = Table.TransformColumnTypes(
        #"Promoted Headers",
        {
            {"TICKET_NUMBER", type text},
            {"IS_AGGREGATE", type logical},
            {"PADDED_BADGE_NUMBER", type text},
            {"OFFICER_DISPLAY_NAME", type text},
            {"TYPE", type text},
            {"WG2", type text},
            {"TEAM", type text},
            {"Year", Int64.Type},
            {"Month", Int64.Type}
        }
    ),

    // Filter: September 2025, Individual tickets only, Parking violations
    #"Filtered Data" = Table.SelectRows(
        #"Changed Type",
        each [Year] = 2025
            and [Month] = 9
            and ([IS_AGGREGATE] = false or [IS_AGGREGATE] = null)
            and [TYPE] = "P"
            and [WG2] <> null and [WG2] <> "" and [WG2] <> "AGGREGATE"
            and [OFFICER_DISPLAY_NAME] <> null
            and [OFFICER_DISPLAY_NAME] <> ""
            and [OFFICER_DISPLAY_NAME] <> "MULTIPLE OFFICERS"
    ),

    // Group by officer and count
    #"Grouped by Officer" = Table.Group(
        #"Filtered Data",
        {"OFFICER_DISPLAY_NAME", "PADDED_BADGE_NUMBER", "WG2", "TEAM"},
        {{"Parking_Count", each Table.RowCount(_), Int64.Type}}
    ),

    // Sort descending
    #"Sorted" = Table.Sort(#"Grouped by Officer", {{"Parking_Count", Order.Descending}}),

    // Take top 5
    #"Top 5" = Table.FirstN(#"Sorted", 5),

    // Add rank
    #"Added Rank" = Table.AddIndexColumn(#"Top 5", "Rank", 1, 1, Int64.Type),

    // Reorder and rename
    #"Final" = Table.RenameColumns(
        Table.SelectColumns(
            #"Added Rank",
            {"Rank", "OFFICER_DISPLAY_NAME", "PADDED_BADGE_NUMBER", "WG2", "TEAM", "Parking_Count"}
        ),
        {
            {"OFFICER_DISPLAY_NAME", "Officer"},
            {"PADDED_BADGE_NUMBER", "Badge"},
            {"WG2", "Bureau"},
            {"Parking_Count", "Parking Summons"}
        }
    )
in
    #"Final"
