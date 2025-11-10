// 🕒 2025-09-08-17-30-00
// Project: SummonsMaster/Top_5_Parking_Dynamic
// Author: R. A. Carucci
// Purpose: Generate Top 5 Parking Violations for Most Recent Month in Data

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
        Table.PromoteHeaders(ATS_Court_Data_Sheet, [PromoteAllScalars = true]),

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
            // --- TYPO FIXED ON THIS LINE ---
            {"ETL_VERSION", type text}
        }
    ),

    // Find the most recent month using the YearMonthKey for efficiency
    MaxYearMonthKey = List.Max(#"Changed Type"[YearMonthKey]),

    // Filter for the most recent month using the key
    #"Filtered Recent Month" = 
        Table.SelectRows(#"Changed Type", each [YearMonthKey] = MaxYearMonthKey),

    // Filter for Parking violations only (TYPE = "P")
    #"Filtered Parking Only" = 
        Table.SelectRows(#"Filtered Recent Month", each [TYPE] = "P"),

    // Filter out records without officer assignment
    #"Filtered Assigned Officers" = Table.SelectRows(
        #"Filtered Parking Only",
        each [WG2] <> null and [WG2] <> ""
    ),

    // Group by Officer and count tickets
    #"Grouped by Officer" = Table.Group(
        #"Filtered Assigned Officers",
        {"OFFICER_DISPLAY_NAME", "WG2"},
        {{"Summons_Count", each Table.RowCount(_), type number}}
    ),

    // Sort by summons count descending
    #"Sorted by Count" = 
        Table.Sort(#"Grouped by Officer", {{"Summons_Count", Order.Descending}}),

    // Take top 5
    #"Top 5 Officers" = Table.FirstN(#"Sorted by Count", 5),

    // Add rank column
    #"Added Index" = Table.AddIndexColumn(#"Top 5 Officers", "Rank", 1, 1),

    // Reorder columns for better display
    #"Reordered Columns" = Table.ReorderColumns(
        #"Added Index",
        {"Rank", "OFFICER_DISPLAY_NAME", "WG2", "Summons_Count"}
    ),

    // Rename columns for better display
    #"Renamed Columns" = Table.RenameColumns(
        #"Reordered Columns",
        {
            {"OFFICER_DISPLAY_NAME", "Officer"},
            {"WG2", "Bureau"},
            {"Summons_Count", "Summons Count"}
        }
    )
in
    #"Renamed Columns"