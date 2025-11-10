// 🕒 2025-10-14-14-00 - FINAL VERSION
// Project: 13-Month Parking Violations Trend
// Purpose: Rolling 13-month view (Sep 2024 - Sep 2025)
// Data: Backfill aggregates (Sep 24 - Aug 25) + Sept 2025 individual tickets

let
    // Load processed data (contains both backfill aggregates and Sept 2025 individual)
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
            {"TICKET_COUNT", Int64.Type},
            {"IS_AGGREGATE", type logical},
            {"TYPE", type text},
            {"Year", Int64.Type},
            {"Month", Int64.Type},
            {"YearMonthKey", Int64.Type},
            {"Month_Year", type text},
            {"ETL_VERSION", type text}
        }
    ),

    // Filter for Parking violations only
    #"Parking Only" = Table.SelectRows(#"Changed Type", each [TYPE] = "P"),

    // Split into aggregates and individual
    #"Aggregates" = Table.SelectRows(#"Parking Only", each [IS_AGGREGATE] = true),
    #"Individual" = Table.SelectRows(#"Parking Only", each [IS_AGGREGATE] = false or [IS_AGGREGATE] = null),

    // Group aggregates by month and sum TICKET_COUNT
    #"Aggregate Grouped" = Table.Group(
        #"Aggregates",
        {"Year", "Month", "YearMonthKey", "Month_Year"},
        {
            {"Total_Parking", each List.Sum([TICKET_COUNT]), Int64.Type},
            {"Data_Source", each "Historical Aggregate", type text}
        }
    ),

    // Group September 2025 individual by month
    #"Individual Grouped" = Table.Group(
        #"Individual",
        {"Year", "Month", "YearMonthKey", "Month_Year"},
        {
            {"Total_Parking", each Table.RowCount(_), Int64.Type},
            {"Data_Source", each "September 2025 E-Ticket", type text}
        }
    ),

    // Combine both datasets
    #"Combined" = Table.Combine({#"Aggregate Grouped", #"Individual Grouped"}),

    // Sort by YearMonthKey
    #"Sorted" = Table.Sort(#"Combined", {{"YearMonthKey", Order.Ascending}}),

    // Add formatted date for display
    #"Added Month Name" = Table.AddColumn(
        #"Sorted",
        "Period",
        each Date.ToText(#date([Year], [Month], 1), "MMM yyyy"),
        type text
    ),

    // Final column order
    #"Final" = Table.SelectColumns(
        #"Added Month Name",
        {"YearMonthKey", "Period", "Year", "Month", "Month_Year", "Total_Parking", "Data_Source"}
    )
in
    #"Final"
