// Data Source Verification Query
// Purpose: Verify the transition script correctly split historical vs e-ticket data
// Use this as a temporary query to validate your data

let
    // Load your main dataset
    Source = Excel.Workbook(
        File.Contents(
            "C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons\summons_powerbi_latest.xlsx"),
        null, true),
    
    ATS_Court_Data_Sheet = Source{[Item = "Summons_Data", Kind = "Sheet"]}[Data],
    #"Promoted Headers" = Table.PromoteHeaders(ATS_Court_Data_Sheet, [PromoteAllScalars = true]),
    
    // Change types
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers", {
        {"ISSUE_DATE", type datetime}, 
        {"ETL_VERSION", type text},
        {"TYPE", type text},
        {"Month_Year", type text}
    }),
    
    // Group by ETL_VERSION and Month_Year to see the split
    #"Grouped Rows" = Table.Group(#"Changed Type", {"ETL_VERSION", "Month_Year", "TYPE"}, {
        {"Count", each Table.RowCount(_), Int64.Type},
        {"Min Date", each List.Min([ISSUE_DATE]), type nullable datetime},
        {"Max Date", each List.Max([ISSUE_DATE]), type nullable datetime}
    }),
    
    // Sort by Month_Year to see chronological progression
    #"Sorted Rows" = Table.Sort(#"Grouped Rows", {{"Month_Year", Order.Ascending}, {"TYPE", Order.Ascending}})

in
    #"Sorted Rows"

