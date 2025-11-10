// 🕒 2025-09-08-16-10-00
// Summons_Analytics/Patrol_Summons_Filtered
// Author: R. A. Carucci
// Purpose: Load and filter summons data to show only Patrol Bureau summons

let
    // Load the enhanced dataset from your ETL output
    Source = Excel.Workbook(
        File.Contents(
            "C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons\summons_powerbi_latest.xlsx"
        ),
        null, true),

    // Select the Summons_Data sheet
    Summons_Data_Sheet = Source{[Item = "Summons_Data", Kind = "Sheet"]}[Data],

// Promote headers
#"Promoted Headers" = Table.PromoteHeaders(Summons_Data_Sheet, [PromoteAllScalars = true]),

// Set data types based on your enhanced dataset structure
#"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers", {
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
    {"SOURCE_FILE", type text}, {"PROCESSING_TIMESTAMP", type datetime}, {
  "ETL_VERSION", type text
}
}),

// Filter to only September 2025 data AND Patrol Bureau (WG2)
#"Filtered September 2025 Patrol" = Table.SelectRows(#"Changed Type",
    each [Month_Year] = "09-25" and [WG2] = "Patrol Bureau"),

// Add calculated columns for better analysis
#"Added Summons Category" = Table.AddColumn(#"Filtered September 2025 Patrol",
                                            "Summons_Category", each 
        if [TYPE] = "P" then "Parking" 
        else if [TYPE] = "M" then "Moving"
        else if [TYPE] = "C" then "Complaints"
        else "Other", type text),

#"Added Amount Category" = Table.AddColumn(#"Added Summons Category",
                                           "Amount_Category", each 
        if [TOTAL_PAID_AMOUNT] = 0 then "No Payment"
        else if [TOTAL_PAID_AMOUNT] <= 50 then "Low ($1-$50)"
        else if [TOTAL_PAID_AMOUNT] <= 200 then "Medium ($51-$200)"
        else "High ($201+)", type text),

#"Added Status Category" = Table.AddColumn(#"Added Amount Category",
                                           "Status_Category", each 
        if [STATUS] = "ACTI" then "Active"
        else if [STATUS] = "FTA" then "Failure to Appear"
        else if [STATUS] = "PAID" then "Paid"
        else if [STATUS] = "DISM" then "Dismissed"
        else "Other", type text),

// Sort by issue date (most recent first)
#"Sorted Rows" = Table.Sort(#"Added Status Category", {{"ISSUE_DATE", Order.Descending}}),

// Create summary table with M and P counts for September 2025 Patrol Bureau
#"Summary Counts" = Table.Group(
    #"Filtered September 2025 Patrol",
    {"TYPE"},
    {{"Count", each Table.RowCount(_), type number}}
),

// Filter to only show M and P types
#"M and P Counts" = Table.SelectRows(#"Summary Counts", 
    each [TYPE] = "M" or [TYPE] = "P"),

// Add descriptive names
#"Final Summary" = Table.AddColumn(#"M and P Counts", "Type_Description", each 
    if [TYPE] = "M" then "Moving Violations" 
    else if [TYPE] = "P" then "Parking Violations" 
    else [TYPE], type text)

in
#"Final Summary"