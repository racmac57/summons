let
    // Load the processed Excel file
    Source = Excel.Workbook(File.Contents("C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons\summons_powerbi_latest.xlsx"), null, true),
    
    // Get the Summons_Data sheet
    SummonsData = Source{[Item="Summons_Data",Kind="Sheet"]}[Data],
    
    // Promote headers
    PromotedHeaders = Table.PromoteHeaders(SummonsData, [PromoteAllScalars=true]),
    
    // Set data types for key columns (using actual column names with line breaks)
    ChangedTypes = Table.TransformColumnTypes(PromotedHeaders, {
        {"TICKET#(lf)NUMBER", type text},
        {"OFFICER#(lf)NAME", type text},
        {"ISSUE#(lf)DATE", type datetime},
        {"TOTAL PAID#(lf)AMOUNT", type number},
        {"FINE #(lf)AMOUNT", type number},
        {"COST #(lf)AMOUNT", type number},
        {"ASSESSED#(lf)AMOUNT", type number},
        {"YearMonthKey", Int64.Type},
        {"Year", Int64.Type},
        {"Month", Int64.Type},
        {"Quarter", Int64.Type},
        {"FiscalYear", Int64.Type},
        {"FiscalQuarter", Int64.Type},
        {"DATA_QUALITY_SCORE", type number},
        {"VIOLATION_TYPE", type text},
        {"Month_Year", type text}
    }),
    
    // Filter out records with blank TICKET_NUMBER
    FilteredRows = Table.SelectRows(ChangedTypes, each [#"TICKET#(lf)NUMBER"] <> null and [#"TICKET#(lf)NUMBER"] <> "")

in
    FilteredRows