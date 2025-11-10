# Summons Exports — Structure & Schema Summary

- **E-Ticket CSV load error:** Error tokenizing data. C error: Expected 1 fields in line 226, saw 2

- **ATS Excel**: rows=4091, cols=17, first_sheet='Sheet1'

## E-Ticket Columns

_E-Ticket CSV not loaded._

## ATS (Court) Columns

| column_name                         | inferred_dtype   | logical_type   |   non_null |   null_pct |   distinct | sample_values                                                                    |
|:------------------------------------|:-----------------|:---------------|-----------:|-----------:|-----------:|:---------------------------------------------------------------------------------|
| MUNICIPAL COURT - REPORTS ON DEMAND | object           | category       |       4091 |       0    |         76 | HACKENSACK MUNICIPAL COURT; LIST OF TICKETS ISSUED BY AGENCY - ( 0223 ); NUMBER… |
| Unnamed: 1                          | object           | category       |       4085 |       0.15 |         70 | OFFICER NAME; P.O. M ANTISTA; P.O. M JACOBSEN                                    |
| Unnamed: 10                         | object           | category       |       1294 |      68.37 |         39 | PAYMENT DATE; 08/21/2025; 09/05/2025                                             |
| Unnamed: 11                         | object           | category       |       4086 |       0.12 |          5 | ASSESSED AMOUNT; 0; 90                                                           |
| Unnamed: 12                         | object           | category       |       4086 |       0.12 |         18 | FINE AMOUNT; 29; 31                                                              |
| Unnamed: 13                         | object           | category       |       4086 |       0.12 |          7 | COST AMOUNT; 21; 24                                                              |
| Unnamed: 14                         | object           | category       |       4086 |       0.12 |         16 | MISC AMOUNT; 1.5; 1.65                                                           |
| Unnamed: 15                         | object           | category       |       4086 |       0.12 |         28 | TOTAL PAID AMOUNT; 51.5; 56.65                                                   |
| Unnamed: 16                         | object           | category       |       4086 |       0.12 |         18 | CITY COST AMOUNT; 37.5; 30.5                                                     |
| Unnamed: 2                          | object           | category       |       4085 |       0.15 |          2 | ; 0223                                                                           |
| Unnamed: 3                          | object           | text           |       4085 |       0.15 |       4085 | TICKET NUMBER; E25026479; E25026544                                              |
| Unnamed: 4                          | object           | category       |       4085 |       0.15 |         32 | ISSUE DATE; 08/21/2025; 08/28/2025                                               |
| Unnamed: 5                          | object           | category       |       4085 |       0.15 |        123 | VIOLATION NUMBER; 170-7; 39:4-138D                                               |
| Unnamed: 6                          | object           | category       |       4085 |       0.15 |          3 | TYPE; P; M                                                                       |
| Unnamed: 7                          | object           | category       |       4085 |       0.15 |          6 | STATUS; DISP; ACTI                                                               |
| Unnamed: 8                          | object           | category       |       1327 |      67.56 |         39 | DISPOSITION DATE; 08/21/2025; 09/05/2025                                         |
| Unnamed: 9                          | object           | category       |         35 |      99.14 |          4 | FIND CD; 3; 1                                                                    |

### ATS Sample (first 5 rows)

| MUNICIPAL COURT - REPORTS ON DEMAND                      | Unnamed: 1     | Unnamed: 2   | Unnamed: 3   | Unnamed: 4   | Unnamed: 5   | Unnamed: 6   | Unnamed: 7   | Unnamed: 8   | Unnamed: 9   | Unnamed: 10   | Unnamed: 11   | Unnamed: 12   | Unnamed: 13   | Unnamed: 14   | Unnamed: 15   | Unnamed: 16   |
|:---------------------------------------------------------|:---------------|:-------------|:-------------|:-------------|:-------------|:-------------|:-------------|:-------------|:-------------|:--------------|:--------------|:--------------|:--------------|:--------------|:--------------|:--------------|
| HACKENSACK MUNICIPAL COURT                               | nan            | nan          | nan          | nan          | nan          | nan          | nan          | nan          | nan          | nan           | nan           | nan           | nan           | nan           | nan           | nan           |
| LIST OF TICKETS ISSUED BY AGENCY - ( 0223 )              | nan            | nan          | nan          | nan          | nan          | nan          | nan          | nan          | nan          | nan           | nan           | nan           | nan           | nan           | nan           | nan           |
| NUMBER OF CASES ISSUED BETWEEN 08/01/2025 and 08/31/2025 | nan            | nan          | nan          | nan          | nan          | nan          | nan          | nan          | nan          | nan           | nan           | nan           | nan           | nan           | nan           | nan           |
|                                                          | OFFICER        |              | TICKET       | ISSUE        | VIOLATION    | TYPE         | STATUS       | DISPOSITION  | FIND         | PAYMENT       | ASSESSED      | FINE          | COST          | MISC          | TOTAL PAID    | CITY COST     |
|                                                          | NAME           |              | NUMBER       | DATE         | NUMBER       |              |              | DATE         | CD           | DATE          | AMOUNT        | AMOUNT        | AMOUNT        | AMOUNT        | AMOUNT        | AMOUNT        |
| 0135                                                     | P.O. M ANTISTA | 0223         | E25026479    | 08/21/2025   | 170-7        | P            | DISP         | 08/21/2025   | nan          | 08/21/2025    | 0             | 29            | 21            | 1.5           | 51.5          | 37.5          |