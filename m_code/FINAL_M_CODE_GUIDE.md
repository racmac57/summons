# FINAL M Code - Complete Power BI Solution

## Date: October 14, 2025
## All 4 Queries Ready to Use

---

## Data Source Overview

Your data comes from **one Excel file** that combines:
- **Historical data** (Sep 2024 - Aug 2025): 24 aggregate records from backfill CSV
- **Current data** (Sep 2025): 4,599 individual e-ticket records

**Single Source File:**
```
C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons\summons_powerbi_latest.xlsx
```

---

## 4 M Code Queries Created

### **1. Top 5 Moving Violations - September 2025**
**File:** `FINAL_Top_5_Moving_Sept_2025.m`

**Purpose:** Show top 5 officers with most moving violations in September 2025

**Output Columns:**
- Rank (1-5)
- Officer (name with badge)
- Badge (4-digit)
- Bureau
- Team
- Moving Summons (count)

**Expected Results:**
- 5 officers with highest moving violation counts
- Only September 2025 data
- Only individual tickets (not aggregates)

---

### **2. Top 5 Parking Violations - September 2025**
**File:** `FINAL_Top_5_Parking_Sept_2025.m`

**Purpose:** Show top 5 officers with most parking violations in September 2025

**Output Columns:**
- Rank (1-5)
- Officer (name with badge)
- Badge (4-digit)
- Bureau
- Team
- Parking Summons (count)

**Expected Results:**
- 5 officers with highest parking violation counts
- Only September 2025 data
- Only individual tickets (not aggregates)

---

### **3. 13-Month Moving Violations Trend**
**File:** `FINAL_13_Month_Moving_Trend.m`

**Purpose:** Show moving violation counts for each month (Sep 2024 - Sep 2025)

**Output Columns:**
- YearMonthKey (e.g., 202409)
- Period (e.g., "Sep 2024")
- Year
- Month
- Month_Year (e.g., "09-24")
- Total_Moving (count)
- Data_Source ("Historical Aggregate" or "September 2025 E-Ticket")

**Expected Results:**
- 13 rows (one per month from Sep 2024 to Sep 2025)
- Historical months show aggregate counts
- September 2025 shows actual e-ticket count

**Example Output:**
```
Period      Total_Moving  Data_Source
Sep 2024    463          Historical Aggregate
Oct 2024    379          Historical Aggregate
...
Aug 2025    679          Historical Aggregate
Sep 2025    577          September 2025 E-Ticket
```

---

### **4. 13-Month Parking Violations Trend**
**File:** `FINAL_13_Month_Parking_Trend.m`

**Purpose:** Show parking violation counts for each month (Sep 2024 - Sep 2025)

**Output Columns:**
- YearMonthKey (e.g., 202409)
- Period (e.g., "Sep 2024")
- Year
- Month
- Month_Year (e.g., "09-24")
- Total_Parking (count)
- Data_Source ("Historical Aggregate" or "September 2025 E-Ticket")

**Expected Results:**
- 13 rows (one per month from Sep 2024 to Sep 2025)
- Historical months show aggregate counts
- September 2025 shows actual e-ticket count

**Example Output:**
```
Period      Total_Parking  Data_Source
Sep 2024    2,566         Historical Aggregate
Oct 2024    2,616         Historical Aggregate
...
Aug 2025    3,399         Historical Aggregate
Sep 2025    4,037         September 2025 E-Ticket
```

---

## How to Add to Power BI

### For Each Query:

1. **Open Power BI Desktop**
2. Click **Home** → **Transform Data** (opens Power Query Editor)
3. Click **New Source** → **Blank Query**
4. Click **Advanced Editor**
5. **Open the .m file** in Notepad
6. **Copy all the code** (Ctrl+A, Ctrl+C)
7. **Paste** into Advanced Editor (Ctrl+V)
8. Click **Done**
9. **Rename the query** (right-click → Rename):
   - `Top_5_Moving_Sept_2025`
   - `Top_5_Parking_Sept_2025`
   - `13_Month_Moving_Trend`
   - `13_Month_Parking_Trend`
10. Repeat for all 4 queries
11. Click **Close & Apply**

---

## Dashboard Visualizations

### **Recommended Visuals:**

#### **1. Top 5 Cards**
- Use: `Top_5_Moving_Sept_2025` and `Top_5_Parking_Sept_2025`
- Visual: Table or Matrix
- Shows: Current month officer performance

#### **2. Trend Line Charts**
- Use: `13_Month_Moving_Trend` and `13_Month_Parking_Trend`
- Visual: Line Chart or Area Chart
- X-axis: Period
- Y-axis: Total_Moving or Total_Parking
- Legend: Data_Source (to distinguish historical vs current)

#### **3. Combined Trend**
- Merge both trend queries
- Show Moving and Parking on same chart
- Use dual axis if needed

---

## Data Refresh Process

### **Monthly Process (for October 2025 and beyond):**

1. **Get new e-ticket export** for the month (e.g., `25_10_e_ticketexport.csv`)

2. **Update Python script config:**
   ```python
   # In SummonsMaster_Simple.py, update line 61:
   ETICKET_FILE = Path(
       r"C:\Users\carucci_r\OneDrive - City of Hackensack\05_EXPORTS\_Summons\E_Ticket\25_10_e_ticketexport.csv"
   )
   ```

3. **Run Python script:**
   ```bash
   python SummonsMaster_Simple.py
   ```

4. **Refresh Power BI:**
   - Open Power BI file
   - Click **Refresh** button
   - All 4 queries automatically update

**That's it!** The M code doesn't need changes - it reads whatever is in the Excel file.

---

## Data Validation

After refresh, verify:

✅ **Top 5 queries show current month** (e.g., October 2025 instead of September)
✅ **Badge numbers are 4 digits** (e.g., "0223", "1234")
✅ **No "MULTIPLE OFFICERS"** in Top 5 results
✅ **Trend shows 13 months** (oldest month drops, newest month added)
✅ **Data_Source column** correctly shows "Historical Aggregate" vs current month

---

## Troubleshooting

### Problem: Top 5 shows old month

**Cause:** Python script didn't run or used old data

**Fix:**
1. Check Excel file modified date
2. Re-run Python script
3. Refresh Power BI

---

### Problem: Trends only show 12 months

**Cause:** Backfill data missing a month

**Fix:** Check that backfill CSV has all months from Sep 2024 - Aug 2025

---

### Problem: "MULTIPLE OFFICERS" appears in Top 5

**Cause:** Aggregate records not filtered out

**Fix:** Check that IS_AGGREGATE filter is working:
```m
each ([IS_AGGREGATE] = false or [IS_AGGREGATE] = null)
```

---

### Problem: Badge numbers show as decimals

**Cause:** Column type wrong

**Fix:** Ensure in M code:
```m
{"PADDED_BADGE_NUMBER", type text}
```

---

## Expected Counts (for Verification)

### **September 2025:**
- Total Records: 4,599
- Moving (M): 577
- Parking (P): 4,037
- Special (C): 9

### **Historical Aggregate (per month avg):**
- Moving: ~400
- Parking: ~2,800

---

## File Locations Summary

### **Python Script:**
```
C:\Users\carucci_r\OneDrive - City of Hackensack\02_ETL_Scripts\Summons\SummonsMaster_Simple.py
```

### **Backfill Data:**
```
C:\Users\carucci_r\OneDrive - City of Hackensack\02_ETL_Scripts\Summons\backfill_data\25_08_Hackensack Police Department - Summons Dashboard.csv
```

### **E-Ticket Data:**
```
C:\Users\carucci_r\OneDrive - City of Hackensack\05_EXPORTS\_Summons\E_Ticket\25_09_e_ticketexport.csv
```

### **Output (Power BI Source):**
```
C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons\summons_powerbi_latest.xlsx
```

### **M Code Files:**
```
C:\Users\carucci_r\OneDrive - City of Hackensack\02_ETL_Scripts\Summons\m_code\
  - FINAL_Top_5_Moving_Sept_2025.m
  - FINAL_Top_5_Parking_Sept_2025.m
  - FINAL_13_Month_Moving_Trend.m
  - FINAL_13_Month_Parking_Trend.m
```

---

**You now have a complete, automated solution!**
