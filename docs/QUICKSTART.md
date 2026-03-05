# Quick Start Guide

## Sales ETL System - Getting Started

### What This Project Does
Extracts raw sales data → Transforms it with business logic → Loads into analytics table

### 5-Minute Setup

#### Step 1: Create Database Tables (SE11)

Create three transparent tables:

1. **ZSALES_RAW** - copy structure from ZSALES_RAW.txt
2. **ZSALES_ANALYTICS** - copy structure from ZSALES_ANALYTICS.txt
3. **ZETL_LOG** - copy structure from ZETL_LOG.txt

#### Step 2: Create ABAP Classes (SE24)

Create these classes in order:

1. **ZCL_ETL_LOGGER** (no dependencies)
2. **ZCL_ETL_EXTRACTOR** (needs ZCL_ETL_LOGGER)
3. **ZCL_ETL_TRANSFORMER** (needs ZCL_ETL_LOGGER)
4. **ZCL_ETL_LOADER** (needs ZCL_ETL_LOGGER)
5. **ZCL_ETL_ORCHESTRATOR** (needs all above classes)

#### Step 3: Create Main Report (SE38)

Create program **Z_SALES_ETL_MAIN** from Z_SALES_ETL_MAIN.abap

### Running Your First ETL

1. Execute `Z_SALES_ETL_MAIN` (SE38/SA38)
2. Selection screen appears:
   - From Date: Keep default (7 days ago)
   - To Date: Keep default (today)
   - Test Mode: ✓ Checked
3. Press Execute (F8)
4. Watch the ETL process run!

### Expected Output

```
**************************************************************
*                                                            *
*            Sales Data ETL Process                          *
*                                                            *
**************************************************************

Processing Date Range: 02/26/2026 to 03/05/2026
Test Mode: Yes

ETL Run ID: ETL20260305120000

=== EXTRACT Phase ===
120000 EXTRACT S Starting extraction from 20260226 to 20260305
120001 EXTRACT S Extracted 5 records successfully

=== TRANSFORM Phase ===
120002 TRANSFORM S Starting data transformation
120003 TRANSFORM S Transformed 5 of 5 records

=== LOAD Phase ===
120004 LOAD S Starting data load
120005 LOAD S Loaded 5 of 5 records

*** ETL Process Completed Successfully ***

============================================================
ETL Process Summary
============================================================
ETL Run ID:    ETL20260305120000
Start Time:    20260305120000
End Time:      20260305120005
Duration:      5 seconds
============================================================

Test mode - No data committed to database
```

### Understanding the Output

**Extract Phase:**
- Reads from ZSALES_RAW table
- Filters by date range
- Sample data automatically loaded for demo

**Transform Phase:**
- Calculates discounts (based on quantity)
- Calculates taxes (8%)
- Calculates profit margins
- Categorizes sales (HIGH/MEDIUM/LOW)

**Load Phase:**
- Validates data quality
- Writes to ZSALES_ANALYTICS
- Updates source record status

### Sample Data Included

The system includes 5 test records:
- Transaction T000001: 10 units @ $99.99
- Transaction T000002: 5 units @ $149.99
- Transaction T000003: 20 units @ $99.99
- Transaction T000004: 3 units @ $299.99
- Transaction T000005: 15 units @ $149.99

### Transformation Examples

**Record 1 (T000001):**
- Quantity: 10, Unit Price: $99.99
- Gross: $999.90
- Discount: $0 (no discount for qty ≤ 10)
- Tax: $79.99 (8%)
- Net: $1,079.89
- Category: MEDIUM

**Record 3 (T000003):**
- Quantity: 20, Unit Price: $99.99
- Gross: $1,999.80
- Discount: $199.98 (10% for qty > 15)
- Tax: $143.99 (8% on $1,799.82)
- Net: $1,943.81
- Category: MEDIUM (just under $2000)

### Test Mode vs Production Mode

**Test Mode (Default):**
- Checkbox checked
- No COMMIT WORK
- Data not persisted
- Safe for testing

**Production Mode:**
- Checkbox unchecked
- COMMIT WORK executed
- Data persisted to database
- Use after testing

### Common Operations

**Check logs in table ZETL_LOG:**
```abap
SE16N → ZETL_LOG → Execute
Filter by ETL_RUN_ID to see specific run
```

**View analytics results:**
```abap
SE16N → ZSALES_ANALYTICS → Execute
Check transformed data with calculations
```

**Add more source data:**
```abap
SE16N → ZSALES_RAW → Create entries
Set STATUS = 'N' for new records
Run ETL again
```

### Troubleshooting

**Problem: No records extracted**
- Check ZSALES_RAW has records with STATUS = 'N'
- Verify date range includes your data

**Problem: Transformation errors**
- Check unit_price is not zero
- Verify currency field is populated

**Problem: Load validation fails**
- Ensure all required fields populated
- Check category is HIGH/MEDIUM/LOW

### Next Steps

1. **Add Real Data**: Insert actual sales records into ZSALES_RAW
2. **Customize Logic**: Modify transformation rules in ZCL_ETL_TRANSFORMER
3. **Schedule**: Set up background job (SM36) to run daily
4. **Monitor**: Check ZETL_LOG regularly for errors
5. **Extend**: Add more transformation logic as needed

### Key Files Reference

- `PROJECT_STRUCTURE.md` - Full architecture documentation
- `Z_SALES_ETL_MAIN.abap` - Main executable program
- `ZCL_ETL_ORCHESTRATOR.abap` - ETL coordinator
- `ZCL_ETL_TRANSFORMER.abap` - Business logic here

### Support

For issues or questions:
1. Check PROJECT_STRUCTURE.md for detailed documentation
2. Review log entries in ZETL_LOG table
3. Enable debug mode (F5) in SE38 to step through

---

**Congratulations! You now have a working ABAP ETL system.**
